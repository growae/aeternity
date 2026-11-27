use super::*;
use ae_chain::repository::pow;
use ae_sync::chain::pow::{generator, packing, producer};
use ae_sync::keys;
use serde_json::Value;

fn native() -> Value {
    serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/erlang_stratum_identity.json"
    ))
    .unwrap()
}
fn keys(t: &Temp) -> keys::Worker {
    keys::Worker::spawn(keys::Config::new(
        t.0.join("managed-keys"),
        b"isolated fixture".to_vec(),
    ))
    .unwrap()
}
fn managed(
    db: &Arc<Database>,
    importer: &chain::Importer,
    clock: Arc<Wall>,
    keys: keys::Client,
) -> (Inputs, ae_storage::snapshots::Owner) {
    let (mut inputs, snapshots) = configuration(db, importer, clock);
    inputs.production = Some(production::Inputs::managed(
        snapshots.clone(),
        keys,
        AccountPublicKey([9; 32]),
    ));
    (inputs, snapshots)
}

#[test]
fn managed_stratum_samples_new_candidate_keys_without_changing_the_reward_beneficiary() {
    let native = native();
    for row in native["preparation"].as_array().unwrap() {
        assert_eq!(row["candidate"], row["miner"]);
        assert_eq!(row["beneficiary"], 9);
        assert_eq!(row["parent_matches"], true);
    }
    let stopped = native["dispatch"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["mode"] == "stratum" && r["running"] == false)
        .unwrap();
    assert_eq!(
        stopped["events"],
        serde_json::json!(["start_mining", "stratum_new_candidate"])
    );
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let keys = keys(&t);
    let first_key = keys.info().keys.candidate;
    let clock = Wall::new(first().time);
    let (inputs, snapshots) = managed(&db, &importer, clock.clone(), keys.client());
    let owner = owner(&db, inputs);
    let accounting = owner.client();
    let client = accounting.mining().unwrap();
    let services = services::Owner::new(Default::default()).unwrap();
    let _connection = registered(&services.client());
    client.attach_production(services.client()).unwrap();
    let one = latest(&client, 1);
    assert_eq!(one.candidate.header().miner, first_key.0);
    assert_eq!(one.candidate.header().beneficiary, [9; 32]);
    let next = keys
        .client()
        .promote_candidate(first_key)
        .unwrap()
        .blocking_wait()
        .unwrap()
        .candidate;
    assert_ne!(next, first_key);
    clock.set(first().time + 60_000);
    let block = fixture::blocks(&NetworkProfile::mainnet())
        .into_iter()
        .next()
        .unwrap();
    settle(
        &mut importer
            .import_created_block(block.encode().unwrap(), WALL)
            .unwrap(),
    )
    .unwrap();
    let two = latest(&client, 2);
    assert_eq!(two.candidate.parent(), block.hash().unwrap());
    assert_eq!(two.candidate.header().miner, next.0);
    assert_eq!(two.candidate.header().beneficiary, [9; 32]);
    assert_eq!(client.production_status().unwrap().miner, Some(next));
    keys.shutdown().unwrap();
    until(|| accounting.is_closed());
    let report = owner.shutdown();
    assert!(matches!(
        report.failure,
        Some(Error::ProductionKeys(keys::Error::Stopped))
    ));
    assert!(!report.production.unwrap().status.running);
    assert_eq!(snapshots.live_snapshots(), 0);
    let _ = services.shutdown();
    stop(importer);
}

#[test]
fn managed_stratum_preparation_retains_its_captured_key_through_promotion() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let keys = keys(&t);
    let captured = keys.info().keys.candidate;
    let clock = Wall::new(first().time);
    let (release, receive) = mpsc::channel();
    *clock.gate.lock().unwrap() = Some(receive);
    let (inputs, snapshots) = managed(&db, &importer, clock, keys.client());
    let owner = owner(&db, inputs);
    let client = owner.client().mining().unwrap();
    let services = services::Owner::new(Default::default()).unwrap();
    let _connection = registered(&services.client());
    client.attach_production(services.client()).unwrap();
    until(|| client.production_status().unwrap().preparing.is_some());
    let next = keys
        .client()
        .promote_candidate(captured)
        .unwrap()
        .blocking_wait()
        .unwrap()
        .candidate;
    assert_ne!(next, captured);
    release.send(()).unwrap();
    let work = latest(&client, 1);
    assert_eq!(work.candidate.header().miner, captured.0);
    let report = owner.shutdown();
    assert!(report.failure.is_none());
    let preparation = report
        .production
        .unwrap()
        .last_preparation
        .unwrap()
        .unwrap();
    assert_eq!(preparation.request.miner, captured);
    assert_eq!(snapshots.live_snapshots(), 0);
    let _ = services.shutdown();
    keys.shutdown().unwrap();
    stop(importer);
}

#[test]
fn stopped_managed_keys_close_stratum_and_join_a_blocked_preparation_before_release() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let keys = keys(&t);
    let clock = Wall::new(first().time);
    let (release, receive) = mpsc::channel();
    *clock.gate.lock().unwrap() = Some(receive);
    let (inputs, snapshots) = managed(&db, &importer, clock, keys.client());
    let owner = owner(&db, inputs);
    let accounting = owner.client();
    let client = accounting.mining().unwrap();
    let services = services::Owner::new(Default::default()).unwrap();
    let _connection = registered(&services.client());
    client.attach_production(services.client()).unwrap();
    until(|| client.production_status().unwrap().preparing.is_some());
    keys.shutdown().unwrap();
    until(|| accounting.is_closed());
    let (send, receive) = mpsc::channel();
    let join = thread::spawn(move || send.send(owner.shutdown()).unwrap());
    assert!(matches!(
        receive.recv_timeout(Duration::from_millis(40)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release.send(()).unwrap();
    let report = receive.recv_timeout(Duration::from_secs(5)).unwrap();
    join.join().unwrap();
    assert!(matches!(
        report.failure,
        Some(Error::ProductionKeys(keys::Error::Stopped))
    ));
    assert_eq!(report.production.as_ref().unwrap().status.published, 0);
    assert!(report.production.unwrap().last_preparation.is_some());
    assert_eq!(snapshots.live_snapshots(), 0);
    let _ = services.shutdown();
    stop(importer);
}

struct ControllerClock(Mutex<Option<mpsc::Receiver<()>>>);
impl producer::Clock for ControllerClock {
    fn now(&self) -> Result<(u64, Instant), producer::Error> {
        if let Some(receive) = self.0.lock().unwrap().take() {
            receive.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        producer::SystemClock.now()
    }
}

#[test]
fn coordinated_stratum_waits_for_the_controller_and_publishes_while_micro_production_is_stopped() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let keys = keys(&t);
    let candidate_key = keys.info().keys.candidate;
    let network = NetworkProfile::mainnet();
    let pool = ae_storage::pool::worker::Worker::spawn(
        db.clone(),
        network.clone(),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let clock = Wall::new(first().time);
    let (mut inputs, snapshots) = configuration(&db, &importer, clock.clone());
    let preparation = fixture::execution(&network).with_config(|c| {
        let fate = c.fate.unwrap();
        pow::Config {
            identity: db.view().unwrap().identity().clone(),
            profile: Arc::new(pow::Profile {
                contracts: ae_hyperchain::consensus_contract::Contracts {
                    owner: AccountPublicKey([0; 32]),
                    election: [0; 32],
                    staking: [0; 32],
                    rewards: [0; 32],
                },
                network: c.network.clone(),
                context: fate.context.clone(),
                order: fate.order.clone(),
                overrides: fate.overrides.clone(),
                diagnostic: fate.diagnostic_profile,
                limits: c.fate_limits,
                maximum_steps: c.maximum_fate_steps,
                maximum_history_reads: 1024,
            }),
            execution: pow::Execution {
                aevm: c.aevm.cloned(),
                ..Default::default()
            },
            micro_cycle_ms: 3000,
        }
    });
    let generator = generator::Worker::spawn(
        db.clone(),
        snapshots.clone(),
        pool.client(),
        generator::Config {
            preparation,
            candidate: Default::default(),
            packing: packing::Config {
                timeout: ae_chain::hyperchain::candidate::Timeout::Disabled,
                window_ms: 3000,
            },
            controls: 8,
            publications: 4,
            queued_transactions: 1000,
            queued_bytes: 1 << 20,
            feed_batch: 16,
            poll_interval: Duration::from_millis(2),
        },
        clock,
    )
    .unwrap();
    let (release, receive) = mpsc::channel();
    let controller = producer::Worker::spawn(
        &importer,
        generator.client(),
        keys.client(),
        producer::Config {
            enabled: false,
            external_key_work: true,
            resume_current: true,
            poll: Duration::from_millis(2),
            ..Default::default()
        },
        ControllerClock(Mutex::new(Some(receive))),
    )
    .unwrap();
    // Equal genesis hashes do not make two database owners interchangeable.
    let other = Temp::new();
    let other_db = open(&other);
    let other_importer = super::importer(&other_db, 1);
    let other_snapshots =
        ae_storage::snapshots::Owner::start(other_db.clone(), Default::default()).unwrap();
    assert!(matches!(
        production::Worker::new(
            &other_importer.stratum_admitter(),
            production::Inputs::coordinated(
                other_snapshots.clone(),
                controller.client(),
                AccountPublicKey([9; 32])
            ),
            Wall::new(first().time),
        ),
        Err(Error::ProductionController(producer::Error::Binding))
    ));
    assert_eq!(other_snapshots.live_snapshots(), 0);
    stop(other_importer);
    inputs.production = Some(production::Inputs::coordinated(
        snapshots.clone(),
        controller.client(),
        AccountPublicKey([9; 32]),
    ));
    let owner = owner(&db, inputs);
    let accounting = owner.client();
    let client = accounting.mining().unwrap();
    let services = services::Owner::new(Default::default()).unwrap();
    let _connection = registered(&services.client());
    client.attach_production(services.client()).unwrap();
    until(|| client.production_status().unwrap().waiting_for_identity);
    assert_eq!(client.production_status().unwrap().published, 0);
    assert!(controller.client().status().promotion_pending);
    release.send(()).unwrap();
    let work = latest(&client, 1);
    assert_eq!(work.candidate.header().miner, candidate_key.0);
    assert_eq!(work.candidate.header().beneficiary, [9; 32]);
    let status = controller.client().status();
    assert!(!status.enabled);
    assert_eq!(status.signatures, 0);
    assert_eq!(status.admissions, 0);
    assert!(status.mining.is_none());
    // Genesis has another miner: no key promotion is submitted, and the
    // current candidate is still valid for preparing external key work.
    assert_eq!(status.promotions, 0);
    assert!(controller.client().front().is_none());
    controller.client().request_shutdown();
    until(|| accounting.is_closed());
    let report = owner.shutdown();
    assert!(matches!(
        report.failure,
        Some(Error::ProductionController(producer::Error::Closed))
    ));
    let mut report = controller.shutdown().unwrap();
    until(|| report.poll());
    assert!(report.status.failure.is_none());
    drop(report);
    drop(generator.shutdown().unwrap());
    pool.shutdown().unwrap();
    keys.shutdown().unwrap();
    let _ = services.shutdown();
    assert_eq!(snapshots.live_snapshots(), 0);
    stop(importer);
}
