use super::super::production;
use super::*;
use ae_stratum::{session, share::User};
use std::sync::atomic::AtomicI64;
struct Wall {
    value: AtomicI64,
    gate: Mutex<Option<mpsc::Receiver<()>>>,
}
impl Wall {
    fn new(value: u64) -> Arc<Self> {
        Arc::new(Self {
            value: AtomicI64::new(value.try_into().unwrap()),
            gate: Mutex::new(None),
        })
    }
    fn set(&self, value: u64) {
        self.value
            .store(value.try_into().unwrap(), Ordering::Release);
    }
}
impl Clock for Wall {
    fn unix_millis(&self) -> i128 {
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        self.value.load(Ordering::Acquire).into()
    }
}
fn configuration(
    db: &Arc<Database>,
    importer: &chain::Importer,
    clock: Arc<Wall>,
) -> (Inputs, ae_storage::snapshots::Owner) {
    let h = first();
    let snapshots = ae_storage::snapshots::Owner::start(db.clone(), Default::default()).unwrap();
    let mut production = production::Inputs::new(
        snapshots.clone(),
        AccountPublicKey(h.miner),
        AccountPublicKey(h.beneficiary),
    );
    production.retry = Duration::from_millis(10);
    let mut config = inputs(importer);
    config.clock = clock;
    config.production = Some(production);
    (config, snapshots)
}
fn owner(db: &Arc<Database>, config: Inputs) -> Accounting {
    Accounting::start_mining(
        db.clone(),
        Default::default(),
        Default::default(),
        Default::default(),
        config,
    )
    .unwrap()
}
fn registered(services: &services::Client) -> services::Connection {
    let connection = services.connect().unwrap();
    connection
        .execute(session::Command {
            id: 1,
            operation: session::Operation::Register {
                user: User {
                    account: "ak_miner".into(),
                    worker: "one".into(),
                },
            },
        })
        .unwrap();
    connection
}
fn latest(client: &Client, minimum: u64) -> journal::Publication {
    until(|| client.production_status().unwrap().published >= minimum);
    client.production_status().unwrap().last.unwrap()
}
fn native_active(name: &str) -> usize {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../ae-stratum/tests/fixtures/erlang_stratum_feed.json"
    ))
    .unwrap();
    fixture["lifecycle"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == name)
        .unwrap()["active"]
        .as_u64()
        .unwrap() as usize
}
#[test]
fn native_generation_rules_keep_same_generation_micro_work_and_retire_key_work_before_accounting() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let mut blocks = fixture::blocks(&NetworkProfile::mainnet()).into_iter();
    let first_block = blocks.next().unwrap();
    settle(
        &mut importer
            .import_created_block(first_block.encode().unwrap(), WALL)
            .unwrap(),
    )
    .unwrap();
    let snapshots = ae_storage::snapshots::Owner::start(db.clone(), Default::default()).unwrap();
    let header = first();
    let prepared = importer
        .stratum_builder(snapshots, Default::default())
        .unwrap()
        .start(
            preparation::Request {
                parent: first_block.hash().unwrap(),
                miner: AccountPublicKey(header.miner),
                beneficiary: AccountPublicKey(header.beneficiary),
                release_info: 0,
            },
            Arc::new(ClockValue((header.time + 10_000).into())),
        )
        .unwrap()
        .wait()
        .unwrap();
    let candidate = Prepared::from_completion(prepared)
        .unwrap()
        .candidate()
        .as_ref()
        .clone();
    let mut writer = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let epoch = writer.begin_mining(Default::default()).unwrap();
    writer
        .publish_work(&epoch, 1, candidate.clone(), date(), Default::default())
        .unwrap();
    let micro = blocks.next().unwrap();
    assert!(matches!(micro, Block::Micro(_)));
    settle(
        &mut importer
            .import_created_block(micro.encode().unwrap(), WALL)
            .unwrap(),
    )
    .unwrap();
    let frame = writer.refresh_work(&epoch, Default::default()).unwrap();
    assert!(!frame.invalidated);
    assert_eq!(
        frame.info.active.len(),
        native_active("same_generation_micro")
    );
    let next = blocks.next().unwrap();
    assert!(matches!(next, Block::Key(_)));
    settle(
        &mut importer
            .import_created_block(next.encode().unwrap(), WALL)
            .unwrap(),
    )
    .unwrap();
    // Do not refresh first: accepting a solution must itself observe retirement
    // and persist conversion plus Inactive atomically against the chain status.
    let ingress = writer.begin_ingress(Default::default()).unwrap();
    let receipt = writer
        .accept_mining(
            &ingress,
            1,
            input(&candidate, &header),
            Default::default(),
            &epoch,
            Default::default(),
        )
        .unwrap();
    let stored = db.stratum(Default::default()).unwrap();
    let journal = stored.mining(Default::default()).unwrap();
    assert_eq!(journal.info().active.len(), native_active("key_top"));
    let record = journal.get(receipt.id).unwrap().unwrap();
    assert_eq!(
        record.state,
        journal::State::Done(journal::Outcome::Inactive)
    );
    assert_eq!(record.accounted_key, Some(receipt.share_key));
    drop(journal);
    drop(stored);
    drop(writer);
    stop(importer);
}
#[test]
fn rollback_to_the_same_generation_and_feed_gaps_never_restore_old_authority() {
    for gap in [false, true] {
        let t = Temp::new();
        let initial = open(&t);
        let identity = initial.view().unwrap().identity().clone();
        drop(initial);
        let db = Arc::new(
            Database::open_with_notifications(
                &t.0,
                identity,
                Default::default(),
                Default::default(),
                ae_storage::Notifications {
                    chain: ae_storage::events::Config {
                        records: if gap { 1 } else { 8 },
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let importer = importer(&db, 1);
        let h = first();
        let candidate = candidate(&h);
        let mut writer = storage::Writer::start(db.clone(), Default::default()).unwrap();
        let epoch = writer.begin_mining(Default::default()).unwrap();
        writer
            .publish_work(&epoch, 1, candidate, date(), Default::default())
            .unwrap();
        settle(
            &mut importer
                .import_created_block(Block::Key(h).encode().unwrap(), WALL)
                .unwrap(),
        )
        .unwrap();
        let update = ae_chain::repository::rollback::prepare(
            &db.view().unwrap(),
            ae_chain::repository::rollback::Target::Height(0),
            Default::default(),
        )
        .unwrap();
        db.commit(update).unwrap();
        let frame = writer.refresh_work(&epoch, Default::default()).unwrap();
        assert!(frame.invalidated);
        assert_eq!(frame.gap, gap);
        assert!(frame.info.active.is_empty());
        assert!(
            !writer
                .refresh_work(&epoch, Default::default())
                .unwrap()
                .invalidated
        );
        drop(writer);
        stop(importer);
    }
}
#[test]
fn notification_pressure_retains_one_durable_publication_and_never_dispatches_it_twice() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let clock = Wall::new(first().time);
    let (config, snapshots) = configuration(&db, &importer, clock);
    let owner = owner(&db, config);
    let account = owner.client();
    let client = account.mining().unwrap();
    assert!(matches!(
        client.publish(candidate(&first()), date()),
        Err(pool::Error::Invalid)
    ));
    let services = services::Owner::new(services::Config {
        retained_reply_bytes: 2048,
        ..Default::default()
    })
    .unwrap();
    let service = services.client();
    let connection = registered(&service);
    let mut views = vec![];
    while let Ok(view) = service.registered_connections() {
        views.push(view);
    }
    client.attach_production(service.clone()).unwrap();
    assert!(client.attach_production(service.clone()).is_err());
    until(|| client.production_status().unwrap().waiting_broadcast);
    let publication = latest(&client, 1);
    assert_eq!(publication.candidate, candidate(&first()));
    assert_eq!(publication.sequence, 1);
    assert_eq!(snapshots.live_snapshots(), 0);
    thread::sleep(Duration::from_millis(35));
    assert_eq!(client.production_status().unwrap().published, 1);
    assert_eq!(client.production_status().unwrap().broadcasts, 0);
    drop(views);
    until(|| client.production_status().unwrap().broadcasts == 1);
    let event = connection.next_notification().unwrap().unwrap();
    assert!(
        matches!(&*event, session::Chain::Block(work) if work == &publication.candidate.work().unwrap())
    );
    drop(event);
    thread::sleep(Duration::from_millis(30));
    assert!(connection.next_notification().unwrap().is_none());
    assert_eq!(client.info().unwrap().last.unwrap().sequence, 1);
    let report = owner.shutdown();
    assert!(report.failure.is_none());
    let production = report.production.unwrap();
    assert!(!production.status.running);
    assert!(production.pending_publication.is_none());
    drop(report.last);
    drop(services);
    stop(importer);
}
#[test]
fn shutting_down_joins_blocked_preparation_before_releasing_the_accounting_lease() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let clock = Wall::new(first().time);
    let (send, receive) = mpsc::channel();
    *clock.gate.lock().unwrap() = Some(receive);
    let (config, snapshots) = configuration(&db, &importer, clock);
    let owner = owner(&db, config);
    let client = owner.client().mining().unwrap();
    let services = services::Owner::new(Default::default()).unwrap();
    client.attach_production(services.client()).unwrap();
    until(|| client.production_status().unwrap().preparing.is_some());
    let shutdown = thread::spawn(move || owner.shutdown());
    thread::sleep(Duration::from_millis(20));
    assert!(!shutdown.is_finished());
    assert!(storage::Writer::start(db.clone(), Default::default()).is_err());
    send.send(()).unwrap();
    let report = shutdown.join().unwrap();
    assert!(report.failure.is_none());
    assert_eq!(snapshots.live_snapshots(), 0);
    assert!(!client.production_status().unwrap().running);
    let writer = storage::Writer::start(db.clone(), Default::default()).unwrap();
    drop(writer);
    drop(services);
    stop(importer);
}
#[test]
fn scheduled_cache_expiry_preserves_native_active_work_but_missing_cache_cannot_admit() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let clock = Wall::new(first().time);
    let (mut config, _) = configuration(&db, &importer, clock.clone());
    let p = config.production.as_mut().unwrap();
    p.cleanup_interval = Duration::from_millis(10);
    p.cache_age = Duration::from_secs(1);
    let owner = owner(&db, config);
    let account = owner.client();
    let client = account.mining().unwrap();
    let services = services::Owner::new(Default::default()).unwrap();
    client.attach_production(services.client()).unwrap();
    let publication = latest(&client, 1);
    clock.set(first().time + 3000);
    until(|| {
        db.stratum(Default::default())
            .unwrap()
            .query(|tx| tx.candidate(publication.candidate.hash()))
            .unwrap()
            .is_none()
    });
    assert_eq!(client.info().unwrap().active.len(), 1);
    let id = accepted(&account, &input(&publication.candidate, &first()));
    let record = done(&client, id);
    assert_eq!(
        record.state,
        journal::State::Done(journal::Outcome::MissingCandidate)
    );
    assert_eq!(record.accounted_key, None);
    assert_eq!(
        db.view().unwrap().status().tip,
        Some(publication.candidate.parent())
    );
    assert_eq!(client.production_status().unwrap().published, 1);
    assert!(owner.shutdown().failure.is_none());
    drop(services);
    stop(importer);
}
#[test]
fn closed_broadcaster_preserves_the_exact_pending_work_and_stops_intake() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let clock = Wall::new(first().time);
    let (send, receive) = mpsc::channel();
    *clock.gate.lock().unwrap() = Some(receive);
    let (config, snapshots) = configuration(&db, &importer, clock);
    let owner = owner(&db, config);
    let account = owner.client();
    let client = account.mining().unwrap();
    let services = services::Owner::new(Default::default()).unwrap();
    client.attach_production(services.client()).unwrap();
    until(|| client.production_status().unwrap().preparing.is_some());
    drop(services);
    send.send(()).unwrap();
    until(|| account.is_closed());
    let report = owner.shutdown();
    assert!(matches!(
        report.failure,
        Some(Error::Services(services::Error::Closed))
    ));
    let production = report.production.unwrap();
    assert_eq!(
        production.pending_publication.unwrap().candidate,
        candidate(&first())
    );
    assert_eq!(snapshots.live_snapshots(), 0);
    stop(importer);
}
mod tcp;
#[test]
fn a_finished_preparation_is_retired_even_when_the_chain_returns_to_its_original_parent() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let clock = Wall::new(first().time);
    let (mut config, snapshots) = configuration(&db, &importer, clock.clone());
    let mut production = production::Worker::new(
        &config.admitter,
        config.production.take().unwrap(),
        clock.clone(),
    )
    .unwrap();
    let services = services::Owner::new(Default::default()).unwrap();
    production.shared.attach(services.client()).unwrap();
    let mut writer = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let epoch = writer.begin_mining(Default::default()).unwrap();
    let initial = writer.refresh_work(&epoch, Default::default()).unwrap();
    let original_top = initial.top;
    production
        .tick(&mut writer, &epoch, Default::default(), initial, true)
        .unwrap();
    until(|| production.task_finished());
    let old_id = production.shared.status().preparing.unwrap();
    assert_eq!(snapshots.live_snapshots(), 1);
    settle(
        &mut importer
            .import_created_block(Block::Key(first()).encode().unwrap(), WALL)
            .unwrap(),
    )
    .unwrap();
    let update = ae_chain::repository::rollback::prepare(
        &db.view().unwrap(),
        ae_chain::repository::rollback::Target::Height(0),
        Default::default(),
    )
    .unwrap();
    db.commit(update).unwrap();
    // Another journal operation has already consumed the event sequence.
    let frame = writer.refresh_work(&epoch, Default::default()).unwrap();
    assert!(frame.invalidated);
    assert_eq!(frame.top, original_top);
    let frame = writer.refresh_work(&epoch, Default::default()).unwrap();
    assert!(!frame.invalidated);
    let (send, receive) = mpsc::channel();
    *clock.gate.lock().unwrap() = Some(receive);
    production
        .tick(&mut writer, &epoch, Default::default(), frame, true)
        .unwrap();
    assert_eq!(production.shared.status().published, 0);
    assert_eq!(production.shared.status().stale, 1);
    assert_ne!(production.shared.status().preparing, Some(old_id));
    send.send(()).unwrap();
    until(|| production.task_finished());
    let frame = writer.refresh_work(&epoch, Default::default()).unwrap();
    production
        .tick(&mut writer, &epoch, Default::default(), frame, true)
        .unwrap();
    assert_eq!(production.shared.status().published, 1);
    let report = production.finish();
    assert!(report.last_preparation.unwrap().unwrap().id != old_id);
    assert_eq!(snapshots.live_snapshots(), 0);
    drop(writer);
    drop(services);
    stop(importer);
}
mod recovery;
#[test]
fn same_generation_microblocks_keep_work_until_a_conductor_reply_requests_fresh_preparation() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let clock = Wall::new(first().time);
    let (config, snapshots) = configuration(&db, &importer, clock.clone());
    let owner = owner(&db, config);
    let account = owner.client();
    let client = account.mining().unwrap();
    let services = services::Owner::new(Default::default()).unwrap();
    client.attach_production(services.client()).unwrap();
    let first_work = latest(&client, 1);
    clock.set(first().time + 60_000);
    let id = accepted(&account, &input(&first_work.candidate, &first()));
    assert!(matches!(
        done(&client, id).state,
        journal::State::Done(journal::Outcome::Accepted(_))
    ));
    let older = latest(&client, 2);
    let micro = fixture::blocks(&NetworkProfile::mainnet()).remove(1);
    assert!(matches!(micro, Block::Micro(_)));
    settle(
        &mut importer
            .import_created_block(micro.encode().unwrap(), WALL)
            .unwrap(),
    )
    .unwrap();
    thread::sleep(Duration::from_millis(35));
    assert_eq!(client.production_status().unwrap().published, 2);
    assert_eq!(client.info().unwrap().active, vec![older.candidate.clone()]);
    let stale = accepted(&account, &input(&first_work.candidate, &first()));
    assert_eq!(
        done(&client, stale).state,
        journal::State::Done(journal::Outcome::Inactive)
    );
    let fresh = latest(&client, 3);
    assert_eq!(fresh.candidate.parent(), micro.hash().unwrap());
    let active = client.info().unwrap().active;
    assert_eq!(active.len(), native_active("fresh_reply"));
    assert_eq!(active, vec![fresh.candidate, older.candidate]);
    assert_eq!(snapshots.live_snapshots(), 0);
    assert!(owner.shutdown().failure.is_none());
    drop(services);
    stop(importer);
}
