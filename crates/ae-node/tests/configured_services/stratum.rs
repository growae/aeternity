use super::*;
use ae_node::{configuration::stratum::Plan, stratum::configured as stratum};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn native_replacement(name: &str) -> Value {
    let rows: Value =
        serde_json::from_str(include_str!("../fixtures/erlang_stratum_continuity.json")).unwrap();
    rows.as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == name)
        .unwrap()
        .clone()
}
fn configuration(lab: &Lab) -> stratum::Config {
    let cfg = Configuration::from_value(json!({"stratum":{
        "enabled":true,"connection":{"port":9999,"max_connections":4,"num_acceptors":2},
        "reward":{"keys":{"dir":lab.0.join("stratum-keys")},"beneficiaries":[format!("{}:1",api::encode(ApiType::AccountPubkey,&payer().public_key().0))]}
    }})).unwrap();
    let context = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    let prepared = Plan::resolve(
        &cfg.resolve(&context).unwrap(),
        b"ae_configured_services",
        &lab.0,
    )
    .unwrap()
    .unwrap()
    .load_or_create()
    .unwrap();
    let mut config = stratum::Config::new(prepared).unwrap();
    config.transport.bind = "127.0.0.1:0".parse().unwrap();
    config
}
fn settings(lab: &Lab, s: stratum::Config) -> root::Config {
    let mut config = root_config(lab);
    config.stratum = Some(Arc::new(s));
    config
}
async fn until(mut f: impl FnMut() -> bool) {
    within(async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
}
fn start(lab: &Lab, config: root::Config) -> root::Running {
    root::launch(
        lab.services("pow_cuckoo", json!({"dev_mode":true}), Default::default()),
        config,
        Arc::new(group::SystemClocks::default()),
        || Ok(Default::default()),
    )
    .unwrap()
}
async fn subscribe(s: &stratum::Handle) -> BufReader<tokio::net::TcpStream> {
    let mut stream = tokio::net::TcpStream::connect(s.transport.address())
        .await
        .unwrap();
    stream.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"mining.subscribe\",\"params\":[\"test/1\",null,null,9999]}\n").await.unwrap();
    let mut stream = BufReader::new(stream);
    let mut line = String::new();
    within(stream.read_line(&mut line)).await.unwrap();
    let response: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(response["id"], 1);
    assert!(response["error"].is_null(), "{response}");
    stream
}
fn joined(report: &stratum::Report) {
    assert!(report.settled(), "{report:?}");
    let signer = report.signer.unwrap();
    assert!(signer.joined && !signer.accepting);
    assert!(report.accounting.is_some());
}

#[tokio::test]
async fn stratum_root_composes_reward_signing_work_pending_beneficiary_and_tcp_in_stopped_modes() {
    for mode in ["dev_mode", "offline_mode", "maintenance_mode"] {
        let lab = Lab::new();
        let s = configuration(&lab);
        let reward = s.beneficiary();
        let p = lab.services("pow_cuckoo", json!({mode:true}), Default::default());
        let snapshots = p.snapshots().clone();
        let running = root::launch(
            p,
            settings(&lab, s),
            Arc::new(group::SystemClocks::default()),
            || Ok(Default::default()),
        )
        .unwrap();
        let h = running.handle();
        root_ready(&h, 1).await;
        let stratum = h.stratum().unwrap();
        let mut stream = subscribe(&stratum).await;
        let mining = stratum.accounting.mining().unwrap();
        until(|| mining.production_status().unwrap().published > 0).await;
        let candidate = mining.production_status().unwrap().last.unwrap();
        let producer = h.production().unwrap().current().unwrap().producer;
        assert!(!producer.status().enabled);
        assert!(producer.status().mining.is_none());
        assert_eq!(candidate.candidate.header().beneficiary, reward.0);
        let miner = h
            .workers()
            .unwrap()
            .bindings()
            .keys
            .unwrap()
            .current()
            .unwrap()
            .info()
            .keys
            .candidate;
        assert_eq!(candidate.candidate.header().miner, miner.0);
        assert_ne!(reward, miner);
        let pending = within(h.pending_key().unwrap().get().unwrap())
            .await
            .unwrap();
        assert_eq!(pending.result.as_ref().unwrap().beneficiary, reward.0);
        let signature = stratum
            .signer
            .sign_binary(b"configured reward key", reward)
            .unwrap()
            .wait_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert!(ae_crypto::verify_ed25519(
            &reward,
            b"configured reward key",
            &signature
        ));
        h.set_enabled(true).unwrap();
        until(|| producer.status().enabled).await;
        h.set_enabled(false).unwrap();
        until(|| !producer.status().enabled).await;
        assert!(!stratum.transport.is_closed());
        let address = stratum.transport.address();
        drop(pending);
        let exit = within(running.shutdown()).await.unwrap();
        assert_root_joined(&exit);
        assert!(exit.status.failure.is_none(), "{:?}", exit.status);
        joined(&exit.stratum.as_ref().unwrap().report);
        assert_eq!(snapshots.live_snapshots(), 0);
        let mut line = String::new();
        assert_eq!(within(stream.read_line(&mut line)).await.unwrap(), 0);
        assert!(std::net::TcpListener::bind(address).is_ok());
    }
}

#[tokio::test]
async fn stratum_root_bind_failure_retains_accounting_and_signer_retirement() {
    let lab = Lab::new();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut s = configuration(&lab);
    s.transport.bind = listener.local_addr().unwrap();
    let running = start(&lab, settings(&lab, s));
    let h = running.handle();
    until(|| !h.status().running).await;
    let exit = running.wait().await.unwrap();
    assert_root_joined(&exit);
    assert!(matches!(exit.status.failure, Some(root::Error::Stratum(_))));
    let s = exit.stratum.unwrap();
    assert!(matches!(
        s.start_error,
        Some(ae_node::stratum::Error::Runtime(
            ae_stratum::runtime::Error::Io(std::io::ErrorKind::AddrInUse)
        ))
    ));
    joined(&s.report);
    assert!(s.report.transport.is_none());
}

#[tokio::test]
async fn stratum_root_retires_before_worker_rebinding_and_preserves_output_backpressure() {
    let lab = Lab::new();
    let mut config = settings(&lab, configuration(&lab));
    config.outputs = 1;
    let reward = config.stratum.as_ref().unwrap().beneficiary();
    let running = start(&lab, config);
    let h = running.handle();
    root_ready(&h, 1).await;
    let importer = h.importer().unwrap();
    for (child, starts) in [(workers::Child::Keys, 2), (workers::Child::Admission, 3)] {
        // Keep the single root output slot occupied until retirement is joined.
        let old = h.stratum().unwrap();
        h.workers()
            .unwrap()
            .restart(child, 1)
            .unwrap()
            .wait()
            .await
            .unwrap();
        until(|| old.signer.info().joined).await;
        assert_eq!(h.status().production_starts, starts - 1);
        let mut reports = 0;
        until(|| {
            while let Some(o) = h.try_output() {
                if let root::Output::StratumRetired(s) = o {
                    joined(&s.report);
                    reports += 1;
                }
            }
            assert!(
                h.status().running && h.status().failure.is_none(),
                "{:?}",
                h.status()
            );
            h.status().ready && h.status().production_starts == starts
        })
        .await;
        assert_eq!(reports, 1);
        assert!(old.transport.is_closed() && old.accounting.is_closed());
        assert!(!old.signer.same_worker(&h.stratum().unwrap().signer));
        let s = h.stratum().unwrap();
        let signature = s
            .signer
            .sign_binary(b"same reward key", reward)
            .unwrap()
            .wait_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert!(ae_crypto::verify_ed25519(
            &reward,
            b"same reward key",
            &signature
        ));
        assert!(Arc::ptr_eq(&importer, &h.importer().unwrap()));
    }
    let exit = running.shutdown().await.unwrap();
    assert_root_joined(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
    joined(&exit.stratum.unwrap().report);
}

async fn packet(stream: &mut BufReader<tokio::net::TcpStream>) -> Value {
    let mut line = String::new();
    assert!(within(stream.read_line(&mut line)).await.unwrap() > 0);
    serde_json::from_str(&line).unwrap()
}
async fn authorize(stream: &mut BufReader<tokio::net::TcpStream>) {
    let account = api::encode(ApiType::AccountPubkey, &payer().public_key().0);
    let raw = json!({"jsonrpc":"2.0","id":2,"method":"mining.authorize","params":[format!("{account}.one"),null]}).to_string()+"\n";
    stream.get_mut().write_all(raw.as_bytes()).await.unwrap();
    loop {
        let reply = packet(stream).await;
        if reply["id"] == 2 {
            assert_eq!(reply["result"], true, "{reply}");
            break;
        }
    }
}
fn drain(h: &root::Handle) {
    while let Some(output) = h.try_output() {
        assert!(
            !matches!(output, root::Output::StratumRetired(_)),
            "Stratum restarted on producer replacement"
        );
    }
}

#[tokio::test]
async fn stratum_root_preserves_authorized_session_nonce_accounting_and_signer_across_producer_replacement()
 {
    let lab = Lab::new();
    let mut config = configuration(&lab);
    config.transport.session.skip_blocks = 0;
    let running = start(&lab, settings(&lab, config));
    let h = running.handle();
    root_ready(&h, 1).await;
    let old = h.stratum().unwrap();
    let mining = old.accounting.mining().unwrap();
    let epoch = mining.epoch();
    let mut stream = subscribe(&old).await;
    authorize(&mut stream).await;
    let connection = old.transport.connections().pop().unwrap();
    let before = within(connection.status().unwrap()).await.unwrap().unwrap();
    let nonce = before.session.extra_nonce();
    let account = before.session.account().unwrap().to_owned();
    drop(before);
    until(|| mining.production_status().unwrap().published > 0).await;
    let group = h.production().unwrap();
    for generator in [true, false] {
        let previous = mining.production_status().unwrap().last.unwrap();
        let current = group.current().unwrap();
        if generator {
            current.generator.request_shutdown();
        } else {
            current.producer.request_shutdown();
        }
        until(|| {
            drain(&h);
            assert!(
                h.status().running && h.status().failure.is_none(),
                "{:?}",
                h.status()
            );
            h.status().ready
                && group.current().is_some_and(|g| g.id != current.id)
                && mining
                    .production_status()
                    .unwrap()
                    .last
                    .as_ref()
                    .is_some_and(|p| p.sequence > previous.sequence)
        })
        .await;
        let native = native_replacement(if generator { "generator" } else { "conductor" });
        assert_eq!(
            group.current().unwrap().id != current.id,
            native["producer_children_replaced"].as_bool().unwrap()
        );
        assert_eq!(
            h.status().stratum_starts == 1,
            native["stratum_children_preserved"].as_bool().unwrap()
        );
        assert_eq!(
            h.status().running && h.status().production_starts == 1,
            native["supervisors_preserved"].as_bool().unwrap()
        );
        let next = mining.production_status().unwrap().last.unwrap();
        assert_eq!(mining.info().unwrap().active, vec![next.candidate.clone()]);
        assert_eq!(mining.epoch(), epoch);
        assert_eq!(mining.info().unwrap().epoch, epoch);
        assert_eq!(h.status().stratum_starts, 1);
        assert_eq!(h.status().production_starts, 1);
        assert!(old.signer.same_worker(&h.stratum().unwrap().signer));
        assert!(old.signer.info().accepting && !old.accounting.is_closed());
        assert_eq!(old.transport.connections()[0].id(), connection.id());
        let job = next.candidate.work().unwrap().id().unwrap();
        loop {
            let message = packet(&mut stream).await;
            if message["method"] == "mining.notify" && message["params"][0] == job {
                break;
            }
        }
        let after = within(connection.status().unwrap()).await.unwrap().unwrap();
        assert_eq!(after.session.extra_nonce(), nonce);
        assert_eq!(after.session.account(), Some(account.as_str()));
        assert!(after.session.accepts_blocks());
        assert_eq!(
            after.session.phase(),
            ae_stratum::session::Phase::Authorized
        );
    }
    let exit = running.shutdown().await.unwrap();
    assert_root_joined(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
    joined(&exit.stratum.unwrap().report);
    let mut line = String::new();
    assert_eq!(within(stream.read_line(&mut line)).await.unwrap(), 0);
}

#[tokio::test]
async fn stratum_root_drop_retains_terminal_report_and_conflicting_miner_is_rejected() {
    let lab = Lab::new();
    let mut config = settings(&lab, configuration(&lab));
    config.production.mining = Some(ae_sync::chain::pow::producer::mining::Config::new(
        AccountPublicKey([1; 32]),
    ));
    let failed = root::launch(
        lab.services("pow_cuckoo", json!({"dev_mode":true}), Default::default()),
        config,
        Arc::new(group::SystemClocks::default()),
        || Ok(Default::default()),
    )
    .err()
    .unwrap();
    assert_eq!(failed.error, root::Error::Configuration);
    drop(failed);
    let running = start(&lab, settings(&lab, configuration(&lab)));
    let h = running.handle();
    root_ready(&h, 1).await;
    let old = h.stratum().unwrap();
    drop(running);
    until(|| !h.status().running).await;
    let exit = h.take_exit().unwrap();
    assert_root_joined(&exit);
    joined(&exit.stratum.unwrap().report);
    assert!(old.signer.info().joined && old.transport.is_closed());
    assert!(h.take_exit().is_none());
}

#[tokio::test]
async fn stratum_root_restarts_a_permanent_listener_once_then_retains_restart_limit_failure() {
    let lab = Lab::new();
    let running = start(&lab, settings(&lab, configuration(&lab)));
    let h = running.handle();
    root_ready(&h, 1).await;
    let first = h.stratum().unwrap();
    first.transport.request_shutdown();
    let mut reports = 0;
    until(|| {
        while let Some(o) = h.try_output() {
            if let root::Output::StratumRetired(r) = o {
                joined(&r.report);
                reports += 1;
            }
        }
        assert!(
            h.status().running && h.status().failure.is_none(),
            "{:?}",
            h.status()
        );
        h.status().ready && h.status().stratum_starts == 2
    })
    .await;
    assert_eq!(reports, 1);
    let native = native_replacement("stratum");
    assert_eq!(
        h.status().production_starts == 1,
        native["producer_children_preserved"].as_bool().unwrap()
    );
    assert_eq!(
        h.status().stratum_starts == 2,
        native["stratum_children_replaced"].as_bool().unwrap()
    );
    assert!(first.signer.info().joined);
    h.stratum().unwrap().transport.request_shutdown();
    until(|| !h.status().running).await;
    let exit = running.wait().await.unwrap();
    assert_root_joined(&exit);
    assert_eq!(
        exit.status.failure,
        Some(root::Error::Stratum(stratum::Failure::RestartLimit))
    );
    joined(&exit.stratum.unwrap().report);
}

type Wait = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);
struct Gate(std::sync::Mutex<Option<Wait>>);
impl generator::Clock for Gate {
    fn unix_millis(&self) -> i128 {
        let pair = self.0.lock().unwrap().take();
        if let Some((entered, release)) = pair {
            entered.send(()).unwrap();
            release.recv_timeout(Duration::from_secs(15)).unwrap();
        }
        generator::SystemClock.unix_millis()
    }
}
struct Clocks(Arc<Gate>);
impl group::Clocks for Clocks {
    fn generator(&self, _: u64) -> Arc<dyn generator::Clock> {
        Arc::new(generator::SystemClock)
    }
    fn producer(&self, _: u64) -> Box<dyn ae_sync::chain::pow::producer::Clock> {
        Box::new(ae_sync::chain::pow::producer::SystemClock)
    }
    fn preparation(&self, generation: u64) -> Arc<dyn generator::Clock> {
        if generation == 0 {
            Arc::new(generator::SystemClock)
        } else {
            self.0.clone()
        }
    }
    fn second(&self) -> u64 {
        0
    }
}

#[tokio::test]
async fn stratum_root_shutdown_keeps_reward_signer_until_blocked_preparation_is_joined() {
    let lab = Lab::new();
    let p = lab.services("pow_cuckoo", json!({"dev_mode":true}), Default::default());
    let snapshots = p.snapshots().clone();
    let (entered, wait) = std::sync::mpsc::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let clocks = Clocks(Arc::new(Gate(std::sync::Mutex::new(Some((
        entered, blocked,
    ))))));
    let running = root::launch(
        p,
        settings(&lab, configuration(&lab)),
        Arc::new(clocks),
        || Ok(Default::default()),
    )
    .unwrap();
    let h = running.handle();
    root_ready(&h, 1).await;
    let s = h.stratum().unwrap();
    until(|| wait.try_recv().is_ok()).await;
    let shutdown = tokio::spawn(running.shutdown());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!shutdown.is_finished());
    assert!(s.signer.info().accepting);
    release.send(()).unwrap();
    let exit = within(shutdown).await.unwrap().unwrap();
    assert_root_joined(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
    let report = exit.stratum.unwrap().report;
    joined(&report);
    assert!(
        report
            .accounting
            .unwrap()
            .production
            .unwrap()
            .last_preparation
            .is_some()
    );
    assert_eq!(snapshots.live_snapshots(), 0);
}

#[tokio::test]
async fn stratum_producer_gap_under_output_pressure_keeps_tcp_and_retires_unclaimed_work() {
    let lab = Lab::new();
    let mut config = settings(&lab, configuration(&lab));
    config.outputs = 1;
    config.production.outputs = 1;
    let running = start(&lab, config);
    let h = running.handle();
    root_ready(&h, 1).await;
    let old = h.stratum().unwrap();
    let mining = old.accounting.mining().unwrap();
    until(|| mining.production_status().unwrap().published > 0).await;
    let mut held = None;
    until(|| {
        held = h.try_output();
        held.is_some()
    })
    .await;
    let current = h.production().unwrap().current().unwrap();
    current.producer.request_shutdown();
    until(|| mining.production_status().unwrap().waiting_for_identity).await;
    assert!(mining.info().unwrap().active.is_empty());
    assert!(
        h.production()
            .unwrap()
            .current()
            .is_none_or(|g| g.id == current.id)
    );
    assert_eq!(h.status().stratum_starts, 1);
    let mut stream = subscribe(&old).await;
    authorize(&mut stream).await;
    assert!(old.signer.info().accepting && !old.accounting.is_closed());
    drop(held);
    until(|| {
        drain(&h);
        h.status().ready
            && h.production()
                .unwrap()
                .current()
                .is_some_and(|g| g.id != current.id)
            && mining.production_status().unwrap().published >= 2
    })
    .await;
    assert_eq!(h.status().stratum_starts, 1);
    assert!(!old.transport.connections()[0].is_closed());
    let exit = running.shutdown().await.unwrap();
    assert_root_joined(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
}

#[tokio::test]
async fn stratum_replacement_joins_obsolete_preparation_before_dispatching_new_work_on_the_same_service()
 {
    let lab = Lab::new();
    let p = lab.services("pow_cuckoo", json!({"dev_mode":true}), Default::default());
    let snapshots = p.snapshots().clone();
    let (entered, wait) = std::sync::mpsc::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let clocks = Clocks(Arc::new(Gate(std::sync::Mutex::new(Some((
        entered, blocked,
    ))))));
    let running = root::launch(
        p,
        settings(&lab, configuration(&lab)),
        Arc::new(clocks),
        || Ok(Default::default()),
    )
    .unwrap();
    let h = running.handle();
    root_ready(&h, 1).await;
    until(|| wait.try_recv().is_ok()).await;
    let old = h.stratum().unwrap();
    let mining = old.accounting.mining().unwrap();
    let task = mining.production_status().unwrap().preparing.unwrap();
    let current = h.production().unwrap().current().unwrap();
    current.generator.request_shutdown();
    until(|| {
        drain(&h);
        h.status().ready
            && h.production()
                .unwrap()
                .current()
                .is_some_and(|g| g.id != current.id)
            && mining.production_status().unwrap().producer != Some(current.id)
    })
    .await;
    assert_eq!(mining.production_status().unwrap().preparing, Some(task));
    assert_eq!(mining.production_status().unwrap().published, 0);
    assert_eq!(h.status().stratum_starts, 1);
    let mut stream = subscribe(&old).await;
    authorize(&mut stream).await;
    assert!(old.signer.info().accepting);
    release.send(()).unwrap();
    until(|| mining.production_status().unwrap().published == 1).await;
    assert!(mining.production_status().unwrap().stale >= 1);
    assert_eq!(
        mining.production_status().unwrap().producer,
        Some(h.production().unwrap().current().unwrap().id)
    );
    assert_eq!(mining.info().unwrap().active.len(), 1);
    assert!(!old.accounting.is_closed());
    let exit = running.shutdown().await.unwrap();
    assert_root_joined(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
    assert_eq!(snapshots.live_snapshots(), 0);
}

#[tokio::test]
async fn stratum_supervised_source_rejects_a_foreign_database_before_starting_services() {
    let lab = Lab::new();
    let p = lab.services("pow_cuckoo", json!({"dev_mode":true}), Default::default());
    let snapshots = p.snapshots().clone();
    let database = p.configured().chain().database().clone();
    let running = launch_root(&lab, p);
    let h = running.handle();
    root_ready(&h, 1).await;
    assert!(h.production().unwrap().uses_snapshots(&snapshots));
    let other = Lab::new();
    let foreign = start(&other, root_config(&other));
    let f = foreign.handle();
    root_ready(&f, 1).await;
    assert!(!f.production().unwrap().uses_snapshots(&snapshots));
    let failure = stratum::Live::start(
        &configuration(&lab),
        stratum::Inputs {
            database,
            snapshots,
            admitter: h.importer().unwrap().stratum_admitter(),
            producers: f.production().unwrap(),
            pool: h
                .workers()
                .unwrap()
                .bindings()
                .admission
                .current()
                .unwrap()
                .client,
            clock: Arc::new(generator::SystemClock),
            strictly_follow_top: false,
        },
    )
    .err()
    .unwrap();
    assert!(matches!(
        failure.error,
        ae_node::stratum::Error::Configuration
    ));
    assert!(
        failure.report.signer.is_none()
            && failure.report.accounting.is_none()
            && failure.report.transport.is_none()
    );
    assert_root_joined(&running.shutdown().await.unwrap());
    assert_root_joined(&foreign.shutdown().await.unwrap());
}
