#[path = "../../ae-sync/tests/common/mod.rs"]
mod common;
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{eoe::local_host, parent_cache, parent_connector::runtime as connector};
use ae_node::hyperchain as root;
use ae_p2p::{discovery::Peer, noise, registry::Command};
use ae_storage::pool;
use ae_sync::{
    chain::hyperchain::{
        conductor::{self, Conductor, Output, runtime as producer},
        eoe::{self, owner, owner::production},
    },
    observe,
};
use ae_vm_fate::maps::order::Otp26;
use common::{
    hyperchain::{
        Node,
        parents::{ParentBackend, parent},
    },
    within,
};
use std::{
    net::Ipv4Addr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
fn key(n: u64) -> [u8; 32] {
    let mut b = [0; 32];
    b[24..].copy_from_slice(&n.to_be_bytes());
    b
}
fn signer(n: u64) -> Ed25519SecretKey {
    Ed25519SecretKey::from_seed(&key(n))
}
#[derive(Clone)]
struct Clock {
    now: Arc<Mutex<(u64, Instant)>>,
    fail: Arc<AtomicBool>,
}
impl Clock {
    fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new((1000, Instant::now()))),
            fail: Arc::new(AtomicBool::new(false)),
        }
    }
}
impl producer::Clock for Clock {
    fn now(&self) -> Result<(u64, Instant), conductor::Error> {
        if self.fail.load(Ordering::Acquire) {
            Err(conductor::Error::Time)
        } else {
            Ok(*self.now.lock().unwrap())
        }
    }
}
fn configuration(node: &Node, cache: pool::worker::GossipCache) -> conductor::Config {
    conductor::Config {
        eoe: owner::Config {
            eoe: eoe::Config {
                host: local_host::Config {
                    network: node.config.profile.network.clone(),
                    election_contract: node.config.profile.contracts.election,
                    parent_epoch_length: node.config.rules.consensus.parent_epoch_length.clone(),
                    parent_start_height: node.config.rules.consensus.parent_start.clone(),
                    limits: Default::default(),
                },
                block_time: node.config.rules.consensus.child_block_time.clone(),
                coordinator: Default::default(),
                runtime: Default::default(),
                epochs: Default::default(),
            },
            votes: Default::default(),
            pool: Default::default(),
            calls: Default::default(),
        },
        production: production::Config {
            producer: ae_chain::repository::hyperchain::producer::Config {
                production_time: 1.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        pool: pool::Config {
            consensus: ae_protocol::ConsensusType::Pos,
            policy: ae_mempool::pool::Config {
                minimum_miner_gas_price: 1.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        pool_worker: pool::worker::Settings {
            origins_interval: None,
            ..Default::default()
        },
        admission: Default::default(),
        snapshots: Default::default(),
        resilience: Some(Default::default()),
        gossip: cache,
        parent: conductor::ParentConfig {
            pins: None,
            cache: parent_cache::Config {
                child_start_height: 0.into(),
                max_size: 200.into(),
                retry_interval: 1000.into(),
                finality: 2.into(),
            },
            cache_runtime: Default::default(),
            connector: connector::Config {
                fetch_interval: None,
                ..Default::default()
            },
            hosts: 3,
            seed: [0; 16],
        },
        events_per_step: 4,
        producing: false,
        block_producer: true,
    }
}
#[derive(Clone)]
struct Targets;
impl parent_cache::runtime::Targets for Targets {
    fn targets(
        &mut self,
        _: &ae_types::Unsigned,
    ) -> Result<Vec<num_bigint::BigInt>, parent_cache::runtime::TargetError> {
        Ok(vec![1.into(), 6.into()])
    }
}
fn inputs(
    node: &Node,
    identity: root::Identity,
    change: impl FnOnce(&mut conductor::Config),
) -> root::Inputs<Otp26> {
    let (cache, controller) = pool::worker::GossipCache::managed(200, 10000).unwrap();
    let mut cfg = configuration(node, cache);
    change(&mut cfg);
    let conductor = Conductor::start_with_targets(
        node.db.clone(),
        node.config.clone(),
        [12, 13, 14]
            .into_iter()
            .map(|n| {
                let k = signer(n);
                (k.public_key(), k)
            })
            .collect(),
        cfg,
        Arc::new(ParentBackend {
            missing: AtomicBool::new(false),
        }),
        Targets,
    )
    .unwrap();
    conductor.parent().post_block(parent(8, 1008)).unwrap();
    conductor.parent().barrier(Duration::from_secs(5)).unwrap();
    root::Inputs {
        conductor,
        cache: controller,
        identity,
    }
}
fn fixed(n: u8) -> root::Identity {
    root::Identity::Fixed(Arc::new(noise::Identity::from_secret([n; 32])))
}
fn config() -> root::Config {
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let std::net::SocketAddr::V4(addr) = listener.local_addr().unwrap() else {
        panic!()
    };
    let mut c = root::Config::default();
    c.connections.network.listen = Some(addr);
    c.connections.peer.port = addr.port();
    c.connections.network.startup.include_default_peers = false;
    c.connections.network.registry.policy.maximum_outbound = 1;
    c.connections.network.registry.policy.maximum_probes = 0;
    c.connections.health_interval = Duration::from_millis(5);
    c.connections.propagation.poll = Duration::from_millis(5);
    c.connections.sync.refresh_interval = Duration::from_millis(5);
    c.workers.gc = None;
    c.workers.poll = Duration::from_millis(5);
    c.producer.poll = Duration::from_millis(1);
    c.poll = Duration::from_millis(1);
    c
}
fn launch(input: root::Inputs<Otp26>, c: root::Config, clock: Clock) -> root::Running<Otp26> {
    match root::launch(input, c, clock, || Ok(Default::default())) {
        Ok(r) => r,
        Err(e) => panic!("{:?}", e.error),
    }
}
async fn ready(h: &mut root::Handle<Otp26>) {
    within(async {
        while !h.status().ready {
            assert!(
                h.status().running && h.status().failure.is_none(),
                "{:?}",
                h.status()
            );
            h.changed().await.unwrap();
        }
    })
    .await;
}
fn tick(h: &root::Handle<Otp26>, clock: &Clock) {
    if let Some(d) = h.status().producer.as_ref().and_then(|s| s.deadline) {
        let mut t = clock.now.lock().unwrap();
        t.0 += d.saturating_duration_since(t.1).as_millis() as u64;
        t.1 = d;
    }
}
async fn produce(h: &root::Handle<Otp26>, clock: &Clock, count: usize) {
    h.start_production().unwrap();
    let end = Instant::now() + Duration::from_secs(25);
    let mut keys = 0;
    while keys < count {
        assert!(
            Instant::now() < end && h.status().failure.is_none(),
            "{:?}",
            h.status()
        );
        tick(h, clock);
        while let Some(output) = h.try_output() {
            match output {
                Output::Attempt(a) => {
                    keys += usize::from(matches!(a.outcome, production::Outcome::Produced { .. }))
                }
                Output::Stopped(_) | Output::SessionReplaced { .. } => (),
                o => panic!("{o:?}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    h.stop_production().unwrap();
}
fn clean(exit: &root::Exit<Otp26>) {
    assert!(
        exit.status.joined && !exit.status.running && !exit.status.ready,
        "{:?}",
        exit.status
    );
    if let Some(w) = &exit.status.workers {
        assert!(w.joined);
    }
    if let Some(c) = &exit.status.connections {
        assert!(c.joined);
    }
    let p = exit.producer.as_ref().unwrap().as_ref().unwrap();
    assert!(
        p.pool.is_ok()
            && p.parent.as_ref().is_none_or(Result::is_ok)
            && p.connector_joined
            && p.pins_joined
    );
}
async fn link(a: &root::Handle<Otp26>, b: &root::Handle<Otp26>, id: ae_types::PeerPublicKey) {
    let one = a.connections().unwrap().current().unwrap();
    let two = b.connections().unwrap().current().unwrap();
    let addr = two.network.status().listening.unwrap();
    one.network
        .command(Command::Add(Peer {
            id,
            host: b"127.0.0.1".to_vec(),
            port: addr.port(),
            address: Ipv4Addr::LOCALHOST,
            source: Ipv4Addr::LOCALHOST,
            trusted: true,
        }))
        .unwrap()
        .wait()
        .await
        .unwrap();
    one.network
        .command(Command::Connect(Some(id)))
        .unwrap()
        .wait()
        .await
        .unwrap();
    within(async {
        while two.peer.status().counts.incoming_pings == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}
#[tokio::test]
async fn root_owns_real_production_encrypted_peers_and_joined_shutdown() {
    let a = Node::new(1);
    let b = Node::new(1);
    let ca = Clock::new();
    let cb = Clock::new();
    let input = inputs(&a, fixed(111), |_| {});
    let old_importer = input.conductor.importer();
    let old_pool = input.conductor.pool();
    let ra = launch(input, config(), ca.clone());
    let rb = launch(inputs(&b, fixed(112), |_| {}), config(), cb.clone());
    let mut ha = ra.handle();
    let mut hb = rb.handle();
    ready(&mut ha).await;
    ready(&mut hb).await;
    produce(&ha, &ca, 2).await;
    link(
        &hb,
        &ha,
        noise::Identity::from_secret([111; 32]).public_key(),
    )
    .await;
    produce(&ha, &ca, 2).await;
    within(async {
        while a.db.view().unwrap().status().tip != b.db.view().unwrap().status().tip {
            assert!(ha.status().failure.is_none() && hb.status().failure.is_none());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let hash = a.db.view().unwrap().status().tip.unwrap();
    assert_eq!(
        a.db.view().unwrap().require_record(hash).unwrap().roots,
        b.db.view().unwrap().require_record(hash).unwrap().roots
    );
    let ga = ha.connections().unwrap().current().unwrap();
    let gb = hb.connections().unwrap().current().unwrap();
    let sa = within(ra.shutdown()).await.unwrap();
    let sb = within(rb.shutdown()).await.unwrap();
    clean(&sa);
    clean(&sb);
    assert!(sa.status.failure.is_none() && sb.status.failure.is_none());
    assert!(
        ga.network.status().joined
            && ga.peer.status().joined
            && gb.network.status().joined
            && gb.peer.status().joined
    );
    assert!(matches!(
        old_importer.import_block(vec![], 0),
        Err(ae_sync::chain::Error::Closed)
    ));
    assert!(!old_pool.info().accepting);
    assert!(matches!(ha.start_production(), Err(root::Error::Closed)));
}
#[tokio::test]
async fn permanent_group_exit_matches_original_zero_restart_root_policy() {
    let native: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/erlang_root_supervision.json")).unwrap();
    assert_eq!(native["root"]["intensity"], 0);
    assert_eq!(native["root"]["period"], 10);
    for case in native["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["kind"] == "normal" || c["kind"] == "boom")
    {
        assert_eq!(case["root_reason"], "shutdown");
        assert_eq!(
            case["trace"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|r| r["event"] == "started")
                .count(),
            3
        );
    }
    for child in [
        root::Child::Workers,
        root::Child::Connections,
        root::Child::Producer,
    ] {
        let node = Node::new(1);
        let clock = Clock::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let running = match root::launch(
            inputs(&node, fixed(113), |_| {}),
            config(),
            clock.clone(),
            move || {
                seen.fetch_add(1, Ordering::AcqRel);
                Ok(Default::default())
            },
        ) {
            Ok(r) => r,
            Err(e) => panic!("{:?}", e.error),
        };
        let mut h = running.handle();
        ready(&mut h).await;
        match child {
            root::Child::Workers => h.workers().unwrap().shutdown(),
            root::Child::Connections => h.connections().unwrap().shutdown(),
            root::Child::Producer => clock.fail.store(true, Ordering::Release),
        }
        let exit = within(running.wait()).await.unwrap();
        clean(&exit);
        assert!(exit.status.failure.is_some());
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert!(h.status().joined);
        assert!(h.take_exit().is_none());
    }
}
#[tokio::test]
async fn startup_failure_unwinds_groups_and_releases_storage_after_closed_clients_drop() {
    for stage in 0..4 {
        let node = Node::new(1);
        let mut c = config();
        match stage {
            0 => c.workers.poll = Duration::ZERO,
            1 => (),
            2 => c.connections.health_interval = Duration::ZERO,
            3 => c.producer.outputs = 0,
            _ => unreachable!(),
        }
        let input = inputs(
            &node,
            if stage == 1 {
                root::Identity::Managed
            } else {
                fixed(114)
            },
            |_| {},
        );
        let old = input.conductor.importer();
        let running = launch(input, c, Clock::new());
        let exit = within(running.wait()).await.unwrap();
        clean(&exit);
        assert!(exit.status.failure.is_some());
        assert!(matches!(
            old.import_block(vec![], 0),
            Err(ae_sync::chain::Error::Closed)
        ));
        assert_eq!(exit.status.connections.is_some(), stage == 3);
        drop(exit);
        drop(old);
        let replacement = launch(inputs(&node, fixed(114), |_| {}), config(), Clock::new());
        let mut h = replacement.handle();
        ready(&mut h).await;
        clean(&within(replacement.shutdown()).await.unwrap());
    }
}
#[tokio::test]
async fn dropping_running_owner_still_joins_and_keeps_a_single_shutdown_receipt() {
    let node = Node::new(1);
    let running = launch(inputs(&node, fixed(115), |_| {}), config(), Clock::new());
    let mut h = running.handle();
    ready(&mut h).await;
    drop(running);
    within(async {
        while h.status().running {
            h.changed().await.unwrap();
        }
    })
    .await;
    let exit = h.take_exit().unwrap();
    clean(&exit);
    assert!(h.take_exit().is_none());
}
#[tokio::test]
async fn startup_deadline_and_early_stop_release_prepared_owners() {
    for stop in [false, true] {
        let node = Node::new(1);
        let mut c = config();
        c.startup_timeout = Duration::from_nanos(1);
        let running = launch(inputs(&node, fixed(116), |_| {}), c, Clock::new());
        if stop {
            running.handle().request_shutdown();
        }
        let exit = within(running.wait()).await.unwrap();
        clean(&exit);
        assert!(!exit.status.ready);
        if stop {
            assert!(exit.status.failure.is_none());
        } else {
            assert!(matches!(
                exit.status.failure,
                Some(root::Error::StartupTimeout(_))
            ));
        }
    }
}
#[test]
fn no_async_runtime_returns_the_original_inputs_without_losing_ownership() {
    let node = Node::new(1);
    let input = inputs(&node, fixed(117), |_| {});
    let e = match root::launch(input, config(), Clock::new(), || Ok(Default::default())) {
        Err(e) => e,
        Ok(_) => panic!(),
    };
    assert_eq!(e.error, root::Error::NoRuntime);
    let report = e.inputs.conductor.shutdown();
    assert!(report.pool.is_ok() && report.parent.as_ref().is_none_or(Result::is_ok));
}
#[tokio::test]
async fn output_pressure_retains_all_completed_production_results_through_shutdown() {
    let node = Node::new(1);
    let clock = Clock::new();
    let mut c = config();
    c.outputs = 1;
    c.producer.outputs = 1;
    let running = launch(inputs(&node, fixed(118), |_| {}), c, clock.clone());
    let mut h = running.handle();
    ready(&mut h).await;
    h.start_production().unwrap();
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(
            Instant::now() < end && h.status().failure.is_none(),
            "{:?}",
            h.status()
        );
        tick(&h, &clock);
        if h.status().producer.as_ref().unwrap().completed >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    for _ in 0..30 {
        tick(&h, &clock);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let exit = within(running.shutdown()).await.unwrap();
    clean(&exit);
    let report = exit.producer.as_ref().unwrap().as_ref().unwrap();
    let outputs = exit
        .retained
        .iter()
        .chain(&report.queued_outputs)
        .chain(report.output.iter());
    let mut hashes = std::collections::BTreeSet::new();
    let mut attempts = 0;
    for output in outputs {
        if let Output::Attempt(a) = output {
            attempts += 1;
            if let production::Outcome::Produced { key, .. } = &a.outcome {
                assert!(hashes.insert(key.hash));
            }
        }
    }
    assert_eq!(attempts, report.status.completed);
    assert!(hashes.len() >= 2);
}
#[tokio::test]
async fn connection_task_panic_is_reported_without_restarting_or_claiming_joined_cleanup() {
    let node = Node::new(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let running = match root::launch(
        inputs(&node, fixed(119), |_| {}),
        config(),
        Clock::new(),
        move || -> Result<observe::Outputs, observe::Error> {
            seen.fetch_add(1, Ordering::AcqRel);
            panic!("injected factory failure")
        },
    ) {
        Ok(r) => r,
        Err(e) => panic!("{:?}", e.error),
    };
    let exit = within(running.wait()).await.unwrap();
    assert!(!exit.status.joined && exit.status.failure.is_some());
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert!(exit.status.workers.unwrap().joined);
    assert!(exit.producer.unwrap().is_ok());
}

#[path = "hyperchain_root/pinning.rs"]
mod pinning;

#[tokio::test]
async fn managed_keys_and_connection_replacement_keep_the_same_producer() {
    use ae_sync::runtime::workers::Child;
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory =
        Directory(std::env::temp_dir().join(format!("ae-node-root-keys-{}", std::process::id())));
    let node = Node::new(1);
    let keys = ae_sync::keys::Config::new(directory.0.clone(), b"synthetic root test".to_vec());
    let mut persisted = None;
    for restart in [true, false] {
        let input = inputs(&node, root::Identity::Managed, |_| {});
        let pool = input.conductor.pool();
        let importer = input.conductor.importer();
        let mut cfg = config();
        cfg.workers.keys = Some(keys.clone());
        let clock = Clock::new();
        let running = launch(input, cfg, clock.clone());
        let mut h = running.handle();
        ready(&mut h).await;
        let workers = h.workers().unwrap();
        let old_keys = workers.bindings().keys.unwrap().current().unwrap();
        let identity = old_keys
            .peer_identity()
            .unwrap()
            .await
            .unwrap()
            .public_key();
        if let Some(expected) = persisted {
            assert_eq!(identity, expected);
        }
        persisted = Some(identity);
        if restart {
            produce(&h, &clock, 2).await;
            let completed = h.status().producer.unwrap().completed;
            assert_eq!(
                workers
                    .restart(Child::Keys, 1)
                    .unwrap()
                    .wait()
                    .await
                    .unwrap(),
                2
            );
            assert!(!old_keys.info().running);
            assert_eq!(
                workers
                    .bindings()
                    .keys
                    .unwrap()
                    .current()
                    .unwrap()
                    .info()
                    .keys
                    .peer,
                identity
            );
            let mut connections = h.connections().unwrap();
            let previous = connections.current().unwrap();
            previous.peer.shutdown();
            within(async {
                while connections.status().generation < 2 || !connections.status().ready {
                    assert!(h.status().failure.is_none());
                    connections.changed().await.unwrap();
                }
            })
            .await;
            ready(&mut h).await;
            assert!(previous.network.status().joined && previous.peer.status().joined);
            assert_eq!(workers.status().admission_generation, 1);
            assert_eq!(workers.status().votes_generation, 1);
            assert!(pool.info().accepting);
            let genesis = node
                .db
                .view()
                .unwrap()
                .require_block(node.config.identity.genesis)
                .unwrap();
            within(
                importer
                    .import_block(genesis.encode().unwrap(), 1_000_000)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
            produce(&h, &clock, 2).await;
            assert!(h.status().producer.unwrap().completed > completed);
            // A second root proves that the managed key is actually used by
            // the encrypted listener, beyond just inspecting the key worker.
            let peer_node = Node::new(1);
            let peer = launch(
                inputs(&peer_node, fixed(120), |_| {}),
                config(),
                Clock::new(),
            );
            let mut ph = peer.handle();
            ready(&mut ph).await;
            link(&ph, &h, identity).await;
            clean(&within(peer.shutdown()).await.unwrap());
        }
        clean(&within(running.shutdown()).await.unwrap());
        assert!(!pool.info().accepting);
    }
}

#[tokio::test]
async fn shutdown_quiesces_production_with_a_blocked_driver_clock() {
    struct Gate {
        entered: Arc<AtomicBool>,
        released: Arc<AtomicBool>,
        clock: Clock,
    }
    impl producer::Clock for Gate {
        fn now(&self) -> Result<(u64, Instant), conductor::Error> {
            self.entered.store(true, Ordering::Release);
            while !self.released.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
            producer::Clock::now(&self.clock)
        }
    }
    struct Release(Arc<AtomicBool>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let entered = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let release = Release(released.clone());
    let node = Node::new(1);
    let running = match root::launch(
        inputs(&node, fixed(122), |_| {}),
        config(),
        Gate {
            entered: entered.clone(),
            released,
            clock: Clock::new(),
        },
        || Ok(Default::default()),
    ) {
        Ok(r) => r,
        Err(e) => panic!("{:?}", e.error),
    };
    let mut h = running.handle();
    ready(&mut h).await;
    within(async {
        while !entered.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    for _ in 0..32 {
        h.stop_production().unwrap();
        h.start_production().unwrap();
    }
    h.request_shutdown();
    within(async {
        while h.status().phase != root::Phase::Stopping {
            h.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(h.start_production(), Err(root::Error::Closed));
    drop(release);
    let exit = within(running.wait()).await.unwrap();
    clean(&exit);
    assert!(exit.status.failure.is_none());
    assert!(!exit.producer.unwrap().unwrap().status.producing);
}

#[tokio::test]
async fn driver_panic_retains_service_shutdown_and_does_not_claim_a_receipt() {
    struct PanicClock(Arc<AtomicBool>, Clock);
    impl producer::Clock for PanicClock {
        fn now(&self) -> Result<(u64, Instant), conductor::Error> {
            assert!(!self.0.load(Ordering::Acquire), "injected clock panic");
            producer::Clock::now(&self.1)
        }
    }
    let node = Node::new(1);
    let panic = Arc::new(AtomicBool::new(false));
    let input = inputs(&node, fixed(123), |_| {});
    let pool = input.conductor.pool();
    let running = match root::launch(
        input,
        config(),
        PanicClock(panic.clone(), Clock::new()),
        || Ok(Default::default()),
    ) {
        Ok(r) => r,
        Err(e) => panic!("{:?}", e.error),
    };
    let mut h = running.handle();
    ready(&mut h).await;
    panic.store(true, Ordering::Release);
    let exit = within(running.wait()).await.unwrap();
    assert!(!exit.status.joined && !exit.status.running && exit.status.failure.is_some());
    let report = exit.producer.unwrap().unwrap();
    let driver = report.production_driver.unwrap();
    assert!(driver.exit.is_err());
    assert_eq!(driver.failure, Some(conductor::Error::DriverPanicked));
    assert!(driver.pending.is_none());
    assert!(report.pool.is_ok() && report.connector_joined && report.owner.errors.is_empty());
    assert!(exit.status.workers.unwrap().joined && exit.status.connections.unwrap().joined);
    assert!(!pool.info().accepting);
}

#[tokio::test]
async fn supervised_driver_replacement_keeps_importer_keys_pool_and_encrypted_peers() {
    use producer::supervision::Notice;
    let node = Node::new(1);
    let peer = Node::new(1);
    let clock = Clock::new();
    let clocks = clock.clone();
    let first = Arc::new(AtomicBool::new(false));
    let fail = first.clone();
    let input = inputs(&node, fixed(124), |_| {});
    let importer = input.conductor.importer();
    let pool = input.conductor.pool();
    let keys = input.conductor.preset_key_binding();
    let old_key = keys.current().unwrap();
    let running = root::launch_supervised(
        input,
        config(),
        move |generation| {
            Ok(Clock {
                now: clocks.now.clone(),
                fail: if generation == 1 {
                    fail.clone()
                } else {
                    Arc::new(AtomicBool::new(false))
                },
            })
        },
        || Ok(Default::default()),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error));
    let mut h = running.handle();
    ready(&mut h).await;
    produce(&h, &clock, 2).await;
    let previous = h.connections().unwrap().current().unwrap();
    first.store(true, Ordering::Release);
    let mut retired = 0;
    within(async {
        loop {
            assert!(h.status().failure.is_none(), "{:?}", h.status());
            if let Some(notice) = h.try_production_notice() {
                match notice {
                    Notice::Retired(report) => {
                        assert!(report.exit.is_ok());
                        assert_eq!(report.failure, Some(conductor::Error::Time));
                        retired += 1;
                    }
                    Notice::Restarted { generation: 2 } => break,
                    n => panic!("{n:?}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    ready(&mut h).await;
    assert_eq!(retired, 1);
    assert!(Arc::ptr_eq(&importer, &h.importer().unwrap()));
    assert!(old_key.same_worker(&keys.current().unwrap()) && pool.info().accepting);
    assert!(previous.peer.status().running && previous.network.status().running);
    assert_eq!(h.status().producer.unwrap().consensus.attempts, 0);
    let other = launch(inputs(&peer, fixed(125), |_| {}), config(), Clock::new());
    let mut p = other.handle();
    ready(&mut p).await;
    link(&p, &h, noise::Identity::from_secret([124; 32]).public_key()).await;
    produce(&h, &clock, 2).await;
    within(async {
        while node.db.view().unwrap().status().tip != peer.db.view().unwrap().status().tip {
            assert!(h.status().failure.is_none() && p.status().failure.is_none());
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    let exit = within(running.shutdown()).await.unwrap();
    clean(&exit);
    assert!(exit.status.failure.is_none());
    let group = exit.production.unwrap();
    assert_eq!(group.status.attempts, 1);
    assert!(group.driver.unwrap().exit.is_ok());
    assert!(!pool.info().accepting);
    clean(&within(other.shutdown()).await.unwrap());
}

#[tokio::test]
async fn supervised_restart_exhaustion_reaches_the_zero_restart_root_with_all_reports() {
    use producer::supervision::{Error, Notice};
    let node = Node::new(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let running = root::launch_supervised(
        inputs(&node, fixed(126), |_| {}),
        config(),
        move |_| {
            seen.fetch_add(1, Ordering::AcqRel);
            let clock = Clock::new();
            clock.fail.store(true, Ordering::Release);
            Ok(clock)
        },
        || Ok(Default::default()),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error));
    let exit = within(running.wait()).await.unwrap();
    clean(&exit);
    assert_eq!(
        exit.status.failure,
        Some(root::Error::Production(Error::RestartLimit))
    );
    assert_eq!(calls.load(Ordering::Acquire), 6);
    assert_eq!(exit.production.as_ref().unwrap().status.attempts, 6);
    let retired: Vec<_> = exit
        .production_notices
        .iter()
        .filter_map(|n| match n {
            Notice::Retired(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(retired.len(), 6);
    for (i, report) in retired.iter().enumerate() {
        assert_eq!(report.generation, i as u64 + 1);
        assert_eq!(report.failure, Some(conductor::Error::Time));
    }
    assert!(exit.producer.unwrap().unwrap().pool.is_ok());
}

#[tokio::test]
async fn unread_supervision_notices_bound_replacement_and_survive_shutdown() {
    use producer::supervision::Notice;
    let node = Node::new(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let mut cfg = config();
    cfg.outputs = 1;
    cfg.producer.outputs = 1;
    let running = root::launch_supervised(
        inputs(&node, fixed(127), |_| {}),
        cfg,
        move |_| {
            seen.fetch_add(1, Ordering::AcqRel);
            let clock = Clock::new();
            clock.fail.store(true, Ordering::Release);
            Ok(clock)
        },
        || Ok(Default::default()),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error));
    let h = running.handle();
    within(async {
        while !h
            .status()
            .production
            .is_some_and(|p| p.generation == 2 && p.pending_notice)
        {
            assert!(h.status().failure.is_none());
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(calls.load(Ordering::Acquire), 2);
    }
    let exit = within(running.shutdown()).await.unwrap();
    clean(&exit);
    assert!(matches!(
        exit.production_notices.as_slice(),
        [Notice::Retired(_)]
    ));
    let report = exit.production.unwrap();
    assert!(matches!(
        report.pending,
        Some(Notice::Restarted { generation: 2 })
    ));
    assert_eq!(report.driver.unwrap().failure, Some(conductor::Error::Time));
    assert!(exit.producer.unwrap().unwrap().pool.is_ok());
}

#[tokio::test]
async fn shutdown_during_replacement_constructor_keeps_workers_live_and_joins_the_result() {
    let node = Node::new(1);
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    struct Release(Arc<AtomicBool>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let guard = Release(release.clone());
    let seen = entered.clone();
    let running = root::launch_supervised(
        inputs(&node, fixed(128), |_| {}),
        config(),
        move |generation| {
            let clock = Clock::new();
            if generation == 1 {
                clock.fail.store(true, Ordering::Release);
            } else {
                seen.store(true, Ordering::Release);
                while !release.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            Ok(clock)
        },
        || Ok(Default::default()),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error));
    let h = running.handle();
    within(async {
        while !entered.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert!(h.workers().unwrap().status().ready);
    assert!(h.connections().unwrap().status().ready);
    h.request_shutdown();
    drop(guard);
    let exit = within(running.wait()).await.unwrap();
    clean(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
    assert!(exit.production.unwrap().driver.unwrap().exit.is_ok());
}
