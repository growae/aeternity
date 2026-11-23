#[path = "conductor/connections.rs"]
mod connections;
#[path = "conductor/connector_supervision.rs"]
mod connector_supervision;
#[path = "conductor/controls.rs"]
mod controls;
#[path = "conductor/eoe_supervision.rs"]
mod eoe_supervision;
#[path = "conductor/parent_http.rs"]
mod parent_http;
#[path = "conductor/pinning.rs"]
mod pinning;
#[path = "conductor/pins.rs"]
mod pins;
#[path = "conductor/targets.rs"]
mod targets;
#[path = "conductor/vote_supervision.rs"]
mod vote_supervision;
use super::*;
use ae_hyperchain::{
    parent_cache::{self, runtime as cache},
    parent_connector::runtime as connector,
};
use ae_storage::pool;
use ae_sync::chain::hyperchain::{
    conductor::{self, Conductor, Output, Progress},
    eoe::owner::production,
};
use ae_vm_fate::maps::order::Otp26;
use common::hyperchain::parents::{ParentBackend, parent, wait};
use num_bigint::BigInt;
use std::sync::{Mutex, atomic::AtomicBool};

#[derive(Clone)]
struct Targets;
impl cache::Targets for Targets {
    fn targets(&mut self, _: &Unsigned) -> Result<Vec<BigInt>, cache::TargetError> {
        Ok(vec![1.into(), 6.into()])
    }
}
fn config(node: &Node, producing: bool) -> conductor::Config {
    let mut eoe = cfg(node);
    eoe.eoe.block_time = node.config.rules.consensus.child_block_time.clone();
    conductor::Config {
        eoe,
        production: production::Config {
            producer: managed::producer::Config {
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
        resilience: None,
        gossip: Default::default(),
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
        producing,
        block_producer: true,
    }
}
fn new(node: &Node, c: conductor::Config) -> Result<Conductor<Otp26>, conductor::Error> {
    Conductor::start_with_targets(
        node.db.clone(),
        node.config.clone(),
        [12, 13, 14]
            .into_iter()
            .map(|n| {
                let k = signer(n);
                (k.public_key(), k)
            })
            .collect(),
        c,
        Arc::new(ParentBackend {
            missing: AtomicBool::new(false),
        }),
        Targets,
    )
}
fn start(node: &Node, producing: bool) -> Conductor<Otp26> {
    let c = new(node, config(node, producing)).unwrap();
    c.parent().post_block(parent(8, 1008)).unwrap();
    c.parent().barrier(Duration::from_secs(5)).unwrap();
    wait(|| {
        [1, 6].into_iter().all(|h| {
            matches!(
                c.parent().snapshot().cache.blocks().get(&h.into()),
                Some(parent_cache::Entry::Block(_))
            )
        })
    });
    c
}
fn tick(c: &mut Conductor<Otp26>, wall: &mut u64, clock: &mut Instant) -> Progress {
    let p = c.step(*wall, *clock).unwrap();
    if let Some(d) = c.status().deadline {
        *wall += d.saturating_duration_since(*clock).as_millis() as u64;
        *clock = d;
    }
    std::thread::sleep(Duration::from_millis(1));
    p
}
fn shutdown(c: Conductor<Otp26>) -> conductor::Shutdown<Otp26> {
    let importer = c.importer();
    let pool = c.pool();
    let parent = c.parent();
    let votes = c.votes();
    let s = c.shutdown();
    assert!(s.pool.as_ref().is_ok_and(|i| !i.running && !i.accepting));
    assert!(s.parent.as_ref().is_none_or(Result::is_ok) && s.connector_joined && s.pins_joined);
    assert!(s.accepted.is_ok());
    assert!(!pool.info().accepting && parent.is_closed() && !votes.info().accepting);
    assert!(matches!(
        importer.import_block(vec![], 0),
        Err(ae_sync::chain::Error::Closed)
    ));
    s
}
#[test]
fn continuous_owner_produces_successive_native_keys_and_joins_all_children() {
    for index in [1, 3, 5] {
        let node = Node::new(index);
        let mut c = start(&node, true);
        let mut wall = 1000;
        let mut clock = Instant::now();
        let end = clock + Duration::from_secs(25);
        let mut keys = vec![];
        let mut eoe = 0;
        while keys.len() < 4 {
            assert!(
                Instant::now() < end,
                "stalled {:?}, wall {wall}",
                c.status()
            );
            tick(&mut c, &mut wall, &mut clock);
            if let Some(output) = c.take_output() {
                match output {
                    Output::Attempt(done) => {
                        if let production::Outcome::Produced { key, is_eoe, .. } = done.outcome {
                            keys.push(key.hash);
                            eoe += usize::from(is_eoe);
                        }
                    }
                    other => panic!("unexpected output {other:?}"),
                }
            }
        }
        assert!(eoe > 0);
        assert_eq!(node.db.view().unwrap().status().tip, keys.last().copied());
        for (i, hash) in keys.iter().enumerate() {
            let Block::Key(k) = node.db.view().unwrap().require_block(*hash).unwrap() else {
                panic!()
            };
            assert_eq!(k.common.height, (i + 1) as u64);
            ae_hyperchain::signing::verify_key(&k).unwrap();
        }
        let s = shutdown(c);
        assert!(s.owner.errors.is_empty());
    }
}
#[tokio::test]
async fn received_top_does_not_preempt_captured_production_or_restart_a_side_key() {
    let node = Node::new(1);
    let mut c = start(&node, true);
    let mut wall = 1000;
    let mut clock = Instant::now();
    let original = node.db.view().unwrap().status().tip.unwrap();
    let end = clock + Duration::from_secs(10);
    while c.attempt().is_none_or(|a| a.decision().is_none()) {
        assert!(Instant::now() < end);
        c.step(wall, clock).unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    for b in node.blocks().iter().take(4) {
        c.importer()
            .import_block(b.encode().unwrap(), 1_000_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    let received = node.db.view().unwrap().status().tip.unwrap();
    assert_ne!(received, original);
    let result = loop {
        assert!(Instant::now() < end);
        tick(&mut c, &mut wall, &mut clock);
        if let Some(Output::Attempt(done)) = c.take_output() {
            break done;
        }
    };
    assert_eq!(result.parent, original);
    assert!(matches!(
        result.outcome,
        production::Outcome::Produced { .. }
    ));
    for _ in 0..40 {
        tick(&mut c, &mut wall, &mut clock);
        assert!(c.output().is_none(), "{:?} {:?}", c.output(), c.status());
    }
    assert_eq!(node.db.view().unwrap().status().tip, Some(received));
    assert_eq!(c.status().attempts, 1);
    assert_eq!(c.status().selected_created_keys, 0);
    assert_eq!(c.parent().snapshot().cache.child_top().hash, received);
    shutdown(c);
}
#[tokio::test]
async fn created_micro_does_not_start_stopped_production_and_start_is_idempotent() {
    let node = Node::new(1);
    let mut c = start(&node, false);
    let mut wall = 12000;
    let mut clock = Instant::now();
    let initial = c.parent().snapshot().cache.child_top().hash;
    c.importer()
        .import_created_block(node.blocks()[0].encode().unwrap(), 1_000_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    for _ in 0..30 {
        tick(&mut c, &mut wall, &mut clock);
        assert!(c.take_output().is_none());
    }
    assert_eq!(c.status().attempts, 0);
    assert_eq!(c.status().top_changes, 1);
    assert_eq!(c.status().selected_created_keys, 0);
    assert_eq!(c.parent().snapshot().cache.child_top().hash, initial);
    c.start_production().unwrap();
    c.start_production().unwrap();
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < end);
        tick(&mut c, &mut wall, &mut clock);
        if c.output().is_some() {
            break;
        }
    }
    assert_eq!(c.status().attempts, 1);
    shutdown(c);
}
#[test]
fn stopped_admission_transfers_the_terminal_result_and_never_resubmits() {
    let node = Node::new(1);
    let mut c = start(&node, true);
    let mut wall = 1000;
    let mut clock = Instant::now();
    let importer = c.importer();
    let end = clock + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < end);
        if tick(&mut c, &mut wall, &mut clock)
            == Progress::Production(production::Progress::Submitted(production::Kind::Key))
        {
            break;
        }
    }
    let s = shutdown(c);
    assert!(s.admission.is_some());
    let attempt = s.attempt.as_ref().unwrap();
    assert_eq!(attempt.failure(), Some(&production::Error::Cancelled));
    assert!(matches!(
        importer.import_created_block(vec![], 0),
        Err(ae_sync::chain::Error::Closed)
    ));
    if let Some(Ok(report)) = s.admission {
        assert_eq!(report.accepted.len(), 1);
        assert!(
            node.db
                .view()
                .unwrap()
                .block(report.accepted[0].hash)
                .unwrap()
                .is_some()
        );
    }
}
#[derive(Clone)]
struct ManualClock(Arc<Mutex<(u64, Instant)>>);
impl conductor::runtime::Clock for ManualClock {
    fn now(&self) -> Result<(u64, Instant), conductor::Error> {
        Ok(*self.0.lock().unwrap())
    }
}
#[test]
fn dedicated_runtime_retains_full_output_queue_and_joins_during_backpressure() {
    let node = Node::new(1);
    let c = start(&node, true);
    let parent = c.parent();
    let pool = c.pool();
    let importer = c.importer();
    let clock = ManualClock(Arc::new(Mutex::new((1000, Instant::now()))));
    let runtime = match conductor::runtime::Runtime::spawn(
        c,
        clock.clone(),
        conductor::runtime::Settings {
            poll: Duration::from_millis(1),
            outputs: 1,
        },
    ) {
        Ok(r) => r,
        Err(e) => panic!("{}", e.error),
    };
    let handle = runtime.handle();
    let end = Instant::now() + Duration::from_secs(15);
    while handle.status().completed < 2 {
        assert!(Instant::now() < end, "stalled {:?}", handle.status());
        assert!(handle.status().failure.is_none(), "{:?}", handle.status());
        if let Some(d) = handle.status().deadline {
            let mut t = clock.0.lock().unwrap();
            t.0 += d.saturating_duration_since(t.1).as_millis() as u64;
            t.1 = d;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(handle.status().completed, 2);
    let s = runtime.shutdown().unwrap();
    assert!(handle.joined());
    assert_eq!(s.queued_outputs.len(), 1);
    assert!(s.output.is_some());
    assert!(s.pool.is_ok() && s.parent.as_ref().is_none_or(Result::is_ok) && s.connector_joined);
    assert!(parent.is_closed() && !pool.info().accepting);
    assert_eq!(handle.start_production(), Err(conductor::Error::Closed));
    assert!(matches!(
        importer.import_block(vec![], 0),
        Err(ae_sync::chain::Error::Closed)
    ));
}
#[test]
fn failed_start_releases_owned_services_and_external_importer_stop_is_terminal() {
    let node = Node::new(1);
    let mut invalid = config(&node, true);
    invalid.eoe.calls.requests = 0;
    assert!(matches!(
        new(&node, invalid),
        Err(conductor::Error::Owner(managed::Error::Configuration))
    ));
    let mut disabled = config(&node, false);
    disabled.block_producer = false;
    let mut observer = new(&node, disabled).unwrap();
    assert_eq!(
        observer.start_production(),
        Err(conductor::Error::NotBlockProducer)
    );
    shutdown(observer);
    let c = start(&node, false);
    let failed = match conductor::runtime::Runtime::spawn(
        c,
        conductor::runtime::SystemClock,
        conductor::runtime::Settings {
            poll: Duration::ZERO,
            outputs: 1,
        },
    ) {
        Err(e) => e,
        Ok(_) => panic!("invalid runtime settings accepted"),
    };
    assert!(failed.conductor.pool().info().accepting);
    shutdown(failed.conductor);
    let mut c = start(&node, true);
    c.importer().stop();
    let error = c.step(1000, Instant::now()).unwrap_err();
    assert_eq!(
        error,
        conductor::Error::Importer(ae_sync::chain::Error::Closed)
    );
    assert_eq!(c.start_production(), Err(error));
    let s = shutdown(c);
    assert!(s.attempt.is_none());
}
#[tokio::test]
async fn committed_event_gap_stops_without_inventing_a_restart_or_replaying_events() {
    let node = Node::with_events(ae_storage::events::Config {
        records: 1,
        bytes: 16 << 20,
    });
    let mut c = start(&node, true);
    for b in node.blocks().iter().take(2) {
        c.importer()
            .import_block(b.encode().unwrap(), 1_000_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    let e = c.step(1000, Instant::now()).unwrap_err();
    assert!(matches!(
        e,
        conductor::Error::Events(ae_storage::events::Error::Gap { .. })
    ));
    assert_eq!(c.status().attempts, 0);
    assert_eq!(c.step(1000, Instant::now()), Err(e));
    shutdown(c);
}

#[tokio::test]
async fn continuous_admission_backpressure_retries_the_same_signed_block() {
    let node = Node::new(1);
    let mut config = config(&node, true);
    config.admission.jobs = 1;
    let mut c = new(&node, config).unwrap();
    c.parent().post_block(parent(8, 1008)).unwrap();
    wait(|| {
        matches!(
            c.parent().snapshot().cache.blocks().get(&1.into()),
            Some(parent_cache::Entry::Block(_))
        )
    });
    let before = node.db.view().unwrap().status();
    let genesis = node
        .db
        .view()
        .unwrap()
        .require_block(before.tip.unwrap())
        .unwrap();
    let held = c
        .importer()
        .import_created_block(genesis.encode().unwrap(), 1_000_000)
        .unwrap();
    let mut wall = 1000;
    let mut clock = Instant::now();
    let end = clock + Duration::from_secs(10);
    while c.status().retries < 3 {
        assert!(Instant::now() < end);
        tick(&mut c, &mut wall, &mut clock);
    }
    let signed = c.attempt().unwrap().pending_block().unwrap().clone();
    assert_eq!(c.status().attempts, 1);
    assert_eq!(node.db.view().unwrap().status(), before);
    held.wait().await.unwrap();
    let done = loop {
        assert!(Instant::now() < end);
        tick(&mut c, &mut wall, &mut clock);
        if let Some(Output::Attempt(done)) = c.take_output() {
            break done;
        }
    };
    let production::Outcome::Produced { key, .. } = done.outcome else {
        panic!()
    };
    assert_eq!(key.hash, signed.hash().unwrap());
    assert_eq!(
        node.db.view().unwrap().status().revision,
        before.revision + 1
    );
    assert_eq!(c.status().attempts, 1);
    shutdown(c);
}
#[test]
fn known_native_rejection_starts_fresh_without_advancing_the_unadmitted_parent() {
    let node = Node::new(6);
    let storage = inserted(&node);
    for b in node.blocks().iter().take(4) {
        storage.insert(b, 1_000_000, Origin::Gossip).unwrap();
    }
    drop(storage);
    let before = node.db.view().unwrap().status();
    let mut c = start(&node, true);
    let mut wall = 25003;
    let mut clock = Instant::now();
    let end = clock + Duration::from_secs(10);
    let output = loop {
        assert!(Instant::now() < end);
        tick(&mut c, &mut wall, &mut clock);
        if let Some(o) = c.take_output() {
            break o;
        }
    };
    let Output::Rejected {
        parent,
        micro,
        error,
    } = output
    else {
        panic!("{output:?}")
    };
    assert_eq!(Some(parent), before.tip);
    assert!(micro.is_none());
    assert!(matches!(error, production::Error::Admission(_)));
    assert_eq!(node.db.view().unwrap().status(), before);
    assert_eq!(c.status().rejected, 1);
    while c.status().attempts < 2 {
        assert!(Instant::now() < end);
        tick(&mut c, &mut wall, &mut clock);
    }
    assert_eq!(c.attempt().unwrap().parent(), parent);
    assert!(c.status().failure.is_none());
    shutdown(c);
}
#[path = "conductor/cache_supervision.rs"]
mod cache_supervision;
