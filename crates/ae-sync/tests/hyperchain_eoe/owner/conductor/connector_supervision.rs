use super::*;
use ae_hyperchain::parent_connector::{
    Consensus,
    fetch::{Backend, BackendError, Context, Query, Remote},
};
use conductor::supervision::Notice;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};

fn drive(
    c: &mut Conductor<Otp26>,
    now: Instant,
    notices: &mut Vec<Notice>,
) -> Result<(), conductor::Error> {
    c.step(1_000_000, now)?;
    if let Some(Output::ConsensusSupervision(n)) = c.take_output() {
        notices.push(n);
    }
    Ok(())
}
fn replaced(c: &mut Conductor<Otp26>, now: Instant, generation: u64, notices: &mut Vec<Notice>) {
    let end = Instant::now() + Duration::from_secs(5);
    while c.status().consensus.connector_generation < generation
        || c.status().consensus.pending_notices > 0
    {
        drive(c, now, notices).unwrap();
        assert!(Instant::now() < end, "{:?}", c.status());
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn connector_replacement_retains_cache_importer_and_routes_new_reads() {
    for index in [0, 2, 4] {
        let node = Node::new(index);
        let mut c = start(&node, false);
        let cache = c.parent();
        let before = cache.snapshot();
        let importer = c.importer();
        let votes = c.votes();
        let pool = c.pool();
        let old = c.connector();
        old.check_top().unwrap();
        old.barrier(Duration::from_secs(5)).unwrap();
        assert_eq!(old.snapshot().seed, 1u128.to_be_bytes());
        old.request_stop();
        let mut notices = vec![];
        replaced(&mut c, Instant::now(), 2, &mut notices);
        assert!(
            matches!(notices.as_slice(), [Notice::ConnectorRetired { generation: 1, child }, Notice::ConnectorRestarted { generation: 2 }] if !child.panicked && child.snapshot.seed == 1u128.to_be_bytes())
        );
        assert_eq!(old.request(Query::Top), Err(connector::Error::Closed));
        old.request_stop();
        let new = c.connector();
        assert_eq!(new.snapshot().health, connector::Health::Running);
        assert_eq!(new.snapshot().seed, [0; 16]);
        assert!(new.snapshot().top.is_none());
        assert!(Arc::ptr_eq(&importer, &c.importer()));
        assert!(votes.info().accepting && pool.info().accepting && !cache.is_closed());
        assert_eq!(cache.snapshot().cache.blocks(), before.cache.blocks());
        // A cache miss invokes the newly registered connector, not the closed
        // client cloned before replacement. The unchanged cache accepts it.
        assert_eq!(
            cache.get_block(2.into(), 1000.into()).unwrap().unwrap(),
            parent(2, 1002)
        );
        assert!(
            matches!(new.fetch(Query::Height(3.into())).unwrap(), Consensus::Block { block, .. } if block == parent(3, 1003))
        );
        let native: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/erlang_connector_supervision.json"
        ))
        .unwrap();
        let normal = native["lifecycle"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["kind"] == "normal")
            .unwrap();
        assert_eq!(normal["siblings_unchanged"], true);
        assert_eq!(
            normal["trace"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|e| e["event"] == "started" && e["child"] == "connector")
                .count() as u64,
            c.status().consensus.connector_generation
        );
        assert_eq!(c.status().consensus.attempts, 1);
        let s = shutdown(c);
        assert_eq!(s.consensus.status.connector_generation, 2);
    }
}

struct HeldBackend {
    entered: AtomicUsize,
    release: AtomicBool,
    calls: Mutex<Vec<Query>>,
    fail: AtomicBool,
}
impl Backend for HeldBackend {
    fn fetch(
        &self,
        _: usize,
        query: &Query,
        _: &[u8; 16],
        context: &Context,
    ) -> Result<Remote, BackendError> {
        self.calls.lock().unwrap().push(query.clone());
        if *query == Query::Height(99.into()) {
            self.entered.fetch_add(1, Ordering::AcqRel);
            // Deliberately hold bounded adapter cleanup after cancellation to
            // observe the gap between stop and actual fetch-thread join.
            let end = Instant::now() + Duration::from_secs(5);
            while !self.release.load(Ordering::Acquire) && Instant::now() < end {
                thread::sleep(Duration::from_millis(1));
            }
            if self.fail.load(Ordering::Acquire) {
                return Err(BackendError::ResourceLimit);
            }
            if context.cancelled() {
                return Ok(Remote::OtherError);
            }
        }
        ParentBackend {
            missing: AtomicBool::new(false),
        }
        .fetch(0, query, &[0; 16], context)
    }
}
fn held(node: &Node, fail: bool) -> (Conductor<Otp26>, Arc<HeldBackend>) {
    let backend = Arc::new(HeldBackend {
        entered: AtomicUsize::new(0),
        release: AtomicBool::new(fail),
        calls: Mutex::new(vec![]),
        fail: AtomicBool::new(fail),
    });
    let c = Conductor::start_with_targets(
        node.db.clone(),
        node.config.clone(),
        [12, 13, 14]
            .into_iter()
            .map(|n| {
                let k = signer(n);
                (k.public_key(), k)
            })
            .collect(),
        config(node, false),
        backend.clone(),
        Targets,
    )
    .unwrap();
    c.parent().barrier(Duration::from_secs(5)).unwrap();
    (c, backend)
}

#[test]
fn replacement_waits_for_fetch_join_and_never_replays_old_accepted_reads() {
    let node = Node::new(0);
    let (mut c, backend) = held(&node, false);
    let old = c.connector();
    let calling = old.clone();
    let call = thread::spawn(move || calling.fetch(Query::Height(99.into())));
    wait(|| backend.entered.load(Ordering::Acquire) == 3);
    old.request(Query::Height(98.into())).unwrap();
    old.request_stop();
    let now = Instant::now();
    let mut notices = vec![];
    for _ in 0..5 {
        drive(&mut c, now, &mut notices).unwrap();
    }
    assert_eq!(c.status().consensus.connector_generation, 1);
    assert_eq!(c.status().consensus.attempts, 0);
    assert!(c.status().consensus.connector_restarting);
    assert!(notices.is_empty());
    backend.release.store(true, Ordering::Release);
    replaced(&mut c, now, 2, &mut notices);
    assert_eq!(call.join().unwrap(), Err(connector::Error::Closed));
    assert_eq!(
        backend
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|q| **q == Query::Height(99.into()))
            .count(),
        3
    );
    assert!(
        !backend
            .calls
            .lock()
            .unwrap()
            .contains(&Query::Height(98.into()))
    );
    c.connector().fetch(Query::Height(98.into())).unwrap();
    assert_eq!(
        backend
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|q| **q == Query::Height(98.into()))
            .count(),
        3
    );
    drop(shutdown(c));
}

#[test]
fn connector_work_stop_is_terminal_even_when_stop_is_requested() {
    let node = Node::new(0);
    let (mut c, _) = held(&node, true);
    let old = c.connector();
    old.request(Query::Height(99.into())).unwrap();
    wait(|| matches!(old.snapshot().health, connector::Health::WorkStopped(_)));
    old.request_stop();
    assert!(matches!(
        c.step(1000, Instant::now()),
        Err(conductor::Error::Parent(_))
    ));
    assert_eq!(c.status().consensus.connector_generation, 1);
    assert_eq!(c.status().consensus.attempts, 0);
    drop(shutdown(c));
}

#[test]
fn connector_retirement_backpressure_retains_receipt_before_next_restart() {
    let node = Node::new(0);
    let mut c = start(&node, false); // no pinning: one supervision notice slot
    let now = Instant::now();
    c.connector().request_stop();
    let end = now + Duration::from_secs(5);
    while c.output().is_none() {
        c.step(1000, now).unwrap();
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(matches!(
        c.output(),
        Some(Output::ConsensusSupervision(Notice::ConnectorRetired {
            generation: 1,
            ..
        }))
    ));
    c.step(1000, now).unwrap(); // first replacement, queues its receipt
    assert_eq!(c.status().consensus.connector_generation, 2);
    c.connector().request_stop();
    for _ in 0..5 {
        c.step(1000, now).unwrap();
    }
    assert_eq!(c.status().consensus.connector_generation, 2);
    assert_eq!(c.status().consensus.attempts, 1);
    assert_eq!(c.status().consensus.pending_notices, 1);
    let s = shutdown(c);
    assert!(matches!(
        s.output,
        Some(Output::ConsensusSupervision(Notice::ConnectorRetired {
            generation: 1,
            ..
        }))
    ));
    assert!(matches!(
        s.consensus.notices.as_slice(),
        [Notice::ConnectorRestarted { generation: 2 }]
    ));
}

#[test]
fn dedicated_runtime_replaces_connector_and_preserves_cache_routing() {
    let node = Node::new(0);
    let c = start(&node, false);
    let cache = c.parent();
    let old = c.connector();
    let runtime =
        conductor::runtime::Runtime::spawn(c, conductor::runtime::SystemClock, Default::default())
            .unwrap_or_else(|_| panic!("spawn conductor"));
    let handle = runtime.handle();
    old.request_stop();
    wait(|| {
        handle.status().consensus.connector_generation == 2 || handle.status().failure.is_some()
    });
    assert!(handle.status().failure.is_none(), "{:?}", handle.status());
    assert_eq!(
        cache.get_block(2.into(), 1000.into()).unwrap().unwrap(),
        parent(2, 1002)
    );
    assert_eq!(old.fetch(Query::Top), Err(connector::Error::Closed));
    let s = runtime.shutdown().unwrap();
    assert!(s.connector_joined && s.parent.is_ok());
    assert_eq!(s.consensus.status.attempts, 1);
    let mut notices = s.consensus.notices;
    for output in s.queued_outputs.into_iter().chain(s.output) {
        if let Output::ConsensusSupervision(n) = output {
            notices.push(n);
        }
    }
    assert_eq!(
        notices
            .iter()
            .filter(|n| matches!(n, Notice::ConnectorRetired { generation: 1, .. }))
            .count(),
        1
    );
    assert_eq!(
        notices
            .iter()
            .filter(|n| matches!(n, Notice::ConnectorRestarted { generation: 2 }))
            .count(),
        1
    );
}
