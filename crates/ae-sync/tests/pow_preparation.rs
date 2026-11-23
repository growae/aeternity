mod common;
use ae_chain::{block::Block, repository::pow::key};
use ae_codec::headers::KeyHeader;
use ae_protocol::NetworkProfile;
use ae_storage::snapshots;
use ae_sync::chain::{
    self,
    pow::{generator::Clock, preparation::*},
};
use ae_types::{AccountPublicKey, Hash256};
use ae_vm_fate::maps::order::Otp26;
use common::{Node, ledger_fixtures::*};
use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

struct TestClock(Mutex<Box<dyn FnMut() -> i128 + Send>>);
impl Clock for TestClock {
    fn unix_millis(&self) -> i128 {
        (self.0.lock().unwrap())()
    }
}
fn clock(f: impl FnMut() -> i128 + Send + 'static) -> Arc<dyn Clock> {
    Arc::new(TestClock(Mutex::new(Box::new(f))))
}
fn times(values: &[i128]) -> Arc<dyn Clock> {
    let values = values.to_vec();
    let mut values = values.into_iter();
    clock(move || values.next().expect("unexpected extra clock read"))
}
fn gate(at: usize) -> (Arc<dyn Clock>, mpsc::Receiver<()>, mpsc::SyncSender<()>) {
    let (entered, ready) = mpsc::sync_channel(1);
    let (release, continue_work) = mpsc::sync_channel(1);
    let mut read = 0;
    (
        clock(move || {
            if read == at {
                entered.send(()).unwrap();
                continue_work.recv_timeout(Duration::from_secs(10)).unwrap();
            }
            read += 1;
            1000 + read as i128
        }),
        ready,
        release,
    )
}
fn config(node: &Node) -> Config<Otp26> {
    let identity = node.db.view().unwrap().identity().clone();
    Config {
        rules: Arc::new(rules(identity.genesis)),
        identity,
        execution: Arc::new(execution(&NetworkProfile::mainnet())),
        limits: key::Limits::default(),
    }
}
fn request(header: &KeyHeader) -> Request {
    Request {
        parent: Hash256(header.common.previous_hash),
        miner: AccountPublicKey(header.miner),
        beneficiary: AccountPublicKey(header.beneficiary),
        release_info: header.info.map(u32::from_be_bytes).unwrap_or(0),
    }
}
fn first() -> Request {
    let Block::Key(header) = blocks(&NetworkProfile::mainnet()).remove(0) else {
        panic!()
    };
    request(&header)
}
fn wait_until(mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
}

#[tokio::test]
async fn background_candidates_reconstruct_public_headers_and_admit_retained_proofs() {
    let node = Node::new();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let config = config(&node);
    let importer = chain::Importer::new(
        node.db.clone(),
        config.execution.as_ref().clone(),
        config.rules.as_ref().clone(),
        Default::default(),
    )
    .unwrap();
    let builder = Builder::new(snapshots.clone(), config).unwrap();
    let mut count = 0;
    let mut last_id = None;
    for block in blocks(&NetworkProfile::mainnet()) {
        let prepared = if let Block::Key(original) = &block {
            let input = request(original);
            let calculation = original.time - 17;
            let task = builder
                .start(input, times(&[calculation.into(), original.time.into()]))
                .unwrap();
            let id = task.id();
            assert_ne!(Some(id), last_id);
            last_id = Some(id);
            assert_eq!(task.request(), input);
            let completion = task.wait().unwrap();
            assert_eq!(completion.id, id);
            assert_eq!(completion.request, input);
            assert_eq!(completion.calculation_time, Some(calculation));
            assert_eq!(completion.final_time, Some(original.time));
            let prepared = completion.result.unwrap();
            assert!(!builder.busy());
            assert_eq!(snapshots.live_snapshots(), 1);
            assert_eq!(prepared.header.seal, [0; 42]);
            assert_eq!(prepared.header.nonce, 0);
            assert_eq!(prepared.state.value.candidate.temporary().time, calculation);
            assert_eq!(prepared.state.metadata.status, prepared.state.value.status);
            assert_eq!(
                prepared.state.metadata.source_id,
                prepared.state.value.source_id
            );
            // These are retained public proofs, not newly mined solutions.
            // Ordinary Created admission rejects the unsealed candidate.
            assert!(
                importer
                    .import_created_block(
                        Block::Key(prepared.header.clone()).encode().unwrap(),
                        u64::MAX - 600_000,
                    )
                    .unwrap()
                    .wait()
                    .await
                    .is_err()
            );
            let mut sealed = prepared.header.clone();
            sealed.nonce = original.nonce;
            sealed.seal = original.seal;
            assert_eq!(
                Block::Key(sealed).encode().unwrap(),
                block.encode().unwrap()
            );
            count += 1;
            Some(prepared)
        } else {
            None
        };
        importer
            .import_created_block(block.encode().unwrap(), u64::MAX - 600_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
        if let Some(p) = prepared {
            assert!(
                p.state
                    .value
                    .view
                    .block(block.hash().unwrap())
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                node.db
                    .view()
                    .unwrap()
                    .require_record(block.hash().unwrap())
                    .unwrap()
                    .roots
                    .state,
                p.state.value.candidate.trees().roots()
            );
        }
        assert_eq!(snapshots.live_snapshots(), 0);
    }
    assert_eq!(count, 6);
}

#[test]
fn cancelled_preparation_keeps_ownership_until_join_and_does_not_rewrite_finished_results() {
    let node = Node::new();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let builder = Builder::new(snapshots.clone(), config(&node)).unwrap();
    let clone = builder.clone();
    for at in [0, 1] {
        let (clock, entered, release) = gate(at);
        let mut task = builder.start(first(), clock).unwrap();
        entered.recv_timeout(Duration::from_secs(10)).unwrap();
        let id = task.id();
        assert!(matches!(clone.start(first(), times(&[])), Err(Error::Busy)));
        assert_eq!(snapshots.live_snapshots(), at);
        task.request_cancel();
        assert!(!task.is_finished());
        assert!(task.try_result().is_none());
        assert!(clone.busy());
        release.send(()).unwrap();
        let done = task.wait().unwrap();
        assert_eq!(done.id, id);
        assert_eq!(done.request, first());
        assert!(matches!(done.result, Err(Error::Cancelled)));
        assert!(!builder.busy());
        assert_eq!(snapshots.live_snapshots(), 0);
    }
    let mut task = builder.start(first(), times(&[2000, 1000])).unwrap();
    wait_until(|| task.is_finished());
    // Finished but unread work still owns the single preparation permit.
    assert!(matches!(clone.start(first(), times(&[])), Err(Error::Busy)));
    task.request_cancel();
    let result = task.try_result().unwrap().result.unwrap();
    assert_eq!(result.header.time, 1000);
    assert_eq!(result.state.value.candidate.temporary().time, 2000);
    assert!(task.try_result().is_none());
    assert!(!builder.busy());
    drop(result);
    assert_eq!(snapshots.live_snapshots(), 0);

    let (clock, entered, release) = gate(1);
    let task = builder.start(first(), clock).unwrap();
    entered.recv_timeout(Duration::from_secs(10)).unwrap();
    let (finished, joined) = mpsc::sync_channel(1);
    let retiring = thread::spawn(move || {
        drop(task);
        finished.send(()).unwrap();
    });
    assert!(joined.try_recv().is_err());
    assert!(matches!(clone.start(first(), times(&[])), Err(Error::Busy)));
    release.send(()).unwrap();
    joined.recv_timeout(Duration::from_secs(10)).unwrap();
    retiring.join().unwrap();
    assert!(!builder.busy());
    assert_eq!(snapshots.live_snapshots(), 0);
}

#[test]
fn preparation_failures_keep_timing_and_release_bounded_snapshot_capacity() {
    let node = Node::new();
    let snapshots = snapshots::Owner::start(
        node.db.clone(),
        snapshots::Config {
            maximum_snapshots: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let builder = Builder::new(snapshots.clone(), config(&node)).unwrap();
    for values in [
        vec![-1],
        vec![u64::MAX as i128 + 1],
        vec![1000, -1],
        vec![1000, u64::MAX as i128 + 1],
    ] {
        let done = builder
            .start(first(), times(&values))
            .unwrap()
            .wait()
            .unwrap();
        assert!(matches!(done.result, Err(Error::Time)));
        assert_eq!(done.calculation_time, (values.len() == 2).then_some(1000));
        assert_eq!(done.final_time, None);
        assert_eq!(snapshots.live_snapshots(), 0);
        assert!(!builder.busy());
    }
    for at in [0, 1] {
        let mut read = 0;
        let clock = clock(move || {
            assert_ne!(read, at, "controlled clock panic");
            read += 1;
            1000
        });
        let done = builder.start(first(), clock).unwrap().wait().unwrap();
        assert!(matches!(done.result, Err(Error::Panicked)));
        assert_eq!(done.calculation_time, (at == 1).then_some(1000));
        assert_eq!(snapshots.live_snapshots(), 0);
        assert!(!builder.busy());
    }
    let mut missing = first();
    missing.parent = Hash256([99; 32]);
    let done = builder
        .start(missing, times(&[1000]))
        .unwrap()
        .wait()
        .unwrap();
    assert!(matches!(done.result, Err(Error::Preparation(_))));
    assert_eq!(done.final_time, None);
    assert_eq!(snapshots.live_snapshots(), 0);
    let held = builder
        .start(first(), times(&[1000, 1001]))
        .unwrap()
        .wait()
        .unwrap()
        .result
        .unwrap();
    let done = builder
        .start(first(), times(&[2000]))
        .unwrap()
        .wait()
        .unwrap();
    assert!(matches!(
        done.result,
        Err(Error::Preparation(ae_storage::pow::key::Error::Snapshot(
            snapshots::Error::ResourceLimit
        )))
    ));
    assert!(!builder.busy());
    assert_eq!(snapshots.live_snapshots(), 1);
    drop(held);
    assert_eq!(snapshots.live_snapshots(), 0);
    let mut bad = config(&node);
    bad.identity.execution_profile = Hash256([99; 32]);
    let bad = Builder::new(snapshots.clone(), bad).unwrap();
    assert!(matches!(
        bad.start(first(), times(&[1]))
            .unwrap()
            .wait()
            .unwrap()
            .result,
        Err(Error::Preparation(_))
    ));
    assert_eq!(snapshots.live_snapshots(), 0);
    for fault in 0..4 {
        let mut bad = config(&node);
        match fault {
            0 => bad.identity.consensus = ae_protocol::ConsensusType::Pos,
            1 => bad.execution = Arc::new(execution(&NetworkProfile::uat())),
            2 => Arc::make_mut(&mut bad.rules).genesis_hash = Hash256([99; 32]),
            _ => bad.limits.history = 0,
        }
        assert!(matches!(
            Builder::new(snapshots.clone(), bad),
            Err(Error::Configuration)
        ));
    }
}
