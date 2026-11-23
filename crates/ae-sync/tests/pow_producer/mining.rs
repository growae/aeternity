use super::*;
use ae_sync::chain::pow::{miner, producer::mining as mine};
use std::sync::{Condvar, Mutex};

pub(super) fn config(instances: usize) -> mine::Config {
    let mut config = mine::Config::new(AccountPublicKey([9; 32]));
    config.retry = Duration::from_millis(20);
    config.instances = (0..instances)
        .map(|i| mine::Instance {
            repeats: i as u64 + 1,
            worker: miner::Config {
                requests: 2,
                solver: ae_consensus::cuckoo::solver::Limits {
                    // Exercise real canonical mining dispatch and its explicit
                    // resource error without allocating a 29-bit graph here.
                    memory_bytes: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
        })
        .collect();
    config
}
fn spawn(
    node: &Node,
    cfg: producer::Config,
    mining: mine::Config,
    clock: Arc<dyn generator::Clock>,
) -> producer::Worker {
    producer::Worker::spawn_with_mining(
        &node.importer,
        node.generator(),
        node.keys(),
        cfg,
        producer::SystemClock,
        mine::Inputs {
            snapshots: node._snapshots.clone(),
            config: mining,
            clock,
        },
    )
    .unwrap()
}
fn stopped(node: &Node) -> producer::Config {
    producer::Config {
        enabled: false,
        resume_current: false,
        ..node.config()
    }
}
fn drain(client: &producer::Client) -> Vec<Arc<producer::Output>> {
    let mut result = vec![];
    while let Some(o) = client.front() {
        assert!(client.acknowledge(&o));
        result.push(o);
    }
    result
}
fn healthy(client: &producer::Client) {
    assert!(client.status().failure.is_none(), "{:?}", client.status());
}

#[test]
fn stopped_prebuild_dispatches_both_instances_and_stop_rebuilds_without_solving() {
    let node = Node::new(1, false);
    let worker = spawn(
        &node,
        stopped(&node),
        config(2),
        Arc::new(generator::SystemClock),
    );
    let client = worker.client();
    until(|| {
        healthy(&client);
        client.status().mining.as_ref().unwrap().candidate.is_some()
    });
    let initial = client.status().mining.unwrap();
    let candidate = initial.candidate.unwrap();
    assert_eq!(candidate.request.miner, node.keys().info().keys.candidate);
    assert_eq!(candidate.request.beneficiary, AccountPublicKey([9; 32]));
    assert_eq!(
        candidate.request.parent,
        node.db.view().unwrap().status().tip.unwrap()
    );
    assert!(initial.workers.iter().all(|w| w.submitted == 0));
    assert_eq!(node._snapshots.live_snapshots(), 0);
    drop(drain(&client));
    client.set_enabled(true).unwrap();
    let mut results = vec![];
    until(|| {
        healthy(&client);
        for output in drain(&client) {
            if let producer::Completion::Mining(c) = &output.completion
                && let mine::Completion::Searched {
                    candidate: c,
                    result,
                    selected,
                } = c.as_ref()
            {
                assert_eq!(c.id, candidate.id);
                assert!(!selected);
                assert_eq!(result.work.edge_bits, 29);
                assert!(matches!(result.result, Err(miner::Error::Solve { .. })));
                results.push((result.id, result.work));
            }
        }
        results.len() >= 4
            && results
                .iter()
                .any(|(id, w)| id.sequence == 1 && w.nonces.repeats() == 1)
            && results
                .iter()
                .any(|(id, w)| id.sequence == 1 && w.nonces.repeats() == 2)
    });
    assert_eq!(
        results
            .iter()
            .map(|(id, _)| id.worker)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2
    );
    let first = results
        .iter()
        .find(|(id, w)| id.sequence == 1 && w.nonces.repeats() == 1)
        .unwrap()
        .1
        .nonces;
    let second = results
        .iter()
        .find(|(id, w)| id.sequence == 1 && w.nonces.repeats() == 2)
        .unwrap()
        .1
        .nonces;
    assert_eq!(first.repeats(), 1);
    assert_eq!(second.repeats(), 2);
    assert_eq!(
        second.start(),
        ae_consensus::cuckoo::solver::Nonces::reserve(first.next().into(), 2)
            .unwrap()
            .start()
    );
    let revision = client.set_enabled(false).unwrap();
    until(|| {
        healthy(&client);
        drop(drain(&client));
        let status = client.status();
        let m = status.mining.unwrap();
        status.applied_revision == revision
            && m.active == 0
            && m.preparing.is_none()
            && m.candidate.as_ref().is_some_and(|c| c.id != candidate.id)
    });
    let submitted: Vec<_> = client
        .status()
        .mining
        .unwrap()
        .workers
        .iter()
        .map(|w| w.submitted)
        .collect();
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(
        client
            .status()
            .mining
            .unwrap()
            .workers
            .iter()
            .map(|w| w.submitted)
            .collect::<Vec<_>>(),
        submitted
    );
    let report = finish(worker);
    assert!(report.status.failure.is_none());
    assert!(report.mining.as_ref().unwrap().is_settled());
    assert_eq!(node._snapshots.live_snapshots(), 0);
}

#[test]
fn retained_preparation_output_holds_capacity_before_cpu_dispatch() {
    let node = Node::new(1, false);
    let cfg = producer::Config {
        outputs: 1,
        enabled: true,
        resume_current: false,
        ..node.config()
    };
    let worker = spawn(&node, cfg, config(1), Arc::new(generator::SystemClock));
    let client = worker.client();
    until(|| client.front().is_some());
    let output = client.front().unwrap();
    assert!(
        matches!(&output.completion, producer::Completion::Mining(c) if matches!(c.as_ref(), mine::Completion::Prepared { used: true, .. }))
    );
    assert!(client.acknowledge(&output));
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(client.status().mining.unwrap().workers[0].submitted, 0);
    assert_eq!(node._snapshots.live_snapshots(), 0);
    drop(output);
    until(|| client.status().mining.unwrap().workers[0].submitted > 0);
    let report = finish(worker);
    assert!(report.status.failure.is_none());
    assert!(
        report
            .outputs
            .iter()
            .any(|o| matches!(o.completion, producer::Completion::Mining(_)))
            || report
                .completions
                .iter()
                .any(|o| matches!(o, producer::Completion::Mining(_)))
    );
}

pub(super) struct PreparationGate {
    state: Mutex<(bool, bool)>,
    wake: Condvar,
}
impl PreparationGate {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((false, false)),
            wake: Condvar::new(),
        })
    }
    pub(super) fn entered(&self) -> bool {
        self.state.lock().unwrap().0
    }
    pub(super) fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.wake.notify_all();
    }
}
impl generator::Clock for PreparationGate {
    fn unix_millis(&self) -> i128 {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.wake.notify_all();
        drop(self.wake.wait_while(state, |s| !s.1).unwrap());
        generator::Clock::unix_millis(&generator::SystemClock)
    }
}
pub(super) struct ReleasePreparation(pub(super) Arc<PreparationGate>);
impl Drop for ReleasePreparation {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[test]
fn producer_retirement_keeps_blocked_preparation_and_exclusive_lease_until_joined() {
    let node = Node::new(1, false);
    let gate = PreparationGate::new();
    let worker = spawn(&node, stopped(&node), config(1), gate.clone());
    let _release = ReleasePreparation(gate.clone());
    let client = worker.client();
    until(|| gate.entered() && client.status().mining.as_ref().unwrap().preparing.is_some());
    let id = client.status().mining.unwrap().preparing.unwrap();
    let mut report = worker.shutdown().unwrap();
    assert!(!report.is_settled());
    assert!(!report.poll());
    assert!(matches!(
        producer::Worker::spawn(
            &node.importer,
            node.generator(),
            node.keys(),
            stopped(&node),
            producer::SystemClock
        ),
        Err(producer::Error::Ownership(
            ae_storage::snapshots::Error::AlreadyRunning
        ))
    ));
    gate.release();
    until(|| report.poll());
    assert!(report.completions.iter().any(|c| matches!(c,
        producer::Completion::Mining(c) if matches!(c.as_ref(), mine::Completion::Prepared { id: actual, used: false, result: Err(mine::PreparationError::Build(ae_sync::chain::pow::preparation::Error::Cancelled)), .. } if *actual == id))));
    assert_eq!(node._snapshots.live_snapshots(), 0);
    drop(report);
    let replacement = spawn(
        &node,
        stopped(&node),
        config(0),
        Arc::new(generator::SystemClock),
    );
    assert!(finish(replacement).status.failure.is_none());
}

#[test]
fn constructor_failure_joins_partial_cpu_set_and_releases_the_producer_lease() {
    let node = Node::new(1, false);
    let mut bad = config(2);
    bad.instances[1].worker.requests = 0;
    assert!(matches!(
        producer::Worker::spawn_with_mining(
            &node.importer,
            node.generator(),
            node.keys(),
            stopped(&node),
            producer::SystemClock,
            mine::Inputs {
                snapshots: node._snapshots.clone(),
                config: bad,
                clock: Arc::new(generator::SystemClock)
            }
        ),
        Err(producer::Error::Mining(mine::Error::Worker(
            miner::Error::Configuration
        )))
    ));
    let worker = spawn(
        &node,
        stopped(&node),
        config(1),
        Arc::new(generator::SystemClock),
    );
    assert!(finish(worker).status.failure.is_none());
}

fn import_micro(node: &Node) -> Hash256 {
    let parent = node.db.view().unwrap().status().tip.unwrap();
    let prepared = ae_storage::pow::prepare_micro(
        &node._snapshots,
        &configuration(&node.db, &node.network, node.cycle).preparation,
        pow::Input {
            parent,
            now: 10000,
            limits: Default::default(),
        },
    )
    .unwrap();
    let mut block = prepared.value.candidate.block().clone();
    let signed = node
        .keys()
        .sign_micro_for(node.keys().info().keys.signing, block.header.clone())
        .unwrap()
        .blocking_wait()
        .unwrap();
    block.header = signed.header;
    drop(prepared);
    let block = Block::Micro(block);
    let hash = block.hash().unwrap();
    let mut ticket = node
        .importer
        .import_block(block.encode().unwrap(), u64::MAX - 600_000)
        .unwrap();
    until(|| {
        ticket
            .try_result()
            .map(|r| {
                r.unwrap();
                true
            })
            .unwrap_or(false)
    });
    assert_eq!(node.db.view().unwrap().status().tip, Some(hash));
    hash
}

#[test]
fn same_generation_micro_preserves_registered_work_until_retry_prepares_latest_top() {
    use lifecycle::{Gate, GateClock, Release};
    let node = Node::new(1, false);
    let gate = Gate::new();
    let mut cfg = config(2);
    cfg.retry = Duration::ZERO;
    let worker = producer::Worker::spawn_with_mining(
        &node.importer,
        node.generator(),
        node.keys(),
        producer::Config {
            resume_current: false,
            ..node.config()
        },
        GateClock(gate.clone()),
        mine::Inputs {
            snapshots: node._snapshots.clone(),
            config: cfg,
            clock: Arc::new(generator::SystemClock),
        },
    )
    .unwrap();
    let _release = Release(gate.clone());
    let client = worker.client();
    gate.reach(&client, |s| s.mining.as_ref().unwrap().active == 2);
    let first = client.status().mining.unwrap().candidate.unwrap();
    drop(drain(&client));
    let new_top = import_micro(&node);
    gate.reach(&client, |s| s.top == new_top);
    let status = client.status().mining.unwrap();
    assert_eq!(status.candidate.unwrap().id, first.id);
    assert_eq!(status.active, 1);
    assert!(status.preparing.is_some());
    let outputs = drain(&client);
    assert!(outputs.iter().any(|o| matches!(&o.completion,
        producer::Completion::Mining(c) if matches!(c.as_ref(), mine::Completion::Searched { candidate, .. } if candidate.id == first.id))));
    drop(outputs);
    gate.reach(&client, |s| {
        s.mining
            .as_ref()
            .unwrap()
            .candidate
            .as_ref()
            .is_some_and(|c| c.request.parent == new_top)
    });
    let next = client.status().mining.unwrap().candidate.unwrap();
    assert_ne!(next.id, first.id);
    assert_eq!(
        next.header.common.previous_key_hash,
        first.header.common.previous_key_hash
    );
    client.request_shutdown();
    gate.allow(usize::MAX);
    assert!(finish(worker).status.failure.is_none());
}

#[test]
fn late_preparation_is_observed_but_not_used_after_micro_top_changes() {
    let node = Node::new(1, false);
    let gate = PreparationGate::new();
    let worker = spawn(&node, stopped(&node), config(0), gate.clone());
    let _release = ReleasePreparation(gate.clone());
    let client = worker.client();
    until(|| gate.entered() && client.status().mining.as_ref().unwrap().preparing.is_some());
    let first = client.status().mining.unwrap().preparing.unwrap();
    let new_top = import_micro(&node);
    until(|| client.status().top == new_top);
    gate.release();
    until(|| {
        healthy(&client);
        client
            .status()
            .mining
            .unwrap()
            .candidate
            .as_ref()
            .is_some_and(|c| c.request.parent == new_top)
    });
    let outputs = drain(&client);
    assert!(outputs.iter().any(|o| matches!(&o.completion,
        producer::Completion::Mining(c) if matches!(c.as_ref(), mine::Completion::Prepared { id, used: false, result: Ok(_), .. } if *id == first))));
    assert!(outputs.iter().any(|o| matches!(&o.completion,
        producer::Completion::Mining(c) if matches!(c.as_ref(), mine::Completion::Prepared { id, used: true, .. } if *id != first))));
    assert!(finish(worker).status.failure.is_none());
}

#[test]
fn stopped_key_promotion_precedes_preparation_with_the_new_candidate_identity() {
    let node = Node::new(1, true);
    let before = node.keys().info().keys;
    let worker = spawn(
        &node,
        producer::Config {
            enabled: false,
            ..node.config()
        },
        config(1),
        Arc::new(generator::SystemClock),
    );
    let client = worker.client();
    until(|| {
        healthy(&client);
        client.status().mining.unwrap().candidate.is_some()
    });
    let after = node.keys().info().keys;
    assert_eq!(after.signing, before.candidate);
    assert_ne!(after.candidate, before.candidate);
    let status = client.status();
    assert_eq!(status.promotions, 1);
    assert!(!status.enabled);
    let mining = status.mining.unwrap();
    assert_eq!(mining.candidate.unwrap().request.miner, after.candidate);
    assert_eq!(mining.workers[0].submitted, 0);
    let outputs = drain(&client);
    assert!(matches!(
        &outputs[0].completion,
        producer::Completion::Promoted { result: Ok(_), .. }
    ));
    assert!(
        matches!(&outputs[1].completion, producer::Completion::Mining(c) if matches!(c.as_ref(), mine::Completion::Prepared { used: true, .. }))
    );
    assert!(finish(worker).status.failure.is_none());
}
