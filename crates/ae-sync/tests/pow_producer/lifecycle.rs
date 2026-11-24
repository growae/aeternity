use super::*;
use std::sync::{Condvar, Mutex};

pub(super) struct Gate {
    state: Mutex<(usize, usize, bool)>,
    wake: Condvar,
    origin: Instant,
    elapsed_ms: AtomicU64,
}
impl Gate {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((0, 0, false)),
            wake: Condvar::new(),
            origin: Instant::now(),
            elapsed_ms: AtomicU64::new(0),
        })
    }
    pub(super) fn set_elapsed_ms(&self, milliseconds: u64) {
        self.elapsed_ms.store(milliseconds, Ordering::SeqCst);
    }
    pub(super) fn calls(&self) -> usize {
        self.state.lock().unwrap().0
    }
    pub(super) fn allow(&self, n: usize) {
        self.state.lock().unwrap().1 = n;
        self.wake.notify_all();
    }
    pub(super) fn step(&self) {
        until(|| self.calls() > 0);
        let call = self.calls();
        self.allow(call);
        until(|| self.calls() > call);
    }
    pub(super) fn reach(
        &self,
        client: &producer::Client,
        condition: impl Fn(&producer::Status) -> bool,
    ) {
        let end = Instant::now() + Duration::from_secs(10);
        until(|| self.calls() > 0);
        while !condition(&client.status()) {
            assert!(client.status().failure.is_none(), "{:?}", client.status());
            assert!(Instant::now() < end, "{:?}", client.status());
            self.step();
        }
    }
}
pub(super) struct GateClock(pub(super) Arc<Gate>);
impl producer::Clock for GateClock {
    fn now(&self) -> Result<(u64, Instant), producer::Error> {
        let mut s = self.0.state.lock().unwrap();
        s.0 += 1;
        self.0.wake.notify_all();
        s = self.0.wake.wait_while(s, |s| s.1 < s.0).unwrap();
        let panic = s.2;
        drop(s);
        assert!(!panic, "controlled producer clock failure");
        let (wall, _) = producer::Clock::now(&producer::SystemClock)?;
        Ok((
            wall,
            self.0.origin + Duration::from_millis(self.0.elapsed_ms.load(Ordering::SeqCst)),
        ))
    }
}
pub(super) struct Release(pub(super) Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.allow(usize::MAX);
    }
}
fn start(node: &Node, cfg: producer::Config, gate: &Arc<Gate>) -> producer::Worker {
    producer::Worker::spawn(
        &node.importer,
        node.generator(),
        node.keys(),
        cfg,
        GateClock(gate.clone()),
    )
    .unwrap()
}
fn stop(worker: producer::Worker, gate: &Gate) -> producer::Report {
    worker.client().request_shutdown();
    gate.allow(usize::MAX);
    finish(worker)
}

#[test]
fn shutdown_fences_work_after_a_blocked_clock_returns() {
    let node = Node::new(1, false);
    node.push(1);
    let gate = Gate::new();
    let worker = start(&node, node.config(), &gate);
    let _release = Release(gate.clone());
    until(|| gate.calls() == 1);
    let report = stop(worker, &gate);
    assert_eq!(report.status.signatures, 0);
    assert_eq!(report.status.admissions, 0);
    assert_eq!(node.db.view().unwrap().status().revision, 2);
    assert!(node.keys().info().accepting);
    assert!(node.generator().status().accepting);
}

#[test]
fn shutdown_and_panic_retain_the_original_signing_receipt_without_admission() {
    for panic in [false, true] {
        let node = Node::new(1, false);
        node.push(1);
        let gate = Gate::new();
        let worker = start(&node, node.config(), &gate);
        let _release = Release(gate.clone());
        let client = worker.client();
        gate.reach(&client, |s| s.signatures == 1 && s.pending);
        if panic {
            gate.state.lock().unwrap().2 = true;
            gate.allow(usize::MAX);
            until(|| worker.is_finished());
        }
        let mut report = stop(worker, &gate);
        assert_eq!(
            report.status.failure,
            panic.then_some(producer::Error::Panicked)
        );
        assert_eq!(report.status.admissions, 0);
        assert!(matches!(
            report.completions.as_slice(),
            [producer::Completion::Discarded { result: Ok(_), .. }]
        ));
        assert!(report.poll());
        assert_eq!(report.completions.len(), 1);
        assert_eq!(node.db.view().unwrap().status().revision, 2);
    }
}

#[test]
fn shutdown_preserves_an_accepted_durable_admission_and_never_replays_it() {
    let node = Node::new(1, false);
    node.push(1);
    let gate = Gate::new();
    let worker = start(&node, node.config(), &gate);
    let _release = Release(gate.clone());
    let client = worker.client();
    gate.reach(&client, |s| s.admissions == 1 && s.pending);
    let mut report = stop(worker, &gate);
    let [
        producer::Completion::Admitted {
            hash,
            result: Ok(result),
            ..
        },
    ] = report.completions.as_slice()
    else {
        panic!("{:?}", report.completions)
    };
    assert_eq!(result.accepted.len(), 1);
    assert_eq!(node.db.view().unwrap().status().tip, Some(*hash));
    for _ in 0..10 {
        assert!(report.poll());
    }
    assert_eq!(report.completions.len(), 1);
    assert_eq!(node.db.view().unwrap().status().revision, 3);
    assert_eq!(report.status.admissions, 1);
}

#[test]
fn changed_signing_key_discards_an_already_accepted_signature() {
    let node = Node::new(1, false);
    node.push(1);
    let gate = Gate::new();
    let worker = start(&node, node.config(), &gate);
    let _release = Release(gate.clone());
    let client = worker.client();
    gate.reach(&client, |s| s.signatures == 1 && s.pending);
    let candidate = node.keys().info().keys.candidate;
    node.keys()
        .promote_candidate(candidate)
        .unwrap()
        .blocking_wait()
        .unwrap();
    gate.reach(&client, |s| s.discarded == 1);
    assert!(!client.status().leader);
    assert_eq!(client.status().admissions, 0);
    let report = stop(worker, &gate);
    assert_eq!(report.outputs.len(), 1);
    assert!(matches!(
        report.outputs[0].completion,
        producer::Completion::Discarded { result: Ok(_), .. }
    ));
    assert_eq!(node.db.view().unwrap().status().revision, 2);
}

#[test]
fn disable_is_idempotent_and_fences_pending_signatures_before_reenable() {
    let node = Node::new(1, false);
    node.push(1);
    let gate = Gate::new();
    let worker = start(&node, node.config(), &gate);
    let _release = Release(gate.clone());
    let client = worker.client();
    gate.reach(&client, |s| s.signatures == 1 && s.pending);
    let revision = client.set_enabled(false).unwrap();
    assert_eq!(client.set_enabled(false).unwrap(), revision);
    gate.reach(&client, |s| {
        s.applied_revision == revision && s.discarded == 1
    });
    assert_eq!(client.status().admissions, 0);
    let output = client.front().unwrap();
    assert!(matches!(
        output.completion,
        producer::Completion::Discarded { result: Ok(_), .. }
    ));
    assert!(client.acknowledge(&output));
    drop(output);
    let next = client.set_enabled(true).unwrap();
    assert_eq!(next, revision + 1);
    gate.allow(usize::MAX);
    admitted(&node, &client);
    assert!(finish(worker).status.failure.is_none());
    assert_eq!(client.set_enabled(true), Err(producer::Error::Closed));
}

#[test]
fn startup_recovery_is_explicit_and_foreign_signers_never_start_generation() {
    for foreign in [false, true] {
        let node = Node::new(1, false);
        let other = Node::new(1, false);
        node.push(1);
        let gate = Gate::new();
        let mut cfg = node.config();
        cfg.resume_current = foreign;
        let signer = if foreign { other.keys() } else { node.keys() };
        let worker = producer::Worker::spawn(
            &node.importer,
            node.generator(),
            signer,
            cfg,
            GateClock(gate.clone()),
        )
        .unwrap();
        let _release = Release(gate.clone());
        for _ in 0..10 {
            gate.step();
        }
        assert_eq!(worker.client().status().signatures, 0);
        assert!(!node.generator().status().generating);
        assert_eq!(worker.client().status().leader, !foreign);
        assert!(stop(worker, &gate).status.failure.is_none());
    }
}

#[test]
fn unread_outputs_and_external_clones_apply_backpressure_and_survive_exit() {
    let node = Node::new(1, false);
    let mut cfg = node.config();
    cfg.outputs = 1;
    let worker = node.spawn(cfg);
    let client = worker.client();
    node.push(1);
    until(|| client.front().is_some());
    let first = client.front().unwrap();
    assert!(matches!(
        first.completion,
        producer::Completion::Admitted { result: Ok(_), .. }
    ));
    assert!(client.acknowledge(&first));
    assert!(!client.acknowledge(&first));
    node.push(2);
    until(|| {
        node.generator().candidate().is_some_and(|c| {
            c.prepared().value.candidate.block().transactions == vec![node.transaction(2)]
        })
    });
    std::thread::sleep(Duration::from_millis(node.cycle * 2));
    assert_eq!(client.status().signatures, 1);
    assert_eq!(client.status().admissions, 1);
    drop(first);
    until(|| client.front().is_some());
    let report = finish(worker);
    assert_eq!(report.outputs.len(), 1);
    assert!(matches!(
        report.outputs[0].completion,
        producer::Completion::Admitted { result: Ok(_), .. }
    ));
    assert!(client.front().is_none());
    assert_eq!(report.status.admissions, 2);
}

#[test]
fn binding_and_joined_retirement_prevent_competing_producer_owners() {
    let node = Node::new(1, false);
    let other = Node::new(1, false);
    assert!(matches!(
        producer::Worker::spawn(
            &node.importer,
            other.generator(),
            node.keys(),
            node.config(),
            producer::SystemClock
        ),
        Err(producer::Error::Binding)
    ));
    let mut cfg = node.config();
    cfg.micro_cycle_ms += 1;
    assert!(matches!(
        producer::Worker::spawn(
            &node.importer,
            node.generator(),
            node.keys(),
            cfg,
            producer::SystemClock
        ),
        Err(producer::Error::Binding)
    ));
    cfg = node.config();
    cfg.enabled = false;
    let worker = node.spawn(cfg);
    let spawn = || {
        producer::Worker::spawn(
            &node.importer,
            node.generator(),
            node.keys(),
            cfg,
            producer::SystemClock,
        )
    };
    assert!(matches!(
        spawn(),
        Err(producer::Error::Ownership(snapshots::Error::AlreadyRunning))
    ));
    let report = finish(worker);
    assert!(matches!(
        spawn(),
        Err(producer::Error::Ownership(snapshots::Error::AlreadyRunning))
    ));
    drop(report);
    assert!(finish(spawn().unwrap()).status.failure.is_none());
}

#[test]
fn own_micro_commit_enforces_the_monotonic_sleep_before_next_signature() {
    let node = Node::new(1, false);
    node.push(1);
    let gate = Gate::new();
    let worker = start(&node, node.config(), &gate);
    let _release = Release(gate.clone());
    let client = worker.client();
    gate.reach(&client, |s| s.published == 1 && s.sleeping);
    admitted(&node, &client);
    node.push(2);
    until(|| {
        node.generator().candidate().is_some_and(|c| {
            c.prepared().value.candidate.block().transactions == vec![node.transaction(2)]
        })
    });
    for _ in 0..5 {
        gate.step();
    }
    assert_eq!(client.status().signatures, 1);
    gate.elapsed_ms.store(node.cycle - 1, Ordering::SeqCst);
    gate.step();
    assert_eq!(client.status().signatures, 1);
    gate.elapsed_ms.store(node.cycle, Ordering::SeqCst);
    gate.reach(&client, |s| s.admissions == 2 && s.pending);
    let report = stop(worker, &gate);
    assert!(matches!(
        report.completions.as_slice(),
        [producer::Completion::Admitted { result: Ok(_), .. }]
    ));
    assert_eq!(node.db.view().unwrap().status().revision, 4);
}

#[test]
fn real_top_change_discards_a_signature_accepted_for_the_old_parent() {
    let node = Node::new(1, false);
    node.push(1);
    let gate = Gate::new();
    let worker = start(&node, node.config(), &gate);
    let _release = Release(gate.clone());
    let client = worker.client();
    gate.reach(&client, |s| s.signatures == 1 && s.pending);
    let mut competing = node
        .generator()
        .candidate()
        .unwrap()
        .prepared()
        .value
        .candidate
        .block()
        .clone();
    competing.header.time += 1;
    competing.header = node
        .keys()
        .sign_micro_for(AccountPublicKey(node.genesis.miner), competing.header)
        .unwrap()
        .blocking_wait()
        .unwrap()
        .header;
    let block = Block::Micro(competing);
    let hash = block.hash().unwrap();
    let (now, _) = producer::Clock::now(&producer::SystemClock).unwrap();
    let mut ticket = node
        .importer
        .import_block(block.encode().unwrap(), now)
        .unwrap();
    let mut result = None;
    until(|| {
        result = ticket.try_result();
        result.is_some()
    });
    assert!(result.unwrap().is_ok());
    gate.reach(&client, |s| s.discarded == 1);
    assert_eq!(client.status().admissions, 0);
    assert_eq!(client.status().top, hash);
    let report = stop(worker, &gate);
    assert!(matches!(
        report.outputs[0].completion,
        producer::Completion::Discarded { result: Ok(_), .. }
    ));
    assert_eq!(node.db.view().unwrap().status().tip, Some(hash));
}
