mod common;
use ae_chain::{
    block::{Block, MicroBlock},
    hyperchain::candidate as core,
    repository::{ledger, pow},
};
use ae_protocol::NetworkProfile;
use ae_storage::{pool::worker as pool, snapshots};
use ae_sync::chain::pow::{
    generator::{self as generator, Candidate, Client, Worker},
    packing,
};
use ae_types::AccountPublicKey;
use ae_vm_fate::maps::order::Otp26;
use common::{Node, ledger_fixtures as fixtures};
use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

struct Fixed(i128);
impl generator::Clock for Fixed {
    fn unix_millis(&self) -> i128 {
        self.0
    }
}
struct Gate {
    state: Mutex<(usize, usize, usize)>, // calls, permits, panic count
    wake: Condvar,
    time: i128,
}
impl Gate {
    fn new(time: i128, panics: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((0, 0, panics)),
            wake: Condvar::new(),
            time,
        })
    }
    fn allow(&self, n: usize) {
        self.state.lock().unwrap().1 = n;
        self.wake.notify_all();
    }
    fn calls(&self) -> usize {
        self.state.lock().unwrap().0
    }
}
impl generator::Clock for Gate {
    fn unix_millis(&self) -> i128 {
        let mut s = self.state.lock().unwrap();
        s.0 += 1;
        self.wake.notify_all();
        s = self.wake.wait_while(s, |s| s.1 < s.0).unwrap();
        let panic = s.0 <= s.2;
        drop(s);
        assert!(!panic, "controlled candidate worker panic");
        self.time
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.allow(usize::MAX);
    }
}
fn config(node: &Node) -> generator::Config<Otp26> {
    generator::Config {
        preparation: preparation(node),
        candidate: core::Limits::default(),
        packing: packing::Config {
            timeout: core::Timeout::Disabled,
            window_ms: 3000,
        },
        controls: 8,
        publications: 4,
        queued_transactions: 1000,
        queued_bytes: 1 << 20,
        feed_batch: 16,
        poll_interval: Duration::from_millis(2),
    }
}
fn start(
    node: &Node,
    cfg: generator::Config<Otp26>,
    clock: Arc<dyn generator::Clock>,
) -> (Worker<Otp26>, snapshots::Owner) {
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let worker = Worker::spawn(
        node.db.clone(),
        snapshots.clone(),
        node.client(),
        cfg,
        clock,
    )
    .unwrap();
    (worker, snapshots)
}
fn until(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < end, "bounded generator wait");
        std::thread::sleep(Duration::from_millis(2));
    }
}
fn command(ticket: generator::Ticket) {
    assert_eq!(ticket.wait_timeout(Duration::from_secs(5)), Some(Ok(())));
    assert_eq!(ticket.try_result(), Some(Ok(())));
}
fn push(node: &Node, tx: core::Signed) {
    node.client()
        .force_push(Arc::new(tx))
        .unwrap()
        .wait_timeout(Duration::from_secs(5))
        .unwrap();
}
fn candidate(client: &Client<Otp26>) -> Arc<Candidate<Otp26>> {
    until(|| client.candidate().is_some() && !client.status().worker);
    client.candidate().unwrap()
}
fn assert_public(candidate: &Candidate<Otp26>, original: &MicroBlock) -> Block {
    let mut block = candidate.prepared().value.candidate.block().clone();
    assert_eq!(block.header.signature, [0; 64]);
    block.header.signature = original.header.signature;
    let block = Block::Micro(block);
    assert_eq!(
        block.encode().unwrap(),
        Block::Micro(original.clone()).encode().unwrap()
    );
    block
}
fn drain(client: &Client<Otp26>) {
    while let Some(p) = client.front() {
        assert!(client.acknowledge(&p));
    }
}

#[test]
fn continuous_creation_reproduces_and_admits_public_micro_and_retains_stopped_candidate() {
    let (node, m) = setup();
    for tx in &m.transactions {
        push(&node, tx.clone());
    }
    let (worker, snapshots) = start(&node, config(&node), Arc::new(Fixed(m.header.time.into())));
    let client = worker.client();
    assert!(!client.status().generating);
    command(client.start().unwrap());
    let result = candidate(&client);
    let block = assert_public(&result, &m);
    let published = client.front().unwrap();
    assert!(Arc::ptr_eq(&result, &published.candidate));
    command(client.start().unwrap());
    assert_eq!(client.status().published, 2);
    command(client.stop().unwrap());
    assert!(!client.status().generating);
    assert!(Arc::ptr_eq(&result, &client.candidate().unwrap()));
    insert(&node, &block);
    let report = worker.shutdown().unwrap();
    assert!(report.status.failure.is_none());
    assert_eq!(report.status.attempts, 1);
    assert!(report.retirement.is_none());
    drop(result);
    drop(published);
    drain(&client);
    drop(client);
    drop(report);
    assert_eq!(snapshots.live_snapshots(), 0);
}

#[test]
fn accepted_transaction_event_updates_empty_candidate_using_real_execution() {
    let (node, m) = setup();
    let (worker, _) = start(&node, config(&node), Arc::new(Fixed(m.header.time.into())));
    let client = worker.client();
    command(client.start().unwrap());
    assert!(
        candidate(&client)
            .prepared()
            .value
            .candidate
            .block()
            .transactions
            .is_empty()
    );
    assert!(client.front().is_none());
    for tx in &m.transactions {
        push(&node, tx.clone());
    }
    until(|| client.front().is_some());
    assert_public(&candidate(&client), &m);
    assert_eq!(client.status().attempts, 1);
    assert!(client.status().updates >= 1);
    assert!(worker.shutdown().unwrap().status.failure.is_none());
}

#[test]
fn invalid_incremental_transaction_is_unchanged_without_pool_failure_report() {
    let (node, m) = setup();
    let (worker, _) = start(&node, config(&node), Arc::new(Fixed(m.header.time.into())));
    let client = worker.client();
    command(client.start().unwrap());
    let original = candidate(&client);
    let bad = common::invalid_signature(&m.transactions[0]);
    push(&node, bad.as_ref().clone());
    until(|| client.status().unchanged == 1);
    assert!(Arc::ptr_eq(&original, &client.candidate().unwrap()));
    assert_eq!(
        node.client()
            .inspect(bad.hash())
            .unwrap()
            .wait_timeout(Duration::from_secs(5))
            .unwrap()
            .failures,
        0
    );
    assert!(client.front().is_none());
    assert!(worker.shutdown().unwrap().status.failure.is_none());
}

#[test]
fn public_mainnet_fixed_gas_limit_is_preserved_even_with_zero_local_configuration() {
    let (node, m) = setup();
    let mut cfg = config(&node);
    cfg.preparation.execution.configured_block_gas_limit = 0.into();
    let (worker, _) = start(&node, cfg, Arc::new(Fixed(m.header.time.into())));
    let client = worker.client();
    command(client.start().unwrap());
    assert!(
        candidate(&client)
            .prepared()
            .value
            .candidate
            .block()
            .transactions
            .is_empty()
    );
    push(&node, m.transactions[0].clone());
    until(|| client.front().is_some());
    assert_public(&candidate(&client), &m);
    assert!(worker.shutdown().unwrap().status.failure.is_none());
}

#[test]
fn slow_creation_is_preempted_without_blocking_controls_or_publishing_stale_work() {
    let (node, m) = setup();
    let gate = Gate::new(m.header.time.into(), 0);
    let (worker, _) = start(&node, config(&node), gate.clone());
    let _release = Release(gate.clone());
    let client = worker.client();
    command(client.start().unwrap());
    until(|| gate.calls() == 1);
    push(&node, m.transactions[0].clone());
    until(|| client.status().queued_transactions == 1);
    insert(&node, &Block::Micro(m.clone()));
    until(|| client.status().preemptions == 1);
    assert_eq!(client.status().queued_transactions, 0);
    assert!(client.status().retiring);
    assert!(client.candidate().is_none());
    command(client.stop().unwrap());
    assert!(!client.status().generating);
    gate.allow(1);
    until(|| !client.status().worker);
    assert_eq!(client.status().attempts, 1);
    assert!(client.front().is_none());
    command(client.start().unwrap());
    until(|| gate.calls() == 2);
    gate.allow(2);
    let result = candidate(&client);
    assert_eq!(
        result
            .prepared()
            .value
            .candidate
            .block()
            .header
            .common
            .previous_hash,
        Block::Micro(m).hash().unwrap().0
    );
    assert!(
        result
            .prepared()
            .value
            .candidate
            .block()
            .transactions
            .is_empty()
    );
    assert!(worker.shutdown().unwrap().status.failure.is_none());
}

#[test]
fn actual_worker_panics_follow_native_immediate_retry_backoff_and_empty_success_reset() {
    let (node, m) = setup();
    let gate = Gate::new(m.header.time.into(), 2);
    let (worker, _) = start(&node, config(&node), gate.clone());
    let _release = Release(gate.clone());
    let client = worker.client();
    command(client.start().unwrap());
    until(|| gate.calls() == 1);
    gate.allow(1);
    until(|| gate.calls() == 2 && client.status().retries == 1);
    assert_eq!(client.status().retries, 1);
    let at = Instant::now();
    gate.allow(2);
    until(|| client.status().retry_pending);
    assert_eq!(client.status().retries, 2);
    until(|| gate.calls() == 3);
    assert!(at.elapsed() >= Duration::from_millis(100));
    gate.allow(3);
    let result = candidate(&client);
    assert!(
        result
            .prepared()
            .value
            .candidate
            .block()
            .transactions
            .is_empty()
    );
    assert_eq!(client.status().retries, 0);
    assert!(!client.status().retry_pending);
    assert!(client.front().is_none());
    assert_eq!(client.status().last_error, Some(generator::Error::Panicked));
    assert!(worker.shutdown().unwrap().status.failure.is_none());
}

#[test]
fn retained_control_receipts_are_bounded_and_shutdown_has_independent_fence() {
    let (node, m) = setup();
    let mut cfg = config(&node);
    cfg.controls = 1;
    let (worker, _) = start(&node, cfg, Arc::new(Fixed(m.header.time.into())));
    let client = worker.client();
    let receipt = client.start().unwrap();
    assert_eq!(receipt.wait_timeout(Duration::from_secs(5)), Some(Ok(())));
    let clone = receipt.clone();
    drop(receipt);
    assert!(matches!(client.stop(), Err(generator::RequestError::Busy)));
    let report = worker.shutdown().unwrap();
    assert!(report.status.failure.is_none());
    assert_eq!(clone.try_result(), Some(Ok(())));
    assert!(matches!(
        client.start(),
        Err(generator::RequestError::Closed)
    ));
}

#[test]
fn retained_publications_preserve_order_and_backpressure_even_after_acknowledgement() {
    let (node, m) = setup();
    for tx in &m.transactions {
        push(&node, tx.clone());
    }
    let mut cfg = config(&node);
    cfg.publications = 1;
    let (worker, _) = start(&node, cfg, Arc::new(Fixed(m.header.time.into())));
    let client = worker.client();
    command(client.start().unwrap());
    candidate(&client);
    let first = client.front().unwrap();
    assert_eq!(first.sequence, 1);
    assert_eq!(
        client.start().unwrap().wait_timeout(Duration::from_secs(5)),
        Some(Err(generator::RequestError::OutputFull))
    );
    assert!(client.acknowledge(&first));
    assert!(!client.acknowledge(&first));
    assert_eq!(
        client.start().unwrap().wait_timeout(Duration::from_secs(5)),
        Some(Err(generator::RequestError::OutputFull))
    );
    drop(first);
    command(client.start().unwrap());
    assert_eq!(client.front().unwrap().sequence, 2);
    assert!(worker.shutdown().unwrap().status.failure.is_none());
}

#[test]
fn accepted_feed_gap_fails_closed_instead_of_silently_skipping_transactions() {
    let node = Node::with_settings(pool::Settings {
        event_count: 0,
        origins_interval: None,
        ..Default::default()
    });
    let m = advance(&node);
    let gate = Gate::new(m.header.time.into(), 0);
    let (worker, _) = start(&node, config(&node), gate.clone());
    let _release = Release(gate.clone());
    let client = worker.client();
    command(client.start().unwrap());
    until(|| gate.calls() == 1);
    push(&node, m.transactions[0].clone());
    until(|| !client.status().accepting);
    gate.allow(1);
    let report = worker.shutdown().unwrap();
    assert!(matches!(
        report.status.failure,
        Some(generator::Error::PoolFeed(
            pool::events::ReadError::Lagged { .. }
        ))
    ));
    assert!(client.front().is_none());
}

#[test]
fn queued_transaction_bytes_are_bounded_during_slow_creation() {
    let (node, m) = setup();
    let mut cfg = config(&node);
    cfg.queued_bytes = 1;
    let gate = Gate::new(m.header.time.into(), 0);
    let (worker, _) = start(&node, cfg, gate.clone());
    let _release = Release(gate.clone());
    let client = worker.client();
    command(client.start().unwrap());
    until(|| gate.calls() == 1);
    push(&node, m.transactions[0].clone());
    until(|| !client.status().accepting);
    gate.allow(1);
    assert_eq!(
        worker.shutdown().unwrap().status.failure,
        Some(generator::Error::QueueLimit)
    );
    assert!(client.front().is_none());
}

#[test]
fn database_pool_snapshot_binding_and_joined_lease_prevent_duplicate_generators() {
    let (node, m) = setup();
    let cfg = config(&node);
    let (worker, owner) = start(&node, cfg.clone(), Arc::new(Fixed(m.header.time.into())));
    let clock = Arc::new(Fixed(m.header.time.into()));
    assert!(matches!(
        Worker::spawn(
            node.db.clone(),
            owner.clone(),
            node.client(),
            cfg.clone(),
            clock.clone()
        ),
        Err(generator::Error::Ownership(
            snapshots::Error::AlreadyRunning
        ))
    ));
    let foreign = Node::new();
    assert!(matches!(
        Worker::spawn(
            node.db.clone(),
            owner.clone(),
            foreign.client(),
            cfg.clone(),
            clock.clone()
        ),
        Err(generator::Error::Binding)
    ));
    let report = worker.shutdown().unwrap();
    assert!(matches!(
        Worker::spawn(
            node.db.clone(),
            owner.clone(),
            node.client(),
            cfg.clone(),
            clock.clone()
        ),
        Err(generator::Error::Ownership(
            snapshots::Error::AlreadyRunning
        ))
    ));
    drop(report);
    let second = Worker::spawn(node.db.clone(), owner, node.client(), cfg, clock).unwrap();
    assert!(second.shutdown().unwrap().status.failure.is_none());
}

#[test]
fn stopped_pool_is_terminal_and_never_redirects_generator_to_another_worker() {
    let (mut node, m) = setup();
    let (worker, _) = start(&node, config(&node), Arc::new(Fixed(m.header.time.into())));
    let client = worker.client();
    command(client.start().unwrap());
    candidate(&client);
    node.stop_worker();
    until(|| worker.is_finished());
    assert_eq!(
        worker.shutdown().unwrap().status.failure,
        Some(generator::Error::PoolClosed)
    );
    assert!(matches!(
        client.start(),
        Err(generator::RequestError::Closed)
    ));
}

#[test]
fn retry_delay_matches_all_saved_original_generator_boundaries() {
    let native: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/erlang_generator_business.json")).unwrap();
    for row in native["retry_delays"].as_array().unwrap() {
        assert_eq!(
            generator::retry_delay(row["failures"].as_u64().unwrap()).as_millis(),
            row["milliseconds"].as_u64().unwrap() as u128
        );
    }
    assert_eq!(generator::retry_delay(u64::MAX), Duration::from_secs(5));
}

#[test]
fn old_candidate_handles_keep_their_memory_credit_until_released() {
    let (node, m) = setup();
    for tx in &m.transactions {
        push(&node, tx.clone());
    }
    let mut cfg = config(&node);
    cfg.publications = 1;
    let (worker, _) = start(&node, cfg, Arc::new(Fixed(m.header.time.into())));
    let client = worker.client();
    let mut held = Vec::new();
    for expected in 1..=3 {
        command(client.start().unwrap());
        until(|| client.status().published == expected);
        held.push(candidate(&client));
        drain(&client);
        command(client.stop().unwrap());
    }
    command(client.start().unwrap());
    until(|| client.status().attempts == 4 && !client.status().worker);
    assert!(client.candidate().is_none());
    assert_eq!(client.status().published, 3);
    held.remove(0);
    until(|| client.status().published == 4);
    assert_public(&candidate(&client), &m);
    assert!(worker.shutdown().unwrap().status.failure.is_none());
}

#[test]
fn initial_packing_reports_bad_signature_once_while_updates_do_not() {
    let (node, m) = setup();
    let bad = common::invalid_signature(&m.transactions[0]);
    push(&node, bad.as_ref().clone());
    let (worker, _) = start(&node, config(&node), Arc::new(Fixed(m.header.time.into())));
    let client = worker.client();
    command(client.start().unwrap());
    assert!(
        candidate(&client)
            .prepared()
            .value
            .candidate
            .block()
            .transactions
            .is_empty()
    );
    assert_eq!(
        node.client()
            .inspect(bad.hash())
            .unwrap()
            .wait_timeout(Duration::from_secs(5))
            .unwrap()
            .failures,
        1
    );
    for tx in &m.transactions {
        push(&node, tx.clone());
    }
    until(|| client.front().is_some());
    assert_public(&candidate(&client), &m);
    assert_eq!(
        node.client()
            .inspect(bad.hash())
            .unwrap()
            .wait_timeout(Duration::from_secs(5))
            .unwrap()
            .failures,
        1
    );
    assert!(worker.shutdown().unwrap().status.failure.is_none());
}

#[test]
fn stopped_transaction_events_leave_candidate_readable_and_restart_walks_live_pool() {
    let (node, m) = setup();
    let (worker, _) = start(&node, config(&node), Arc::new(Fixed(m.header.time.into())));
    let client = worker.client();
    command(client.start().unwrap());
    let old = candidate(&client);
    command(client.stop().unwrap());
    for tx in &m.transactions {
        push(&node, tx.clone());
    }
    command(client.stop().unwrap());
    assert!(Arc::ptr_eq(&old, &client.candidate().unwrap()));
    assert_eq!(client.status().updates, 0);
    command(client.start().unwrap());
    until(|| client.front().is_some());
    assert_public(&candidate(&client), &m);
    assert_eq!(client.status().attempts, 2);
    assert!(worker.shutdown().unwrap().status.failure.is_none());
}
fn insert(node: &Node, block: &Block) {
    let network = NetworkProfile::mainnet();
    let view = node.db.view().unwrap();
    let mut rules = fixtures::rules(view.identity().genesis);
    rules.admission.now_ms = u64::MAX - 600_000;
    let update = fixtures::execution(&network).with_config(|config| {
        ledger::prepare(&view, block, config, &rules, Default::default()).unwrap()
    });
    node.db.commit(update).unwrap();
}
fn setup() -> (Node, MicroBlock) {
    let node = Node::new();
    let m = advance(&node);
    (node, m)
}
fn advance(node: &Node) -> MicroBlock {
    for b in fixtures::blocks(&NetworkProfile::mainnet()) {
        match b {
            Block::Micro(m) => return m,
            b => insert(node, &b),
        }
    }
    panic!("saved public microblock")
}
fn preparation(node: &Node) -> pow::Config<Otp26> {
    let identity = node.db.view().unwrap().identity().clone();
    fixtures::execution(&NetworkProfile::mainnet()).with_config(|c| {
        let fate = c.fate.unwrap();
        pow::Config {
            identity,
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
    })
}
