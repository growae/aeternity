mod common;
use ae_chain::{
    block::{Block, MicroBlock},
    hyperchain::candidate as core,
    repository::{self as repo, ledger, pow},
};
use ae_protocol::NetworkProfile;
use ae_storage::{
    pool::{self, worker},
    pow as owned, snapshots,
};
use ae_sync::chain::pow::packing::{self, Packing, Progress};
use ae_types::{AccountPublicKey, Hash256};
use ae_vm_fate::maps::order::Otp26;
use common::{Node, ledger_fixtures as fixtures};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

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
    for b in fixtures::blocks(&NetworkProfile::mainnet()) {
        match b {
            Block::Micro(m) => return (node, m),
            b => insert(&node, &b),
        }
    }
    panic!("saved public microblock")
}
fn prepare(
    node: &Node,
    owner: &snapshots::Owner,
    m: &MicroBlock,
    limits: core::Limits,
) -> owned::Prepared<Otp26> {
    let identity = node.db.view().unwrap().identity().clone();
    let config = fixtures::execution(&NetworkProfile::mainnet()).with_config(|c| {
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
    });
    owned::prepare_micro(
        owner,
        &config,
        pow::Input {
            parent: Hash256(m.header.common.previous_hash),
            now: m.header.time.into(),
            limits,
        },
    )
    .unwrap()
}
fn config() -> packing::Config {
    packing::Config {
        timeout: core::Timeout::Disabled,
        window_ms: 3000,
    }
}
fn wait<T>(ticket: worker::Ticket<T>) -> T {
    ticket.wait_timeout(Duration::from_secs(5)).unwrap()
}
fn finish(p: &mut Packing<Otp26>) {
    let end = Instant::now() + Duration::from_secs(5);
    while !p.ready() {
        assert!(Instant::now() < end);
        p.step().unwrap();
        std::thread::yield_now();
    }
}
fn until(p: &mut Packing<Otp26>, target: Progress) {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < end);
        if p.step().unwrap() == target {
            break;
        }
        std::thread::yield_now();
    }
}
fn retire(mut r: packing::Cancelled) -> packing::CancelReport {
    let end = Instant::now() + Duration::from_secs(5);
    while !r.step().unwrap() {
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
    assert!(r.step().unwrap());
    r.finish()
}
fn bytes(p: &owned::Prepared<Otp26>) -> Vec<u8> {
    Block::Micro(p.value.candidate.block().clone())
        .encode()
        .unwrap()
}
fn check(p: &owned::Prepared<Otp26>, original: &MicroBlock) -> Block {
    let mut block = p.value.candidate.block().clone();
    assert_eq!(block.header.signature, [0; 64]);
    block.header.signature = original.header.signature;
    let block = Block::Micro(block);
    assert_eq!(
        block.encode().unwrap(),
        Block::Micro(original.clone()).encode().unwrap()
    );
    assert_eq!(p.value.status, p.metadata.status);
    assert_eq!(p.value.source_id, p.metadata.source_id);
    block
}
#[test]
fn live_pow_pool_packs_saved_public_microblock_for_ordinary_admission() {
    let (node, m) = setup();
    let owner = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let input = prepare(&node, &owner, &m, Default::default());
    let metadata = input.metadata.clone();
    let client = node.client();
    for tx in &m.transactions {
        wait(client.force_push(Arc::new(tx.clone())).unwrap());
    }
    let mut p = Packing::start(input, client.clone(), config()).unwrap();
    finish(&mut p);
    let packed = p.finish().unwrap();
    let block = check(&packed, &m);
    assert_eq!(packed.metadata.cursor, metadata.cursor);
    assert_eq!(packed.metadata.resilience, metadata.resilience);
    insert(&node, &block);
    assert_eq!(
        node.db
            .view()
            .unwrap()
            .require_record(block.hash().unwrap())
            .unwrap()
            .roots
            .state,
        packed.value.candidate.trees().roots()
    );
    assert!(
        packed
            .value
            .reader
            .selected
            .repository()
            .block(block.hash().unwrap())
            .unwrap()
            .is_none()
    );
    drop(packed);
    assert_eq!(owner.live_snapshots(), 0);
}
#[test]
fn accepted_selection_keeps_parent_snapshot_across_real_top_advance() {
    let (node, m) = setup();
    let owner = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let input = prepare(&node, &owner, &m, Default::default());
    let original_status = input.metadata.status;
    let client = node.client();
    wait(
        client
            .force_push(Arc::new(m.transactions[0].clone()))
            .unwrap(),
    );
    let mut p = Packing::start(input, client, config()).unwrap();
    until(&mut p, Progress::Selected);
    let unfinished = p.finish().err().unwrap();
    assert_eq!(unfinished.error, packing::Error::Sequence);
    p = unfinished.pending;
    insert(&node, &Block::Micro(m.clone()));
    assert_ne!(node.db.view().unwrap().status(), original_status);
    assert_eq!(p.metadata().status, original_status);
    finish(&mut p);
    let packed = p.finish().unwrap();
    check(&packed, &m);
    assert_eq!(
        packed.value.reader.selected.repository().status(),
        original_status
    );
}
#[test]
fn cancelled_pow_selection_keeps_its_accepted_receipt_and_releases_snapshot_after_settlement() {
    let (node, m) = setup();
    let owner = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let client = node.client();
    wait(
        client
            .force_push(Arc::new(m.transactions[0].clone()))
            .unwrap(),
    );
    let mut p = Packing::start(
        prepare(&node, &owner, &m, Default::default()),
        client.clone(),
        config(),
    )
    .unwrap();
    assert_eq!(p.step(), Ok(Progress::Waiting));
    assert!(p.in_flight());
    let submitted = client.info().submitted;
    let report = retire(p.cancel());
    assert_eq!(report.selection, Some(Ok(1)));
    assert_eq!(report.failure_reports, None);
    assert_eq!(client.info().submitted, submitted);
    assert_eq!(owner.live_snapshots(), 0);
}
#[test]
fn cancelled_pow_failure_report_is_recorded_once_without_replaying_the_mutation() {
    let (node, m) = setup();
    let owner = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let client = node.client();
    let bad = common::invalid_signature(&m.transactions[0]);
    wait(client.force_push(bad.clone()).unwrap());
    let before = node.db.view().unwrap().status();
    let mut p = Packing::start(
        prepare(&node, &owner, &m, Default::default()),
        client.clone(),
        config(),
    )
    .unwrap();
    until(
        &mut p,
        Progress::Applied {
            accepted: 0,
            rejected: 1,
        },
    );
    assert_eq!(p.step(), Ok(Progress::Waiting));
    assert!(p.in_flight());
    let submitted = client.info().submitted;
    let report = retire(p.cancel());
    assert_eq!(report.failure_reports, Some(Ok(vec![false])));
    assert_eq!(report.selection, None);
    assert_eq!(client.info().submitted, submitted);
    assert_eq!(wait(client.inspect(bad.hash()).unwrap()).failures, 1);
    assert_eq!(node.db.view().unwrap().status(), before);
    assert_eq!(owner.live_snapshots(), 0);
}
#[test]
fn foreign_pool_and_changed_binding_return_the_exact_original_pow_input() {
    let (node, m) = setup();
    let foreign = Node::new();
    let owner = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    for fault in 0..5 {
        let mut input = prepare(&node, &owner, &m, Default::default());
        let original = bytes(&input);
        match fault {
            1 => input.value.source_id += 1,
            2 => input.metadata.status.revision += 1,
            3 => input.value.status.revision += 1,
            4 => Arc::make_mut(&mut input.value.reader.profile).network = NetworkProfile::uat(),
            _ => (),
        }
        let p = if fault == 0 {
            foreign.client()
        } else {
            node.client()
        };
        let submitted = p.info().submitted;
        let failure = Packing::start(input, p.clone(), config()).err().unwrap();
        assert_eq!(failure.error, packing::Error::Binding);
        assert_eq!(bytes(&failure.input), original);
        assert_eq!(p.info().submitted, submitted);
        assert_eq!(owner.live_snapshots(), 1);
        drop(failure);
        assert_eq!(owner.live_snapshots(), 0);
    }
}
#[test]
fn stopped_pow_pool_is_not_replaced_or_replayed_by_a_new_worker() {
    let (mut node, m) = setup();
    let owner = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let client = node.client();
    let input = prepare(&node, &owner, &m, Default::default());
    node.stop_worker();
    let mut p = Packing::start(input, client.clone(), config()).unwrap();
    let error = packing::Error::Pool {
        stage: packing::Stage::Selection,
        error: worker::Error::Stopped,
    };
    assert_eq!(p.step(), Err(error.clone()));
    assert_eq!(p.retry(), Err(error.clone()));
    let replacement = worker::Worker::spawn(
        node.db.clone(),
        NetworkProfile::mainnet(),
        pool::Config::default(),
        worker::Settings {
            origins_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(p.step(), Err(error));
    assert_eq!(replacement.info().submitted, 0);
    let report = retire(p.cancel());
    assert_eq!(report.selection, None);
    replacement.shutdown().unwrap();
}
#[test]
fn expired_pow_candidate_finishes_empty_without_submitting_pool_work() {
    let (node, m) = setup();
    let owner = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let client = node.client();
    wait(
        client
            .force_push(Arc::new(m.transactions[0].clone()))
            .unwrap(),
    );
    let input = prepare(&node, &owner, &m, Default::default());
    let original = bytes(&input);
    let submitted = client.info().submitted;
    let mut p = Packing::start(
        input,
        client.clone(),
        packing::Config {
            timeout: core::Timeout::Milliseconds(1),
            window_ms: 1,
        },
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(p.step(), Ok(Progress::Finished));
    assert!(p.expired());
    assert_eq!(bytes(&p.finish().unwrap()), original);
    assert_eq!(client.info().submitted, submitted);
}
#[test]
fn execution_resource_failure_retains_selection_and_never_rerequests_or_reports_it() {
    let (node, m) = setup();
    let owner = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let client = node.client();
    wait(
        client
            .force_push(Arc::new(m.transactions[0].clone()))
            .unwrap(),
    );
    let mut p = Packing::start(
        prepare(
            &node,
            &owner,
            &m,
            core::Limits {
                offered: 0,
                ..Default::default()
            },
        ),
        client.clone(),
        config(),
    )
    .unwrap();
    until(&mut p, Progress::Selected);
    let submitted = client.info().submitted;
    let error = p.step().unwrap_err();
    assert!(matches!(
        error,
        packing::Error::Execution {
            retryable: true,
            ..
        }
    ));
    p.retry().unwrap();
    assert_eq!(p.step(), Err(error));
    assert_eq!(client.info().submitted, submitted);
    let report = retire(p.cancel());
    assert_eq!(report.failure_reports, None);
    assert_eq!(report.discarded_failures, 0);
    assert_eq!(owner.live_snapshots(), 0);
}

#[test]
fn consumed_pow_selection_failure_keeps_the_first_result_through_pool_join() {
    let (mut node, m) = setup();
    let owner = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let mut early = prepare(&node, &owner, &m, Default::default());
    insert(&node, &Block::Micro(m.clone()));
    let mut next = m.clone();
    next.header.common.previous_hash = Block::Micro(m.clone()).hash().unwrap().0;
    next.header.time += 3000;
    let later = prepare(&node, &owner, &next, Default::default());
    // Deliberately mismatched candidate/reader: the accepted pool request must
    // return Missing from the old immutable view, never read today's database.
    early.value.candidate = later.value.candidate;
    drop(later.value.reader);
    let client = node.client();
    let mut p = Packing::start(early, client.clone(), config()).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    let error = loop {
        assert!(Instant::now() < end);
        match p.step() {
            Err(e) => break e,
            Ok(_) => std::thread::yield_now(),
        }
    };
    assert!(matches!(
        error,
        packing::Error::Pool {
            stage: packing::Stage::Selection,
            error: worker::Error::Service(pool::Failure::Repository(repo::Error::Missing(_)))
        }
    ));
    let mut cancelled = p.cancel();
    assert_eq!(cancelled.step(), Err(error.clone()));
    node.stop_worker();
    assert!(cancelled.retire_pool(&client).unwrap());
    assert_eq!(cancelled.step(), Err(error));
    drop(cancelled);
    assert_eq!(owner.live_snapshots(), 0);
}
