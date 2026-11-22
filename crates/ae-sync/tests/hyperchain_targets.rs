mod common;
use ae_chain::{block::Block, repository as repo};
use ae_hyperchain::parent_cache::runtime::{TargetError, Targets};
use ae_storage::{hyperchain, snapshots};
use ae_sync::chain::hyperchain::{eoe, targets::Live};
use ae_types::{Hash256, Unsigned};
use ae_vm_fate::maps::order::Otp26;
use common::hyperchain::{DATA, Node, Parent};
use num_bigint::BigInt;
use std::sync::Arc;

fn make_source(node: &Node, limits: snapshots::Config) -> Arc<eoe::Source> {
    eoe::Source::new(
        node.db.clone(),
        node.config.identity.clone(),
        node.config.profile.network.clone(),
        limits,
    )
    .unwrap()
}
fn targets(height: u64) -> Vec<BigInt> {
    // The frozen native chain emits epoch 2 at key 3 and epoch 3 at key 6.
    // aec_consensus_hc:target_parent_heights queries those key states, including
    // genesis epoch 1; the negative first target is intentional.
    match height {
        0..=2 => vec![(-4).into(), 16.into()],
        3..=5 => vec![1.into(), 21.into()],
        _ => vec![6.into(), 26.into()],
    }
}
fn assert_history(live: &mut Live<Otp26>, height: u64) {
    for h in 0..=height {
        assert_eq!(live.targets(&h.into()), Ok(targets(h)), "height {h}");
    }
    assert_eq!(live.targets(&(height + 1).into()), Err(TargetError::Native));
}

#[test]
fn live_targets_follow_native_epoch_states_and_keep_historical_protocols() {
    for profile in ["profile", "repeat_profile"] {
        for index in 0..7 {
            let node = Node::from_profile(&DATA, index, profile);
            let source = make_source(&node, Default::default());
            let mut live = Live::new(&source, &node.config).unwrap();
            let owner =
                hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default())
                    .unwrap();
            let mut height = 0;
            assert_history(&mut live, height);
            for b in node.blocks() {
                owner
                    .insert(&b, 1_000_000, repo::ledger::Origin::Gossip)
                    .unwrap();
                if let Block::Key(k) = b {
                    height = k.common.height;
                }
                assert_history(&mut live, height);
                assert_eq!(source.snapshots().live_snapshots(), 0);
            }
        }
    }
}

#[test]
fn rollback_and_reopen_replace_query_state_without_retaining_deleted_heights() {
    let node = Node::new(6);
    let source = make_source(&node, Default::default());
    let mut live = Live::new(&source, &node.config).unwrap();
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    let blocks = node.blocks();
    for b in &blocks {
        owner
            .insert(b, 1_000_000, repo::ledger::Origin::Gossip)
            .unwrap();
    }
    assert_history(&mut live, 7);
    node.db
        .rollback(
            repo::rollback::Target::Key(blocks[5].hash().unwrap()),
            Default::default(),
        )
        .unwrap();
    assert_history(&mut live, 3);
    assert_eq!(live.targets(&7.into()), Err(TargetError::Native));
    drop((source, owner));
    assert_eq!(live.targets(&0.into()), Err(TargetError::Unavailable));
    let node = node.reopen();
    let replacement = make_source(&node, Default::default());
    let mut current = Live::new(&replacement, &node.config).unwrap();
    assert_history(&mut current, 3);
    assert_eq!(live.targets(&3.into()), Err(TargetError::Unavailable));
}

#[test]
fn callback_shares_snapshot_quota_and_releases_it_after_success_and_failure() {
    let node = Node::new(0);
    let source = make_source(
        &node,
        snapshots::Config {
            maximum_snapshots: 1,
            ..Default::default()
        },
    );
    let mut live = Live::new(&source, &node.config).unwrap();
    let held = source.capture().unwrap();
    assert_eq!(live.targets(&0.into()), Err(TargetError::Unavailable));
    drop(held);
    assert_eq!(live.targets(&0.into()), Ok(targets(0)));
    assert_eq!(live.targets(&99.into()), Err(TargetError::Native));
    let huge = Unsigned::from_field_bytes(&[1; 9]).unwrap();
    assert_eq!(live.targets(&huge), Err(TargetError::Native));
    assert_eq!(source.snapshots().live_snapshots(), 0);
    let before = node.db.view().unwrap().status();
    let mut limited = node.config.clone();
    Arc::make_mut(&mut limited.profile).maximum_steps = 0;
    assert_eq!(
        Live::new(&source, &limited).unwrap().targets(&0.into()),
        Err(TargetError::Unavailable)
    );
    assert_eq!(source.snapshots().live_snapshots(), 0);
    assert_eq!(live.targets(&0.into()), Ok(targets(0)));
    assert_eq!(node.db.view().unwrap().status(), before);
}

#[test]
fn retained_provider_does_not_keep_retired_snapshot_authority_or_rebind_to_replacement() {
    let node = Node::new(0);
    let old = make_source(&node, Default::default());
    let mut live = Live::new(&old, &node.config).unwrap();
    let mut cloned = live.clone();
    let mut wrong = node.config.clone();
    wrong.identity.genesis = Hash256::ZERO;
    assert!(matches!(Live::new(&old, &wrong), Err(eoe::Error::Identity)));
    drop(old);
    let current = make_source(&node, Default::default());
    assert_eq!(live.targets(&0.into()), Err(TargetError::Unavailable));
    assert_eq!(cloned.targets(&0.into()), Err(TargetError::Unavailable));
    assert_eq!(
        Live::new(&current, &node.config)
            .unwrap()
            .targets(&0.into()),
        Ok(targets(0))
    );
}

#[test]
fn community_fork_queries_resolve_the_requested_height_from_committed_votes() {
    let node = Node::with_fork(
        0,
        ae_protocol::signaling::Fork {
            start: 1,
            end: 3,
            threshold: 1,
            info: 987654,
            version: 9,
        },
    );
    let source = make_source(&node, Default::default());
    let mut live = Live::new(&source, &node.config).unwrap();
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    for b in node.blocks() {
        owner
            .insert(&b, 1_000_000, repo::ledger::Origin::Gossip)
            .unwrap();
    }
    assert_history(&mut live, 7);
    assert_eq!(source.snapshots().live_snapshots(), 0);
}
