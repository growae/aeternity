mod common;
use ae_chain::repository as repo;
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{
    eoe::{self, coordinator, finalization, local_host, runtime},
    parent_block::ParentBlock,
};
use ae_protocol::{ConsensusType, Protocol};
use ae_state::Trees;
use ae_storage::{hyperchain, snapshots};
use ae_tx::{CanonicalTransaction, ContractOperation, TransactionLeaf};
use ae_types::Unsigned;
use common::hyperchain::{Node, Parent};
use std::{sync::Arc, time::Duration};

type State = snapshots::Captured<(repo::Roots, repo::hyperchain::journal::Checkpoint)>;
fn capture(owner: &snapshots::Owner) -> State {
    owner
        .capture(|v| {
            let hash = v.status().tip.unwrap();
            let record = v.require_record(hash)?;
            let header = v.require_block(hash)?.header().clone();
            v.require_retained(header.common().height)?;
            assert_eq!(record.roots.state.hash().0, header.common().state_hash);
            Ok((record.roots, v.hyperchain_checkpoint()?))
        })
        .unwrap()
}
fn trees(capture: &State) -> Arc<Trees<snapshots::Reader, snapshots::Reader>> {
    Arc::new(capture.value.0.load(capture.reader().clone()).unwrap())
}
fn key(n: u64) -> [u8; 32] {
    let mut b = [0; 32];
    b[24..].copy_from_slice(&n.to_be_bytes());
    b
}

#[test]
fn retained_native_chain_snapshots_preserve_roots_nonces_and_cursor_through_rollback() {
    for profile in ["profile", "repeat_profile"] {
        // Both frozen native map profiles share the same signed canonical chain.
        let node = Node::from_profile(&common::hyperchain::DATA, 0, profile);
        let owner = snapshots::Owner::start(
            node.db.clone(),
            snapshots::Config {
                maximum_snapshots: 32,
                ..Default::default()
            },
        )
        .unwrap();
        let execution =
            hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default())
                .unwrap();
        let mut retained = vec![capture(&owner)];
        for (i, block) in node.blocks().iter().enumerate() {
            execution
                .insert(block, 1_000_000, repo::ledger::Origin::Gossip)
                .unwrap();
            node.check(i);
            let state = capture(&owner);
            let events = node
                .db
                .events()
                .after(&retained.last().unwrap().metadata().cursor, 2)
                .unwrap();
            assert_eq!(events.records.len(), 1);
            assert_eq!(
                events.records[0].change().revision,
                state.metadata().status.revision
            );
            assert_eq!(events.cursor, state.metadata().cursor);
            assert_eq!(state.value.1.revision, state.metadata().status.revision);
            assert_eq!(
                trees(&state).roots().hash().0,
                block.header().common().state_hash
            );
            retained.push(state);
        }
        // Capture concrete account and full contract-store results before the
        // administrative rollback deletes blocks and changes selected state.
        let election = node.config.profile.contracts.election;
        let leader = Ed25519SecretKey::from_seed(&key(12)).public_key();
        let expected: Vec<_> = retained
            .iter()
            .map(|s| {
                finalization::transaction(&trees(s), leader, election, vec![1, 2, 3], &1.into())
                    .unwrap()
                    .encode()
            })
            .collect();
        assert!(
            expected.windows(2).any(|p| p[0] != p[1]),
            "native chain must advance the selected caller nonce"
        );
        let blocks = node.blocks();
        let target = blocks[0].hash().unwrap();
        node.db
            .rollback(repo::rollback::Target::Micro(target), Default::default())
            .unwrap();
        let after = capture(&owner);
        assert_eq!(after.metadata().status.tip, Some(target));
        for (s, bytes) in retained.iter().zip(expected) {
            assert_eq!(
                finalization::transaction(&trees(s), leader, election, vec![1, 2, 3], &1.into())
                    .unwrap()
                    .encode(),
                bytes
            );
            assert_eq!(trees(s).roots(), s.value.0.state);
        }
        let cursor = after.metadata().cursor.clone();
        drop((after, retained, owner, execution));
        let node = node.reopen();
        assert!(matches!(
            node.db.events().after(&cursor, 1),
            Err(ae_storage::events::Error::WrongSource)
        ));
        let owner = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
        assert_eq!(capture(&owner).metadata().status.tip, Some(target));
    }
}

fn apply(
    client: &runtime::Client<snapshots::Reader, snapshots::Reader>,
    command: runtime::Command<snapshots::Reader, snapshots::Reader>,
) {
    let call = match client.call(command) {
        Ok(c) => c,
        Err(_) => panic!("EOE command refused"),
    };
    assert_eq!(
        call.wait(Duration::from_secs(5)).unwrap().unwrap(),
        runtime::Applied::Unit
    );
}

#[test]
fn eoe_runtime_signs_from_owned_state_and_preserves_explicit_finalize_view() {
    let node = Node::new(0);
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let old = capture(&snapshots);
    let old_trees = trees(&old);
    let execution =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    for b in node.blocks() {
        execution
            .insert(&b, 1_000_000, repo::ledger::Origin::Gossip)
            .unwrap();
    }
    let current = capture(&snapshots);
    let current_trees = trees(&current);
    let keys: Vec<_> = (12..=14)
        .map(|n| Ed25519SecretKey::from_seed(&key(n)))
        .collect();
    let leader = keys[0].public_key();
    assert_ne!(
        old_trees.accounts.lookup(leader).unwrap().unwrap().nonce(),
        current_trees
            .accounts
            .lookup(leader)
            .unwrap()
            .unwrap()
            .nonce()
    );
    let mut host = local_host::LocalHost::new(
        local_host::Config {
            network: node.config.profile.network.clone(),
            election_contract: node.config.profile.contracts.election,
            parent_epoch_length: 5.into(),
            parent_start_height: 1.into(),
            limits: Default::default(),
        },
        local_host::SelectedTip {
            trees: Some(old_trees.clone()),
            protocol: Protocol::Ceres,
            consensus: ConsensusType::Pos,
        },
        (12..=14)
            .map(|n| {
                let k = Ed25519SecretKey::from_seed(&key(n));
                (k.public_key(), k)
            })
            .collect(),
    );
    host.insert_parent(ParentBlock {
        hash: key(99).to_vec(),
        height: 31.into(),
        previous_hash: Some(key(98).to_vec()),
        time: 12345.into(),
    })
    .unwrap();
    let service = runtime::Runtime::spawn(
        coordinator::Coordinator::new(host, 1000.into(), Default::default()),
        node.config.profile.order.clone(),
        Default::default(),
    )
    .unwrap();
    let client = service.client();
    apply(
        &client,
        runtime::Command::Negotiate(eoe::Negotiation {
            epoch: 7.into(),
            height: 50.into(),
            hash: key(9).to_vec(),
            leader,
            validators: keys
                .iter()
                .enumerate()
                .map(|(i, k)| {
                    (
                        k.public_key(),
                        Unsigned::from(if i == 0 { 4u64 } else { 3 }),
                    )
                })
                .collect(),
            seed: Some(key(8).to_vec()),
            current_length: 10.into(),
        }),
    );
    // Drain the generated proposals/votes/commits before requesting replies.
    let mut votes = 0;
    while votes < 10 {
        match service.recv_timeout(Duration::from_secs(5)).unwrap() {
            runtime::Output::Service(coordinator::ServiceOutput::Publication(
                local_host::Output::Vote { .. },
            )) => votes += 1,
            other => panic!("unexpected output: {other:?}"),
        }
    }
    apply(
        &client,
        runtime::Command::SelectTip(local_host::SelectedTip {
            trees: Some(current_trees.clone()),
            protocol: Protocol::Ceres,
            consensus: ConsensusType::Pos,
        }),
    );
    // A completed FSM uses the explicit caller view even after selected tip
    // changes. The node reader remains pinned on the service thread.
    for (request, view) in [(1, old_trees.clone()), (2, current_trees.clone())] {
        apply(
            &client,
            runtime::Command::Finalize {
                request,
                view: view.clone(),
            },
        );
        let replies = loop {
            match service.recv_timeout(Duration::from_secs(5)).unwrap() {
                runtime::Output::Service(coordinator::ServiceOutput::Finalized {
                    request: id,
                    replies,
                }) => {
                    assert_eq!(id, request);
                    break replies;
                }
                runtime::Output::Service(coordinator::ServiceOutput::Publication(
                    local_host::Output::ParentBlock { .. },
                )) => (),
                other => panic!("unexpected output: {other:?}"),
            }
        };
        for (i, reply) in replies.iter().enumerate() {
            let eoe::Reply::Finalized(local_host::Finalized::Signed(signed)) = reply else {
                panic!("{reply:?}")
            };
            let TransactionLeaf::Contract(tx) = signed.transaction().leaf() else {
                panic!()
            };
            let ContractOperation::Call { call_data, .. } = tx.operation() else {
                panic!()
            };
            let expected = finalization::transaction(
                &view,
                leader,
                node.config.profile.contracts.election,
                call_data.clone(),
                &Unsigned::from(i as u64 + 1),
            )
            .unwrap();
            assert_eq!(
                *signed.as_ref(),
                finalization::sign(expected, &keys[0], &node.config.profile.network)
            );
        }
    }
    drop((old, current, old_trees, current_trees, client));
    service.shutdown().unwrap();
    assert_eq!(snapshots.live_snapshots(), 0);
}
