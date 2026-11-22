mod common;
#[path = "hyperchain_eoe/owner.rs"]
mod owner;
use ae_chain::{
    block::Block,
    publication,
    repository::{self as repo, ledger::Origin},
};
use ae_codec::headers::Header;
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{
    eoe::{self, coordinator, finalization, local_host, runtime},
    parent_block::ParentBlock,
};
use ae_mempool::hc_votes;
use ae_storage::{hyperchain, snapshots};
use ae_sync::chain::hyperchain::eoe as live;
use ae_tx::{ContractOperation, SignedTransaction, Transaction, TransactionLeaf};
use ae_types::Unsigned;
use common::hyperchain::{Node, Parent};
use std::{
    collections::BTreeMap,
    sync::Arc,
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
fn config(node: &Node) -> live::Config {
    live::Config {
        host: local_host::Config {
            network: node.config.profile.network.clone(),
            election_contract: node.config.profile.contracts.election,
            parent_epoch_length: 5.into(),
            parent_start_height: 1.into(),
            limits: Default::default(),
        },
        block_time: 10000.into(),
        coordinator: Default::default(),
        runtime: Default::default(),
        epochs: Default::default(),
    }
}
fn source(node: &Node) -> Arc<live::Source> {
    live::Source::new(
        node.db.clone(),
        node.config.identity.clone(),
        node.config.profile.network.clone(),
        Default::default(),
    )
    .unwrap()
}
fn connect(source: &Arc<live::Source>, node: &Node) -> live::Connected {
    source
        .connect(
            config(node),
            BTreeMap::new(),
            node.config.profile.order.clone(),
        )
        .unwrap()
}
fn call(
    client: &live::Client,
    command: runtime::Command<live::StateReader, live::StateReader>,
) -> runtime::Call {
    match client.call(command) {
        Ok(c) => c,
        Err(_) => panic!("command refused"),
    }
}
fn apply(
    client: &live::Client,
    command: runtime::Command<live::StateReader, live::StateReader>,
) -> runtime::Applied {
    call(client, command)
        .wait(Duration::from_secs(5))
        .unwrap()
        .unwrap()
}
fn stats(client: &live::Client) -> runtime::VoteSnapshot {
    let runtime::Applied::Votes(s) = apply(
        client,
        runtime::Command::Votes(runtime::VoteQuery {
            epoch: 7.into(),
            kind: None,
            maximum: 0,
        }),
    ) else {
        panic!()
    };
    s
}
fn drive(epochs: &mut live::Epochs, revision: u64) {
    let start = Instant::now();
    while epochs.cursor().revision() < revision {
        assert!(start.elapsed() < Duration::from_secs(5));
        match epochs.step().unwrap() {
            live::Progress::Waiting | live::Progress::Backpressured => {
                std::thread::sleep(Duration::from_millis(1))
            }
            _ => (),
        }
    }
}
fn vote(
    node: &Node,
    mode: eoe::Mode,
    kind: eoe::VoteType,
    n: u64,
) -> SignedTransaction<Transaction> {
    let fields: eoe::Payload = [
        (
            eoe::payload::HEIGHT.to_vec(),
            eoe::Field::Integer(50.into()),
        ),
        match mode {
            eoe::Mode::Fork => (
                eoe::payload::FORK_HASH.to_vec(),
                eoe::Field::Binary(key(9).to_vec()),
            ),
            eoe::Mode::Length => (
                eoe::payload::LENGTH_DELTA.to_vec(),
                eoe::Field::Integer(0.into()),
            ),
        },
    ]
    .into();
    let k = signer(n);
    let payload = eoe::payload::create_payload(&fields, &k, Default::default()).unwrap();
    eoe::payload::create_transaction(
        k.public_key(),
        7.into(),
        mode.type_number(kind),
        &payload,
        &k,
        &node.config.profile.network,
        Default::default(),
    )
    .unwrap()
}
fn admit(client: &live::Client, signed: SignedTransaction<Transaction>) {
    assert!(matches!(
        apply(
            client,
            runtime::Command::Admit {
                signed: Box::new(signed),
                event: hc_votes::Event::Received
            }
        ),
        runtime::Applied::Admission(hc_votes::PushOutcome::Accepted { .. })
    ));
}
fn inserted(node: &Node) -> hyperchain::Owner<ae_vm_fate::maps::order::Otp26, Parent> {
    hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap()
}

#[test]
fn committed_native_epochs_keep_original_origin_and_pool_clearing_rules() {
    for origin in [Origin::Gossip, Origin::Created, Origin::Sync] {
        let node = Node::new(0);
        let source = source(&node);
        let live::Connected {
            runtime,
            mut epochs,
        } = connect(&source, &node);
        let client = runtime.client();
        let insert = inserted(&node);
        admit(
            &client,
            vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13),
        );
        let mut expected_epoch = Unsigned::from(0u64);
        let mut entries = 1;
        let mut notifications = 0;
        for (i, b) in node.blocks().iter().enumerate() {
            let cursor = node.db.events().cursor();
            let committed = insert.insert(b, 1_000_000, origin).unwrap();
            node.check(i);
            for effect in node.db.events().after(&cursor, 1).unwrap().records[0].effects() {
                if let publication::Effect::Consensus {
                    event: ae_hyperchain::block::Event::NewEpoch(info),
                    ..
                } = effect
                {
                    let ae_vm_fate::value::Value::Integer(n) = &info.epoch else {
                        panic!()
                    };
                    let next = Unsigned::from_field_bytes(&n.to_bytes_be().1).unwrap();
                    if expected_epoch != 0.into() && next > expected_epoch {
                        entries = 0;
                    }
                    expected_epoch = next;
                    notifications += 1;
                }
            }
            drive(&mut epochs, committed.change().revision);
            let pool = stats(&client);
            assert_eq!(pool.epoch(), &expected_epoch);
            assert_eq!(pool.entries(), entries);
            let current = source.capture().unwrap();
            assert_eq!(current.metadata.status, node.db.view().unwrap().status());
            assert_eq!(
                current.tip.trees.unwrap().roots().hash().0,
                b.header().common().state_hash
            );
        }
        assert_eq!(epochs.acknowledged(), notifications);
        assert_eq!(
            notifications,
            if matches!(origin, Origin::Sync) { 0 } else { 2 }
        );
        drop(epochs);
        assert!(client.is_closed());
        runtime.shutdown().unwrap();
        assert_eq!(source.snapshots().live_snapshots(), 0);
    }
}

fn alternative(block: &Block) -> Block {
    let Block::Key(mut header) = block.clone() else {
        panic!()
    };
    header.nonce += 1;
    let signer = (12..=14)
        .map(signer)
        .find(|k| k.public_key().0 == header.miner)
        .unwrap();
    let signature = signer.sign(&Header::Key(header.clone()).signature_bytes().unwrap());
    for (word, bytes) in header.seal[..16]
        .iter_mut()
        .zip(signature.0.chunks_exact(4))
    {
        *word = u32::from_be_bytes(bytes.try_into().unwrap());
    }
    Block::Key(header)
}
#[test]
fn signed_side_epoch_variants_update_pool_without_replacing_selected_state() {
    let native: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/erlang_hc_committed_eoe.json")).unwrap();
    let node = Node::new(0);
    let source = source(&node);
    let live::Connected {
        runtime,
        mut epochs,
    } = connect(&source, &node);
    let client = runtime.client();
    let insert = inserted(&node);
    let blocks = node.blocks();
    for b in &blocks {
        let c = insert.insert(b, 1_000_000, Origin::Gossip).unwrap();
        drive(&mut epochs, c.change().revision);
    }
    let selected = node.db.view().unwrap().status().tip;
    admit(
        &client,
        vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13),
    );
    for ((index, expected_epoch, expected_entries), expected) in [(5, 2, 1), (10, 3, 0)]
        .into_iter()
        .zip(native["branches"].as_array().unwrap())
    {
        let side = alternative(&blocks[index]);
        assert_eq!(hex::encode(side.encode().unwrap()), expected["block"]);
        assert_eq!(hex::encode(side.hash().unwrap().0), expected["hash"]);
        assert_eq!(
            hex::encode(side.header().common().state_hash),
            expected["state_hash"]
        );
        assert_eq!(hex::encode(selected.unwrap().0), expected["selected_hash"]);
        assert_eq!(expected["keep_selected"], true);
        let native_epoch = expected["events"][1]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e[1][0] == serde_json::json!(["atom", "new_epoch"]))
            .unwrap()[1][1][1]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e[0] == serde_json::json!(["atom", "epoch"]))
            .unwrap()[1][1]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert_eq!(native_epoch, expected_epoch as u64);
        let c = insert.insert(&side, 1_000_000, Origin::Gossip).unwrap();
        assert_eq!(c.change().old_tip, selected);
        assert_eq!(Some(c.change().new_tip), selected);
        assert_eq!(
            node.db
                .view()
                .unwrap()
                .require_record(side.hash().unwrap())
                .unwrap()
                .roots
                .state
                .hash()
                .0,
            side.header().common().state_hash
        );
        drive(&mut epochs, c.change().revision);
        let pool = stats(&client);
        assert_eq!(pool.epoch(), &Unsigned::from(expected_epoch as u64));
        assert_eq!(pool.entries(), expected_entries);
        assert_eq!(source.capture().unwrap().metadata.status.tip, selected);
    }
    assert_eq!(epochs.acknowledged(), 4);
    drop(epochs);
    runtime.shutdown().unwrap();
}

#[test]
fn rollback_and_feed_gap_close_the_fixed_runtime_and_restart_from_a_fresh_cursor() {
    for gap in [false, true] {
        let node = Node::with_events(ae_storage::events::Config {
            records: if gap { 1 } else { 1024 },
            ..Default::default()
        });
        let source = source(&node);
        let live::Connected {
            runtime,
            mut epochs,
        } = connect(&source, &node);
        let client = runtime.client();
        let insert = inserted(&node);
        let blocks = node.blocks();
        assert!(matches!(
            source.connect(
                config(&node),
                BTreeMap::new(),
                node.config.profile.order.clone()
            ),
            Err(live::Error::AlreadyRunning)
        ));
        for b in &blocks[..3] {
            let c = insert.insert(b, 1_000_000, Origin::Gossip).unwrap();
            if !gap {
                drive(&mut epochs, c.change().revision);
            }
        }
        if !gap {
            node.db
                .rollback(
                    repo::rollback::Target::Micro(blocks[0].hash().unwrap()),
                    Default::default(),
                )
                .unwrap();
        }
        let error = epochs.step().unwrap_err();
        assert!(matches!(
            (&error, gap),
            (
                live::Error::Source(ae_storage::events::Error::Gap { .. }),
                true
            ) | (live::Error::Rollback { .. }, false)
        ));
        assert!(client.is_closed());
        assert_eq!(epochs.retry(Default::default()), Err(error));
        runtime.shutdown().unwrap();
        let live::Connected {
            runtime: new_runtime,
            epochs: new_epochs,
        } = connect(&source, &node);
        assert_eq!(new_epochs.cursor(), &node.db.events().cursor());
        assert_eq!(stats(&new_runtime.client()).epoch(), &0.into());
        assert!(client.is_closed());
        assert!(epochs.step().is_err());
        let cursor = new_epochs.cursor().clone();
        drop((epochs, new_epochs));
        new_runtime.shutdown().unwrap();
        drop((source, insert));
        let node = node.reopen();
        assert!(matches!(
            node.db.events().after(&cursor, 1),
            Err(ae_storage::events::Error::WrongSource)
        ));
        let replacement = live::Source::new(
            node.db.clone(),
            node.config.identity.clone(),
            node.config.profile.network.clone(),
            Default::default(),
        )
        .unwrap();
        let live::Connected { runtime, epochs } = connect(&replacement, &node);
        assert_ne!(epochs.cursor(), &cursor);
        assert_eq!(epochs.cursor(), &node.db.events().cursor());
        assert!(client.is_closed());
        drop(epochs);
        runtime.shutdown().unwrap();
    }
}

#[test]
fn epoch_rejection_retries_the_same_event_and_unread_receipts_apply_backpressure() {
    let node = Node::new(0);
    let source = source(&node);
    let mut cfg = config(&node);
    cfg.runtime.command_capacity = 1;
    cfg.coordinator.maximum_queued_events = 1;
    let live::Connected {
        runtime,
        mut epochs,
    } = source
        .connect(cfg, BTreeMap::new(), node.config.profile.order.clone())
        .unwrap();
    let client = runtime.client();
    let insert = inserted(&node);
    for b in node.blocks().iter().take(6) {
        insert.insert(b, 1_000_000, Origin::Gossip).unwrap();
    }
    let start = Instant::now();
    while epochs.step().unwrap() != live::Progress::Loaded {
        assert!(start.elapsed() < Duration::from_secs(5));
    }
    let cursor = epochs.cursor().clone();
    let held = call(
        &client,
        runtime::Command::Votes(runtime::VoteQuery {
            epoch: 7.into(),
            kind: None,
            maximum: 0,
        }),
    );
    assert_eq!(epochs.step(), Ok(live::Progress::Backpressured));
    assert_eq!(epochs.cursor(), &cursor);
    held.wait(Duration::from_secs(5)).unwrap().unwrap();
    assert_eq!(epochs.step(), Ok(live::Progress::Submitted));
    let start = Instant::now();
    loop {
        match epochs.step() {
            Ok(live::Progress::Waiting) => {
                assert!(start.elapsed() < Duration::from_secs(5));
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(live::Error::Rejected(coordinator::Error::ResourceLimit)) => break,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(epochs.cursor(), &cursor);
    assert_eq!(stats(&client).epoch(), &0.into());
    apply(&client, runtime::Command::Configure(Default::default()));
    epochs.retry(Default::default()).unwrap();
    drive(&mut epochs, node.db.view().unwrap().status().revision);
    assert_eq!(epochs.acknowledged(), 1);
    assert_eq!(stats(&client).epoch(), &2.into());
    let service = live::service::Service::spawn(epochs, Duration::from_millis(1)).unwrap();
    let epochs = service.shutdown().unwrap();
    assert_eq!(epochs.acknowledged(), 1);
    drop(epochs);
    runtime.shutdown().unwrap();
}

#[test]
fn pending_eoe_completion_reads_the_latest_committed_tip_despite_epoch_feed_backlog() {
    let node = Node::new(0);
    let source = source(&node);
    let k = signer(12);
    let leader = k.public_key();
    let live::Connected { runtime, epochs } = source
        .connect(
            config(&node),
            [(leader, k)].into(),
            node.config.profile.order.clone(),
        )
        .unwrap();
    let client = runtime.client();
    let caller = source.capture().unwrap().tip.trees.unwrap();
    let old_nonce = caller
        .accounts
        .lookup(leader)
        .unwrap()
        .unwrap()
        .nonce()
        .clone();
    apply(
        &client,
        runtime::Command::CacheParent(ParentBlock {
            hash: key(99).to_vec(),
            height: 31.into(),
            previous_hash: key(98).to_vec(),
            time: 12345.into(),
        }),
    );
    apply(
        &client,
        runtime::Command::Negotiate(eoe::Negotiation {
            epoch: 7.into(),
            height: 50.into(),
            hash: key(9).to_vec(),
            leader,
            validators: vec![
                (leader, 4.into()),
                (signer(13).public_key(), 3.into()),
                (signer(14).public_key(), 3.into()),
            ],
            seed: Some(key(8).to_vec()),
            current_length: 10.into(),
        }),
    );
    for _ in 0..2 {
        assert!(matches!(
            runtime.recv_timeout(Duration::from_secs(5)).unwrap(),
            runtime::Output::Service(coordinator::ServiceOutput::Publication(
                local_host::Output::Vote { .. }
            ))
        ));
    }
    apply(
        &client,
        runtime::Command::Finalize {
            request: 77,
            view: caller.clone(),
        },
    );
    let insert = inserted(&node);
    for b in node.blocks() {
        insert.insert(&b, 1_000_000, Origin::Gossip).unwrap();
    }
    let current = source.capture().unwrap();
    let trees = current.tip.trees.unwrap();
    assert_ne!(
        &old_nonce,
        trees.accounts.lookup(leader).unwrap().unwrap().nonce()
    );
    assert!(epochs.cursor().revision() < current.metadata.status.revision);
    for mode in [eoe::Mode::Fork, eoe::Mode::Length] {
        for kind in [eoe::VoteType::Vote, eoe::VoteType::Commit] {
            admit(&client, vote(&node, mode, kind, 13));
        }
    }
    let replies = loop {
        match runtime.recv_timeout(Duration::from_secs(5)).unwrap() {
            runtime::Output::Service(coordinator::ServiceOutput::Finalized {
                request: 77,
                replies,
            }) => break replies,
            runtime::Output::Service(coordinator::ServiceOutput::Publication(_)) => (),
            other => panic!("{other:?}"),
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
            &trees,
            leader,
            node.config.profile.contracts.election,
            call_data.clone(),
            &(i as u64 + 1).into(),
        )
        .unwrap();
        assert_eq!(
            *signed.as_ref(),
            finalization::sign(expected, &signer(12), &node.config.profile.network)
        );
    }
    drop((epochs, caller, trees));
    runtime.shutdown().unwrap();
    assert_eq!(source.snapshots().live_snapshots(), 0);
}

#[test]
fn cache_hit_bypasses_live_state_and_failed_capture_keeps_vote_admission_retryable() {
    let node = Node::new(0);
    let source = live::Source::new(
        node.db.clone(),
        node.config.identity.clone(),
        node.config.profile.network.clone(),
        snapshots::Config {
            maximum_snapshots: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let live::Connected { runtime, epochs } = connect(&source, &node);
    let client = runtime.client();
    let cached = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13);
    let cache = ae_storage::pool::worker::GossipCache::new(16, 64).unwrap();
    assert!(!cache.check(cached.hash()));
    apply(
        &client,
        runtime::Command::LiveGossipCache(Some(Arc::new(cache.clone()))),
    );
    let held = source.capture().unwrap();
    assert!(matches!(
        apply(
            &client,
            runtime::Command::Admit {
                signed: Box::new(cached),
                event: hc_votes::Event::Received
            }
        ),
        runtime::Applied::Admission(hc_votes::PushOutcome::Cached)
    ));
    let cache_before = cache.stats().unwrap();
    let signed = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 14);
    let result = call(
        &client,
        runtime::Command::Admit {
            signed: Box::new(signed.clone()),
            event: hc_votes::Event::Received,
        },
    )
    .wait(Duration::from_secs(5))
    .unwrap();
    assert_eq!(
        result,
        Err(coordinator::Error::Host(local_host::Error::SelectedState(
            local_host::SelectedError::ResourceLimit
        )))
    );
    assert_eq!(stats(&client).entries(), 0);
    assert_eq!(cache.stats().unwrap(), cache_before);
    drop(held);
    admit(&client, signed);
    assert_eq!(stats(&client).entries(), 1);
    drop(epochs);
    runtime.shutdown().unwrap();
}

#[test]
fn lost_epoch_receipt_is_quarantined_and_cannot_be_rebound_to_replacement() {
    let node = Node::new(0);
    let source = source(&node);
    let mut cfg = config(&node);
    cfg.runtime.output_capacity = 1;
    let live::Connected {
        runtime,
        mut epochs,
    } = source
        .connect(cfg, BTreeMap::new(), node.config.profile.order.clone())
        .unwrap();
    let client = runtime.client();
    let insert = inserted(&node);
    for b in node.blocks().iter().take(6) {
        insert.insert(b, 1_000_000, Origin::Gossip).unwrap();
    }
    let start = Instant::now();
    while epochs.step().unwrap() != live::Progress::Loaded {
        assert!(start.elapsed() < Duration::from_secs(5));
    }
    admit(
        &client,
        vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13),
    );
    admit(
        &client,
        vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 14),
    );
    assert_eq!(epochs.step(), Ok(live::Progress::Submitted));
    assert_eq!(epochs.step(), Ok(live::Progress::Waiting));
    client.request_stop();
    assert_eq!(epochs.step(), Err(live::Error::ReceiptLost));
    assert!(epochs.in_flight());
    assert_eq!(
        epochs.retry(Default::default()),
        Err(live::Error::ReceiptLost)
    );
    runtime.shutdown().unwrap();
    let live::Connected {
        runtime: new_runtime,
        epochs: new_epochs,
    } = connect(&source, &node);
    assert_eq!(epochs.step(), Err(live::Error::ReceiptLost));
    assert_eq!(stats(&new_runtime.client()).epoch(), &0.into());
    drop((epochs, new_epochs));
    new_runtime.shutdown().unwrap();
}

#[tokio::test]
async fn joined_epoch_worker_drives_the_existing_shared_vote_authority() {
    use ae_sync::votes;
    let node = Node::new(0);
    let source = source(&node);
    let live::Connected { runtime, epochs } = connect(&source, &node);
    let publications = votes::Publications::new(&node.db, Default::default()).unwrap();
    let mut owner = match votes::eoe::Owner::new(runtime, publications.clone()) {
        Ok(o) => o,
        Err(_) => panic!("output claim"),
    };
    let worker = votes::Worker::attach_eoe(
        node.db.clone(),
        Default::default(),
        &owner,
        Default::default(),
    )
    .unwrap();
    let client = worker.client();
    let mut events = publications.listen().unwrap();
    let epochs = live::service::Service::spawn(epochs, Duration::from_millis(1)).unwrap();
    let signed = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13);
    assert!(matches!(
        common::within(
            client
                .push(signed.encode(), hc_votes::Event::Received)
                .unwrap()
        )
        .await
        .unwrap(),
        hc_votes::PushOutcome::Accepted { .. }
    ));
    let start = Instant::now();
    loop {
        match owner.route().unwrap() {
            votes::eoe::Routed::Idle => {
                assert!(start.elapsed() < Duration::from_secs(5));
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            votes::eoe::Routed::Published => break,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(
        common::within(events.recv())
            .await
            .unwrap()
            .transaction
            .as_ref(),
        &signed
    );
    let insert = inserted(&node);
    let blocks = node.blocks();
    for (range, expected_epoch, entries) in [(0..6, 2, 1), (6..13, 3, 0)] {
        for b in &blocks[range] {
            insert.insert(b, 1_000_000, Origin::Gossip).unwrap();
        }
        let revision = node.db.view().unwrap().status().revision;
        let start = Instant::now();
        while epochs.status().cursor.revision() < revision {
            assert!(epochs.status().failure.is_none());
            assert!(start.elapsed() < Duration::from_secs(5));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let pool = common::within(client.stats().unwrap()).await.unwrap();
        assert_eq!(pool.epoch, Unsigned::from(expected_epoch as u64));
        assert_eq!(pool.entries, entries);
    }
    let epoch_state = epochs.shutdown().unwrap();
    assert_eq!(epoch_state.acknowledged(), 2);
    drop(epoch_state);
    assert!(owner.client().is_closed());
    worker.shutdown().unwrap();
    owner.shutdown().unwrap();
    assert_eq!(source.snapshots().live_snapshots(), 0);
}
