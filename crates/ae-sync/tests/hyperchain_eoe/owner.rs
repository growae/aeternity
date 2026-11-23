#[path = "owner/candidate.rs"]
mod candidate;
#[path = "owner/conductor.rs"]
mod conductor;
#[path = "owner/producer.rs"]
mod producer;
#[path = "owner/production.rs"]
mod production;
#[path = "owner/recovery.rs"]
mod recovery;
use super::*;
use ae_storage::pool::worker::GossipCache;
use ae_sync::{chain::hyperchain::eoe::owner as managed, votes};

fn cfg(node: &Node) -> managed::Config {
    managed::Config {
        eoe: config(node),
        votes: Default::default(),
        pool: Default::default(),
        calls: Default::default(),
    }
}
fn inputs(node: &Node, source: &Arc<live::Source>, config: &managed::Config) -> managed::Inputs {
    managed::Inputs {
        source: source.clone(),
        database: node.db.clone(),
        publications: votes::Publications::new(&node.db, config.votes).unwrap(),
        gossip: GossipCache::new(16, 128).unwrap(),
        parent: None,
    }
}
fn start(
    node: &Node,
    source: &Arc<live::Source>,
    config: managed::Config,
    owned: &[u64],
) -> managed::Owner {
    managed::Owner::start(
        inputs(node, source, &config),
        config,
        owned
            .iter()
            .map(|n| {
                let k = signer(*n);
                (k.public_key(), k)
            })
            .collect(),
        node.config.profile.order.clone(),
    )
    .unwrap()
}
fn unit(call: runtime::Call) {
    assert_eq!(
        call.wait(Duration::from_secs(5)).unwrap().unwrap(),
        runtime::Applied::Unit
    );
}
fn negotiation() -> eoe::Negotiation {
    eoe::Negotiation {
        epoch: 7.into(),
        height: 50.into(),
        hash: key(9).to_vec(),
        leader: signer(12).public_key(),
        validators: [(12, 4), (13, 3), (14, 3)]
            .into_iter()
            .map(|(n, w)| (signer(n).public_key(), w.into()))
            .collect(),
        seed: Some(key(8).to_vec()),
        current_length: 10.into(),
    }
}
fn prepare(owner: &managed::Owner) {
    unit(
        owner
            .cache_parent(ParentBlock {
                hash: key(99).to_vec(),
                height: 31.into(),
                previous_hash: Some(key(98).to_vec()),
                time: 12345.into(),
            })
            .unwrap(),
    );
    unit(owner.negotiate(negotiation()).unwrap());
}
fn step(owner: &mut managed::Owner) {
    match owner.step().unwrap() {
        managed::Progress::Output(runtime::Output::Service(
            coordinator::ServiceOutput::Publication(local_host::Output::ParentBlock { .. }),
        )) => (),
        managed::Progress::Output(other) => panic!("unexpected supervision output {other:?}"),
        _ => (),
    }
}
fn until(owner: &mut managed::Owner, mut ready: impl FnMut(&managed::Owner) -> bool) {
    let start = Instant::now();
    while !ready(owner) {
        assert!(start.elapsed() < Duration::from_secs(5));
        step(owner);
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn complete(owner: &mut managed::Owner, request: &mut managed::Request) -> managed::Completion {
    let start = Instant::now();
    loop {
        if let Some(reply) = request.try_reply().unwrap() {
            return reply;
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        step(owner);
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn composed_finalization_retains_unread_and_cloned_result_capacity_after_shutdown() {
    let node = Node::new(0);
    let source = source(&node);
    let mut config = cfg(&node);
    config.calls.requests = 1;
    let mut owner = start(&node, &source, config, &[12, 13, 14]);
    prepare(&owner);
    let mut request = owner.finalize().unwrap();
    assert_eq!(
        request.captured().status.tip,
        node.db.view().unwrap().status().tip
    );
    assert!(matches!(
        owner.finalize(),
        Err(managed::Error::Backpressured)
    ));
    until(&mut owner, |o| o.pending_requests() == 0);
    assert_eq!(owner.retained_requests(), 1);
    assert!(owner.retained_reply_bytes() > 0);
    assert_eq!(source.snapshots().live_snapshots(), 0);
    let result = request.try_reply().unwrap().unwrap();
    let trees = source.capture().unwrap().tip.trees.unwrap();
    for (i, reply) in result.replies().iter().enumerate() {
        let eoe::Reply::Finalized(local_host::Finalized::Signed(signed)) = reply else {
            panic!("{reply:?}");
        };
        let TransactionLeaf::Contract(tx) = signed.transaction().leaf() else {
            panic!()
        };
        let ContractOperation::Call { call_data, .. } = tx.operation() else {
            panic!()
        };
        let expected = finalization::transaction(
            &trees,
            signer(12).public_key(),
            node.config.profile.contracts.election,
            call_data.clone(),
            &(i as u64 + 1).into(),
        )
        .unwrap();
        assert_eq!(
            **signed,
            finalization::sign(expected, &signer(12), &node.config.profile.network)
        );
    }
    drop(trees);
    drop(request);
    let retained = result.clone();
    drop(result);
    assert_eq!(owner.retained_requests(), 1);
    assert!(matches!(
        owner.finalize(),
        Err(managed::Error::Backpressured)
    ));
    let report = owner.shutdown();
    assert!(report.errors.is_empty());
    assert!(report.abandoned_requests.is_empty());
    assert_eq!(source.snapshots().live_snapshots(), 0);
    assert!(matches!(
        &retained.replies()[0],
        eoe::Reply::Finalized(local_host::Finalized::Signed(_))
    ));
    // Old retained results neither bind nor block a new runtime incarnation.
    let new = start(&node, &source, cfg(&node), &[]);
    assert_eq!(new.retained_requests(), 0);
    drop(retained);
    assert!(new.shutdown().errors.is_empty());
}

#[test]
fn native_epoch_reset_preserves_waiting_caller_until_explicit_cancel_or_drop() {
    let node = Node::new(0);
    let source = source(&node);
    let mut owner = start(&node, &source, cfg(&node), &[12]);
    prepare(&owner);
    let mut request = owner.finalize().unwrap();
    // A later command receipt fences the initial GetFinalize callback.
    assert!(matches!(
        owner
            .entropy(9.into())
            .unwrap()
            .wait(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        runtime::Applied::Entropy(Some(_))
    ));
    let insert = inserted(&node);
    for block in node.blocks().iter().take(6) {
        insert.insert(block, 1_000_000, Origin::Gossip).unwrap();
    }
    let revision = node.db.events().cursor().revision();
    until(&mut owner, |o| o.epoch_cursor().revision() == revision);
    assert_eq!(owner.acknowledged_epochs(), 1);
    assert!(request.try_reply().unwrap().is_none());
    assert_eq!(owner.pending_requests(), 1);
    assert_eq!(source.snapshots().live_snapshots(), 1);
    request.cancel();
    until(&mut owner, |o| o.pending_requests() == 0);
    assert_eq!(
        request.try_reply().unwrap_err(),
        managed::RequestError::Failed(coordinator::CallFailure::Cancelled)
    );
    drop(request);
    assert_eq!(owner.retained_requests(), 0);
    assert_eq!(source.snapshots().live_snapshots(), 0);
    prepare(&owner);
    let request = owner.finalize().unwrap();
    assert!(matches!(
        owner
            .entropy(9.into())
            .unwrap()
            .wait(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        runtime::Applied::Entropy(Some(_))
    ));
    drop(request);
    until(&mut owner, |o| o.pending_requests() == 0);
    assert_eq!(owner.retained_requests(), 0);
    assert_eq!(source.snapshots().live_snapshots(), 0);
    assert!(owner.shutdown().errors.is_empty());
}

#[test]
fn rejected_finalize_and_oversized_result_have_distinct_known_and_unknown_outcomes() {
    let node = Node::new(0);
    let source = source(&node);
    let mut config = cfg(&node);
    config.eoe.coordinator.maximum_requests = 0;
    let mut owner = start(&node, &source, config, &[]);
    let mut request = owner.finalize().unwrap();
    until(&mut owner, |o| o.pending_requests() == 0);
    assert_eq!(
        request.try_reply().unwrap_err(),
        managed::RequestError::Rejected(coordinator::Error::ResourceLimit)
    );
    drop(request);
    assert_eq!(source.snapshots().live_snapshots(), 0);
    unit(owner.configure_runtime(Default::default()).unwrap());
    let mut request = owner.finalize().unwrap();
    let result = complete(&mut owner, &mut request);
    assert_eq!(
        result.replies(),
        &[eoe::Reply::NotReady, eoe::Reply::NotReady]
    );
    drop((request, result));
    assert!(owner.shutdown().errors.is_empty());

    let mut config = cfg(&node);
    config.calls.reply_bytes = 1;
    let mut owner = start(&node, &source, config, &[]);
    let mut request = owner.finalize().unwrap();
    let start = Instant::now();
    loop {
        match owner.step() {
            Err(managed::Error::ResourceLimit) => break,
            Ok(_) => (),
            other => panic!("{other:?}"),
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(1));
    }
    let report = owner.shutdown();
    assert_eq!(report.abandoned_requests, vec![request.id()]);
    assert!(matches!(
        report.retained_output,
        Some(runtime::Output::Service(
            coordinator::ServiceOutput::Finalized { .. }
        ))
    ));
    assert_eq!(
        request.try_reply().unwrap_err(),
        managed::RequestError::Unknown
    );
    assert_eq!(source.snapshots().live_snapshots(), 0);
}

#[test]
fn source_gap_abandons_pending_calls_joins_workers_and_does_not_rebind_old_handles() {
    let node = Node::with_events(ae_storage::events::Config {
        records: 1,
        bytes: 1 << 20,
    });
    let source = source(&node);
    let mut owner = start(&node, &source, cfg(&node), &[12]);
    prepare(&owner);
    let old_votes = owner.votes();
    let mut request = owner.finalize().unwrap();
    let insert = inserted(&node);
    for block in node.blocks().iter().take(2) {
        insert.insert(block, 1_000_000, Origin::Gossip).unwrap();
    }
    assert!(matches!(
        owner.step(),
        Err(managed::Error::Source(live::Error::Source(
            ae_storage::events::Error::Gap { .. }
        )))
    ));
    assert!(owner.retry(Default::default()).is_err());
    let report = owner.shutdown();
    assert_eq!(report.abandoned_requests, vec![request.id()]);
    assert_eq!(
        request.try_reply().unwrap_err(),
        managed::RequestError::Unknown
    );
    assert_eq!(source.snapshots().live_snapshots(), 0);
    assert!(!old_votes.info().accepting);
    let new = start(&node, &source, cfg(&node), &[]);
    assert_eq!(
        new.epoch_cursor().revision(),
        node.db.events().cursor().revision()
    );
    assert!(old_votes.stats().is_err());
    assert!(new.shutdown().errors.is_empty());
}

#[tokio::test]
async fn composed_epochs_votes_and_parent_delivery_use_one_cache_pool_and_source() {
    use ae_sync::chain::Importer;
    use common::hyperchain::parents::Pair;
    let node = Node::new(0);
    let source = source(&node);
    let pair = Pair::new(false);
    let config = cfg(&node);
    let mut inputs = inputs(&node, &source, &config);
    inputs.parent = Some(pair.client.clone());
    let cached = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13);
    assert!(!inputs.gossip.check(cached.hash()));
    let mut publications = inputs.publications.listen().unwrap();
    let mut owner = managed::Owner::start(
        inputs,
        config,
        BTreeMap::new(),
        node.config.profile.order.clone(),
    )
    .unwrap();
    let client = owner.votes();
    assert_eq!(
        common::within(
            client
                .push(cached.encode(), hc_votes::Event::Received)
                .unwrap()
        )
        .await
        .unwrap(),
        hc_votes::PushOutcome::Cached
    );
    let signed = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 14);
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
    for _ in 0..10 {
        step(&mut owner);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(
        *common::within(publications.recv())
            .await
            .unwrap()
            .transaction,
        signed
    );
    let (importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let mut delivery = owner.parent_delivery(accepted).unwrap();
    let other = Node::new(0);
    let (foreign_importer, foreign) = Importer::new_hyperchain_scoped(
        other.db.clone(),
        other.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    assert!(matches!(
        owner.parent_delivery(foreign),
        Err(managed::Error::Configuration)
    ));
    foreign_importer.stop();
    foreign_importer.join().await;
    for (index, block) in node.blocks().iter().enumerate() {
        common::within(
            importer
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        node.check(index);
        common::within(async {
            loop {
                let p = delivery.step().unwrap();
                step(&mut owner);
                if p == ae_sync::chain::hyperchain::delivery::Progress::Idle
                    && owner.epoch_cursor().revision() == node.db.events().cursor().revision()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
    }
    let stats = common::within(client.stats().unwrap()).await.unwrap();
    assert_eq!(stats.epoch, 3.into());
    assert_eq!(stats.entries, 0);
    assert_eq!(owner.acknowledged_epochs(), 2);
    assert!(delivery.acknowledged() > 0);
    // Cache lookup publishes exactly one observation; the coordinator already
    // delivered this parent internally to both children.
    assert!(matches!(
        owner
            .entropy(4.into())
            .unwrap()
            .wait(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        runtime::Applied::Entropy(Some(_))
    ));
    let mut observed = 0;
    for _ in 0..20 {
        match owner.step().unwrap() {
            managed::Progress::Output(runtime::Output::Service(
                coordinator::ServiceOutput::Publication(local_host::Output::ParentBlock {
                    epoch,
                    ..
                }),
            )) => {
                assert_eq!(epoch, 4.into());
                observed += 1;
            }
            managed::Progress::Output(other) => panic!("{other:?}"),
            _ => (),
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(observed, 1);
    importer.stop();
    importer.join().await;
    drop(delivery);
    assert!(owner.shutdown().errors.is_empty());
    assert_eq!(source.snapshots().live_snapshots(), 0);
}

#[test]
fn startup_failure_releases_exclusive_runtime_and_snapshot_leases() {
    let node = Node::new(0);
    let source = source(&node);
    let config = cfg(&node);
    let mut wrong = inputs(&node, &source, &config);
    let other = Node::new(0);
    wrong.database = other.db.clone();
    assert!(matches!(
        managed::Owner::start(
            wrong,
            config,
            BTreeMap::new(),
            node.config.profile.order.clone()
        ),
        Err(managed::Error::Configuration)
    ));
    let mut config = cfg(&node);
    config.pool.maximum_inputs = 0;
    assert!(matches!(
        managed::Owner::start(
            inputs(&node, &source, &config),
            config,
            BTreeMap::new(),
            node.config.profile.order.clone()
        ),
        Err(managed::Error::Votes(_))
    ));
    assert_eq!(source.snapshots().live_snapshots(), 0);
    let owner = start(&node, &source, cfg(&node), &[]);
    drop(owner);
    let owner = start(&node, &source, cfg(&node), &[]);
    assert!(owner.shutdown().errors.is_empty());
}

#[test]
fn reply_byte_pressure_preserves_the_next_result_until_earlier_clones_are_released() {
    let node = Node::new(0);
    let source = source(&node);
    let mut owner = start(&node, &source, cfg(&node), &[]);
    let mut request = owner.finalize().unwrap();
    let result = complete(&mut owner, &mut request);
    let bytes = owner.retained_reply_bytes();
    drop((request, result));
    assert!(owner.shutdown().errors.is_empty());

    let mut config = cfg(&node);
    config.calls.reply_bytes = bytes;
    let mut owner = start(&node, &source, config, &[]);
    let mut first = owner.finalize().unwrap();
    let mut second = owner.finalize().unwrap();
    assert!(first.id() < second.id());
    let result = complete(&mut owner, &mut first);
    let clone = result.clone();
    drop((first, result));
    let started = Instant::now();
    while owner.step().unwrap() != managed::Progress::Backpressured {
        assert!(started.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(second.try_reply().unwrap().is_none());
    assert_eq!(owner.retained_reply_bytes(), bytes);
    assert_eq!(owner.pending_requests(), 1);
    drop(clone);
    let result = complete(&mut owner, &mut second);
    assert_eq!(
        result.replies(),
        &[eoe::Reply::NotReady, eoe::Reply::NotReady]
    );
    drop((second, result));
    assert_eq!(owner.retained_requests(), 0);
    assert_eq!(owner.retained_reply_bytes(), 0);
    assert!(owner.shutdown().errors.is_empty());
}

#[test]
fn rejected_cancellation_retains_the_caller_view_until_explicit_retry() {
    let node = Node::new(0);
    let source = source(&node);
    let mut owner = start(&node, &source, cfg(&node), &[12]);
    prepare(&owner);
    let mut request = owner.finalize().unwrap();
    unit(owner.retry_runtime().unwrap());
    for _ in 0..10 {
        step(&mut owner);
    }
    unit(
        owner
            .configure_runtime(coordinator::Config {
                maximum_outputs: 0,
                ..Default::default()
            })
            .unwrap(),
    );
    request.cancel();
    let started = Instant::now();
    loop {
        match owner.step() {
            Err(managed::Error::CancelRejected(id, coordinator::Error::ResourceLimit)) => {
                assert_eq!(id, request.id());
                break;
            }
            Ok(_) => (),
            other => panic!("{other:?}"),
        }
        assert!(started.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(owner.pending_requests(), 1);
    assert_eq!(source.snapshots().live_snapshots(), 1);
    assert!(request.try_reply().unwrap().is_none());
    unit(owner.configure_runtime(Default::default()).unwrap());
    owner.retry(Default::default()).unwrap();
    until(&mut owner, |o| o.pending_requests() == 0);
    assert_eq!(
        request.try_reply().unwrap_err(),
        managed::RequestError::Failed(coordinator::CallFailure::Cancelled)
    );
    assert_eq!(source.snapshots().live_snapshots(), 0);
    drop(request);
    assert!(owner.shutdown().errors.is_empty());
}

#[test]
fn operational_eoe_stop_is_forwarded_and_requires_explicit_runtime_recovery() {
    let node = Node::new(0);
    let source = source(&node);
    let mut config = cfg(&node);
    config.eoe.coordinator.maximum_outputs = 0;
    let mut owner = start(&node, &source, config, &[12, 13, 14]);
    prepare(&owner);
    // Votes use the independent publication queue. Fill the EOE output
    // boundary with a finalization result, retaining this exact request.
    let mut request = owner.finalize().unwrap();
    let started = Instant::now();
    loop {
        match owner.step().unwrap() {
            managed::Progress::Output(runtime::Output::WorkStopped(_)) => break,
            managed::Progress::Output(other) => panic!("{other:?}"),
            _ => (),
        }
        assert!(started.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(owner.failure().is_none());
    unit(owner.configure_runtime(Default::default()).unwrap());
    let result = complete(&mut owner, &mut request);
    assert!(matches!(
        &result.replies()[0],
        eoe::Reply::Finalized(local_host::Finalized::Signed(_))
    ));
    drop((request, result));
    assert!(owner.shutdown().errors.is_empty());
}

#[test]
fn candidate_finalization_uses_staged_nonce_and_retains_exact_snapshot() {
    use ae_storage::hyperchain::candidate as owned;
    use ae_sync::chain::Importer;
    use common::hyperchain::parents::Pair;
    let node = Node::new(1);
    let source = source(&node);
    let pair = Pair::new(false);
    let mut owner = start(&node, &source, cfg(&node), &[12, 13, 14]);
    let (importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let mut candidate = accepted
        .producer()
        .candidate(
            source.snapshots(),
            owned::Request {
                binding: repo::hyperchain::producer::Binding {
                    network: node.config.profile.network.clone(),
                    election: node.config.profile.contracts.election,
                    parent_start: 1.into(),
                    parent_length: 5.into(),
                    block_time: 4.into(),
                },
                input: owned::Input::Micro(repo::hyperchain::candidate::Input {
                    parent: node.db.view().unwrap().status().tip.unwrap(),
                    now: 999,
                    limits: Default::default(),
                }),
                parent: pair.client.snapshot().cache,
                pins: Arc::new(Default::default()),
                limits: Default::default(),
            },
            || Ok(()),
        )
        .unwrap();
    let (trees, old_nonce) = {
        let owned::Candidate::Micro(p) = candidate.outcome.as_mut().unwrap() else {
            panic!()
        };
        let nonce = p
            .candidate
            .trees()
            .accounts
            .lookup(signer(12).public_key())
            .unwrap()
            .unwrap()
            .nonce()
            .clone();
        let mut spend = ae_tx::Spend::new(
            signer(12).public_key(),
            ae_types::Id::new(ae_types::IdTag::Account, signer(13).public_key().0),
        )
        .unwrap();
        spend.set_nonce(nonce.add(&1.into()));
        spend.set_fee(1_000_000_000_000.into());
        spend.set_amount(1.into());
        let signed = finalization::sign(
            Transaction::new(TransactionLeaf::Spend(spend)),
            &signer(12),
            &node.config.profile.network,
        );
        let applied = p
            .candidate
            .update::<(), _, _>(&[signed], &p.reader)
            .unwrap();
        assert_eq!(applied.accepted, vec![0]);
        (p.candidate.trees().clone(), nonce)
    };
    prepare(&owner);
    // Independent votes arrive asynchronously. Complete both negotiations
    // before exercising the ready-path candidate view. A pending native call
    // deliberately uses the selected state instead of its caller's overlay.
    let mut settled = owner.finalize().unwrap();
    let selected = complete(&mut owner, &mut settled);
    for (i, reply) in selected.replies().iter().enumerate() {
        let eoe::Reply::Finalized(local_host::Finalized::Signed(signed)) = reply else {
            panic!("{reply:?}")
        };
        let TransactionLeaf::Contract(tx) = signed.transaction().leaf() else {
            panic!()
        };
        assert_eq!(tx.nonce(), &old_nonce.add(&(i as u64 + 1).into()));
    }
    drop((settled, selected));
    let mut request = owner.finalize_candidate(&candidate).unwrap();
    assert_eq!(request.captured().source_id, candidate.metadata.source_id);
    assert_eq!(request.captured().status, candidate.metadata.status);
    let result = complete(&mut owner, &mut request);
    for (i, reply) in result.replies().iter().enumerate() {
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
            signer(12).public_key(),
            node.config.profile.contracts.election,
            call_data.clone(),
            &(i as u64 + 1).into(),
        )
        .unwrap();
        assert_eq!(
            **signed,
            finalization::sign(expected, &signer(12), &node.config.profile.network)
        );
        assert_eq!(tx.nonce(), &old_nonce.add(&(i as u64 + 2).into()));
    }
    let selected = source.capture().unwrap();
    assert_eq!(
        selected
            .tip
            .trees
            .as_ref()
            .unwrap()
            .accounts
            .lookup(signer(12).public_key())
            .unwrap()
            .unwrap()
            .nonce(),
        &old_nonce
    );
    drop(selected);
    assert_eq!(source.snapshots().live_snapshots(), 1);
    let mut foreign_metadata = candidate.metadata.clone();
    foreign_metadata.source_id = foreign_metadata.source_id.wrapping_add(1);
    let actual = std::mem::replace(&mut candidate.metadata, foreign_metadata);
    assert!(matches!(
        owner.finalize_candidate(&candidate),
        Err(managed::Error::Configuration)
    ));
    candidate.metadata = actual;
    importer.stop();
    assert!(owner.shutdown().errors.is_empty());
    drop((candidate, trees));
    assert_eq!(source.snapshots().live_snapshots(), 0);
}
