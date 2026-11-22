use super::*;
use crate::test_support::hyperchain as fixture;

const HC_NOW: u64 = 1_000_000;
struct BeforeInsert {
    inner: Box<dyn super::super::hyperchain::Engine>,
    action: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl super::super::hyperchain::Engine for BeforeInsert {
    fn insert(
        &self,
        block: &Block,
        now: u64,
        origin: ledger::Origin,
        scope: &ae_storage::policy::Scope,
        check: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<ae_storage::Committed, Error> {
        let action = self.action.lock().unwrap().take();
        if let Some(action) = action {
            action();
        }
        self.inner.insert(block, now, origin, scope, check)
    }
}
fn importer(node: &fixture::Node, inputs: fixture::Inputs, config: Config) -> Importer {
    Importer::new_hyperchain(node.db.clone(), node.config.clone(), inputs, config).unwrap()
}

#[tokio::test]
async fn native_finalized_chains_import_in_both_generation_directions() {
    for index in 0..7 {
        for forward in [false, true] {
            let node = fixture::Node::new(index);
            let inputs = fixture::Inputs::default();
            let importer = importer(&node, inputs.clone(), Default::default());
            let mut leader = node
                .db
                .view()
                .unwrap()
                .require_block(node.config.identity.genesis)
                .unwrap();
            let mut micros = vec![];
            for (i, b) in node.blocks().into_iter().enumerate() {
                if let Block::Micro(_) = b {
                    if forward {
                        let report = within(
                            importer
                                .import(
                                    packet(&leader, std::slice::from_ref(&b), Direction::Forward),
                                    Hash256::ZERO,
                                    Direction::Forward,
                                    HC_NOW,
                                )
                                .unwrap()
                                .wait(),
                        )
                        .await
                        .unwrap();
                        assert_eq!(report.accepted.len(), 1);
                        node.check(i);
                    } else {
                        micros.push(b);
                    }
                } else {
                    let bytes = packet(&b, &micros, Direction::Backward);
                    let report = within(
                        importer
                            .import(
                                bytes.clone(),
                                b.hash().unwrap(),
                                Direction::Backward,
                                HC_NOW,
                            )
                            .unwrap()
                            .wait(),
                    )
                    .await
                    .unwrap();
                    assert_eq!(report.accepted.len(), micros.len() + 1);
                    node.check(i);
                    let duplicate = within(
                        importer
                            .import(bytes, b.hash().unwrap(), Direction::Backward, HC_NOW)
                            .unwrap()
                            .wait(),
                    )
                    .await
                    .unwrap();
                    assert!(
                        duplicate
                            .accepted
                            .iter()
                            .all(|a| a.outcome == Outcome::Present)
                    );
                    let view = node.db.view().unwrap();
                    assert_eq!(
                        view.hyperchain_event(view.status().revision)
                            .unwrap()
                            .unwrap()
                            .context
                            .origin,
                        ae_chain::publication::Origin::Synced
                    );
                    micros.clear();
                    leader = b;
                }
            }
            assert_eq!(inputs.reads.lock().unwrap().len(), 14); // Initial snapshot + 13 new blocks.
            importer.stop();
            importer.join().await;
        }
    }
}

#[tokio::test]
async fn hc_gossip_created_and_duplicate_admission_share_the_durable_owner() {
    let node = fixture::Node::new(6);
    let inputs = fixture::Inputs::default();
    let importer = importer(
        &node,
        inputs.clone(),
        Config {
            jobs: 1,
            ..Default::default()
        },
    );
    assert!(
        Importer::new_hyperchain(
            node.db.clone(),
            node.config.clone(),
            fixture::Inputs::default(),
            Default::default()
        )
        .is_err()
    );
    let genesis = node
        .db
        .view()
        .unwrap()
        .require_block(node.config.identity.genesis)
        .unwrap();
    assert_eq!(
        within(
            importer
                .import_block(genesis.encode().unwrap(), HC_NOW)
                .unwrap()
                .wait()
        )
        .await
        .unwrap()
        .accepted[0]
            .outcome,
        Outcome::Present
    );
    for (i, b) in node.blocks().into_iter().enumerate() {
        let source = node.db.events();
        let cursor = source.cursor();
        let ticket = if i % 2 == 0 {
            importer.import_created_block(b.encode().unwrap(), HC_NOW)
        } else {
            importer.import_block(b.encode().unwrap(), HC_NOW)
        }
        .unwrap();
        assert_eq!(importer.available().0, 0);
        assert!(matches!(
            importer.import_block(b.encode().unwrap(), HC_NOW),
            Err(Error::Busy)
        ));
        within(ticket.wait()).await.unwrap();
        node.check(i);
        let view = node.db.view().unwrap();
        let expected = if i % 2 == 0 {
            ae_chain::publication::Origin::Created
        } else {
            ae_chain::publication::Origin::Received
        };
        let journal = view
            .hyperchain_event(view.status().revision)
            .unwrap()
            .unwrap();
        assert_eq!(journal.context.origin, expected);
        let events = source.after(&cursor, 1).unwrap();
        assert_eq!(events.records.len(), 1);
        assert_eq!(
            events.records[0]
                .effects()
                .filter(|e| matches!(e, ae_chain::publication::Effect::Consensus { .. }))
                .count(),
            journal.events.len()
        );
        if let Block::Micro(mut corrupt) = b.clone()
            && !corrupt.transactions.is_empty()
        {
            corrupt.transactions.clear();
            let failure = within(
                importer
                    .import_block(Block::Micro(corrupt).encode().unwrap(), HC_NOW)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap_err();
            assert!(matches!(
                failure.error,
                Error::Validation {
                    operational: false,
                    ..
                }
            ));
            assert_eq!(node.db.view().unwrap().status(), view.status());
        }
    }
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn hc_parent_snapshot_failures_and_policy_conflicts_recapture_before_retry() {
    let node = fixture::Node::new(0);
    let inputs = fixture::Inputs::default();
    let importer = importer(&node, inputs.clone(), Default::default());
    let first = node.blocks()[0].clone();
    inputs.fail.store(true, Ordering::Release);
    let failure = within(
        importer
            .import_block(first.encode().unwrap(), HC_NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        failure.error,
        Error::Validation {
            operational: true,
            ..
        }
    ));
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    assert!(!importer.recovery_required());
    inputs.fail.store(false, Ordering::Release);
    let db = node.db.clone();
    inputs.hook(move || {
        db.update_resilience(
            ae_chain::resilience::Event::Start,
            ae_chain::resilience::Config {
                depth: 1,
                from_start: true,
            },
        )
        .unwrap();
    });
    within(
        importer
            .import_block(first.encode().unwrap(), HC_NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    node.check(0);
    let reads = inputs.reads.lock().unwrap().clone();
    assert_eq!(reads.len(), 4);
    assert_eq!(reads[2].0, 1);
    assert_eq!(reads[3].0, 1);
    assert!(reads[3].1 > reads[2].1);
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn hc_cancelled_capture_cannot_publish_or_consume_the_next_host_snapshot() {
    let node = fixture::Node::new(0);
    let inputs = fixture::Inputs::default();
    let importer = importer(
        &node,
        inputs.clone(),
        Config {
            jobs: 1,
            ..Default::default()
        },
    );
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    inputs.hook(move || {
        let _ = entered.send(());
        resume.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    let first = node.blocks()[0].clone();
    let ticket = importer
        .import_block(first.encode().unwrap(), HC_NOW)
        .unwrap();
    within(ready).await.unwrap();
    drop(ticket);
    release.send(()).unwrap();
    within(async {
        while importer.available().0 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    within(
        importer
            .import_block(first.encode().unwrap(), HC_NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    node.check(0);
    assert_eq!(inputs.reads.lock().unwrap().len(), 3);
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn hc_parent_policy_replacement_closes_the_old_importer_during_capture() {
    let node = fixture::Node::new(0);
    let inputs = fixture::Inputs::default();
    let policy = crate::resilience::Owner::new(node.db.clone(), Default::default()).unwrap();
    let child = policy.start().unwrap();
    let importer = importer(&node, inputs.clone(), Default::default());
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    inputs.hook(move || {
        let _ = entered.send(());
        resume.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    let first = node.blocks()[0].clone();
    let ticket = importer
        .import_block(first.encode().unwrap(), HC_NOW)
        .unwrap();
    within(ready).await.unwrap();
    child.shutdown().unwrap();
    drop(policy);
    let next = crate::resilience::Owner::new(node.db.clone(), Default::default()).unwrap();
    let child = next.start().unwrap();
    assert!(node.db.policy_ready());
    release.send(()).unwrap();
    let failure = within(ticket.wait()).await.unwrap_err();
    assert_eq!(failure.error, Error::Closed);
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    assert_eq!(importer.check_open(), Err(Error::Closed));
    importer.stop();
    importer.join().await;
    drop(importer);
    let fresh = super::hyperchain::importer(&node, inputs, Default::default());
    within(
        fresh
            .import_block(first.encode().unwrap(), HC_NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    node.check(0);
    fresh.stop();
    fresh.join().await;
    child.shutdown().unwrap();
}

#[tokio::test]
async fn hc_forward_key_validation_and_backward_structure_fail_before_any_prefix() {
    let node = fixture::Node::new(0);
    let importer = importer(&node, Default::default(), Default::default());
    let blocks = node.blocks();
    let Block::Key(mut invalid) = blocks[1].clone() else {
        panic!()
    };
    invalid.common.state_hash = [1; 32];
    let failure = within(
        importer
            .import(
                packet(
                    &Block::Key(invalid),
                    &[blocks[0].clone()],
                    Direction::Forward,
                ),
                Hash256::ZERO,
                Direction::Forward,
                HC_NOW,
            )
            .unwrap()
            .wait(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        failure.error,
        Error::Validation {
            operational: false,
            ..
        }
    ));
    assert!(failure.progress.unwrap().accepted.is_empty());
    let failure = within(
        importer
            .import(
                packet(&blocks[1], &[blocks[2].clone()], Direction::Backward),
                blocks[1].hash().unwrap(),
                Direction::Backward,
                HC_NOW,
            )
            .unwrap()
            .wait(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        failure.error,
        Error::Generation(generation::Error::NonConsecutive)
    );
    assert!(failure.progress.unwrap().accepted.is_empty());
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn hc_duplicate_between_admission_and_owner_capture_retries_to_present() {
    let node = fixture::Node::new(0);
    let mut importer = importer(&node, Default::default(), Default::default());
    let context = Arc::get_mut(&mut importer.context).unwrap();
    let strategy::Strategy::Hyperchain(hc) = &mut context.strategy else {
        panic!()
    };
    // Move the real engine into a one-shot race adapter, without replacing
    // preparation, publication or the storage owner's acknowledgement path.
    let inner = std::mem::replace(&mut hc.engine, Box::new(Empty));
    let db = node.db.clone();
    let config = node.config.clone();
    let b = node.blocks()[0].clone();
    let first = b.clone();
    hc.engine = Box::new(BeforeInsert {
        inner,
        action: Mutex::new(Some(Box::new(move || {
            let view = db.view().unwrap();
            let mut state = view.hyperchain_checkpoint().unwrap().state;
            let mut host = fixture::Parent::default();
            let update =
                repository::prepare_hyperchain(&view, &b, &config, HC_NOW, &mut state, &mut host)
                    .unwrap();
            db.commit(update).unwrap();
        }))),
    });
    let report = within(
        importer
            .import_block(first.encode().unwrap(), HC_NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    assert_eq!(report.accepted[0].outcome, Outcome::Present);
    node.check(0);
    assert_eq!(node.db.view().unwrap().status().revision, 2);
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn hc_invalid_finalized_generation_retains_only_the_acknowledged_micro_prefix() {
    let node = fixture::Node::new(6);
    let importer = importer(&node, Default::default(), Default::default());
    for b in node.blocks().into_iter().take(4) {
        within(
            importer
                .import_block(b.encode().unwrap(), HC_NOW)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
    }
    let probe = &fixture::DATA["rows"][6]["rows"][4]["fork_probe"];
    let micro = fixture::block(&probe["micro"]);
    let key = Block::decode(
        &hex::decode(probe["key_block"].as_str().unwrap()).unwrap(),
        node.db.view().unwrap().config().block(),
    )
    .unwrap();
    let revision = node.db.view().unwrap().status().revision;
    let failure = within(
        importer
            .import(
                packet(&key, std::slice::from_ref(&micro), Direction::Backward),
                key.hash().unwrap(),
                Direction::Backward,
                HC_NOW,
            )
            .unwrap()
            .wait(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        failure.error,
        Error::Validation {
            operational: false,
            ..
        }
    ));
    let accepted = failure.progress.unwrap().accepted;
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].hash, micro.hash().unwrap());
    assert!(matches!(accepted[0].outcome, Outcome::Committed { .. }));
    let view = node.db.view().unwrap();
    assert_eq!(view.status().revision, revision + 1);
    assert_eq!(view.status().tip, Some(micro.hash().unwrap()));
    assert!(view.block(key.hash().unwrap()).unwrap().is_none());
    view.verify_tip().unwrap();
    let trees = view
        .trees(micro.hash().unwrap(), Default::default())
        .unwrap();
    for row in probe["micro"]["calls"].as_array().unwrap() {
        let id = hex::decode(row["id"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let call = ae_state::calls::Call::deserialize(
            id,
            &hex::decode(row["encoded"].as_str().unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(trees.calls.lookup(&call.contract, id).unwrap(), Some(call));
    }
    assert!(!importer.recovery_required());
    importer.stop();
    importer.join().await;
}
struct Empty;
impl super::super::hyperchain::Engine for Empty {
    fn insert(
        &self,
        _: &Block,
        _: u64,
        _: ledger::Origin,
        _: &ae_storage::policy::Scope,
        _: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<ae_storage::Committed, Error> {
        panic!("unused placeholder")
    }
}

#[tokio::test]
async fn hc_competing_native_branches_keep_side_publications_and_reorganize_the_tip() {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../ae-hyperchain/tests/fixtures/erlang_hc_durable.json"
    ))
    .unwrap();
    for profile in ["profile", "repeat_profile"] {
        let mut node = fixture::Node::new(0);
        Arc::make_mut(&mut node.config.profile).order = fixture::order(&corpus[profile]);
        let importer = importer(&node, Default::default(), Default::default());
        let mut sides = 0;
        for row in corpus["rows"].as_array().unwrap() {
            let block = fixture::block(row);
            let source = node.db.events();
            let cursor = source.cursor();
            assert_eq!(
                hex::encode(node.db.view().unwrap().status().tip.unwrap().0),
                row["old_tip"]
            );
            let report = within(
                importer
                    .import_block(block.encode().unwrap(), HC_NOW)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
            assert!(matches!(
                report.accepted[0].outcome,
                Outcome::Committed { .. }
            ));
            let view = node.db.view().unwrap();
            view.verify_tip().unwrap();
            assert_eq!(hex::encode(view.status().tip.unwrap().0), row["new_tip"]);
            let record = view.require_record(block.hash().unwrap()).unwrap();
            assert_eq!(
                record.roots.state.hash().0,
                block.header().common().state_hash
            );
            if row["new_tip"] == row["old_tip"] {
                sides += 1;
            }
            let events = source.after(&cursor, 1).unwrap();
            assert_eq!(events.records.len(), 1);
            let journal = view
                .hyperchain_event(view.status().revision)
                .unwrap()
                .unwrap();
            assert_eq!(
                events.records[0]
                    .effects()
                    .filter(|e| matches!(e, ae_chain::publication::Effect::Consensus { .. }))
                    .count(),
                journal.events.len()
            );
        }
        assert_eq!(sides, 2);
        importer.stop();
        importer.join().await;
    }
}
