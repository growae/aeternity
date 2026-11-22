use super::*;
use ae_storage::{hyperchain::candidate as owned, snapshots};
use common::hyperchain::parents::Pair;
fn binding(node: &Node) -> repo::hyperchain::producer::Binding {
    repo::hyperchain::producer::Binding {
        network: node.config.profile.network.clone(),
        election: node.config.profile.contracts.election,
        parent_start: 1.into(),
        parent_length: 5.into(),
        block_time: 4.into(),
    }
}
fn request(node: &Node, pair: &Pair, input: owned::Input) -> owned::Request {
    owned::Request {
        binding: binding(node),
        input,
        parent: pair.client.snapshot().cache,
        pins: Arc::new(Default::default()),
        limits: Default::default(),
    }
}
fn micro(node: &Node) -> owned::Input {
    owned::Input::Micro(build::Input {
        parent: node.db.view().unwrap().status().tip.unwrap(),
        now: 999,
        limits: Default::default(),
    })
}
#[test]
fn owned_key_candidates_match_native_headers_and_share_admission_cache_and_effect_ownership() {
    for profile in ["profile", "repeat_profile"] {
        for row in DATA["rows"].as_array().unwrap() {
            let mut node = Node::new(row["corpus_index"].as_u64().unwrap() as usize);
            Arc::make_mut(&mut node.config.profile).order =
                common::hyperchain::order(&DATA[profile]);
            let pair = Pair::new(false);
            let owner =
                hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default())
                    .unwrap();
            let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
            let blocks = node.blocks();
            for count in 0..=blocks.len() {
                if count > 0 {
                    owner
                        .insert(&blocks[count - 1], 1_000_000, Origin::Gossip)
                        .unwrap();
                }
                let cases = if count == 0 {
                    &row["candidates"]
                } else {
                    &row["rows"][count - 1]["candidates"]
                };
                for c in cases.as_array().unwrap().iter().filter(|c| {
                    ["normal", "hole", "eoe", "wrong_producer"]
                        .contains(&c["name"].as_str().unwrap())
                }) {
                    let view = node.db.view().unwrap();
                    let before = owner.snapshot().unwrap();
                    let checkpoint = view.hyperchain_checkpoint().unwrap();
                    let input = input(view.status().tip.unwrap(), c);
                    let mut expected_state = before.1.clone();
                    let mut expected_host = Parent::default();
                    let direct = build::key(
                        &view,
                        &node.config,
                        input,
                        &mut expected_state,
                        &mut expected_host,
                    );
                    let prepared = owner
                        .prepare_candidate(
                            &snapshots,
                            request(&node, &pair, owned::Input::Key(input)),
                            || Ok::<_, ()>(()),
                        )
                        .unwrap();
                    assert_eq!(
                        owner.snapshot().unwrap(),
                        (before.0, expected_state, before.2)
                    );
                    assert_eq!(prepared.metadata.status, view.status());
                    assert_eq!(prepared.metadata.source_id, view.source().source_id());
                    let epochs = prepared
                        .effects
                        .entries()
                        .iter()
                        .map(|e| match e {
                            repo::hyperchain::inputs::Effect::Parent { epoch, .. } => epoch.clone(),
                            e => panic!("unexpected effect {e:?}"),
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(epochs, expected_host.reads);
                    match prepared.outcome {
                        Ok(owned::Candidate::Key(p)) => {
                            let direct = direct.unwrap();
                            let time = c["clocks"][1].as_u64().unwrap();
                            let header = p.candidate.finish::<()>(time).unwrap();
                            assert_eq!(header, direct.candidate.finish::<()>(time).unwrap());
                            assert_eq!(
                                Block::Key(header).encode().unwrap(),
                                bytes(&c["result"]["block"])
                            );
                            assert_eq!(
                                p.candidate.trees().roots(),
                                direct.candidate.trees().roots()
                            );
                        }
                        Err(e) => {
                            assert!(!e.operational);
                            assert!(direct.is_err());
                            assert!(!c["result"]["error"].is_null());
                        }
                        _ => panic!("wrong candidate kind"),
                    }
                    assert_eq!(
                        node.db.view().unwrap().hyperchain_checkpoint().unwrap(),
                        checkpoint
                    );
                    assert_eq!(snapshots.live_snapshots(), 0);
                }
            }
        }
    }
}
#[test]
fn candidate_guards_limits_and_foreign_snapshot_factory_roll_back_and_release_slots() {
    let node = Node::new(1);
    let pair = Pair::new(false);
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    let snapshots = snapshots::Owner::start(
        node.db.clone(),
        snapshots::Config {
            maximum_snapshots: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let original = owner.snapshot().unwrap();
    let mut checks = 0;
    assert!(matches!(
        owner.prepare_candidate(&snapshots, request(&node, &pair, micro(&node)), || {
            checks += 1;
            if checks == 2 { Err("stopped") } else { Ok(()) }
        }),
        Err(owned::Error::Guard("stopped"))
    ));
    assert_eq!(checks, 2);
    assert_eq!(owner.snapshot().unwrap(), original);
    assert_eq!(snapshots.live_snapshots(), 0);
    let mut bad = request(&node, &pair, micro(&node));
    bad.binding.block_time = 8.into();
    assert!(matches!(
        owner.prepare_candidate(&snapshots, bad, || Ok::<_, ()>(())),
        Err(owned::Error::Repository(repo::Error::Identity))
    ));
    let foreign = Node::new(1);
    let other = snapshots::Owner::start(foreign.db.clone(), Default::default()).unwrap();
    assert!(matches!(
        owner.prepare_candidate(&other, request(&node, &pair, micro(&node)), || Ok::<_, ()>(
            ()
        )),
        Err(owned::Error::Repository(repo::Error::Identity))
    ));
    let mut bad = request(&node, &pair, micro(&node));
    bad.limits.maximum_effects = 0;
    assert!(matches!(
        owner.prepare_candidate(&snapshots, bad, || Ok::<_, ()>(())),
        Err(owned::Error::Execution(_))
    ));
    assert_eq!(owner.snapshot().unwrap(), original);
    let retained = owner
        .prepare_candidate(&snapshots, request(&node, &pair, micro(&node)), || {
            Ok::<_, ()>(())
        })
        .unwrap();
    let after = owner.snapshot().unwrap();
    assert!(retained.outcome.is_ok());
    assert!(matches!(
        owner.prepare_candidate(&snapshots, request(&node, &pair, micro(&node)), || Ok::<
            _,
            (),
        >(
            ()
        )),
        Err(owned::Error::Snapshot(snapshots::Error::ResourceLimit))
    ));
    assert_eq!(owner.snapshot().unwrap(), after);
    drop(retained);
    assert_eq!(snapshots.live_snapshots(), 0);
    assert!(
        owner
            .prepare_candidate(&snapshots, request(&node, &pair, micro(&node)), || Ok::<
                _,
                (),
            >(
                ()
            ))
            .unwrap()
            .outcome
            .is_ok()
    );
}
#[test]
fn owned_micro_updates_keep_captured_parent_after_top_advance_and_owner_drop() {
    let data: J =
        serde_json::from_str(include_str!("../fixtures/erlang_hc_micro_candidate.json")).unwrap();
    let c = &data["rows"][0]["candidates"][1];
    let node = Node::new(data["rows"][0]["corpus_index"].as_u64().unwrap() as usize);
    let pair = Pair::new(false);
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let prepared = owner
        .prepare_candidate(&snapshots, request(&node, &pair, micro(&node)), || {
            Ok::<_, ()>(())
        })
        .unwrap();
    let owned::Candidate::Micro(mut prepared) = prepared.outcome.unwrap() else {
        panic!()
    };
    let parent = prepared.candidate.block().header.common.previous_hash;
    let blocks = node.blocks();
    owner.insert(&blocks[0], 1_000_000, Origin::Gossip).unwrap();
    assert_ne!(node.db.view().unwrap().status().tip.unwrap().0, parent);
    drop((owner, snapshots, pair));
    let mut packing = candidate::Packing::new::<()>(
        prepared.candidate,
        0.into(),
        0,
        candidate::Timeout::Disabled,
        100,
    )
    .unwrap();
    for batch in c["batches"].as_array().unwrap() {
        assert_eq!(
            packing.request::<()>(|| 0).unwrap().unwrap().parent.0,
            parent
        );
        let txs = batch
            .as_array()
            .unwrap()
            .iter()
            .map(|b| ae_tx::SignedTransaction::decode(&bytes(b)).unwrap())
            .collect::<Vec<_>>();
        packing
            .selected::<(), _, _>(&txs, false, &prepared.reader)
            .unwrap();
    }
    assert!(packing.request::<()>(|| 0).unwrap().is_none());
    prepared.candidate = packing.finish::<()>().unwrap();
    assert_eq!(
        Block::Micro(prepared.candidate.block().clone())
            .encode()
            .unwrap(),
        bytes(&c["result"]["block"])
    );
    for u in c["updates"].as_array().unwrap() {
        let txs = u["transactions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| ae_tx::SignedTransaction::decode(&bytes(b)).unwrap())
            .collect::<Vec<_>>();
        let result = prepared
            .candidate
            .update::<(), _, _>(&txs, &prepared.reader);
        if u["error"].is_string() {
            assert!(matches!(result, Err(candidate::Error::NoUpdate)));
        } else {
            result.unwrap();
            assert_eq!(
                Block::Micro(prepared.candidate.block().clone())
                    .encode()
                    .unwrap(),
                bytes(&u["result"]["block"])
            );
        }
    }
    assert_eq!(
        prepared.candidate.block().header.common.previous_hash,
        parent
    );
    assert_ne!(prepared.status, node.db.view().unwrap().status());
}
#[test]
fn importer_candidate_capability_expires_with_exact_context() {
    use ae_sync::chain::{Error, Importer};
    let node = Node::new(1);
    let pair = Pair::new(false);
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let (importer, mut accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let producer = accepted.producer();
    let prepared = producer
        .candidate(&snapshots, request(&node, &pair, micro(&node)), || Ok(()))
        .unwrap();
    assert!(prepared.outcome.is_ok());
    assert!(!prepared.effects.entries().is_empty());
    assert!(accepted.take().unwrap().entries().is_empty());
    importer.stop();
    assert!(matches!(
        producer.candidate(&snapshots, request(&node, &pair, micro(&node)), || Ok(())),
        Err(Error::Closed)
    ));
    drop((importer, accepted));
    let (replacement, next) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    assert!(matches!(
        producer.candidate(&snapshots, request(&node, &pair, micro(&node)), || Ok(())),
        Err(Error::Closed)
    ));
    assert!(
        next.producer()
            .candidate(&snapshots, request(&node, &pair, micro(&node)), || Ok(()))
            .unwrap()
            .outcome
            .is_ok()
    );
    replacement.stop();
}
