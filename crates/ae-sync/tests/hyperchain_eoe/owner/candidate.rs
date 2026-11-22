use super::*;
use ae_chain::hyperchain::candidate as core;
use ae_storage::{
    hyperchain::candidate as owned,
    pool::{self, worker},
};
use ae_sync::{chain::Importer, chain::hyperchain::eoe::owner::candidate as build};
use ae_types::AccountPublicKey;
use common::hyperchain::parents::Pair;
use serde_json::Value as J;
use std::sync::LazyLock;
pub(super) static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!(
        "../../fixtures/erlang_hc_vote_application.json"
    ))
    .unwrap()
});
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn input(node: &Node) -> build::Input {
    build::Input {
        parent: node.db.view().unwrap().status().tip.unwrap(),
        limits: Default::default(),
        pins: Arc::new(Default::default()),
        inputs: Default::default(),
        timeout: core::Timeout::Disabled,
        window_ms: 1000,
    }
}
pub(super) fn pool(node: &Node) -> worker::Worker {
    worker::Worker::spawn(
        node.db.clone(),
        node.config.profile.network.clone(),
        pool::Config {
            consensus: ae_protocol::ConsensusType::Pos,
            policy: ae_mempool::pool::Config {
                minimum_miner_gas_price: 1.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        worker::Settings {
            origins_interval: None,
            ..Default::default()
        },
    )
    .unwrap()
}
pub(super) fn owner(
    node: &Node,
    source: &Arc<live::Source>,
    pair: &Pair,
    mut config: managed::Config,
) -> managed::Owner {
    config.eoe.block_time = 4.into();
    let mut i = inputs(node, source, &config);
    i.parent = Some(pair.client.clone());
    managed::Owner::start(
        i,
        config,
        [12, 13, 14]
            .into_iter()
            .map(|n| {
                let k = signer(n);
                (k.public_key(), k)
            })
            .collect(),
        node.config.profile.order.clone(),
    )
    .unwrap()
}
fn drive<O: ae_vm_fate::maps::MapOrder + Clone>(
    owner: &mut managed::Owner,
    build: &mut build::Build<O>,
    now: i128,
) -> Vec<build::Progress> {
    let end = Instant::now() + Duration::from_secs(10);
    let mut progress = vec![];
    while !build.ready() {
        assert!(Instant::now() < end, "candidate stalled: {build:?}");
        step(owner);
        progress.push(build.step(owner, now).unwrap());
        std::thread::sleep(Duration::from_millis(1));
    }
    progress
}
#[tokio::test]
async fn real_eoe_pool_and_candidate_retry_preserve_nonces_then_admit_created_microblocks() {
    let mut admitted = 0;
    let mut rejected = 0;
    for profile in ["profile", "repeat_profile"] {
        for row in DATA["rows"].as_array().unwrap() {
            for with_spend in [false, true] {
                let mut node = Node::new(row["corpus_index"].as_u64().unwrap() as usize);
                Arc::make_mut(&mut node.config.profile).order =
                    common::hyperchain::order(&DATA[profile]);
                let storage = inserted(&node);
                for b in node.blocks().iter().take(4) {
                    storage.insert(b, 1_000_000, Origin::Gossip).unwrap();
                }
                drop(storage);
                let c = &row["parents"][1]["cases"][if with_spend { 4 } else { 3 }];
                let leader = AccountPublicKey(bytes(&c["leader"]).try_into().unwrap());
                let source = source(&node);
                let pair = Pair::new(false);
                let (importer, accepted) = Importer::new_hyperchain_scoped(
                    node.db.clone(),
                    node.config.clone(),
                    pair.inputs(),
                    Default::default(),
                )
                .unwrap();
                let mut config = cfg(&node);
                config.calls.requests = 1;
                let mut owner = owner(&node, &source, &pair, config);
                let pool = pool(&node);
                let client = pool.client();
                if with_spend {
                    client
                        .force_push(Arc::new(
                            SignedTransaction::decode(&bytes(&c["transactions"][0])).unwrap(),
                        ))
                        .unwrap()
                        .wait()
                        .unwrap();
                }
                let parent = node.db.view().unwrap().status().tip.unwrap();
                let trees = node
                    .db
                    .view()
                    .unwrap()
                    .trees(parent, Default::default())
                    .unwrap();
                let old_nonce = trees
                    .accounts
                    .lookup(leader)
                    .unwrap()
                    .unwrap()
                    .nonce()
                    .clone();
                drop(trees);
                unit(
                    owner
                        .negotiate(eoe::Negotiation {
                            epoch: 1.into(),
                            height: 3.into(),
                            hash: parent.0.to_vec(),
                            leader,
                            validators: [(12, 4), (13, 3), (14, 3)]
                                .into_iter()
                                .map(|(n, w)| (signer(n).public_key(), w.into()))
                                .collect(),
                            seed: Some(key(999).to_vec()),
                            current_length: 3.into(),
                        })
                        .unwrap(),
                );
                let mut building = owner
                    .micro_candidate(accepted.producer(), client.clone(), input(&node))
                    .unwrap();
                let progress = drive(&mut owner, &mut building, c["now"].as_i64().unwrap().into());
                assert_eq!(building.refreshes(), u8::from(with_spend));
                assert!(building.reserved_gas() > &0.into());
                let first = progress
                    .iter()
                    .position(|p| *p == build::Progress::Answered { refresh: false })
                    .unwrap();
                let created = progress
                    .iter()
                    .position(|p| *p == build::Progress::Prepared)
                    .unwrap();
                assert!(first < created);
                let packing = progress
                    .iter()
                    .position(|p| matches!(p, build::Progress::Packing(_)))
                    .unwrap();
                assert!(
                    progress[created + 1..packing]
                        .iter()
                        .all(|p| matches!(p, build::Progress::Effects(_)))
                );
                assert!(
                    !progress[packing..]
                        .iter()
                        .any(|p| matches!(p, build::Progress::Effects(_)))
                );
                assert_eq!(
                    progress
                        .iter()
                        .filter(|p| matches!(p, build::Progress::Requested { refresh: true }))
                        .count(),
                    usize::from(with_spend)
                );
                assert_eq!(owner.pending_requests(), 0);
                assert_eq!(owner.retained_requests(), 0);
                assert_eq!(owner.retained_reply_bytes(), 0);
                let completed = building.finish().unwrap();
                assert!(completed.candidate.effects.entries().is_empty());
                assert!(completed.is_eoe);
                assert_eq!(completed.candidate.metadata.status.tip, Some(parent));
                let owned::Candidate::Micro(prepared) =
                    completed.candidate.outcome.as_ref().unwrap()
                else {
                    panic!()
                };
                let txs = &prepared.candidate.block().transactions;
                assert_eq!(txs.len(), 2 + usize::from(with_spend));
                for (i, tx) in txs.iter().enumerate() {
                    assert_eq!(
                        tx.transaction().view().nonce(),
                        old_nonce.add(&(i as u64 + 1).into())
                    );
                }
                let stored = node.db.view().unwrap();
                let current = stored.trees(parent, Default::default()).unwrap();
                assert_eq!(
                    current.accounts.lookup(leader).unwrap().unwrap().nonce(),
                    &old_nonce
                );
                drop((current, stored));
                let keys = ae_hyperchain::signing::Keys::new([signer(12), signer(13), signer(14)]);
                let signed = Block::Micro(prepared.candidate.signed::<()>(&keys, leader).unwrap());
                let hash = signed.hash().unwrap();
                let before = node.db.view().unwrap().status();
                let result = common::within(
                    importer
                        .import_created_block(signed.encode().unwrap(), 1_000_000)
                        .unwrap()
                        .wait(),
                )
                .await;
                // Native creation keeps the key parent's version even when
                // the next micro height crosses the configured protocol fork.
                // Ordinary Created admission remains authoritative.
                match node.config.profile.network.validate_version(
                    signed.header().common().height,
                    signed.header().common().version,
                ) {
                    Ok(_) => {
                        result.unwrap();
                        admitted += 1;
                        assert_eq!(node.db.view().unwrap().status().tip, Some(hash));
                        assert_eq!(node.db.view().unwrap().require_block(hash).unwrap(), signed);
                    }
                    Err(error) => {
                        assert_eq!(
                            result.unwrap_err().error,
                            ae_sync::chain::Error::Validation {
                                operational: false,
                                reason: format!("Protocol({error:?})")
                            }
                        );
                        assert_eq!(node.db.view().unwrap().status(), before);
                        rejected += 1;
                    }
                }
                drop(completed);
                assert_eq!(source.snapshots().live_snapshots(), 0);
                assert!(owner.shutdown().errors.is_empty());
                pool.shutdown().unwrap();
                importer.stop();
                importer.join().await;
            }
        }
    }
    assert_eq!((admitted, rejected), (12, 4));
}
#[test]
fn initial_reply_pressure_retries_without_creating_candidate_or_replaying_completed_requests() {
    let node = Node::new(1);
    let source = source(&node);
    let pair = Pair::new(false);
    let (_importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let mut config = cfg(&node);
    config.calls.requests = 1;
    let mut owner = owner(&node, &source, &pair, config);
    let pool = pool(&node);
    let mut held = owner.finalize().unwrap();
    let reply = complete(&mut owner, &mut held);
    let mut building = owner
        .micro_candidate(accepted.producer(), pool.client(), input(&node))
        .unwrap();
    let error = build::Error::Owner(managed::Error::Backpressured);
    assert_eq!(building.step(&mut owner, 999), Err(error.clone()));
    assert!(building.candidate().is_none());
    assert_eq!(pool.info().submitted, 0);
    assert_eq!(building.step(&mut owner, 999), Err(error));
    drop((held, reply));
    building.retry().unwrap();
    drive(&mut owner, &mut building, 999);
    let completed = building.finish().unwrap();
    assert!(!completed.is_eoe);
    assert_eq!(completed.refreshes, 0);
    assert_eq!(completed.reserved_gas, 0.into());
    let owned::Candidate::Micro(p) = completed.candidate.outcome.as_ref().unwrap() else {
        panic!()
    };
    assert!(p.candidate.block().transactions.is_empty());
    drop(completed);
    assert_eq!(source.snapshots().live_snapshots(), 0);
    assert_eq!(owner.retained_requests(), 0);
    assert!(owner.shutdown().errors.is_empty());
    pool.shutdown().unwrap();
}
#[test]
fn pending_build_cannot_rebind_eoe_replacement_and_cancel_remains_terminal() {
    let node = Node::new(1);
    let source = source(&node);
    let pair = Pair::new(false);
    let (_importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let mut owner = owner(&node, &source, &pair, cfg(&node));
    let pool = pool(&node);
    let mut building = owner
        .micro_candidate(accepted.producer(), pool.client(), input(&node))
        .unwrap();
    assert_eq!(
        building.step(&mut owner, 999).unwrap(),
        build::Progress::Requested { refresh: false }
    );
    let report = owner.shutdown();
    assert!(report.errors.is_empty());
    let mut replacement = self::owner(&node, &source, &pair, cfg(&node));
    assert_eq!(
        building.step(&mut replacement, 999),
        Err(build::Error::Binding)
    );
    assert_eq!(building.retry(), Err(build::Error::Binding));
    assert_eq!(replacement.pending_requests(), 0);
    drop(building);
    let mut cancelled = replacement
        .micro_candidate(accepted.producer(), pool.client(), input(&node))
        .unwrap();
    cancelled.step(&mut replacement, 999).unwrap();
    cancelled.cancel();
    assert_eq!(
        cancelled.step(&mut replacement, 999),
        Err(build::Error::Cancelled)
    );
    assert_eq!(cancelled.retry(), Err(build::Error::Cancelled));
    drop(cancelled);
    until(&mut replacement, |o| o.pending_requests() == 0);
    assert_eq!(replacement.retained_requests(), 0);
    assert_eq!(source.snapshots().live_snapshots(), 0);
    assert_eq!(pool.info().submitted, 0);
    assert!(replacement.shutdown().errors.is_empty());
    pool.shutdown().unwrap();
}
#[test]
fn retired_importer_prevents_a_build_from_advancing_after_initial_vote_submission() {
    let node = Node::new(1);
    let source = source(&node);
    let pair = Pair::new(false);
    let (importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let mut owner = owner(&node, &source, &pair, cfg(&node));
    let pool = pool(&node);
    let mut building = owner
        .micro_candidate(accepted.producer(), pool.client(), input(&node))
        .unwrap();
    building.step(&mut owner, 999).unwrap();
    drop(importer);
    let error = build::Error::Source(ae_sync::chain::Error::Closed);
    assert_eq!(building.step(&mut owner, 999), Err(error.clone()));
    assert_eq!(building.retry(), Err(error));
    assert!(building.candidate().is_none());
    drop(building);
    until(&mut owner, |o| o.pending_requests() == 0);
    assert_eq!(source.snapshots().live_snapshots(), 0);
    assert_eq!(pool.info().submitted, 0);
    assert!(owner.shutdown().errors.is_empty());
    pool.shutdown().unwrap();
}

fn prepared<O: ae_vm_fate::maps::MapOrder + Clone>(
    owner: &mut managed::Owner,
    building: &mut build::Build<O>,
) {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < end);
        step(owner);
        if building.step(owner, 999).unwrap() == build::Progress::Prepared {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn rejected_candidate_notification_retries_without_repeating_acknowledged_prefix_or_packing_early()
{
    use ae_sync::chain::hyperchain::delivery;
    let node = Node::new(1);
    let source = source(&node);
    let pair = Pair::new(false);
    let (_importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let config = cfg(&node);
    let coordinator = config.eoe.coordinator;
    let mut owner = owner(&node, &source, &pair, config);
    let pool = pool(&node);
    let mut building = owner
        .micro_candidate(accepted.producer(), pool.client(), input(&node))
        .unwrap();
    prepared(&mut owner, &mut building);
    let count = building.pending_effects().unwrap().len();
    assert!(
        count >= 2,
        "expected multiple entropy notifications: {count}"
    );
    assert!(building.candidate().unwrap().effects.entries().is_empty());
    let end = Instant::now() + Duration::from_secs(5);
    while building.acknowledged_effects() == 0 {
        assert!(Instant::now() < end);
        step(&mut owner);
        building.step(&mut owner, 999).unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(!building.effect_in_flight());
    assert_eq!(pool.info().submitted, 0);
    unit(
        owner
            .configure_runtime(coordinator::Config {
                maximum_queued_events: 0,
                ..coordinator
            })
            .unwrap(),
    );
    let expected = build::Error::Effects(delivery::Error::Eoe(coordinator::Error::ResourceLimit));
    loop {
        assert!(Instant::now() < end);
        step(&mut owner);
        match building.step(&mut owner, 999) {
            Err(e) => {
                assert_eq!(e, expected);
                break;
            }
            Ok(_) => std::thread::sleep(Duration::from_millis(1)),
        }
    }
    let pending = building.pending_effects().unwrap().clone();
    assert_eq!(pending.len(), count - 1);
    assert_eq!(building.acknowledged_effects(), 1);
    assert_eq!(building.step(&mut owner, 999), Err(expected));
    assert_eq!(building.pending_effects().unwrap(), &pending);
    assert_eq!(pool.info().submitted, 0);
    unit(owner.configure_runtime(coordinator).unwrap());
    building.retry().unwrap();
    drive(&mut owner, &mut building, 999);
    assert_eq!(building.acknowledged_effects(), count as u64);
    let completed = building.finish().unwrap();
    assert_eq!(completed.acknowledged_effects, count as u64);
    assert!(completed.candidate.effects.entries().is_empty());
    assert!(completed.candidate.outcome.is_ok());
    assert!(pool.info().submitted > 0);
    drop(completed);
    assert!(owner.shutdown().errors.is_empty());
    pool.shutdown().unwrap();
}

#[test]
fn failed_candidate_creation_delivers_retained_effect_prefix_without_selecting_pool() {
    let node = Node::new(1);
    let source = source(&node);
    let pair = Pair::new(true);
    let (_importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let mut owner = owner(&node, &source, &pair, cfg(&node));
    let pool = pool(&node);
    let request = input(&node);
    let before = node.db.view().unwrap().status();
    let mut building = owner
        .micro_candidate(accepted.producer(), pool.client(), request)
        .unwrap();
    prepared(&mut owner, &mut building);
    assert!(building.candidate().unwrap().outcome.is_err());
    assert_eq!(
        building
            .pending_effects()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        vec![ae_chain::repository::hyperchain::inputs::Effect::RequestParent(1.into()); 2]
    );
    assert!(!building.ready());
    drive(&mut owner, &mut building, 999);
    let completed = building.finish().unwrap();
    assert!(completed.candidate.outcome.is_err());
    assert!(completed.candidate.effects.entries().is_empty());
    assert_eq!(completed.acknowledged_effects, 2);
    assert!(!completed.is_eoe);
    assert_eq!(completed.refreshes, 0);
    assert_eq!(pool.info().submitted, 0);
    assert_eq!(node.db.view().unwrap().status(), before);
    assert!(owner.shutdown().errors.is_empty());
    pool.shutdown().unwrap();
}
