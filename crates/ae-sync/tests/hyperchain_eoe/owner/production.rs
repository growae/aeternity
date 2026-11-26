use super::*;
use ae_hyperchain::producer::Decision;
use ae_sync::chain::{
    Importer,
    hyperchain::{delivery, eoe::owner::production},
};
use common::hyperchain::parents::Pair;
use common::mining_metrics::MiningMetrics;
use production::{Attempt, Config, Kind, Outcome, Progress};
use std::time::Instant;
fn config() -> Config {
    Config {
        producer: managed::producer::Config {
            production_time: 1.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}
fn drive<O: ae_vm_fate::maps::MapOrder + Clone + Send + Sync + 'static>(
    owner: &mut managed::Owner,
    importer: &Importer<O>,
    attempt: &mut Attempt<O>,
    parents: &mut delivery::Delivery<O, live::StateReader, live::StateReader>,
    mut wall: u64,
) -> Result<Vec<Progress>, production::Error> {
    let end = Instant::now() + Duration::from_secs(15);
    let mut clock = Instant::now();
    let mut progress = vec![];
    while !attempt.ready() {
        assert!(Instant::now() < end, "stalled {attempt:?}");
        step(owner);
        parents.step().unwrap();
        let p = attempt.step(owner, importer, wall, clock)?;
        if p == Progress::Waiting
            && let Some(deadline) = attempt.deadline()
        {
            wall += deadline.saturating_duration_since(clock).as_millis() as u64;
            clock = deadline;
        }
        progress.push(p);
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(progress)
}
#[tokio::test]
async fn empty_micro_is_skipped_and_real_producer_seals_and_admits_a_key() {
    for index in [1, 3, 5, 6] {
        let node = Node::new(index);
        let (metrics, _binding) = MiningMetrics::bind(node.db.instrumentation());
        let source = source(&node);
        let pair = Pair::new(false);
        let (importer, accepted) = Importer::new_hyperchain_scoped(
            node.db.clone(),
            node.config.clone(),
            pair.inputs(),
            Default::default(),
        )
        .unwrap();
        let producer = accepted.producer();
        let mut owner = candidate::owner(&node, &source, &pair, cfg(&node));
        let mut parents = owner.parent_delivery(accepted).unwrap();
        let pool = candidate::pool(&node);
        let original = node.db.view().unwrap().status().tip.unwrap();
        let mut attempt = owner
            .begin_production(producer.clone(), pool.client(), config())
            .unwrap();
        assert!(matches!(
            owner.begin_production(producer.clone(), pool.client(), config()),
            Err(production::Error::Busy)
        ));
        assert_eq!(
            owner.prepare_producer(
                &producer,
                managed::producer::Input {
                    now: 1000.into(),
                    config: Default::default()
                }
            ),
            Err(managed::Error::Backpressured)
        );
        let progress = drive(&mut owner, &importer, &mut attempt, &mut parents, 1000).unwrap();
        assert!(progress.contains(&Progress::MicroSkipped));
        assert!(!progress.contains(&Progress::Submitted(Kind::Micro)));
        assert!(progress.contains(&Progress::Accepted(Kind::Key)));
        let done = attempt.finish().unwrap();
        assert_eq!(done.original_parent, original);
        assert_eq!(done.parent, original);
        assert_eq!(done.skipped, Some(production::Skipped::Empty));
        let Outcome::Produced {
            micro,
            key,
            is_eoe,
            hole,
        } = done.outcome
        else {
            panic!()
        };
        assert!(micro.is_none() && !is_eoe && !hole);
        assert_eq!(node.db.view().unwrap().status().tip, Some(key.hash));
        let Block::Key(block) = node.db.view().unwrap().require_block(key.hash).unwrap() else {
            panic!()
        };
        assert_eq!(block.common.previous_hash, original.0);
        assert_eq!(block.common.height, 1);
        ae_hyperchain::signing::verify_key(&block).unwrap();
        assert_eq!(metrics.counts(), (1, 0));
        assert!(metrics.key_info().is_empty());
        drop(
            owner
                .begin_production(producer, pool.client(), config())
                .unwrap(),
        );
        drop(parents);
        assert!(owner.shutdown().errors.is_empty());
        pool.shutdown().unwrap();
        importer.stop();
        importer.join().await;
    }
}
#[tokio::test]
async fn selected_epoch_end_produces_vote_micro_then_key_using_the_admitted_micro_parent() {
    let mut admitted = 0;
    let mut rejected = 0;
    for profile in ["profile", "repeat_profile"] {
        for index in [1, 3, 5, 6] {
            for with_spend in [false, true] {
                let mut node = Node::new(index);
                Arc::make_mut(&mut node.config.profile).order =
                    common::hyperchain::order(&candidate::DATA[profile]);
                let storage = inserted(&node);
                for b in node.blocks().iter().take(4) {
                    storage.insert(b, 1_000_000, Origin::Gossip).unwrap();
                }
                drop(storage);
                // This checks successful EOE admission and parent selection.
                // A 4 ms negotiation can legitimately lose quorum when the
                // host preempts the worker. Keep importer and EOE timing bound
                // to the same value; epoch 1's length delta remains zero.
                node.config.rules.consensus.child_block_time = 1000.into();
                let source = source(&node);
                let pair = Pair::new(false);
                let (importer, accepted) = Importer::new_hyperchain_scoped(
                    node.db.clone(),
                    node.config.clone(),
                    pair.inputs(),
                    Default::default(),
                )
                .unwrap();
                let producer = accepted.producer();
                let mut settings = cfg(&node);
                settings.eoe.block_time = 1000.into();
                let mut owner = candidate::configured_owner(&node, &source, &pair, settings);
                let mut parents = owner.parent_delivery(accepted).unwrap();
                let pool = candidate::pool(&node);
                if with_spend {
                    let row = candidate::DATA["rows"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|r| r["corpus_index"] == index)
                        .unwrap();
                    let bytes = hex::decode(
                        row["parents"][1]["cases"][4]["transactions"][0]
                            .as_str()
                            .unwrap(),
                    )
                    .unwrap();
                    pool.client()
                        .force_push(Arc::new(SignedTransaction::decode(&bytes).unwrap()))
                        .unwrap()
                        .wait()
                        .unwrap();
                }
                let original = node.db.view().unwrap().status().tip.unwrap();
                let mut attempt = owner
                    .begin_production(producer, pool.client(), config())
                    .unwrap();
                let result = drive(&mut owner, &importer, &mut attempt, &mut parents, 25003);
                if index == 6 {
                    let e = result.unwrap_err();
                    assert!(
                        matches!(
                            e,
                            production::Error::Admission(ae_sync::chain::Failure {
                                error: ae_sync::chain::Error::Validation {
                                    operational: false,
                                    ..
                                },
                                ..
                            })
                        ),
                        "{e:?}"
                    );
                    assert_eq!(attempt.parent(), original);
                    assert!(attempt.accepted_micro().is_none());
                    assert!(attempt.key_candidate().is_none());
                    assert_eq!(attempt.retry(), Err(e));
                    rejected += 1;
                } else {
                    let progress = result.unwrap();
                    assert_eq!(
                        progress
                            .iter()
                            .filter(|p| matches!(
                                p,
                                Progress::Micro(managed::candidate::Progress::Requested {
                                    refresh: true
                                })
                            ))
                            .count(),
                        usize::from(with_spend)
                    );
                    let micro = progress
                        .iter()
                        .position(|p| *p == Progress::Accepted(Kind::Micro))
                        .unwrap();
                    let key = progress
                        .iter()
                        .position(|p| *p == Progress::KeyPrepared)
                        .unwrap();
                    assert!(micro < key);
                    let done = attempt.finish().unwrap();
                    let Outcome::Produced {
                        micro: Some(micro),
                        key,
                        is_eoe,
                        hole,
                    } = done.outcome
                    else {
                        panic!()
                    };
                    assert!(is_eoe && !hole);
                    let Block::Micro(block) =
                        node.db.view().unwrap().require_block(micro.hash).unwrap()
                    else {
                        panic!()
                    };
                    assert_eq!(
                        block.transactions.len(),
                        2 + usize::from(with_spend),
                        "profile {profile}, corpus {index}, spend {with_spend}, progress {progress:?}"
                    );
                    assert_eq!(done.parent, micro.hash);
                    let Block::Key(block) =
                        node.db.view().unwrap().require_block(key.hash).unwrap()
                    else {
                        panic!()
                    };
                    assert_eq!(block.common.previous_hash, micro.hash.0);
                    assert!(block.end_of_epoch);
                    assert_eq!(node.db.view().unwrap().status().tip, Some(key.hash));
                    admitted += 1;
                }
                drop(parents);
                assert!(owner.shutdown().errors.is_empty());
                pool.shutdown().unwrap();
                importer.stop();
                importer.join().await;
            }
        }
    }
    assert_eq!((admitted, rejected), (12, 4));
}
fn decided<O: ae_vm_fate::maps::MapOrder + Clone + Send + Sync + 'static>(
    owner: &mut managed::Owner,
    importer: &Importer<O>,
    attempt: &mut Attempt<O>,
    wall: u64,
    tick: Instant,
) {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < end);
        step(owner);
        if attempt.step(owner, importer, wall, tick).unwrap() == Progress::Decided {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}
#[tokio::test]
async fn normal_leader_keeps_its_parent_and_delayed_holes_reconsider_the_latest_tip() {
    for delayed_hole in [false, true] {
        let node = Node::new(1);
        if delayed_hole {
            let storage = inserted(&node);
            for b in node.blocks().iter().take(2) {
                storage.insert(b, 1_000_000, Origin::Gossip).unwrap();
            }
        }
        let source = source(&node);
        let pair = Pair::new(false);
        let (importer, accepted) = Importer::new_hyperchain_scoped(
            node.db.clone(),
            node.config.clone(),
            pair.inputs(),
            Default::default(),
        )
        .unwrap();
        let producer = accepted.producer();
        let mut owner = candidate::owner(&node, &source, &pair, cfg(&node));
        let mut parents = owner.parent_delivery(accepted).unwrap();
        let pool = candidate::pool(&node);
        let original = node.db.view().unwrap().status().tip.unwrap();
        let tick = Instant::now();
        let wall = if delayed_hole { 15004 } else { 1000 };
        let mut attempt = owner
            .begin_production(producer, pool.client(), config())
            .unwrap();
        decided(&mut owner, &importer, &mut attempt, wall, tick);
        assert!(
            matches!(&attempt.decision().unwrap().outcome, Ok(Decision::Produce { holes, .. }) if holes.is_some() == delayed_hole)
        );
        let deadline = attempt.deadline().unwrap();
        assert_eq!(deadline.duration_since(tick), Duration::from_millis(3));
        assert_eq!(
            attempt
                .step(
                    &mut owner,
                    &importer,
                    wall + 2,
                    deadline - Duration::from_millis(1)
                )
                .unwrap(),
            Progress::Waiting
        );
        assert_eq!(pool.info().submitted, 0);
        let before = if delayed_hole { 2 } else { 0 };
        for b in node.blocks().iter().skip(before).take(2) {
            importer
                .import_block(b.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait()
                .await
                .unwrap();
        }
        let latest = node.db.view().unwrap().status().tip.unwrap();
        assert_ne!(latest, original);
        assert_eq!(
            attempt
                .step(&mut owner, &importer, wall + 3, deadline)
                .unwrap(),
            Progress::Awoke
        );
        assert_eq!(
            attempt.parent(),
            if delayed_hole { latest } else { original }
        );
        if delayed_hole {
            decided(&mut owner, &importer, &mut attempt, wall + 3, deadline);
            assert_eq!(attempt.decision().unwrap().status.tip, Some(latest));
            assert_eq!(attempt.decision().unwrap().key_height, 2);
            assert!(matches!(
                attempt.decision().unwrap().outcome,
                Ok(Decision::Produce { holes: None, .. })
            ));
            assert_eq!(pool.info().submitted, 0);
            assert!(attempt.pending_block().is_none());
            attempt.cancel();
            drop(attempt);
            drop(parents);
            assert!(owner.shutdown().errors.is_empty());
            pool.shutdown().unwrap();
            importer.stop();
            importer.join().await;
            continue;
        }
        // drive() uses a fresh clock; retain monotonic ordering explicitly here.
        let end = Instant::now() + Duration::from_secs(10);
        let mut clock = deadline;
        let mut wall = wall + 3;
        while !attempt.ready() {
            assert!(Instant::now() < end, "{attempt:?}");
            step(&mut owner);
            parents.step().unwrap();
            let progress = attempt.step(&mut owner, &importer, wall, clock).unwrap();
            if progress == Progress::Waiting
                && let Some(next) = attempt.deadline()
            {
                wall += next.saturating_duration_since(clock).as_millis() as u64;
                clock = next;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let done = attempt.finish().unwrap();
        if delayed_hole {
            assert_eq!(done.decisions, 2);
            assert!(matches!(done.outcome, Outcome::Wait(_)));
            assert_eq!(node.db.view().unwrap().status().tip, Some(latest));
            assert_eq!(pool.info().submitted, 0);
        } else {
            assert_eq!(done.decisions, 1);
            let Outcome::Produced { key, .. } = done.outcome else {
                panic!()
            };
            assert_eq!(
                node.db
                    .view()
                    .unwrap()
                    .require_block(key.hash)
                    .unwrap()
                    .header()
                    .common()
                    .previous_hash,
                original.0
            );
        }
        drop(parents);
        assert!(owner.shutdown().errors.is_empty());
        pool.shutdown().unwrap();
        importer.stop();
        importer.join().await;
    }
}
#[tokio::test]
async fn delayed_and_immediate_holes_build_one_key_without_micro_or_pool_selection() {
    for wall in [15004, 15007] {
        let node = Node::new(1);
        let storage = inserted(&node);
        for b in node.blocks().iter().take(2) {
            storage.insert(b, 1_000_000, Origin::Gossip).unwrap();
        }
        drop(storage);
        let source = source(&node);
        let pair = Pair::new(false);
        let (importer, accepted) = Importer::new_hyperchain_scoped(
            node.db.clone(),
            node.config.clone(),
            pair.inputs(),
            Default::default(),
        )
        .unwrap();
        let producer = accepted.producer();
        let mut owner = candidate::owner(&node, &source, &pair, cfg(&node));
        let mut parents = owner.parent_delivery(accepted).unwrap();
        let pool = candidate::pool(&node);
        let before = node.db.view().unwrap().status();
        let mut attempt = owner
            .begin_production(producer, pool.client(), config())
            .unwrap();
        drive(&mut owner, &importer, &mut attempt, &mut parents, wall).unwrap();
        let done = attempt.finish().unwrap();
        assert_eq!(done.decisions, if wall == 15004 { 2 } else { 1 });
        let Outcome::Produced {
            micro,
            key,
            hole,
            is_eoe,
        } = done.outcome
        else {
            panic!()
        };
        assert!(micro.is_none() && hole && !is_eoe);
        let Block::Key(block) = node.db.view().unwrap().require_block(key.hash).unwrap() else {
            panic!()
        };
        assert!(block.hole);
        assert_eq!(block.common.height, 2);
        assert_eq!(block.common.previous_hash, before.tip.unwrap().0);
        assert_eq!(
            node.db.view().unwrap().status().revision,
            before.revision + 1
        );
        assert_eq!(pool.info().submitted, 0);
        drop(parents);
        assert!(owner.shutdown().errors.is_empty());
        pool.shutdown().unwrap();
        importer.stop();
        importer.join().await;
    }
}

#[tokio::test]
async fn admission_backpressure_retains_signed_block_and_explicit_retry_submits_it_once() {
    let node = Node::new(1);
    let (metrics, _binding) = MiningMetrics::bind(node.db.instrumentation());
    let source = source(&node);
    let pair = Pair::new(false);
    let (importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        ae_sync::chain::Config {
            jobs: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let producer = accepted.producer();
    let mut owner = candidate::owner(&node, &source, &pair, cfg(&node));
    let mut parents = owner.parent_delivery(accepted).unwrap();
    let pool = candidate::pool(&node);
    let before = node.db.view().unwrap().status();
    let block = node
        .db
        .view()
        .unwrap()
        .require_block(before.tip.unwrap())
        .unwrap();
    let held = importer
        .import_created_block(block.encode().unwrap(), 1_000_000)
        .unwrap();
    let mut attempt = owner
        .begin_production(producer, pool.client(), config())
        .unwrap();
    let e = drive(&mut owner, &importer, &mut attempt, &mut parents, 1000).unwrap_err();
    assert_eq!(e, production::Error::Source(ae_sync::chain::Error::Busy));
    assert!(!attempt.admission_in_flight());
    let signed = attempt.pending_block().unwrap().clone();
    assert_eq!(node.db.view().unwrap().status(), before);
    assert_eq!(metrics.counts(), (1, 0));
    held.wait().await.unwrap();
    attempt.retry().unwrap();
    drive(&mut owner, &importer, &mut attempt, &mut parents, 1003).unwrap();
    let done = attempt.finish().unwrap();
    assert_eq!(metrics.counts(), (1, 0));
    assert!(metrics.key_info().is_empty());
    let Outcome::Produced { key, .. } = done.outcome else {
        panic!()
    };
    assert_eq!(key.hash, signed.hash().unwrap());
    assert_eq!(
        node.db.view().unwrap().require_block(key.hash).unwrap(),
        signed
    );
    assert_eq!(
        node.db.view().unwrap().status().revision,
        before.revision + 1
    );
    drop(parents);
    assert!(owner.shutdown().errors.is_empty());
    pool.shutdown().unwrap();
    importer.stop();
    importer.join().await;
}
#[tokio::test]
async fn missing_parent_returns_native_retry_and_retired_importers_or_cancelled_attempts_stay_terminal()
 {
    let node = Node::new(1);
    let source = source(&node);
    let pair = Pair::new(true);
    let (importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let producer = accepted.producer();
    let mut owner = candidate::owner(&node, &source, &pair, cfg(&node));
    let mut parents = owner.parent_delivery(accepted).unwrap();
    let pool = candidate::pool(&node);
    let mut attempt = owner
        .begin_production(producer.clone(), pool.client(), config())
        .unwrap();
    drive(&mut owner, &importer, &mut attempt, &mut parents, 1000).unwrap();
    assert_eq!(attempt.finish().unwrap().outcome, Outcome::RetryNotInCache);
    assert_eq!(pool.info().submitted, 0);
    let mut attempt = owner
        .begin_production(producer.clone(), pool.client(), config())
        .unwrap();
    attempt.cancel();
    assert_eq!(
        attempt.step(&mut owner, &importer, 1000, Instant::now()),
        Err(production::Error::Cancelled)
    );
    assert_eq!(attempt.retry(), Err(production::Error::Cancelled));
    drop(attempt);
    let mut attempt = owner
        .begin_production(producer, pool.client(), config())
        .unwrap();
    importer.stop();
    importer.join().await;
    assert_eq!(
        attempt.step(&mut owner, &importer, 1000, Instant::now()),
        Err(production::Error::Source(ae_sync::chain::Error::Closed))
    );
    assert_eq!(
        attempt.retry(),
        Err(production::Error::Source(ae_sync::chain::Error::Closed))
    );
    drop(parents);
    assert!(owner.shutdown().errors.is_empty());
    pool.shutdown().unwrap();
}

#[tokio::test]
async fn original_conductor_scheduling_key_bytes_and_native_exits_match_both_otp_profiles() {
    let data: serde_json::Value =
        serde_json::from_str(include_str!("../../fixtures/erlang_hc_production.json")).unwrap();
    let mut compared = 0;
    let mut keys = 0;
    for profile in ["profile", "repeat_profile"] {
        for row in data["rows"].as_array().unwrap() {
            for c in row["cases"].as_array().unwrap() {
                let mut node = Node::new(row["corpus_index"].as_u64().unwrap() as usize);
                Arc::make_mut(&mut node.config.profile).order =
                    common::hyperchain::order(&data[profile]);
                let storage = inserted(&node);
                for b in node
                    .blocks()
                    .iter()
                    .take(c["count"].as_u64().unwrap() as usize)
                {
                    storage.insert(b, 1_000_000, Origin::Gossip).unwrap();
                }
                drop(storage);
                let source = source(&node);
                let pair = Pair::new(c["missing"].as_bool().unwrap());
                let (importer, accepted) = Importer::new_hyperchain_scoped(
                    node.db.clone(),
                    node.config.clone(),
                    pair.inputs(),
                    Default::default(),
                )
                .unwrap();
                let producer = accepted.producer();
                let mut cfg = cfg(&node);
                cfg.eoe.block_time = 4.into();
                let mut i = inputs(&node, &source, &cfg);
                i.parent = Some(pair.client.clone());
                let mut owner = managed::Owner::start(
                    i,
                    cfg,
                    c["owned"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|n| {
                            let k = signer(n.as_u64().unwrap());
                            (k.public_key(), k)
                        })
                        .collect(),
                    node.config.profile.order.clone(),
                )
                .unwrap();
                let mut parents = owner.parent_delivery(accepted).unwrap();
                let pool = candidate::pool(&node);
                let mut attempt = owner
                    .begin_production(producer, pool.client(), config())
                    .unwrap();
                let mut wall = c["now"].as_u64().unwrap();
                let mut clock = Instant::now();
                let end = clock + Duration::from_secs(15);
                let mut sleeps = vec![];
                let mut micro = 0;
                while !attempt.ready() {
                    assert!(Instant::now() < end, "native case {c} stalled {attempt:?}");
                    step(&mut owner);
                    parents.step().unwrap();
                    let p = attempt
                        .step(&mut owner, &importer, wall, clock)
                        .unwrap_or_else(|e| {
                            panic!("native {} {} {e:?}", row["protocol"], c["name"])
                        });
                    if p == Progress::Waiting
                        && let Some(next) = attempt.deadline()
                    {
                        let ms = next.saturating_duration_since(clock).as_millis() as u64;
                        if ms > 0 {
                            sleeps.push(ms);
                        }
                        wall += ms;
                        clock = next;
                    }
                    if p == Progress::MicroSkipped {
                        micro += 1;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                let done = attempt.finish().unwrap();
                assert_eq!(
                    sleeps,
                    c["sleeps"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|x| x.as_u64().unwrap())
                        .collect::<Vec<_>>(),
                    "{}",
                    c["name"]
                );
                assert_eq!(wall, c["final_now"].as_u64().unwrap());
                assert_eq!(hex::encode(done.original_parent.0), c["parent"]);
                assert_eq!(done.parent, done.original_parent);
                assert_eq!(micro, c["micro"].as_array().unwrap().len());
                if c["result"]["kind"] == "key" {
                    let Outcome::Produced {
                        micro, key, is_eoe, ..
                    } = done.outcome
                    else {
                        panic!("{c}")
                    };
                    assert!(micro.is_none() && !is_eoe);
                    let signed = node.db.view().unwrap().require_block(key.hash).unwrap();
                    assert_eq!(
                        hex::encode(signed.encode().unwrap()),
                        c["result"]["signed"],
                        "{}",
                        c["name"]
                    );
                    let Block::Key(mut header) = signed else {
                        panic!()
                    };
                    assert_eq!(
                        hex::encode(header.common.state_hash),
                        c["result"]["state_root"]
                    );
                    header.seal = [0; 42];
                    assert_eq!(
                        hex::encode(Block::Key(header).encode().unwrap()),
                        c["result"]["block"]
                    );
                    assert_eq!(node.db.view().unwrap().status().tip, Some(key.hash));
                    if c["name"] == "micro_parent" {
                        assert!(matches!(
                            done.skipped,
                            Some(production::Skipped::Failed { .. })
                        ));
                    }
                    keys += 1;
                } else {
                    let expected = if c["name"] == "not_producer" {
                        Outcome::Wait(ae_hyperchain::producer::Wait::NotProducer)
                    } else if c["name"] == "missing_parent" {
                        Outcome::RetryNotInCache
                    } else {
                        Outcome::Wait(ae_hyperchain::producer::Wait::SlotFilled)
                    };
                    assert_eq!(done.outcome, expected, "{c}");
                    assert_eq!(
                        node.db.view().unwrap().status().tip,
                        Some(done.original_parent)
                    );
                }
                drop(parents);
                assert!(owner.shutdown().errors.is_empty());
                pool.shutdown().unwrap();
                importer.stop();
                importer.join().await;
                compared += 1;
            }
        }
    }
    assert_eq!((compared, keys), (72, 40));
}

#[tokio::test]
async fn cancelled_attempt_transfers_unread_admission_for_reconciliation_without_retry() {
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
    let producer = accepted.producer();
    let mut owner = candidate::owner(&node, &source, &pair, cfg(&node));
    let mut parents = owner.parent_delivery(accepted).unwrap();
    let pool = candidate::pool(&node);
    let mut attempt = owner
        .begin_production(producer, pool.client(), config())
        .unwrap();
    let mut clock = Instant::now();
    let end = clock + Duration::from_secs(10);
    let mut wall = 1000;
    loop {
        assert!(Instant::now() < end);
        step(&mut owner);
        parents.step().unwrap();
        let progress = attempt.step(&mut owner, &importer, wall, clock).unwrap();
        if progress == Progress::Submitted(Kind::Key) {
            break;
        }
        if progress == Progress::Waiting
            && let Some(next) = attempt.deadline()
        {
            wall += next.saturating_duration_since(clock).as_millis() as u64;
            clock = next;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let hash = attempt.pending_block().unwrap().hash().unwrap();
    assert!(attempt.take_abandoned_admission().is_none());
    assert!(attempt.admission_in_flight());
    attempt.cancel();
    let receipt = attempt.take_abandoned_admission().unwrap();
    assert!(!attempt.admission_in_flight());
    assert!(attempt.take_abandoned_admission().is_none());
    assert_eq!(attempt.retry(), Err(production::Error::Cancelled));
    match receipt.wait().await {
        Ok(report) => {
            assert_eq!(report.accepted.len(), 1);
            assert_eq!(report.accepted[0].hash, hash);
            assert!(node.db.view().unwrap().block(hash).unwrap().is_some());
        }
        Err(f) => {
            assert_eq!(f.error, ae_sync::chain::Error::Cancelled);
            assert!(f.progress.is_none_or(|r| r.accepted.is_empty()));
        }
    }
    assert_eq!(
        attempt.step(&mut owner, &importer, wall, clock),
        Err(production::Error::Cancelled)
    );
    drop(attempt);
    drop(parents);
    assert!(owner.shutdown().errors.is_empty());
    pool.shutdown().unwrap();
    importer.stop();
    importer.join().await;
}
