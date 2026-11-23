use super::*;
use conductor::supervision::Notice;

fn native(kind: &str) -> serde_json::Value {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../ae-hyperchain/tests/fixtures/erlang_eoe_pool_lifetime.json"
    )))
    .unwrap();
    fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == kind)
        .unwrap()["result"]
        .clone()
}
fn drive(
    c: &mut Conductor<Otp26>,
    now: Instant,
    outputs: &mut Vec<Output>,
) -> Result<Progress, conductor::Error> {
    let progress = c.step(1_000_000, now)?;
    if let Some(output) = c.take_output() {
        outputs.push(output);
    }
    Ok(progress)
}
fn replaced(c: &mut Conductor<Otp26>, now: Instant, generation: u64, outputs: &mut Vec<Output>) {
    let end = Instant::now() + Duration::from_secs(5);
    while c.status().consensus.eoe_generation < generation
        || c.status().consensus.pending_notices > 0
    {
        drive(c, now, outputs).unwrap();
        assert!(Instant::now() < end, "{:?}", c.status());
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[tokio::test]
async fn eoe_replacement_preserves_pool_epochs_importer_and_parent_siblings() {
    let node = Node::new(0);
    let mut c = start(&node, false);
    let old = c.eoe();
    let importer = c.importer();
    let cache = c.parent();
    let connector = c.connector();
    let votes = c.votes();
    let hub = old.independent_pool().unwrap().clone();
    let pool = hub.client().unwrap();
    let signed = vote(&node, eoe::Mode::Fork, eoe::VoteType::Proposal, 13);
    common::within(
        votes
            .push(signed.encode(), hc_votes::Event::Received)
            .unwrap(),
    )
    .await
    .unwrap();
    old.request_stop();
    let now = Instant::now();
    let mut outputs = vec![];
    replaced(&mut c, now, 2, &mut outputs);
    let next = c.eoe();
    assert!(!old.same_runtime(&next));
    assert!(pool.same_pool(&next.independent_pool().unwrap().client().unwrap()));
    assert_eq!(pool.inspect(|p| p.len()).unwrap(), 1);
    assert_eq!(native("eoe_normal")["pool_same"], true);
    assert!(Arc::ptr_eq(&importer, &c.importer()));
    assert!(cache.same_runtime(&c.parent()));
    connector.barrier(Duration::from_secs(5)).unwrap();
    assert_eq!(c.status().consensus.connector_generation, 1);
    assert!(votes.info().accepting && c.pool().info().accepting);
    old.request_stop();
    assert!(!next.is_closed());
    let retired = outputs
        .iter()
        .find_map(|o| match o {
            Output::ConsensusSupervision(Notice::EoeRetired { child, .. }) => Some(child),
            _ => None,
        })
        .unwrap();
    assert!(retired.child.runtime.result.is_ok());
    assert!(retired.child.requests.is_empty());
    assert_eq!(c.status().consensus.attempts, 1);
    for block in node.blocks().iter().take(6) {
        importer
            .import_block(block.encode().unwrap(), 1_000_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    let revision = node.db.events().cursor().revision();
    let end = Instant::now() + Duration::from_secs(5);
    while c.status().epoch_revision < revision {
        drive(&mut c, now, &mut outputs).unwrap();
        assert!(Instant::now() < end);
    }
    assert_eq!(
        pool.inspect(|p| (p.epoch().clone(), p.len())).unwrap(),
        (2.into(), 1)
    );
    node.db.view().unwrap().verify_tip().unwrap();
    drop(shutdown(c));
}

#[test]
fn active_production_settles_before_resuming_after_eoe_replacement() {
    for stage in [
        Progress::Started,
        Progress::Production(production::Progress::DecisionRequested),
        Progress::Production(production::Progress::KeyPrepared),
        Progress::Production(production::Progress::Submitted(production::Kind::Key)),
    ] {
        let node = Node::new(1);
        let mut c = start(&node, true);
        let mut wall = 1000;
        let mut now = Instant::now();
        let end = now + Duration::from_secs(25);
        loop {
            let progress = tick(&mut c, &mut wall, &mut now);
            c.take_output();
            if progress == stage {
                break;
            }
            assert!(Instant::now() < end, "{stage:?}: {:?}", c.status());
        }
        let old = c.eoe();
        old.request_stop();
        let mut stopped = None;
        let mut produced = None;
        while produced.is_none() {
            tick(&mut c, &mut wall, &mut now);
            if let Some(output) = c.take_output() {
                match output {
                    Output::Stopped(s) => stopped = Some(s),
                    Output::Attempt(done)
                        if matches!(done.outcome, production::Outcome::Produced { .. }) =>
                    {
                        produced = Some(done)
                    }
                    _ => (),
                }
            }
            assert!(Instant::now() < end, "{stage:?}: {:?}", c.status());
        }
        let stopped = stopped.expect("old attempt settled");
        let production::Outcome::Produced { key, .. } = produced.unwrap().outcome else {
            unreachable!()
        };
        let view = node.db.view().unwrap();
        view.verify_tip().unwrap();
        assert_eq!(view.status().tip, Some(key.hash));
        if let Some(Ok(accepted)) = stopped.admission {
            assert_ne!(accepted.accepted[0].hash, key.hash);
            assert!(view.require_block(accepted.accepted[0].hash).is_ok());
        }
        assert!(!c.eoe().same_runtime(&old));
        assert_eq!(c.status().consensus.eoe_generation, 2);
        assert_eq!(c.status().consensus.attempts, 1);
        assert!(c.status().producing);
        drop(shutdown(c));
    }
}

#[test]
fn eoe_uses_the_shared_consensus_budget_at_native_inclusive_thirty_second_boundary() {
    for mixed in [false, true] {
        for last in [30, 31] {
            let node = Node::new(0);
            let mut c = start(&node, false);
            let now = Instant::now();
            let mut outputs = vec![];
            let pool = c.eoe().independent_pool().unwrap().client().unwrap();
            let native = native("consensus_intensity");
            for attempt in 1..=4 {
                if mixed && attempt == 2 {
                    c.parent().request_stop();
                } else if mixed && attempt == 3 {
                    c.connector().request_stop();
                } else {
                    c.eoe().request_stop();
                }
                let time = now + Duration::from_secs(if attempt == 4 { last } else { 0 });
                let end = Instant::now() + Duration::from_secs(5);
                let stopped = loop {
                    match drive(&mut c, time, &mut outputs) {
                        Err(conductor::Error::ConsensusRestartLimit) => break true,
                        Err(e) => panic!("{e:?}"),
                        Ok(_) => (),
                    }
                    if c.status().consensus.attempts == attempt && !c.status().consensus.restarting
                    {
                        break false;
                    }
                    assert!(Instant::now() < end, "{:?}", c.status());
                    std::thread::sleep(Duration::from_millis(1));
                };
                if last == 30 {
                    assert_eq!(
                        !stopped,
                        native["trace"][(attempt - 1) as usize]["consensus_alive"]
                            .as_bool()
                            .unwrap()
                    );
                } else {
                    assert!(!stopped);
                }
                assert!(pool.same_pool(&c.eoe().independent_pool().unwrap().client().unwrap()));
                assert!(c.votes().info().accepting && c.pool().info().accepting);
            }
            assert_eq!(c.status().consensus.attempts, 4);
            assert_eq!(
                c.status().failure,
                (last == 30).then_some(conductor::Error::ConsensusRestartLimit)
            );
            drop(shutdown(c));
        }
    }
}

#[test]
fn retained_eoe_notice_backpressures_restart_and_shutdown_preserves_gap() {
    let node = Node::new(0);
    let mut c = start(&node, false);
    c.eoe().request_stop();
    let now = Instant::now();
    let end = now + Duration::from_secs(5);
    while c.output().is_none() {
        c.step(1000, now).unwrap();
        assert!(Instant::now() < end);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(matches!(
        c.output(),
        Some(Output::ConsensusSupervision(Notice::EoeRetired {
            generation: 1,
            ..
        }))
    ));
    assert_eq!(c.status().consensus.attempts, 0);
    assert!(c.status().consensus.eoe_restarting);
    let exit = shutdown(c);
    assert!(exit.owner.errors.is_empty());
    assert!(exit.owner.retirement.is_none());
    assert_eq!(exit.consensus.status.attempts, 0);
}

#[tokio::test]
async fn dedicated_conductor_runtime_recovers_eoe_without_replacing_importer() {
    let node = Node::new(0);
    let c = start(&node, false);
    let old = c.eoe();
    let importer = c.importer();
    let runtime =
        conductor::runtime::Runtime::spawn(c, conductor::runtime::SystemClock, Default::default())
            .unwrap_or_else(|_| panic!("spawn conductor"));
    let handle = runtime.handle();
    old.request_stop();
    wait(|| handle.status().consensus.eoe_generation == 2 || handle.status().failure.is_some());
    assert!(handle.status().failure.is_none(), "{:?}", handle.status());
    importer
        .import_block(node.blocks()[0].encode().unwrap(), 1_000_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    node.check(0);
    let exit = runtime.shutdown().unwrap();
    assert!(exit.owner.errors.is_empty());
    assert_eq!(exit.consensus.status.attempts, 1);
    assert!(old.is_closed() && handle.joined());
}

#[test]
fn native_inner_eoe_exhaustion_restarts_outer_child_and_keeps_malformed_pool_entry() {
    let node = Node::new(0);
    let mut c = start(&node, false);
    let old = c.eoe();
    let pool = old.independent_pool().unwrap().client().unwrap();
    let original = vote(&node, eoe::Mode::Fork, eoe::VoteType::Proposal, 13);
    let ae_tx::TransactionLeaf::HcVote(vote) = original.transaction().view().leaf().clone() else {
        unreachable!()
    };
    let mut fields: eoe::Payload = vote
        .data()
        .iter()
        .map(|(k, v)| (k.clone(), eoe::Field::Binary(v.clone())))
        .collect();
    fields.insert(
        eoe::payload::SIGNATURE.to_vec(),
        eoe::Field::Binary(vec![1]),
    );
    let key = signer(13);
    let malformed = eoe::payload::create_transaction(
        key.public_key(),
        7.into(),
        vote.vote_type().clone(),
        &fields,
        &key,
        &node.config.profile.network,
        Default::default(),
    )
    .unwrap();
    let now = Instant::now();
    let mut outputs = vec![];
    let native = native("inner_exhaustion");
    for attempt in 1..=4 {
        pool.admit(ae_hyperchain::eoe::pool::Admission {
            signed: malformed.clone(),
            event: hc_votes::Event::Received,
            protocol: node.config.profile.network.protocol_at(0).unwrap(),
            consensus: ae_protocol::ConsensusType::Pos,
        })
        .unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            drive(&mut c, now, &mut outputs).unwrap();
            let restarted = outputs
                .iter()
                .filter(|o| {
                    matches!(
                        o,
                        Output::Supervision(runtime::Output::Service(
                            coordinator::ServiceOutput::Restarted {
                                mode: eoe::Mode::Fork,
                                ..
                            }
                        ))
                    )
                })
                .count();
            if (attempt < 4 && restarted == attempt)
                || (attempt == 4 && c.status().consensus.eoe_generation == 2)
            {
                break;
            }
            assert!(Instant::now() < end, "attempt {attempt}: {:?}", c.status());
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            c.status().consensus.eoe_generation == 2,
            native["trace"][attempt - 1]["outer_replaced"]
        );
        assert!(pool.same_pool(&c.eoe().independent_pool().unwrap().client().unwrap()));
        assert_eq!(pool.inspect(|p| p.len()).unwrap(), 1);
    }
    assert!(!c.eoe().same_runtime(&old));
    assert_eq!(c.status().consensus.attempts, 1);
    assert!(c.votes().info().accepting && c.pool().info().accepting);
    assert!(outputs.iter().any(|o| matches!(
        o,
        Output::Supervision(runtime::Output::Service(
            coordinator::ServiceOutput::Stopped
        ))
    )));
    assert!(c.status().failure.is_none());
    drop(shutdown(c));
}

#[tokio::test]
async fn accepted_entropy_notification_is_reported_after_join_without_replaying_its_effect() {
    use ae_chain::repository::hyperchain::inputs::Effect;
    let expected = {
        let node = Node::new(0);
        let c = start(&node, false);
        c.importer()
            .import_block(node.blocks()[0].encode().unwrap(), 1_000_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
        shutdown(c).accepted.unwrap().entries().to_vec()
    };
    assert!(matches!(expected.first(), Some(Effect::Parent { .. })));
    let node = Node::new(0);
    let mut config = config(&node, false);
    config.eoe.eoe.runtime.output_capacity = 1;
    let mut c = new(&node, config).unwrap();
    c.parent().post_block(parent(8, 1008)).unwrap();
    c.parent().barrier(Duration::from_secs(5)).unwrap();
    wait(|| c.parent().snapshot().cache.lookup(&1.into()).is_ok());
    c.importer()
        .import_block(node.blocks()[0].encode().unwrap(), 1_000_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let old = c.eoe();
    for _ in 0..2 {
        assert!(matches!(
            apply(&old, runtime::Command::Entropy(3.into())),
            runtime::Applied::Entropy(Some(_))
        ));
    }
    let filler = call(&old, runtime::Command::Entropy(3.into()));
    // This releases one output slot and submits the first real entropy effect
    // behind the filler. The filler then blocks on the occupied output slot.
    let now = Instant::now();
    c.step(1_000_000, now).unwrap();
    old.request_stop();
    let mut outputs = vec![];
    replaced(&mut c, now, 2, &mut outputs);
    let exit = outputs
        .iter()
        .find_map(|o| match o {
            Output::ConsensusSupervision(Notice::EoeRetired { child, .. }) => Some(child),
            _ => None,
        })
        .unwrap();
    let lost = exit.delivery.as_ref().expect("accepted entropy receipt");
    assert_eq!(lost.effect, expected[0]);
    assert_eq!(lost.result, Err(runtime::CallError::Closed));
    drop(filler);
    for _ in 0..5 {
        drive(&mut c, now, &mut outputs).unwrap();
    }
    let exit = shutdown(c);
    let acknowledged = exit.delivery.acknowledged() as usize;
    assert_eq!(
        1 + acknowledged + exit.delivery.pending().len(),
        expected.len()
    );
    assert_eq!(
        exit.delivery.pending().iter().collect::<Vec<_>>(),
        expected[1 + acknowledged..].iter().collect::<Vec<_>>()
    );
}

#[test]
fn subsequent_cache_retirement_preserves_the_unknown_key_effect_from_old_eoe() {
    use ae_sync::chain::hyperchain::delivery;
    let node = Node::new(1);
    let storage = inserted(&node);
    for block in node.blocks().iter().take(4) {
        storage.insert(block, 1_000_000, Origin::Gossip).unwrap();
    }
    drop(storage);
    let mut config = config(&node, true);
    config.eoe.eoe.runtime.output_capacity = 1;
    let mut c = new(&node, config).unwrap();
    c.parent().post_block(parent(8, 1008)).unwrap();
    c.parent().barrier(Duration::from_secs(5)).unwrap();
    wait(|| c.parent().snapshot().cache.lookup(&1.into()).is_ok());
    let mut wall = 25003;
    let mut now = Instant::now();
    let end = now + Duration::from_secs(15);
    loop {
        let progress = tick(&mut c, &mut wall, &mut now);
        c.take_output();
        if progress == Progress::Production(production::Progress::KeyPrepared) {
            break;
        }
        assert!(Instant::now() < end, "{:?}", c.status());
    }
    let old = c.eoe();
    for _ in 0..2 {
        assert!(matches!(
            apply(&old, runtime::Command::Entropy(3.into())),
            runtime::Applied::Entropy(Some(_))
        ));
    }
    let filler = call(&old, runtime::Command::Entropy(3.into()));
    assert_eq!(
        c.step(wall, now).unwrap(),
        Progress::Production(production::Progress::Effects(delivery::Progress::Submitted))
    );
    old.request_stop();
    loop {
        c.step(wall, now).unwrap();
        if matches!(
            c.take_output(),
            Some(Output::ConsensusSupervision(Notice::EoeRetired { .. }))
        ) {
            break;
        }
        assert!(Instant::now() < end, "{:?}", c.status());
        std::thread::sleep(Duration::from_millis(1));
    }
    // The old key effect has already been classified unknown. Cache retirement
    // has no matching accepted receipt and must not erase that diagnosis.
    c.parent().request_stop();
    let stopped = loop {
        c.step(wall, now).unwrap();
        if let Some(Output::Stopped(report)) = c.take_output() {
            break report;
        }
        assert!(Instant::now() < end, "{:?}", c.status());
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(
        stopped.key_effect_error,
        Some(delivery::Error::ReceiptLost(delivery::Target::Eoe))
    );
    assert_eq!(stopped.acknowledged_key_effects, 0);
    assert!(!stopped.discarded_key_effects.is_empty());
    drop(filler);
    drop(shutdown(c));
}
