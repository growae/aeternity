use super::*;
use ae_chain::repository::hyperchain::{inputs::Effect, producer as prepared};
use ae_hyperchain::{producer as core, signing::Keys};
use ae_sync::chain::Importer;
use ae_vm_fate::value::ObjectPayload;
use common::hyperchain::parents::Pair;
use serde_json::{Value as J, json};
use std::sync::LazyLock;
static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../../fixtures/erlang_hc_producer_owner.json")).unwrap()
});
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn int(n: impl std::fmt::Display) -> J {
    json!(["integer", n.to_string()])
}
fn uint(n: &Unsigned) -> J {
    int(num_bigint::BigInt::from_bytes_be(
        num_bigint::Sign::Plus,
        n.as_bytes(),
    ))
}
fn bin(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(b)])
}
fn tuple(v: Vec<J>) -> J {
    json!(["tuple", v])
}
fn list(v: Vec<J>) -> J {
    json!(["list", v])
}
fn map(v: Vec<(J, J)>) -> J {
    json!(["map", v])
}
fn object(p: &ObjectPayload) -> J {
    match p {
        ObjectPayload::Bytes(b) => bin(b),
        _ => panic!("nonbinary address"),
    }
}
fn norm(j: &J) -> J {
    match j {
        J::Array(a) if a.len() == 2 && a[0] == "map" => {
            let mut pairs: Vec<_> = a[1].as_array().unwrap().iter().map(norm).collect();
            pairs.sort_by_key(|p| p[0].to_string());
            json!(["map", pairs])
        }
        J::Array(a) => J::Array(a.iter().map(norm).collect()),
        J::Object(o) => J::Object(o.iter().map(|(k, v)| (k.clone(), norm(v))).collect()),
        _ => j.clone(),
    }
}
fn cache(s: &ae_hyperchain::block::State) -> J {
    json!({"leaders":map(vec![
        (atom("epochs"),list(s.leaders.epochs().iter().map(int).collect())),
        (atom("epoch_infos"),map(s.leaders.ranges().iter().map(|(e,(f,l))|(int(e),tuple(vec![int(f),int(l)]))).collect())),
        (atom("schedule"),map(s.leaders.schedule().iter().map(|(h,p)|(int(h),object(p))).collect()))]),
        "starts":list(s.epoch_starts.iter().map(|e|tuple(vec![int(&e.epoch),int(&e.height),int(&e.time)])).collect())})
}
fn keys(k: &Keys) -> J {
    json!({"active":k.active().map(|k|bin(k.0)).unwrap_or(atom("not_set")),"candidate":k.candidate().map(|k|bin(k.0)).unwrap_or(atom("not_set"))})
}
fn decision(d: &core::Decision) -> J {
    let value = match d {
        core::Decision::Wait { reason, after } => tuple(vec![
            atom("wait"),
            atom(match reason {
                core::Wait::SlotFilled => "slot_filled",
                core::Wait::NotProducer => "not_producer",
            }),
            int(after),
        ]),
        core::Decision::RetryNotInCache { after } => {
            tuple(vec![atom("error"), atom("not_in_cache"), int(after)])
        }
        core::Decision::Produce {
            leader,
            holes,
            after,
        } => tuple(vec![
            atom("ok"),
            match holes {
                None => tuple(vec![atom("leader"), object(leader)]),
                Some(n) => tuple(vec![atom("leader_hole"), object(leader), int(n)]),
            },
            int(after),
        ]),
    };
    json!({"kind":"ok","value":value})
}
fn effects<'a>(
    parents: impl IntoIterator<Item = &'a Effect>,
    negotiation: Option<&eoe::Negotiation>,
) -> J {
    let mut values = parents
        .into_iter()
        .map(|effect| match effect {
            Effect::Parent { epoch, block } => tuple(vec![
                atom("parent"),
                int(epoch),
                tuple(vec![
                    atom("block"),
                    bin(&block.hash),
                    uint(&block.height),
                    bin(block.previous_hash.as_deref().unwrap()),
                    uint(&block.time),
                ]),
            ]),
            Effect::RequestParent(_) => panic!("native parent exists"),
        })
        .collect::<Vec<_>>();
    if let Some(n) = negotiation {
        values.push(tuple(vec![
            atom("negotiate"),
            uint(&n.epoch),
            uint(&n.height),
            bin(&n.hash),
            bin(n.leader.0),
            list(
                n.validators
                    .iter()
                    .map(|(k, w)| tuple(vec![bin(k.0), uint(w)]))
                    .collect(),
            ),
            n.seed.as_ref().map(bin).unwrap_or(atom("undefined")),
            uint(&n.current_length),
        ]));
    }
    list(values)
}
fn input(now: i64) -> prepared::Input {
    prepared::Input {
        now: now.into(),
        config: prepared::Config {
            production_time: 1.into(),
            ..Default::default()
        },
    }
}
fn binding(node: &Node) -> prepared::Binding {
    prepared::Binding {
        network: node.config.profile.network.clone(),
        election: node.config.profile.contracts.election,
        parent_start: 1.into(),
        parent_length: 5.into(),
        block_time: 4.into(),
    }
}
fn request(node: &Node, pair: &Pair, k: Keys, now: i64) -> hyperchain::ProducerRequest {
    hyperchain::ProducerRequest {
        binding: binding(node),
        input: input(now),
        keys: k,
        parent: pair.client.snapshot().cache,
    }
}
#[test]
fn original_real_chain_producer_matches_all_decisions_caches_and_effects() {
    for profile in ["profile", "repeat_profile"] {
        for row in DATA["rows"].as_array().unwrap() {
            let mut node = Node::new(row["corpus_index"].as_u64().unwrap() as usize);
            Arc::make_mut(&mut node.config.profile).order =
                common::hyperchain::order(&DATA[profile]);
            let inserted = inserted(&node);
            let pair = Pair::new(false);
            let mut signing = Keys::new(
                row["owned"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|n| signer(n.as_u64().unwrap())),
            );
            let blocks = node.blocks();
            for i in 0..=blocks.len() {
                let steps = if i == 0 {
                    &row["producer"]
                } else {
                    inserted
                        .insert(&blocks[i - 1], 1_000_000, Origin::Gossip)
                        .unwrap();
                    node.check(i - 1);
                    assert_eq!(
                        norm(&cache(&inserted.snapshot().unwrap().1)),
                        norm(&row["rows"][i - 1]["cache"])
                    );
                    &row["rows"][i - 1]["producer"]
                };
                for step in steps.as_array().unwrap() {
                    let before = node.db.view().unwrap().status();
                    let checkpoint = node.db.view().unwrap().hyperchain_checkpoint().unwrap();
                    let cursor = node.db.events().cursor();
                    let host = inserted.snapshot().unwrap().2;
                    let produced = inserted
                        .prepare_producer(
                            request(&node, &pair, signing, step["now"].as_i64().unwrap()),
                            || Ok::<_, ()>(()),
                        )
                        .unwrap();
                    let p = produced.prepared;
                    assert_eq!(inserted.snapshot().unwrap().2, host);
                    assert_eq!(
                        decision(p.decision.as_ref().unwrap()),
                        step["result"],
                        "step {i} {step}"
                    );
                    assert_eq!(keys(&p.keys), step["keys"]);
                    assert_eq!(
                        norm(&cache(&inserted.snapshot().unwrap().1)),
                        norm(&step["cache"])
                    );
                    assert_eq!(
                        effects(p.parents.entries(), p.negotiation.as_ref()),
                        step["reads"]["events"]
                    );
                    assert_eq!(p.key_height, step["key_height"].as_u64().unwrap());
                    assert_eq!(hex::encode(p.key_hash.0), step["key_hash"]);
                    assert_eq!(p.status, before);
                    assert_eq!(produced.cursor, cursor);
                    assert_eq!(node.db.view().unwrap().status(), before);
                    assert_eq!(
                        node.db.view().unwrap().hyperchain_checkpoint().unwrap(),
                        checkpoint
                    );
                    assert_eq!(node.db.events().cursor(), cursor);
                    signing = p.keys;
                }
            }
        }
    }
}
#[test]
fn producer_guards_binding_and_resource_failures_keep_shared_state_unchanged() {
    let node = Node::new(1);
    let pair = Pair::new(false);
    let inserted = inserted(&node);
    let original = inserted.snapshot().unwrap();
    let make = || {
        request(
            &node,
            &pair,
            Keys::new([signer(12), signer(13), signer(14)]),
            0,
        )
    };
    let mut bad = make();
    bad.binding.block_time = 5.into();
    assert!(matches!(
        inserted.prepare_producer(bad, || Ok::<_, ()>(())),
        Err(hyperchain::ProducerError::Repository(repo::Error::Identity))
    ));
    let mut checks = 0;
    assert!(matches!(
        inserted.prepare_producer(make(), || {
            checks += 1;
            if checks == 2 { Err("stopped") } else { Ok(()) }
        }),
        Err(hyperchain::ProducerError::Guard("stopped"))
    ));
    assert_eq!(checks, 2);
    assert_eq!(inserted.snapshot().unwrap(), original);
    let mut small = make();
    small.input.config.inputs.maximum_effects = 0;
    let failed = inserted
        .prepare_producer(small, || Ok::<_, ()>(()))
        .unwrap()
        .prepared;
    assert!(failed.decision.unwrap_err().operational);
    assert!(failed.parents.entries().is_empty());
    assert!(failed.negotiation.is_none());
    assert!(failed.keys.candidate().is_none());
    assert_eq!(inserted.snapshot().unwrap(), original);
    let valid = inserted
        .prepare_producer(make(), || Ok::<_, ()>(()))
        .unwrap()
        .prepared;
    assert!(valid.decision.is_ok());
    assert!(!valid.parents.entries().is_empty());
    assert_ne!(inserted.snapshot().unwrap().1, original.1);
    // A read prepares transient native cache only, until block admission commits it.
    assert_eq!(
        node.db
            .view()
            .unwrap()
            .hyperchain_checkpoint()
            .unwrap()
            .state,
        original.1
    );
}
fn production_owner(
    node: &Node,
    pair: &Pair,
    mut config: managed::Config,
    owned: &[u64],
) -> managed::Owner {
    config.eoe.block_time = 4.into();
    let source = source(node);
    let mut i = inputs(node, &source, &config);
    i.parent = Some(pair.client.clone());
    managed::Owner::start(
        i,
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
fn drive_production(owner: &mut managed::Owner) {
    until(owner, |o| o.production().is_some_and(|p| p.ready()));
}
#[tokio::test]
async fn managed_producer_delivers_parent_and_negotiation_before_decision_on_actual_chain() {
    let row = &DATA["rows"][3];
    let node = Node::new(6);
    let pair = Pair::new(false);
    let mut owner = production_owner(&node, &pair, cfg(&node), &[12, 13, 14]);
    let (importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let producer = accepted.producer();
    let mut delivery = owner.parent_delivery(accepted).unwrap();
    let blocks = node.blocks();
    for i in 0..=blocks.len() {
        let steps = if i == 0 {
            &row["producer"]
        } else {
            common::within(
                importer
                    .import_block(blocks[i - 1].encode().unwrap(), 1_000_000)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
            node.check(i - 1);
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
            &row["rows"][i - 1]["producer"]
        };
        for expected in steps.as_array().unwrap() {
            let selected = node.db.view().unwrap().status();
            let old = keys(owner.signing());
            owner
                .prepare_producer(&producer, input(expected["now"].as_i64().unwrap()))
                .unwrap();
            assert!(matches!(
                owner.prepare_producer(&producer, input(0)),
                Err(managed::Error::Backpressured)
            ));
            let pending = owner.production().unwrap();
            assert_eq!(
                effects(pending.remaining_parents(), pending.negotiation()),
                expected["reads"]["events"]
            );
            let count =
                pending.remaining_parents().len() + usize::from(pending.negotiation().is_some());
            assert_eq!(
                decision(pending.decision().as_ref().unwrap()),
                expected["result"]
            );
            assert_eq!(keys(owner.signing()), old);
            if count > 0 {
                assert!(owner.take_production().is_none());
            }
            drive_production(&mut owner);
            assert_eq!(owner.production().unwrap().acknowledged(), count as u64);
            let ready = owner.take_production().unwrap();
            assert_eq!(decision(&ready.outcome.unwrap()), expected["result"]);
            assert_eq!(ready.status, selected);
            assert_eq!(ready.key_height, expected["key_height"].as_u64().unwrap());
            assert_eq!(hex::encode(ready.key_hash.0), expected["key_hash"]);
            assert_eq!(keys(owner.signing()), expected["keys"]);
            assert!(owner.production().is_none());
        }
    }
    importer.stop();
    importer.join().await;
    assert!(matches!(
        owner.prepare_producer(&producer, input(0)),
        Err(managed::Error::ProductionSource(
            ae_sync::chain::Error::Closed
        ))
    ));
    drop(delivery);
    assert!(owner.shutdown().errors.is_empty());
}

#[tokio::test]
async fn rejected_negotiation_retains_exact_attempt_and_never_repeats_parent_prefix() {
    let node = Node::new(6);
    let pair = Pair::new(false);
    let mut config = cfg(&node);
    config.eoe.coordinator.machine.maximum_validators = 0;
    let mut owner = production_owner(&node, &pair, config, &[12, 13, 14]);
    let (importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let producer = accepted.producer();
    for block in node.blocks().iter().take(4) {
        common::within(
            importer
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
    }
    owner.prepare_producer(&producer, input(25000)).unwrap();
    let expected = owner.production().unwrap().negotiation().unwrap().clone();
    let parent_count = owner.production().unwrap().remaining_parents().len();
    assert!(parent_count > 0);
    let start = Instant::now();
    let error = loop {
        if let Err(e) = owner.step() {
            break e;
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    assert!(matches!(
        error,
        managed::Error::Producer(ae_sync::chain::hyperchain::delivery::Error::Eoe(
            coordinator::Error::Step(eoe::StepError::Core(eoe::Failure::ResourceLimit))
        ))
    ));
    let pending = owner.production().unwrap();
    assert_eq!(pending.acknowledged(), parent_count as u64);
    assert!(pending.remaining_parents().is_empty());
    assert_eq!(pending.negotiation(), Some(&expected));
    assert!(!pending.in_flight());
    assert!(owner.take_production().is_none());
    assert!(owner.signing().candidate().is_none());
    // Even a subsequent selected microblock does not rebase the retained attempt.
    let selected = node.db.view().unwrap().status();
    common::within(
        importer
            .import_block(node.blocks()[4].encode().unwrap(), 1_000_000)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    unit(owner.configure_runtime(Default::default()).unwrap());
    owner.retry(Default::default()).unwrap();
    drive_production(&mut owner);
    assert_eq!(
        owner.production().unwrap().acknowledged(),
        parent_count as u64 + 1
    );
    let ready = owner.take_production().unwrap();
    assert_eq!(ready.status, selected);
    assert_eq!(owner.signing().candidate(), Some(expected.leader));
    importer.stop();
    importer.join().await;
    assert!(owner.shutdown().errors.is_empty());
}

#[tokio::test]
async fn producer_binding_and_shutdown_keep_unacknowledged_effects_with_their_owner() {
    let node = Node::new(0);
    let mut pair = Pair::new(false);
    let mut owner = production_owner(&node, &pair, cfg(&node), &[12, 13, 14]);
    let other = Node::new(0);
    let (foreign, accepted) = Importer::new_hyperchain_scoped(
        other.db.clone(),
        other.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    assert!(matches!(
        owner.prepare_producer(&accepted.producer(), input(0)),
        Err(managed::Error::Configuration)
    ));
    assert!(owner.production().is_none());
    foreign.stop();
    foreign.join().await;
    let (importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    owner
        .prepare_producer(&accepted.producer(), input(0))
        .unwrap();
    let count = owner.production().unwrap().remaining_parents().len();
    assert!(count > 0);
    pair.cache.take().unwrap().shutdown().unwrap();
    assert!(owner.take_production().is_none());
    assert!(owner.step().is_err());
    assert!(owner.retry(Default::default()).is_err());
    let report = owner.shutdown();
    let pending = report.producer.unwrap();
    assert_eq!(pending.remaining_parents().len(), count);
    assert_eq!(pending.acknowledged(), 0);
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn missing_parent_is_requested_before_retry_decision_and_next_attempt_uses_new_snapshot() {
    let node = Node::new(0);
    let pair = Pair::new(true);
    let mut owner = production_owner(&node, &pair, cfg(&node), &[12, 13, 14]);
    let (importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let source = accepted.producer();
    owner.prepare_producer(&source, input(0)).unwrap();
    assert!(matches!(
        owner.production().unwrap().decision(),
        Ok(core::Decision::RetryNotInCache { .. })
    ));
    assert!(
        matches!(owner.production().unwrap().remaining_parents().front(),Some(Effect::RequestParent(h)) if h==&1.into())
    );
    assert!(owner.take_production().is_none());
    drive_production(&mut owner);
    assert!(matches!(
        owner.take_production().unwrap().outcome,
        Ok(core::Decision::RetryNotInCache { .. })
    ));
    assert!(owner.signing().candidate().is_none());
    pair.backend
        .missing
        .store(false, std::sync::atomic::Ordering::Release);
    pair.post(1, 1001);
    owner.prepare_producer(&source, input(0)).unwrap();
    drive_production(&mut owner);
    assert!(matches!(
        owner.take_production().unwrap().outcome,
        Ok(core::Decision::Produce { .. })
    ));
    assert!(owner.signing().candidate().is_some());
    importer.stop();
    importer.join().await;
    assert!(owner.shutdown().errors.is_empty());
}
