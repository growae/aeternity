mod common;
use ae_chain::{
    block::Block,
    hyperchain::candidate::{self, Micro, Packing, Timeout},
    repository::{hyperchain::candidate as build, ledger::Origin},
};
use ae_crypto::Ed25519SecretKey;
use ae_execution::transactions::{self as execution, batch};
use ae_hyperchain::signing::Keys;
use ae_storage::hyperchain;
use ae_trie::NodeWriter;
use ae_tx::{SignedTransaction, Transaction};
use ae_types::AccountPublicKey;
use ae_vm_fate::value::ObjectPayload;
use common::hyperchain::{Node, Parent};
use num_bigint::BigInt;
use serde_json::{Value as J, json};
use std::{
    collections::VecDeque,
    sync::{Arc, LazyLock},
};
static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_hc_micro_candidate.json")).unwrap()
});
fn key(n: u64) -> [u8; 32] {
    let mut k = [0; 32];
    k[24..].copy_from_slice(&n.to_be_bytes());
    k
}
fn keys() -> Keys {
    Keys::new([12, 13, 14].map(|n| Ed25519SecretKey::from_seed(&key(n))))
}
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn txs(j: &J) -> Vec<SignedTransaction<Transaction>> {
    j.as_array()
        .unwrap()
        .iter()
        .map(|s| SignedTransaction::decode(&bytes(s)).unwrap())
        .collect()
}
fn norm(j: &J) -> J {
    match j {
        J::Array(a) if a.len() == 2 && a[0] == "map" => {
            let mut p = a[1]
                .as_array()
                .unwrap()
                .iter()
                .map(norm)
                .collect::<Vec<_>>();
            p.sort_by_key(|a| a[0].to_string());
            json!(["map", p])
        }
        J::Array(a) => J::Array(a.iter().map(norm).collect()),
        J::Object(o) => J::Object(o.iter().map(|(k, v)| (k.clone(), norm(v))).collect()),
        _ => j.clone(),
    }
}
fn cache(s: &ae_hyperchain::block::State) -> J {
    let int = |n: &BigInt| json!(["integer", n.to_string()]);
    let list = |n: Vec<J>| json!(["list", n]);
    let tuple = |n: Vec<J>| json!(["tuple", n]);
    let map = |n: Vec<(J, J)>| json!(["map", n]);
    json!({"leaders":map(vec![
 (json!(["atom","epochs"]),list(s.leaders.epochs().iter().map(int).collect())),
 (json!(["atom","epoch_infos"]),map(s.leaders.ranges().iter().map(|(e,(f,l))|(int(e),tuple(vec![int(f),int(l)]))).collect())),
 (json!(["atom","schedule"]),map(s.leaders.schedule().iter().map(|(h,p)|{let ObjectPayload::Bytes(b)=p else{panic!()};(int(h),json!(["binary",hex::encode(b)]))}).collect()))]),
 "starts":list(s.epoch_starts.iter().map(|e|tuple(vec![int(&e.epoch),int(&e.height),int(&e.time)])).collect())})
}
fn observe<A: NodeWriter + Clone + std::fmt::Debug, C: NodeWriter + Clone + std::fmt::Debug>(
    micro: &Micro<A, C>,
    leader: AccountPublicKey,
    expected: &J,
) {
    assert_eq!(
        Block::Micro(micro.block().clone()).encode().unwrap(),
        bytes(&expected["block"])
    );
    let signed = micro.signed::<()>(&keys(), leader).unwrap();
    assert_eq!(
        Block::Micro(signed).encode().unwrap(),
        bytes(&expected["signed"])
    );
    assert_eq!(
        hex::encode(micro.trees().roots().hash().0),
        expected["trees"]["state_root"]
    );
    for a in expected["trees"]["accounts"].as_array().unwrap() {
        let k = AccountPublicKey(bytes(&a["key"]).try_into().unwrap());
        assert_eq!(
            micro
                .trees()
                .accounts
                .lookup(k)
                .unwrap()
                .unwrap()
                .serialize()
                .unwrap(),
            bytes(&a["encoded"])
        );
    }
    let h = micro.block().header.common.height.into();
    let p = micro.context().protocol;
    let used = micro
        .block()
        .transactions
        .iter()
        .map(|s| {
            ae_execution::transactions::accounting::used_gas(
                s.transaction().view(),
                &h,
                p,
                micro.trees(),
            )
            .unwrap()
        })
        .sum::<BigInt>();
    assert_eq!(used.to_string(), expected["used_gas"]);
    assert_eq!(expected["events"], json!(["list", []]));
}
fn native_case(node: &Node, base: &ae_hyperchain::block::State, c: &J) -> Block {
    let view = node.db.view().unwrap();
    let before = view.status();
    let parent = before.tip.unwrap();
    let mut state = base.clone();
    let mut host = Parent::default();
    let prepared = build::micro(
        &view,
        &node.config,
        build::Input {
            parent,
            now: c["now"].as_i64().unwrap().into(),
            limits: Default::default(),
        },
        &mut state,
        &mut host,
    )
    .unwrap();
    let timeout = if c["timeout"][0] == "integer" {
        Timeout::Milliseconds(c["timeout"][1].as_str().unwrap().parse().unwrap())
    } else {
        Timeout::Default
    };
    let mut clocks = c["clocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n.as_i64().unwrap())
        .collect::<VecDeque<_>>();
    let mut reads = vec![];
    let initial = if let Some(t) = clocks.pop_front() {
        reads.push(t);
        t
    } else {
        0
    };
    let mut last = initial;
    let mut packing = Packing::new::<()>(
        prepared.candidate,
        c["offset"].as_i64().unwrap().into(),
        initial,
        timeout,
        c["window"].as_i64().unwrap(),
    )
    .unwrap();
    let mut requests = vec![];
    let mut failures = vec![];
    let mut batches = c["batches"].as_array().unwrap().iter();
    loop {
        let request = packing
            .request::<()>(|| {
                last = clocks.pop_front().unwrap_or(last);
                reads.push(last);
                last
            })
            .unwrap();
        let Some(r) = request else { break };
        requests.push(json!({"gas":r.gas.to_string(),"hash":hex::encode(r.parent.0),"ignore":r.ignored.iter().map(|h|hex::encode(h.0)).collect::<Vec<_>>(),"options":{"deadline":r.deadlines.bound,"ceiling":r.deadlines.ceiling,"packed":r.packed}}));
        let txs = txs(batches.next().unwrap_or(&json!([])));
        let applied = packing
            .selected::<(), _, _>(&txs, false, &prepared.reader)
            .unwrap();
        if !applied.rejected.is_empty() {
            failures.push(
                applied
                    .rejected
                    .iter()
                    .map(|(i, rejected)| {
                        let reason = match rejected {
                            batch::Rejection::Failure(batch::Failure::Transaction(e)) => {
                                match e.as_ref() {
                                    execution::Error::Primitive(
                                        ae_execution::Error::NonceTooHigh,
                                    ) => "tx_nonce_too_high_for_account",
                                    execution::Error::Signature(
                                        execution::SignatureError::CheckFailed,
                                    ) => "signature_check_failed",
                                    other => panic!("unexpected rejection: {other:?}"),
                                }
                            }
                            other => panic!("unexpected rejection: {other:?}"),
                        };
                        json!({"hash":hex::encode(txs[*i].hash().0),"reason":["atom",reason]})
                    })
                    .collect::<Vec<_>>(),
            );
        }
    }
    assert_eq!(json!(requests), c["requests"], "{}", c["name"]);
    assert_eq!(json!(reads), c["clock_reads"]);
    assert_eq!(json!(failures), c["failures"]);
    assert_eq!(packing.expired(), c["expired"].as_bool().unwrap());
    let mut micro = packing.finish::<()>().unwrap();
    let ObjectPayload::Bytes(leader) = state
        .leaders
        .leader(&micro.block().header.common.height.into())
        .unwrap()
    else {
        panic!()
    };
    let leader = AccountPublicKey(leader.as_slice().try_into().unwrap());
    observe(&micro, leader, &c["result"]);
    assert_eq!(norm(&cache(&state)), norm(&c["result"]["cache"]));
    for update in c["updates"].as_array().unwrap() {
        let old = micro.block().clone();
        let roots = micro.trees().roots();
        let result = micro.update::<(), _, _>(&txs(&update["transactions"]), &prepared.reader);
        if update["error"].is_string() {
            assert!(
                matches!(result, Err(candidate::Error::NoUpdate)),
                "{result:?}"
            );
            assert_eq!(*micro.block(), old);
            assert_eq!(micro.trees().roots(), roots);
        } else {
            result.unwrap();
            observe(&micro, leader, &update["result"]);
        }
    }
    assert_eq!(node.db.view().unwrap().status(), before);
    Block::Micro(micro.signed::<()>(&keys(), leader).unwrap())
}
#[test]
fn original_micro_candidate_packing_deadlines_gas_and_incremental_nonce_retry_match() {
    for profile in ["profile", "repeat_profile"] {
        for row in DATA["rows"].as_array().unwrap() {
            let mut node = Node::new(row["corpus_index"].as_u64().unwrap() as usize);
            Arc::make_mut(&mut node.config.profile).order =
                common::hyperchain::order(&DATA[profile]);
            let inserted =
                hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default())
                    .unwrap();
            for c in row["candidates"].as_array().unwrap() {
                native_case(&node, &inserted.snapshot().unwrap().1, c);
            }
            for (i, block) in node.blocks().iter().enumerate() {
                inserted.insert(block, 1_000_000, Origin::Gossip).unwrap();
                node.check(i);
                for c in row["rows"][i]["candidates"].as_array().unwrap() {
                    native_case(&node, &inserted.snapshot().unwrap().1, c);
                }
                if !row["rows"][i]["rejected_parent"].is_null() {
                    let view = node.db.view().unwrap();
                    assert!(matches!(
                        build::micro(
                            &view,
                            &node.config,
                            build::Input {
                                parent: view.status().tip.unwrap(),
                                now: 1_000_000,
                                limits: Default::default()
                            },
                            &mut inserted.snapshot().unwrap().1,
                            &mut Parent::default()
                        ),
                        Err(build::Error::Candidate(candidate::Error::WrongParent))
                    ));
                }
            }
        }
    }
}

#[test]
fn constructed_and_updated_candidates_use_normal_created_admission() {
    for row in DATA["rows"].as_array().unwrap() {
        for at_key_two in [false, true] {
            let node = Node::new(row["corpus_index"].as_u64().unwrap() as usize);
            let owner =
                hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default())
                    .unwrap();
            if at_key_two {
                for b in node.blocks().iter().take(4) {
                    owner.insert(b, 1_000_000, Origin::Gossip).unwrap();
                }
            }
            let cases = if at_key_two {
                &row["rows"][3]["candidates"]
            } else {
                &row["candidates"]
            };
            let c = cases
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["name"] == "passes")
                .unwrap();
            let b = native_case(&node, &owner.snapshot().unwrap().1, c);
            let before = node.db.view().unwrap().status();
            let result = owner.insert(&b, 1_000_000, Origin::Created);
            // Native construction retains its key parent's version. The mixed
            // fixture changes protocol at the following microblock height;
            // ordinary admission must still reject that old-version candidate.
            if node
                .config
                .profile
                .network
                .validate_version(b.header().common().height, b.header().common().version)
                .is_err()
            {
                let Err(hyperchain::Error::Prepare(e)) = result else {
                    panic!("expected protocol rejection")
                };
                assert!(
                    matches!(*e, hyperchain::PrepareError::Execution(ref e) if matches!(**e, ae_chain::hyperchain::Error::Protocol(_)))
                );
                assert_eq!(node.db.view().unwrap().status(), before);
            } else {
                result.unwrap();
                let view = node.db.view().unwrap();
                assert_eq!(view.status().tip, Some(b.hash().unwrap()));
                assert_eq!(
                    view.require_record(b.hash().unwrap())
                        .unwrap()
                        .roots
                        .state
                        .hash()
                        .0,
                    b.header().common().state_hash
                );
                assert_eq!(view.require_block(b.hash().unwrap()).unwrap(), b);
            }
        }
    }
}

#[test]
fn resource_failures_preserve_pending_selection_and_entire_incremental_update() {
    let node = Node::new(3);
    let view = node.db.view().unwrap();
    let before = view.status();
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    let mut state = owner.snapshot().unwrap().1;
    let mut host = Parent::default();
    let c = DATA["rows"][1]["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "passes")
        .unwrap();
    let mut batch = txs(&c["batches"][0]);
    batch[1] = txs(&c["batches"][1]).remove(0);
    for limits in [
        candidate::Limits {
            transactions: 1,
            ..Default::default()
        },
        candidate::Limits {
            signed_bytes: batch[0].encode().len(),
            ..Default::default()
        },
        candidate::Limits {
            offered: 1,
            ..Default::default()
        },
    ] {
        let prepared = build::micro(
            &view,
            &node.config,
            build::Input {
                parent: before.tip.unwrap(),
                now: 100,
                limits,
            },
            &mut state,
            &mut host,
        )
        .unwrap();
        let mut packing = Packing::new::<()>(
            prepared.candidate.clone(),
            0.into(),
            0,
            Timeout::Disabled,
            0,
        )
        .unwrap();
        assert!(matches!(
            packing.selected::<(), _, _>(&batch, false, &prepared.reader),
            Err(candidate::Error::Sequence)
        ));
        let request = packing
            .request::<()>(|| panic!("disabled timeout"))
            .unwrap()
            .unwrap();
        let gas = request.gas.clone();
        let error = packing
            .selected::<(), _, _>(&batch, false, &prepared.reader)
            .unwrap_err();
        assert!(matches!(error, candidate::Error::ResourceLimit));
        assert!(error.is_operational::<Parent>());
        let same = packing
            .request::<()>(|| panic!("outstanding request"))
            .unwrap()
            .unwrap();
        assert_eq!(*same.gas, gas);
        assert!(!same.packed);
        assert!(same.ignored.is_empty());
        let accepted = packing
            .selected::<(), _, _>(&batch[..1], false, &prepared.reader)
            .unwrap();
        assert_eq!(accepted.accepted, vec![0]);
        packing.request::<()>(|| 0).unwrap();
        packing
            .selected::<(), _, _>(&[], true, &prepared.reader)
            .unwrap();
        assert!(packing.expired());
        assert!(packing.request::<()>(|| panic!()).unwrap().is_none());
        let mut done = packing.finish::<()>().unwrap();
        let block = done.block().clone();
        let roots = done.trees().roots();
        if limits.offered == 1 {
            assert!(matches!(
                done.update::<(), _, _>(&batch, &prepared.reader),
                Err(candidate::Error::ResourceLimit)
            ));
        } else {
            assert!(matches!(
                done.update::<(), _, _>(&batch[1..], &prepared.reader),
                Err(candidate::Error::ResourceLimit)
            ));
        }
        assert_eq!(*done.block(), block);
        assert_eq!(done.trees().roots(), roots);
        assert!(matches!(
            done.update::<(), _, _>(&[], &prepared.reader),
            Err(candidate::Error::NoUpdate)
        ));
        assert!(matches!(
            done.signed::<()>(&Keys::default(), AccountPublicKey(key(999))),
            Err(candidate::Error::Signing(_))
        ));
    }
    assert_eq!(node.db.view().unwrap().status(), before);
}

#[test]
fn preparation_failures_leave_cache_host_and_repository_unchanged() {
    let node = Node::new(3);
    let view = node.db.view().unwrap();
    let before = view.status();
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    let state_before = owner.snapshot().unwrap().1;
    let host_before = Parent::default();
    let input = build::Input {
        parent: before.tip.unwrap(),
        now: 1,
        limits: Default::default(),
    };
    for failure in 0..4 {
        let mut state = state_before.clone();
        let mut host = host_before.clone();
        let mut config = node.config.clone();
        let mut input = input;
        match failure {
            0 => Arc::make_mut(&mut config.profile).maximum_steps = 0,
            1 => config.limits.overlay.nodes = 0,
            2 => input.now = i128::MAX,
            3 => config.identity.genesis = ae_types::Hash256::ZERO,
            _ => unreachable!(),
        }
        let Err(e) = build::micro(&view, &config, input, &mut state, &mut host) else {
            panic!("expected failure {failure}")
        };
        assert!(e.is_operational::<Parent>(), "{e:?}");
        assert_eq!(state, state_before);
        assert_eq!(host, host_before);
        assert_eq!(node.db.view().unwrap().status(), before);
    }
    // A failure does not poison the same immutable input or consensus cache.
    let mut state = state_before;
    let mut host = host_before;
    build::micro(&view, &node.config, input, &mut state, &mut host).unwrap();
}

#[test]
fn vm_failure_after_valid_spends_rolls_back_and_retry_keeps_captured_parent() {
    let node = Node::new(3);
    let view = node.db.view().unwrap();
    let before = view.status();
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    let mut state = owner.snapshot().unwrap().1;
    let prepared = build::micro(
        &view,
        &node.config,
        build::Input {
            parent: before.tip.unwrap(),
            now: 999,
            limits: Default::default(),
        },
        &mut state,
        &mut Parent::default(),
    )
    .unwrap();
    let c = DATA["rows"][1]["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "passes")
        .unwrap();
    let transactions = vec![
        txs(&c["batches"][0]).remove(0),
        txs(&c["batches"][1]).remove(0),
        txs(&c["updates"][1]["transactions"]).remove(0),
    ];
    let mut failed_reader = prepared.reader.clone();
    Arc::make_mut(&mut failed_reader.profile).maximum_steps = 0;
    let mut updated = prepared.candidate.clone();
    let original_block = updated.block().clone();
    let original_roots = updated.trees().roots();
    let error = updated
        .update::<(), _, _>(&transactions, &failed_reader)
        .unwrap_err();
    assert!(error.is_operational::<Parent>(), "{error:?}");
    assert_eq!(*updated.block(), original_block);
    assert_eq!(updated.trees().roots(), original_roots);
    let mut packing =
        Packing::new::<()>(prepared.candidate, 0.into(), 0, Timeout::Disabled, 0).unwrap();
    packing.request::<()>(|| 0).unwrap();
    let error = packing
        .selected::<(), _, _>(&transactions, false, &failed_reader)
        .unwrap_err();
    assert!(error.is_operational::<Parent>(), "{error:?}");
    let pending = packing.request::<()>(|| panic!()).unwrap().unwrap();
    assert!(!pending.packed);
    assert!(pending.ignored.is_empty());
    assert_eq!(*pending.gas, BigInt::from(6_000_000));
    // An unrelated new selected top must not rebase this captured candidate.
    for b in node.blocks().iter().take(4) {
        owner.insert(b, 1_000_000, Origin::Gossip).unwrap();
    }
    assert_ne!(node.db.view().unwrap().status(), before);
    assert_eq!(view.status(), before);
    assert_eq!(
        updated
            .update::<(), _, _>(&transactions, &prepared.reader)
            .unwrap()
            .accepted,
        vec![0, 1, 2]
    );
    assert_eq!(
        packing
            .selected::<(), _, _>(&transactions, false, &prepared.reader)
            .unwrap()
            .accepted,
        vec![0, 1, 2]
    );
    packing.request::<()>(|| 0).unwrap();
    packing
        .selected::<(), _, _>(&[], false, &prepared.reader)
        .unwrap();
    let packed = packing.finish::<()>().unwrap();
    assert_eq!(packed.block(), updated.block());
    assert_eq!(packed.trees().roots(), updated.trees().roots());
    let ObjectPayload::Bytes(leader) = state.leaders.leader(&1.into()).unwrap() else {
        panic!()
    };
    observe(
        &packed,
        AccountPublicKey(leader.as_slice().try_into().unwrap()),
        &c["updates"][1]["result"],
    );
}
