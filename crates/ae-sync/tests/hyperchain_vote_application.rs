mod common;
use ae_chain::{
    block::Block,
    hyperchain::candidate as core,
    repository::{hyperchain::candidate as build, ledger::Origin},
};
use ae_hyperchain::{
    eoe::{Reply, local_host::Finalized},
    signing::Keys,
};
use ae_storage::hyperchain;
use ae_types::{AccountPublicKey, Hash256};
use common::hyperchain::{Node, Parent};
use num_bigint::BigInt;
use serde_json::Value as J;
use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock},
};
static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_hc_vote_application.json")).unwrap()
});
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn hash(j: &J) -> Hash256 {
    Hash256(bytes(j).try_into().unwrap())
}
fn results(j: &J) -> core::VoteResults {
    j.as_array()
        .unwrap()
        .iter()
        .map(|r| {
            if let Some(tx) = r.get("transaction") {
                Reply::Finalized(Finalized::Signed(Box::new(
                    ae_tx::SignedTransaction::decode(&bytes(tx)).unwrap(),
                )))
            } else if r["error"] == "no_consensus" {
                Reply::NoConsensus
            } else {
                Reply::NotReady
            }
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap()
}
fn keys() -> Keys {
    Keys::new([12u64, 13, 14].map(|n| {
        let mut seed = [0; 32];
        seed[24..].copy_from_slice(&n.to_be_bytes());
        ae_crypto::Ed25519SecretKey::from_seed(&seed)
    }))
}
fn observe<
    A: ae_trie::NodeWriter + Clone + std::fmt::Debug,
    C: ae_trie::NodeWriter + Clone + std::fmt::Debug,
>(
    candidate: &core::Micro<A, C>,
    leader: AccountPublicKey,
    expected: &J,
) {
    assert_eq!(
        Block::Micro(candidate.block().clone()).encode().unwrap(),
        bytes(&expected["block"])
    );
    assert_eq!(
        Block::Micro(candidate.signed::<()>(&keys(), leader).unwrap())
            .encode()
            .unwrap(),
        bytes(&expected["signed"])
    );
    assert_eq!(
        hex::encode(candidate.trees().roots().hash().0),
        expected["trees"]["state_root"]
    );
    for row in expected["trees"]["accounts"].as_array().unwrap() {
        let key = AccountPublicKey(bytes(&row["key"]).try_into().unwrap());
        assert_eq!(
            candidate
                .trees()
                .accounts
                .lookup(key)
                .unwrap()
                .unwrap()
                .serialize()
                .unwrap(),
            bytes(&row["encoded"])
        );
    }
}
#[test]
fn original_conductor_reservation_updates_and_one_refresh_match_under_both_otp_profiles() {
    let mut builds = 0;
    let mut refreshes = 0;
    let mut attempts = 0;
    for profile in ["profile", "repeat_profile"] {
        for row in DATA["rows"].as_array().unwrap() {
            for parent in row["parents"].as_array().unwrap() {
                let mut node = Node::new(row["corpus_index"].as_u64().unwrap() as usize);
                Arc::make_mut(&mut node.config.profile).order =
                    common::hyperchain::order(&DATA[profile]);
                let owner = hyperchain::Owner::start(
                    node.db.clone(),
                    node.config.clone(),
                    Parent::default(),
                )
                .unwrap();
                for b in node
                    .blocks()
                    .iter()
                    .take(parent["count"].as_u64().unwrap() as usize)
                {
                    owner.insert(b, 1_000_000, Origin::Gossip).unwrap();
                }
                let base = owner.snapshot().unwrap().1;
                for c in parent["cases"].as_array().unwrap() {
                    let view = node.db.view().unwrap();
                    let h = hash(&c["parent"]);
                    let header = view.require_block(h).unwrap().header();
                    let mut state = base.clone();
                    let mut host = Parent::default();
                    let p = build::micro(
                        &view,
                        &node.config,
                        build::Input {
                            parent: h,
                            now: c["now"].as_i64().unwrap().into(),
                            limits: Default::default(),
                        },
                        &mut state,
                        &mut host,
                    )
                    .unwrap();
                    let mut votes = core::Votes::new(&results(&c["initial"]), 16 << 20).unwrap();
                    let gas = votes.reserved_gas(
                        header.common().height,
                        ae_protocol::Protocol::try_from(header.common().version).unwrap(),
                    );
                    assert_eq!(
                        (BigInt::from(6_000_000) - &gas).to_string(),
                        c["requests"][0]["gas"]
                    );
                    let transactions = c["transactions"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|v| {
                            let tx = ae_tx::SignedTransaction::decode(&bytes(v)).unwrap();
                            (tx.hash(), tx)
                        })
                        .collect::<BTreeMap<_, _>>();
                    let mut packing = core::Packing::new::<()>(
                        p.candidate,
                        gas,
                        0,
                        core::Timeout::Disabled,
                        1000,
                    )
                    .unwrap();
                    for r in c["requests"].as_array().unwrap() {
                        let request = packing.request::<()>(|| 0).unwrap().unwrap();
                        assert_eq!(request.parent, h);
                        assert_eq!(request.gas.to_string(), r["gas"]);
                        let selected = r["selected"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|h| transactions[&hash(h)].clone())
                            .collect::<Vec<_>>();
                        packing
                            .selected::<(), _, _>(
                                &selected,
                                r["expired"].as_bool().unwrap(),
                                &p.reader,
                            )
                            .unwrap();
                    }
                    assert!(packing.request::<()>(|| 0).unwrap().is_none());
                    let mut candidate = packing.finish::<()>().unwrap();
                    let leader = AccountPublicKey(bytes(&c["leader"]).try_into().unwrap());
                    observe(&candidate, leader, &c["packed"]);
                    let mut updates = c["updates"].as_array().unwrap().iter();
                    let mut retries = c["refreshes"].as_array().unwrap().iter();
                    while votes.outcome().is_none() {
                        let before = Block::Micro(candidate.block().clone()).encode().unwrap();
                        let progress = votes
                            .apply::<(), _, _, _, _>(&mut candidate, &p.reader)
                            .unwrap();
                        let expected = updates.next().unwrap();
                        attempts += 1;
                        if let Some(result) = expected.get("result") {
                            assert_eq!(progress, core::VoteProgress::Applied);
                            observe(&candidate, leader, result);
                        } else {
                            assert_eq!(expected["error"], "no_update_to_block_candidate");
                            assert_eq!(
                                Block::Micro(candidate.block().clone()).encode().unwrap(),
                                before
                            );
                            assert!(!matches!(progress, core::VoteProgress::Applied));
                        }
                        if votes.needs_refresh() {
                            let r = retries.next().unwrap();
                            assert_eq!(
                                hex::encode(candidate.trees().roots().hash().0),
                                r["state_root"]
                            );
                            votes.refresh(&results(&r["results"])).unwrap();
                            refreshes += 1;
                        }
                    }
                    assert!(updates.next().is_none());
                    assert!(retries.next().is_none());
                    assert_eq!(votes.outcome(), c["is_eoe"].as_bool());
                    observe(&candidate, leader, &c["result"]);
                    builds += 1;
                }
            }
        }
    }
    assert_eq!(builds, 208);
    assert_eq!(refreshes, 128);
    assert_eq!(attempts, 384);
}
#[test]
fn operational_update_limits_retain_first_vote_and_do_not_request_a_native_nonce_refresh() {
    let node = Node::new(1);
    let view = node.db.view().unwrap();
    let h = view.status().tip.unwrap();
    let c = &DATA["rows"][0]["parents"][0]["cases"][3];
    let initial = results(&c["initial"]);
    assert!(matches!(
        core::Votes::new(&initial, 1),
        Err(core::VoteError::ResourceLimit)
    ));
    let mut votes = core::Votes::new(&initial, 16 << 20).unwrap();
    let mut state = ae_hyperchain::block::State::default();
    let mut host = Parent::default();
    let mut p = build::micro(
        &view,
        &node.config,
        build::Input {
            parent: h,
            now: 999,
            limits: core::Limits {
                offered: 0,
                ..Default::default()
            },
        },
        &mut state,
        &mut host,
    )
    .unwrap();
    let before = Block::Micro(p.candidate.block().clone()).encode().unwrap();
    for _ in 0..2 {
        assert!(matches!(
            votes.apply::<(), _, _, _, _>(&mut p.candidate, &p.reader),
            Err(core::Error::ResourceLimit)
        ));
        assert!(!votes.needs_refresh());
        assert_eq!(votes.outcome(), None);
        assert_eq!(
            Block::Micro(p.candidate.block().clone()).encode().unwrap(),
            before
        );
    }
    let mut state = ae_hyperchain::block::State::default();
    let mut p = build::micro(
        &view,
        &node.config,
        build::Input {
            parent: h,
            now: 999,
            limits: Default::default(),
        },
        &mut state,
        &mut host,
    )
    .unwrap();
    assert_eq!(
        votes
            .apply::<(), _, _, _, _>(&mut p.candidate, &p.reader)
            .unwrap(),
        core::VoteProgress::Applied
    );
    assert_eq!(
        votes
            .apply::<(), _, _, _, _>(&mut p.candidate, &p.reader)
            .unwrap(),
        core::VoteProgress::Applied
    );
    assert_eq!(votes.outcome(), Some(true));
    observe(
        &p.candidate,
        AccountPublicKey(bytes(&c["leader"]).try_into().unwrap()),
        &c["result"],
    );
}
