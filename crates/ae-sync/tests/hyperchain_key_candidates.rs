mod common;
use ae_chain::repository::Read;
use ae_chain::{
    block::Block,
    hyperchain::{self as core, candidate},
    repository::{self as repo, hyperchain::candidate as build, ledger::Origin},
};
use ae_codec::headers::{Header, HeaderMode};
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::signing::{Keys, Seal};
use ae_storage::hyperchain;
use ae_types::AccountPublicKey;
use ae_vm_fate::value::ObjectPayload;
use common::hyperchain::{Node, Parent};
use num_bigint::BigInt;
use serde_json::{Value as J, json};
use std::sync::{Arc, LazyLock};
static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_hc_key_candidate.json")).unwrap()
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

fn input(parent: ae_types::Hash256, c: &J) -> build::KeyInput {
    build::KeyInput {
        parent,
        production: candidate::KeyInput {
            miner: AccountPublicKey(bytes(&c["miner"]).try_into().unwrap()),
            beneficiary: AccountPublicKey(bytes(&c["beneficiary"]).try_into().unwrap()),
            hole: c["hole"].as_bool().unwrap(),
            end_of_epoch: c["eoe"].as_bool().unwrap(),
            time: c["clocks"][0].as_u64().unwrap(),
            release_info: c["release"].as_u64().unwrap() as u32,
        },
    }
}
fn config(node: &Node, c: &J, height: u64) -> hyperchain::Config<ae_vm_fate::maps::order::Otp26> {
    let mut config = node.config.clone();
    if c["signal"] == true {
        let profile = Arc::make_mut(&mut config.profile);
        profile.network = profile
            .network
            .clone()
            .with_community_fork(Some(ae_protocol::signaling::Fork {
                start: height,
                end: height + 1,
                threshold: 1,
                info: 987654,
                version: 9,
            }))
            .unwrap();
    }
    config
}
fn native_case(node: &Node, base: &ae_hyperchain::block::State, c: &J) -> Option<Block> {
    let view = node.db.view().unwrap();
    let before = view.status();
    let parent = before.tip.unwrap();
    let previous = view.require_block(parent).unwrap().header();
    let height = previous.common().height + u64::from(matches!(previous, Header::Key(_)));
    let config = config(node, c, height);
    let mut state = base.clone();
    let mut host = Parent::default();
    let expected = &c["result"];
    let result = build::key(&view, &config, input(parent, c), &mut state, &mut host);
    if expected["error"].is_null() {
        assert!(
            result.is_ok(),
            "{} at {height}: {:?}",
            c["name"],
            result.as_ref().err()
        );
    }
    assert_eq!(
        norm(&cache(&state)),
        norm(&expected["cache"]),
        "cache {} at {}",
        c["name"],
        height
    );
    assert_eq!(node.db.view().unwrap().status(), before);
    if !expected["error"].is_null() {
        assert_eq!(
            expected["error"],
            json!([
                "tuple",
                [
                    ["atom", "error"],
                    [
                        "tuple",
                        [
                            ["atom", "leader_validation_failed"],
                            ["atom", "invalid_leader"]
                        ]
                    ]
                ]
            ])
        );
        assert!(matches!(
            result,
            Err(build::Error::Candidate(candidate::Error::Chain(
                core::Error::InvalidLeader
            )))
        ));
        assert_eq!(expected["reads"], json!([c["clocks"][0]]));
        return None;
    }
    let prepared = result.unwrap();
    let candidate = prepared.candidate;
    assert_eq!(prepared.status, before);
    assert_eq!(prepared.source_id, view.source().source_id());
    assert_eq!(
        hex::encode(candidate.trees().roots().hash().0),
        expected["trees"]["state_root"]
    );
    for a in expected["trees"]["accounts"].as_array().unwrap() {
        let k = AccountPublicKey(bytes(&a["key"]).try_into().unwrap());
        assert_eq!(
            candidate
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
    let env = candidate.calculation_environment();
    assert_eq!(
        env.difficulty.to_string(),
        expected["temporary"]["difficulty"]
    );
    assert_eq!(
        env.timestamp.to_string(),
        expected["temporary"]["time"].to_string()
    );
    assert_eq!(hex::encode(env.key_hash), expected["temporary"]["hash"]);
    let temporary = Header::decode(
        &bytes(&expected["temporary"]["header"]),
        HeaderMode::Hyperchain,
    )
    .unwrap();
    assert_eq!(temporary.hash().unwrap().0, env.key_hash);
    let header = candidate
        .finish::<()>(c["clocks"][1].as_u64().unwrap())
        .unwrap();
    assert_eq!(
        Block::Key(header.clone()).encode().unwrap(),
        bytes(&expected["block"])
    );
    assert_eq!(expected["reads"], c["clocks"]);
    let mut keys = keys();
    let Seal::Found(seal) = keys.seal(&header).unwrap() else {
        panic!()
    };
    let signed = Block::Key(ae_codec::headers::KeyHeader { seal, ..header });
    assert_eq!(signed.encode().unwrap(), bytes(&expected["signed"]));
    let admission = repo::prepare_hyperchain(
        &view,
        &signed,
        &config,
        1_000_000,
        &mut base.clone(),
        &mut Parent::default(),
    );
    let native = &expected["admission"];
    if native["value"] == json!(["atom", "ok"]) {
        admission.unwrap();
    } else if native["value"] == json!(["tuple", [["atom", "error"], ["atom", "eoe_invalid"]]]) {
        assert!(
            matches!(admission,Err(repo::HyperchainError::Execution(ref e)) if matches!(**e,core::Error::InvalidEndOfEpoch)),
            "{admission:?}"
        );
    } else {
        assert_eq!(
            native["value"],
            json!([
                "tuple",
                [
                    ["atom", "error"],
                    [
                        "tuple",
                        [
                            ["atom", "header"],
                            ["atom", "signature_verification_failed"]
                        ]
                    ]
                ]
            ])
        );
        assert!(
            matches!(admission,Err(repo::HyperchainError::Execution(ref e)) if matches!(**e,core::Error::Seal(ae_hyperchain::signing::Error::Signature))),
            "{admission:?}"
        );
    }
    assert_eq!(node.db.view().unwrap().status(), before);
    Some(signed)
}
#[test]
fn original_key_hole_state_headers_seals_and_admission_match_both_profiles() {
    for profile in ["profile", "repeat_profile"] {
        for row in DATA["rows"].as_array().unwrap() {
            let mut node = Node::new(row["corpus_index"].as_u64().unwrap() as usize);
            Arc::make_mut(&mut node.config.profile).order =
                common::hyperchain::order(&DATA[profile]);
            let owner =
                hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default())
                    .unwrap();
            for c in row["candidates"].as_array().unwrap() {
                if c["signal"] != true {
                    native_case(&node, &owner.snapshot().unwrap().1, c);
                }
            }
            for (i, b) in node.blocks().iter().enumerate() {
                owner.insert(b, 1_000_000, Origin::Gossip).unwrap();
                node.check(i);
                for c in row["rows"][i]["candidates"].as_array().unwrap() {
                    if c["signal"] != true {
                        native_case(&node, &owner.snapshot().unwrap().1, c);
                    }
                }
            }
        }
    }
}

#[test]
fn signaling_candidates_match_when_fork_configuration_is_bound_before_bootstrap() {
    for profile in ["profile", "repeat_profile"] {
        for row in DATA["rows"].as_array().unwrap().iter().take(3) {
            for (count, cases) in std::iter::once((0, &row["candidates"])).chain(
                row["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .map(|(i, r)| (i + 1, &r["candidates"])),
            ) {
                let c = cases
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|c| c["signal"] == true)
                    .unwrap();
                let h = Header::decode(
                    &bytes(&c["result"]["temporary"]["header"]),
                    HeaderMode::Hyperchain,
                )
                .unwrap()
                .common()
                .height;
                let mut node = Node::with_fork(
                    row["corpus_index"].as_u64().unwrap() as usize,
                    ae_protocol::signaling::Fork {
                        start: h,
                        end: h + 1,
                        threshold: 1,
                        info: 987654,
                        version: 9,
                    },
                );
                Arc::make_mut(&mut node.config.profile).order =
                    common::hyperchain::order(&DATA[profile]);
                let owner = hyperchain::Owner::start(
                    node.db.clone(),
                    node.config.clone(),
                    Parent::default(),
                )
                .unwrap();
                for b in node.blocks().iter().take(count) {
                    owner.insert(b, 1_000_000, Origin::Gossip).unwrap();
                }
                native_case(&node, &owner.snapshot().unwrap().1, c);
            }
        }
    }
}

#[test]
fn generated_eoe_keys_commit_and_reopen_across_protocols() {
    for row in DATA["rows"].as_array().unwrap() {
        for index in [4, 9] {
            let node = Node::new(row["corpus_index"].as_u64().unwrap() as usize);
            let owner =
                hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default())
                    .unwrap();
            for b in node.blocks().iter().take(index + 1) {
                owner.insert(b, 1_000_000, Origin::Gossip).unwrap();
            }
            let c = row["rows"][index]["candidates"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["name"] == "eoe")
                .unwrap();
            let block = native_case(&node, &owner.snapshot().unwrap().1, c).unwrap();
            owner.insert(&block, 1_000_000, Origin::Created).unwrap();
            drop(owner);
            let reopened = node.reopen();
            let view = reopened.db.view().unwrap();
            let hash = block.hash().unwrap();
            assert_eq!(view.status().tip, Some(hash));
            assert_eq!(view.require_block(hash).unwrap(), block);
            assert_eq!(
                view.require_record(hash).unwrap().roots.state.hash().0,
                block.header().common().state_hash
            );
        }
    }
}

#[test]
fn key_preparation_operational_failures_preserve_cache_host_roots_and_allow_retry() {
    let node = Node::new(3);
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    for b in node.blocks().iter().take(5) {
        owner.insert(b, 1_000_000, Origin::Gossip).unwrap();
    }
    let view = node.db.view().unwrap();
    let before = view.status();
    let base = owner.snapshot().unwrap().1;
    let c = DATA["rows"][1]["rows"][4]["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "eoe")
        .unwrap();
    let input = input(before.tip.unwrap(), c);
    for failure in 0..7 {
        let mut config = node.config.clone();
        let mut state = base.clone();
        let mut host = Parent::default();
        match failure {
            0 => Arc::make_mut(&mut config.profile).maximum_steps = 0,
            1 => config.limits.overlay.nodes = 0,
            2 => config.limits.history = 0,
            3 => config.rules.max_reward_history = 0,
            4 => config.rules.max_calls = 0,
            5 => host.unavailable = true,
            6 => config.identity.genesis = ae_types::Hash256::ZERO,
            _ => unreachable!(),
        }
        let saved = host.clone();
        let Err(e) = build::key(&view, &config, input, &mut state, &mut host) else {
            panic!("expected failure {failure}")
        };
        assert!(e.is_operational::<Parent>(), "{failure}: {e:?}");
        assert_eq!(state, base);
        assert_eq!(host, saved);
        assert_eq!(node.db.view().unwrap().status(), before);
    }
    native_case(&node, &base, c);
    // Changing the protocol identity of an existing database must stay closed.
    let mut wrong = node.config.clone();
    let p = Arc::make_mut(&mut wrong.profile);
    p.network = p
        .network
        .clone()
        .with_community_fork(Some(ae_protocol::signaling::Fork {
            start: 3,
            end: 4,
            threshold: 1,
            info: 98,
            version: 9,
        }))
        .unwrap();
    assert!(matches!(
        build::key(
            &view,
            &wrong,
            input,
            &mut base.clone(),
            &mut Parent::default()
        ),
        Err(build::Error::Repository(repo::Error::Identity))
    ));
}

#[test]
fn captured_key_state_survives_top_advance_and_missing_seal_retry() {
    let node = Node::new(3);
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    let blocks = node.blocks();
    for b in blocks.iter().take(5) {
        owner.insert(b, 1_000_000, Origin::Gossip).unwrap();
    }
    let view = node.db.view().unwrap();
    let before = view.status();
    let mut state = owner.snapshot().unwrap().1;
    let c = DATA["rows"][1]["rows"][4]["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "eoe")
        .unwrap();
    let prepared = build::key(
        &view,
        &node.config,
        input(before.tip.unwrap(), c),
        &mut state,
        &mut Parent::default(),
    )
    .unwrap();
    let candidate = prepared.candidate;
    let roots = candidate.trees().roots();
    let time = c["clocks"][1].as_u64().unwrap();
    let header = candidate.finish::<()>(time).unwrap();
    assert_eq!(
        Keys::default().seal(&header).unwrap(),
        Seal::NoSolution { retry_ms: 1000 }
    );
    owner.insert(&blocks[5], 1_000_000, Origin::Gossip).unwrap();
    assert_ne!(node.db.view().unwrap().status(), before);
    assert_eq!(view.status(), before);
    assert_eq!(candidate.trees().roots(), roots);
    assert_eq!(candidate.finish::<()>(time).unwrap(), header);
    let Seal::Found(seal) = keys().seal(&header).unwrap() else {
        panic!()
    };
    let signed = Block::Key(ae_codec::headers::KeyHeader { seal, ..header });
    assert_eq!(signed.encode().unwrap(), bytes(&c["result"]["signed"]));
    owner.insert(&signed, 1_000_000, Origin::Created).unwrap();
    assert_eq!(
        node.db
            .view()
            .unwrap()
            .require_record(signed.hash().unwrap())
            .unwrap()
            .roots
            .state,
        roots
    );
}
