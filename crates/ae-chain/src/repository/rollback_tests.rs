use super::*;
use crate::repository::rollback::{self, Failure, Target};
use ae_protocol::{Activation, Protocol};
use std::sync::LazyLock;
static ROLLBACK: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../../tests/fixtures/erlang_rollback.json")).unwrap()
});
fn decode_block(value: &J) -> Block {
    Block::decode(
        &hex::decode(value.as_str().unwrap()).unwrap(),
        DecodeConfig::default(),
    )
    .unwrap()
}
fn setup(row: &J) -> Memory {
    let blocks: Vec<_> = row["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r, decode_block(&r["block"])))
        .collect();
    let genesis = blocks[0].1.hash().unwrap();
    let network = NetworkProfile::custom(
        b"ae_rollback".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::Ceres,
        }],
    )
    .unwrap();
    let id = Identity::new(&network, genesis, Hash256::ZERO);
    let mut db = Memory::default();
    let mut writes = Writes::new();
    writes.insert(IDENTITY.to_vec(), Some(id.encode()));
    for (row, b) in blocks {
        let hash = b.hash().unwrap();
        let record = Record {
            roots: Roots {
                state: StateRoots::default(),
                names: None,
                oracles: None,
            },
            difficulty: 1.into(),
            fees: 0.into(),
            fraud: false,
            fork: genesis,
        };
        store_block(&mut writes, &b, &record, &ReadConfig::default()).unwrap();
        if row["selected"] == true {
            writes.insert(key(CANONICAL, hash.0), Some(vec![]));
            if let Block::Key(k) = &b {
                writes.insert(
                    key(KEYS, k.common.height.to_be_bytes()),
                    Some(hash.0.to_vec()),
                );
            }
        }
        if let Block::Key(k) = &b {
            writes.insert(key(ENDS, hash.0), Some(vec![]));
            writes.insert(key(ENDS, k.common.previous_key_hash), None);
        }
    }
    for pair in row["before"]["locations"].as_array().unwrap() {
        writes.insert(key(LOCATIONS, h(&pair[0]).0), Some(h(&pair[1]).0.to_vec()));
    }
    let top = h(&row["before"]["top"]);
    writes.insert(
        STATUS.to_vec(),
        Some(
            Status {
                revision: 1,
                tip: Some(top),
            }
            .encode(),
        ),
    );
    let change = Change {
        revision: 1,
        inserted: Some(top),
        removed: vec![],
        discarded: vec![],
        old_tip: None,
        new_tip: top,
        ancestor: None,
        detached: vec![],
        attached: vec![top],
        requeue: vec![],
        confirm: vec![],
    };
    writes.insert(key(EVENTS, 1u64.to_be_bytes()), Some(change.encode()));
    db.apply(writes);
    db
}
fn set(v: &J) -> BTreeSet<Hash256> {
    v.as_array().unwrap().iter().map(h).collect()
}
#[test]
fn original_rollback_boundaries_match_and_surviving_shared_transactions_remain_readable() {
    let mut accepted = 0;
    let mut native_dangling = 0;
    let mut shared_repairs = 0;
    for row in ROLLBACK["rows"].as_array().unwrap() {
        let mut db = setup(row);
        let view = View::load(db.clone()).unwrap();
        view.verify_tip().unwrap();
        let hash = h(&row["target"]);
        let target = match row["mode"].as_str().unwrap() {
            "height" => Target::Height(row["height"].as_u64().unwrap()),
            "key" => Target::Key(hash),
            "micro" => Target::Micro(hash),
            _ => panic!(),
        };
        let result = rollback::prepare(&view, target, Default::default());
        if !view.is_canonical(hash).unwrap() {
            assert_eq!(result.unwrap_err(), Failure::NotCanonical);
            if row["result"]["kind"] == "ok" {
                assert!(!set(&row["after"]["headers"]).contains(&hash));
                native_dangling += 1;
            }
            continue;
        }
        if row["result"]["kind"] != "ok" {
            assert_eq!(result.unwrap_err(), Failure::WrongType);
            continue;
        }
        let plan = result.unwrap_or_else(|e| panic!("{e:?}: {} {}", row["mode"], row["height"]));
        let before = set(&row["before"]["headers"]);
        let after = set(&row["after"]["headers"]);
        assert_eq!(
            plan.change.removed.iter().copied().collect::<BTreeSet<_>>(),
            before.difference(&after).copied().collect()
        );
        assert_eq!(plan.change.new_tip, h(&row["after"]["top"]));
        assert!(plan.change.inserted.is_none());
        let mut expected_transactions = set(&row["after"]["transactions"]);
        for r in row["blocks"].as_array().unwrap() {
            let b = decode_block(&r["block"]);
            if after.contains(&b.hash().unwrap())
                && let Block::Micro(m) = b
            {
                for tx in m.transactions {
                    if expected_transactions.insert(tx.hash()) {
                        shared_repairs += 1;
                    }
                }
            }
        }
        let detached: BTreeSet<_> = plan.change.detached.iter().copied().collect();
        db.apply(plan.writes.clone());
        let current = View::load(db.clone()).unwrap();
        current.verify_tip().unwrap();
        assert_eq!(
            current
                .source
                .prefix(&[BLOCKS], 100)
                .unwrap()
                .into_iter()
                .map(|(k, _)| fixed_hash(&k[1..]).unwrap())
                .collect::<BTreeSet<_>>(),
            after
        );
        assert_eq!(
            current
                .source
                .prefix(&[TRANSACTIONS], 100)
                .unwrap()
                .into_iter()
                .map(|(k, _)| fixed_hash(&k[1..]).unwrap())
                .collect::<BTreeSet<_>>(),
            expected_transactions
        );
        for hash in before {
            assert!(view.require_block(hash).is_ok());
            assert_eq!(
                current.block(hash).unwrap().is_some(),
                after.contains(&hash)
            );
            if !after.contains(&hash) {
                assert!(current.record(hash).unwrap().is_none());
                assert!(!current.is_canonical(hash).unwrap());
            }
        }
        for pair in row["before"]["locations"].as_array().unwrap() {
            let block = h(&pair[1]);
            let expected = (after.contains(&block) && !detached.contains(&block)).then_some(block);
            assert_eq!(current.location(h(&pair[0])).unwrap(), expected);
        }
        assert_eq!(current.event(2).unwrap(), Some(plan.change));
        accepted += 1;
    }
    assert_eq!(accepted, 52);
    assert_eq!(native_dangling, 4);
    assert!(shared_repairs > 0);
    eprintln!(
        "Rollback comparisons: {accepted} valid, {native_dangling} dangling targets rejected, {shared_repairs} shared references preserved"
    );
}
#[test]
fn rollback_limits_old_events_and_malformed_events_leave_inputs_unchanged() {
    let row = &ROLLBACK["rows"][2];
    let db = setup(row);
    let view = View::load(db.clone()).unwrap();
    let original = db.kv.clone();
    for limits in [
        rollback::Limits {
            blocks: 0,
            ..Default::default()
        },
        rollback::Limits {
            ancestry: 0,
            ..Default::default()
        },
        rollback::Limits {
            transactions: 0,
            ..Default::default()
        },
        rollback::Limits {
            reference_scan: 0,
            ..Default::default()
        },
        rollback::Limits {
            commit_bytes: 0,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            rollback::prepare(&view, Target::Height(2), limits),
            Err(Failure::Repository(Error::ResourceLimit))
        ));
        assert_eq!(view.source.kv, original);
    }
    assert!(matches!(
        rollback::prepare(&view, Target::Height(9999), Default::default()),
        Err(Failure::UnknownHeight(9999))
    ));
    let event = view.event(1).unwrap().unwrap();
    let old = event.encode_version(1);
    assert_eq!(Change::decode(&old).unwrap(), event);
    let plan = rollback::prepare(&view, Target::Height(2), Default::default()).unwrap();
    let encoded = plan.change.encode();
    assert_eq!(Change::decode(&encoded).unwrap(), plan.change);
    let value = decode(&encoded).unwrap();
    let mut parts = items(&value).unwrap().to_vec();
    parts[2] = binary(Hash256::ZERO.0);
    assert!(Change::decode(&list(parts).encode()).is_err());
    assert!(Change::decode(&[encoded, vec![0]].concat()).is_err());
}
#[test]
fn original_mainnet_startup_checkpoint_uses_a_strict_top_height_boundary() {
    for row in ROLLBACK["startup"].as_array().unwrap() {
        let mut db = setup(&ROLLBACK["rows"][0]);
        let mut header = match decode_block(&ROLLBACK["rows"][0]["blocks"][7]["block"]) {
            Block::Key(k) => k,
            _ => panic!(),
        };
        header.common.height = row["height"].as_u64().unwrap();
        let top = Block::Key(header);
        let hash = top.hash().unwrap();
        let mut writes = Writes::new();
        writes.insert(key(BLOCKS, hash.0), Some(top.encode().unwrap()));
        writes.insert(
            STATUS.to_vec(),
            Some(
                Status {
                    revision: 1,
                    tip: Some(hash),
                }
                .encode(),
            ),
        );
        let id = Identity::new(&NetworkProfile::mainnet(), Hash256::ZERO, Hash256::ZERO);
        writes.insert(IDENTITY.to_vec(), Some(id.encode()));
        writes.insert(
            key(KEYS, rollback::COMMUNITY_FORK_HEIGHT.to_be_bytes()),
            Some(if row["matching"] == true {
                rollback::community_fork_hash().0.to_vec()
            } else {
                Hash256::ZERO.0.to_vec()
            }),
        );
        db.apply(writes);
        let view = View::load(db.clone()).unwrap();
        let required = rollback::community_fork_target(&view).unwrap();
        assert_eq!(
            required.is_some(),
            row["restart_requested"].as_bool().unwrap()
        );
        if required.is_some() {
            assert_eq!(row["after"], row["checkpoint"]);
        } else {
            assert_eq!(row["after"], row["before"]);
        }
    }
}
