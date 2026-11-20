use super::*;
use serde_json::{Value as J, json};
use std::{collections::BTreeSet, sync::Arc};
#[derive(Clone, Debug, Default)]
struct Memory {
    kv: Arc<BTreeMap<Vec<u8>, Vec<u8>>>,
}
impl NodeReader for Memory {
    fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        Ok(self.kv.get(&key(NODES, h.0)).cloned())
    }
}
impl Read for Memory {
    fn source_id(&self) -> u64 {
        1
    }
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.kv.get(k).cloned())
    }
    fn prefix(&self, p: &[u8], limit: usize) -> Result<Entries, Error> {
        let a = self
            .kv
            .range(p.to_vec()..)
            .take_while(|(k, _)| k.starts_with(p))
            .take(limit.saturating_add(1))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        if a.len() > limit {
            Err(Error::ResourceLimit)
        } else {
            Ok(a)
        }
    }
}
impl Memory {
    fn apply(&mut self, w: Writes) {
        for (k, v) in w {
            match v {
                Some(v) => {
                    Arc::make_mut(&mut self.kv).insert(k, v);
                }
                None => {
                    Arc::make_mut(&mut self.kv).remove(&k);
                }
            }
        }
    }
}
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn bin(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(b)])
}
fn tuple(v: Vec<J>) -> J {
    json!(["tuple", v])
}
fn h(v: &J) -> Hash256 {
    Hash256(
        hex::decode(v.as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap(),
    )
}
fn u(v: &J) -> Unsigned {
    Unsigned::from_decimal(v.as_str().unwrap()).unwrap()
}
fn data() -> J {
    serde_json::from_str(include_str!("../../tests/fixtures/erlang_chain_store.json")).unwrap()
}
fn entries(v: &View<Memory>, prefix: u8) -> J {
    json!([
        "list",
        v.source
            .prefix(&[prefix], 1000)
            .unwrap()
            .into_iter()
            .map(|(k, b)| tuple(vec![bin(&k[1..]), bin(b)]))
            .collect::<Vec<_>>()
    ])
}
#[test]
fn original_pow_branch_traces_match_work_ties_locations_and_requeue_order() {
    let d = data();
    let mut transitions = 0;
    for trace in d["traces"].as_array().unwrap() {
        let trace = trace.as_array().unwrap();
        let first = Block::decode(
            &hex::decode(trace[0]["block"].as_str().unwrap()).unwrap(),
            DecodeConfig::default(),
        )
        .unwrap();
        let genesis = first.hash().unwrap();
        let mut db = Memory::default();
        let id = Identity::new(&NetworkProfile::mainnet(), genesis, Hash256::ZERO);
        Arc::make_mut(&mut db.kv).insert(IDENTITY.to_vec(), id.encode());
        Arc::make_mut(&mut db.kv).insert(STATUS.to_vec(), Status::default().encode());
        let mut pool = BTreeSet::new();
        for (i, row) in trace.iter().enumerate() {
            let b = Block::decode(
                &hex::decode(row["block"].as_str().unwrap()).unwrap(),
                DecodeConfig::default(),
            )
            .unwrap();
            let hash = b.hash().unwrap();
            let v = View::load(db.clone()).unwrap();
            let mut record = Record {
                roots: Roots {
                    state: StateRoots::default(),
                    names: None,
                    oracles: None,
                },
                difficulty: 0.into(),
                fees: 0.into(),
                fraud: false,
                fork: genesis,
            };
            let mut change = if i == 0 {
                record.difficulty = Target(match b {
                    Block::Key(ref k) => k.target,
                    _ => panic!(),
                })
                .difficulty()
                .unwrap();
                Change {
                    revision: 1,
                    inserted: hash,
                    old_tip: None,
                    new_tip: hash,
                    ancestor: None,
                    detached: vec![],
                    attached: vec![hash],
                    requeue: vec![],
                    confirm: vec![],
                }
            } else {
                let parent = Hash256(b.header().common().previous_hash);
                let parent_block = v.require_block(parent).unwrap();
                let prior = v.require_record(parent).unwrap();
                record.difficulty = match b {
                    Block::Key(ref k) => prior
                        .difficulty
                        .add(&Target(k.target).difficulty().unwrap()),
                    _ => prior.difficulty,
                };
                record.fork = if v.status.tip != Some(parent)
                    && !v.children(parent, 100).unwrap().is_empty()
                {
                    hash
                } else {
                    prior.fork
                };
                let fees = if matches!(parent_block, Block::Key(_)) {
                    0.into()
                } else {
                    prior.fees
                };
                record.fees = match &b {
                    Block::Key(_) => fees,
                    Block::Micro(m) => m
                        .transactions
                        .iter()
                        .fold(fees, |a, t| a.add(t.transaction().view().fee())),
                };
                selection(&v, &b, &record, 100).unwrap()
            };
            assert_eq!(record.difficulty, u(&row["difficulty"]), "{}", row["name"]);
            assert_eq!(record.fees, u(&row["fees"]));
            assert_eq!(record.fork, h(&row["fork"]));
            assert_eq!(change.new_tip, h(&row["top"]));
            assert_eq!(
                change.old_tip != Some(change.new_tip),
                row["changed"].as_bool().unwrap()
            );
            let mut writes = Writes::new();
            store_block(&mut writes, &b, &record).unwrap();
            canonical(&v, &b, &mut change, &mut writes).unwrap();
            let mut effects = vec![];
            for tx in &change.requeue {
                pool.insert(*tx);
                effects.extend([
                    tuple(vec![atom("remove_location"), bin(tx.0)]),
                    tuple(vec![atom("pool_add"), bin(tx.0)]),
                    tuple(vec![atom("event"), atom("tx_received"), bin(tx.0)]),
                ]);
            }
            for (tx, block) in &change.confirm {
                pool.remove(tx);
                effects.extend([
                    tuple(vec![atom("add_location"), bin(tx.0), bin(block.0)]),
                    tuple(vec![atom("pool_remove"), bin(tx.0)]),
                ]);
            }
            assert_eq!(json!(["list", effects]), row["effects"], "{}", row["name"]);
            assert_eq!(
                json!(["list", pool.iter().map(|h| bin(h.0)).collect::<Vec<_>>()]),
                row["pool"]
            );
            let pending = publish(
                Update {
                    identity: id.clone(),
                    source_id: 1,
                    expected: v.status,
                    change,
                    writes,
                    events: vec![],
                    key_fraud: None,
                    proof: None,
                },
                1_000_000,
            )
            .unwrap();
            db.apply(pending.writes);
            let next = View::load(db.clone()).unwrap();
            assert_eq!(entries(&next, LOCATIONS), row["locations"]);
            assert_eq!(
                next.event(next.status.revision).unwrap().unwrap().new_tip,
                h(&row["top"])
            );
            assert!(v.block(hash).unwrap().is_none());
            transitions += 1;
            let branch = match b {
                Block::Key(ref k) => k.clone(),
                Block::Micro(ref m) => {
                    require_key(&next, Hash256(m.header.common.previous_key_hash)).unwrap()
                }
            };
            let snapshot = super::super::vm_history(&next, &branch, 100).unwrap();
            for height in 0..branch.common.height.min(
                next.require_block(next.status.tip.unwrap())
                    .unwrap()
                    .header()
                    .common()
                    .height
                    + 1,
            ) {
                let mut k = branch.clone();
                while k.common.height > height {
                    k = require_key(&next, Hash256(k.common.previous_key_hash)).unwrap();
                }
                let expected = Header::Key(k).hash().unwrap().0;
                let actual = ae_execution::chain_history::select(
                    &snapshot,
                    height,
                    Header::Key(branch.clone()).hash().unwrap().0,
                    || Ok(()),
                )
                .unwrap();
                assert_eq!(actual.branch, expected);
                assert_eq!(
                    Some(Hash256(actual.selected)),
                    next.canonical_key(height).unwrap()
                );
            }
            assert!(matches!(
                super::super::vm_history(&next, &branch, 0),
                Err(Error::ResourceLimit)
            ));
            if i > 0 {
                assert!(
                    matches!(selection(&v, &b, &record, 0), Err(Error::ResourceLimit))
                        || Hash256(b.header().common().previous_hash) == v.status.tip.unwrap()
                        || v.require_record(v.status.tip.unwrap()).unwrap().difficulty
                            >= record.difficulty
                );
            }
        }
    }
    assert_eq!(transitions, 63);
    for r in d["comparisons"].as_array().unwrap() {
        assert_eq!(
            keep_pow_tip(&u(&r["old"]), &u(&r["new"])),
            r["keep"].as_bool().unwrap()
        );
    }
}
#[test]
fn storage_refuses_to_lose_noncanonical_transaction_size_metadata() {
    use ae_tx::{CanonicalTransaction, SignedTransaction, Transaction};
    let d = data();
    let row = &d["traces"][0][2];
    let mut block = Block::decode(
        &hex::decode(row["block"].as_str().unwrap()).unwrap(),
        DecodeConfig::default(),
    )
    .unwrap();
    let Block::Micro(ref mut micro) = block else {
        panic!()
    };
    let canonical = micro.transactions[0].transaction().encode();
    assert_eq!(canonical[0], 0xf8);
    assert!(canonical[2] < 0x80);
    let mut incoming = vec![0xf8, canonical[1] + 1, 0x81];
    incoming.extend_from_slice(&canonical[2..]);
    let transaction = Transaction::decode(&incoming).unwrap();
    assert_eq!(transaction.encode(), canonical);
    assert_ne!(transaction.view().wire_size(), canonical.len());
    micro.transactions[0] = SignedTransaction::new(transaction, vec![]);
    let record = Record {
        roots: Roots {
            state: StateRoots::default(),
            names: None,
            oracles: None,
        },
        difficulty: 0.into(),
        fees: 0.into(),
        fraud: false,
        fork: Hash256::ZERO,
    };
    let mut writes = Writes::new();
    assert_eq!(
        store_block(&mut writes, &block, &record),
        Err(Error::UnsupportedBlock)
    );
    assert!(writes.is_empty());
    assert!(InsertError::<Memory>::from(Error::UnsupportedBlock).is_operational());
}
#[test]
fn generation_reset_policy_and_persistent_records_reject_malformed_values() {
    for r in data()["metadata"].as_array().unwrap() {
        let d = data();
        let row = &d["traces"][0][if r["parent"] == "key" { 1 } else { 2 }];
        let b = Block::decode(
            &hex::decode(row["block"].as_str().unwrap()).unwrap(),
            DecodeConfig::default(),
        )
        .unwrap();
        let record = Record {
            roots: Roots {
                state: StateRoots::default(),
                names: None,
                oracles: None,
            },
            difficulty: 77.into(),
            fees: u(&r["fees"]),
            fraud: r["fraud"].as_bool().unwrap(),
            fork: Hash256::ZERO,
        };
        let (fees, fraud) = incoming(&b.header(), &record);
        assert_eq!(
            tuple(vec![
                json!(["integer", fees.to_string()]),
                atom(if fraud { "true" } else { "false" })
            ]),
            r["result"]
        );
    }
    for height in [0, 1, 4, 5, 6, 10, 11] {
        assert_eq!(
            check_policy(height, 10, Policy::default()).is_ok(),
            height >= 5
        );
        assert_eq!(
            check_policy(
                height,
                10,
                Policy {
                    origin: Origin::Sync,
                    sync: SyncPolicy::Finalized(5),
                    ..Default::default()
                }
            )
            .is_ok(),
            height > 5
        );
        assert_eq!(
            check_policy(
                height,
                10,
                Policy {
                    origin: Origin::Sync,
                    sync: SyncPolicy::Depth(5),
                    ..Default::default()
                }
            )
            .is_ok(),
            height >= 5
        );
        assert!(
            check_policy(
                height,
                10,
                Policy {
                    origin: Origin::Sync,
                    ..Default::default()
                }
            )
            .is_ok()
        );
    }
    for bytes in [&[][..], &[0xc0], &[0xff], &[0x80]] {
        assert!(Record::decode(bytes).is_err());
        assert!(Identity::decode(bytes).is_err());
        assert!(Change::decode(bytes).is_err());
        assert!(Status::decode(bytes).is_err());
    }
    assert!(
        Status::decode(
            &Status {
                revision: 0,
                tip: Some(Hash256::ZERO)
            }
            .encode()
        )
        .is_err()
    );
}
