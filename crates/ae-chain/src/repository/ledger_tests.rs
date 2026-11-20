use super::*;
use crate::block::DecodeConfig;
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
    fn visit_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        visit: &mut Visitor<'_>,
    ) -> Result<(), Error> {
        for (i, (k, v)) in self.kv.range(start.to_vec()..end.to_vec()).enumerate() {
            if i >= limit {
                return Err(Error::ResourceLimit);
            }
            visit(k, v)?;
        }
        Ok(())
    }
    fn range(&self, start: &[u8], end: &[u8], limit: usize) -> Result<Entries, Error> {
        let out: Vec<_> = self
            .kv
            .range(start.to_vec()..end.to_vec())
            .take(limit.saturating_add(1))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if out.len() > limit {
            Err(Error::ResourceLimit)
        } else {
            Ok(out)
        }
    }
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
                    inserted: Some(hash),
                    removed: vec![],
                    discarded: vec![],
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
            store_block(&mut writes, &b, &record, &ReadConfig::default()).unwrap();
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
                    policy_generation: v.resilience().generation,
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
fn storage_preserves_noncanonical_transaction_size_metadata() {
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
    store_block(&mut writes, &block, &record, &ReadConfig::default()).unwrap();
    let id = Identity::new(&NetworkProfile::mainnet(), Hash256::ZERO, Hash256::ZERO);
    put(&mut writes, IDENTITY.to_vec(), id.encode());
    put(&mut writes, STATUS.to_vec(), Status::default().encode());
    let mut memory = Memory::default();
    memory.apply(writes);
    let v = View::load(memory).unwrap();
    assert_eq!(v.require_block(block.hash().unwrap()).unwrap(), block);
}
#[test]
fn original_mnesia_records_preserve_metadata_and_share_last_written_transactions() {
    use ae_tx::{SignedTransaction, Transaction};
    fn projection(tx: &SignedTransaction<Transaction>) -> J {
        let mut layers = vec![];
        let mut view = tx.transaction().view();
        let mut signatures = tx.signatures();
        loop {
            layers.push(json!({
                "size": view.wire_size(),
                "signatures": signatures.iter().map(|s| hex::encode(s.0)).collect::<Vec<_>>(),
                "prices": (1..=8).map(|p| {
                    let costs = view.pricing(&50.into(), ae_protocol::Protocol::try_from(p).unwrap());
                    json!({"protocol":p,"fee":costs.fee_gas.to_string(),"gas":costs.gas_limit.to_string(),"minimum":costs.minimum_fee.to_string()})
                }).collect::<Vec<_>>()
            }));
            let Some(inner) = view.inner() else { break };
            signatures = view.layer().unwrap().inner_signatures();
            view = inner;
        }
        let context = match tx.transaction().leaf() {
            ae_tx::TransactionLeaf::Contract(t)
                if matches!(t.operation(), ae_tx::ContractOperation::Call { .. }) =>
            {
                json!([
                    hex::encode(t.call_origin()),
                    t.call_stack()
                        .iter()
                        .map(|n| num_bigint::BigUint::from_bytes_be(n.as_bytes()).to_string())
                        .collect::<Vec<_>>()
                ])
            }
            _ => json!([]),
        };
        json!({"canonical":hex::encode(tx.encode()),"hash":hex::encode(tx.hash().0),"layers":layers,"context":context})
    }
    let data: J =
        serde_json::from_str(include_str!("../../tests/fixtures/erlang_tx_records.json")).unwrap();
    let id = Identity::new(&NetworkProfile::mainnet(), Hash256::ZERO, Hash256::ZERO);
    let mut initial = Writes::new();
    put(&mut initial, IDENTITY.to_vec(), id.encode());
    put(&mut initial, STATUS.to_vec(), Status::default().encode());
    let mut db = Memory::default();
    db.apply(initial);
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
    let mut kinds = BTreeSet::new();
    for row in data["rows"].as_array().unwrap() {
        let first = h(&row["hashes"][0]);
        let mut retained = None;
        for (step, expected) in ["before", "after_noncanonical", "after_canonical"]
            .into_iter()
            .enumerate()
        {
            let block = Block::decode(
                &hex::decode(row["inputs"][step].as_str().unwrap()).unwrap(),
                Default::default(),
            )
            .unwrap();
            assert_eq!(block.hash().unwrap(), h(&row["hashes"][step]));
            let Block::Micro(m) = &block else { panic!() };
            let tx = &m.transactions[0];
            kinds.insert(format!("{:?}", tx.transaction().view().kind()));
            assert_eq!(projection(tx), row[expected]);
            let encoded = ae_tx::storage::encode(tx, Default::default()).unwrap();
            let restored = ae_tx::storage::decode(&encoded, Default::default()).unwrap();
            assert_eq!(restored, *tx);
            let mut writes = Writes::new();
            store_block(&mut writes, &block, &record, &ReadConfig::default()).unwrap();
            db.apply(writes);
            let view = View::load(db.clone()).unwrap();
            assert_eq!(view.transaction(tx.hash()).unwrap().unwrap(), *tx);
            let Block::Micro(old) = view.require_block(first).unwrap() else {
                panic!()
            };
            assert_eq!(projection(&old.transactions[0]), row[expected]);
            if step == 0 {
                retained = Some(view);
            }
            let Block::Micro(pinned) = retained.as_ref().unwrap().require_block(first).unwrap()
            else {
                panic!()
            };
            assert_eq!(projection(&pinned.transactions[0]), row["before"]);
        }
    }
    assert_eq!(kinds.len(), 27);
    assert_eq!(data["rows"].as_array().unwrap().len(), 264);
    for row in data["contexts"].as_array().unwrap() {
        let raw = hex::decode(row["block"].as_str().unwrap()).unwrap();
        let (ae_codec::headers::Header::Micro(header), _) =
            ae_codec::headers::Header::decode_partial(
                &raw,
                ae_codec::headers::HeaderMode::ProofOfWork,
            )
            .unwrap()
        else {
            panic!()
        };
        let record_bytes = list(vec![
            binary(hex::decode(row["expected"]["canonical"].as_str().unwrap()).unwrap()),
            number(row["size"].as_u64().unwrap()),
            list(vec![]),
            list(vec![]),
            list(vec![
                binary(hex::decode(row["context"][0].as_str().unwrap()).unwrap()),
                list(
                    row["context"][1]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|n| binary(u(n).as_bytes()))
                        .collect(),
                ),
            ]),
        ])
        .encode();
        let mut stored = ae_tx::storage::MAGIC.to_vec();
        stored.extend(record_bytes);
        let tx = ae_tx::storage::decode(&stored, Default::default()).unwrap();
        let ae_tx::TransactionLeaf::Contract(call) = tx.transaction().leaf() else {
            panic!()
        };
        assert_eq!(
            Block::decode(&raw, Default::default()).is_ok(),
            call.actor().tag == ae_types::IdTag::Account
        );
        let block = Block::Micro(crate::block::MicroBlock {
            header,
            transactions: vec![tx],
            proof: None,
        });
        assert_eq!(block.encode().unwrap(), raw);
        let Block::Micro(m) = &block else { panic!() };
        assert_eq!(projection(&m.transactions[0]), row["expected"]);
        let mut writes = Writes::new();
        store_block(&mut writes, &block, &record, &ReadConfig::default()).unwrap();
        db.apply(writes);
        let Block::Micro(restored) = View::load(db.clone())
            .unwrap()
            .require_block(block.hash().unwrap())
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(projection(&restored.transactions[0]), row["expected"]);
    }
    assert_eq!(data["contexts"].as_array().unwrap().len(), 36);
    assert_eq!(data["restart_blocks"], 828);
}
#[test]
fn stored_channel_trees_require_and_retain_the_explicit_atom_profile() {
    use ae_tx::{CanonicalTransaction, SignedTransaction, Transaction, WireTransaction};
    let data: J =
        serde_json::from_str(include_str!("../../tests/fixtures/erlang_tx_records.json")).unwrap();
    let row = data["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "aesc_force_progress_tx_1")
        .unwrap();
    let mut block = Block::decode(
        &hex::decode(row["inputs"][0].as_str().unwrap()).unwrap(),
        Default::default(),
    )
    .unwrap();
    let Block::Micro(m) = &mut block else {
        panic!()
    };
    let wire = WireTransaction::decode(&m.transactions[0].transaction().encode()).unwrap();
    let mut trees = Trees {
        accounts: Default::default(),
        contracts: Contracts::new(MemoryStore::default()),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    let mut name =
        ae_state::names::Name::new([42; 32], ae_types::AccountPublicKey([1; 32]), 100.into());
    name.status = b"stored_channel_status".to_vec();
    trees.names.enter_name(&name).unwrap();
    let state = trees.serialize_to_binary(Default::default()).unwrap();
    let fields = wire
        .fields()
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                if k == "offchain_trees" {
                    ae_codec::object::Value::Binary(state.clone())
                } else {
                    v.clone()
                },
            )
        })
        .collect();
    let encoded = WireTransaction::new(wire.kind(), wire.version(), fields)
        .unwrap()
        .encode();
    let config = ReadConfig {
        existing_name_status_atoms: Some(Arc::new(BTreeSet::from([name.status]))),
        ..Default::default()
    };
    let tx =
        Transaction::decode_with_state_config(&encoded, config.transaction, config.block().state)
            .unwrap();
    m.transactions[0] = SignedTransaction::new(tx, vec![]);
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
    assert!(matches!(
        store_block(&mut writes, &block, &record, &ReadConfig::default()),
        Err(Error::TransactionStorage(
            ae_tx::storage::Error::Transaction(ae_tx::Error::StateTransport(
                ae_state::transport::Error::MissingAtomProfile(_)
            ))
        ))
    ));
    assert!(writes.is_empty());
    store_block(&mut writes, &block, &record, &config).unwrap();
    put(
        &mut writes,
        IDENTITY.to_vec(),
        Identity::new(&NetworkProfile::mainnet(), Hash256::ZERO, Hash256::ZERO).encode(),
    );
    put(&mut writes, STATUS.to_vec(), Status::default().encode());
    let mut db = Memory::default();
    db.apply(writes);
    let hash = block.hash().unwrap();
    assert!(matches!(
        View::load(db.clone()).unwrap().require_block(hash),
        Err(Error::TransactionStorage(
            ae_tx::storage::Error::Transaction(ae_tx::Error::StateTransport(
                ae_state::transport::Error::MissingAtomProfile(_)
            ))
        ))
    ));
    assert_eq!(
        View::load_with_config(db, config)
            .unwrap()
            .require_block(hash)
            .unwrap(),
        block
    );
}
#[test]
fn block_and_transaction_record_corruption_cannot_replace_a_pinned_view() {
    let block = Block::decode(
        &hex::decode(data()["traces"][0][2]["block"].as_str().unwrap()).unwrap(),
        Default::default(),
    )
    .unwrap();
    let Block::Micro(m) = &block else { panic!() };
    let tx = m.transactions[0].hash();
    let hash = block.hash().unwrap();
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
    store_block(&mut writes, &block, &record, &ReadConfig::default()).unwrap();
    put(
        &mut writes,
        IDENTITY.to_vec(),
        Identity::new(&NetworkProfile::mainnet(), Hash256::ZERO, Hash256::ZERO).encode(),
    );
    put(&mut writes, STATUS.to_vec(), Status::default().encode());
    let mut db = Memory::default();
    db.apply(writes);
    let pinned = View::load(db.clone()).unwrap();
    let block_key = key(BLOCKS, hash.0);
    let tx_key = key(TRANSACTIONS, tx.0);
    let mut limited = ReadConfig::default();
    limited.body.max_items = 0;
    let mut empty = Writes::new();
    assert_eq!(
        store_block(&mut empty, &block, &record, &limited),
        Err(Error::ResourceLimit)
    );
    assert!(empty.is_empty());
    limited = ReadConfig {
        max_transactions: 0,
        ..Default::default()
    };
    assert_eq!(
        View::load_with_config(db.clone(), limited)
            .unwrap()
            .require_block(hash),
        Err(Error::ResourceLimit)
    );
    for mode in 0..7 {
        let mut bad = db.clone();
        let map = Arc::make_mut(&mut bad.kv);
        match mode {
            0 => {
                map.remove(&tx_key);
            }
            1 => {
                map.get_mut(&tx_key).unwrap()[5] = 99;
            }
            2 => {
                map.get_mut(&block_key).unwrap()[8] = 99;
            }
            3 => {
                map.get_mut(&block_key).unwrap().push(0);
            }
            _ => {
                let raw = map.get(&block_key).unwrap();
                let Value::List(mut f) = Value::decode(&raw[9..]).unwrap() else {
                    panic!()
                };
                match mode {
                    4 => f[1] = list(vec![binary([42; 32])]),
                    5 => f[2] = binary([0]),
                    _ => {
                        let Value::Bytes(header) = &mut f[0] else {
                            panic!()
                        };
                        header[10] ^= 1;
                    }
                }
                let mut altered = raw[..9].to_vec();
                altered.extend(list(f).encode());
                map.insert(block_key.clone(), altered);
            }
        }
        assert!(
            View::load(bad).unwrap().require_block(hash).is_err(),
            "mode {mode}"
        );
        assert_eq!(pinned.require_block(hash).unwrap(), block);
    }
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
            check_policy(height, 10, Policy::default(), Default::default(), None).is_ok(),
            height >= 5
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

#[path = "rollback_tests.rs"]
mod rollback_tests;
#[path = "scan_tests.rs"]
mod scan_tests;
