use super::*;
use crate::{
    block::{Block, MicroBlock},
    repository::{Identity, Status},
};
use ae_codec::headers::{CommonHeader, MicroHeader};
use ae_protocol::NetworkProfile;
use ae_trie::{MemoryStore, NodeReader};
use ae_tx::{CanonicalTransaction, SignedTransaction, Transaction};
use serde_json::Value;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
    sync::Arc,
};

#[derive(Clone, Default)]
struct Memory {
    data: Arc<BTreeMap<Vec<u8>, Vec<u8>>>,
    trace: Rc<RefCell<Vec<Vec<u8>>>>,
    copied: Rc<Cell<usize>>,
    fault: Option<Vec<u8>>,
}
impl Memory {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        Arc::make_mut(&mut self.data).insert(key, value);
    }
    fn remove(&mut self, key: &[u8]) {
        Arc::make_mut(&mut self.data).remove(key);
    }
}
impl NodeReader for Memory {
    fn node(&self, _: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        panic!("unmetered node read")
    }
}
impl Read for Memory {
    fn source_id(&self) -> u64 {
        1
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, repo::Error> {
        self.get_bounded(key, usize::MAX)
    }
    fn get_bounded(&self, key: &[u8], limit: usize) -> Result<Option<Vec<u8>>, repo::Error> {
        self.trace.borrow_mut().push(key.to_vec());
        if self.fault.as_deref() == Some(key) {
            return Err(repo::Error::Backend("injected".into()));
        }
        self.data
            .get(key)
            .map(|v| {
                if v.len() > limit {
                    return Err(repo::Error::ResourceLimit);
                }
                self.copied.set(self.copied.get() + v.len());
                Ok(v.clone())
            })
            .transpose()
    }
    fn prefix(&self, _: &[u8], _: usize) -> Result<repo::Entries, repo::Error> {
        panic!("scan")
    }
    fn range(&self, _: &[u8], _: &[u8], _: usize) -> Result<repo::Entries, repo::Error> {
        panic!("scan")
    }
    fn visit_range(
        &self,
        _: &[u8],
        _: &[u8],
        _: usize,
        _: &mut repo::Visitor<'_>,
    ) -> Result<(), repo::Error> {
        panic!("scan")
    }
}
fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_gas_transactions.json"
    ))
    .unwrap()
}
fn number(v: &Value) -> BigInt {
    super::super::tests::number(v)
}
fn database(r: &Value) -> (Memory, Hash256) {
    let trees = Trees::deserialize_from_binary(
        &hex::decode(r["trees"].as_str().unwrap()).unwrap(),
        MemoryStore::default(),
        MemoryStore::default(),
        Default::default(),
    )
    .unwrap();
    let record = Record {
        roots: Roots::of(&trees),
        difficulty: 1.into(),
        fees: 0.into(),
        fraud: false,
        fork: Hash256::ZERO,
    };
    let txs = r["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            SignedTransaction::new(
                Transaction::decode(&hex::decode(v.as_str().unwrap()).unwrap()).unwrap(),
                vec![],
            )
        })
        .collect();
    let block = Block::Micro(MicroBlock {
        header: MicroHeader {
            common: CommonHeader {
                version: r["protocol"].as_u64().unwrap() as u32,
                height: r["height"].as_u64().unwrap(),
                previous_hash: [0; 32],
                previous_key_hash: [0; 32],
                state_hash: record.roots.state.hash().0,
            },
            transactions_hash: [0; 32],
            time: 42,
            proof_of_fraud: None,
            signature: [0; 64],
        },
        transactions: txs,
        proof: None,
    });
    let hash = block.hash().unwrap();
    let mut db = Memory::default();
    if let Block::Micro(b) = &block {
        for tx in &b.transactions {
            db.put(repo::key(repo::TRANSACTIONS, tx.hash().0), tx.encode());
        }
    }
    for (h, b) in trees.calls.tree().store().nodes() {
        db.put(repo::key(repo::NODES, h.0), b.clone());
    }
    db.put(repo::key(repo::BLOCKS, hash.0), block.encode().unwrap());
    db.put(repo::key(repo::RECORDS, hash.0), record.encode());
    db.put(
        repo::IDENTITY.to_vec(),
        Identity::new(&NetworkProfile::mainnet(), hash, Hash256::ZERO).encode(),
    );
    db.put(
        repo::STATUS.to_vec(),
        Status {
            revision: 1,
            tip: Some(hash),
        }
        .encode(),
    );
    (db, hash)
}
fn reader(db: Memory) -> Reader<Memory> {
    Reader::new(
        &View::load(db).unwrap(),
        6_000_000.into(),
        ReadLimits::default(),
    )
    .unwrap()
}
fn modernize(db: &mut Memory, hash: Hash256) {
    use ae_trie::Value as V;
    let block = View::load(db.clone())
        .unwrap()
        .block(hash)
        .unwrap()
        .unwrap();
    let (hashes, proof) = match &block {
        Block::Micro(b) => (
            b.transactions
                .iter()
                .map(|t| V::Bytes(t.hash().0.to_vec()))
                .collect(),
            b.proof
                .as_ref()
                .map(|p| p.encode().unwrap())
                .unwrap_or_default(),
        ),
        Block::Key(_) => (vec![], vec![]),
    };
    let mut bytes = b"\0aeblock\x01".to_vec();
    bytes.extend(
        V::List(vec![
            V::Bytes(block.header().encode().unwrap()),
            V::List(hashes),
            V::Bytes(proof),
        ])
        .encode(),
    );
    db.put(repo::key(repo::BLOCKS, hash.0), bytes);
}
#[test]
fn original_micro_statistics_use_historical_pricing_and_resulting_call_state() {
    let f = fixture();
    assert_eq!(f["rows"].as_array().unwrap().len(), 224);
    for r in f["rows"].as_array().unwrap() {
        for modern in [false, true] {
            let (mut db, hash) = database(r);
            if modern {
                modernize(&mut db, hash);
            }
            let mut read = reader(db);
            assert_eq!(read.tip(), Some(hash));
            let header = read.header(hash).unwrap().unwrap();
            assert!(header.micro);
            assert_eq!(header.time, 42);
            let result = read.uncached_stats(hash, header.protocol, || Ok(()));
            if r["result"]["kind"] == "ok" {
                let got = result.unwrap();
                let want = &r["result"]["value"];
                assert_eq!(
                    got.minimum,
                    (!want["minimum"].is_null()).then(|| number(&want["minimum"])),
                    "{r}"
                );
                assert_eq!(got.used, number(&want["used"]), "{r}");
                assert_eq!(got.total, number(&want["total"]), "{r}");
            } else {
                assert!(
                    matches!(
                        result,
                        Err(Error::Accounting(
                            ae_execution::transactions::accounting::Error::MissingCall { .. }
                        ))
                    ),
                    "{r}: {result:?}"
                );
            }
        }
    }
    for r in f["missing"].as_array().unwrap() {
        let sample = f["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["name"] == "empty" && s["protocol"] == r["protocol"])
            .unwrap();
        let (mut db, h) = database(sample);
        if r["block"] == false {
            db.remove(&repo::key(repo::BLOCKS, h.0));
        }
        if r["state"] == false {
            db.remove(&repo::key(repo::RECORDS, h.0));
        }
        assert_eq!(r["result"]["value"]["error"], "block_not_found");
        assert!(
            matches!(reader(db).stats(h,r["protocol"].as_u64().unwrap()as u32),Err(Error::MissingState(v))if v==h)
        );
    }
}
#[test]
fn observations_keep_snapshot_state_errors_and_work_limits_distinct() {
    let f = fixture();
    let r = f["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == 6 && r["name"] == "call" && r["vm_gas"] == 7)
        .unwrap();
    let (mut db, h) = database(r);
    let stable = reader(db.clone());
    let expected = stable.uncached_stats(h, 6, || Ok(())).unwrap();
    db.remove(&repo::key(repo::RECORDS, h.0));
    assert_eq!(stable.uncached_stats(h, 6, || Ok(())).unwrap(), expected);
    assert!(matches!(
        reader(db).stats(h, 6),
        Err(Error::MissingState(_))
    ));
    for limits in [
        point::Limits {
            read_bytes: 64,
            ..Default::default()
        },
        point::Limits {
            reads: 1,
            ..Default::default()
        },
        point::Limits {
            node_bytes: 0,
            ..Default::default()
        },
    ] {
        let (db, h) = database(r);
        let v = View::load(db.clone()).unwrap();
        db.copied.set(0);
        let result = Reader::new(
            &v,
            6_000_000.into(),
            ReadLimits {
                reads: limits,
                ..Default::default()
            },
        )
        .and_then(|r| r.uncached_stats(h, 6, || Ok(())));
        assert!(matches!(result, Err(Error::ResourceLimit)), "{result:?}");
        assert!(db.copied.get() <= limits.read_bytes);
    }
    let (mut db, h) = database(r);
    let record = Record::decode(&db.data[&repo::key(repo::RECORDS, h.0)]).unwrap();
    let node = repo::key(repo::NODES, record.roots.state.calls.unwrap().0);
    db.fault = Some(node.clone());
    assert!(matches!(
        reader(db.clone()).stats(h, 6),
        Err(Error::Backend(repo::Error::Trie(ae_trie::Error::Backend(
            _
        ))))
    ));
    db.fault = None;
    db.remove(&node);
    assert!(matches!(
        reader(db).stats(h, 6),
        Err(Error::Backend(repo::Error::Trie(
            ae_trie::Error::MissingNode(_)
        )))
    ));
    let (db, h) = database(r);
    assert!(matches!(
        reader(db.clone()).uncached_stats(h, 6, || Err(Error::Backend(repo::Error::PolicyClosed))),
        Err(Error::Backend(repo::Error::PolicyClosed))
    ));
    assert!(matches!(reader(db).stats(h, 5), Err(Error::InvalidObject)));

    let (mut db, h) = database(r);
    let key = repo::key(repo::RECORDS, h.0);
    let mut record = Record::decode(&db.data[&key]).unwrap();
    record.roots.state.calls = None;
    db.put(key, record.encode());
    assert!(matches!(
        reader(db).stats(h, 6),
        Err(Error::Backend(repo::Error::Corrupt(
            "gas observation state binding"
        )))
    ));
}

#[test]
fn empty_and_pre_ceres_observations_still_load_all_state_roots() {
    let f = fixture();
    for protocol in [1, 6] {
        for name in ["empty", "spend"] {
            let r = f["rows"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["protocol"] == protocol && r["name"] == name)
                .unwrap();
            let (mut db, h) = database(r);
            let record = Record::decode(&db.data[&repo::key(repo::RECORDS, h.0)]).unwrap();
            let node = repo::key(repo::NODES, record.roots.state.calls.unwrap().0);
            assert!(reader(db.clone()).stats(h, protocol).is_ok());
            db.remove(&node);
            assert!(matches!(
                reader(db).stats(h, protocol),
                Err(Error::Backend(repo::Error::Trie(
                    ae_trie::Error::MissingNode(_)
                )))
            ));
        }
    }
}

#[test]
fn streamed_blocks_validate_transactions_before_reading_missing_state() {
    let f = fixture();
    let r = f["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == 6 && r["name"] == "mixed")
        .unwrap();
    for modern in [false, true] {
        for corrupt in [false, true] {
            let (mut db, h) = database(r);
            let block = View::load(db.clone()).unwrap().block(h).unwrap().unwrap();
            let Block::Micro(b) = block else {
                unreachable!()
            };
            let last = repo::key(repo::TRANSACTIONS, b.transactions.last().unwrap().hash().0);
            if modern {
                modernize(&mut db, h);
            }
            db.remove(&repo::key(repo::RECORDS, h.0));
            if corrupt {
                db.put(last.clone(), vec![0]);
            } else {
                db.remove(&last);
            }
            let read = reader(db.clone());
            db.trace.borrow_mut().clear();
            assert!(matches!(
                read.uncached_stats(h, 6, || Ok(())),
                Err(Error::Backend(_))
            ));
            let trace = db.trace.borrow();
            assert!(trace.contains(&last));
            assert!(!trace.contains(&repo::key(repo::RECORDS, h.0)));
        }
        let (mut db, h) = database(r);
        if modern {
            modernize(&mut db, h);
        }
        let read = reader(db.clone());
        db.trace.borrow_mut().clear();
        assert!(matches!(
            read.uncached_stats(h, 6, || Err(Error::Backend(repo::Error::PolicyClosed))),
            Err(Error::Backend(repo::Error::PolicyClosed))
        ));
        assert!(db.trace.borrow().is_empty());
    }
}

// Repeated authenticated transaction references make a small wire block with
// substantial per-transaction stored metadata. This is a query-record fixture,
// not a claim that the artificial duplicate-transaction block is admissible.
fn metadata_block(modern: bool) -> (Memory, Hash256, Vec<u8>, usize) {
    let f = fixture();
    let r = f["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == 5 && r["name"] == "call")
        .unwrap();
    let (mut db, h) = database(r);
    let Block::Micro(mut b) = View::load(db.clone()).unwrap().block(h).unwrap().unwrap() else {
        unreachable!()
    };
    let signed = b.transactions[0].clone();
    let ae_tx::TransactionLeaf::Contract(c) = signed.transaction().leaf() else {
        unreachable!()
    };
    let mut c = c.clone();
    c.set_call_context([9; 32], vec![ae_types::Unsigned::from(u64::MAX); 8192])
        .unwrap();
    let stored = SignedTransaction::new(
        Transaction::new(ae_tx::TransactionLeaf::Contract(c)),
        vec![],
    );
    assert_eq!(stored.encode(), signed.encode());
    let record = ae_tx::storage::encode(&stored, Default::default()).unwrap();
    assert!(record.len() > signed.encode().len() * 100);
    let txkey = repo::key(repo::TRANSACTIONS, signed.hash().0);
    db.put(txkey.clone(), record.clone());
    b.transactions = vec![signed; 32];
    db.put(
        repo::key(repo::BLOCKS, h.0),
        Block::Micro(b).encode().unwrap(),
    );
    if modern {
        modernize(&mut db, h);
    }
    (db, h, txkey, record.len())
}

#[test]
fn streamed_metadata_accounting_uses_two_metered_passes_and_can_cancel_between_transactions() {
    let f = fixture();
    let native = &f["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == 5 && r["name"] == "call")
        .unwrap()["result"]["value"];
    for modern in [false, true] {
        let (db, h, txkey, record_bytes) = metadata_block(modern);
        let view = View::load(db.clone()).unwrap();
        let block = view
            .stream_block(h, &mut || Ok::<_, repo::Error>(()))
            .unwrap()
            .unwrap();
        let mut seen = 0;
        block
            .for_each(&mut || Ok::<_, repo::Error>(()), |signed| {
                let ae_tx::TransactionLeaf::Contract(c) = signed.transaction().leaf() else {
                    panic!("contract metadata lost")
                };
                assert_eq!(c.call_origin(), [9; 32]);
                assert_eq!(c.call_stack().len(), 8192);
                seen += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, 32);
        let read = reader(db.clone());
        db.trace.borrow_mut().clear();
        db.copied.set(0);
        let stats = read.uncached_stats(h, 5, || Ok(())).unwrap();
        assert_eq!(stats.minimum, Some(number(&native["minimum"])));
        assert_eq!(stats.used, number(&native["used"]) * BigInt::from(32));
        assert_eq!(stats.total, number(&native["total"]));
        let trace = db.trace.borrow();
        let state = trace
            .iter()
            .position(|k| *k == repo::key(repo::RECORDS, h.0))
            .unwrap();
        assert_eq!(trace[..state].iter().filter(|k| **k == txkey).count(), 32);
        assert_eq!(trace[state..].iter().filter(|k| **k == txkey).count(), 32);
        assert!(db.copied.get() >= 64 * record_bytes);
        drop(trace);
        db.trace.borrow_mut().clear();
        assert!(matches!(
            read.uncached_stats(h, 5, || {
                let trace = db.trace.borrow();
                if trace.iter().filter(|k| **k == txkey).count() >= 35 {
                    Err(Error::Backend(repo::Error::PolicyClosed))
                } else {
                    Ok(())
                }
            }),
            Err(Error::Backend(repo::Error::PolicyClosed))
        ));
        assert_eq!(
            db.trace.borrow().iter().filter(|k| **k == txkey).count(),
            35
        );

        // Both passes consume the same aggregate read credit. The first pass
        // and root reads fit; accounting cannot get a fresh unmetered budget.
        let first_pass = db.data[repo::IDENTITY].len()
            + db.data[repo::STATUS].len()
            + db.data[&repo::key(repo::BLOCKS, h.0)].len()
            + 32 * record_bytes
            + db.data[&repo::key(repo::RECORDS, h.0)].len();
        let limit = first_pass + (32 << 10);
        db.trace.borrow_mut().clear();
        db.copied.set(0);
        let limited = Reader::new(
            &view,
            6_000_000.into(),
            ReadLimits {
                reads: point::Limits {
                    read_bytes: limit,
                    ..ReadLimits::default().reads
                },
                ..Default::default()
            },
        )
        .unwrap();
        assert!(matches!(
            limited.uncached_stats(h, 5, || Ok(())),
            Err(Error::ResourceLimit)
        ));
        assert!(db.trace.borrow().contains(&repo::key(repo::RECORDS, h.0)));
        assert!(db.copied.get() <= limit);
        let config = repo::ReadConfig {
            max_transaction_record_bytes: record_bytes - 1,
            ..Default::default()
        };
        let view = View::load_with_config(db.clone(), config).unwrap();
        db.copied.set(0);
        let limited = Reader::new(&view, 6_000_000.into(), Default::default()).unwrap();
        assert!(matches!(
            limited.uncached_stats(h, 5, || Ok(())),
            Err(Error::ResourceLimit)
        ));
        assert!(db.copied.get() < record_bytes);
    }
}

#[test]
fn streamed_modern_records_enforce_canonical_transport_size_and_identity() {
    let (mut db, h, _, _) = metadata_block(true);
    let view = View::load(db.clone()).unwrap();
    let wire = view.block(h).unwrap().unwrap().encode().unwrap();
    assert!(wire.len() > db.data[&repo::key(repo::BLOCKS, h.0)].len());
    for limit in [wire.len() - 1, wire.len()] {
        let view = View::load_with_config(
            db.clone(),
            repo::ReadConfig {
                max_block_bytes: limit,
                ..Default::default()
            },
        )
        .unwrap();
        let full = view.block(h);
        let streamed = view.stream_block(h, &mut || Ok::<_, repo::Error>(()));
        assert_eq!(full.is_ok(), streamed.is_ok());
        assert_eq!(streamed.is_ok(), limit == wire.len());
    }
    let wrong = Hash256([7; 32]);
    db.put(
        repo::key(repo::BLOCKS, wrong.0),
        db.data[&repo::key(repo::BLOCKS, h.0)].clone(),
    );
    let view = View::load(db.clone()).unwrap();
    assert!(matches!(
        view.stream_block(wrong, &mut || Ok::<_, repo::Error>(())),
        Err(repo::Error::Corrupt("block identity"))
    ));
    let mut bytes = db.data[&repo::key(repo::BLOCKS, h.0)].clone();
    bytes[b"\0aeblock".len()] = 2;
    db.put(repo::key(repo::BLOCKS, h.0), bytes);
    let view = View::load(db).unwrap();
    assert!(matches!(
        view.stream_block(h, &mut || Ok::<_, repo::Error>(())),
        Err(repo::Error::Corrupt("block record version"))
    ));
}

#[test]
fn streamed_proofs_and_noncanonical_envelopes_are_validated_before_state() {
    use ae_trie::Value as V;
    let f = fixture();
    let r = f["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == 6 && r["name"] == "empty")
        .unwrap();
    for modern in [false, true] {
        let (mut db, h) = database(r);
        let Block::Micro(mut block) = View::load(db.clone()).unwrap().block(h).unwrap().unwrap()
        else {
            unreachable!()
        };
        let mut second = block.header.clone();
        second.time += 1;
        block.proof = Some(Box::new(crate::fraud::Proof {
            first: StoredHeader::Micro(block.header.clone()),
            second: StoredHeader::Micro(second),
            public_key: vec![1; 32],
        }));
        let raw_header = StoredHeader::Micro(block.header.clone()).encode().unwrap();
        db.put(
            repo::key(repo::BLOCKS, h.0),
            Block::Micro(block).encode().unwrap(),
        );
        if modern {
            modernize(&mut db, h);
        }
        assert!(reader(db.clone()).stats(h, 6).is_ok());
        let bytes = &db.data[&repo::key(repo::BLOCKS, h.0)];
        let prefix = if modern {
            b"\0aeblock\x01".to_vec()
        } else {
            raw_header
        };
        let V::List(mut fields) = V::from_rlp(&rlp::decode(&bytes[prefix.len()..]).unwrap()) else {
            unreachable!()
        };
        if modern {
            fields[2] = V::Bytes(vec![0xc0]);
        } else {
            fields[3] = V::List(vec![V::Bytes(vec![])]);
        }
        let mut corrupt = prefix;
        corrupt.extend(V::List(fields).encode());
        db.put(repo::key(repo::BLOCKS, h.0), corrupt);
        db.remove(&repo::key(repo::RECORDS, h.0));
        let mut read = reader(db.clone());
        db.trace.borrow_mut().clear();
        assert!(matches!(
            read.stats(h, 6),
            Err(Error::Backend(repo::Error::Corrupt(_)))
        ));
        assert!(!db.trace.borrow().contains(&repo::key(repo::RECORDS, h.0)));
    }
    // Native RLP accepts a long-form length for this short list. Database
    // canonical round-trip validation must reject it, as View.block does.
    let (mut db, h) = database(r);
    let bytes = &db.data[&repo::key(repo::BLOCKS, h.0)];
    let (header, body) =
        StoredHeader::decode_partial(bytes, ae_codec::headers::HeaderMode::ProofOfWork).unwrap();
    assert_eq!(body, [0xc4, 101, 6, 0xc0, 0xc0]);
    let mut noncanonical = header.encode().unwrap();
    noncanonical.extend([0xf8, 4, 101, 6, 0xc0, 0xc0]);
    db.put(repo::key(repo::BLOCKS, h.0), noncanonical);
    let view = View::load(db).unwrap();
    assert!(matches!(view.block(h), Err(repo::Error::Corrupt(_))));
    assert!(matches!(
        view.stream_block(h, &mut || Ok::<_, repo::Error>(())),
        Err(repo::Error::Corrupt(_))
    ));
}
