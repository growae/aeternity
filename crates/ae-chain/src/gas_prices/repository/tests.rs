use super::*;
use crate::{
    block::MicroBlock,
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
#[test]
fn original_micro_statistics_use_historical_pricing_and_resulting_call_state() {
    let f = fixture();
    assert_eq!(f["rows"].as_array().unwrap().len(), 224);
    for r in f["rows"].as_array().unwrap() {
        let (db, hash) = database(r);
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
