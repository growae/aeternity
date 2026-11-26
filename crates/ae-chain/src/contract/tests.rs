use super::*;
use crate::{
    block::Block,
    repository::{Identity, Roots, Status},
};
use ae_codec::{
    api::{self, ApiType},
    headers::{CommonHeader, KeyHeader},
};
use ae_protocol::NetworkProfile;
use ae_state::StateRoots;
use ae_trie::{MemoryStore, NodeReader, Trie};
use ae_types::Hash256;
use serde_json::Value as Json;
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
    fn root(&mut self, entries: &Json) -> Option<Hash256> {
        let mut tree = Trie::new(MemoryStore::default());
        for pair in entries.as_array().unwrap() {
            tree.put(
                &Nibbles::from_bytes(&hex::decode(pair[0].as_str().unwrap()).unwrap()),
                Value::Bytes(hex::decode(pair[1].as_str().unwrap()).unwrap()),
            )
            .unwrap();
        }
        for (hash, bytes) in tree.store().nodes() {
            self.put(repo::key(repo::NODES, hash.0), bytes.clone());
        }
        tree.root_hash()
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
    fn get_bounded(&self, key: &[u8], bytes: usize) -> Result<Option<Vec<u8>>, repo::Error> {
        self.trace.borrow_mut().push(key.to_vec());
        if self.fault.as_deref() == Some(key) {
            return Err(repo::Error::Backend("injected".into()));
        }
        self.data
            .get(key)
            .map(|value| {
                if value.len() > bytes {
                    return Err(repo::Error::ResourceLimit);
                }
                self.copied.set(self.copied.get() + value.len());
                Ok(value.clone())
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
fn fixture() -> Json {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_http_contracts.json"
    ))
    .unwrap()
}
fn database(row: &Json) -> (Memory, Hash256) {
    let mut db = Memory::default();
    let roots = StateRoots {
        accounts: db.root(&row["accounts"]),
        contracts: db.root(&row["contracts"]),
        ..Default::default()
    };
    assert_eq!(
        roots.hash().0.to_vec(),
        hex::decode(row["state_hash"].as_str().unwrap()).unwrap()
    );
    let record = Record {
        roots: Roots {
            state: roots,
            names: None,
            oracles: None,
        },
        difficulty: 1.into(),
        fees: 0.into(),
        fraud: false,
        fork: Hash256::ZERO,
    };
    let block = Block::Key(KeyHeader {
        common: CommonHeader {
            version: 1,
            height: 0,
            previous_hash: [0; 32],
            previous_key_hash: [0; 32],
            state_hash: record.roots.state.hash().0,
        },
        miner: [0; 32],
        beneficiary: [0; 32],
        target: 0x2100ffff,
        seal: [0; 42],
        nonce: 0,
        time: 0,
        info: None,
        hole: false,
        end_of_epoch: false,
    });
    let hash = block.hash().unwrap();
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
    match row["input"]["scenario"].as_str().unwrap() {
        "no_top" => db.put(
            repo::STATUS.to_vec(),
            Status {
                revision: 0,
                tip: None,
            }
            .encode(),
        ),
        "no_state" => db.remove(&repo::key(repo::RECORDS, hash.0)),
        "storage_error" => db.fault = Some(repo::key(repo::RECORDS, hash.0)),
        _ => (),
    }
    (db, hash)
}
fn kind(row: &Json) -> Kind {
    match row["input"]["operation"].as_str().unwrap() {
        "GetContract" => Kind::Metadata,
        "GetContractCode" => Kind::Code,
        _ => Kind::Proof,
    }
}
fn key() -> [u8; 32] {
    let mut key = [0; 32];
    key[31] = 1;
    key
}
#[test]
fn native_contract_state_queries_match_all_valid_identifier_cases_and_exact_proof_bytes() {
    let f = fixture();
    let mut count = 0;
    for row in f["dispatcher"].as_array().unwrap() {
        if row["input"]["pubkey"] != "valid"
            || matches!(
                row["input"]["scenario"].as_str().unwrap(),
                "overload" | "unstable"
            )
        {
            continue;
        }
        count += 1;
        let (db, _) = database(row);
        let view = View::load(db).unwrap();
        let result = Reader::new(&view, Default::default())
            .unwrap()
            .query(key(), kind(row));
        let code = match &result {
            Ok(_) => 200,
            Err(Error::NotPresent | Error::AccountNotPresent) => 404,
            Err(Error::NoStateTrees) if kind(row) != Kind::Proof => 404,
            _ => 500,
        };
        assert_eq!(
            code,
            row["code"].as_u64().unwrap(),
            "{} {result:?}",
            row["input"]
        );
        match result {
            Ok(Reply::Metadata(c)) => {
                let b = &row["body"];
                assert_eq!(api::encode(ApiType::ContractPubkey, &c.public_key), b["id"]);
                assert_eq!(
                    api::encode(ApiType::AccountPubkey, &c.owner.0),
                    b["owner_id"]
                );
                assert_eq!(u64::from(c.version.vm), b["vm_version"].as_u64().unwrap());
                assert_eq!(u64::from(c.version.abi), b["abi_version"].as_u64().unwrap());
                assert_eq!(c.active, b["active"].as_bool().unwrap());
                assert_eq!(
                    c.deposit,
                    ae_types::Unsigned::from_decimal(&b["deposit"].to_string()).unwrap()
                );
                assert_eq!(
                    c.referrers
                        .iter()
                        .map(|k| api::encode(ApiType::ContractPubkey, k))
                        .collect::<Vec<_>>(),
                    b["referrer_ids"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|v| v.as_str().unwrap().to_owned())
                        .collect::<Vec<_>>()
                );
            }
            Ok(Reply::Code(bytes)) => assert_eq!(
                bytes,
                api::decode_as(
                    ApiType::ContractBytearray,
                    row["body"]["bytecode"].as_str().unwrap()
                )
                .unwrap()
            ),
            Ok(Reply::Proof(bytes)) => assert_eq!(
                bytes,
                api::decode_as(ApiType::Poi, row["body"]["poi"].as_str().unwrap()).unwrap(),
                "{}",
                row["input"]
            ),
            Err(_) => (),
        }
    }
    assert_eq!(count, 165);
}
#[test]
fn contract_queries_bound_reads_proofs_and_decode_before_copy_without_partial_results() {
    let f = fixture();
    let (db, _) = database(&f["dispatcher"][0]);
    let view = View::load(db.clone()).unwrap();
    for kind in [Kind::Metadata, Kind::Code, Kind::Proof] {
        for limits in [
            Limits {
                reads: point::Limits {
                    read_bytes: 64,
                    ..Default::default()
                },
                ..Default::default()
            },
            Limits {
                reads: point::Limits {
                    reads: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
            Limits {
                reads: point::Limits {
                    value_bytes: 16,
                    ..Default::default()
                },
                ..Default::default()
            },
        ] {
            db.copied.set(0);
            db.trace.borrow_mut().clear();
            let result = Reader::new(&view, limits).and_then(|r| r.query(key(), kind));
            assert_eq!(result, Err(Error::ResourceLimit));
            assert!(db.copied.get() <= limits.reads.read_bytes);
        }
    }
    for proof in [
        transport::Limits {
            max_work_bytes: 8,
            ..Default::default()
        },
        transport::Limits {
            max_entries: 1,
            ..Default::default()
        },
    ] {
        assert_eq!(
            Reader::new(
                &view,
                Limits {
                    proof,
                    ..Default::default()
                }
            )
            .unwrap()
            .query(key(), Kind::Proof),
            Err(Error::ResourceLimit)
        );
    }
    assert!(
        Reader::new(&view, Default::default())
            .unwrap()
            .query(key(), Kind::Proof)
            .is_ok()
    );
}
#[test]
fn contract_queries_preserve_snapshot_and_distinguish_missing_records_nodes_and_corruption() {
    let f = fixture();
    let (mut db, top) = database(&f["dispatcher"][0]);
    let old = View::load(db.clone()).unwrap();
    let before = Reader::new(&old, Default::default())
        .unwrap()
        .query(key(), Kind::Proof)
        .unwrap();
    db.remove(&repo::key(repo::RECORDS, top.0));
    let new = View::load(db.clone()).unwrap();
    assert_eq!(
        Reader::new(&new, Default::default())
            .unwrap()
            .query(key(), Kind::Proof),
        Err(Error::NoStateTrees)
    );
    assert_eq!(
        Reader::new(&old, Default::default())
            .unwrap()
            .query(key(), Kind::Proof)
            .unwrap(),
        before
    );
    let (mut db, top) = database(&f["dispatcher"][0]);
    let k = repo::key(repo::RECORDS, top.0);
    let mut record = Record::decode(&db.data[&k]).unwrap();
    let contract_root = record.roots.state.contracts.unwrap();
    record.roots.state.accounts = None;
    db.put(k, record.encode());
    let view = View::load(db).unwrap();
    assert!(matches!(
        Reader::new(&view, Default::default())
            .unwrap()
            .query(key(), Kind::Metadata),
        Err(Error::Backend(repo::Error::Corrupt(
            "contract state binding"
        )))
    ));
    let (mut db, _) = database(&f["dispatcher"][0]);
    db.remove(&repo::key(repo::NODES, contract_root.0));
    let view = View::load(db).unwrap();
    assert!(matches!(
        Reader::new(&view, Default::default())
            .unwrap()
            .query(key(), Kind::Proof),
        Err(Error::Backend(_))
    ));
}
