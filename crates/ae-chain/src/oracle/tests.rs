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
        "../../tests/fixtures/erlang_http_oracles.json"
    ))
    .unwrap()
}
fn database(row: &Json) -> (Memory, Hash256) {
    let mut db = Memory::default();
    let roots = StateRoots {
        oracles: db.root(&row["oracles"]),
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
            height: 1000,
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
fn key(n: u8) -> [u8; 32] {
    let mut key = [0; 32];
    key[31] = n;
    key
}
fn kind(row: &Json) -> Kind {
    let input = &row["input"];
    match input["operation"].as_str().unwrap() {
        "GetOracleByPubkey" => Kind::Metadata,
        "GetOracleQueryByPubkeyAndQueryId" => {
            Kind::Query(key(input["query_id"].as_u64().unwrap_or(1) as u8))
        }
        _ => Kind::Queries {
            after: input["from"].as_u64().map(|n| key(n as u8)),
            filter: match input["type"].as_str() {
                Some("open") => QueryFilter::Open,
                Some("closed") => QueryFilter::Closed,
                _ => QueryFilter::All,
            },
            limit: input["limit"].as_u64().unwrap_or(20) as usize,
        },
    }
}
fn number(n: &ae_types::Unsigned) -> Json {
    serde_json::from_str(&num_bigint::BigUint::from_bytes_be(n.as_bytes()).to_str_radix(10))
        .unwrap()
}
fn query_body(q: &Query) -> Json {
    serde_json::json!({
        "id": api::encode(ApiType::OracleQueryId, &q.id),
        "sender_id": api::encode_id(q.sender),
        "sender_nonce": number(&q.sender_nonce),
        "oracle_id": api::encode_id(q.oracle),
        "query": api::encode(ApiType::OracleQuery, &q.query),
        "response": api::encode(ApiType::OracleResponse, q.response.as_deref().unwrap_or_default()),
        "ttl": number(&q.expires_by),
        "response_ttl": {"type":"delta", "value":number(&q.response_ttl)},
        "fee": number(&q.fee),
    })
}
fn serialized(row: &Json, key: &[u8]) -> Vec<u8> {
    let pair = row["oracles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|pair| hex::decode(pair[0].as_str().unwrap()).unwrap() == key)
        .unwrap();
    hex::decode(pair[1].as_str().unwrap()).unwrap()
}
#[test]
fn native_oracle_reads_match_current_state_queries_pagination_filters_and_arbitrary_integers() {
    let f = fixture();
    let mut count = 0;
    for row in f["dispatcher"].as_array().unwrap() {
        let input = &row["input"];
        if (input["pubkey"].is_string() && input["pubkey"] != "valid")
            || matches!(input["scenario"].as_str().unwrap(), "overload" | "unstable")
            || input["from"].is_string()
            || input["query_id"].is_string()
        {
            continue;
        }
        count += 1;
        let k = key(input["pubkey"].as_u64().unwrap_or(1) as u8);
        let kind = kind(row);
        let (db, _) = database(row);
        let view = View::load(db).unwrap();
        let result = Reader::new(&view, Default::default())
            .unwrap()
            .query(k, kind);
        let status = match &result {
            Ok(_) => 200,
            Err(Error::NoStateTrees) if matches!(kind, Kind::Queries { .. }) => 200,
            Err(Error::NotPresent | Error::NoStateTrees) => 404,
            _ => 500,
        };
        assert_eq!(status, row["code"].as_u64().unwrap(), "{input} {result:?}");
        match result {
            Ok(Reply::Metadata(o)) => {
                let b = &row["body"];
                assert_eq!(api::encode(ApiType::OraclePubkey, &o.public_key), b["id"]);
                assert_eq!(number(&o.query_fee), b["query_fee"]);
                assert_eq!(number(&o.expires_by), b["ttl"]);
                assert_eq!(number(&o.abi_version), b["abi_version"]);
                if o.abi_version == 1.into() || o.abi_version == 3.into() {
                    assert_eq!(
                        api::encode(ApiType::ContractBytearray, &o.query_format),
                        b["query_format"]
                    );
                    assert_eq!(
                        api::encode(ApiType::ContractBytearray, &o.response_format),
                        b["response_format"]
                    );
                } else if input["format"] != "binary" {
                    assert_eq!(
                        std::str::from_utf8(&o.query_format).unwrap(),
                        b["query_format"]
                    );
                    assert_eq!(
                        std::str::from_utf8(&o.response_format).unwrap(),
                        b["response_format"]
                    );
                } else {
                    assert_eq!(o.query_format, [0, 255, 192, 175, 226, 130]);
                    assert_eq!(o.response_format, o.query_format);
                }
                assert_eq!(o.serialize().unwrap(), serialized(row, &k));
            }
            Ok(Reply::Query(q)) => {
                assert_eq!(query_body(&q), row["body"], "{input}");
                assert_eq!(q.serialize().unwrap(), serialized(row, &query_key(k, q.id)));
            }
            Ok(Reply::Queries(queries)) => {
                assert_eq!(
                    serde_json::json!({"oracle_queries":queries.iter().map(query_body).collect::<Vec<_>>()}),
                    row["body"],
                    "{input}"
                );
                for q in queries {
                    assert_eq!(
                        q.serialize().unwrap(),
                        serialized(row, &query_key(q.oracle.value, q.id))
                    );
                }
            }
            Err(Error::NoStateTrees) if matches!(kind, Kind::Queries { .. }) => {
                assert_eq!(row["body"], serde_json::json!({"oracle_queries":[]}));
            }
            Err(Error::NoStateTrees) => assert_eq!(row["body"]["error_code"], "no_state_trees"),
            Err(Error::NotPresent) => assert_eq!(row["body"]["error_code"], "not_found"),
            Err(_) => assert_eq!(status, 500),
        }
    }
    assert_eq!(count, 520);
}
fn all(limit: usize) -> Kind {
    Kind::Queries {
        after: None,
        filter: QueryFilter::All,
        limit,
    }
}
#[test]
fn oracle_queries_bound_copying_decoding_scans_and_complete_retained_results() {
    let f = fixture();
    let (db, _) = database(&f["dispatcher"][0]);
    let view = View::load(db.clone()).unwrap();
    for kind in [Kind::Metadata, Kind::Query(key(1)), all(20)] {
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
                    node_bytes: 1,
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
            Limits {
                reads: point::Limits {
                    rlp: rlp::DecodeLimits {
                        max_items: 4,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ..Default::default()
            },
            Limits {
                retained_bytes: 1,
                ..Default::default()
            },
        ] {
            db.copied.set(0);
            assert_eq!(
                Reader::new(&view, limits).and_then(|r| r.query(key(1), kind)),
                Err(Error::ResourceLimit),
                "{kind:?} {limits:?}"
            );
            assert!(db.copied.get() <= limits.reads.read_bytes);
        }
    }
    let row = &f["dispatcher"][0];
    let first = serialized(row, &query_key(key(1), key(1))).len() + std::mem::size_of::<Query>();
    let limits = Limits {
        retained_bytes: first,
        ..Default::default()
    };
    assert!(
        Reader::new(&view, limits)
            .unwrap()
            .query(key(1), all(1))
            .is_ok()
    );
    assert_eq!(
        Reader::new(&view, limits).unwrap().query(key(1), all(20)),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        Reader::new(
            &view,
            Limits {
                scanned_items: 1,
                ..Default::default()
            }
        )
        .unwrap()
        .query(
            key(1),
            Kind::Queries {
                filter: QueryFilter::Closed,
                after: None,
                limit: 1
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        Reader::new(
            &view,
            Limits {
                returned_items: 1,
                ..Default::default()
            }
        )
        .unwrap()
        .query(key(1), all(2)),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        Reader::new(
            &view,
            Limits {
                scanned_items: 0,
                retained_bytes: 0,
                ..Default::default()
            }
        )
        .unwrap()
        .query(key(1), all(0)),
        Ok(Reply::Queries(vec![]))
    );
}
#[test]
fn oracle_reads_keep_immutable_snapshots_authenticate_roots_and_ignore_expiry_cache() {
    let f = fixture();
    let row = &f["dispatcher"][0];
    let (mut db, top) = database(row);
    let record_key = repo::key(repo::RECORDS, top.0);
    let mut record = Record::decode(&db.data[&record_key]).unwrap();
    let root = record.roots.state.oracles.unwrap();
    let absent_cache = Hash256([99; 32]);
    record.roots.oracles = Some(absent_cache);
    db.put(record_key.clone(), record.encode());
    db.fault = Some(repo::key(repo::NODES, absent_cache.0));
    let old = View::load(db.clone()).unwrap();
    let before = Reader::new(&old, Default::default())
        .unwrap()
        .query(key(1), all(20))
        .unwrap();
    assert!(!db.trace.borrow().contains(db.fault.as_ref().unwrap()));
    db.remove(&record_key);
    let new = View::load(db.clone()).unwrap();
    assert_eq!(
        Reader::new(&new, Default::default())
            .unwrap()
            .query(key(1), all(20)),
        Err(Error::NoStateTrees)
    );
    assert_eq!(
        Reader::new(&old, Default::default())
            .unwrap()
            .query(key(1), all(20))
            .unwrap(),
        before
    );
    record.roots.state.oracles = None;
    db.put(record_key, record.encode());
    let view = View::load(db).unwrap();
    assert_eq!(
        Reader::new(&view, Default::default())
            .unwrap()
            .query(key(1), Kind::Metadata),
        Err(Error::Backend(repo::Error::Corrupt("oracle state binding")))
    );
    let (mut db, _) = database(row);
    db.remove(&repo::key(repo::NODES, root.0));
    let view = View::load(db).unwrap();
    assert!(matches!(
        Reader::new(&view, Default::default())
            .unwrap()
            .query(key(1), all(20)),
        Err(Error::Backend(_))
    ));
}
#[test]
fn oracle_objects_are_checked_before_decoding_and_corruption_is_not_absence() {
    let f = fixture();
    for operation in [Kind::Metadata, Kind::Query(key(1)), all(20)] {
        let mut row = f["dispatcher"][0].clone();
        let bad_key = if operation == Kind::Metadata {
            key(1).to_vec()
        } else {
            query_key(key(1), key(1)).to_vec()
        };
        let pair = row["oracles"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|p| hex::decode(p[0].as_str().unwrap()).unwrap() == bad_key)
            .unwrap();
        pair[1] = Json::String("C0".into());
        let roots = StateRoots {
            oracles: Memory::default().root(&row["oracles"]),
            ..Default::default()
        };
        row["state_hash"] = Json::String(hex::encode(roots.hash().0));
        let (db, _) = database(&row);
        let view = View::load(db).unwrap();
        assert_eq!(
            Reader::new(&view, Default::default())
                .unwrap()
                .query(key(1), operation),
            Err(Error::InvalidObject)
        );
    }
}
