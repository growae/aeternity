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
    serde_json::from_str(include_str!("../../tests/fixtures/erlang_http_names.json")).unwrap()
}
fn database(row: &Json) -> (Memory, Hash256) {
    let mut db = Memory::default();
    let roots = StateRoots {
        names: db.root(&row["names"]),
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
fn kind(row: &Json) -> Kind {
    match row["input"]["operation"].as_str().unwrap() {
        "GetNameEntryByName" => Kind::Name,
        "GetNameEntryByNameHash" => Kind::Hash,
        _ => Kind::Auction,
    }
}
fn input(row: &Json) -> Option<Vec<u8>> {
    let field = if kind(row) == Kind::Hash {
        "name_hash"
    } else {
        "name"
    };
    let bytes = hex::decode(row["params_hex"][field].as_str().unwrap()).unwrap();
    if kind(row) == Kind::Hash {
        api::decode_as(ApiType::Name, std::str::from_utf8(&bytes).unwrap()).ok()
    } else {
        Some(bytes)
    }
}
fn number(n: &ae_types::Unsigned) -> Json {
    serde_json::from_str(&num_bigint::BigUint::from_bytes_be(n.as_bytes()).to_str_radix(10))
        .unwrap()
}
fn error(e: &Error) -> (u64, Option<&'static str>) {
    match e {
        Error::NotPresent => (404, Some("name_not_found")),
        Error::Revoked => (404, Some("name_revoked")),
        Error::NoStateTrees => (400, Some("no_state_trees")),
        Error::Normalization(e) => match e {
            idna::Error::NoRegistrar => (400, Some("no_registrar")),
            idna::Error::RegistrarUnknown => (400, Some("registrar_unknown")),
            idna::Error::MultipleNamespaces => (400, Some("multiple_namespaces")),
            idna::Error::NoLabelInRegistrar => (400, Some("no_label_in_registrar")),
            idna::Error::InvalidCodepoint(_) | idna::Error::BadLabel(_) => {
                (400, Some("invalid_name"))
            }
            idna::Error::ReferenceException { .. } => (500, None),
        },
        Error::ResourceLimit => (503, None),
        _ => (500, None),
    }
}
fn serialized(row: &Json, key: &[u8]) -> Vec<u8> {
    let pair = row["names"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| hex::decode(p[0].as_str().unwrap()).unwrap() == key)
        .unwrap();
    hex::decode(pair[1].as_str().unwrap()).unwrap()
}
#[test]
fn native_name_and_auction_reads_match_normalization_state_order_pointers_and_hash_widths() {
    let f = fixture();
    let mut count = 0;
    for row in f["dispatcher"].as_array().unwrap() {
        if matches!(
            row["input"]["scenario"].as_str().unwrap(),
            "unstable" | "overload"
        ) {
            continue;
        }
        let Some(input) = input(row) else {
            continue;
        };
        count += 1;
        let (db, _) = database(row);
        let view = View::load(db).unwrap();
        let reply = Reader::new(&view, Default::default())
            .unwrap()
            .query(&input, kind(row));
        let code = reply.as_ref().map_or_else(|e| error(e).0, |_| 200);
        assert_eq!(
            code,
            row["code"].as_u64().unwrap(),
            "{} {reply:?}",
            row["input"]
        );
        match reply {
            Ok(Reply::Name(n)) => {
                let b = &row["body"];
                assert_eq!(api::encode(ApiType::Name, &n.hash), b["id"]);
                assert_eq!(
                    api::encode(ApiType::AccountPubkey, &n.owner.value),
                    b["owner"]
                );
                assert_eq!(number(&n.expires_by), b["ttl"]);
                let pointers = b["pointers"].as_array().unwrap();
                assert_eq!(n.pointers.len(), pointers.len());
                for (p, expected) in n.pointers.iter().zip(pointers) {
                    assert_eq!(
                        api::encode(ApiType::Bytearray, &p.key),
                        expected["encoded_key"]
                    );
                    if let Ok(key) = std::str::from_utf8(&p.key) {
                        assert_eq!(key, expected["key"]);
                    }
                    let encoded = match &p.value {
                        ae_state::names::PointerValue::Id(id) => api::encode_id(*id),
                        ae_state::names::PointerValue::Data(data) => {
                            api::encode(ApiType::Bytearray, data)
                        }
                    };
                    assert_eq!(encoded, expected["id"]);
                }
                assert_eq!(n.serialize().unwrap(), serialized(row, &n.hash));
            }
            Ok(Reply::Auction(a)) => {
                assert_eq!(
                    serde_json::json!({"id":api::encode(ApiType::Name,&a.name_hash),"highest_bidder":api::encode(ApiType::AccountPubkey,&a.bidder.value),"highest_bid":number(&a.bid),"started_at":number(&a.started),"ends_at":number(&a.expires_by)}),
                    row["body"]
                );
                assert_eq!(a.serialize().unwrap(), serialized(row, &a.hash()));
            }
            Err(e) => {
                if let Some(code) = error(&e).1 {
                    assert_eq!(row["body"]["error_code"], code, "{}", row["input"]);
                }
            }
        }
    }
    assert_eq!(count, 563);
}
fn request(kind: Kind) -> Vec<u8> {
    if kind == Kind::Hash {
        hashing::name_hash(b"alice.chain").0.to_vec()
    } else {
        b"alice.chain".to_vec()
    }
}
#[test]
fn name_queries_bound_storage_decoding_input_pointers_and_retention_before_owned_decoding() {
    let f = fixture();
    let row = &f["dispatcher"][0];
    let (db, _) = database(row);
    let view = View::load(db.clone()).unwrap();
    for kind in [Kind::Name, Kind::Hash, Kind::Auction] {
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
            Limits {
                reads: point::Limits {
                    node_bytes: 1,
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
                input_bytes: 2,
                ..Default::default()
            },
            Limits {
                retained_bytes: 1,
                ..Default::default()
            },
        ] {
            db.copied.set(0);
            assert_eq!(
                Reader::new(&view, limits).and_then(|r| r.query(&request(kind), kind)),
                Err(Error::ResourceLimit),
                "{kind:?} {limits:?}"
            );
            assert!(db.copied.get() <= limits.reads.read_bytes);
        }
    }
    assert_eq!(
        Reader::new(
            &view,
            Limits {
                pointers: 3,
                ..Default::default()
            }
        )
        .unwrap()
        .query(b"alice.chain", Kind::Name),
        Err(Error::ResourceLimit)
    );
    let bytes = serialized(row, &hashing::name_hash(b"alice.chain").0);
    let retained_bytes =
        bytes.len() + std::mem::size_of::<Name>() + 4 * std::mem::size_of::<Pointer>();
    for (limit, ok) in [(retained_bytes, true), (retained_bytes - 1, false)] {
        let result = Reader::new(
            &view,
            Limits {
                retained_bytes: limit,
                ..Default::default()
            },
        )
        .unwrap()
        .query(b"alice.chain", Kind::Name);
        assert_eq!(result.is_ok(), ok);
        if !ok {
            assert_eq!(result, Err(Error::ResourceLimit));
        }
    }
}
#[test]
fn name_queries_keep_snapshots_authenticate_roots_and_leave_expiry_cache_unread() {
    let f = fixture();
    let row = &f["dispatcher"][0];
    let (mut db, top) = database(row);
    let record_key = repo::key(repo::RECORDS, top.0);
    let mut record = Record::decode(&db.data[&record_key]).unwrap();
    let names_root = record.roots.state.names.unwrap();
    let cache = Hash256([99; 32]);
    record.roots.names = Some(cache);
    db.put(record_key.clone(), record.encode());
    db.fault = Some(repo::key(repo::NODES, cache.0));
    let old = View::load(db.clone()).unwrap();
    let before = Reader::new(&old, Default::default())
        .unwrap()
        .query(b"alice.chain", Kind::Name)
        .unwrap();
    assert!(!db.trace.borrow().contains(db.fault.as_ref().unwrap()));
    db.remove(&record_key);
    let new = View::load(db.clone()).unwrap();
    assert_eq!(
        Reader::new(&new, Default::default())
            .unwrap()
            .query(b"alice.chain", Kind::Name),
        Err(Error::NoStateTrees)
    );
    assert_eq!(
        Reader::new(&old, Default::default())
            .unwrap()
            .query(b"alice.chain", Kind::Name)
            .unwrap(),
        before
    );
    record.roots.state.names = None;
    db.put(record_key, record.encode());
    let view = View::load(db).unwrap();
    assert_eq!(
        Reader::new(&view, Default::default())
            .unwrap()
            .query(b"alice.chain", Kind::Name),
        Err(Error::Backend(repo::Error::Corrupt("name state binding")))
    );
    let (mut db, _) = database(row);
    db.remove(&repo::key(repo::NODES, names_root.0));
    let view = View::load(db).unwrap();
    assert!(matches!(
        Reader::new(&view, Default::default())
            .unwrap()
            .query(b"alice.chain", Kind::Name),
        Err(Error::Backend(_))
    ));
    // A malformed text name is rejected without opening a damaged trie.
    assert_eq!(
        Reader::new(&view, Default::default())
            .unwrap()
            .query(b"alice", Kind::Name),
        Err(Error::Normalization(idna::Error::NoRegistrar))
    );
}
#[test]
fn missing_state_precedes_normalization_and_backend_failures_are_not_absent_names() {
    let f = fixture();
    let row = &f["dispatcher"][0];
    let (mut db, top) = database(row);
    db.remove(&repo::key(repo::RECORDS, top.0));
    let view = View::load(db).unwrap();
    for input in [b"alice".as_slice(), b"", b"\xff.chain"] {
        assert_eq!(
            Reader::new(&view, Default::default())
                .unwrap()
                .query(input, Kind::Name),
            Err(Error::NoStateTrees)
        );
    }
    let (mut db, top) = database(row);
    db.fault = Some(repo::key(repo::RECORDS, top.0));
    let view = View::load(db).unwrap();
    assert!(matches!(
        Reader::new(&view, Default::default())
            .unwrap()
            .query(b"alice", Kind::Name),
        Err(Error::Backend(_))
    ));
}
fn corrupt(row: &Json, key: &[u8], bytes: Vec<u8>) -> Json {
    let mut row = row.clone();
    let pair = row["names"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|p| hex::decode(p[0].as_str().unwrap()).unwrap() == key)
        .unwrap();
    pair[1] = Json::String(hex::encode(bytes));
    let roots = StateRoots {
        names: Memory::default().root(&row["names"]),
        ..Default::default()
    };
    row["state_hash"] = Json::String(hex::encode(roots.hash().0));
    row
}
#[test]
fn corrupt_name_status_owner_and_auction_objects_do_not_become_valid_client_records() {
    let f = fixture();
    let row = &f["dispatcher"][0];
    let hash = hashing::name_hash(b"alice.chain").0;
    let mut n = Name::deserialize(hash, &serialized(row, &hash)).unwrap();
    let mut cases = vec![
        (Kind::Name, hash.to_vec(), vec![0xc0]),
        (
            Kind::Auction,
            hashing::auction_hash(hash).to_vec(),
            vec![0xc0],
        ),
    ];
    n.status = b"other".to_vec();
    cases.push((Kind::Name, hash.to_vec(), n.serialize().unwrap()));
    n.status = b"claimed".to_vec();
    n.owner.tag = IdTag::Oracle;
    cases.push((Kind::Name, hash.to_vec(), n.serialize().unwrap()));
    let akey = hashing::auction_hash(hash);
    let mut a = Auction::deserialize(akey, &serialized(row, &akey)).unwrap();
    a.bidder.tag = IdTag::Contract;
    cases.push((Kind::Auction, akey.to_vec(), a.serialize().unwrap()));
    for (kind, key, bytes) in cases {
        let row = corrupt(row, &key, bytes);
        let (db, _) = database(&row);
        let view = View::load(db).unwrap();
        assert_eq!(
            Reader::new(&view, Default::default())
                .unwrap()
                .query(b"alice.chain", kind),
            Err(Error::InvalidObject)
        );
    }
}
