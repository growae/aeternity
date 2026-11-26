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
        "../../tests/fixtures/erlang_http_channels.json"
    ))
    .unwrap()
}
fn database(row: &Json) -> (Memory, Hash256) {
    let mut db = Memory::default();
    let roots = StateRoots {
        channels: db.root(&row["channels"]),
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
fn input(row: &Json) -> Option<[u8; 32]> {
    let raw = hex::decode(row["params_hex"]["pubkey"].as_str().unwrap()).unwrap();
    api::decode_as(ApiType::Channel, std::str::from_utf8(&raw).ok()?)
        .ok()?
        .try_into()
        .ok()
}
fn key() -> [u8; 32] {
    let mut k = [0; 32];
    k[31] = 1;
    k
}
#[test]
fn native_channel_reads_match_all_versions_delegates_authorization_and_state_failures() {
    let f = fixture();
    let mut count = 0;
    for row in f["dispatcher"].as_array().unwrap() {
        if matches!(
            row["input"]["scenario"].as_str().unwrap(),
            "overload" | "unstable"
        ) {
            continue;
        }
        let Some(key) = input(row) else { continue };
        count += 1;
        let (db, _) = database(row);
        let view = View::load(db).unwrap();
        let result = Reader::new(&view, Default::default()).unwrap().query(key);
        let code = match &result {
            Ok(_) => 200,
            Err(Error::NotPresent | Error::NoStateTrees) => 404,
            Err(_) => 500,
        };
        assert_eq!(
            row["code"].as_u64().unwrap(),
            code,
            "{} {result:?}",
            row["input"]
        );
        match result {
            Ok(channel) => {
                assert_eq!(
                    channel.for_client().unwrap(),
                    row["body"],
                    "{}",
                    row["input"]
                );
                let bytes = hex::decode(row["channels"][0][1].as_str().unwrap()).unwrap();
                assert_eq!(channel.serialize().unwrap(), bytes);
            }
            Err(Error::NotPresent) => assert_eq!(row["body"]["error_code"], "not_found"),
            Err(Error::NoStateTrees) => assert_eq!(row["body"]["error_code"], "no_state_trees"),
            _ => (),
        }
    }
    assert_eq!(count, 252);
}
#[test]
fn channel_queries_bound_reads_rlp_delegates_and_retained_objects_before_owned_decode() {
    let f = fixture();
    let row = &f["dispatcher"][0];
    let (db, _) = database(row);
    let view = View::load(db.clone()).unwrap();
    for reads in [
        point::Limits {
            read_bytes: 64,
            ..Default::default()
        },
        point::Limits {
            reads: 1,
            ..Default::default()
        },
        point::Limits {
            value_bytes: 16,
            ..Default::default()
        },
        point::Limits {
            node_bytes: 1,
            ..Default::default()
        },
        point::Limits {
            rlp: rlp::DecodeLimits {
                max_items: 4,
                ..Default::default()
            },
            ..Default::default()
        },
    ] {
        db.copied.set(0);
        assert_eq!(
            Reader::new(
                &view,
                Limits {
                    reads,
                    ..Default::default()
                }
            )
            .and_then(|r| r.query(key())),
            Err(Error::ResourceLimit)
        );
        assert!(db.copied.get() <= reads.read_bytes);
    }
    for delegates in [0, 2, 3] {
        assert_eq!(
            Reader::new(
                &view,
                Limits {
                    delegates,
                    ..Default::default()
                }
            )
            .unwrap()
            .query(key()),
            Err(Error::ResourceLimit)
        );
    }
    let bytes = hex::decode(row["channels"][0][1].as_str().unwrap()).unwrap();
    let size = bytes.len() + std::mem::size_of::<Channel>() + 4 * std::mem::size_of::<Id>();
    for (retained_bytes, ok) in [(size, true), (size - 1, false)] {
        assert_eq!(
            Reader::new(
                &view,
                Limits {
                    retained_bytes,
                    ..Default::default()
                }
            )
            .unwrap()
            .query(key())
            .is_ok(),
            ok
        );
    }
}
#[test]
fn channel_queries_authenticate_roots_keep_snapshots_and_never_traverse_unrelated_trees() {
    let f = fixture();
    let row = &f["dispatcher"][0];
    let (mut db, top) = database(row);
    let record_key = repo::key(repo::RECORDS, top.0);
    let mut record = Record::decode(&db.data[&record_key]).unwrap();
    let root = record.roots.state.channels.unwrap();
    let unrelated = Hash256([99; 32]);
    record.roots.names = Some(unrelated);
    db.put(record_key.clone(), record.encode());
    db.fault = Some(repo::key(repo::NODES, unrelated.0));
    let old = View::load(db.clone()).unwrap();
    let before = Reader::new(&old, Default::default())
        .unwrap()
        .query(key())
        .unwrap();
    assert!(!db.trace.borrow().contains(db.fault.as_ref().unwrap()));
    db.remove(&record_key);
    let new = View::load(db.clone()).unwrap();
    assert_eq!(
        Reader::new(&new, Default::default()).unwrap().query(key()),
        Err(Error::NoStateTrees)
    );
    assert_eq!(
        Reader::new(&old, Default::default())
            .unwrap()
            .query(key())
            .unwrap(),
        before
    );
    record.roots.state.channels = None;
    db.put(record_key, record.encode());
    let view = View::load(db).unwrap();
    assert_eq!(
        Reader::new(&view, Default::default()).unwrap().query(key()),
        Err(Error::Backend(repo::Error::Corrupt(
            "channel state binding"
        )))
    );
    let (mut db, _) = database(row);
    db.remove(&repo::key(repo::NODES, root.0));
    let view = View::load(db).unwrap();
    assert!(matches!(
        Reader::new(&view, Default::default()).unwrap().query(key()),
        Err(Error::Backend(_))
    ));
}
