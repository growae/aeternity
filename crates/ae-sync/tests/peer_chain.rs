use ae_chain::{
    block::Block,
    repository::{self as repo, Error, Identity, Read, ReadConfig, Status, View, key},
};
use ae_codec::headers::HeaderMode;
use ae_p2p::message::{Kind, Limits, Message, Value};
use ae_storage::pool::Stored;
use ae_sync::peer::snapshot;
use ae_trie::NodeReader;
use ae_types::Hash256;
use serde_json::Value as Json;
use std::{cell::Cell, collections::BTreeMap, rc::Rc, sync::Arc};

#[derive(Clone)]
struct Store {
    values: Arc<BTreeMap<Vec<u8>, Vec<u8>>>,
    reads: Rc<Cell<usize>>,
    fail_after: Option<usize>,
}
impl NodeReader for Store {
    fn node(&self, _: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        panic!("serving requires no state tree")
    }
}
impl Read for Store {
    fn source_id(&self) -> u64 {
        1
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let count = self.reads.get();
        if self.fail_after == Some(count) {
            return Err(Error::Backend("injected snapshot read failure".into()));
        }
        self.reads.set(count + 1);
        Ok(self.values.get(key).cloned())
    }
    fn prefix(&self, _: &[u8], _: usize) -> Result<repo::Entries, Error> {
        panic!("queries use bounded point reads, not global scans")
    }
    fn range(&self, _: &[u8], _: &[u8], _: usize) -> Result<repo::Entries, Error> {
        panic!("queries use bounded point reads, not global scans")
    }
    fn visit_range(
        &self,
        _: &[u8],
        _: &[u8],
        _: usize,
        _: &mut repo::Visitor<'_>,
    ) -> Result<(), Error> {
        panic!("queries use bounded point reads, not global scans")
    }
}
fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn hash(v: &Json) -> Hash256 {
    Hash256(bytes(v).try_into().unwrap())
}
fn fixture() -> Json {
    serde_json::from_str(include_str!("fixtures/erlang_peer_chain.json")).unwrap()
}
fn mode(profile: &Json) -> HeaderMode {
    if profile["mode"] == "hc" {
        HeaderMode::Hyperchain
    } else {
        HeaderMode::ProofOfWork
    }
}
fn database(profile: &Json, scenario: &Json) -> Store {
    let mut values = BTreeMap::new();
    let identity = Identity {
        network: b"ae_dev_peer_chain".to_vec(),
        genesis: hash(&profile["genesis"]),
        protocols: Hash256([0; 32]),
        execution_profile: Hash256([0; 32]),
    };
    values.insert(repo::IDENTITY.to_vec(), identity.encode());
    let tip = (!scenario["top"].is_null()).then(|| hash(&scenario["top"]));
    values.insert(
        repo::STATUS.to_vec(),
        Status {
            revision: u64::from(tip.is_some()),
            tip,
        }
        .encode(),
    );
    for row in profile["blocks"].as_array().unwrap() {
        let encoded = bytes(&row["block"]);
        let block = Block::decode(
            &encoded,
            ae_chain::block::DecodeConfig {
                mode: mode(profile),
                ..Default::default()
            },
        )
        .unwrap();
        let id = block.hash().unwrap();
        assert_eq!(id, hash(&row["hash"]));
        assert_eq!(block.encode().unwrap(), encoded);
        values.insert(key(repo::BLOCKS, id.0), encoded);
        if !scenario["omitted"]
            .as_array()
            .unwrap()
            .contains(&row["hash"])
        {
            let mut height = block.header().common().height.to_be_bytes().to_vec();
            height.extend(id.0);
            values.insert(key(repo::HEIGHTS, height), vec![]);
        }
    }
    for row in scenario["keys"].as_array().unwrap() {
        values.insert(
            key(repo::KEYS, row["height"].as_u64().unwrap().to_be_bytes()),
            bytes(&row["hash"]),
        );
    }
    for id in scenario["canonical"].as_array().unwrap() {
        values.insert(key(repo::CANONICAL, bytes(id)), vec![]);
    }
    Store {
        values: Arc::new(values),
        reads: Rc::new(Cell::new(0)),
        fail_after: None,
    }
}
fn stored(store: Store, profile: &Json) -> Stored<Store> {
    Stored::load(
        View::load_with_config(
            store,
            ReadConfig {
                mode: mode(profile),
                ..Default::default()
            },
        )
        .unwrap(),
    )
    .unwrap()
}
fn request(row: &Json) -> Message {
    Message::decode_packet(&bytes(&row["request"]), Default::default()).unwrap()
}

#[test]
fn original_successor_and_generation_traversal_matches_complete_wire_responses() {
    let native = fixture();
    let mut counts = [0; 3];
    for profile in native["profiles"].as_array().unwrap() {
        for scenario in profile["scenarios"].as_array().unwrap() {
            let view = stored(database(profile, scenario), profile);
            for row in scenario["rows"].as_array().unwrap() {
                let request = request(row);
                let result = snapshot::serve(&view, &request, Default::default());
                if row["result"]["kind"] == "response" {
                    let response = result.unwrap().unwrap();
                    assert_eq!(
                        response.encode_packet(Default::default()).unwrap(),
                        bytes(&row["result"]["packet"]),
                        "{} {} {request:?}",
                        profile["mode"],
                        scenario["name"]
                    );
                    counts[0] += 1;
                } else if row["result"]["reason"] == "{badmatch,undefined}" {
                    assert!(
                        matches!(result, Err(snapshot::Error::Storage(Error::Uninitialized))),
                        "{result:?}"
                    );
                    counts[1] += 1;
                } else {
                    assert_eq!(
                        row["result"]["reason"],
                        "{badmatch,{error,chain_too_short}}"
                    );
                    assert!(
                        matches!(
                            result,
                            Err(snapshot::Error::Storage(Error::MissingCanonicalKey(_)))
                        ),
                        "{result:?}"
                    );
                    counts[2] += 1;
                }
            }
        }
    }
    assert_eq!(counts, [5100, 12, 504]);
}

#[test]
fn response_quotas_and_every_storage_failure_prefix_remain_operational_errors() {
    let native = fixture();
    let profile = &native["profiles"][0];
    let scenario = profile["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "k2" && s["omitted"].as_array().unwrap().is_empty())
        .unwrap();
    let mut kinds = std::collections::BTreeSet::new();
    for row in scenario["rows"].as_array().unwrap() {
        let request = request(row);
        if row["result"]["kind"] != "response" {
            continue;
        }
        let expected =
            Message::decode_packet(&bytes(&row["result"]["packet"]), Default::default()).unwrap();
        let ae_p2p::message::Details::Response {
            message: Some(Ok(body)),
            ..
        } = expected.details()
        else {
            continue;
        };
        let nonempty = match body.kind() {
            Kind::Generation => {
                matches!(&body.fields()["micro_blocks"], Value::List(items) if items.len() == 2)
            }
            Kind::HeaderHashes => {
                matches!(&body.fields()["header_hashes"], Value::List(items) if items.len() == 2)
            }
            _ => false,
        };
        if !nonempty || !kinds.insert((request.kind(), request.version())) {
            continue;
        }
        let store = database(profile, scenario);
        let view = stored(store.clone(), profile);
        store.reads.set(0);
        snapshot::serve(&view, &request, Default::default()).unwrap();
        let reads = store.reads.get();
        assert!(reads > 2);
        for prefix in 0..reads {
            let view = stored(
                Store {
                    fail_after: Some(prefix),
                    ..store.clone()
                },
                profile,
            );
            // View construction itself reads identity/status/pool revision; reset
            // only after capturing the immutable snapshot for the request.
            store.reads.set(0);
            assert!(
                matches!(
                    snapshot::serve(&view, &request, Default::default()),
                    Err(snapshot::Error::Storage(Error::Backend(_)))
                ),
                "prefix {prefix}/{reads}"
            );
            store.reads.set(reads + 100);
        }
        for limits in [
            Limits {
                body_bytes: 1,
                ..Default::default()
            },
            Limits {
                items: 0,
                ..Default::default()
            },
        ] {
            let view = stored(database(profile, scenario), profile);
            assert!(matches!(
                snapshot::serve(&view, &request, limits),
                Err(snapshot::Error::Storage(Error::ResourceLimit))
            ));
        }
    }
    assert_eq!(kinds.len(), 3);
}
