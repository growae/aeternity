use ae_p2p::message::{self, ChainPoi};
use ae_sync::peer::capabilities::{CHAIN_POI, Error, Limits, Registry, Value};
use ae_types::PeerPublicKey;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;

fn peer(n: u8) -> PeerPublicKey {
    PeerPublicKey([n; 32])
}
fn poi(n: u64) -> ChainPoi {
    let hash = |n| {
        let mut b = vec![0; 32];
        b[31] = n;
        b
    };
    ChainPoi {
        height: n.into(),
        root_hash: hash(1),
        genesis: hash(2),
        top: hash(3),
        poi: vec![n as u8],
    }
}
fn value(v: &Json) -> Value {
    if let Some(b) = v.get("opaque") {
        return Value::Opaque(hex::decode(b.as_str().unwrap()).unwrap());
    }
    Value::ChainPoi(
        v[CHAIN_POI]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| ChainPoi {
                height: p["height"].as_str().unwrap().parse::<u64>().unwrap().into(),
                root_hash: hex::decode(p["root_hash"].as_str().unwrap()).unwrap(),
                genesis: hex::decode(p["genesis"].as_str().unwrap()).unwrap(),
                top: hex::decode(p["top"].as_str().unwrap()).unwrap(),
                poi: hex::decode(p["poi"].as_str().unwrap()).unwrap(),
            })
            .collect(),
    )
}
fn map(v: &Json) -> BTreeMap<String, Value> {
    v.as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), value(v)))
        .collect()
}
#[test]
fn original_registry_callbacks_and_production_wire_boundaries() {
    let fixture: Json =
        serde_json::from_str(include_str!("fixtures/erlang_capabilities.json")).unwrap();
    assert_eq!(fixture["full_node_started"], false);
    assert_eq!(fixture["server"]["remove_peer_crashes"], true);
    assert_eq!(fixture["server"]["arbitrary_erlang_term_roundtrip"], true);
    let mut registry = Registry::new(Limits::default()).unwrap();
    assert_eq!(fixture["rows"].as_array().unwrap().len(), 11);
    for row in fixture["rows"].as_array().unwrap() {
        let op = &row["operation"];
        let key = op["key"].as_str().unwrap_or("");
        match op["kind"].as_str().unwrap() {
            "set" => registry
                .set(key, &Value::ChainPoi(vec![poi(op["n"].as_u64().unwrap())]))
                .unwrap(),
            "opaque" => registry
                .set(
                    key,
                    &Value::Opaque(op["bytes"].as_str().unwrap().as_bytes().to_vec()),
                )
                .unwrap(),
            "delete" => registry.delete(key).unwrap(),
            "register" => {
                let p = peer(op["peer"].as_u64().unwrap() as u8);
                registry.attach(p, 1).unwrap();
                let entries = op["entries"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|e| (e["key"].as_str().unwrap().to_owned(), value(&e["value"])))
                    .collect();
                registry.replace(p, &1, &entries).unwrap();
            }
            "remove" => {
                // Native callback raises before returning a state; the harness
                // preserves supplied state only to compare subsequent queries.
                assert_eq!(
                    row["reply"],
                    json!({"exception":"error","reason":"function_clause"})
                );
            }
            _ => panic!("{op}"),
        }
        assert_eq!(registry.local(), &map(&row["state"]["local"]));
        for key in [CHAIN_POI, "future", "missing"] {
            assert_eq!(
                registry.get(key),
                row["gets"][key]
                    .as_object()
                    .map(|_| value(&row["gets"][key]))
                    .as_ref()
            );
            let expected: Vec<_> = row["queries"][key]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| (peer(v["peer"].as_u64().unwrap() as u8), value(&v["value"])))
                .collect();
            let actual: Vec<_> = registry
                .peers_with(key)
                .into_iter()
                .map(|p| (p.peer, p.value))
                .collect();
            assert_eq!(actual, expected, "{row}");
        }
    }
    for case in fixture["encodings"].as_array().unwrap() {
        let mut r = Registry::<u64>::new(Limits::default()).unwrap();
        let name = case["name"].as_str().unwrap();
        let records = match name {
            "empty" => vec![],
            "one" => vec![poi(1)],
            "duplicates" => vec![poi(1), poi(2)],
            "zero_records" => {
                r.set(CHAIN_POI, &Value::ChainPoi(vec![])).unwrap();
                vec![]
            }
            "two_records" => {
                r.set(CHAIN_POI, &Value::ChainPoi(vec![poi(1), poi(2)]))
                    .unwrap();
                vec![]
            }
            "unknown" | "unknown_map" => {
                r.set("future", &Value::Opaque(vec![])).unwrap();
                vec![]
            }
            "opaque" => {
                r.set(CHAIN_POI, &Value::Opaque(b"value".to_vec())).unwrap();
                vec![]
            }
            // Binary keys are not represented by the Rust string-key API.
            "binary_key" => {
                assert_eq!(case["result"]["kind"], "exception");
                continue;
            }
            _ => panic!("{case}"),
        };
        if case["result"]["kind"] == "ok" {
            assert_eq!(
                hex::encode(message::encode_capabilities(&records, Default::default()).unwrap()),
                case["result"]["wire"]
            );
            if name != "empty" {
                r.set(
                    CHAIN_POI,
                    &Value::ChainPoi(vec![records.last().unwrap().clone()]),
                )
                .unwrap();
            }
            assert_eq!(
                r.wire().unwrap(),
                records.last().cloned().into_iter().collect::<Vec<_>>()
            );
        } else {
            assert!(r.wire().is_err(), "{case}");
        }
    }
    for case in fixture["decodings"].as_array().unwrap() {
        let result = message::decode_capabilities(
            &hex::decode(case["wire"].as_str().unwrap()).unwrap(),
            Default::default(),
        );
        if case["result"]["kind"] == "ok" {
            let expected: Vec<_> = case["result"]["entries"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|e| {
                    let Value::ChainPoi(records) = value(&e["value"]) else {
                        panic!()
                    };
                    records
                })
                .collect();
            assert_eq!(result.unwrap(), expected);
        } else {
            assert!(result.is_err());
        }
    }
}

#[test]
fn atomic_budgets_generation_fences_and_empty_ping_retention() {
    let limits = Limits {
        local_entries: 1,
        peers: 1,
        entries_per_peer: 1,
        key_bytes: 16,
        value_bytes: 400,
        total_bytes: 750,
    };
    let mut r = Registry::new(limits).unwrap();
    let original = Value::Opaque(vec![1; 100]);
    r.set("future", &original).unwrap();
    let before = r.retained_bytes();
    assert_eq!(
        r.set("future", &Value::Opaque(vec![0; 400])),
        Err(Error::Capacity)
    );
    assert_eq!(r.set("extra", &original), Err(Error::Capacity));
    assert_eq!(r.get("future"), Some(&original));
    assert_eq!(r.retained_bytes(), before);
    r.attach(peer(1), 1).unwrap();
    assert_eq!(r.attach(peer(2), 1), Err(Error::Capacity));
    r.receive(peer(1), &1, &[poi(1), poi(2)]).unwrap();
    let before = r.retained_bytes();
    let expected = r.peers_with(CHAIN_POI);
    r.receive(peer(1), &1, &[]).unwrap();
    assert_eq!(r.peers_with(CHAIN_POI), expected);
    let oversized = BTreeMap::from([("future".into(), Value::Opaque(vec![2; 335]))]);
    assert_eq!(r.replace(peer(1), &1, &oversized), Err(Error::Capacity));
    assert_eq!(r.retained_bytes(), before);
    assert_eq!(r.peers_with(CHAIN_POI), expected);
    r.attach(peer(1), 2).unwrap();
    r.receive(peer(1), &2, &[poi(3)]).unwrap();
    assert!(!r.remove(peer(1), &1));
    assert_eq!(r.receive(peer(1), &1, &[poi(9)]), Err(Error::Stale));
    assert_eq!(
        r.peers_with(CHAIN_POI)[0].value,
        Value::ChainPoi(vec![poi(3)])
    );
    assert!(r.remove(peer(1), &2));
    assert!(!r.remove(peer(1), &2));
    r.delete("future").unwrap();
    assert_eq!(r.retained_bytes(), 0);
    r.attach(peer(1), 3).unwrap();
    r.close();
    assert_eq!(r.retained_bytes(), 0);
    assert_eq!(r.set("future", &original), Err(Error::Closed));
    assert_eq!(r.attach(peer(1), 4), Err(Error::Closed));
}
