use super::*;
use crate::registry::startup::{Configuration, Reason};
use ae_p2p::{address, discovery::Target};
use serde_json::{Value, json};
use std::net::{IpAddr, Ipv6Addr};

fn bytes(v: &Value) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn addresses(v: &Value) -> Vec<Vec<u8>> {
    v.as_array().unwrap().iter().map(bytes).collect()
}
fn ip(v: &Value) -> IpAddr {
    let a = v.as_array().unwrap();
    if a.len() == 4 {
        IpAddr::V4(Ipv4Addr::from(std::array::from_fn::<_, 4, _>(|i| {
            u8::try_from(a[i].as_u64().unwrap()).unwrap()
        })))
    } else {
        assert_eq!(a.len(), 8);
        IpAddr::V6(Ipv6Addr::from(std::array::from_fn::<_, 8, _>(|i| {
            u16::try_from(a[i].as_u64().unwrap()).unwrap()
        })))
    }
}
fn row(v: &Value) -> Record {
    Record {
        id: PeerPublicKey(bytes(&v["key"]).try_into().unwrap()),
        host: bytes(&v["host"]),
        port: v["port"].as_str().unwrap().parse().unwrap(),
        address: ip(&v["address"]),
        source: ip(&v["source"]),
        trusted: v["trusted"].as_bool().unwrap(),
    }
}
fn pj(p: &discovery::Peer) -> Value {
    json!({"key":hex::encode(p.id.0),"host":hex::encode(&p.host),
        "port":p.port.to_string(),"address":p.address.octets(),
        "source":p.source.octets(),"trusted":p.trusted})
}
fn sorted(mut v: Vec<Value>) -> Vec<Value> {
    v.sort_by_key(Value::to_string);
    v
}

#[test]
fn original_initializer_inputs_restore_through_durable_startup_with_explicit_race_repairs() {
    let data: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_peer_startup.json"
    )))
    .unwrap();
    for flag in ["full_node_started", "network_access", "real_dns_queries"] {
        assert_eq!(data[flag], false);
    }
    let scenarios = data["scenarios"].as_array().unwrap();
    assert_eq!(scenarios.len(), 15);
    let mut children = 0;
    let mut repairs = 0;
    for scenario in scenarios {
        let label = scenario["label"].as_str().unwrap();
        assert_eq!(scenario["joined_processes"], true, "{label}");
        children += scenario["resolver_processes"].as_u64().unwrap();
        let temp = Temp::new();
        let network = scenario["network"].as_str().unwrap().as_bytes();
        let (profile, genesis_network) = match network {
            b"ae_mainnet" => (NetworkProfile::mainnet(), Network::Mainnet),
            b"ae_uat" => (NetworkProfile::uat(), Network::Uat),
            _ => (
                NetworkProfile::custom(
                    network.to_vec(),
                    NetworkProfile::mainnet().activations().to_vec(),
                )
                .unwrap(),
                Network::Mainnet,
            ),
        };
        let identity = Identity::new(
            &profile,
            genesis::header(genesis_network).unwrap().hash().unwrap(),
            Hash256::ZERO,
        );
        let database = Arc::new(Database::open(&temp.0, identity).unwrap());
        let stored: Vec<_> = scenario["stored"]
            .as_array()
            .unwrap()
            .iter()
            .map(row)
            .collect();
        let mut writer = Writer::start(database.clone(), Default::default()).unwrap();
        writer
            .commit(
                writer.version().unwrap(),
                &stored
                    .iter()
                    .cloned()
                    .map(peers::Change::Put)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        drop(writer);
        let mut cfg = config();
        cfg.policy.maximum_outbound = 0;
        let mut owner = service(database.clone(), cfg);
        let configuration = Configuration {
            peers: addresses(&scenario["peers"]),
            blocked_peers: addresses(&scenario["blocked"]),
            defaults: Some(addresses(&scenario["defaults"])),
            include_default_peers: scenario["flag"] == true,
            ..Default::default()
        };
        assert!(
            owner
                .configure_startup(0, configuration, Default::default())
                .unwrap()
                .ready
        );
        // Real RocksDB publication and policy transitions; numeric callbacks
        // are supplied explicitly, without opening a network transport.
        for now in 1..=32 {
            owner.advance_startup(now, 64).unwrap();
            for effect in drain(&mut owner.core) {
                match effect {
                    Effect::Resolve { token, host } => {
                        let ip: Ipv4Addr = std::str::from_utf8(&host).unwrap().parse().unwrap();
                        owner
                            .apply(
                                now,
                                Command::Resolved {
                                    token,
                                    address: Some(ip),
                                },
                            )
                            .unwrap();
                    }
                    Effect::Blocked(_) => {}
                    other => panic!("unexpected startup effect {other:?} in {label}"),
                }
            }
            let status = owner.startup_status().unwrap();
            if status.queued == 0 && status.protected_trusted == 0 {
                break;
            }
        }
        let status = owner.startup_status().unwrap();
        assert_eq!((status.queued, status.protected_trusted), (0, 0), "{label}");
        let actual: Vec<_> = owner.policy().pool().all(Target::Both).map(pj).collect();
        let mut expected = scenario["adds"].as_array().unwrap().clone();
        let diagnostics = owner.startup_diagnostics(0, 4096).unwrap();
        if scenario["repair"] == true {
            repairs += 1;
            match label {
                "blocked-restoration"
                | "same-key-different-block-endpoint"
                | "list-subtraction-multiplicity"
                | "administrative-block-has-no-transport-port" => {
                    let blocked = scenario["blocks"].as_array().unwrap();
                    assert!(
                        expected
                            .iter()
                            .any(|p| blocked.iter().any(|b| b["key"] == p["key"]))
                    );
                    expected.retain(|p| !blocked.iter().any(|b| b["key"] == p["key"]));
                    assert!(diagnostics.iter().any(|d| d.reason == Reason::Blocked));
                }
                "conflicting-configured-endpoints" => {
                    assert_eq!(expected.len(), 2);
                    let first =
                        address::parse(&bytes(&scenario["peers"][0]), Default::default()).unwrap();
                    expected.retain(|p| {
                        bytes(&p["host"]) == first.host
                            && p["port"] == first.port.as_ref().unwrap().as_decimal()
                    });
                    assert_eq!(expected.len(), 1);
                    assert!(diagnostics.iter().any(|d| d.reason == Reason::Shadowed));
                }
                "malformed-shape-crashes-original" => {
                    assert_eq!(scenario["result"]["kind"], "exception");
                    assert!(expected.is_empty());
                    assert!(
                        diagnostics
                            .iter()
                            .any(|d| matches!(d.reason, Reason::Address(_)))
                    );
                }
                "stored-ipv6-source" => {
                    assert_eq!(expected.len(), 1);
                    expected.clear();
                    assert!(
                        diagnostics
                            .iter()
                            .any(|d| d.reason == Reason::UnsupportedSource)
                    );
                }
                _ => panic!("unclassified native divergence {label}"),
            }
        } else {
            assert_eq!(scenario["result"]["kind"], "ok");
        }
        assert_eq!(
            sorted(actual),
            sorted(expected.clone()),
            "complete peers {label}"
        );
        let blocked: Vec<_> = owner.policy().blocked().map(|p| json!({"key":hex::encode(p.id.0),"host":hex::encode(&p.host),"port":p.port.as_ref().map(|p|p.as_decimal())})).collect();
        assert_eq!(
            sorted(blocked),
            sorted(scenario["blocks"].as_array().unwrap().clone()),
            "raw blocks {label}"
        );
        let mut expected_rows: BTreeMap<_, _> = stored.into_iter().map(|p| (p.id, p)).collect();
        for block in owner.policy().blocked() {
            expected_rows.remove(&block.id);
        }
        for p in &expected {
            if p["trusted"] == false {
                let p = row(p);
                expected_rows.insert(p.id, p);
            }
        }
        assert_eq!(durable(&database), expected_rows, "stored rows {label}");
        assert_eq!(database.view().unwrap().status(), Default::default());
        drop(owner);
        drop(database);
        eprintln!("Original startup {label}: complete peer and raw block fields checked");
    }
    assert_eq!((children, repairs), (26, 7));
}
