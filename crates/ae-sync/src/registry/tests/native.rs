use super::*;
use ae_p2p::{
    discovery::{Class, Target},
    registry::{Direction, Info, Kind, Status, Token},
};
use serde_json::{Value, json};
fn number(k: PeerPublicKey) -> u64 {
    u64::from_be_bytes(k.0[24..].try_into().unwrap())
}
fn ip(v: &Value) -> Ipv4Addr {
    Ipv4Addr::from(std::array::from_fn::<_, 4, _>(|i| {
        v[i].as_u64().unwrap() as u8
    }))
}
fn from_json(v: &Value) -> discovery::Peer {
    discovery::Peer {
        id: key(v["id"].as_u64().unwrap()),
        address: ip(&v["address"]),
        source: ip(&v["source"]),
        host: hex::decode(v["host"].as_str().unwrap()).unwrap(),
        port: v["port"].as_u64().unwrap() as u16,
        trusted: v["trusted"].as_bool().unwrap(),
    }
}
fn pj(p: &discovery::Peer) -> Value {
    json!({"id":number(p.id),"address":p.address.octets(),"source":p.source.octets(),"host":hex::encode(&p.host),"port":p.port,"trusted":p.trusted})
}
fn snapshot(r: &Registry, actors: &BTreeMap<u64, Token>) -> Value {
    let counts = r.counts();
    let peers:Vec<_>=r.pool().all(Target::Both).map(|p|{let st=r.pool().state(p.id).unwrap();json!({"peer":pj(p),"class":match st.class{Class::Verified=>"verified",Class::Unverified=>"unverified"},"available":st.available,"selected":st.selected,"rejections":st.rejections})}).collect();
    let connections:Vec<_>=r.connections().map(|c|json!({"id":number(c.peer.id),"actor":actors.iter().find(|(_,t)|**t==c.token).unwrap().0,
        "direction":match c.direction{Direction::Inbound=>"inbound",Direction::Outbound=>"outbound"},"status":match c.status{Status::Connecting=>"connecting",Status::Connected=>"connected"},
        "probe":c.probe.is_some(),"kind":match c.kind{Kind::Permanent=>"permanent",Kind::Temporary=>"temporary"},"ping":c.status==Status::Connected&&r.connection_timer(c.peer.id).is_some()})).collect();
    json!({"peers":peers,"connections":connections,"counts":{"inbound":counts.inbound,"outbound":counts.outbound,"probes":counts.probes,"blocked":counts.blocked,"hosts":counts.hosts},"blocked":r.blocked().map(|p|number(p.id)).collect::<Vec<_>>()})
}

#[test]
fn original_registry_traces_publish_only_after_real_peer_storage_commits() {
    let data: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../ae-p2p/tests/fixtures/erlang_registry.json"
    )))
    .unwrap();
    let mut total = 0;
    for scenario in data["scenarios"].as_array().unwrap() {
        let temp = Temp::new();
        let database = db(&temp);
        let mut cfg = config();
        cfg.policy.maximum_inbound = scenario["max_inbound"].as_u64().unwrap() as usize;
        cfg.policy.maximum_outbound = scenario["max_outbound"].as_u64().unwrap() as usize;
        cfg.policy.single_outbound_per_group = scenario["single_group"].as_bool().unwrap();
        if scenario["label"] == "grouped-resolver-and-trusted-retries" {
            cfg.policy.resolver_retries = 2;
        }
        let unblock = cfg.policy.unblock_interval;
        let mut s = service(database.clone(), cfg);
        let mut actors = BTreeMap::new();
        let mut resolutions = BTreeMap::new();
        let mut expected_rows = BTreeMap::new();
        let mut now = 0;
        for (index, row) in scenario["steps"].as_array().unwrap().iter().enumerate() {
            now += 1;
            let id = key(row["id"].as_u64().unwrap_or(0));
            let command = match row["op"].as_str().unwrap() {
                "add" => Command::Add(from_json(&row["peer"])),
                "delete" => Command::Delete(id),
                "block" => Command::Block(Info::from(&from_json(&row["peer"]))),
                "unblock" => Command::Unblock(id),
                "connect" => Command::Connect(Some(id)),
                "connect_random" => Command::Connect(None),
                "probe" => Command::Probe(if row["class"] == "verified" {
                    Class::Verified
                } else {
                    Class::Unverified
                }),
                "connected" => Command::Connected {
                    peer: id,
                    token: actors[&row["actor"].as_u64().unwrap()],
                },
                "alive" => Command::Alive {
                    peer: id,
                    token: actors[&row["actor"].as_u64().unwrap()],
                },
                "failure" => Command::Failed {
                    peer: id,
                    token: actors[&row["actor"].as_u64().unwrap()],
                    reason: match row["reason"].as_str().unwrap() {
                        "failed" => policy::Failure::Failed,
                        "closed" => policy::Failure::Closed,
                        "down" => policy::Failure::Down,
                        "dead" => policy::Failure::Dead,
                        _ => panic!(),
                    },
                },
                "inbound" => {
                    let p = from_json(&row["peer"]);
                    let token = match s
                        .apply(now, Command::ReserveInbound { address: p.address })
                        .unwrap()
                    {
                        Outcome::Reserved(t) => t,
                        _ => panic!(),
                    };
                    assert!(drain(&mut s.core).is_empty());
                    actors.insert(row["actor"].as_u64().unwrap(), token);
                    Command::Accept {
                        token,
                        peer: Info::from(&p),
                    }
                }
                "ping" | "first_ping_done" => Command::PingDone {
                    peer: id,
                    token: s.policy().connection(id).unwrap().token,
                },
                "ping_timeout" => {
                    let t = s.policy().connection_timer(id).unwrap();
                    now = now.max(t.deadline);
                    Command::Timeout(t.token)
                }
                "suspect" => Command::Suspect(id),
                "expire_blocks" => {
                    now += unblock + 1;
                    Command::IsBlocked(key(0))
                }
                "queue" => {
                    let p = from_json(&row["peer"]);
                    s.apply(
                        now,
                        Command::Resolve {
                            peer: Info::from(&p),
                            source: if row["source"].is_null() {
                                None
                            } else {
                                Some(ip(&row["source"]))
                            },
                            trusted: p.trusted,
                        },
                    )
                    .unwrap();
                    let effects = drain(&mut s.core);
                    if let Some(token) = effects.iter().find_map(|e| match e {
                        Effect::Resolve { token, .. } => Some(*token),
                        _ => None,
                    }) {
                        resolutions.insert(p.host, token);
                        Command::Resolved {
                            token,
                            address: None,
                        }
                    } else {
                        Command::IsBlocked(key(0))
                    }
                }
                "resolve_error" | "resolve_success" => {
                    let host = hex::decode(row["host"].as_str().unwrap()).unwrap();
                    if let Some(t) = s.policy().resolution_timer(&host) {
                        now = now.max(t.deadline);
                        s.apply(now, Command::Timeout(t.token)).unwrap();
                        let token = drain(&mut s.core)
                            .iter()
                            .find_map(|e| match e {
                                Effect::Resolve { token, .. } => Some(*token),
                                _ => None,
                            })
                            .unwrap();
                        resolutions.insert(host.clone(), token);
                    }
                    Command::Resolved {
                        token: resolutions[&host],
                        address: if row["op"] == "resolve_success" {
                            Some(ip(&row["address"]))
                        } else {
                            None
                        },
                    }
                }
                op => panic!("unknown {op}"),
            };
            let result = s.apply(now, command).unwrap();
            for e in row["effects"].as_array().unwrap() {
                match e["kind"].as_str().unwrap() {
                    "persist" => {
                        let p = record(&from_json(&e["peer"]));
                        expected_rows.insert(p.id, p);
                    }
                    "delete" => {
                        expected_rows.remove(&key(e["peer"]["id"].as_u64().unwrap()));
                    }
                    _ => {}
                }
            }
            assert_eq!(
                durable(&database),
                expected_rows,
                "durability {} step {index}",
                scenario["label"]
            );
            assert_eq!(s.core.records, expected_rows);
            // Only inspect executable deliveries after checking the durable table.
            let effects = drain(&mut s.core);
            for e in &effects {
                if let Effect::Dial { token, peer, .. } = e {
                    let native = row["effects"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|e| e["kind"] == "dial" && e["id"] == number(peer.id))
                        .unwrap();
                    actors.insert(native["actor"].as_u64().unwrap(), *token);
                }
            }
            let actual:Vec<_>=effects.iter().filter_map(|e|match e{
                Effect::Dial{token,peer,probe}=>Some(json!({"kind":"dial","id":number(peer.id),"actor":actors.iter().find(|(_,t)|**t==*token).unwrap().0,"transport":if probe.is_some(){"tcp"}else{"noise"}})),
                Effect::Ping{peer,..}=>Some(json!({"kind":"ping","id":number(*peer)})),Effect::Blocked(id)=>Some(json!({"kind":"blocked","id":number(*id)})),
                Effect::Unblocked(id)=>Some(json!({"kind":"unblocked","id":number(*id)})),Effect::Close(_)|Effect::ProbeStatus{..}=>None,
                _=>panic!("unexpected native delivery")}).collect();
            let expected: Vec<_> = row["effects"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|e| !matches!(e["kind"].as_str().unwrap(), "close" | "persist" | "delete"))
                .cloned()
                .collect();
            assert_eq!(
                actual, expected,
                "effects {} step {index}",
                scenario["label"]
            );
            let observed = snapshot(s.policy(), &actors);
            for field in ["peers", "connections", "counts", "blocked"] {
                assert_eq!(
                    observed[field], row["state"][field],
                    "{field} {} step {index}",
                    scenario["label"]
                );
            }
            if row["op"] == "inbound" {
                let result = match result {
                    Outcome::Accepted(Kind::Permanent) => "permanent",
                    Outcome::Accepted(Kind::Temporary) => "temporary",
                    Outcome::Rejected(policy::Rejection::Blocked) => "blocked",
                    Outcome::Rejected(policy::Rejection::AlreadyConnected) => "already_connected",
                    Outcome::Rejected(policy::Rejection::TrustedAddressChanged) => {
                        "trusted_address_changed"
                    }
                    r => panic!("{r:?}"),
                };
                assert_eq!(row["result"], result);
            }
            total += 1;
        }
        let expected = expected_rows;
        drop(s);
        drop(database);
        let database = db(&temp);
        let s = service(database.clone(), config());
        assert_eq!(s.core.records, expected);
        assert_eq!(durable(&database), expected);
    }
    assert_eq!(total, 109);
    eprintln!(
        "Durable registry: 109 original lifecycle steps across nine scenarios; every delivery follows real peer persistence"
    );
}
