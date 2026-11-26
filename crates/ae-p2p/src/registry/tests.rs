use super::*;
use serde_json::{Value, json};
mod metrics;

fn key(n: u64) -> PeerPublicKey {
    let mut b = [0; 32];
    b[24..].copy_from_slice(&n.to_be_bytes());
    PeerPublicKey(b)
}
fn number(k: PeerPublicKey) -> u64 {
    u64::from_be_bytes(k.0[24..].try_into().unwrap())
}
fn peer(n: u64, trusted: bool) -> Peer {
    Peer {
        id: key(n),
        address: Ipv4Addr::new(10, n as u8, 1, 1),
        source: Ipv4Addr::new(20, n as u8, 2, 2),
        host: format!("peer{n}").into_bytes(),
        port: 3000 + n as u16,
        trusted,
    }
}
fn config() -> Config {
    Config {
        pool: discovery::Config {
            verified_buckets: 1,
            verified_size: 16,
            unverified_buckets: 1,
            unverified_size: 16,
            maximum_references: 1,
            standby_times: vec![1_000_000],
            maximum_update_lapse: 100_000_000,
            maximum_rejections: 2,
            ..Default::default()
        },
        ..Default::default()
    }
}
fn registry(c: Config) -> Registry {
    Registry::with_seed(key(128), c, &[1, 2, 3, 4], [7; 32]).unwrap()
}
fn apply(r: &mut Registry, now: u64, c: Command) -> Change {
    let p = r.prepare(now, c).unwrap();
    r.commit(p).unwrap()
}
fn reserve(r: &mut Registry, now: u64, address: Ipv4Addr) -> Token {
    match apply(r, now, Command::ReserveInbound { address }).result {
        Outcome::Reserved(t) => t,
        _ => panic!(),
    }
}
fn add_connected(r: &mut Registry, now: u64, p: Peer) -> Token {
    apply(r, now, Command::Add(p.clone()));
    let token = match r.connection(p.id) {
        Some(c) => c.token,
        None => match apply(r, now, Command::Connect(Some(p.id))).result {
            Outcome::Selected(t) => t,
            _ => panic!(),
        },
    };
    apply(r, now, Command::Connected { peer: p.id, token });
    token
}
fn ip(v: &Value) -> Ipv4Addr {
    Ipv4Addr::from(std::array::from_fn::<_, 4, _>(|i| {
        v[i].as_u64().unwrap() as u8
    }))
}
fn from_json(v: &Value) -> Peer {
    Peer {
        id: key(v["id"].as_u64().unwrap()),
        address: ip(&v["address"]),
        source: ip(&v["source"]),
        host: hex::decode(v["host"].as_str().unwrap()).unwrap(),
        port: v["port"].as_u64().unwrap() as u16,
        trusted: v["trusted"].as_bool().unwrap(),
    }
}
fn pj(p: &Peer) -> Value {
    json!({"id":number(p.id),"address":p.address.octets(),"source":p.source.octets(),"host":hex::encode(&p.host),"port":p.port,"trusted":p.trusted})
}
fn class(c: Class) -> &'static str {
    match c {
        Class::Verified => "verified",
        Class::Unverified => "unverified",
    }
}
fn snapshot(r: &Registry, actors: &BTreeMap<u64, Token>) -> Value {
    let s = &r.state;
    let counts = r.counts();
    let peers:Vec<_>=r.pool().all(Target::Both).map(|p| { let st=r.pool().state(p.id).unwrap();json!({"peer":pj(p),"class":class(st.class),"available":st.available,"selected":st.selected,"rejections":st.rejections}) }).collect();
    let connections:Vec<_>=r.connections().map(|c|json!({"id":number(c.peer.id),"actor":actors.iter().find(|(_,t)|**t==c.token).unwrap().0,"direction":match c.direction {Direction::Inbound=>"inbound",Direction::Outbound=>"outbound"},"status":match c.status {Status::Connecting=>"connecting",Status::Connected=>"connected"},"probe":c.probe.is_some(),"kind":match c.kind {Kind::Permanent=>"permanent",Kind::Temporary=>"temporary"},"ping":c.status==Status::Connected && s.connections[&c.peer.id].timer.is_some()})).collect();
    let resolvers:Vec<_>=s.hosts.iter().map(|(host,h)|{
        assert!(h.attempt.is_none());let delay=s.config.resolver_backoff[(h.retries.min(s.config.resolver_backoff.len() as u64)-1) as usize];
        assert!(s.timers[&h.timer].at<=s.now+delay && s.timers[&h.timer].at>=s.now+delay-1000);
        json!({"host":hex::encode(host),"retries":h.retries,"delay":delay,"peers":h.peers.values().map(|p|json!({"id":number(p.info.id),"host":hex::encode(&p.info.host),"port":p.info.port,"source":p.source.map(|s|s.octets()),"trusted":p.trusted})).collect::<Vec<_>>()})
    }).collect();
    json!({"peers":peers,"connections":connections,"counts":{"inbound":counts.inbound,"outbound":counts.outbound,"probes":counts.probes,"blocked":counts.blocked,"hosts":counts.hosts},"blocked":r.blocked().map(|p|number(p.id)).collect::<Vec<_>>(),"resolvers":resolvers,"timers":{"connect":s.connect_timer.is_some(),"verified_probe":s.probe_timers[0].is_some(),"unverified_probe":s.probe_timers[1].is_some()}})
}
fn invariant(r: &Registry) {
    let s = &r.state;
    let c = r.counts();
    assert!(s.connections.len() + s.pending.len() <= s.config.maximum_connections);
    assert!(
        c.outbound <= s.config.maximum_outbound
            && c.probes <= s.config.maximum_probes
            && c.temporary <= s.config.maximum_temporary
    );
    assert!(
        c.pending_inbound <= s.config.maximum_pending_inbound
            && c.blocked <= s.config.maximum_blocked
    );
    assert!(
        c.hosts <= s.config.maximum_hosts && c.resolving_peers <= s.config.maximum_resolving_peers
    );
    let mut sockets: BTreeMap<SocketAddrV4, BTreeSet<PeerPublicKey>> = BTreeMap::new();
    for p in s.pool.all(Target::Both) {
        assert_eq!(
            s.pool.state(p.id).unwrap().selected,
            s.connections.contains_key(&p.id)
        );
        assert!(!s.blocked.contains_key(&p.id));
        sockets.entry(p.socket()).or_default().insert(p.id);
    }
    assert_eq!(sockets, s.sockets);
    let mut timers = BTreeSet::new();
    timers.extend(s.connect_timer);
    timers.extend(s.probe_timers.iter().flatten().copied());
    let mut owners = BTreeSet::new();
    for (id, o) in &s.connections {
        assert_eq!(*id, o.connection.peer.id);
        assert!(owners.insert(o.connection.token));
        let p = s.pool.find(*id).unwrap();
        assert_eq!(p.socket(), o.connection.peer.socket());
        if let Some(t) = o.timer {
            assert!(timers.insert(t));
            assert!(
                matches!(s.timers[&t].kind,TimerKind::Owner(i,g)if i==*id && g==o.connection.token)
            );
        }
    }
    for (token, p) in &s.pending {
        assert!(owners.insert(*token));
        assert!(timers.insert(p.timer));
        assert!(matches!(s.timers[&p.timer].kind,TimerKind::Inbound(g)if g==*token));
    }
    for (host, h) in &s.hosts {
        assert!(!h.peers.is_empty());
        assert!(h.peers.len() <= s.config.maximum_peers_per_host);
        assert!(timers.insert(h.timer));
        assert!(matches!(&s.timers[&h.timer].kind,TimerKind::Resolve(v)if v==host));
        if let Some(t) = h.attempt {
            assert!(owners.insert(t));
        }
    }
    assert_eq!(timers, s.timers.keys().copied().collect());
    for t in timers.iter().chain(owners.iter()) {
        assert_eq!(t.registry, s.registry);
        assert!(t.sequence > 0 && t.sequence <= s.sequence);
    }
    for id in s.probe_status.keys() {
        assert!(s.pool.find(*id).is_some());
    }
}

#[test]
fn original_registry_lifecycles_preserve_peer_policy_membership_and_persistence() {
    let data: Value =
        serde_json::from_str(include_str!("../../tests/fixtures/erlang_registry.json")).unwrap();
    native_lifecycles(&data, 109);
}
fn native_lifecycles(data: &Value, expected_steps: usize) {
    let mut total = 0;
    for scenario in data["scenarios"].as_array().unwrap() {
        let mut c = config();
        c.maximum_inbound = scenario["max_inbound"].as_u64().unwrap() as usize;
        c.maximum_outbound = scenario["max_outbound"].as_u64().unwrap() as usize;
        c.single_outbound_per_group = scenario["single_group"].as_bool().unwrap();
        if scenario["label"] == "grouped-resolver-and-trusted-retries" {
            c.resolver_retries = 2;
        }
        let mut r = registry(c);
        let mut actors = BTreeMap::new();
        let mut resolutions = BTreeMap::new();
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
                        "failed" => Failure::Failed,
                        "closed" => Failure::Closed,
                        "down" => Failure::Down,
                        "dead" => Failure::Dead,
                        _ => panic!(),
                    },
                },
                "inbound" => {
                    let p = from_json(&row["peer"]);
                    let token = reserve(&mut r, now, p.address);
                    actors.insert(row["actor"].as_u64().unwrap(), token);
                    Command::Accept {
                        token,
                        peer: Info::from(&p),
                    }
                }
                "ping" | "first_ping_done" => Command::PingDone {
                    peer: id,
                    token: r.connection(id).unwrap().token,
                },
                "ping_timeout" => {
                    let t = r.state.connections[&id].timer.unwrap();
                    now = now.max(r.state.timers[&t].at);
                    Command::Timeout(t)
                }
                "suspect" => Command::Suspect(id),
                "expire_blocks" => {
                    now = now.max(r.state.next_unblock.unwrap() + 1);
                    Command::IsBlocked(key(0))
                }
                "queue" => {
                    let p = from_json(&row["peer"]);
                    let host = p.host.clone();
                    let started = apply(
                        &mut r,
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
                    );
                    if let Some(token) = started.effects.iter().find_map(|e| match e {
                        Effect::Resolve { token, .. } => Some(*token),
                        _ => None,
                    }) {
                        resolutions.insert(host, token);
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
                    if let Some(h) = r.state.hosts.get(&host) {
                        let timer = h.timer;
                        now = now.max(r.state.timers[&timer].at);
                        let started = apply(&mut r, now, Command::Timeout(timer));
                        let token = started
                            .effects
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
                op => panic!("unhandled native operation {op}"),
            };
            let prepared = r.prepare(now, command).unwrap();
            if let Some(expected) = row.get("metrics") {
                metrics::assert_native(prepared.peer_metrics(), expected);
            }
            let change = r.commit(prepared).unwrap();
            for e in &change.effects {
                if let Effect::Dial { token, peer, .. } = e {
                    let native = row["effects"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|v| v["kind"] == "dial" && v["id"] == number(peer.id))
                        .unwrap();
                    actors.insert(native["actor"].as_u64().unwrap(), *token);
                }
            }
            let observed:Vec<_>=change.effects.iter().filter_map(|e|match e {
                Effect::Store(discovery::Effect::Persist(p))=>Some(json!({"kind":"persist","peer":pj(p)})),
                Effect::Store(discovery::Effect::Delete(p))=>Some(json!({"kind":"delete","peer":pj(p)})),
                Effect::Dial {token,peer,probe}=>Some(json!({"kind":"dial","id":number(peer.id),"actor":actors.iter().find(|(_,t)|**t==*token).unwrap().0,"transport":if probe.is_some(){"tcp"}else{"noise"}})),
                Effect::Ping {peer,..}=>Some(json!({"kind":"ping","id":number(*peer)})),
                Effect::Blocked(id)=>Some(json!({"kind":"blocked","id":number(*id)})),
                Effect::Unblocked(id)=>Some(json!({"kind":"unblocked","id":number(*id)})),
                Effect::Close(_)|Effect::ProbeStatus{..}=>None,
                _=>panic!("unexpected effect in native registry trace"),
            }).collect();
            let expected: Vec<_> = row["effects"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|v| v["kind"] != "close")
                .cloned()
                .collect();
            assert_eq!(
                observed, expected,
                "effects {} step {index} {row}",
                scenario["label"]
            );
            assert_eq!(
                snapshot(&r, &actors),
                row["state"],
                "state {} step {index} {row}",
                scenario["label"]
            );
            if row["op"] == "inbound" {
                let result = match change.result {
                    Outcome::Accepted(Kind::Permanent) => "permanent",
                    Outcome::Accepted(Kind::Temporary) => "temporary",
                    Outcome::Rejected(Rejection::Blocked) => "blocked",
                    Outcome::Rejected(Rejection::AlreadyConnected) => "already_connected",
                    Outcome::Rejected(Rejection::TrustedAddressChanged) => {
                        "trusted_address_changed"
                    }
                    other => panic!("{other:?}"),
                };
                assert_eq!(row["result"], result);
            }
            invariant(&r);
            total += 1;
        }
    }
    assert_eq!(total, expected_steps);
    println!(
        "Original registry: {total} lifecycle steps; owner retirement and repaired socket indices checked separately"
    );
}

#[test]
fn prepared_changes_do_not_publish_or_leak_effects_and_stale_plans_are_rejected() {
    let mut r = registry(config());
    let p = peer(1, false);
    let staged = r.prepare(1, Command::Add(p.clone())).unwrap();
    assert_eq!(staged.persistence().count(), 1);
    assert!(r.pool().find(p.id).is_none());
    assert_eq!(r.revision(), 0);
    let stale = r.prepare(1, Command::Add(peer(2, false))).unwrap();
    r.commit(staged).unwrap();
    assert_eq!(r.commit(stale).unwrap_err(), Error::StalePlan);
    assert!(r.pool().find(key(2)).is_none());
    let other = registry(config())
        .prepare(1, Command::Add(peer(3, false)))
        .unwrap();
    assert_eq!(r.commit(other).unwrap_err(), Error::StalePlan);
    let before = r.state.random.clone().next_u64();
    let sequence = r.state.sequence;
    let failed_write = r.prepare(2, Command::Connect(Some(p.id))).unwrap();
    assert!(matches!(failed_write.result(), Outcome::Selected(_)));
    drop(failed_write);
    assert_eq!(r.state.random.clone().next_u64(), before);
    assert_eq!(r.state.sequence, sequence);
    assert!(r.connection(p.id).is_none());
    assert!(r.pool().state(p.id).unwrap().available);
    let token = match apply(&mut r, 2, Command::Connect(Some(p.id))).result {
        Outcome::Selected(t) => t,
        _ => panic!(),
    };
    let mut foreign = registry(config());
    assert_eq!(
        apply(&mut foreign, 2, Command::Connected { peer: p.id, token }).result,
        Outcome::Stale
    );
    invariant(&r);
    invariant(&foreign);
}

#[test]
fn replaced_connections_ignore_every_stale_callback_and_repair_failed_start_reservations() {
    let mut r = registry(config());
    let p = peer(200, false);
    let old = add_connected(&mut r, 1, p.clone());
    let fresh = reserve(&mut r, 2, p.address);
    assert_eq!(
        apply(
            &mut r,
            2,
            Command::Accept {
                token: fresh,
                peer: Info::from(&p)
            }
        )
        .result,
        Outcome::Accepted(Kind::Permanent)
    );
    for c in [
        Command::Alive {
            peer: p.id,
            token: old,
        },
        Command::Connected {
            peer: p.id,
            token: old,
        },
        Command::PingDone {
            peer: p.id,
            token: old,
        },
        Command::Failed {
            peer: p.id,
            token: old,
            reason: Failure::Down,
        },
        Command::Failed {
            peer: p.id,
            token: old,
            reason: Failure::Dead,
        },
        Command::Failed {
            peer: p.id,
            token: old,
            reason: Failure::Closed,
        },
    ] {
        let changed = apply(&mut r, 3, c);
        assert_eq!(changed.result, Outcome::Stale);
        assert!(changed.effects.is_empty());
        assert_eq!(r.connection(p.id).unwrap().token, fresh);
    }
    assert_eq!(
        apply(
            &mut r,
            3,
            Command::Accept {
                token: fresh,
                peer: Info::from(&p)
            }
        )
        .result,
        Outcome::Stale
    );
    apply(&mut r, 4, Command::Delete(p.id));
    apply(&mut r, 5, Command::Add(p.clone()));
    let attempt = match apply(&mut r, 5, Command::Connect(Some(p.id))).result {
        Outcome::Selected(t) => t,
        _ => panic!(),
    };
    apply(
        &mut r,
        6,
        Command::Failed {
            peer: p.id,
            token: attempt,
            reason: Failure::Start,
        },
    );
    assert!(r.connection(p.id).is_none());
    assert!(r.pool().state(p.id).unwrap().available);
    let probe = match apply(&mut r, 7, Command::Probe(Class::Unverified)).result {
        Outcome::Selected(t) => t,
        other => panic!("{other:?}"),
    };
    apply(
        &mut r,
        8,
        Command::Alive {
            peer: p.id,
            token: probe,
        },
    );
    assert_eq!(r.probe_status(p.id), Some(true));
    apply(&mut r, 9, Command::Delete(p.id));
    assert_eq!(r.probe_status(p.id), None);
    assert_eq!(r.socket_peers(p.socket()).count(), 0);
    invariant(&r);
}

#[test]
fn temporary_inbound_and_pending_admission_have_owned_caps_and_deadlines() {
    let mut c = config();
    c.maximum_inbound = 0;
    c.maximum_temporary = 1;
    c.maximum_pending_inbound = 1;
    c.maximum_connections = 2;
    c.admission_timeout = 10;
    let mut r = registry(c);
    let p = peer(1, false);
    let token = reserve(&mut r, 0, p.address);
    assert!(matches!(
        r.prepare(0, Command::ReserveInbound { address: p.address }),
        Err(Error::ResourceLimit)
    ));
    let timer = r.state.pending[&token].timer;
    assert_eq!(
        apply(&mut r, 9, Command::Timeout(timer)).result,
        Outcome::Ignored
    );
    assert_eq!(
        apply(
            &mut r,
            9,
            Command::Accept {
                token,
                peer: Info::from(&p)
            }
        )
        .result,
        Outcome::Accepted(Kind::Temporary)
    );
    assert_eq!(
        apply(&mut r, 10, Command::Timeout(timer)).result,
        Outcome::Stale
    );
    let q = peer(2, false);
    let second = reserve(&mut r, 10, q.address);
    assert_eq!(
        apply(
            &mut r,
            10,
            Command::Accept {
                token: second,
                peer: Info::from(&q)
            }
        )
        .result,
        Outcome::Rejected(Rejection::Capacity)
    );
    assert!(r.pool().find(q.id).is_none());
    let close = apply(&mut r, 11, Command::PingDone { peer: p.id, token });
    assert!(close.effects.contains(&Effect::Close(token)));
    assert!(r.pool().state(p.id).unwrap().available);
    let third = reserve(&mut r, 12, q.address);
    let deadline = r.state.pending[&third].timer;
    assert!(
        apply(&mut r, 22, Command::Timeout(deadline))
            .effects
            .contains(&Effect::Close(third))
    );
    assert_eq!(
        apply(
            &mut r,
            22,
            Command::Accept {
                token: third,
                peer: Info::from(&q)
            }
        )
        .result,
        Outcome::Stale
    );
    invariant(&r);
}

#[test]
fn lazy_batch_unblock_is_strict_and_sampling_is_bounded_unique_and_filtered() {
    let mut c = config();
    c.unblock_interval = 10;
    c.maximum_outbound = 0;
    let mut r = registry(c);
    let p = peer(1, false);
    apply(&mut r, 1, Command::Block(Info::from(&p)));
    apply(&mut r, 2, Command::Delete(p.id));
    assert_eq!(
        apply(&mut r, 11, Command::IsBlocked(p.id)).result,
        Outcome::Bool(true)
    );
    assert_eq!(
        apply(&mut r, 12, Command::IsBlocked(p.id)).result,
        Outcome::Bool(false)
    );
    for n in 1..=8 {
        let p = peer(n, false);
        let token = reserve(&mut r, 12, p.address);
        apply(
            &mut r,
            12,
            Command::Accept {
                token,
                peer: Info::from(&p),
            },
        );
    }
    for percent in [None, Some(0), Some(50), Some(100)] {
        assert_eq!(
            apply(
                &mut r,
                13,
                Command::Sample {
                    maximum: 0,
                    percent_connected: percent,
                    exclude: vec![]
                }
            )
            .result,
            Outcome::Peers(vec![])
        );
        let Outcome::Peers(peers) = apply(
            &mut r,
            13,
            Command::Sample {
                maximum: usize::MAX,
                percent_connected: percent,
                exclude: vec![key(1), key(2)],
            },
        )
        .result
        else {
            panic!()
        };
        assert!(peers.len() <= 6);
        let ids: BTreeSet<_> = peers.iter().map(|p| p.id).collect();
        assert_eq!(ids.len(), peers.len());
        assert!(!ids.contains(&key(1)) && !ids.contains(&key(2)));
    }
    assert_eq!(
        apply(
            &mut r,
            14,
            Command::SampleConnected {
                maximum: 8,
                direction: Some(Direction::Outbound)
            }
        )
        .result,
        Outcome::Peers(vec![])
    );
    invariant(&r);
    let mut multiple = false;
    for _ in 0..100 {
        let Outcome::Peers(peers) = apply(
            &mut r,
            14,
            Command::Sample {
                maximum: 1000,
                percent_connected: Some(1),
                exclude: vec![],
            },
        )
        .result
        else {
            panic!()
        };
        multiple |= peers.len() > 1;
    }
    assert!(
        multiple,
        "the connected target uses the requested sample size before clamping results to current membership"
    );
}

#[test]
fn resolution_jobs_coalesce_bound_retry_and_exclude_late_results_after_timeout_or_replacement() {
    let mut c = config();
    c.maximum_hosts = 1;
    c.maximum_peers_per_host = 2;
    c.maximum_resolving_peers = 2;
    c.maximum_outbound = 0;
    c.resolver_retries = 1;
    c.resolver_backoff = vec![5, 15];
    c.resolution_timeout = 4;
    let mut r = registry(c);
    let p = Info {
        id: key(1),
        host: b"shared".to_vec(),
        port: 3001,
    };
    let q = Info {
        id: key(2),
        ..p.clone()
    };
    let begin = apply(
        &mut r,
        0,
        Command::Resolve {
            peer: p.clone(),
            source: Some(Ipv4Addr::LOCALHOST),
            trusted: false,
        },
    );
    let Effect::Resolve { token: first, .. } = begin.effects[0] else {
        panic!()
    };
    assert!(
        apply(
            &mut r,
            0,
            Command::Resolve {
                peer: q.clone(),
                source: None,
                trusted: true
            }
        )
        .effects
        .is_empty()
    );
    assert!(matches!(
        r.prepare(
            0,
            Command::Resolve {
                peer: Info {
                    id: key(3),
                    ..p.clone()
                },
                source: None,
                trusted: false
            }
        ),
        Err(Error::ResourceLimit)
    ));
    assert!(matches!(
        r.prepare(
            0,
            Command::Resolve {
                peer: Info {
                    host: b"second".to_vec(),
                    ..p.clone()
                },
                source: None,
                trusted: false
            }
        ),
        Err(Error::ResourceLimit)
    ));
    let timeout = r.state.hosts[&p.host].timer;
    let done = apply(&mut r, 4, Command::Timeout(timeout));
    assert!(done.effects.contains(&Effect::CancelResolve(first)));
    assert_eq!(r.state.hosts[&p.host].retries, 1);
    let retry = r.state.hosts[&p.host].timer;
    assert_eq!(r.state.timers[&retry].at, 9);
    assert_eq!(
        apply(
            &mut r,
            5,
            Command::Resolved {
                token: first,
                address: Some(Ipv4Addr::LOCALHOST)
            }
        )
        .result,
        Outcome::Stale
    );
    let started = apply(&mut r, 9, Command::Timeout(retry));
    let Effect::Resolve { token: second, .. } = started.effects[0] else {
        panic!()
    };
    apply(
        &mut r,
        10,
        Command::Resolved {
            token: second,
            address: None,
        },
    );
    assert_eq!(r.state.hosts[&p.host].peers.len(), 1);
    assert!(r.state.hosts[&p.host].peers.contains_key(&q.id));
    apply(
        &mut r,
        10,
        Command::Resolve {
            peer: p.clone(),
            source: None,
            trusted: false,
        },
    );
    assert_eq!(r.state.hosts[&p.host].peers.len(), 1);
    let retry = r.state.hosts[&p.host].timer;
    assert_eq!(r.state.timers[&retry].at, 25);
    let started = apply(&mut r, 25, Command::Timeout(retry));
    let Effect::Resolve { token: third, .. } = started.effects[0] else {
        panic!()
    };
    apply(
        &mut r,
        25,
        Command::Resolved {
            token: third,
            address: Some(Ipv4Addr::new(1, 2, 3, 4)),
        },
    );
    assert!(r.pool().find(p.id).is_none());
    let found = r.pool().find(q.id).unwrap();
    assert!(found.trusted);
    assert_eq!(found.source, found.address);
    assert_eq!(r.counts().hosts, 0);
    let begin = apply(
        &mut r,
        26,
        Command::Resolve {
            peer: p.clone(),
            source: Some(Ipv4Addr::LOCALHOST),
            trusted: false,
        },
    );
    let Effect::Resolve { token: fourth, .. } = begin.effects[0] else {
        panic!()
    };
    assert_eq!(
        apply(
            &mut r,
            26,
            Command::Resolved {
                token: third,
                address: None
            }
        )
        .result,
        Outcome::Stale
    );
    apply(
        &mut r,
        27,
        Command::Resolved {
            token: fourth,
            address: Some(Ipv4Addr::new(5, 6, 7, 8)),
        },
    );
    assert_eq!(r.pool().find(p.id).unwrap().source, Ipv4Addr::LOCALHOST);
    invariant(&r);
}

#[test]
fn timers_preserve_backoff_jitter_and_owner_generation_with_checked_time_and_capacity() {
    let mut c = config();
    c.maximum_outbound = 6;
    c.maximum_connections = 16;
    c.attempt_timeout = 10;
    let mut r = registry(c);
    for n in 1..=5 {
        apply(&mut r, n, Command::Add(peer(n, true)));
        let timer = r.state.connect_timer.unwrap();
        assert_eq!(r.state.timers[&timer].at, n + (1000u64 << (n - 1)));
    }
    let conn = r.connection(key(1)).unwrap().clone();
    let timer = r.state.connections[&key(1)].timer.unwrap();
    apply(
        &mut r,
        6,
        Command::Connected {
            peer: key(1),
            token: conn.token,
        },
    );
    assert_eq!(
        apply(&mut r, 11, Command::Timeout(timer)).result,
        Outcome::Stale
    );
    apply(
        &mut r,
        11,
        Command::PingDone {
            peer: key(1),
            token: conn.token,
        },
    );
    let first = r.state.connections[&key(1)].timer.unwrap();
    apply(
        &mut r,
        12,
        Command::PingDone {
            peer: key(1),
            token: conn.token,
        },
    );
    assert_eq!(
        apply(&mut r, 12, Command::Timeout(first)).result,
        Outcome::Stale
    );
    let revision = r.revision();
    assert!(matches!(
        r.prepare(1, Command::Delete(key(1))),
        Err(Error::TimeReversed)
    ));
    assert!(matches!(
        r.prepare(u64::MAX, Command::Delete(key(1))),
        Err(Error::TimeOverflow)
    ));
    assert_eq!(revision, r.revision());
    let mut c = config();
    c.analytics = true;
    let mut monitor = registry(c);
    apply(&mut monitor, 0, Command::Add(peer(1, true)));
    let timer = monitor.state.connect_timer.unwrap();
    assert!((101..=150).contains(&monitor.state.timers[&timer].at));
    monitor.state.sequence = u64::MAX;
    assert!(matches!(
        monitor.prepare(
            1,
            Command::ReserveInbound {
                address: Ipv4Addr::LOCALHOST
            }
        ),
        Err(Error::ResourceLimit)
    ));
    let mut c = config();
    c.resolver_backoff.clear();
    assert!(matches!(
        Registry::new(key(1), c),
        Err(Error::Configuration)
    ));
    invariant(&r);
}

#[test]
fn randomized_registry_churn_keeps_owned_resources_indices_and_persistence_coherent() {
    let mut c = config();
    c.pool.verified_size = 4;
    c.pool.unverified_size = 4;
    c.pool.standby_times = vec![1, 3];
    c.pool.maximum_update_lapse = 10;
    c.maximum_outbound = 3;
    c.maximum_inbound = 2;
    c.maximum_temporary = 2;
    c.maximum_pending_inbound = 3;
    c.maximum_connections = 8;
    c.maximum_blocked = 8;
    c.maximum_hosts = 3;
    c.maximum_peers_per_host = 3;
    c.maximum_resolving_peers = 6;
    c.resolver_backoff = vec![2, 4];
    c.resolution_timeout = 3;
    c.admission_timeout = 4;
    c.attempt_timeout = 5;
    c.ping_interval = 3;
    c.unblock_interval = 7;
    let mut r = registry(c);
    let mut rng = StdRng::seed_from_u64(212);
    let mut persisted = BTreeMap::new();
    let mut now = 0;
    let mut old_tokens = vec![];
    for _ in 0..5000 {
        now += 1;
        let n = rng.gen_range(1..=12);
        let p = peer(n, rng.gen_ratio(1, 8));
        let current = r.connection(p.id).cloned();
        let command = match rng.gen_range(0..17) {
            0 => Command::Add(p),
            1 => Command::Delete(p.id),
            2 => Command::Block(Info::from(&p)),
            3 => Command::Unblock(p.id),
            4 => Command::Connect(if rng.gen_bool(0.5) { Some(p.id) } else { None }),
            5 => Command::Probe(if rng.gen_bool(0.5) {
                Class::Verified
            } else {
                Class::Unverified
            }),
            6 => Command::ReserveInbound { address: p.address },
            7 => r
                .state
                .pending
                .keys()
                .next()
                .map_or(Command::IsBlocked(p.id), |&token| Command::Accept {
                    token,
                    peer: Info::from(&p),
                }),
            8 => current.map_or(Command::IsBlocked(p.id), |c| Command::Connected {
                peer: p.id,
                token: c.token,
            }),
            9 => current.map_or(Command::IsBlocked(p.id), |c| Command::Alive {
                peer: p.id,
                token: c.token,
            }),
            10 => current.map_or(Command::IsBlocked(p.id), |c| Command::Failed {
                peer: p.id,
                token: c.token,
                reason: [
                    Failure::Start,
                    Failure::Failed,
                    Failure::Closed,
                    Failure::Down,
                    Failure::Dead,
                ][rng.gen_range(0..5)],
            }),
            11 => current.map_or(Command::IsBlocked(p.id), |c| Command::PingDone {
                peer: p.id,
                token: c.token,
            }),
            12 => {
                if let Some(t) = r.timers().min_by_key(|t| t.deadline) {
                    now = now.max(t.deadline);
                    Command::Timeout(t.token)
                } else {
                    Command::IsBlocked(p.id)
                }
            }
            13 => Command::Resolve {
                peer: Info {
                    host: format!("host{}", n % 3).into_bytes(),
                    ..Info::from(&p)
                },
                source: None,
                trusted: p.trusted,
            },
            14 => r.state.hosts.values().find_map(|h| h.attempt).map_or(
                Command::IsBlocked(p.id),
                |token| Command::Resolved {
                    token,
                    address: if rng.gen_bool(0.5) {
                        Some(p.address)
                    } else {
                        None
                    },
                },
            ),
            15 => old_tokens
                .choose(&mut rng)
                .map_or(Command::IsBlocked(p.id), |&token| Command::Failed {
                    peer: p.id,
                    token,
                    reason: Failure::Down,
                }),
            _ => Command::Suspect(p.id),
        };
        let revision = r.revision();
        match r.prepare(now, command) {
            Ok(prepared) => {
                let change = r.commit(prepared).unwrap();
                for e in change.effects {
                    match e {
                        Effect::Store(discovery::Effect::Persist(p)) => {
                            persisted.insert(p.id, p);
                        }
                        Effect::Store(discovery::Effect::Delete(p)) => {
                            persisted.remove(&p.id);
                        }
                        Effect::Dial { token, .. } => old_tokens.push(token),
                        _ => {}
                    }
                }
                if let Outcome::Reserved(token) = change.result {
                    old_tokens.push(token);
                }
            }
            Err(e) => {
                assert_eq!(e, Error::ResourceLimit);
                assert_eq!(revision, r.revision());
            }
        }
        invariant(&r);
        let actual: BTreeMap<_, _> = r
            .pool()
            .all(Target::Both)
            .filter(|p| !p.trusted)
            .map(|p| (p.id, p.clone()))
            .collect();
        assert_eq!(persisted, actual);
    }
    let shutdown = apply(&mut r, now + 1, Command::Shutdown);
    assert!(r.is_closed());
    assert_eq!(r.connections().count(), 0);
    assert_eq!(r.timers().count(), 0);
    assert_eq!(r.counts().hosts, 0);
    assert_eq!(r.counts().pending_inbound, 0);
    assert!(
        shutdown
            .effects
            .iter()
            .all(|e| matches!(e, Effect::Close(_) | Effect::CancelResolve(_)))
    );
    assert!(matches!(
        r.prepare(now + 2, Command::Connect(None)),
        Err(Error::Closed)
    ));
    invariant(&r);
}
