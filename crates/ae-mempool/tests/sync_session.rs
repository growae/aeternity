use ae_mempool::sync::{Snapshot, Unfold, session::*};
use ae_trie::Nibbles;
use ae_types::Hash256;
use serde_json::{Value as J, json};
use std::{collections::BTreeMap, sync::Arc};

fn peer(n: u64) -> Hash256 {
    let mut bytes = [0; 32];
    bytes[24..].copy_from_slice(&n.to_be_bytes());
    Hash256(bytes)
}
fn number(p: Hash256) -> u64 {
    u64::from_be_bytes(p.0[24..].try_into().unwrap())
}
fn tree() -> Arc<Snapshot> {
    Arc::new(Snapshot::build([peer(1)], Default::default()).unwrap())
}
fn path(s: &str) -> Nibbles {
    Nibbles::new(s.chars().map(|c| c.to_digit(16).unwrap() as u8).collect()).unwrap()
}
fn text(p: &Nibbles) -> String {
    p.as_slice()
        .iter()
        .map(|n| char::from_digit(u32::from(*n), 16).unwrap())
        .collect()
}
fn unfold(j: &J) -> Unfold {
    match j["kind"].as_str().unwrap() {
        "node" => Unfold::Node {
            path: path(j["path"].as_str().unwrap()),
            node: hex::decode(j["node"].as_str().unwrap()).unwrap(),
        },
        "leaf" => Unfold::Leaf(path(j["path"].as_str().unwrap())),
        "subtree" => Unfold::Subtree(path(j["path"].as_str().unwrap())),
        "key" => Unfold::Key(hex::decode(j["key"].as_str().unwrap()).unwrap()),
        _ => panic!("bad fixture unfold"),
    }
}
fn external(u: &Unfold) -> J {
    match u {
        Unfold::Node { path, node } => {
            json!({"kind":"node","path":text(path),"node":hex::encode(node)})
        }
        Unfold::Leaf(p) => json!({"kind":"leaf","path":text(p)}),
        Unfold::Subtree(p) => json!({"kind":"subtree","path":text(p)}),
        Unfold::Key(k) => json!({"kind":"key","key":hex::encode(k)}),
    }
}
fn kind(w: &Work) -> &'static str {
    match w {
        Work::Tree => "tree",
        Work::Init => "init",
        Work::Unfold { .. } => "unfold",
        Work::Get(_) => "get",
        Work::Finish(_) => "finish",
    }
}
fn effects(es: &[Effect]) -> J {
    json!(es.iter().filter_map(|e| if let Effect::Start(a) = e {
        let mut row = json!({"peer":number(a.peer),"kind":kind(&a.work),"timeout":a.work.timeout_ms()});
        match a.work.as_ref() {
            Work::Unfold { request, delay_ms } => { row["request"] = json!(request.iter().map(external).collect::<Vec<_>>()); row["delay"] = json!(delay_ms); }
            Work::Get(keys) => row["keys"] = json!(keys.iter().map(text).collect::<Vec<_>>()),
            Work::Finish(done) => row["done"] = json!(done),
            _ => {}
        }
        Some(row)
    } else { None }).collect::<Vec<_>>())
}
fn sync_state(s: &Session) -> J {
    json!({"peer":number(s.peer()),"connection":s.connection(),"tree":s.tree().is_some(),"gets":s.gets().map(|ks| ks.iter().map(text).collect::<Vec<_>>())})
}
fn state(m: &Manager) -> J {
    let local = match m.local() {
        Local::Idle => json!({"phase":"idle"}),
        Local::Cooldown(_) => json!({"phase":"cooldown"}),
        Local::Active(s) => {
            let mut row = sync_state(s);
            row["phase"] = json!("active");
            row
        }
    };
    json!({"local":local,"remote":m.incoming().iter().map(sync_state).collect::<Vec<_>>()})
}
fn selected<'a>(m: &'a Manager, p: &J) -> Option<&'a Session> {
    if p == "local" {
        if let Local::Active(s) = m.local() {
            return Some(s);
        }
    } else if let Some(n) = p.as_u64() {
        return m.incoming().iter().find(|s| s.peer() == peer(n));
    }
    None
}
fn reply(r: &J) -> Result<Reply, Failure> {
    if r == "tree" {
        Ok(Reply::Tree(tree()))
    } else if r == "init" {
        Ok(Reply::Init)
    } else if r == "error" {
        Err(Failure::Action)
    } else if let Some(us) = r["unfold"].as_array() {
        Ok(Reply::Unfold(us.iter().map(unfold).collect()))
    } else if r.get("not_ready").is_some() {
        Ok(Reply::TreeNotReady)
    } else if let Some(n) = r["get"].as_u64() {
        Ok(Reply::Fetched(n as usize))
    } else if r.get("finish").is_some() {
        Ok(Reply::Finished)
    } else {
        panic!("bad fixture reply: {r}")
    }
}
fn request<T>(r: Result<T, RequestError>) -> &'static str {
    match r {
        Ok(_) => "ok",
        Err(RequestError::TooManySyncs) => "too_many_simultaneous_sync_attempts",
        Err(RequestError::AlreadySyncing) => "already_syncing_with_peer",
        Err(RequestError::NoActiveSync) => "no_active_sync",
        Err(RequestError::TreeNotReady) => "tree_not_ready",
        Err(RequestError::Protocol(_)) => "protocol_error",
    }
}
fn pending(m: &Manager) -> Action {
    if let Local::Active(s) = m.local() {
        s.pending().unwrap().clone()
    } else {
        panic!("not active")
    }
}
fn complete(m: &mut Manager, reply: Reply) -> Transition {
    let a = pending(m);
    m.complete(&a.token, Ok(reply))
}
fn ready(m: &mut Manager) {
    m.connect(peer(1), 1);
    complete(m, Reply::Tree(tree()));
    complete(m, Reply::Init);
}
fn foreign() -> Token {
    let mut m = Manager::default();
    m.connect(peer(99), 99);
    pending(&m).token
}

#[test]
fn original_callback_traces_match_with_explicit_lifecycle_repairs() {
    let data: J =
        serde_json::from_str(include_str!("fixtures/erlang_pool_sync_session.json")).unwrap();
    assert_eq!(data["resync_ms"], RESYNC_MS);
    let mut count = 0;
    for case in data["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let mut m = Manager::default();
        let stale = foreign();
        let mut retired = BTreeMap::new();
        for (index, row) in case["steps"].as_array().unwrap().iter().enumerate() {
            count += 1;
            let op = &row["input"];
            let mut answer = "ok";
            let es = match op["op"].as_str().unwrap() {
                "connect" => m.connect(
                    peer(op["peer"].as_u64().unwrap()),
                    op["connection"].as_u64().unwrap(),
                ),
                "accept" => match m.accept(
                    peer(op["peer"].as_u64().unwrap()),
                    op["connection"].as_u64().unwrap(),
                ) {
                    Ok(es) => es,
                    Err(e) => {
                        answer = request::<()>(Err(e));
                        vec![]
                    }
                },
                "complete" => {
                    let key = (op["peer"].to_string(), op["action"].to_string());
                    let token = selected(&m, &op["peer"])
                        .and_then(Session::pending)
                        .filter(|a| kind(&a.work) == op["action"].as_str().unwrap())
                        .map(|a| a.token.clone())
                        .or_else(|| retired.get(&key).cloned())
                        .unwrap_or_else(|| stale.clone());
                    retired.insert(key, token.clone());
                    m.complete(&token, reply(&op["result"])).effects
                }
                "down" => {
                    let monitor = selected(&m, &op["peer"])
                        .map(|s| s.monitor().clone())
                        .unwrap_or_else(|| stale.clone());
                    m.connection_down(&monitor, op["replacement"].as_u64())
                }
                "timeout" => {
                    let token = selected(&m, &op["peer"])
                        .and_then(Session::pending)
                        .map(|a| a.token.clone())
                        .unwrap_or_else(|| stale.clone());
                    m.timeout(&token).effects
                }
                "resync" => {
                    let token = match m.local() {
                        Local::Cooldown(t) if op["stale"] == false => t.clone(),
                        _ => stale.clone(),
                    };
                    m.resync(&token);
                    vec![]
                }
                "remote_finish" => m.finish_remote(peer(op["peer"].as_u64().unwrap())),
                "get" => {
                    answer = request(m.authorize_get(peer(op["peer"].as_u64().unwrap())));
                    vec![]
                }
                "unfold" => {
                    answer = request(m.respond_wire(peer(op["peer"].as_u64().unwrap()), &[]));
                    vec![]
                }
                _ => panic!("bad fixture operation"),
            };
            let mut expected_state = row["state"].clone();
            let mut expected_effects = row["effects"].clone();
            let mut expected_reply = row["reply"].clone();
            // Original /4 callback drops incoming builds when local=not_synced.
            // Rust fixes this starvation, and consumes each completion only once.
            if name == "incoming_idle" && (3..=5).contains(&index) {
                expected_state["remote"][0]["tree"] = json!(true);
            }
            if name == "incoming_idle" && index == 4 {
                expected_reply = json!("ok");
            }
            // A failing finish in the reference schedules another finish forever.
            // Rust terminates this best-effort action and releases its monitor.
            if name == "failure_finish" && index == 5 {
                expected_state["local"] = json!({"phase":"idle"});
                expected_effects = json!([]);
            }
            assert_eq!(state(&m), expected_state, "{name} step {index} {op}");
            assert_eq!(effects(&es), expected_effects, "{name} step {index} {op}");
            assert_eq!(answer, expected_reply, "{name} step {index}");
        }
        m.shutdown();
    }
    assert_eq!(count, 126);
}

#[test]
fn incoming_builds_are_independent_authorized_and_released_on_finish_or_failure() {
    let mut m = Manager::default();
    assert_eq!(m.authorize_get(peer(2)), Err(RequestError::NoActiveSync));
    m.accept(peer(2), 22).unwrap();
    assert!(m.authorize_get(peer(2)).is_ok());
    assert_eq!(
        m.respond_wire(peer(2), &[]),
        Err(RequestError::TreeNotReady)
    );
    let s = &m.incoming()[0];
    let action = s.pending().unwrap().clone();
    let monitor = s.monitor().clone();
    m.complete(&action.token, Ok(Reply::Tree(tree())));
    assert!(m.respond_wire(peer(2), &[]).unwrap().is_empty());
    assert!(
        m.complete(&action.token, Err(Failure::Action))
            .effects
            .is_empty()
    );
    assert!(m.incoming()[0].tree().is_some());
    let es = m.finish_remote(peer(2));
    assert!(matches!(es.as_slice(),[Effect::Unwatch(t)] if *t==monitor));
    assert_eq!(
        m.respond_wire(peer(2), &[]),
        Err(RequestError::NoActiveSync)
    );
    m.accept(peer(2), 23).unwrap();
    assert!(
        m.complete(&action.token, Ok(Reply::Tree(tree())))
            .effects
            .is_empty()
    );
    let action = m.incoming()[0].pending().unwrap().clone();
    let tr = m.timeout(&action.token);
    assert_eq!(tr.failure, Some(Failure::Timeout));
    assert!(m.incoming().is_empty());
    assert!(matches!(
        tr.effects.as_slice(),
        [Effect::CancelAction(_), Effect::Unwatch(_)]
    ));
}

#[test]
fn retired_actions_and_replaced_monitors_cannot_advance_a_session() {
    let mut m = Manager::default();
    m.connect(peer(1), 1);
    let first = pending(&m);
    let old_monitor = if let Local::Active(s) = m.local() {
        s.monitor().clone()
    } else {
        unreachable!()
    };
    m.connection_down(&old_monitor, Some(2));
    assert_eq!(pending(&m).token, first.token);
    assert!(m.connection_down(&old_monitor, None).is_empty());
    m.complete(&first.token, Ok(Reply::Tree(tree())));
    let init = pending(&m);
    assert!(m.timeout(&first.token).effects.is_empty());
    assert!(m.complete(&foreign(), Ok(Reply::Init)).effects.is_empty());
    assert_eq!(pending(&m).token, init.token);
    let bad = m.complete(&init.token, Ok(Reply::Fetched(10)));
    assert_eq!(bad.failure, Some(Failure::InvalidReply));
    assert_eq!(*pending(&m).work, Work::Finish(false));
    let tr = m.timeout(&pending(&m).token);
    assert_eq!(tr.failure, Some(Failure::Timeout));
    assert!(matches!(m.local(), Local::Idle));
    assert!(matches!(
        tr.effects.as_slice(),
        [Effect::CancelAction(_), Effect::Unwatch(_)]
    ));
    m.connect(peer(1), 1);
    assert!(
        m.complete(&first.token, Ok(Reply::Tree(tree())))
            .effects
            .is_empty()
    );
}

#[test]
fn success_uses_get_response_count_and_cooldown_capability_then_shutdown_cleans_all() {
    let mut m = Manager::default();
    ready(&mut m);
    complete(&mut m, Reply::Unfold(vec![]));
    assert_eq!(*pending(&m).work, Work::Get(vec![]));
    complete(&mut m, Reply::Fetched(10)); // Count need not equal admissions or requested hashes.
    assert_eq!(*pending(&m).work, Work::Finish(true));
    let tr = complete(&mut m, Reply::Finished);
    let timer = if let Local::Cooldown(t) = m.local() {
        t.clone()
    } else {
        panic!()
    };
    assert!(
        matches!(tr.effects.last(),Some(Effect::Resync{token,delay_ms:RESYNC_MS}) if *token==timer)
    );
    assert!(!m.resync(&foreign()));
    assert!(m.connect(peer(2), 2).is_empty());
    m.accept(peer(2), 2).unwrap();
    let tr = m.shutdown();
    assert!(
        matches!(tr.as_slice(),[Effect::CancelResync(t),Effect::CancelAction(_),Effect::Unwatch(_)] if *t==timer)
    );
    assert!(!m.resync(&timer));
    assert!(m.shutdown().is_empty());
    ready(&mut m);
    complete(&mut m, Reply::Unfold(vec![]));
    complete(&mut m, Reply::Fetched(0));
    complete(&mut m, Reply::Finished);
    let timer = if let Local::Cooldown(t) = m.local() {
        t.clone()
    } else {
        panic!()
    };
    assert!(m.resync(&timer));
    assert!(!m.resync(&timer));
    assert!(!m.connect(peer(3), 3).is_empty());
}

#[test]
fn cumulative_keys_rounds_and_incoming_snapshots_have_independent_resource_limits() {
    let mut m = Manager::new(Limits {
        get_items: 2,
        get_bytes: 64,
        ..Default::default()
    });
    ready(&mut m);
    let more = vec![
        Unfold::Key(peer(2).0.to_vec()),
        Unfold::Node {
            path: path(""),
            node: vec![],
        },
    ];
    complete(&mut m, Reply::Unfold(more.clone()));
    complete(&mut m, Reply::Unfold(more));
    let tr = complete(&mut m, Reply::Unfold(vec![Unfold::Key(peer(3).0.to_vec())]));
    assert_eq!(tr.failure, Some(Failure::ResourceLimit));
    assert_eq!(*pending(&m).work, Work::Finish(false));
    let mut m = Manager::new(Limits {
        rounds: 2,
        ..Default::default()
    });
    ready(&mut m);
    complete(&mut m, Reply::TreeNotReady);
    let tr = complete(&mut m, Reply::TreeNotReady);
    assert_eq!(tr.failure, Some(Failure::ResourceLimit));
    let mut m = Manager::new(Limits {
        get_bytes: 31,
        ..Default::default()
    });
    ready(&mut m);
    let tr = complete(&mut m, Reply::Unfold(vec![Unfold::Key(peer(2).0.to_vec())]));
    assert_eq!(tr.failure, Some(Failure::ResourceLimit));
    let mut limits = Limits::default();
    limits.exchange.tree_bytes = 0;
    let mut m = Manager::new(limits);
    m.accept(peer(2), 2).unwrap();
    let token = m.incoming()[0].pending().unwrap().token.clone();
    assert_eq!(
        m.complete(&token, Ok(Reply::Tree(tree()))).failure,
        Some(Failure::ResourceLimit)
    );
    assert!(m.incoming().is_empty());
    m.accept(peer(2), 2).unwrap(); // Failure releases capacity for retry.
    let mut m = Manager::new(Limits {
        incoming: 100,
        ..Default::default()
    });
    for p in 1..=5 {
        m.accept(peer(p), p).unwrap();
    }
    assert_eq!(
        request(m.accept(peer(1), 1)),
        "too_many_simultaneous_sync_attempts"
    );
}

#[test]
fn malformed_responses_abort_and_odd_get_paths_fail_at_binary_encoding() {
    let mut m = Manager::default();
    ready(&mut m);
    let tr = complete(&mut m, Reply::Unfold(vec![Unfold::Subtree(path(""))]));
    assert_eq!(
        tr.failure,
        Some(Failure::Protocol(ae_mempool::sync::Error::InvalidMessage))
    );
    assert_eq!(*pending(&m).work, Work::Finish(false));
    let mut m = Manager::default();
    ready(&mut m);
    complete(&mut m, Reply::Unfold(vec![Unfold::Leaf(path("1"))]));
    let action = pending(&m);
    let Work::Get(keys) = action.work.as_ref() else {
        panic!()
    };
    assert!(encode_get(keys, m.limits()).is_err());
    let keys = [path(""), path("12"), path("12")];
    assert_eq!(
        encode_get(&keys, Limits::default()).unwrap(),
        vec![vec![], vec![0x12], vec![0x12]]
    );
    assert_eq!(
        encode_get(
            &keys,
            Limits {
                get_bytes: 1,
                ..Default::default()
            }
        ),
        Err(Failure::ResourceLimit)
    );
}

#[test]
fn two_session_managers_complete_every_native_exchange_through_wire_boundaries() {
    let data: J = serde_json::from_str(include_str!("fixtures/erlang_pool_sync.json")).unwrap();
    for row in data["exchanges"].as_array().unwrap() {
        let build = |name: &str| {
            Arc::new(
                Snapshot::build(
                    row[name].as_array().unwrap().iter().map(|h| {
                        Hash256::try_from(hex::decode(h.as_str().unwrap()).unwrap().as_slice())
                            .unwrap()
                    }),
                    Default::default(),
                )
                .unwrap(),
            )
        };
        let mut client = Manager::default();
        let mut server = Manager::default();
        client.connect(peer(2), 2);
        let local = build("local");
        let remote = build("remote");
        let mut get_keys = vec![];
        let mut retried = false;
        for _ in 0..100 {
            let a = pending(&client);
            let response = match a.work.as_ref() {
                Work::Tree => Reply::Tree(local.clone()),
                Work::Init => {
                    server.accept(peer(1), 1).unwrap();
                    Reply::Init
                }
                Work::Unfold { request, .. } => {
                    let wire = ae_mempool::sync::encode_batch(request, Default::default()).unwrap();
                    match server.respond_wire(peer(1), &wire) {
                        Err(RequestError::TreeNotReady) => {
                            assert!(!retried);
                            retried = true;
                            let token = server.incoming()[0].pending().unwrap().token.clone();
                            server.complete(&token, Ok(Reply::Tree(remote.clone())));
                            Reply::TreeNotReady
                        }
                        Ok(response) => Reply::Unfold(
                            ae_mempool::sync::decode_batch(&response, Default::default()).unwrap(),
                        ),
                        other => panic!("{other:?}"),
                    }
                }
                Work::Get(keys) => {
                    server.authorize_get(peer(1)).unwrap();
                    get_keys = keys.iter().map(text).collect::<Vec<_>>();
                    let wire = encode_get(keys, client.limits()).unwrap();
                    assert!(wire.iter().all(|k| {
                        row["remote"]
                            .as_array()
                            .unwrap()
                            .contains(&json!(hex::encode(k)))
                    }));
                    Reply::Fetched(wire.len())
                }
                Work::Finish(true) => {
                    server.finish_remote(peer(1));
                    Reply::Finished
                }
                Work::Finish(false) => panic!("exchange failed"),
            };
            assert!(client.complete(&a.token, Ok(response)).failure.is_none());
            if matches!(client.local(), Local::Cooldown(_)) {
                break;
            }
        }
        assert!(retried);
        assert!(matches!(client.local(), Local::Cooldown(_)));
        assert!(server.incoming().is_empty());
        assert_eq!(json!(get_keys), row["gets"]);
    }
}

#[test]
fn incoming_and_outgoing_to_the_same_peer_have_separate_action_and_monitor_lifetimes() {
    let mut m = Manager::default();
    m.connect(peer(1), 1);
    m.accept(peer(1), 1).unwrap();
    let local = pending(&m);
    let incoming = m.incoming()[0].pending().unwrap().clone();
    let local_monitor = if let Local::Active(s) = m.local() {
        s.monitor().clone()
    } else {
        panic!()
    };
    let remote_monitor = m.incoming()[0].monitor().clone();
    assert_ne!(local.token, incoming.token);
    assert_ne!(local_monitor, remote_monitor);
    let es = m.connection_down(&local_monitor, None);
    assert!(
        matches!(es.as_slice(),[Effect::CancelAction(t),Effect::Unwatch(m)]if *t==local.token && *m==local_monitor)
    );
    assert!(
        m.complete(&local.token, Ok(Reply::Tree(tree())))
            .effects
            .is_empty()
    );
    m.complete(&incoming.token, Ok(Reply::Tree(tree())));
    assert!(m.respond_wire(peer(1), &[]).is_ok());
    m.connection_down(&remote_monitor, Some(2));
    assert!(m.connection_down(&remote_monitor, None).is_empty());
    assert!(m.incoming()[0].tree().is_some());
    assert_eq!(m.incoming()[0].connection(), 2);
    let new_monitor = m.incoming()[0].monitor().clone();
    m.connection_down(&new_monitor, None);
    assert_eq!(m.authorize_get(peer(1)), Err(RequestError::NoActiveSync));
}
