use super::*;
use serde_json::{Value as Json, json};
fn fixture() -> Json {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_peer_analytics.json"
    ))
    .unwrap()
}
fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn unsigned(v: &Json) -> Unsigned {
    Unsigned::from_decimal(v.as_str().unwrap()).unwrap()
}
fn host(v: &Json) -> Host {
    match v.get("binary") {
        Some(b) => Host::Binary(bytes(b)),
        None => Host::List(
            v["list"]
                .as_array()
                .unwrap()
                .iter()
                .map(|i| i.as_i64().unwrap())
                .collect(),
        ),
    }
}
fn info(v: &Json) -> PeerInfo {
    match v.as_str() {
        Some("unresolved") => PeerInfo::Unresolved,
        Some("unknown") => PeerInfo::Unknown,
        _ => PeerInfo::Resolved(NodeInfo {
            network_id: bytes(&v["network_id"]),
            version: bytes(&v["version"]),
            os: bytes(&v["os"]),
            revision: bytes(&v["revision"]),
            vendor: bytes(&v["vendor"]),
        }),
    }
}
fn stat(v: &Json) -> Stat {
    Stat {
        host: host(&v["host"]),
        port: unsigned(&v["port"]),
        first_seen: v["first_seen"].as_i64().unwrap(),
        last_seen: v["last_seen"].as_i64().unwrap(),
        genesis: bytes(&v["genesis"]),
        top: bytes(&v["top"]),
        difficulty: unsigned(&v["difficulty"]),
        info: info(&v["info"]),
    }
}
fn stats(v: &Json) -> BTreeMap<PeerPublicKey, Stat> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                PeerPublicKey(bytes(&r["peer"]).try_into().unwrap()),
                stat(&r["stat"]),
            )
        })
        .collect()
}
fn engine(v: &Json) -> Engine {
    let mut e = Engine::new(Config {
        enabled: v["enabled"].as_bool().unwrap(),
        ..Default::default()
    })
    .unwrap();
    e.stats = stats(&v["stats"]);
    if v["pending"] == 1 {
        e.pending.insert(peer(7), 0);
    }
    e
}
fn peer(n: u8) -> PeerPublicKey {
    let mut b = [0; 32];
    b[31] = n;
    PeerPublicKey(b)
}
fn announcement(p: u8, temporary: bool) -> Announcement {
    Announcement {
        peer: peer(p),
        host: Host::Binary(b"new.host".to_vec()),
        port: 1234u64.into(),
        genesis: peer(3).0.to_vec(),
        top: peer(4).0.to_vec(),
        difficulty: Unsigned::from_field_bytes(&{
            let mut b = vec![0; 26];
            b[0] = 1;
            b
        })
        .unwrap(),
        temporary,
    }
}
fn snapshot(v: &Json) -> Snapshot {
    Snapshot {
        enabled: v["enabled"].as_bool().unwrap(),
        stats: stats(&v["stats"]),
        pending: v["pending"].as_u64().unwrap() as usize,
    }
}
#[test]
fn status_decisions_match_original_enabled_temporary_age_and_pending_rules() {
    for (i, row) in fixture()["rows"].as_array().unwrap().iter().enumerate() {
        let mut e = engine(&row["before"]);
        let r = e
            .record(
                announcement(7, row["temporary"].as_bool().unwrap()),
                90000,
                100000 + row["age"].as_i64().unwrap(),
            )
            .unwrap();
        assert_eq!(
            r.probe.is_some(),
            row["scheduled"].as_bool().unwrap(),
            "{i}"
        );
        assert!(!r.deferred);
        assert_eq!(e.snapshot(), snapshot(&row["after_state"]), "{i}");
    }
}
#[test]
fn sweep_retains_native_age_boundary_and_pending_requests_even_after_removal() {
    for row in fixture()["sweeps"].as_array().unwrap() {
        let mut e = engine(&row["before"]);
        e.sweep();
        assert_eq!(e.snapshot(), snapshot(&row["after_state"]));
        assert_eq!(
            e.update_info(peer(7), PeerInfo::Unknown).is_ok(),
            row["late_info_succeeds"].as_bool().unwrap()
        );
    }
}
fn value(v: &Value) -> Json {
    match v {
        Value::Binary(b) => json!({"binary":hex::encode(b)}),
        Value::Integer(i) => json!({"integer":i.to_string()}),
        Value::List(l) => json!({"list":l}),
    }
}
#[test]
fn original_client_fields_preserve_raw_host_forms_big_numbers_and_info_states() {
    for row in fixture()["clients"].as_array().unwrap() {
        let s = stat(&row["stat"]);
        let fields = json!(
            s.for_client()
                .iter()
                .map(|(k, v)| json!([hex::encode(k.as_bytes()), value(v)]))
                .collect::<Vec<_>>()
        );
        assert_eq!(fields, row["fields"]);
        let snap = Snapshot {
            enabled: true,
            stats: BTreeMap::from([(peer(7), s)]),
            pending: 0,
        };
        assert_eq!(
            snap.for_client().keys().next().unwrap(),
            row["peer"].as_str().unwrap()
        );
    }
}
#[test]
fn probe_outcomes_match_actual_original_server_coalescing_and_restart() {
    for row in fixture()["servers"].as_array().unwrap() {
        let mut e = Engine::new(Config {
            enabled: true,
            ..Default::default()
        })
        .unwrap();
        let mut a = announcement(9, false);
        a.genesis = peer(1).0.to_vec();
        a.top = peer(2).0.to_vec();
        a.difficulty = 5u64.into();
        let p = e.record(a.clone(), 10, 11).unwrap().probe.unwrap();
        assert_eq!(e.stats, stats(&row["before"]));
        a.top = peer(3).0.to_vec();
        a.difficulty = 6u64.into();
        assert!(e.record(a, 12, 13).unwrap().probe.is_none());
        let expected = stats(&row["after_stats"]);
        let result = match row["outcome"].as_str().unwrap() {
            "no_connection" => ProbeResult::NoConnection,
            "unknown" => ProbeResult::Unknown,
            "success" => match &expected[&peer(9)].info {
                PeerInfo::Resolved(i) => ProbeResult::Resolved(i.clone()),
                _ => panic!(),
            },
            _ => panic!(),
        };
        e.complete(p, result).unwrap();
        assert_eq!(e.stats, expected);
        assert_eq!(e.snapshot().pending, 0);
    }
}
#[test]
fn operational_limits_roll_back_records_and_bound_probes_without_replaying_stale_completions() {
    let mut e = Engine::new(Config {
        enabled: true,
        limits: Limits {
            peers: 2,
            probes: 1,
            ..Default::default()
        },
    })
    .unwrap();
    let p = e
        .record(announcement(1, false), 10, 11)
        .unwrap()
        .probe
        .unwrap();
    let second = e.record(announcement(2, false), 20, 21).unwrap();
    assert!(second.deferred && second.probe.is_none());
    let before = e.snapshot();
    assert_eq!(e.record(announcement(3, false), 30, 31), Err(Error::Limit));
    assert_eq!(e.snapshot(), before);
    e.complete(p, ProbeResult::NoConnection).unwrap();
    let p2 = e
        .record(announcement(1, false), 12, 13)
        .unwrap()
        .probe
        .unwrap();
    assert_eq!(e.complete(p, ProbeResult::Unknown), Err(Error::Stale));
    assert_eq!(e.stats[&peer(1)].info, PeerInfo::Unresolved);
    let oversized = NodeInfo {
        network_id: vec![0; 5000],
        version: vec![],
        os: vec![],
        revision: vec![],
        vendor: vec![],
    };
    assert_eq!(
        e.complete(p2, ProbeResult::Resolved(oversized)),
        Err(Error::Limit)
    );
    assert_eq!(e.snapshot().pending, 0);
    assert_eq!(e.stats[&peer(1)].info, PeerInfo::Unresolved);
    let p3 = e
        .record(announcement(1, false), 50, 100000)
        .unwrap()
        .probe
        .unwrap();
    assert_eq!(e.sweep(), 1);
    assert_eq!(e.complete(p3, ProbeResult::Unknown), Err(Error::Missing));
    assert_eq!(e.snapshot().pending, 0);
    let mut limited = Engine::new(Config {
        enabled: true,
        limits: Limits {
            retained_bytes: 1,
            ..Default::default()
        },
    })
    .unwrap();
    assert_eq!(
        limited.record(announcement(1, true), 0, 0),
        Err(Error::Limit)
    );
    assert!(limited.snapshot().stats.is_empty());
}
#[test]
fn worker_drains_accepted_queue_and_dropped_replies_before_join() {
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };
    let clock = Arc::new(AtomicI64::new(100));
    let now = clock.clone();
    let worker = Worker::with_clock(
        Config {
            enabled: true,
            limits: Limits {
                requests: 4,
                ..Default::default()
            },
        },
        Arc::new(move || Ok(now.fetch_add(1, Ordering::SeqCst))),
    )
    .unwrap();
    let client = worker.client();
    let (entered, release) = worker::pause(&client);
    entered.recv().unwrap();
    let first = client.log(announcement(7, true), None).unwrap();
    drop(client.log(announcement(7, true), None).unwrap());
    let snapshot = client.snapshot().unwrap();
    assert!(matches!(client.snapshot(), Err(Error::Busy)));
    assert_eq!(client.info().pending, 4);
    worker.request_shutdown();
    assert!(matches!(client.sweep(), Err(Error::Closed)));
    release.send(()).unwrap();
    worker.shutdown().unwrap();
    first.blocking_wait().unwrap();
    let stat = &snapshot.blocking_wait().unwrap().stats[&peer(7)];
    assert_eq!((stat.first_seen, stat.last_seen), (100, 103));
    assert_eq!(clock.load(Ordering::SeqCst), 104);
    assert_eq!(client.info().pending_bytes, 0);
    assert!(client.info().joined);
}
#[test]
fn worker_restart_resets_ephemeral_state_and_clock_failures_leave_it_unchanged() {
    use std::sync::Arc;
    let worker = Worker::with_clock(
        Config {
            enabled: true,
            ..Default::default()
        },
        Arc::new(|| Err(Error::Clock)),
    )
    .unwrap();
    let client = worker.client();
    assert_eq!(
        client
            .log(announcement(1, false), None)
            .unwrap()
            .blocking_wait(),
        Err(Error::Clock)
    );
    assert!(
        client
            .snapshot()
            .unwrap()
            .blocking_wait()
            .unwrap()
            .stats
            .is_empty()
    );
    worker::fail(&client);
    assert_eq!(worker.shutdown(), Err(Error::Panicked));
    assert!(matches!(client.snapshot(), Err(Error::Closed)));
    let next = Worker::spawn(Config {
        enabled: true,
        ..Default::default()
    })
    .unwrap();
    let current = next.client();
    assert!(
        current
            .snapshot()
            .unwrap()
            .blocking_wait()
            .unwrap()
            .stats
            .is_empty()
    );
    current
        .log(announcement(1, false), None)
        .unwrap()
        .blocking_wait()
        .unwrap();
    next.shutdown().unwrap();
    assert_eq!(current.info().probe_completions, 1);
    assert_eq!(current.info().probes, 0);
    let disabled = Worker::with_clock(
        Config::default(),
        Arc::new(|| panic!("disabled analytics must not read clock")),
    )
    .unwrap();
    assert!(
        !disabled
            .client()
            .log(announcement(1, false), None)
            .unwrap()
            .blocking_wait()
            .unwrap()
            .deferred
    );
    disabled.shutdown().unwrap();
}

#[test]
fn native_enabled_direction_limits_preserve_separate_operational_connection_capacity() {
    let mut config = ae_p2p::registry::Config::default();
    let capacity = config.maximum_connections;
    configure_network(&mut config, true);
    assert_eq!(config.maximum_inbound, NATIVE_CONNECTION_LIMIT);
    assert_eq!(config.maximum_outbound, NATIVE_CONNECTION_LIMIT);
    assert_eq!(config.maximum_connections, capacity);
    assert!(config.analytics);
    assert!(ae_p2p::registry::Registry::new(peer(1), config.clone()).is_ok());
    config.maximum_inbound = NATIVE_CONNECTION_LIMIT + 1;
    assert!(ae_p2p::registry::Registry::new(peer(1), config).is_err());
    let mut disabled = ae_p2p::registry::Config::default();
    let limits = (disabled.maximum_inbound, disabled.maximum_outbound);
    configure_network(&mut disabled, false);
    assert!(!disabled.analytics);
    assert_eq!(
        (disabled.maximum_inbound, disabled.maximum_outbound),
        limits
    );
}
