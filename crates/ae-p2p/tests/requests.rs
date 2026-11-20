use ae_p2p::message::{Details, Kind, Limits as WireLimits, Message, Value};
use ae_p2p::requests::{
    Completion, DEFAULT_TIMEOUT, Error, Failure, Limits, PingAction, PingVersions, Requests, Token,
};
use ae_types::Unsigned;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

fn fixture() -> Json {
    serde_json::from_str(include_str!("fixtures/erlang_peer_requests.json")).unwrap()
}
fn kind(name: &str) -> Kind {
    Kind::ALL
        .iter()
        .copied()
        .find(|k| k.name() == name)
        .unwrap()
}
fn fields(value: &Json) -> BTreeMap<String, Value> {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), field(v)))
        .collect()
}
fn field(value: &Json) -> Value {
    if let Some(hex) = value.get("binary") {
        Value::Binary(hex::decode(hex.as_str().unwrap()).unwrap())
    } else if let Some(n) = value.get("integer") {
        Value::Integer(Unsigned::from_decimal(n.as_str().unwrap()).unwrap())
    } else if let Some(n) = value.as_u64() {
        Value::Integer(n.into())
    } else if let Some(b) = value.as_bool() {
        Value::Boolean(b)
    } else if let Some(v) = value.as_array() {
        Value::List(v.iter().map(field).collect())
    } else {
        Value::Record(fields(value))
    }
}
fn message(kind: Kind, version: u16, fs: BTreeMap<String, Value>) -> Message {
    Message::new(kind, version, fs, WireLimits::default()).unwrap()
}
fn ping(version: u16, versions: Value) -> Message {
    let mut fs = BTreeMap::from([
        ("port".into(), Value::Integer(3015.into())),
        ("share".into(), Value::Integer(0.into())),
        ("genesis_hash".into(), Value::Binary(vec![0; 32])),
        ("difficulty".into(), Value::Integer(1.into())),
        ("best_hash".into(), Value::Binary(vec![1; 32])),
        ("sync_allowed".into(), Value::Boolean(true)),
        ("peers".into(), Value::List(vec![])),
    ]);
    if version == 2 {
        fs.insert("versions".into(), versions);
        fs.insert("height".into(), Value::Integer(1.into()));
        fs.insert("capabilities".into(), Value::Binary(vec![0xc0]));
    }
    message(Kind::Ping, version, fs)
}
fn request(kind: Kind) -> Message {
    let hash = |n| {
        let mut h = vec![0; 32];
        h[31] = n;
        Value::Binary(h)
    };
    let fs: Vec<(&str, Value)> = match kind {
        Kind::GetHeaderByHash => vec![("hash", hash(1))],
        Kind::GetHeaderByHeight => {
            vec![("height", Value::Integer(1.into())), ("top_hash", hash(2))]
        }
        Kind::GetSuccessors => vec![
            ("from_hash", hash(1)),
            ("target_hash", hash(2)),
            ("n", Value::Integer(3.into())),
        ],
        Kind::GetGeneration => vec![("hash", hash(1)), ("forward", Value::Boolean(true))],
        Kind::PoolInit | Kind::GetNodeInfo => vec![],
        Kind::PoolUnfold => vec![("unfolds", Value::List(vec![Value::Binary(vec![1])]))],
        Kind::PoolGet => vec![("tx_hashes", Value::List(vec![hash(1)]))],
        Kind::PoolFinish => vec![("done", Value::Boolean(true))],
        _ => panic!("unsupported test request"),
    };
    message(
        kind,
        kind.latest(1).unwrap(),
        fs.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    )
}
fn expansion(hash: Value, response: bool) -> Message {
    let (kind, list) = if response {
        (Kind::BlockTransactions, "txs")
    } else {
        (Kind::GetBlockTransactions, "tx_hashes")
    };
    message(
        kind,
        1,
        BTreeMap::from([("hash".into(), hash), (list.into(), Value::List(vec![]))]),
    )
}
fn response(body: &Message) -> Message {
    Message::response_ok(body, WireLimits::default()).unwrap()
}
fn peer_error(kind: Kind) -> Message {
    Message::response_error(kind, vec![0, 255], WireLimits::default()).unwrap()
}
fn normal(value: &Value) -> Json {
    match value {
        Value::Binary(v) => json!({"binary":hex::encode(v)}),
        Value::Boolean(b) => json!(b),
        Value::List(v) => Json::Array(v.iter().map(normal).collect()),
        _ => panic!("unexpected reply field"),
    }
}
fn reply(c: &Completion, tokens: &BTreeMap<String, (Token, Instant)>) -> Json {
    let label = tokens.iter().find(|(_, (t, _))| *t == c.token).unwrap().0;
    let result = match &c.result {
        Err(Failure::Timeout) => json!(["error", "request_timeout"]),
        Err(Failure::Disconnected) => json!(["error", "disconnected"]),
        Err(e) => panic!("unexpected {e:?}"),
        Ok(envelope) => match envelope.details() {
            Details::Response { message: None, .. } => {
                json!(["error", normal(&envelope.fields()["reason"])])
            }
            Details::Response {
                message: Some(Ok(body)),
                ..
            } => match body.kind() {
                Kind::PoolInit => json!("ok"),
                Kind::PoolUnfold => json!(["ok", normal(&body.fields()["unfolds"])]),
                Kind::Transactions => json!(["ok", normal(&body.fields()["txs"])]),
                Kind::PoolFinish => json!(["ok", normal(&body.fields()["done"])]),
                _ => panic!("unexpected successful reply"),
            },
            _ => panic!("unexpected envelope"),
        },
    };
    json!([label, result])
}
fn sorted(mut values: Vec<Json>) -> Vec<Json> {
    values.sort_by_key(Json::to_string);
    values
}

#[test]
fn native_request_lifecycle_and_explicit_repairs() {
    let fixture = fixture();
    assert_eq!(fixture["timeouts"], json!([750, 30000, 30000]));
    let now = Instant::now();
    let mut steps = 0;
    for scenario in fixture["scenarios"].as_array().unwrap() {
        let name = scenario["name"].as_str().unwrap();
        let mut tracker = Requests::new(Limits::default(), 1).unwrap();
        let mut tokens = BTreeMap::new();
        for (index, step) in scenario["steps"].as_array().unwrap().iter().enumerate() {
            steps += 1;
            let at = now + Duration::from_millis(index as u64);
            let op = &step["op"];
            let mut completions = vec![];
            let mut timeout_ping_two = false;
            if op == "close" {
                completions = tracker.disconnect();
            } else {
                match op[0].as_str().unwrap() {
                    "start" | "slot" => {
                        let label = op[1].as_str().unwrap().to_owned();
                        let message = if op[0] == "start" {
                            request(kind(op[2].as_str().unwrap()))
                        } else if op[2] == "ping" {
                            ping(op[3].as_u64().unwrap() as u16, Value::List(vec![]))
                        } else {
                            expansion(field(&op[2][1]), false)
                        };
                        let custom = (message.kind() == Kind::GetNodeInfo)
                            .then_some(Duration::from_secs(60));
                        let started = tracker.start(&message, at, custom);
                        if step["result"] == "accepted" {
                            let started = started.unwrap();
                            assert_eq!(started.deadline - at, custom.unwrap_or(DEFAULT_TIMEOUT));
                            tokens.insert(label, (started.token, started.deadline));
                            if op[0] == "start" {
                                assert_eq!(
                                    step["sent"],
                                    json!([hex::encode(
                                        message.encode_packet(WireLimits::default()).unwrap()
                                    )])
                                );
                            }
                        } else {
                            assert_eq!(started.unwrap_err(), Error::AlreadyPending);
                        }
                    }
                    "error" => {
                        completions = tracker
                            .receive_response(peer_error(kind(op[1].as_str().unwrap())), at)
                            .completions;
                    }
                    "response" => {
                        let body = message(
                            kind(op[1].as_str().unwrap()),
                            op[2].as_u64().unwrap() as u16,
                            fields(&op[3]),
                        );
                        completions = tracker.receive_response(response(&body), at).completions;
                    }
                    "timeout" => {
                        let (token, deadline) = &tokens[op[1].as_str().unwrap()];
                        timeout_ping_two = op[2] == "ping" && op[3] == 2;
                        let update = tracker.timeout(token, *deadline);
                        if timeout_ping_two {
                            assert_eq!(update.ping, PingAction::Selected(1));
                            assert_eq!(update.completions.len(), 1);
                            assert_eq!(update.completions[0].result, Err(Failure::Timeout));
                        }
                        completions = update.completions;
                    }
                    "remove" => {
                        let body = expansion(field(&op[1][1]), true);
                        let update = tracker.receive_response(response(&body), at);
                        assert_eq!(update.completions.len(), 1);
                        assert!(update.completions[0].result.is_ok());
                    }
                    _ => panic!("unknown op"),
                }
            }
            let pending: Vec<_> = tokens
                .iter()
                .filter(|(_, (t, _))| tracker.contains(t))
                .map(|(l, _)| l.clone())
                .collect();
            if name == "wrong_pool_response" && index == 1 {
                assert_eq!(step["pending"], json!([]));
                assert_eq!(pending, vec!["a"]);
                assert!(completions.is_empty());
                continue;
            }
            assert_eq!(json!(pending), step["pending"], "{name} {index}");
            for (label, (token, _)) in &tokens {
                assert_eq!(
                    json!(tracker.contains(token)),
                    step["timers"][label],
                    "{name} {label}"
                );
            }
            assert_eq!(
                tracker.is_closed(),
                step["closed"].as_bool().unwrap(),
                "{name} {index}"
            );
            if let Some(selected) = step["selected"].as_u64() {
                assert_eq!(tracker.ping_version(), selected as u16);
            }
            let mut expected = step["replies"].as_array().unwrap().clone();
            if name == "wrong_pool_response" && index == 2 {
                assert!(expected.is_empty());
                expected = vec![json!(["a", ["error", "disconnected"]])];
            }
            let timed_out = expected
                .iter()
                .find(|r| r[1] == json!(["error", "request_timeout"]))
                .map(|r| r[0].clone());
            if let Some(label) = timed_out {
                expected.retain(|r| !(r[0] == label && r[1] == json!(["error", "disconnected"])));
            }
            let actual = if timeout_ping_two {
                vec![]
            } else {
                completions.iter().map(|c| reply(c, &tokens)).collect()
            };
            assert_eq!(sorted(actual), sorted(expected), "{name} {index}");
        }
    }
    assert_eq!(steps, 53);
}

#[test]
fn native_version_advertisements_and_safe_empty_intersection() {
    for row in fixture()["versions"].as_array().unwrap() {
        let minimum = row["minimum"].as_u64().unwrap() as u16;
        let mut versions = match PingVersions::new(minimum) {
            Ok(v) => v,
            Err(e) => {
                assert_eq!(e, Error::InvalidConfiguration);
                assert_eq!(row["selected"], "error");
                continue;
            }
        };
        let message = ping(2, field(&row["advertised"]));
        let result = versions.observe(&message, true);
        if row["selected"] == "error" {
            assert_eq!(result, Err(Error::NoCommonPingVersion));
            assert_eq!(versions.selected(), None);
        } else {
            let expected = row["selected"].as_u64().unwrap() as u16;
            assert_eq!(result, Ok(PingAction::Selected(expected)));
            assert_eq!(versions.selected(), Some(expected));
        }
    }
}

#[test]
fn deadlines_close_once_and_tokens_cannot_cross_connections() {
    let now = Instant::now();
    let mut a = Requests::new(Limits::default(), 1).unwrap();
    let mut b = Requests::new(Limits::default(), 1).unwrap();
    let header = request(Kind::GetHeaderByHash);
    let old = a.start(&header, now, None).unwrap();
    let other = b.start(&header, now, None).unwrap();
    assert_ne!(old.token, other.token);
    assert!(b.timeout(&old.token, old.deadline).completions.is_empty());
    assert!(
        a.timeout(&old.token, old.deadline - Duration::from_nanos(1))
            .completions
            .is_empty()
    );
    assert_eq!(a.next_deadline(), Some(old.deadline));
    let update = a.receive_response(peer_error(Kind::Header), old.deadline);
    assert_eq!(update.completions.len(), 1);
    assert_eq!(update.completions[0].result, Err(Failure::Timeout));
    assert!(a.is_closed());
    assert!(a.next_deadline().is_none());
    assert!(a.disconnect().is_empty());
    assert!(a.timeout(&old.token, old.deadline).completions.is_empty());
    assert_eq!(
        a.start(&header, now, None).unwrap_err(),
        Error::Disconnected
    );
    assert!(b.contains(&other.token));
    let stale = b
        .receive_response(peer_error(Kind::Header), now)
        .completions
        .pop()
        .unwrap();
    assert_eq!(stale.token, other.token);
    let new = b.start(&header, now, None).unwrap();
    assert_ne!(new.token, other.token);
    assert!(b.timeout(&other.token, old.deadline).completions.is_empty());
    assert!(b.contains(&new.token));
}

#[test]
fn bounded_expansions_and_request_admission_are_atomic() {
    let now = Instant::now();
    let mut tracker = Requests::new(
        Limits {
            pending: 2,
            expansions: 1,
        },
        1,
    )
    .unwrap();
    let expand = expansion(Value::Binary(vec![1; 32]), false);
    let first = tracker.start(&expand, now, None).unwrap();
    assert_eq!(
        tracker.start(&expand, now, None).unwrap_err(),
        Error::AlreadyPending
    );
    assert_eq!(
        tracker
            .start(&expansion(Value::Binary(vec![2; 32]), false), now, None)
            .unwrap_err(),
        Error::ResourceLimit
    );
    assert_eq!(
        tracker
            .start(&expansion(Value::Binary(vec![1]), false), now, None)
            .unwrap_err(),
        Error::InvalidRequest
    );
    assert_eq!(
        tracker
            .start(&request(Kind::PoolInit), now, Some(Duration::ZERO))
            .unwrap_err(),
        Error::InvalidRequest
    );
    assert_eq!(
        tracker.start(&response(&expand), now, None).unwrap_err(),
        Error::InvalidRequest
    );
    assert_eq!(tracker.len(), 1);
    assert!(tracker.contains(&first.token));
    let info = tracker
        .start(
            &request(Kind::GetNodeInfo),
            now,
            Some(Duration::from_millis(750)),
        )
        .unwrap();
    assert_eq!(info.deadline, now + Duration::from_millis(750));
    assert_eq!(
        tracker
            .start(&request(Kind::PoolInit), now, None)
            .unwrap_err(),
        Error::ResourceLimit
    );
    let update = tracker.expire(info.deadline);
    assert_eq!(update.completions.len(), 2);
    assert_eq!(
        update
            .completions
            .iter()
            .filter(|c| c.result == Err(Failure::Timeout))
            .count(),
        1
    );
    assert!(tracker.is_closed());
    let mut zero = Requests::new(
        Limits {
            pending: 0,
            expansions: 0,
        },
        1,
    )
    .unwrap();
    assert_eq!(
        zero.start(&request(Kind::PoolInit), now, None).unwrap_err(),
        Error::ResourceLimit
    );
    assert!(zero.is_empty());
}

#[test]
fn ping_probe_has_separate_completion_and_respects_minimum() {
    let now = Instant::now();
    let p1 = ping(1, Value::List(vec![]));
    let p2 = ping(2, Value::List(vec![]));
    let mut tracker = Requests::new(Limits::default(), 1).unwrap();
    assert_eq!(tracker.receive_ping(&p1, now).ping, PingAction::None);
    let caller = tracker.start(&p1, now, None).unwrap();
    let first = tracker.receive_response(response(&p1), now);
    assert_eq!(first.ping, PingAction::ProbeV2);
    assert_eq!(first.completions[0].token, caller.token);
    assert!(!first.completions[0].probe);
    let probe = tracker.start_probe(&p2, now).unwrap();
    let other = tracker
        .start(&request(Kind::PoolInit), now + Duration::from_secs(1), None)
        .unwrap();
    let fallback = tracker.expire(probe.deadline);
    assert_eq!(fallback.ping, PingAction::Selected(1));
    assert_eq!(fallback.completions.len(), 1);
    assert!(fallback.completions[0].probe);
    assert_ne!(probe.token, caller.token);
    assert!(tracker.contains(&other.token));
    assert!(!tracker.is_closed());
    assert_eq!(
        tracker.start_probe(&p2, now).unwrap_err(),
        Error::InvalidRequest
    );
    let mut strict = Requests::new(Limits::default(), 2).unwrap();
    assert_eq!(
        strict.start(&p1, now, None).unwrap_err(),
        Error::InvalidRequest
    );
    let pending = strict.start(&p2, now, None).unwrap();
    let update = strict.expire(pending.deadline);
    assert_eq!(update.completions[0].result, Err(Failure::Timeout));
    assert!(strict.is_closed());
    assert_eq!(strict.ping_version(), 2);
    assert!(strict.receive_ping(&p2, now).completions.is_empty());
    let mut strict = Requests::new(Limits::default(), 2).unwrap();
    strict.start(&request(Kind::PoolInit), now, None).unwrap();
    let update = strict.receive_ping(&p1, now);
    assert_eq!(update.error, Some(Error::NoCommonPingVersion));
    assert_eq!(update.completions[0].result, Err(Failure::Disconnected));
    assert!(strict.is_closed());
}

#[test]
fn malformed_responses_and_failed_negotiation_retire_connection() {
    let now = Instant::now();
    let mut tracker = Requests::new(Limits::default(), 1).unwrap();
    tracker.start(&request(Kind::PoolGet), now, None).unwrap();
    let malformed = message(
        Kind::Response,
        1,
        BTreeMap::from([
            ("result".into(), Value::Boolean(true)),
            ("type".into(), Value::Integer(9.into())),
            ("reason".into(), Value::Binary(vec![])),
            ("object".into(), Value::Binary(vec![0xff])),
        ]),
    );
    let update = tracker.receive_response(malformed, now);
    assert_eq!(update.error, Some(Error::MalformedResponse));
    assert_eq!(update.completions.len(), 1);
    assert_eq!(update.completions[0].result, Err(Failure::Disconnected));
    assert!(tracker.is_closed());
    let mut tracker = Requests::new(Limits::default(), 1).unwrap();
    let p2 = ping(
        2,
        field(&json!([{"protocol":{"binary":"70696e67"},"vsns":[]}])),
    );
    tracker.start(&p2, now, None).unwrap();
    tracker.start(&request(Kind::PoolInit), now, None).unwrap();
    let update = tracker.receive_response(response(&p2), now);
    assert_eq!(update.error, Some(Error::NoCommonPingVersion));
    assert_eq!(update.completions.len(), 2);
    assert_eq!(
        update.completions[0].result,
        Err(Failure::NoCommonPingVersion)
    );
    assert!(tracker.is_closed());
}

#[test]
fn mapped_error_routes_keep_wire_request_and_response_types_distinct() {
    let now = Instant::now();
    let native = fixture();
    let mapping = |name: &str| {
        native["mappings"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["kind"] == name)
            .unwrap()["mapped"]
            .as_str()
            .unwrap()
    };
    for (outgoing, incoming, slot) in [
        (Kind::GetHeaderByHash, Kind::Header, "get_header"),
        (Kind::GetHeaderByHeight, Kind::Header, "get_header"),
        (Kind::GetSuccessors, Kind::HeaderHashes, "get_n_successors"),
        (Kind::GetGeneration, Kind::Generation, "get_generation"),
        (Kind::PoolInit, Kind::PoolInit, "tx_pool"),
        (Kind::PoolUnfold, Kind::PoolUnfold, "tx_pool"),
        (Kind::PoolGet, Kind::Transactions, "tx_pool"),
        (Kind::PoolFinish, Kind::PoolFinish, "tx_pool"),
        (Kind::GetNodeInfo, Kind::NodeInfo, "node_info"),
    ] {
        assert_eq!(mapping(incoming.name()), slot);
        if !matches!(
            outgoing,
            Kind::PoolInit | Kind::PoolUnfold | Kind::PoolGet | Kind::PoolFinish
        ) {
            assert_eq!(mapping(outgoing.name()), slot);
        }
        let mut tracker = Requests::new(Limits::default(), 1).unwrap();
        let started = tracker.start(&request(outgoing), now, None).unwrap();
        let envelope = peer_error(incoming);
        let update = tracker.receive_response(envelope.clone(), now);
        assert_eq!(update.completions.len(), 1);
        assert_eq!(update.completions[0].token, started.token);
        assert_eq!(update.completions[0].result, Ok(envelope));
        assert!(tracker.next_deadline().is_none());
        assert!(!tracker.is_closed());
        assert!(
            tracker
                .receive_response(peer_error(incoming), now)
                .completions
                .is_empty()
        );
    }
    for name in ["ping", "tx_pool", "txps_get", "block_txs"] {
        assert_eq!(mapping(name), name);
    }
}

#[test]
fn block_hash_correlation_probe_upgrade_and_invalid_deadlines() {
    let now = Instant::now();
    let mut tracker = Requests::new(Limits::default(), 1).unwrap();
    let h1 = Value::Binary(vec![1; 32]);
    let h2 = Value::Binary(vec![2; 32]);
    let first = tracker
        .start(&expansion(h1.clone(), false), now, None)
        .unwrap();
    assert!(
        tracker
            .receive_response(response(&expansion(h2.clone(), true)), now)
            .completions
            .is_empty()
    );
    let second = tracker
        .start(&expansion(h2.clone(), false), now, None)
        .unwrap();
    let update = tracker.receive_response(response(&expansion(h2, true)), now);
    assert_eq!(update.completions[0].token, second.token);
    assert!(tracker.contains(&first.token));
    assert_eq!(
        tracker
            .start(&request(Kind::GetNodeInfo), now, Some(Duration::MAX))
            .unwrap_err(),
        Error::DeadlineOverflow
    );
    assert_eq!(tracker.len(), 1);
    let p2 = ping(2, Value::List(vec![]));
    let probe = tracker.start_probe(&p2, now).unwrap();
    let update = tracker.receive_response(response(&p2), now);
    assert_eq!(update.ping, PingAction::Selected(2));
    assert_eq!(tracker.ping_version(), 2);
    assert_eq!(update.completions[0].token, probe.token);
    assert!(update.completions[0].probe);
    assert!(
        tracker
            .timeout(&probe.token, probe.deadline)
            .completions
            .is_empty()
    );
    assert_eq!(tracker.ping_version(), 2);
    let update = tracker.receive_response(response(&expansion(Value::Binary(vec![]), true)), now);
    assert_eq!(update.error, Some(Error::MalformedResponse));
    assert_eq!(update.completions[0].token, first.token);
    assert!(tracker.is_closed());
}
