use super::*;

pub(super) fn assert_native(snapshots: &[PeerMetrics], expected: &Value) {
    let actual: Vec<_> = snapshots.iter().flat_map(|s| [
        ("count", s.inbound + s.outbound), ("inbound", s.inbound),
        ("outbound", s.outbound), ("verified", s.verified),
        ("unverified", s.unverified), ("errored", s.standby), ("blocked", s.blocked),
    ].into_iter().map(|(suffix, value)| json!({"name":["ae","epoch","aecore","peers",suffix],"value":value}))).collect();
    // Ping counters are tested at the transport completion boundary; this
    // comparison covers all seven peer gauges and their exact emission order.
    let gauges: Vec<_> = expected
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["name"].as_array().unwrap().len() == 5)
        .cloned()
        .collect();
    assert_eq!(actual, gauges);
}
fn native() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_peer_metrics.json"
    ))
    .unwrap()
}
#[test]
fn peer_metrics_match_original_callbacks_with_real_pool_and_connection_lifecycles() {
    let data = native();
    native_lifecycles(&data, 95);
}
#[test]
fn peer_metrics_match_stale_callbacks_and_known_or_unknown_resolver_completions() {
    let data = native();
    let rows = data["callbacks"].as_array().unwrap();
    assert_eq!(rows.len(), 20);
    for row in rows {
        let name = row["name"].as_str().unwrap();
        let mut c = config();
        c.maximum_outbound = 0;
        let mut r = registry(c);
        let stale = Token {
            registry: 0,
            sequence: 1,
        };
        let timer = |callback| {
            Command::TimerFired(Timer {
                token: stale,
                deadline: 0,
                callback,
            })
        };
        let mut token = stale;
        if name.starts_with("resolved_known") || name.starts_with("resolved_initial") {
            let mut p = peer(1, false);
            p.host = b"known".to_vec();
            let change = apply(
                &mut r,
                0,
                Command::Resolve {
                    peer: Info::from(&p),
                    source: None,
                    trusted: false,
                },
            );
            token = change
                .effects
                .iter()
                .find_map(|e| match e {
                    Effect::Resolve { token, .. } => Some(*token),
                    _ => None,
                })
                .unwrap();
            if name.starts_with("resolved_known") {
                // Native initial nxdomain dispatches resolve_peer (no gauge).
                // The subsequent retry uses resolved_hostname callbacks.
                let initial = r
                    .prepare(
                        0,
                        Command::Resolved {
                            token,
                            address: None,
                        },
                    )
                    .unwrap();
                assert!(initial.peer_metrics().is_empty());
                r.commit(initial).unwrap();
                let timer = r.resolution_timer(b"known").unwrap();
                let retry = r
                    .prepare(timer.deadline, Command::TimerFired(timer))
                    .unwrap();
                assert!(retry.peer_metrics().is_empty());
                token = r
                    .commit(retry)
                    .unwrap()
                    .effects
                    .iter()
                    .find_map(|e| match e {
                        Effect::Resolve { token, .. } => Some(*token),
                        _ => None,
                    })
                    .unwrap();
            }
        }
        let command = match name {
            "is_blocked" => Command::IsBlocked(key(99)),
            "unblock_all" => Command::UnblockAll,
            "all_peers" => Command::AllPeers {
                target: Target::Both,
                maximum: 16,
            },
            "delete_missing" => Command::Delete(key(99)),
            "connected_stale" => Command::Connected {
                peer: key(99),
                token: stale,
            },
            "alive_stale" => Command::Alive {
                peer: key(99),
                token: stale,
            },
            "failed_stale" | "closed_stale" | "dead_stale" | "process_down" => Command::Failed {
                peer: key(99),
                token: stale,
                reason: match name {
                    "failed_stale" => Failure::Failed,
                    "closed_stale" => Failure::Closed,
                    "dead_stale" => Failure::Dead,
                    _ => Failure::Down,
                },
            },
            "connect_timeout" => timer(TimerCallback::Connect),
            "probe_timeout" => timer(TimerCallback::Probe(Class::Verified)),
            "ping_timeout" => timer(TimerCallback::Ping),
            "resolve_timeout" => timer(TimerCallback::Resolve),
            n if n.starts_with("resolved_") => Command::Resolved {
                token,
                address: n.ends_with("ok").then_some(Ipv4Addr::new(9, 8, 7, 6)),
            },
            _ => panic!("{name}"),
        };
        let prepared = r.prepare(r.state.now.max(1), command).unwrap();
        assert_native(prepared.peer_metrics(), &row["metrics"]);
        r.commit(prepared).unwrap();
    }
}
#[test]
fn peer_metrics_follow_typed_timer_identity_and_atomic_query_admission() {
    let mut r = registry(config());
    apply(&mut r, 0, Command::Add(peer(1, false)));
    let timer = r
        .timers()
        .find(|t| t.callback == TimerCallback::Connect)
        .unwrap();
    let revision = r.revision();
    assert!(matches!(
        r.prepare(
            1,
            Command::AllPeers {
                target: Target::Both,
                maximum: 0
            }
        ),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(r.revision(), revision);
    let prepared = r
        .prepare(timer.deadline, Command::TimerFired(timer))
        .unwrap();
    assert_eq!(prepared.peer_metrics()[0].outbound, 1);
    r.commit(prepared).unwrap();
    let stale = r
        .prepare(timer.deadline, Command::TimerFired(timer))
        .unwrap();
    assert_eq!(stale.peer_metrics().len(), 1);
    assert_eq!(r.commit(stale).unwrap().result, Outcome::Stale);
    let token = r.connection(key(1)).unwrap().token;
    let connection_timer = r.connection_timer(key(1)).unwrap();
    assert_eq!(connection_timer.callback, TimerCallback::Connection);
    apply(
        &mut r,
        timer.deadline,
        Command::Connected {
            peer: key(1),
            token,
        },
    );
    let stale = r
        .prepare(timer.deadline, Command::TimerFired(connection_timer))
        .unwrap();
    assert_eq!(stale.peer_metrics().len(), 1);
    assert_eq!(r.commit(stale).unwrap().result, Outcome::Stale);
    apply(
        &mut r,
        timer.deadline,
        Command::PingDone {
            peer: key(1),
            token,
        },
    );
    let ping = r.connection_timer(key(1)).unwrap();
    assert_eq!(ping.callback, TimerCallback::Ping);
    let prepared = r.prepare(ping.deadline, Command::TimerFired(ping)).unwrap();
    assert!(prepared.peer_metrics().is_empty());
    r.commit(prepared).unwrap();
    let prepared = r
        .prepare(
            ping.deadline,
            Command::AllPeers {
                target: Target::Both,
                maximum: 1,
            },
        )
        .unwrap();
    assert_eq!(prepared.peer_metrics()[0].verified, 1);
    assert_eq!(
        r.commit(prepared).unwrap().result,
        Outcome::Peers(vec![Info::from(&peer(1, false))])
    );
    for command in [
        Command::Sample {
            maximum: 1,
            percent_connected: None,
            exclude: vec![],
        },
        Command::Shutdown,
    ] {
        let prepared = r.prepare(ping.deadline, command).unwrap();
        assert!(prepared.peer_metrics().is_empty());
        r.commit(prepared).unwrap();
    }
}
