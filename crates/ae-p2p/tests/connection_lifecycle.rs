use ae_p2p::{
    message::{Kind, Message},
    requests::{DEFAULT_TIMEOUT, Error, Failure, Limits, PingAction, Requests},
};
use serde_json::Value;
use std::time::Instant;

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/erlang_connection_lifecycle.json")).unwrap()
}
fn packet(row: &Value) -> Message {
    Message::decode_packet(
        &hex::decode(row["packet"].as_str().unwrap()).unwrap(),
        Default::default(),
    )
    .unwrap()
}
fn example(native: &Value, version: u16) -> Message {
    packet(
        native["incoming"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["version"] == version && r["versions"] == "normal")
            .unwrap(),
    )
}
fn configured(native: &Value, minimum: u16, previous: u16) -> (Requests, Instant) {
    let mut requests = Requests::new(Limits::default(), minimum).unwrap();
    let mut now = Instant::now();
    assert_eq!(requests.ping_version(), minimum);
    match previous {
        0 => (),
        1 => {
            let started = requests.start(&example(native, 2), now, None).unwrap();
            now = started.deadline;
            assert_eq!(
                requests.timeout(&started.token, now).ping,
                PingAction::Selected(1)
            );
        }
        2 => {
            assert_eq!(
                requests.receive_ping(&example(native, 2), now).ping,
                PingAction::Selected(2)
            );
        }
        _ => unreachable!(),
    }
    (requests, now)
}

#[test]
fn native_selection_covers_minimums_fallbacks_and_advertisements() {
    let native = fixture();
    assert_eq!(native["callback_only"], true);
    assert_eq!(native["full_node_started"], false);
    assert_eq!(native["sockets_opened"], 0);
    let mut checked = 0;
    // This compares negotiation, not admission/application effect ordering.
    // The latter remains recorded by the original callback in the fixture.
    for direction in ["incoming", "responses"] {
        for row in native[direction].as_array().unwrap() {
            let minimum = row["minimum"].as_u64().unwrap() as u16;
            let previous = row["previous"].as_u64().unwrap() as u16;
            let previous = (previous != 0).then_some(previous);
            let ping = packet(row);
            let (mut requests, now) = configured(&native, minimum, previous.unwrap_or(0));
            let update = if direction == "incoming" {
                requests.receive_ping(&ping, now)
            } else {
                let pending = requests.start(&ping, now, None).unwrap();
                let update = requests.receive_response(
                    Message::response_ok(&ping, Default::default()).unwrap(),
                    now,
                );
                assert_eq!(update.completions.len(), 1, "{row}");
                assert_eq!(update.completions[0].token, pending.token, "{row}");
                update
            };
            assert_eq!(update.ping_before, previous, "{row}");
            assert!(!update.ping_busy, "{row}");
            if row["outcome"]["kind"] == "exception" {
                assert_eq!(row["outcome"]["origin"]["module"], "lists");
                assert_eq!(row["outcome"]["origin"]["function"], "max");
                assert_eq!(update.error, Some(Error::NoCommonPingVersion), "{row}");
                assert!(requests.is_closed(), "{row}");
            } else {
                assert_eq!(update.error, None, "{row}");
                assert!(!requests.is_closed(), "{row}");
                let selected = row["outcome"]["state"]["selected"]
                    .as_u64()
                    .map(|v| v as u16);
                assert_eq!(requests.selected_ping_version(), selected, "{row}");
                assert_eq!(
                    requests.ping_version(),
                    selected.unwrap_or(minimum),
                    "{row}"
                );
                let action = if previous == Some(ping.version()) {
                    PingAction::None
                } else if previous.is_none() && ping.version() == 1 {
                    if direction == "responses" {
                        PingAction::ProbeV2
                    } else {
                        PingAction::None
                    }
                } else {
                    PingAction::Selected(selected.unwrap())
                };
                assert_eq!(update.ping, action, "{row}");
                if direction == "responses" {
                    assert!(update.completions[0].result.is_ok(), "{row}");
                }
            }
            checked += 1;
        }
    }
    assert_eq!(checked, 480);
}

#[test]
fn native_busy_pings_leave_negotiation_untouched_at_either_minimum() {
    let native = fixture();
    let rows = native["busy"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    for row in rows {
        let minimum = row["minimum"].as_u64().unwrap() as u16;
        let ping = example(&native, row["version"].as_u64().unwrap() as u16);
        let (mut requests, now) = configured(&native, minimum, 0);
        let pending = requests.start(&ping, now, None).unwrap();
        let update = requests.receive_ping(&ping, now);
        assert!(update.ping_busy);
        assert_eq!(update.ping, PingAction::None);
        assert_eq!(update.error, None);
        assert!(update.completions.is_empty());
        assert!(requests.contains(&pending.token));
        assert_eq!(requests.selected_ping_version(), None);
        assert_eq!(requests.ping_version(), minimum);
        let response =
            Message::response_error(Kind::Ping, b"already_pinging".to_vec(), Default::default())
                .unwrap()
                .encode_packet(Default::default())
                .unwrap();
        let sent = row["effects"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["action"] == "send")
            .unwrap();
        assert_eq!(hex::encode(response), sent["packet"]);
    }
}

#[test]
fn native_v2_timeout_keeps_connection_and_selects_v1_at_either_minimum() {
    let native = fixture();
    let rows = native["timeouts"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    for row in rows {
        let minimum = row["minimum"].as_u64().unwrap() as u16;
        let version = row["version"].as_u64().unwrap() as u16;
        let (mut requests, now) = configured(&native, minimum, 0);
        let pending = requests
            .start(&example(&native, version), now, None)
            .unwrap();
        assert_eq!(pending.deadline, now + DEFAULT_TIMEOUT);
        let update = requests.timeout(&pending.token, pending.deadline);
        assert_eq!(update.completions.len(), 1);
        assert_eq!(update.completions[0].token, pending.token);
        // Rust resolves its bounded ticket once. Native v2 leaves its caller
        // unanswered; native v1 additionally sends a stray disconnected reply.
        assert_eq!(update.completions[0].result, Err(Failure::Timeout));
        assert_eq!(requests.is_closed(), row["outcome"]["kind"] == "stop");
        assert!(requests.is_empty());
        assert_eq!(
            requests.selected_ping_version(),
            row["outcome"]["state"]["selected"]
                .as_u64()
                .map(|v| v as u16)
        );
        if version == 2 {
            assert_eq!(update.ping, PingAction::Selected(1));
            assert_eq!(requests.ping_version(), 1);
            let retry = requests
                .start(&example(&native, 1), pending.deadline, None)
                .unwrap();
            assert_ne!(retry.token, pending.token);
            assert!(
                requests
                    .timeout(&pending.token, pending.deadline)
                    .completions
                    .is_empty()
            );
            assert!(requests.contains(&retry.token));
            let reply = requests.receive_response(
                Message::response_ok(&example(&native, 1), Default::default()).unwrap(),
                pending.deadline,
            );
            assert_eq!(reply.completions[0].token, retry.token);
            assert!(reply.completions[0].result.is_ok());
            assert_eq!(reply.ping, PingAction::None);
            assert_eq!(requests.ping_version(), 1);
        }
    }
}
