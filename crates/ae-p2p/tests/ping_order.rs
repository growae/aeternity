use ae_p2p::{
    message::{Kind, Message},
    requests::{Error, Failure, Limits, PingAction, Requests},
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

fn native() -> Value {
    serde_json::from_str(include_str!("fixtures/erlang_connection_lifecycle.json")).unwrap()
}
fn packet(row: &Value) -> Message {
    Message::decode_packet(
        &hex::decode(row["packet"].as_str().unwrap()).unwrap(),
        Default::default(),
    )
    .unwrap()
}
fn ping(native: &Value, version: u16, versions: &str) -> Message {
    packet(
        native["incoming"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["version"] == version && r["versions"] == versions)
            .unwrap(),
    )
}
fn seed(native: &Value, minimum: u16, previous: u16) -> (Requests, Instant) {
    let mut requests = Requests::new(Limits::default(), minimum).unwrap();
    let mut now = Instant::now();
    match previous {
        0 => (),
        1 => {
            let pending = requests
                .start(&ping(native, 2, "normal"), now, None)
                .unwrap();
            now = pending.deadline;
            requests.timeout(&pending.token, now);
        }
        2 => {
            requests.receive_ping(&ping(native, 2, "normal"), now);
        }
        _ => unreachable!(),
    }
    (requests, now)
}

#[test]
fn original_ping_callbacks_reach_response_or_caller_before_negotiation_failure() {
    let native = native();
    let mut checked = 0;
    let mut native_failures = 0;
    for direction in ["incoming", "responses"] {
        for row in native[direction].as_array().unwrap() {
            let previous = row["previous"].as_u64().unwrap() as u16;
            let before = (previous != 0).then_some(previous);
            let (mut requests, now) =
                seed(&native, row["minimum"].as_u64().unwrap() as u16, previous);
            let ping = packet(row);
            let update = if direction == "incoming" {
                requests.receive_ping_deferred(&ping, now)
            } else {
                let pending = requests.start(&ping, now, None).unwrap();
                let envelope = Message::response_ok(&ping, Default::default()).unwrap();
                let update = requests.receive_response_deferred(envelope.clone(), now);
                assert_eq!(update.completions.len(), 1);
                assert_eq!(update.completions[0].token, pending.token);
                assert_eq!(update.completions[0].result, Ok(envelope));
                update
            };
            assert_eq!(update.error, None, "{row}");
            assert_eq!(update.ping_before, before);
            assert_eq!(requests.selected_ping_version(), before);
            assert!(!requests.is_closed());
            let receipt = update.ping_deferred.unwrap();
            let committed = requests.finish_ping(&receipt).unwrap();
            if row["outcome"]["kind"] == "exception" {
                native_failures += 1;
                let effects = row["effects"].as_array().unwrap();
                let earlier = if direction == "incoming" {
                    "send"
                } else {
                    "caller_reply"
                };
                let earlier = effects.iter().position(|e| e["action"] == earlier).unwrap();
                let negotiation = effects
                    .iter()
                    .rposition(|e| e["action"] == "minimum_ping")
                    .unwrap();
                assert!(earlier < negotiation, "{row}");
                assert_eq!(committed.error, Some(Error::NoCommonPingVersion));
                assert!(requests.is_closed());
            } else {
                assert_eq!(committed.error, None, "{row}");
                assert_eq!(committed.ping, update.ping);
                assert_eq!(
                    requests.selected_ping_version(),
                    row["outcome"]["state"]["selected"]
                        .as_u64()
                        .map(|v| v as u16)
                );
                assert!(!requests.is_closed());
            }
            checked += 1;
        }
    }
    assert_eq!((checked, native_failures), (480, 48));
}

#[test]
fn deferred_ping_fences_request_slots_timers_and_other_connection_receipts() {
    let native = native();
    let (mut requests, now) = seed(&native, 1, 0);
    let info = Message::new(Kind::GetNodeInfo, 1, BTreeMap::new(), Default::default()).unwrap();
    let outstanding = requests
        .start(&info, now, Some(Duration::from_millis(1)))
        .unwrap();
    let update = requests.receive_ping_deferred(&ping(&native, 2, "normal"), now);
    let token = update.ping_deferred.unwrap();
    assert_eq!(
        requests.start(&info, now, None).unwrap_err(),
        Error::AlreadyPending
    );
    assert!(requests.next_deadline().is_none());
    assert!(
        requests
            .timeout(&outstanding.token, outstanding.deadline)
            .completions
            .is_empty()
    );
    assert!(requests.expire(outstanding.deadline).completions.is_empty());
    assert_eq!(
        requests
            .receive_ping(&ping(&native, 1, "normal"), now)
            .error,
        Some(Error::AlreadyPending)
    );
    let (mut other, _) = seed(&native, 1, 0);
    let other_token = other
        .receive_ping_deferred(&ping(&native, 2, "normal"), now)
        .ping_deferred
        .unwrap();
    assert_eq!(
        requests.finish_ping(&other_token).unwrap_err(),
        Error::InvalidRequest
    );
    assert_eq!(
        other.finish_ping(&token).unwrap_err(),
        Error::InvalidRequest
    );
    assert_eq!(requests.selected_ping_version(), None);
    assert_eq!(
        requests.finish_ping(&token).unwrap().ping,
        PingAction::Selected(2)
    );
    assert_eq!(
        requests.finish_ping(&token).unwrap_err(),
        Error::InvalidRequest
    );
    assert_eq!(requests.next_deadline(), Some(outstanding.deadline));
    let expired = requests.expire(outstanding.deadline);
    assert_eq!(expired.completions[0].token, outstanding.token);
    assert_eq!(expired.completions[0].result, Err(Failure::Timeout));
    assert!(requests.is_closed());
    assert!(!other.is_closed());
    other.finish_ping(&other_token).unwrap();
}

#[test]
fn deferred_busy_and_failed_callbacks_retire_only_their_own_request_state() {
    let native = native();
    let (mut requests, now) = seed(&native, 2, 0);
    let ping = ping(&native, 2, "none");
    let outgoing = requests.start(&ping, now, None).unwrap();
    let busy = requests.receive_ping_deferred(&ping, now);
    assert!(busy.ping_busy);
    assert_eq!(busy.ping, PingAction::None);
    let token = busy.ping_deferred.unwrap();
    assert!(requests.finish_ping(&token).unwrap().completions.is_empty());
    assert!(requests.contains(&outgoing.token));
    let response = requests.receive_response_deferred(
        Message::response_ok(&ping, Default::default()).unwrap(),
        now,
    );
    assert!(response.completions[0].result.is_ok());
    assert_eq!(response.completions[0].token, outgoing.token);
    assert!(!requests.is_closed());
    assert_eq!(
        requests.finish_ping(&token).unwrap_err(),
        Error::InvalidRequest
    );
    let failure = requests
        .finish_ping(&response.ping_deferred.unwrap())
        .unwrap();
    assert_eq!(failure.error, Some(Error::NoCommonPingVersion));
    assert!(failure.completions.is_empty());
    assert!(requests.is_closed());
}
