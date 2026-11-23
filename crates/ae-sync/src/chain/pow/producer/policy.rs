use super::*;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Decision {
    Ignore,
    Clear,
    Wait,
    Sign,
}
pub(super) fn signing(leader: bool, present: bool, fresh: bool, sleeping: bool) -> Decision {
    if !leader {
        Decision::Ignore
    } else if !present || !fresh {
        Decision::Clear
    } else if sleeping {
        Decision::Wait
    } else {
        Decision::Sign
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Action {
    Start,
    Stop,
    Sleep,
    None,
}
pub(super) fn origin(leader: bool, origin: publication::Origin, kind: publication::Kind) -> Action {
    use publication::{Kind, Origin};
    match (leader, origin, kind) {
        (true, Origin::Created | Origin::Received, Kind::Key) => Action::Start,
        (false, Origin::Received, Kind::Key) => Action::Stop,
        (true, Origin::Created, Kind::Micro) => Action::Sleep,
        _ => Action::None,
    }
}

pub(super) fn leader(available: bool, miner: AccountPublicKey, signer: AccountPublicKey) -> bool {
    available && miner == signer
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_native_notification_origin_and_leader_decisions_match() {
        let native: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/erlang_pow_micro_producer.json"
        ))
        .unwrap();
        for row in native["notifications"].as_array().unwrap() {
            let value = |key: &str| row[key].as_bool().unwrap();
            let (effects, candidate) = match signing(
                value("leader"),
                value("present"),
                value("fresh"),
                value("sleep"),
            ) {
                Decision::Ignore => (vec![], false),
                Decision::Clear => (vec!["fetch"], false),
                Decision::Wait => (vec!["fetch"], true),
                Decision::Sign => (vec!["fetch", "sign", "admit"], false),
            };
            assert_eq!(
                serde_json::json!(effects),
                row["result"]["effects"],
                "{row}"
            );
            assert_eq!(candidate, row["result"]["candidate"].as_bool().unwrap());
        }
        for row in native["origins"].as_array().unwrap() {
            use publication::{Kind, Origin};
            let (o, k) = match row["origin"].as_str().unwrap() {
                "block_created" => (Origin::Created, Kind::Key),
                "block_received" => (Origin::Received, Kind::Key),
                "micro_block_created" => (Origin::Created, Kind::Micro),
                "micro_block_received" => (Origin::Received, Kind::Micro),
                "block_synced" => (Origin::Synced, Kind::Key),
                _ => unreachable!(),
            };
            let effects = match origin(row["leader"].as_bool().unwrap(), o, k) {
                Action::Start => vec!["start_generator"],
                Action::Stop => vec!["stop_generator"],
                Action::Sleep => vec!["sleep"],
                Action::None => vec![],
            };
            assert_eq!(
                serde_json::json!(effects),
                row["result"]["effects"],
                "{row}"
            );
        }
        for row in native["leadership"].as_array().unwrap() {
            assert_eq!(
                leader(
                    row["available"].as_bool().unwrap(),
                    AccountPublicKey([if row["own"].as_bool().unwrap() { 1 } else { 2 }; 32]),
                    AccountPublicKey([1; 32])
                ),
                row["leader"].as_bool().unwrap()
            );
        }
    }
}
