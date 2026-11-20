use ae_chain::{
    repository::ledger::{Origin, Policy},
    resilience::{Config, Event, State},
};
use serde_json::{Value, json};

#[test]
fn original_resilience_lifecycle_finalization_and_origin_policy_match() {
    let data: Value =
        serde_json::from_str(include_str!("fixtures/erlang_resilience.json")).unwrap();
    let mut transitions = 0;
    let mut policies = 0;
    for trace in data["traces"].as_array().unwrap() {
        let mut state = State::default();
        let mut finalized = None;
        let mut height = 0;
        let mut key = true;
        for row in trace.as_array().unwrap() {
            let e = &row["event"];
            match e["kind"].as_str().unwrap() {
                "start" | "cold" | "synced" | "config" => {
                    let event = match e["kind"].as_str().unwrap() {
                        "cold" => {
                            state = State::default();
                            Event::Start
                        }
                        "start" => Event::Start,
                        "synced" => Event::Synced,
                        "config" => Event::ConfigChanged,
                        _ => unreachable!(),
                    };
                    let (next, ensure) = state.transition(
                        event,
                        Config {
                            depth: e["depth"].as_u64().unwrap(),
                            from_start: e["from_start"].as_bool().unwrap(),
                        },
                    );
                    if let Some(d) = ensure {
                        finalized = ae_chain::resilience::finalized_at(height, d).or(finalized);
                    }
                    state = next;
                }
                "top" => {
                    height = e["height"].as_u64().unwrap();
                    key = e["key"].as_bool().unwrap();
                    if key {
                        finalized = state.key_top_finalized(height).or(finalized);
                    }
                }
                "clear" => finalized = None,
                "same_top" => {}
                _ => panic!("unknown reference action"),
            }
            let expected = &row["expected"];
            for (name, value) in [
                ("cached", json!(state.configured_depth().unwrap_or(0))),
                ("active", json!(state.recorded_active())),
                ("synced", json!(state.synchronized())),
                ("finalized", json!(finalized)),
                ("height", json!(height)),
                ("key", json!(key)),
            ] {
                assert_eq!(value, expected[name], "{name}: {e}");
            }
            for p in expected["policies"].as_array().unwrap() {
                for (origin, name) in [(Origin::Sync, "sync"), (Origin::Gossip, "gossip")] {
                    assert_eq!(
                        Policy {
                            origin,
                            ..Default::default()
                        }
                        .allows(
                            p["height"].as_u64().unwrap(),
                            p["top"].as_u64().unwrap(),
                            state,
                            finalized
                        ),
                        p[name].as_bool().unwrap(),
                        "{e}: {p}"
                    );
                    policies += 1;
                }
            }
            transitions += 1;
        }
    }
    assert_eq!(transitions, 240);
    assert_eq!(policies, 36480);
    assert_eq!(data["restart_finalized"], 123);
}
