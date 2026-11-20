use ae_chain::repository::Error;
use ae_chain::repository::retention::{Config, Phase, Selection, State, Tree};
use ae_trie::Value;
use serde_json::Value as Json;

#[test]
fn original_gc_switch_guards_match_and_retention_does_not_reset_when_disabled() {
    let data: Json = serde_json::from_str(include_str!("fixtures/erlang_gc.json")).unwrap();
    let rows = data["rows"].as_array().unwrap();
    for row in rows {
        let config = Config {
            enabled: row["enabled"].as_bool().unwrap(),
            history: row["history"].as_u64().unwrap(),
            minimum_height: row["minimum_height"].as_u64().unwrap(),
            during_sync: row["during_sync"].as_bool().unwrap(),
            trees: Selection::default(),
        };
        let state = State {
            height: row["last"].as_u64().unwrap(),
            phase: if row["busy"] == true {
                Phase::Roots
            } else {
                Phase::Complete
            },
            ..Default::default()
        };
        assert_eq!(
            config
                .eligible(
                    row["height"].as_u64().unwrap(),
                    row["key"].as_bool().unwrap(),
                    row["synced"].as_bool().unwrap(),
                    &state
                )
                .unwrap(),
            row["switch"].as_bool().unwrap(),
            "{row}"
        );
    }
    let mut safer_disabled = 0;
    for row in data["gates"].as_array().unwrap() {
        let allowed = row["height"].as_u64().unwrap() >= row["last"].as_u64().unwrap();
        if row["enabled"] == true {
            assert_eq!(allowed, row["reachable"].as_bool().unwrap());
        } else {
            assert_eq!(row["reachable"], true);
            safer_disabled += usize::from(!allowed);
        }
    }
    assert_eq!(rows.len(), 6912);
    assert_eq!(data["gates"].as_array().unwrap().len(), 70);
    assert!(safer_disabled > 0);
    println!(
        "GC policy comparisons: {} switches, 70 gates, {safer_disabled} disabled-retention repairs",
        rows.len()
    );
}

#[test]
fn gc_state_codec_selection_and_height_bounds_are_strict() {
    let mut state = State::default();
    assert_eq!(State::decode(&state.encode()).unwrap(), state);
    assert!(Selection::new(&[]).is_err());
    let selection = Selection::new(&[Tree::Names, Tree::Oracles]).unwrap();
    state = state.start(100, selection).unwrap();
    assert_eq!(state.floors, [0, 0, 0, 0, 100, 100, 100, 100]);
    assert!(state.retains(4, 99));
    assert!(!state.retains(4, 98));
    assert!(state.retains(0, 0));
    assert!(matches!(state.start(100, selection), Err(Error::GcActive)));
    state.phase = Phase::Complete;
    assert!(matches!(
        state.start(99, selection),
        Err(Error::Pruned { .. })
    ));
    assert_eq!(State::decode(&state.encode()).unwrap(), state);
    let Value::List(fields) = Value::decode(&state.encode()).unwrap() else {
        panic!()
    };
    for (i, value) in [
        (0, Value::Bytes(vec![2])),
        (1, Value::Bytes(vec![0])),
        (4, Value::Bytes(vec![5])),
        (5, Value::Bytes(vec![1; 33])),
        (6, Value::Bytes(vec![0, 1])),
    ] {
        let mut fields = fields.clone();
        fields[i] = value;
        assert!(State::decode(&Value::List(fields).encode()).is_err());
    }
    let mut invalid = state.clone();
    invalid.floors[6] = 0;
    assert!(State::decode(&invalid.encode()).is_err());
    let encoded = state.encode();
    for n in 0..encoded.len() {
        assert!(State::decode(&encoded[..n]).is_err());
    }
    assert!(
        Config {
            history: 49,
            ..Default::default()
        }
        .eligible(100, true, true, &State::default())
        .is_err()
    );
    assert!(
        !Config {
            enabled: true,
            history: u64::MAX,
            ..Default::default()
        }
        .eligible(u64::MAX, true, true, &state)
        .unwrap()
    );
}
