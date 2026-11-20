use super::*;
use serde_json::Value as Json;

fn info(enabled: bool, synced: bool) -> Info {
    Info {
        version: 0,
        running: true,
        enabled,
        synced,
        faulted: false,
        failures: 0,
        pages: 0,
        observed_revision: 0,
        state: State::default(),
        last_error: None,
    }
}
#[test]
fn original_worker_controls_retry_and_startup_baseline_match() {
    let data: Json = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_gc_worker.json"
    ))
    .unwrap();
    let traces = data["traces"].as_array().unwrap();
    for trace in traces {
        let mut state = info(
            trace["enabled"].as_bool().unwrap(),
            trace["synced"].as_bool().unwrap(),
        );
        for (action, expected) in trace["actions"]
            .as_array()
            .unwrap()
            .iter()
            .zip(trace["states"].as_array().unwrap())
        {
            let before = state.enabled;
            match action.as_str().unwrap() {
                "enable" => assert_eq!(state.set_enabled(true), before),
                "disable" => assert_eq!(state.set_enabled(false), before),
                "sync_busy" => state.sync_done(true),
                "sync_idle" => state.sync_done(false),
                _ => panic!(),
            }
            assert_eq!(before, expected["previous"].as_bool().unwrap());
            assert_eq!(state.enabled, expected["enabled"].as_bool().unwrap());
            assert_eq!(state.synced, expected["synced"].as_bool().unwrap());
        }
    }
    for row in data["failures"].as_array().unwrap() {
        let mut state = info(row["enabled"].as_bool().unwrap(), true);
        state.failures = row["attempt"].as_u64().unwrap() as u8 - 1;
        if row["reason"] != "normal" {
            state.failed("injected".into());
        }
        assert_eq!(state.enabled, row["first_enabled"].as_bool().unwrap());
        if row["attempts"] == serde_json::json!([2]) {
            assert!(!state.faulted);
            state.failed("injected retry".into());
            assert!(state.faulted);
        }
        assert_eq!(state.enabled, row["second_enabled"].as_bool().unwrap());
    }
    for row in data["baselines"].as_array().unwrap() {
        let state = State {
            epoch: u64::from(!row["last"].is_null()),
            height: row["last"].as_u64().unwrap_or(0),
            ..Default::default()
        };
        let baseline = schedule_baseline(&state, row["top"].as_u64().unwrap_or(0));
        assert_eq!(state.height.max(baseline), row["result"].as_u64().unwrap());
    }
    assert_eq!(traces.len(), 256);
    assert_eq!(data["failures"].as_array().unwrap().len(), 8);
    assert_eq!(data["baselines"].as_array().unwrap().len(), 12);
    println!("GC worker comparisons: 256 control traces, 8 scanner exits, 12 startup baselines");
}
