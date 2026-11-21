use ae_sync::chain::sync::lifecycle::{self, Task};
use serde_json::Value;
fn tasks(row: &Value) -> Vec<Task> {
    row.as_array()
        .unwrap()
        .iter()
        .map(|r| Task {
            id: r["id"].as_u64().unwrap(),
            target: r["target"].as_u64().unwrap(),
            suspect: r["suspect"].as_bool().unwrap(),
        })
        .collect()
}
#[test]
fn original_sync_progress_rounding_ties_suspects_and_zero_target_match() {
    let data: Value =
        serde_json::from_slice(include_bytes!("fixtures/erlang_sync_lifecycle.json")).unwrap();
    for row in data["progress"].as_array().unwrap() {
        let actual = lifecycle::progress(
            &tasks(&row["tasks"]),
            row["top_target"].as_u64().unwrap(),
            row["selected"].as_u64().unwrap(),
        );
        let expected = &row["result"];
        if expected["kind"] == "zero_target" {
            assert_eq!(actual, Err(lifecycle::Error::ZeroTarget));
        } else {
            let actual = actual.unwrap();
            assert_eq!(
                actual.syncing,
                expected["syncing"].as_bool().unwrap(),
                "{row}"
            );
            assert_eq!(
                actual.percent,
                expected["percent"]
                    .as_str()
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
                "{row}"
            );
            assert_eq!(actual.target, expected["target"].as_u64().unwrap(), "{row}");
            assert_eq!(actual.task, expected["task"].as_u64(), "{row}");
        }
    }
    assert_eq!(data["full_node_started"], false);
    println!(
        "Original global progress: {} rows",
        data["progress"].as_array().unwrap().len()
    );
}
