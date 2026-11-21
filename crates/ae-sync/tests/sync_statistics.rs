use ae_sync::chain::sync::{
    Point,
    statistics::{self, Aggregate, Error, Sample, Task},
};
use ae_types::Hash256;
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn target(op: &Value) -> Point {
    let mut hash = [0; 32];
    hash[24..].copy_from_slice(&op["hash"].as_u64().unwrap().to_be_bytes());
    Point {
        height: op["height"].as_u64().unwrap(),
        hash: Hash256(hash),
    }
}
fn samples(op: &Value) -> Vec<Sample> {
    let start = op["start"].as_u64().unwrap();
    (start..start + op["count"].as_u64().unwrap())
        .map(|i| {
            let t0 = i128::from(i) * i128::from(op["stride"].as_i64().unwrap());
            Sample {
                height: i + 1,
                start_us: t0,
                end_us: t0 + i128::from(op["duration"].as_i64().unwrap()),
                micro_blocks: i % 3,
                transactions: 2 * (i % 5),
                work_us: op["work"].as_i64().map(i128::from),
            }
        })
        .collect()
}
fn aggregate(a: Aggregate) -> Value {
    let mut v = json!({"step":a.step,"limit":a.limit,"n":a.entries,"gs":a.generations,
        "mbs":a.micro_blocks,"txs":a.transactions,"t":a.work_us});
    if let Some(t) = a.start_us {
        v["t0"] = json!(t);
    }
    if let Some(t) = a.end_us {
        v["t1"] = json!(t);
    }
    v
}
fn sample(s: Option<Sample>) -> Value {
    s.map_or(Value::Null, |s| {
        json!({"height":s.height,"t0":s.start_us,"t1":s.end_us,
        "mbs":s.micro_blocks,"txs":s.transactions,"t":s.work_us})
    })
}
fn check_log(a: Aggregate, native: &Value) {
    let rates = a.rates().unwrap().unwrap();
    assert_eq!(native[0], a.generations);
    assert_eq!(native[2], a.micro_blocks);
    assert_eq!(native[3], a.transactions);
    for (i, actual) in [
        (1, rates.milliseconds_per_generation),
        (4, rates.wall_seconds),
        (5, rates.work_seconds),
        (6, rates.efficiency_percent),
    ] {
        assert_eq!(
            actual.to_bits(),
            native[i]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap()
                .to_bits(),
            "{native}"
        );
    }
}
fn check_snapshot(task: &Task, native: &Value) {
    let s = task.snapshot();
    assert_eq!(s.id, native["id"].as_u64().unwrap());
    assert_eq!(
        s.target.height,
        native["target"]["height"].as_u64().unwrap()
    );
    assert_eq!(
        s.target
            .hash
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
        native["target"]["hash"].as_str().unwrap()
    );
    assert_eq!(aggregate(s.total), native["total"]);
    assert_eq!(json!(s.levels.map(aggregate)), native["levels"]);
    assert_eq!(json!(s.window_len), native["window_len"]);
    assert_eq!(sample(s.newest), native["newest"]);
    assert_eq!(sample(s.oldest), native["oldest"]);
    assert_eq!(s.last_update_ms.is_some(), native["last_update_set"]);
    match s.generations_per_minute {
        Ok(speed) => {
            assert_eq!(native["estimate"]["kind"], "ok");
            assert_eq!(
                speed.to_bits(),
                native["estimate"]["speed"]
                    .as_str()
                    .unwrap()
                    .parse::<f64>()
                    .unwrap()
                    .to_bits()
            );
        }
        Err(Error::UndefinedEstimate | Error::EmptyEstimate) => {
            assert_eq!(native["estimate"]["reason"], "badarg")
        }
        Err(Error::ZeroDuration) => assert_eq!(native["estimate"]["reason"], "badarith"),
        error => panic!("unexpected estimate: {error:?}"),
    }
    match s.total.rates() {
        Ok(Some(_)) => check_log(s.total, &native["total_log"]["logs"][0]),
        Ok(None) => assert_eq!(native["total_log"]["logs"], json!([])),
        Err(Error::ZeroDuration) => assert_eq!(native["total_log"]["result"]["reason"], "badarith"),
        error => panic!("unexpected total: {error:?}"),
    }
}

#[test]
fn original_statistics_callbacks_rollovers_estimates_and_failure_boundaries_match() {
    let fixture: Value =
        serde_json::from_slice(include_bytes!("fixtures/erlang_sync_statistics.json")).unwrap();
    let mut rows = 0;
    for case in fixture["cases"].as_array().unwrap() {
        let mut tasks = BTreeMap::<u64, Task>::new();
        let mut counts = BTreeMap::<u64, [u64; 5]>::new();
        for (i, row) in case["rows"].as_array().unwrap().iter().enumerate() {
            rows += 1;
            let op = &row["operation"];
            let id = op["id"].as_u64().unwrap();
            let before: BTreeMap<_, _> = tasks
                .iter()
                .map(|(id, t)| (*id, t.snapshot().last_update_ms))
                .collect();
            let now = 1000 + i as i128;
            let result: Result<Vec<Aggregate>, Error> = match op["kind"].as_str().unwrap() {
                "new" => {
                    if let Some(task) = tasks.get_mut(&id) {
                        task.update_target(target(op), now);
                    } else {
                        tasks.insert(id, Task::new(id, target(op), 65_536).unwrap());
                        counts.insert(id, [0; 5]);
                    }
                    Ok(vec![])
                }
                "post" => match tasks.get_mut(&id) {
                    Some(task) => task.post(&samples(op), now),
                    None => Ok(vec![]),
                },
                "done" => match tasks.get(&id) {
                    Some(task) => {
                        let total = task.snapshot().total;
                        match total.rates() {
                            Ok(rates) => {
                                tasks.remove(&id);
                                counts.remove(&id);
                                Ok(rates.map_or(vec![], |_| vec![total]))
                            }
                            Err(error) => Err(error),
                        }
                    }
                    None => Ok(vec![]),
                },
                kind => panic!("{kind}"),
            };
            match result {
                Ok(logs) => {
                    assert_eq!(row["outcome"]["kind"], "ok", "{row}");
                    assert_eq!(logs.len(), row["logs"].as_array().unwrap().len());
                    for (log, native) in logs.into_iter().zip(row["logs"].as_array().unwrap()) {
                        check_log(log, native);
                        if op["kind"] == "post" {
                            let level = [1000, 5000, 10000, 20000, 100000]
                                .iter()
                                .position(|n| *n == log.generations)
                                .unwrap();
                            counts.get_mut(&id).unwrap()[level] += 1;
                        }
                    }
                }
                Err(Error::ZeroDuration) => assert_eq!(row["outcome"]["reason"], "badarith"),
                error => panic!("{error:?}: {row}"),
            }
            let touched: Vec<_> = tasks
                .iter()
                .filter(|(id, t)| {
                    t.snapshot().last_update_ms.is_some()
                        && before.get(id).copied().flatten() != t.snapshot().last_update_ms
                })
                .map(|(id, _)| *id)
                .collect();
            assert_eq!(json!(touched), row["touched"], "{row}");
            assert_eq!(tasks.len(), row["state"].as_array().unwrap().len());
            for (task, native) in tasks.values().zip(row["state"].as_array().unwrap()) {
                check_snapshot(task, native);
                assert_eq!(task.snapshot().rollovers, counts[&task.snapshot().id]);
            }
        }
    }
    assert_eq!(rows, 48);
    assert_eq!(fixture["unknown"]["reason"], "no_estimate");
    assert_eq!(
        statistics::Status::default().estimate(99),
        Err(Error::UnknownTask)
    );
    assert_eq!(fixture["full_node_started"], false);
    assert_eq!(fixture["sockets_opened"], 0);
    println!(
        "Original statistics: {rows} callback steps, all five levels through 100000 generations"
    );
}

#[test]
fn local_statistics_capacity_and_overflow_fail_without_partial_changes() {
    let point = Point {
        height: 1,
        hash: Hash256([1; 32]),
    };
    assert!(matches!(Task::new(1, point, 0), Err(Error::Capacity)));
    assert!(matches!(Task::new(1, point, 65_537), Err(Error::Capacity)));
    let mut task = Task::new(1, point, 2).unwrap();
    let normal = Sample {
        height: 1,
        start_us: 0,
        end_us: 1,
        micro_blocks: 1,
        transactions: 2,
        work_us: None,
    };
    task.post(&[normal], 1).unwrap();
    let before = task.snapshot();
    assert_eq!(task.post(&[normal; 3], 2), Err(Error::Capacity));
    assert_eq!(task.snapshot(), before);
    for invalid in [
        Sample {
            start_us: i128::MIN,
            end_us: i128::MAX,
            ..normal
        },
        Sample {
            micro_blocks: u64::MAX,
            ..normal
        },
        Sample {
            work_us: Some(i128::MAX),
            ..normal
        },
    ] {
        assert_eq!(task.post(&[invalid], 2), Err(Error::Overflow));
        assert_eq!(task.snapshot(), before);
    }
}
