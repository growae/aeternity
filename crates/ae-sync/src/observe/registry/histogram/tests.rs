use super::*;
use serde_json::{Value as Json, json};

fn decode(v: &Json) -> Value {
    if let Some(s) = v["integer"].as_str() {
        return Value::Integer(s.parse().unwrap());
    }
    if let Some(s) = v["float"].as_str() {
        return Value::Float(f64::from_bits(u64::from_str_radix(s, 16).unwrap()));
    }
    if let Some(s) = v["list"].as_array() {
        return Value::List(s.iter().map(decode).collect());
    }
    Value::Atom(v["atom"].as_str().unwrap().into())
}
fn encode(v: &Value) -> Json {
    match v {
        Value::Integer(n) => json!({"integer":n.to_string()}),
        Value::Float(n) => json!({"float":format!("{:016x}",n.to_bits())}),
        _ => panic!("nonnumeric statistic {v:?}"),
    }
}
fn check(h: &Histogram, now: i128, points: &Points, expected: &Json, context: &str) {
    let result = h.values(now, points);
    if expected.get("class").is_some() {
        assert_eq!(result, Err(Error::Arithmetic), "{context}: {expected}");
        return;
    }
    let actual: Vec<Json> = result
        .unwrap_or_else(|e| panic!("{context}: {e:?}"))
        .iter()
        .map(|(p, v)| {
            let key = match p {
                Part::Integer(n) => json!({"integer":n.to_string()}),
                Part::Atom(s) => json!({"atom":s}),
                _ => panic!("unexpected datapoint"),
            };
            json!({"tuple":[key,encode(v)]})
        })
        .collect();
    assert_eq!(
        json!({"result":{"tuple":[{"atom":"ok"},{"list":actual}]}}),
        *expected,
        "{context}"
    );
}
fn config(row: &Json) -> Config {
    Config {
        module: match row["module"].as_str().unwrap() {
            "exometer_slot_slide" => Module::Slots,
            "exometer_slide" => Module::Plain,
            _ => panic!(),
        },
        rounding: match &row["truncate"] {
            Json::Bool(true) => Rounding::Truncate,
            Json::Bool(false) => Rounding::Preserve,
            _ => Rounding::Round,
        },
        time_span_ms: row["time_span"].as_u64().unwrap_or(60),
        slot_period_ms: 10,
        keep_high: row["keep_high"].as_u64().unwrap() as usize,
        max_n: row["max_n"]["integer"].as_str().map(|s| s.parse().unwrap()),
    }
}
fn part(s: &str) -> Part {
    if let Ok(n) = s.parse() {
        Part::Integer(n)
    } else {
        Part::atom(s)
    }
}

#[test]
fn original_histogram_all_192_rows() {
    let fixture: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_histogram.json"
    ))
    .unwrap();
    let selected = Points::Many(
        ["unknown", "mean", "median", "n", "50", "999", "mean"]
            .map(part)
            .to_vec(),
    );
    let mut state = None;
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 192);
    for (index, row) in rows.iter().enumerate() {
        let now = i128::from(row["time_ms"].as_i64().unwrap());
        if row["operation"] == "initial" {
            state = Some(Histogram::new(config(row), Limits::default(), now).unwrap());
        }
        let h = state.as_mut().unwrap();
        match row["operation"].as_str().unwrap() {
            "update" | "update_after_reset" => h.update(now, &decode(&row["input"])).unwrap(),
            "reset" => h.reset(),
            _ => (),
        }
        for (label, points) in [
            ("default", &Points::default_points()),
            ("selected", &selected),
        ] {
            check(
                h,
                now,
                points,
                &row[label],
                &format!("row {index} {label} {row}"),
            );
        }
    }
}

fn check_edges(source: &str, configurations: usize) {
    let fixture: Json = serde_json::from_str(source).unwrap();
    let selected = Points::Many(
        [
            "999", "50", "90", "999", "total", "median", "unknown", "97", "100",
        ]
        .map(part)
        .to_vec(),
    );
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), configurations);
    for (case_index, case) in cases.iter().enumerate() {
        let mut h = Histogram::new(config(case), Limits::default(), 0).unwrap();
        for (index, row) in case["rows"].as_array().unwrap().iter().enumerate() {
            let now = i128::from(row["time_ms"].as_i64().unwrap());
            let context = format!(
                "case {case_index} {} {:?} row {index}",
                case["name"],
                config(case)
            );
            match row["operation"].as_str().unwrap() {
                "batch" => {
                    let Value::List(v) = decode(&row["input"]) else {
                        panic!()
                    };
                    for v in v {
                        h.update(now, &v).unwrap();
                    }
                    continue;
                }
                "update" => {
                    let result = h.update(now, &decode(&row["input"]));
                    if row["update"]["ok"] == true {
                        assert_eq!(result, Ok(()), "{context}");
                    } else {
                        assert_eq!(result, Err(Error::Arithmetic), "{context}");
                    }
                }
                "reset" => h.reset(),
                "read" => (),
                _ => panic!(),
            }
            for (label, points) in [
                ("default", &Points::default_points()),
                ("selected", &selected),
            ] {
                check(&h, now, points, &row[label], &format!("{context} {label}"));
            }
        }
    }
}

#[test]
fn original_histogram_edges_156_configurations() {
    check_edges(
        include_str!("../../../../tests/fixtures/erlang_metrics_histogram_edges.json"),
        156,
    );
}

#[test]
fn original_expired_current_slot_materializes_before_filtering() {
    check_edges(
        include_str!("../../../../tests/fixtures/erlang_metrics_histogram_expiry.json"),
        12,
    );
}

#[test]
fn quotas_and_numeric_errors_leave_existing_statistics_intact() {
    let config = Config {
        module: Module::Plain,
        ..Config::default()
    };
    let limits = Limits {
        retained_bytes: 512,
        ..Limits::default()
    };
    let mut h = Histogram::new(config, limits, 0).unwrap();
    for i in 0..5 {
        h.update(0, &Value::Integer(i.into())).unwrap();
    }
    let before = h.values(0, &Points::One(Part::atom("mean"))).unwrap();
    assert_eq!(h.update(0, &Value::Integer(6.into())), Err(Error::Limit));
    assert_eq!(
        h.values(0, &Points::One(Part::atom("mean"))).unwrap(),
        before
    );
    let mut h = Histogram::new(Config::default(), Limits::default(), 0).unwrap();
    h.update(0, &Value::Integer(4.into())).unwrap();
    let before = h.values(0, &Points::default_points()).unwrap();
    assert_eq!(
        h.update_with(0, &Value::Integer(6.into()), |_, _| Err(Error::Busy)),
        Err(Error::Busy)
    );
    assert_eq!(
        h.update(0, &Value::Float(f64::NAN)),
        Err(Error::InvalidValue)
    );
    h.update(0, &Value::Atom("ignore".into())).unwrap();
    assert_eq!(h.values(0, &Points::default_points()).unwrap(), before);
    assert_eq!(
        h.update(0, &Value::Integer(num_bigint::BigInt::from(1u8) << 4096)),
        Err(Error::Limit)
    );
    assert!(
        Histogram::new(
            Config {
                slot_period_ms: 0,
                ..Config::default()
            },
            Limits::default(),
            0
        )
        .is_err()
    );
    assert!(h.values(0, &Points::Many(vec![])).unwrap().is_empty());
}
