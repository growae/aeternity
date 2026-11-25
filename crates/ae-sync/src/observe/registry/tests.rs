use super::*;
use num_bigint::BigInt;
use serde_json::{Value as Json, json};
use std::{
    sync::atomic::{AtomicI64, Ordering},
    time::Instant,
};

struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now(&self) -> reporter::Time {
        reporter::Time {
            monotonic: Instant::now(),
            unix_microseconds: 1_258_000_000_000_000
                + i128::from(self.0.load(Ordering::Relaxed)) * 1000,
        }
    }
}
fn name(s: &str) -> Name {
    s.split('.').map(Part::atom).collect()
}
fn integer(n: i64) -> Value {
    Value::Integer(n.into())
}
fn decode(v: &Json) -> Value {
    if let Some(s) = v["integer"].as_str() {
        return Value::Integer(s.parse().unwrap());
    }
    if let Some(s) = v["float"].as_str() {
        return Value::Float(f64::from_bits(u64::from_str_radix(s, 16).unwrap()));
    }
    if let Some(s) = v["atom"].as_str() {
        return Value::Atom(s.into());
    }
    if let Some(s) = v["binary"].as_str() {
        return Value::Binary(hex::decode(s).unwrap());
    }
    if let Some(v) = v["list"].as_array() {
        return Value::List(v.iter().map(decode).collect());
    }
    Value::Tuple(v["tuple"].as_array().unwrap().iter().map(decode).collect())
}
fn encode(v: &Value) -> Json {
    match v {
        Value::Integer(n) => json!({"integer":n.to_string()}),
        Value::Float(n) => json!({"float":format!("{:016x}",n.to_bits())}),
        Value::Atom(n) => json!({"atom":n}),
        Value::Binary(n) => json!({"binary":hex::encode_upper(n)}),
        Value::List(n) => json!({"list":n.iter().map(encode).collect::<Vec<_>>()}),
        Value::Tuple(n) => json!({"tuple":n.iter().map(encode).collect::<Vec<_>>()}),
    }
}
fn atom(s: &str) -> Json {
    json!({"atom":s})
}
fn tuple(v: Vec<Json>) -> Json {
    json!({"tuple":v})
}
fn outcome(r: Result<(), Error>) -> Json {
    match r {
        Ok(()) => json!({"result":atom("ok")}),
        Err(Error::InvalidValue) => json!({"class":"error","reason":atom("badarg")}),
        Err(Error::Exists) => json!({"class":"error","reason":atom("exists")}),
        Err(Error::TypeMismatch) => json!({"class":"error","reason":atom("type_mismatch")}),
        Err(Error::InvalidStatus) => {
            json!({"class":"error","reason":tuple(vec![atom("case_clause"),tuple(vec![atom("status"),atom("enabled")])])})
        }
        Err(e) => {
            json!({"result":tuple(vec![atom("error"),atom(match e { Error::NotFound => "not_found", Error::Disabled => "disabled", _ => panic!("unexpected {e:?}") })])})
        }
    }
}
fn reading(r: Result<Reading, Error>) -> Json {
    match r {
        Err(e) => outcome(Err(e)),
        Ok(Reading::Disabled) => json!({"result":tuple(vec![atom("ok"),atom("disabled")])}),
        Ok(Reading::Values(values)) => {
            json!({"result":tuple(vec![atom("ok"),json!({"list":values.iter().map(|(p,v)|tuple(vec![match p { Part::Atom(s) => atom(s), _ => panic!() },encode(v)])).collect::<Vec<_>>()})])})
        }
    }
}
fn points() -> Points {
    Points::Many(
        [
            "value",
            "value16",
            "value32",
            "value64",
            "unknown",
            "ms_since_reset",
            "value",
        ]
        .map(Part::atom)
        .to_vec(),
    )
}
fn check(store: &Store, n: &Name, row: &Json) {
    assert_eq!(reading(store.get_value(n, &points())), row["value"]);
    assert_eq!(
        reading(store.get_value(n, &Points::default_points())),
        row["default"]
    );
    if let Ok(info) = store.info(n) {
        assert_eq!(
            json!({"result":atom(if info.entry.enabled { "enabled" } else { "disabled" })}),
            row["status"]
        );
        assert_eq!(
            json!({"result":if info.datapoints.is_some() { json!({"list":[atom("value"),atom("ms_since_reset")]}) } else { atom("undefined") }}),
            row["datapoints"]
        );
    }
}

#[test]
fn original_scalar_rows_preserve_large_signed_counters_gauges_and_disabled_state() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_registry.json"
    ))
    .unwrap();
    let clock = Arc::new(TestClock(AtomicI64::new(1000)));
    let store = Store::with_clock(Limits::default(), clock.clone()).unwrap();
    let rows = native["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 56);
    for row in rows {
        let kind = if row["kind"] == "counter" {
            Kind::Counter
        } else {
            Kind::Gauge
        };
        let n = name("test.metric");
        clock.0.store(1000, Ordering::Relaxed);
        store.create(&n, kind, true).unwrap();
        store.update(&n, &integer(7)).unwrap();
        if row["status"] == 0 {
            store.set_enabled(&n, false).unwrap();
        }
        clock.0.store(2000, Ordering::Relaxed);
        check(&store, &n, &row["before"]);
        assert_eq!(
            outcome(store.update(&n, &decode(&row["input"]))),
            row["update"]
        );
        check(&store, &n, &row["after_update"]);
        assert_eq!(outcome(store.reset(&n)), row["reset"]);
        check(&store, &n, &row["after_reset"]);
        store.remove(&n).unwrap();
    }
    let n = name("missing");
    assert_eq!(
        outcome(store.update(&n, &integer(1))),
        native["missing"]["update"]
    );
    assert_eq!(outcome(store.reset(&n)), native["missing"]["reset"]);
    assert_eq!(
        reading(store.get_value(&n, &Points::default_points())),
        native["missing"]["read"]
    );
    assert_eq!(store.retained_bytes(), 0);
}

#[test]
fn original_admin_creation_status_and_alias_rows_match_without_claiming_cache_or_admin_recovery() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_registry_owner.json"
    ))
    .unwrap();
    let clock = Arc::new(TestClock(AtomicI64::new(2000)));
    let store = Store::with_clock(Limits::default(), clock).unwrap();
    let n = name("ae.epoch.counter");
    let g = name("ae.epoch.gauge");
    let alias = Alias::Binary(b"counter".to_vec());
    let p = Part::atom("value");
    let mut compared = 0;
    for row in native["rows"].as_array().unwrap() {
        let id = row["id"].as_str().unwrap();
        let r = match id {
            "new_counter" | "duplicate_counter" => outcome(store.create(&n, Kind::Counter, true)),
            "ensure_counter" | "ensure_after_delete" => {
                outcome(store.ensure(&n, Kind::Counter, true))
            }
            "ensure_wrong_type" => outcome(store.ensure(&n, Kind::Gauge, true)),
            "counter_update" => outcome(store.update(&n, &integer(12))),
            "disable_counter" => outcome(store.set_enabled(&n, false)),
            "disabled_counter_update" => outcome(store.update(&n, &integer(9))),
            "reenable_counter" => outcome(store.set_enabled(&n, true)),
            "alias_add" | "alias_duplicate" => match store.add_alias(&alias, &n, &p) {
                Err(Error::Exists) => json!({"result":tuple(vec![atom("error"),atom("exists")])}),
                r => outcome(r),
            },
            "alias_value" | "alias_after_delete" => match store.alias_value(&alias) {
                Ok(Reading::Values(v)) => json!({"result":tuple(vec![atom("ok"),encode(&v[0].1)])}),
                r => reading(r),
            },
            "new_gauge" => outcome(store.create(&g, Kind::Gauge, true)),
            "gauge_float" => outcome(store.update(&g, &Value::Float(1.5))),
            "gauge_atom" => outcome(store.update(&g, &Value::Atom("foo".into()))),
            "gauge_cache_on" | "gauge_cached_update" | "gauge_cached_reset" | "gauge_cache_off" => {
                continue;
            }
            "delete_counter" | "delete_missing" => match store.remove(&n) {
                Ok(()) => outcome(Ok(())),
                Err(StoreError::NotFound) => outcome(Err(Error::NotFound)),
                e => panic!("{e:?}"),
            },
            _ => panic!("unmapped native row {id}"),
        };
        assert_eq!(r, row["result"], "{id}");
        check(
            &store,
            if id.contains("gauge") { &g } else { &n },
            &row["after_operation"],
        );
        compared += 1;
    }
    assert_eq!(compared, 18);
}

#[test]
fn scalar_budget_rejection_is_atomic_and_snapshot_import_cannot_overwrite_live_values() {
    let limits = Limits {
        subscriptions: 2,
        retained_bytes: 2048,
        report: report::Limits {
            integer_bits: 80,
            bytes: 512,
            ..Default::default()
        },
        ..Default::default()
    };
    let store = Store::new(limits).unwrap();
    let n = name("counter");
    let g = name("gauge");
    store.create(&n, Kind::Counter, true).unwrap();
    store.create(&g, Kind::Gauge, true).unwrap();
    assert_eq!(
        store.create(&name("third"), Kind::Gauge, true),
        Err(Error::Limit)
    );
    let large = Value::Integer((BigInt::from(1) << 80) - 1);
    store.update(&n, &large).unwrap();
    let before = store.retained_bytes();
    let catalog = store.catalog(None).unwrap().unwrap();
    assert_eq!(store.update(&n, &integer(1)), Err(Error::Limit));
    assert_eq!(
        store.update(&g, &Value::Binary(vec![0; 513])),
        Err(Error::Limit)
    );
    assert_eq!(
        store.update(&g, &Value::Float(f64::INFINITY)),
        Err(Error::InvalidValue)
    );
    let mut deep = integer(1);
    for _ in 0..34 {
        deep = Value::List(vec![deep]);
    }
    assert_eq!(store.update(&g, &deep), Err(Error::Limit));
    assert_eq!(store.retained_bytes(), before);
    assert!(store.catalog(Some(catalog.revision)).unwrap().is_none());
    assert_eq!(
        store
            .get_value(&n, &Points::One(Part::atom("value")))
            .unwrap(),
        Reading::Values(vec![(Part::atom("value"), large)])
    );
    assert_eq!(
        store.put(vec![Metric {
            entry: Entry {
                name: n.clone(),
                kind: "counter".into(),
                enabled: true
            },
            values: vec![(Part::atom("value"), report::Value::Integer(0.into()))]
        }]),
        Err(StoreError::Configuration)
    );
    let nonnumeric = Value::Tuple(vec![
        Value::Atom("tag".into()),
        Value::List(vec![integer(1), Value::Binary(vec![1, 2, 3])]),
    ]);
    store.update(&g, &nonnumeric).unwrap();
    assert_eq!(
        store
            .get_value(&g, &Points::One(Part::atom("value")))
            .unwrap(),
        Reading::Values(vec![(Part::atom("value"), nonnumeric)])
    );
    assert_eq!(
        Source::read(
            &store,
            &Selector::Metric(g.clone()),
            &Points::One(Part::atom("value"))
        )
        .unwrap()[0]
            .values[0]
            .1,
        report::Value::Other
    );
    store.remove(&n).unwrap();
    store.remove(&g).unwrap();
    assert_eq!(store.retained_bytes(), 0);
}

#[test]
fn shared_counter_updates_are_atomic_and_value_changes_do_not_rearm_subscriptions() {
    let store = Arc::new(Store::new(Limits::default()).unwrap());
    let n = name("counter");
    store.create(&n, Kind::Counter, true).unwrap();
    let revision = store.catalog(None).unwrap().unwrap().revision;
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            let n = n.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    store.update(&n, &integer(1)).unwrap();
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    assert_eq!(
        store
            .get_value(&n, &Points::One(Part::atom("value")))
            .unwrap(),
        Reading::Values(vec![(Part::atom("value"), integer(8000))])
    );
    assert!(store.catalog(Some(revision)).unwrap().is_none());
    assert!(
        !Source::valid(
            &*store,
            &Selector::Metric(n.clone()),
            &Points::One(Part::atom("value16"))
        )
        .unwrap()
    );
    assert!(
        !Source::valid(
            &*store,
            &Selector::Metric(n.clone()),
            &Points::Many(vec![Part::atom("value"), Part::atom("value")])
        )
        .unwrap()
    );
    store.set_enabled(&n, false).unwrap();
    assert!(store.catalog(Some(revision)).unwrap().is_some());
    assert!(
        Source::read(&*store, &Selector::Metric(n), &Points::default_points())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn original_catalog_aggregation_ignores_float_gauges_and_disabled_entries() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_registry.json"
    ))
    .unwrap();
    let store = Store::new(Limits::default()).unwrap();
    let entries = vec![
        (
            vec![Part::atom("a"), Part::Integer(2.into())],
            Kind::Counter,
            integer(5),
            true,
        ),
        (name("a.a"), Kind::Gauge, Value::Float(1.5), true),
        (name("a.z"), Kind::Gauge, integer(10), false),
        (name("b"), Kind::Gauge, integer(9), true),
    ];
    for (n, k, v, enabled) in entries {
        store.create(&n, k, true).unwrap();
        store.update(&n, &v).unwrap();
        if !enabled {
            store.set_enabled(&n, false).unwrap();
        }
    }
    let found = store.find_entries(&name("a")).unwrap();
    let encoded = json!({"list":found.iter().map(|e|tuple(vec![json!({"list":e.name.iter().map(|p|match p {Part::Atom(s)=>atom(s),Part::Integer(n)=>json!({"integer":n.to_string()}),_=>panic!()}).collect::<Vec<_>>()}),atom(&e.kind),atom(if e.enabled {"enabled"} else {"disabled"})])).collect::<Vec<_>>()});
    assert_eq!(encoded, native["catalog"]["find"]);
    assert_eq!(
        found.iter().filter(|e| e.enabled).count(),
        native["catalog"]["count"].as_u64().unwrap() as usize
    );
    let aggregate = store
        .aggregate(&name("a"), &Points::One(Part::atom("value")))
        .unwrap();
    assert_eq!(
        json!({"result":{"list":aggregate.iter().map(|(p,n)|tuple(vec![match p {Part::Atom(s)=>atom(s),_=>panic!()},json!({"integer":n.to_string()})])).collect::<Vec<_>>()}}),
        native["catalog"]["aggregate"]
    );
}

#[test]
fn original_global_gate_status_errors_and_alias_key_controls_match() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_registry_controls.json"
    ))
    .unwrap();
    let store =
        Store::with_clock(Limits::default(), Arc::new(TestClock(AtomicI64::new(2000)))).unwrap();
    let n = name("ae.epoch.counter");
    store.create(&n, Kind::Counter, true).unwrap();
    let latin = Alias::Atom("é".into());
    let binary = Alias::Binary(vec![233]);
    let utf8 = Alias::Binary(vec![195, 169]);
    let unknown = Alias::Binary(b"unknown".to_vec());
    let rows = native["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 20);
    for row in rows {
        let id = row["id"].as_str().unwrap();
        let result = match id {
            "enable_again" | "enable" => outcome(store.set_enabled(&n, true)),
            "disable" | "disable_again" => outcome(store.set_enabled(&n, false)),
            "ensure_disabled_ignores_options" => outcome(store.ensure(&n, Kind::Counter, true)),
            "update" => outcome(store.update(&n, &integer(9))),
            "global_disable" | "global_enable" => {
                json!({"result":atom(if store.set_global_enabled(id=="global_enable").unwrap() {"enabled"} else {"disabled"})})
            }
            "global_update_missing" => {
                outcome(store.update(&name("missing"), &Value::Atom("foo".into())))
            }
            "global_update_existing" => outcome(store.update(&n, &integer(11))),
            "reset_globally_disabled" => outcome(store.reset(&n)),
            "alias_latin1" | "alias_same_binary" | "alias_utf8_distinct" | "alias_unknown_dp" => {
                let alias = match id {
                    "alias_latin1" => &latin,
                    "alias_same_binary" => &binary,
                    "alias_utf8_distinct" => &utf8,
                    _ => &unknown,
                };
                match store.add_alias(
                    alias,
                    &n,
                    &Part::atom(if id == "alias_unknown_dp" {
                        "unknown"
                    } else {
                        "value"
                    }),
                ) {
                    Err(Error::Exists) => {
                        json!({"result":tuple(vec![atom("error"),atom("exists")])})
                    }
                    r => outcome(r),
                }
            }
            "alias_unknown_value" | "alias_after_delete" => {
                match store.alias_value(if id == "alias_unknown_value" {
                    &unknown
                } else {
                    &binary
                }) {
                    Ok(Reading::Values(v)) => {
                        json!({"result":tuple(vec![atom("ok"),encode(&v[0].1)])})
                    }
                    r => reading(r),
                }
            }
            "alias_update" => outcome(store.update_alias(&unknown, &integer(5))),
            "alias_delete" | "alias_delete_again" => outcome(store.delete_alias(&binary)),
            _ => panic!("unmapped {id}"),
        };
        assert_eq!(result, row["result"], "{id}");
        assert_eq!(
            reading(store.get_value(&n, &Points::default_points())),
            row["value"],
            "{id}"
        );
        assert_eq!(
            json!({"result":atom(if store.info(&n).unwrap().entry.enabled {"enabled"} else {"disabled"})}),
            row["status"],
            "{id}"
        );
    }
    // Admission deliberately rejects the Unicode atom which crashes the
    // original alias process before its supervisor recreates it.
    assert_eq!(
        store.add_alias(&Alias::Atom("Ā".into()), &n, &Part::atom("value")),
        Err(Error::InvalidAlias)
    );
    assert!(store.resolve_alias(&utf8).unwrap().is_some());
    store.remove(&n).unwrap();
    assert_eq!(store.alias_value(&utf8), Err(Error::NotFound));
    store.create(&n, Kind::Gauge, true).unwrap();
    store.update(&n, &integer(8)).unwrap();
    assert_eq!(
        store.alias_value(&utf8).unwrap(),
        Reading::Values(vec![(Part::atom("value"), integer(8))])
    );
    store.remove(&n).unwrap();
    store.delete_alias(&utf8).unwrap();
    store.delete_alias(&unknown).unwrap();
    assert_eq!(store.retained_bytes(), 0);
}

#[test]
fn scalar_read_expansion_is_bounded_and_backward_clock_age_is_signed() {
    let clock = Arc::new(TestClock(AtomicI64::new(2000)));
    let limits = Limits {
        retained_bytes: 2048,
        report: report::Limits {
            bytes: 1024,
            ..Default::default()
        },
        ..Default::default()
    };
    let store = Store::with_clock(limits, clock.clone()).unwrap();
    let n = name("gauge");
    store.create(&n, Kind::Gauge, true).unwrap();
    store.update(&n, &Value::Binary(vec![0; 900])).unwrap();
    let p = Points::Many(vec![Part::atom("value"); 4]);
    assert_eq!(store.get_value(&n, &p), Err(Error::Limit));
    assert!(matches!(
        Source::read(&store, &Selector::Metric(n.clone()), &p),
        Err(StoreError::Limit)
    ));
    clock.0.store(1000, Ordering::Relaxed);
    assert_eq!(
        store
            .get_value(&n, &Points::One(Part::atom("ms_since_reset")))
            .unwrap(),
        Reading::Values(vec![(Part::atom("ms_since_reset"), integer(-1000))])
    );
    assert_eq!(store.info(&n).unwrap().timestamp_ms, 2000);
}
