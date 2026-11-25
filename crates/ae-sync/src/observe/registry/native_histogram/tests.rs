use super::*;
use serde_json::{Value as Json, json};
use std::{
    sync::{
        atomic::{AtomicI64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

struct TestClock(AtomicI64);
impl reporter::Clock for TestClock {
    fn now(&self) -> reporter::Time {
        reporter::Time {
            monotonic: Instant::now(),
            unix_microseconds: 1_258_000_000_000_000
                + i128::from(self.0.load(Ordering::SeqCst)) * 1000,
        }
    }
}
fn name() -> Name {
    ["ae", "epoch", "histogram"].map(Part::atom).to_vec()
}
fn alias() -> Alias {
    Alias::Atom("hist_mean".into())
}
fn value(n: i64) -> Value {
    Value::Integer(n.into())
}
fn points() -> Points {
    Points::Many(
        [
            Part::atom("n"),
            Part::atom("mean"),
            Part::atom("unknown"),
            Part::atom("mean"),
            Part::Integer(50.into()),
            Part::Integer(999.into()),
        ]
        .to_vec(),
    )
}
fn raw_points() -> Points {
    Points::Many(
        [
            Part::atom("n"),
            Part::atom("mean"),
            Part::atom("min"),
            Part::atom("max"),
            Part::Integer(50.into()),
            Part::Integer(999.into()),
            Part::atom("total"),
            Part::atom("unknown"),
        ]
        .to_vec(),
    )
}
fn probe(store: &Store) -> probe::Client {
    store.lock().entries[&name()]
        .histogram
        .as_ref()
        .unwrap()
        .client()
        .unwrap()
}
fn setup(options: &HistogramOptions) -> (Arc<TestClock>, Arc<Store>, Owner, Client) {
    let clock = Arc::new(TestClock(AtomicI64::new(2000)));
    let store = Arc::new(Store::with_clock(Limits::default(), clock.clone()).unwrap());
    let owner = Owner::spawn(store.clone()).unwrap();
    let client = owner.client();
    client
        .create_histogram(&name(), options)
        .unwrap()
        .blocking_wait()
        .unwrap();
    (clock, store, owner, client)
}
fn wait(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(
            Instant::now() < deadline,
            "histogram registry transition timed out"
        );
        thread::sleep(Duration::from_millis(1));
    }
}
fn crash(client: &probe::Client) {
    assert_eq!(
        client
            .test_work(|| panic!("injected registry probe failure"))
            .unwrap()
            .blocking_wait(),
        Err(Error::Closed)
    );
}
fn term(v: &Value) -> Json {
    match v {
        Value::Integer(n) => json!({"integer":n.to_string()}),
        Value::Float(n) => json!({"float":format!("{:016x}", n.to_bits())}),
        Value::Atom(a) => json!({"atom":a}),
        _ => panic!("unexpected value"),
    }
}
fn part(p: &Part) -> Json {
    match p {
        Part::Atom(a) => json!({"atom":a}),
        Part::Integer(n) => json!({"integer":n.to_string()}),
        _ => panic!("unexpected point"),
    }
}
fn values(v: &[(Part, Value)]) -> Json {
    json!({"list":v.iter().map(|(p,v)| json!({"tuple":[part(p),term(v)]})).collect::<Vec<_>>()})
}
fn reading(r: Result<Reading, Error>, alias: bool) -> Json {
    match r {
        Ok(Reading::Disabled) => json!({"result":{"tuple":[{"atom":"ok"},{"atom":"disabled"}]}}),
        Ok(Reading::Values(v)) => {
            json!({"result":{"tuple":[{"atom":"ok"},if alias {term(&v[0].1)} else {values(&v)}]}})
        }
        Err(Error::NotFound) => json!({"result":{"tuple":[{"atom":"error"},{"atom":"not_found"}]}}),
        r => panic!("unexpected reading {r:?}"),
    }
}
fn check_row(store: &Store, row: &Json) {
    let label = row["label"].as_str().unwrap();
    let raw = match store.histogram_probe_info(&name()) {
        Ok(Some(_)) => {
            json!({"result":values(&probe(store).values(&raw_points()).unwrap().blocking_wait().unwrap())})
        }
        _ => json!({"result":{"atom":"undefined"}}),
    };
    assert_eq!(raw, row["raw"], "{label} raw");
    assert_eq!(
        reading(store.get_value(&name(), &points()), false),
        row["value"],
        "{label} value"
    );
    assert_eq!(
        reading(store.alias_value(&alias()), true),
        row["alias"],
        "{label} alias"
    );
    let info = store.info(&name()).ok();
    assert_eq!(
        json!({"atom":info.as_ref().map(|i|if i.entry.enabled {"enabled"} else {"disabled"}).unwrap_or("undefined")}),
        row["status"],
        "{label} status"
    );
    assert!(info.as_ref().is_none_or(|i| i.timestamp_ms.is_none()));
    let ttl = info.as_ref().and_then(|i| i.cache_ms).map_or(
        json!({"atom":"undefined"}),
        |n| json!({"integer":n.to_string()}),
    );
    assert_eq!(ttl, row["cache_ms"], "{label} cache ttl");
    let dp = info.and_then(|i| i.datapoints).map_or(
        json!({"atom":"undefined"}),
        |v| json!({"list":v.iter().map(part).collect::<Vec<_>>()}),
    );
    assert_eq!(dp, row["datapoints"], "{label} datapoints");
    for point in ["mean", "unknown"] {
        let cached = store
            .cache_read(&name(), &Part::atom(point))
            .unwrap()
            .map_or(
                json!({"atom":"not_found"}),
                |v| json!({"tuple":[{"atom":"ok"},term(&v)]}),
            );
        assert_eq!(
            cached,
            row[format!("cache_{point}")],
            "{label} {point} cache"
        );
    }
    assert_eq!(
        store.resolve_alias(&alias()).unwrap(),
        Some((name(), Part::atom("mean")))
    );
    assert_eq!(
        store
            .histogram_probe_info(&name())
            .ok()
            .flatten()
            .is_some_and(|i| i.running),
        row["probe_live"].as_bool().unwrap()
    );
    if let Ok(options) = store.histogram_options(&name()) {
        for item in row["options"]["list"].as_array().unwrap() {
            let key = item["tuple"][0]["atom"].as_str().unwrap();
            let val = &item["tuple"][1];
            match key {
                "cache" => assert_eq!(options.cache_ms.to_string(), val["integer"]),
                "time_span" => assert_eq!(options.config.time_span_ms.to_string(), val["integer"]),
                "slot_period" => {
                    assert_eq!(options.config.slot_period_ms.to_string(), val["integer"])
                }
                "keep_high" => assert_eq!(options.config.keep_high.to_string(), val["integer"]),
                "status" => assert_eq!(options.enabled, val["atom"] == "enabled"),
                _ => panic!("uncovered option {key}"),
            }
        }
    }
}

#[test]
fn registry_cache_alias_options_and_recreation_match_sixteen_native_rows() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_histogram_registry_1.json"
    ))
    .unwrap();
    let (clock, store, owner, client) = setup(&HistogramOptions {
        config: Config {
            keep_high: 2,
            time_span_ms: 60,
            ..Default::default()
        },
        cache_ms: 60_000,
        ..Default::default()
    });
    client
        .add_alias(&alias(), &name(), &Part::atom("mean"))
        .unwrap()
        .blocking_wait()
        .unwrap();
    let first = probe(&store);
    let mut retired = vec![];
    for (index, row) in native["rows"].as_array().unwrap().iter().enumerate() {
        clock
            .0
            .store(row["time_ms"].as_i64().unwrap(), Ordering::SeqCst);
        match index {
            0 => (),
            1 => client.update(&name(), &value(12)).unwrap(),
            2 => client.cache_delete(&name(), None).unwrap(),
            3 => client.update_alias(&alias(), &value(100)).unwrap(),
            4 => {
                client
                    .cache_write(&name(), &Part::atom("unknown"), &value(123), 60_000)
                    .unwrap();
            }
            5 => {
                client.reset(&name()).unwrap();
                client.set_enabled(&name(), true).unwrap();
                assert_eq!(first.info().generation, 1);
            }
            6 => {
                client.set_enabled(&name(), false).unwrap();
                assert!(first.info().joined);
                assert_eq!(
                    client.set_enabled(&name(), false),
                    Err(Error::InvalidStatus)
                );
                assert_eq!(client.set_cache(&name(), 0), Err(Error::Disabled));
            }
            7 => {
                client.update(&name(), &value(999)).unwrap();
                client.reset(&name()).unwrap();
            }
            8 => client.set_enabled(&name(), true).unwrap(),
            9 => client.update(&name(), &value(5)).unwrap(),
            10 => client
                .set_histogram(
                    &name(),
                    HistogramSettings {
                        config: Some(Config {
                            time_span_ms: 1,
                            keep_high: 0,
                            ..Default::default()
                        }),
                        cache_ms: Some(0),
                        enabled: None,
                    },
                )
                .unwrap(),
            11 | 13 => (),
            12 => {
                retired.push(probe(&store));
                client.set_enabled(&name(), false).unwrap();
                client.set_enabled(&name(), true).unwrap();
                client.update(&name(), &value(9)).unwrap();
            }
            14 => {
                retired.push(probe(&store));
                client.delete(&name()).unwrap().blocking_wait().unwrap();
            }
            15 => client
                .create_histogram(&name(), &HistogramOptions::default())
                .unwrap()
                .blocking_wait()
                .unwrap(),
            _ => panic!("uncaptured row"),
        }
        check_row(&store, row);
    }
    let last = probe(&store);
    assert!(retired.iter().all(|c| c.info().joined));
    drop(owner);
    assert!(last.info().joined);
    assert_eq!(store.histogram_usage(), probe::Usage::default());
}

#[test]
fn saved_configuration_is_used_by_next_enable_and_automatic_replacement() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_histogram_registry_2.json"
    ))
    .unwrap();
    let (clock, store, owner, client) = setup(&HistogramOptions {
        config: Config {
            time_span_ms: 60,
            ..Default::default()
        },
        ..Default::default()
    });
    let points = Points::Many(["n", "mean", "min", "max"].map(Part::atom).to_vec());
    client.set_enabled(&name(), false).unwrap();
    for (index, row) in native["rows"].as_array().unwrap().iter().enumerate() {
        clock
            .0
            .store(row["time_ms"].as_i64().unwrap(), Ordering::SeqCst);
        match index {
            0 => {
                client
                    .set_histogram(
                        &name(),
                        HistogramSettings {
                            enabled: Some(true),
                            config: Some(Config {
                                time_span_ms: 1,
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    )
                    .unwrap();
                client.update(&name(), &value(7)).unwrap();
            }
            1 | 3 | 5 => (),
            2 => {
                client.set_enabled(&name(), false).unwrap();
                client.set_enabled(&name(), true).unwrap();
                client.update(&name(), &value(8)).unwrap();
            }
            4 => {
                client
                    .set_histogram(
                        &name(),
                        HistogramSettings {
                            config: Some(Config {
                                time_span_ms: 60,
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    )
                    .unwrap();
                let p = probe(&store);
                crash(&p);
                wait(|| p.info().generation == 2);
                client.update(&name(), &value(9)).unwrap();
                assert_eq!(p.info().failures, 1);
            }
            6 => {
                client
                    .ensure_histogram(
                        &name(),
                        &HistogramOptions {
                            enabled: false,
                            config: Config {
                                time_span_ms: 1,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    )
                    .unwrap()
                    .blocking_wait()
                    .unwrap();
                assert_eq!(
                    client
                        .ensure(&name(), Kind::Counter, true)
                        .unwrap()
                        .blocking_wait(),
                    Err(Error::TypeMismatch)
                );
            }
            _ => panic!("uncaptured row"),
        }
        let Reading::Values(v) = client.get_value(&name(), &points).unwrap() else {
            panic!("disabled")
        };
        assert_eq!(values(&v), row["value"], "{}", row["label"]);
    }
    let last = probe(&store);
    drop(owner);
    assert!(last.info().joined);
}

#[test]
fn admin_reaps_disable_delete_and_restarts_without_losing_probe_ownership() {
    for action in [probe::Action::Disable, probe::Action::Delete] {
        let (_, store, owner, client) = setup(&HistogramOptions {
            policy: probe::Policy(vec![probe::Rule {
                threshold: probe::Threshold::Always,
                action,
            }]),
            ..Default::default()
        });
        let p = probe(&store);
        client
            .restart(Manager::Admin, 1)
            .unwrap()
            .blocking_wait()
            .unwrap();
        assert!(p.info().running);
        // Simulate a lost terminal notification while the admin is replaced.
        *store.probe_signal.lock().unwrap() = None;
        crash(&p);
        wait(|| p.info().closing);
        assert!(!p.info().joined);
        client
            .restart(Manager::Admin, 2)
            .unwrap()
            .blocking_wait()
            .unwrap();
        wait(|| p.info().joined);
        if action == probe::Action::Disable {
            assert_eq!(client.get_value(&name(), &points()), Ok(Reading::Disabled));
        } else {
            assert_eq!(client.get_value(&name(), &points()), Err(Error::NotFound));
        }
        assert_eq!(client.info().restart_attempts, 2);
        assert_eq!(store.histogram_usage(), probe::Usage::default());
        drop(owner);
    }
}

struct Block {
    release: Option<mpsc::Sender<()>>,
    ticket: Option<probe::Ticket<()>>,
}
impl Block {
    fn release(mut self) {
        self.release.take().unwrap().send(()).unwrap();
        self.ticket.take().unwrap().blocking_wait().unwrap();
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        if let Some(sender) = self.release.take() {
            let _ = sender.send(());
        }
    }
}
fn block(p: &probe::Client) -> Block {
    let (entered, started) = mpsc::sync_channel(0);
    let (release, released) = mpsc::channel();
    let ticket = p
        .test_work(move || {
            entered.send(()).unwrap();
            released.recv().unwrap();
        })
        .unwrap();
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    Block {
        release: Some(release),
        ticket: Some(ticket),
    }
}

#[test]
fn pending_reads_cannot_refill_reset_cache_and_never_hold_storage_lock() {
    let (_, store, owner, client) = setup(&HistogramOptions {
        cache_ms: 60_000,
        ..Default::default()
    });
    client.update(&name(), &value(9)).unwrap();
    let p = probe(&store);
    let blocked = block(&p);
    let s = store.clone();
    let reader = thread::spawn(move || s.get_value(&name(), &Points::One(Part::atom("mean"))));
    wait(|| store.histogram_usage().pending == 2);
    client.reset(&name()).unwrap();
    assert_eq!(client.metric_info(&name()).unwrap().timestamp_ms, None);
    blocked.release();
    assert_eq!(reader.join().unwrap(), Err(Error::Stale));
    assert_eq!(
        client.cache_read(&name(), &Part::atom("mean")).unwrap(),
        None
    );
    assert_eq!(
        client
            .get_value(&name(), &Points::One(Part::atom("mean")))
            .unwrap(),
        Reading::Values(vec![(Part::atom("mean"), value(0))])
    );
    drop(owner);
}

#[test]
fn shutdown_fences_calls_and_joins_blocked_histogram_before_releasing_store() {
    let (_, store, owner, client) = setup(&HistogramOptions::default());
    let p = probe(&store);
    let blocked = block(&p);
    client.update(&name(), &value(8)).unwrap();
    let (done, completed) = mpsc::channel();
    let closing = thread::spawn(move || {
        drop(owner);
        done.send(()).unwrap();
    });
    wait(|| p.info().closing);
    assert_eq!(store.get_value(&name(), &points()), Err(Error::Closed));
    assert!(completed.try_recv().is_err());
    assert!(!p.info().joined);
    blocked.release();
    completed.recv_timeout(Duration::from_secs(5)).unwrap();
    closing.join().unwrap();
    assert!(p.info().joined);
    assert_eq!(store.histogram_usage(), probe::Usage::default());
    assert_eq!(store.retained_bytes(), 0);
}

#[tokio::test]
async fn native_histograms_feed_reporter_reads_aliases_and_integer_aggregation() {
    let store = Arc::new(Store::new(Limits::default()).unwrap());
    assert_eq!(
        store.create_histogram(&name(), &HistogramOptions::default()),
        Err(Error::Closed)
    );
    let mut owner = Owner::spawn(store.clone()).unwrap();
    let client = owner.client();
    client
        .create_histogram(&name(), &HistogramOptions::default())
        .unwrap()
        .await
        .unwrap();
    client.update(&name(), &value(9)).unwrap();
    let selected = Selector::Metric(name());
    let points = Points::One(Part::atom("mean"));
    assert!(store.valid(&selected, &points).unwrap());
    assert!(
        !store
            .valid(
                &selected,
                &Points::Many(vec![Part::atom("mean"), Part::atom("mean")])
            )
            .unwrap()
    );
    assert_eq!(
        store.read(&selected, &points).unwrap()[0].values,
        vec![(Part::atom("mean"), report::Value::Integer(9.into()))]
    );
    assert_eq!(
        store.aggregate(&name(), &points).unwrap(),
        vec![(Part::atom("mean"), 9.into())]
    );
    assert_eq!(
        store.put(vec![Metric {
            entry: Entry {
                name: name(),
                kind: "gauge".into(),
                enabled: true
            },
            values: vec![]
        }]),
        Err(StoreError::Configuration)
    );
    owner.finish().await;
}

#[test]
fn deleted_registration_cannot_publish_an_old_read_into_reused_name() {
    let (_, store, owner, client) = setup(&HistogramOptions {
        cache_ms: 60_000,
        ..Default::default()
    });
    client.update(&name(), &value(91)).unwrap();
    let p = probe(&store);
    let blocked = block(&p);
    let s = store.clone();
    let reader = thread::spawn(move || s.get_value(&name(), &Points::One(Part::atom("mean"))));
    wait(|| store.histogram_usage().pending == 2);
    let delete = client.delete(&name()).unwrap();
    wait(|| store.info(&name()) == Err(Error::NotFound));
    assert!(!p.info().joined);
    blocked.release();
    delete.blocking_wait().unwrap();
    client
        .create_histogram(
            &name(),
            &HistogramOptions {
                cache_ms: 60_000,
                ..Default::default()
            },
        )
        .unwrap()
        .blocking_wait()
        .unwrap();
    assert_eq!(reader.join().unwrap(), Err(Error::Stale));
    assert_eq!(
        client.cache_read(&name(), &Part::atom("mean")).unwrap(),
        None
    );
    assert!(p.info().joined);
    drop(owner);
}

#[test]
fn changing_future_configuration_preserves_failure_history_and_fresh_enable_resets_it() {
    let (_, store, owner, client) = setup(&HistogramOptions::default());
    let p = probe(&store);
    for failure in 1..=4 {
        client
            .set_histogram(
                &name(),
                HistogramSettings {
                    config: Some(Config {
                        time_span_ms: 10 * failure,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .unwrap();
        crash(&p);
        wait(|| p.info().generation > failure || p.info().closing);
    }
    wait(|| p.info().joined);
    assert_eq!(p.info().failures, 4);
    assert_eq!(p.info().terminal_action, Some(probe::Action::Disable));
    assert_eq!(client.info().parent.restart_attempts, 0);
    client.set_enabled(&name(), true).unwrap();
    let fresh = probe(&store);
    assert_eq!(fresh.info().failures, 0);
    crash(&fresh);
    wait(|| fresh.info().generation == 2);
    assert_eq!(fresh.info().failures, 1);
    drop(owner);
    assert!(fresh.info().joined);
}

#[test]
fn shared_probe_quota_rejects_new_registrations_without_leaks_and_cache_fill_is_atomic() {
    let store = Arc::new(Store::new(Limits::default()).unwrap());
    let owner = Owner::spawn(store.clone()).unwrap();
    for i in 0..32 {
        store
            .create_histogram(&vec![Part::Integer(i.into())], &HistogramOptions::default())
            .unwrap();
    }
    assert_eq!(store.histogram_usage().workers, 32);
    let bytes = store.retained_bytes();
    assert_eq!(
        store.create_histogram(&name(), &HistogramOptions::default()),
        Err(Error::Limit)
    );
    assert_eq!(store.retained_bytes(), bytes);
    assert_eq!(store.info(&name()), Err(Error::NotFound));
    store
        .set_enabled(&vec![Part::Integer(0.into())], false)
        .unwrap();
    store
        .create_histogram(&name(), &HistogramOptions::default())
        .unwrap();
    assert_eq!(store.histogram_usage().workers, 32);
    drop(owner);
    assert_eq!(store.histogram_usage(), probe::Usage::default());

    let store = Arc::new(
        Store::new(Limits {
            retained_bytes: 2048,
            ..Default::default()
        })
        .unwrap(),
    );
    let owner = Owner::spawn(store.clone()).unwrap();
    store
        .create_histogram(
            &name(),
            &HistogramOptions {
                cache_ms: 60_000,
                ..Default::default()
            },
        )
        .unwrap();
    let bytes = store.retained_bytes();
    assert_eq!(
        store.get_value(&name(), &Points::default_points()),
        Err(Error::Limit)
    );
    assert_eq!(store.retained_bytes(), bytes);
    assert_eq!(
        store.cache_read(&name(), &Part::atom("mean")).unwrap(),
        None
    );
    drop(owner);
    assert_eq!(store.histogram_usage(), probe::Usage::default());
}

#[test]
fn sampled_frames_stay_coherent_while_histogram_read_is_blocked() {
    let (_, store, owner, _) = setup(&HistogramOptions::default());
    let frame = |n: i64| {
        ["z-first", "z-second"]
            .map(|name| Metric {
                entry: Entry {
                    name: vec![Part::atom(name)],
                    kind: "gauge".into(),
                    enabled: true,
                },
                values: vec![(Part::atom("mean"), report::Value::Integer(n.into()))],
            })
            .to_vec()
    };
    store.put(frame(7)).unwrap();
    let p = probe(&store);
    let blocked = block(&p);
    let s = store.clone();
    let reader =
        thread::spawn(move || s.read(&Selector::Find(vec![]), &Points::One(Part::atom("mean"))));
    wait(|| store.histogram_usage().pending == 2);
    store.put(frame(99)).unwrap();
    blocked.release();
    let samples = reader.join().unwrap().unwrap();
    assert_eq!(samples.len(), 3);
    for sample in &samples[1..] {
        assert_eq!(
            sample.values,
            vec![(Part::atom("mean"), report::Value::Integer(7.into()))]
        );
    }
    drop(owner);
}

#[test]
fn fresh_cache_ttl_starts_when_the_worker_result_is_published() {
    let (clock, store, owner, client) = setup(&HistogramOptions {
        cache_ms: 60_000,
        ..Default::default()
    });
    client.update(&name(), &value(9)).unwrap();
    let p = probe(&store);
    let blocked = block(&p);
    let s = store.clone();
    let reader = thread::spawn(move || s.get_value(&name(), &Points::One(Part::atom("mean"))));
    wait(|| store.histogram_usage().pending == 2);
    clock.0.store(8000, Ordering::SeqCst);
    blocked.release();
    assert_eq!(
        reader.join().unwrap().unwrap(),
        Reading::Values(vec![(Part::atom("mean"), value(9))])
    );
    assert_eq!(
        client
            .cache_info(&name(), &Part::atom("mean"))
            .unwrap()
            .unwrap()
            .written_microseconds,
        1_258_000_008_000_000
    );
    drop(owner);
}
