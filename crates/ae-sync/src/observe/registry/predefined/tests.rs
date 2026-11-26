use super::*;
use ae_chain::instrumentation::{Publisher, Sink};
use serde_json::{Value as Json, json};
use std::sync::atomic::AtomicI64;

struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now(&self) -> reporter::Time {
        reporter::Time {
            monotonic: Instant::now(),
            unix_microseconds: 1_258_000_000_000_000 + i128::from(self.0.load(Ordering::SeqCst)),
        }
    }
}
fn setup() -> (Arc<Store>, Owner, Arc<Feed>) {
    let clock = Arc::new(TestClock(AtomicI64::new(2_000_000)));
    let store = Arc::new(Store::with_clock(Limits::default(), clock).unwrap());
    let owner = Owner::spawn(store.clone()).unwrap();
    let feed = Feed::install(store.clone()).unwrap();
    (store, owner, feed)
}
fn part(p: &Part) -> Json {
    match p {
        Part::Atom(a) => json!({"atom": a}),
        Part::Integer(n) => json!({"integer": n.to_string()}),
        _ => panic!("point"),
    }
}
fn value(v: &Value) -> Json {
    match v {
        Value::Integer(n) => json!({"integer":n.to_string()}),
        Value::Float(n) => json!({"float":format!("{:016x}",n.to_bits())}),
        _ => panic!("value"),
    }
}
fn reading(r: Reading) -> Json {
    json!({"tuple":[{"atom":"ok"},match r {
        Reading::Disabled => json!({"atom":"disabled"}),
        Reading::Values(v) => json!({"list":v.iter().map(|(p,v)| json!({"tuple":[part(p),value(v)]})).collect::<Vec<_>>()})
    }]})
}
fn scalar(store: &Store, suffix: &str) -> Reading {
    store
        .get_value(&name(suffix), &Points::One(Part::atom("value")))
        .unwrap()
}
#[test]
fn installs_all_original_predefined_entries_and_windows() {
    let (store, owner, _feed) = setup();
    let native: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_predefined.json"
    ))
    .unwrap();
    let rows = native["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 29);
    assert_eq!(store.find_entries(&vec![]).unwrap().len(), 29);
    for row in rows {
        let n = row["name"]["list"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| Part::atom(a["atom"].as_str().unwrap()))
            .collect();
        let info = store.info(&n).unwrap();
        assert_eq!(info.entry.kind, row["type"]);
        assert!(info.entry.enabled);
        assert_eq!(
            reading(store.get_value(&n, &Points::default_points()).unwrap()),
            row["value"]
        );
        assert_eq!(
            json!({"list":info.datapoints.unwrap().iter().map(part).collect::<Vec<_>>()}),
            row["datapoints"]
        );
        if info.entry.kind == "histogram" {
            let options = row["options"]["list"].as_array().unwrap();
            let span = options
                .first()
                .map(|o| {
                    o["tuple"][1]["integer"]
                        .as_str()
                        .unwrap()
                        .parse::<u64>()
                        .unwrap()
                })
                .unwrap_or(60_000);
            assert_eq!(
                store.histogram_options(&n).unwrap().config.time_span_ms,
                span
            );
            assert!(store.histogram_probe_info(&n).unwrap().unwrap().running);
        }
    }
    assert_eq!(store.histogram_usage().workers, 8);
    drop(owner);
    assert_eq!(store.histogram_usage(), histogram::probe::Usage::default());
}
#[test]
fn original_chain_event_intervals_keep_independent_clocks_through_reset_and_disable() {
    let (store, owner, mut feed) = setup();
    let clock = Arc::new(TestClock(AtomicI64::new(0)));
    Arc::get_mut(&mut feed).unwrap().clock = clock.clone();
    for suffix in ["mining.interval", "chain.top_change.interval"] {
        store.remove(&name(suffix)).unwrap();
        store
            .create_histogram(
                &name(suffix),
                &HistogramOptions {
                    config: histogram::Config {
                        module: histogram::Module::Plain,
                        rounding: histogram::Rounding::Preserve,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap();
    }
    let native: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_chain_events.json"
    ))
    .unwrap();
    let steps = [
        (true, Some(0)),
        (true, Some(1001)),
        (false, Some(1_000_000)),
        (true, Some(1999)),
        (true, Some(500)),
        (false, Some(1_100_100)),
        (true, Some(3500)),
        (true, None),
        (true, Some(7500)),
        (true, None),
        (true, Some(10000)),
        (true, None),
        (true, Some(12000)),
        (false, Some(999999)),
        (false, Some(1004999)),
    ];
    let points = Points::Many(["n", "mean", "min", "max"].map(Part::atom).to_vec());
    for ((mining, time), row) in steps.into_iter().zip(native["rows"].as_array().unwrap()) {
        if let Some(time) = time {
            clock.0.store(time, Ordering::SeqCst);
            feed.event(if mining {
                Event::BlockCreated
            } else {
                Event::TopChanged
            });
        } else {
            match row["label"].as_str().unwrap() {
                "disabled" => store.set_enabled(&name("mining.interval"), false).unwrap(),
                "enabled" => store.set_enabled(&name("mining.interval"), true).unwrap(),
                // Reset of the sampled chain probe has no effect on these two
                // event clocks or independent histogram workers.
                "probe_reset" => {}
                _ => panic!("step"),
            }
        }
        for (key, suffix) in [
            ("mining", "mining.interval"),
            ("top", "chain.top_change.interval"),
        ] {
            assert_eq!(
                reading(store.get_value(&name(suffix), &points).unwrap()),
                row[key],
                "{} {key}",
                row["label"]
            );
        }
    }
    assert_eq!(feed.info(), Info::default());
    drop(owner);
}
fn snapshot(height: i128, pings: i128) -> super::super::super::metrics::Snapshot {
    use super::super::super::metrics::{Metric, Number, Snapshot};
    Snapshot {
        chain_revision: 1,
        pool_chain_revision: 1,
        registry_revision: 1,
        metrics: vec![
            Metric {
                name: "ae.epoch.aecore.chain.height.value",
                value: Number::Integer(height),
            },
            Metric {
                name: "ae.epoch.aecore.peers.ping.success.value",
                value: Number::Integer(pings),
            },
            Metric {
                name: "ae.epoch.aecore.chain.total_difficulty",
                value: Number::Integer(99),
            },
        ],
    }
}
#[test]
fn snapshot_ownership_is_atomic_and_never_readds_cumulative_ping_counts() {
    let (store, _owner, feed) = setup();
    let publisher = Publisher::default();
    let _binding = publisher.bind(feed.clone()).unwrap();
    publisher.counter(Counter::PingSuccess, 1);
    publisher.gauge(Gauge::Height, instrumentation::Value::Integer(3));
    let frame = snapshot(999, 400);
    assert_eq!(store.publish(&frame), Err(StoreError::Configuration));
    assert!(
        !store
            .find_entries(&name("chain"))
            .unwrap()
            .iter()
            .any(|e| e.name == name("chain"))
    );
    for _ in 0..3 {
        feed.publish(&frame).unwrap();
    }
    assert_eq!(
        scalar(&store, "chain.height"),
        Reading::Values(vec![(Part::atom("value"), Value::Integer(3.into()))])
    );
    assert_eq!(
        scalar(&store, "peers.ping.success"),
        Reading::Values(vec![(Part::atom("value"), Value::Integer(1.into()))])
    );
    store.reset(&name("peers.ping.success")).unwrap();
    feed.publish(&frame).unwrap();
    assert_eq!(
        scalar(&store, "peers.ping.success"),
        Reading::Values(vec![(Part::atom("value"), Value::Integer(0.into()))])
    );
    store.set_global_enabled(false).unwrap();
    feed.publish(&snapshot(80, 400)).unwrap();
    assert_eq!(
        scalar(&store, "chain.height"),
        Reading::Values(vec![(Part::atom("value"), Value::Integer(3.into()))])
    );
    store.set_global_enabled(true).unwrap();
    store.set_enabled(&name("chain.height"), false).unwrap();
    feed.publish(&snapshot(90, 400)).unwrap();
    store.set_enabled(&name("chain.height"), true).unwrap();
    assert_eq!(
        scalar(&store, "chain.height"),
        Reading::Values(vec![(Part::atom("value"), Value::Integer(3.into()))])
    );
    store.remove(&name("peers.ping.success")).unwrap();
    assert_eq!(
        feed.publish(&snapshot(100, 500)),
        Err(StoreError::Configuration)
    );
    assert_eq!(
        scalar(&store, "chain.height"),
        Reading::Values(vec![(Part::atom("value"), Value::Integer(3.into()))])
    );
    publisher.counter(Counter::PingSuccess, 1);
    assert_eq!(
        feed.info(),
        Info {
            rejected: 1,
            last_error: Some(Error::NotFound)
        }
    );
}
#[test]
fn callback_metrics_values_survive_periodic_frames_reset_and_disabled_updates() {
    let (store, _owner, feed) = setup();
    let publisher = Publisher::default();
    let _binding = publisher.bind(feed).unwrap();
    let peers = &GAUGES[..9];
    let frame: Vec<_> = peers
        .iter()
        .map(|(_, suffix)| Metric {
            entry: Entry {
                name: name(suffix),
                kind: "gauge".into(),
                enabled: true,
            },
            values: vec![(Part::atom("value"), report::Value::Integer(999.into()))],
        })
        .collect();
    for (i, (gauge, _)) in peers.iter().enumerate() {
        publisher.gauge(*gauge, instrumentation::Value::Integer(i as u64 + 1));
    }
    for _ in 0..3 {
        store.put_frame(frame.clone(), true).unwrap();
    }
    for (i, (_, suffix)) in peers.iter().enumerate() {
        assert_eq!(
            scalar(&store, suffix),
            Reading::Values(vec![(Part::atom("value"), Value::Integer((i + 1).into()))])
        );
        store.reset(&name(suffix)).unwrap();
    }
    store.put_frame(frame.clone(), true).unwrap();
    for (_, suffix) in peers {
        assert_eq!(
            scalar(&store, suffix),
            Reading::Values(vec![(Part::atom("value"), Value::Integer(0.into()))])
        );
    }
    store.set_global_enabled(false).unwrap();
    for (gauge, _) in peers {
        publisher.gauge(*gauge, instrumentation::Value::Integer(77));
    }
    store.put_frame(frame.clone(), true).unwrap();
    store.set_global_enabled(true).unwrap();
    for (gauge, suffix) in peers {
        store.set_enabled(&name(suffix), false).unwrap();
        publisher.gauge(*gauge, instrumentation::Value::Integer(88));
        store.put_frame(frame.clone(), true).unwrap();
        store.set_enabled(&name(suffix), true).unwrap();
        assert_eq!(
            scalar(&store, suffix),
            Reading::Values(vec![(Part::atom("value"), Value::Integer(0.into()))])
        );
    }
    // A callback name remains reserved and validated even though the sample
    // cannot overwrite it. Invalid frames must not partially apply height.
    let height = Metric {
        entry: Entry {
            name: name("chain.height"),
            kind: "gauge".into(),
            enabled: true,
        },
        values: vec![(Part::atom("value"), report::Value::Integer(42.into()))],
    };
    let mut bad = frame[1].clone();
    bad.values.clear();
    assert_eq!(
        store.put_frame(vec![height.clone(), bad], true),
        Err(StoreError::Configuration)
    );
    let mut bad = frame[1].clone();
    bad.values[0].1 = report::Value::Other;
    assert_eq!(
        store.put_frame(vec![height.clone(), bad], true),
        Err(StoreError::Configuration)
    );
    store.remove(&name("peers.blocked")).unwrap();
    let mut incomplete = vec![height];
    incomplete.extend(frame);
    assert_eq!(
        store.put_frame(incomplete, true),
        Err(StoreError::Configuration)
    );
    assert_eq!(
        scalar(&store, "chain.height"),
        Reading::Values(vec![(Part::atom("value"), Value::Integer(0.into()))])
    );
}
#[test]
fn installation_failure_preserves_existing_entries_and_retires_created_probes() {
    let store = Arc::new(Store::new(Limits::default()).unwrap());
    let _owner = Owner::spawn(store.clone()).unwrap();
    store
        .create(
            &name("blocks.key.insert_execution_time.error"),
            Kind::Gauge,
            true,
        )
        .unwrap();
    assert!(matches!(Feed::install(store.clone()), Err(Error::Exists)));
    assert_eq!(store.find_entries(&vec![]).unwrap().len(), 1);
    assert_eq!(store.histogram_usage(), histogram::probe::Usage::default());
}
