use super::super::{
    pipeline,
    statsd::supervision::{Owner as Logger, Phase},
};
use super::*;
use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::net::UdpSocket;
fn name() -> String {
    "aec_metrics_main".into()
}
fn metric() -> Name {
    vec![
        Part::atom("ae"),
        Part::atom("reporter"),
        Part::atom("value"),
    ]
}
fn key() -> Subscription {
    Subscription {
        reporter: name(),
        metric: Selector::Metric(metric()),
        points: Points::One(Part::atom("value")),
        retry: true,
        extra: report::Extra::List(vec![]),
    }
}
fn native() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_reporter.json"
    ))
    .unwrap()
}
struct Gate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}
struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}
#[derive(Default)]
struct Gauge {
    gate: Mutex<Option<Gate>>,
    clock: Option<Arc<TestClock>>,
    advance: AtomicU64,
}
impl Source for Gauge {
    fn valid(&self, m: &Selector, p: &Points) -> Result<bool, Error> {
        Ok(m == &Selector::Metric(metric())
            && (p == &Points::One(Part::atom("value")) || p == &Points::default_points()))
    }
    fn read(&self, m: &Selector, _: &Points) -> Result<Vec<Sample>, Error> {
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            let _ = gate.release.recv();
        }
        if let Some(clock) = &self.clock {
            clock.advance(self.advance.swap(0, Ordering::Relaxed));
        }
        Ok(if m == &Selector::Metric(metric()) {
            vec![Sample {
                metric: metric(),
                values: vec![(Part::atom("value"), report::Value::Integer(42.into()))],
            }]
        } else {
            vec![]
        })
    }
}
struct TestClock {
    start: Instant,
    mono_ms: AtomicU64,
    wall_ms: AtomicI64,
}
impl TestClock {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            mono_ms: AtomicU64::new(0),
            wall_ms: AtomicI64::new(1_000_000),
        }
    }
    fn advance(&self, ms: u64) {
        self.mono_ms.fetch_add(ms, Ordering::Relaxed);
        self.wall_ms.fetch_add(ms as i64, Ordering::Relaxed);
    }
}
impl Clock for TestClock {
    fn now(&self) -> Time {
        Time {
            monotonic: self.start + Duration::from_millis(self.mono_ms.load(Ordering::Relaxed)),
            unix_microseconds: i128::from(self.wall_ms.load(Ordering::Relaxed)) * 1000,
        }
    }
}
async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .unwrap()
}
async fn ready(logger: &mut Logger) {
    within(async {
        loop {
            logger.poll().await;
            if logger.handle().status().phase == Phase::Running {
                break;
            }
            logger.changed().await;
        }
    })
    .await;
}
async fn packet(socket: &UdpSocket) -> Vec<u8> {
    let mut b = [0; 1024];
    let n = within(socket.recv(&mut b)).await.unwrap();
    b[..n].to_vec()
}
async fn none(socket: &UdpSocket) {
    let mut b = [0; 1024];
    assert!(
        tokio::time::timeout(Duration::from_millis(25), socket.recv(&mut b))
            .await
            .is_err()
    );
}
async fn setup(
    limits: Limits,
    source: Arc<dyn Source>,
    clock: Arc<dyn Clock>,
) -> (Owner, Logger, UdpSocket) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut logger = Logger::prepare(statsd::Settings {
        target: statsd::Config {
            host: statsd::Host::Name("127.0.0.1".into()),
            port: socket.local_addr().unwrap().port(),
            ..Default::default()
        },
        pipeline: Some(pipeline::Config::default()),
        ..Default::default()
    })
    .unwrap();
    logger.activate();
    ready(&mut logger).await;
    let owner = Owner::spawn_with_clock(limits, source, logger.client(), clock).unwrap();
    (owner, logger, socket)
}
async fn reporter(client: &Client) -> ReporterInfo {
    within(client.reporters().unwrap()).await.unwrap().remove(0)
}
#[tokio::test]
async fn original_real_first_timer_and_disable_enable_join_reporter_before_pipeline_stop() {
    let native = native();
    assert_eq!(native["no_immediate_sample"], true);
    assert_eq!(native["interval_ms"], 10000);
    let (mut owner, mut logger, socket) = setup(
        Default::default(),
        Arc::new(Gauge::default()),
        Arc::new(SystemClock),
    )
    .await;
    let c = owner.client();
    within(c.add(Default::default()).unwrap()).await.unwrap();
    assert!(
        within(c.subscriptions(name()).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    none(&socket).await;
    within(
        c.subscribe(key(), Interval::Named("default".into()))
            .unwrap(),
    )
    .await
    .unwrap();
    none(&socket).await;
    let started = Instant::now();
    let mut b = [0; 1024];
    let n = tokio::time::timeout(Duration::from_secs(12), socket.recv(&mut b))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        &b[..n],
        native["first_real_timer_delivers"]
            .as_str()
            .unwrap()
            .as_bytes()
    );
    assert!(started.elapsed() > Duration::from_secs(9));
    let before = reporter(&c).await;
    within(c.enable(name(), false).unwrap()).await.unwrap();
    let disabled = reporter(&c).await;
    assert!(!disabled.alive && !disabled.enabled && disabled.previous_joined);
    assert_eq!(disabled.intervals[0].timer, before.intervals[0].timer);
    assert!(disabled.intervals[0].armed);
    assert_eq!(
        within(c.subscriptions(name()).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    none(&socket).await;
    within(c.enable(name(), true).unwrap()).await.unwrap();
    let enabled = reporter(&c).await;
    assert!(enabled.alive && enabled.enabled);
    assert_eq!(enabled.generation, before.generation + 1);
    assert_eq!(enabled.intervals[0].timer, before.intervals[0].timer);
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&socket).await, b"ae.reporter.value.value:42|g");
    assert_eq!(
        within(c.unsubscribe_native(key()).unwrap()).await,
        Err(Error::NotFound)
    );
    within(
        c.unsubscribe_all(name(), Selector::Metric(metric()))
            .unwrap(),
    )
    .await
    .unwrap();
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    none(&socket).await;
    within(
        c.subscribe(key(), Interval::Named("default".into()))
            .unwrap(),
    )
    .await
    .unwrap();
    within(c.enable(name(), false).unwrap()).await.unwrap();
    let input = logger.client().input().unwrap();
    within(input.prepare_stop().unwrap().wait()).await.unwrap();
    assert!(input.info().joined && input.info().disabled);
    assert!(logger.client().info().running && logger.client().info().connected);
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    none(&socket).await;
    owner.finish().await;
    assert!(c.info().joined);
    logger.finish().await;
}
#[tokio::test]
async fn original_interval_controls_and_retry_identity_preserve_subscriptions_and_timer_references()
{
    let native = native();
    assert_eq!(
        native["controls"]["retry_flag_is_part_of_subscription_identity"],
        true
    );
    let (mut owner, mut logger, socket) = setup(
        Default::default(),
        Arc::new(Gauge::default()),
        Arc::new(TestClock::new()),
    )
    .await;
    let c = owner.client();
    within(c.add(Default::default()).unwrap()).await.unwrap();
    within(
        c.subscribe(key(), Interval::Named("default".into()))
            .unwrap(),
    )
    .await
    .unwrap();
    within(
        c.set_interval(
            name(),
            "default".into(),
            Period::Milliseconds(60000),
            Some(40000),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let first = reporter(&c).await.intervals.remove(0);
    within(
        c.set_interval(name(), "default".into(), Period::Milliseconds(70000), None)
            .unwrap(),
    )
    .await
    .unwrap();
    let scalar = reporter(&c).await.intervals.remove(0);
    assert_eq!(scalar.interval.delay_ms, 40000);
    assert_ne!(scalar.timer, first.timer);
    within(
        c.set_interval(name(), "default".into(), Period::Manual, None)
            .unwrap(),
    )
    .await
    .unwrap();
    let manual = reporter(&c).await.intervals.remove(0);
    assert_eq!(manual.timer, scalar.timer);
    assert!(!manual.armed);
    assert_eq!(manual.interval.delay_ms, 40000);
    within(c.restart_intervals(name()).unwrap()).await.unwrap();
    assert_eq!(reporter(&c).await.intervals[0].timer, manual.timer);
    within(c.subscribe(key(), Interval::Named("other".into())).unwrap())
        .await
        .unwrap();
    assert_eq!(
        within(c.subscriptions(name()).unwrap()).await.unwrap()[0].interval,
        Interval::Named("default".into())
    );
    within(c.delete_interval(name(), "default".into()).unwrap())
        .await
        .unwrap();
    assert!(reporter(&c).await.intervals.is_empty());
    assert_eq!(
        within(c.subscriptions(name()).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&socket).await, b"ae.reporter.value.value:42|g");
    assert!(reporter(&c).await.intervals.is_empty());
    let mut other = key();
    other.retry = false;
    within(
        c.subscribe(other.clone(), Interval::Named("default".into()))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        within(c.subscriptions(name()).unwrap())
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        within(c.unsubscribe_native(other.clone()).unwrap()).await,
        Err(Error::NotFound)
    );
    within(c.unsubscribe(other).unwrap()).await.unwrap();
    assert_eq!(
        within(c.subscriptions(name()).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
    within(
        c.unsubscribe_all(name(), Selector::Metric(metric()))
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(
        within(c.subscriptions(name()).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    let mut unknown = key();
    unknown.reporter = "no_such_reporter".into();
    assert_eq!(
        within(
            c.subscribe(unknown, Interval::Named("default".into()))
                .unwrap()
        )
        .await,
        Err(Error::UnknownReporter)
    );
    assert_eq!(
        within(c.enable("no_such_reporter".into(), true).unwrap()).await,
        Err(Error::NotFound)
    );
    owner.finish().await;
    logger.finish().await;
}
#[tokio::test]
async fn original_reporter_restart_limit_keeps_mismatched_removal_target_and_recovers_explicitly() {
    let native = native();
    assert_eq!(
        native["reporter_restarts"]["exhaustion_leaves_dead_enabled_reporter_record"],
        true
    );
    let (mut owner, mut logger, socket) = setup(
        Default::default(),
        Arc::new(Gauge::default()),
        Arc::new(TestClock::new()),
    )
    .await;
    let c = owner.client();
    within(c.add(Default::default()).unwrap()).await.unwrap();
    within(
        c.subscribe(key(), Interval::Named("default".into()))
            .unwrap(),
    )
    .await
    .unwrap();
    for generation in 1..=2 {
        within(c.restart(name(), generation).unwrap())
            .await
            .unwrap();
        let r = reporter(&c).await;
        assert!(r.alive && r.previous_joined);
        assert_eq!(r.generation, generation + 1);
        within(c.trigger(name(), "default".into()).unwrap())
            .await
            .unwrap();
        assert_eq!(packet(&socket).await, b"ae.reporter.value.value:42|g");
    }
    assert_eq!(
        within(c.restart(name(), 1).unwrap()).await,
        Err(Error::Stale)
    );
    within(c.restart(name(), 3).unwrap()).await.unwrap();
    let dead = reporter(&c).await;
    assert!(!dead.alive && dead.enabled && dead.previous_joined);
    assert_eq!(dead.generation, 3);
    assert_eq!(dead.restart_attempts, 3);
    within(c.enable(name(), true).unwrap()).await.unwrap();
    assert!(!reporter(&c).await.alive);
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    none(&socket).await;
    assert_eq!(
        within(c.subscriptions(name()).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
    within(c.enable(name(), false).unwrap()).await.unwrap();
    within(c.enable(name(), true).unwrap()).await.unwrap();
    let recovered = reporter(&c).await;
    assert!(recovered.alive);
    assert_eq!(recovered.generation, 4);
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&socket).await, b"ae.reporter.value.value:42|g");
    owner.finish().await;
    logger.finish().await;
}
#[test]
fn original_clock_adjustments_and_restart_windows_match_exact_boundaries() {
    let native: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_reporter_timing.json"
    ))
    .unwrap();
    let n = |r: &serde_json::Value, k: &str| r[k].as_str().unwrap().parse::<i128>().unwrap();
    assert_eq!(native["adjustments"].as_array().unwrap().len(), 68);
    for row in native["adjustments"].as_array().unwrap() {
        assert_eq!(
            timing::adjust(
                n(row, "period") as u64,
                n(row, "planned_us"),
                n(row, "now_us")
            ),
            (n(row, "wait_ms") as u64, n(row, "next_base_us")),
            "{row}"
        );
    }
    assert_eq!(native["restart_windows"].as_array().unwrap().len(), 180);
    for row in native["restart_windows"].as_array().unwrap() {
        let history = row["history_ms"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| i128::from(n.as_i64().unwrap()))
            .collect::<VecDeque<_>>();
        assert_eq!(
            timing::exhausted(&history),
            row["exhausted"].as_bool().unwrap(),
            "{row}"
        );
    }
    let timestamp = |v: &serde_json::Value| {
        i128::from(v[0].as_i64().unwrap()) * 1_000_000_000_000
            + i128::from(v[1].as_i64().unwrap()) * 1_000_000
            + i128::from(v[2].as_i64().unwrap())
    };
    for row in native["fire_timestamps"].as_array().unwrap() {
        assert_eq!(
            timing::next_fire(
                timestamp(&row["timestamp"]),
                row["interval_ms"].as_u64().unwrap()
            ),
            timestamp(&row["result"])
        );
        assert_eq!(row["manual"], row["timestamp"]);
    }
}
#[tokio::test]
async fn actual_delayed_timer_uses_native_drift_adjustment_after_a_slow_source() {
    let clock = Arc::new(TestClock::new());
    let source = Arc::new(Gauge {
        clock: Some(clock.clone()),
        advance: AtomicU64::new(3000),
        ..Default::default()
    });
    let (mut owner, mut logger, socket) = setup(Default::default(), source, clock.clone()).await;
    let c = owner.client();
    let mut config = Config::default();
    config.intervals[0].delay_ms = 2000;
    within(c.add(config).unwrap()).await.unwrap();
    within(
        c.subscribe(key(), Interval::Named("default".into()))
            .unwrap(),
    )
    .await
    .unwrap();
    let timer = reporter(&c).await.intervals[0].timer;
    clock.advance(2000);
    let _ = reporter(&c).await;
    within(async {
        loop {
            if reporter(&c).await.intervals[0].timer != timer {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    none(&socket).await;
    clock.advance(10000);
    let _ = reporter(&c).await;
    assert_eq!(packet(&socket).await, b"ae.reporter.value.value:42|g");
    assert_eq!(clock.mono_ms.load(Ordering::Relaxed), 15000);
    clock.advance(6999);
    let _ = reporter(&c).await;
    none(&socket).await;
    clock.advance(1);
    let _ = reporter(&c).await;
    assert_eq!(packet(&socket).await, b"ae.reporter.value.value:42|g");
    owner.finish().await;
    logger.finish().await;
}
#[tokio::test]
async fn cancelled_disable_and_finish_keep_actual_blocked_work_owned_and_bound_unread_replies() {
    let source = Arc::new(Gauge::default());
    let (mut owner, mut logger, socket) = setup(
        Limits {
            requests: 2,
            ..Default::default()
        },
        source.clone(),
        Arc::new(TestClock::new()),
    )
    .await;
    let c = owner.client();
    within(c.add(Default::default()).unwrap()).await.unwrap();
    within(
        c.subscribe(key(), Interval::Named("default".into()))
            .unwrap(),
    )
    .await
    .unwrap();
    let completed = c.info().completed;
    let first = c.reporters().unwrap();
    let second = c.reporters().unwrap();
    within(async {
        while c.info().completed < completed + 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert_eq!(c.info().pending, 2);
    assert!(matches!(c.reporters(), Err(Error::Busy)));
    assert!(c.info().pending_bytes > 128);
    drop(first);
    within(second).await.unwrap();
    assert_eq!(c.info().pending, 0);
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    *source.gate.lock().unwrap() = Some(Gate {
        entered,
        release: held,
    });
    let release = Release(Some(release));
    let trigger = c.trigger(name(), "default".into()).unwrap();
    within(waiting).await.unwrap();
    drop(trigger);
    let disabled = c.enable(name(), false).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), disabled)
            .await
            .is_err()
    );
    assert_eq!(c.info().pending, 2);
    assert!(matches!(c.reporters(), Err(Error::Busy)));
    drop(release);
    within(async {
        while c.info().pending != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    let r = reporter(&c).await;
    assert!(!r.alive && !r.enabled && r.previous_joined);
    assert_eq!(packet(&socket).await, b"ae.reporter.value.value:42|g");
    within(c.enable(name(), true).unwrap()).await.unwrap();
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    *source.gate.lock().unwrap() = Some(Gate {
        entered,
        release: held,
    });
    let release = Release(Some(release));
    let trigger = c.trigger(name(), "default".into()).unwrap();
    within(waiting).await.unwrap();
    drop(trigger);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), owner.finish())
            .await
            .is_err()
    );
    assert!(c.info().running && !c.info().joined);
    drop(release);
    within(owner.finish()).await;
    assert!(c.info().joined && !c.info().running);
    assert_eq!(packet(&socket).await, b"ae.reporter.value.value:42|g");
    logger.finish().await;
}

#[tokio::test]
async fn native_ordered_set_subscription_keys_preserve_numeric_identity_and_reverse_listing() {
    use report::{Extra, OptionValue, Tag, Value};
    use serde_json::Value as J;
    fn p(v: &J) -> Part {
        if let Some(s) = v["atom"].as_str() {
            Part::atom(s)
        } else if let Some(s) = v["integer"].as_str() {
            Part::Integer(s.parse().unwrap())
        } else if let Some(s) = v["binary"].as_str() {
            Part::Binary(hex::decode(s).unwrap())
        } else {
            Part::List(
                v["list"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|n| n["integer"].as_str().unwrap().parse().unwrap())
                    .collect(),
            )
        }
    }
    fn n(v: &J) -> Value {
        if let Some(s) = v["integer"].as_str() {
            Value::Integer(s.parse().unwrap())
        } else {
            Value::Float(f64::from_bits(
                u64::from_str_radix(v["float"].as_str().unwrap(), 16).unwrap(),
            ))
        }
    }
    let native: J = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_reporter_identity.json"
    ))
    .unwrap();
    let native = &native["identity"];
    let (mut owner, mut logger, _) = setup(
        Default::default(),
        Arc::new(Gauge::default()),
        Arc::new(TestClock::new()),
    )
    .await;
    let c = owner.client();
    within(c.add(Default::default()).unwrap()).await.unwrap();
    for row in native["inputs"].as_array().unwrap() {
        let tuple = row["key"]["tuple"].as_array().unwrap();
        let metric = if let Some(t) = tuple[0]["tuple"].as_array() {
            Selector::Find(t[1]["list"].as_array().unwrap().iter().map(p).collect())
        } else {
            Selector::Metric(tuple[0]["list"].as_array().unwrap().iter().map(p).collect())
        };
        let points = if let Some(l) = tuple[1]["list"].as_array() {
            Points::Many(l.iter().map(p).collect())
        } else {
            Points::One(p(&tuple[1]))
        };
        let retry = tuple[2]["atom"] == "true";
        let extra = if tuple[3]["atom"] == "undefined" {
            Extra::Undefined
        } else {
            Extra::List(
                tuple[3]["list"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|o| {
                        let o = o["tuple"].as_array().unwrap();
                        if o[0]["atom"] == "report_type" {
                            OptionValue::ReportType(o[1]["atom"].as_str().unwrap().into())
                        } else {
                            OptionValue::Tags(
                                o[1]["list"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .map(|t| {
                                        if let Some(t) = t["tuple"].as_array() {
                                            Tag::Pair(n(&t[0]), n(&t[1]))
                                        } else {
                                            Tag::Text(p(t))
                                        }
                                    })
                                    .collect(),
                            )
                        }
                    })
                    .collect(),
            )
        };
        let subscription = Subscription {
            reporter: name(),
            metric,
            points,
            retry,
            extra,
        };
        within(
            c.subscribe(subscription, Interval::Named(format!("slot_{}", row["id"])))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            c.info().subscriptions,
            row["count"].as_u64().unwrap() as usize
        );
    }
    let listed = within(c.subscriptions(name()).unwrap()).await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|s| match &s.interval {
                Interval::Named(n) => n.as_str(),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>(),
        native["listed"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["interval"].as_str().unwrap())
            .collect::<Vec<_>>()
    );
    let ascending = listed
        .iter()
        .rev()
        .map(|s| s.key.clone())
        .collect::<Vec<_>>();
    assert!(
        ascending
            .windows(2)
            .all(|p| super::order::compare(&p[0], &p[1]).is_lt())
    );
    let mut unsupported = key();
    unsupported.extra = Extra::Other;
    assert!(matches!(
        c.subscribe(unsupported, Interval::Milliseconds(1)),
        Err(Error::UnsupportedKey)
    ));
    let mut duplicate = Config {
        name: "duplicate".into(),
        ..Default::default()
    };
    duplicate.intervals.push(duplicate.intervals[0].clone());
    assert!(matches!(c.add(duplicate), Err(Error::Configuration)));
    owner.finish().await;
    logger.finish().await;
}

#[tokio::test]
async fn application_store_updates_wait_for_reporter_ticks_and_survive_logger_replacement() {
    let store = Arc::new(Store::new(Default::default()).unwrap());
    let clock = Arc::new(TestClock::new());
    let (mut owner, mut logger, socket) =
        setup(Default::default(), store.clone(), clock.clone()).await;
    let c = owner.client();
    within(c.add(Default::default()).unwrap()).await.unwrap();
    within(c.subscribe_application(name()).unwrap())
        .await
        .unwrap();
    fn m(name: &str, kind: &str, values: &[(&str, i32)]) -> Metric {
        Metric {
            entry: crate::routing::Entry {
                name: name.split('.').map(Part::atom).collect(),
                kind: kind.into(),
                enabled: true,
            },
            values: values
                .iter()
                .map(|(p, v)| (Part::atom(*p), report::Value::Integer((*v).into())))
                .collect(),
        }
    }
    store
        .put(vec![
            m("ae.epoch.z", "counter", &[("value", 2)]),
            m("ae.epoch.a", "gauge", &[("value", 1)]),
            m("ae.epoch.probe", "probe", &[("one", 3), ("two", 4)]),
            m("unrelated", "gauge", &[("value", 5)]),
        ])
        .unwrap();
    within(async {
        while c.info().subscriptions != 3 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    let subs = within(c.subscriptions(name()).unwrap()).await.unwrap();
    assert_eq!(subs[0].key.points, Points::Many(vec![Part::atom("value")]));
    assert_eq!(subs[1].key.points, Points::default_points());
    assert!(
        subs.iter()
            .all(|s| s.key.extra == report::Extra::Undefined && s.key.retry)
    );
    none(&socket).await;
    clock.advance(10_000);
    let _ = reporter(&c).await;
    for expected in [
        "ae.epoch.a.value:1|g",
        "ae.epoch.probe.one:3|g",
        "ae.epoch.probe.two:4|g",
        "ae.epoch.z.value:2|g",
    ] {
        assert_eq!(packet(&socket).await, expected.as_bytes());
    }
    let timer = reporter(&c).await.intervals[0].timer;
    store
        .put(vec![m("ae.epoch.a", "gauge", &[("value", 9)])])
        .unwrap();
    none(&socket).await;
    let old = logger.handle().current().unwrap();
    let ticket = logger.handle().restart(old.number).unwrap();
    within(async {
        loop {
            logger.poll().await;
            if logger.handle().status().phase == Phase::Running
                && logger.handle().current().unwrap().number > old.number
            {
                break;
            }
            logger.changed().await;
        }
    })
    .await;
    within(ticket.wait()).await.unwrap();
    assert!(old.client.info().joined);
    assert_eq!(reporter(&c).await.intervals[0].timer, timer);
    clock.advance(10_000);
    let _ = reporter(&c).await;
    assert_eq!(packet(&socket).await, b"ae.epoch.a.value:9|g");
    for expected in [
        "ae.epoch.probe.one:3|g",
        "ae.epoch.probe.two:4|g",
        "ae.epoch.z.value:2|g",
    ] {
        assert_eq!(packet(&socket).await, expected.as_bytes());
    }
    owner.finish().await;
    logger.finish().await;
}

#[tokio::test]
async fn numeric_retry_timer_stops_on_missing_metric_and_rearms_on_new_entry() {
    let store = Arc::new(Store::new(Default::default()).unwrap());
    let metric = Metric {
        entry: crate::routing::Entry {
            name: metric(),
            kind: "gauge".into(),
            enabled: true,
        },
        values: vec![(Part::atom("value"), report::Value::Integer(42.into()))],
    };
    store.put(vec![metric.clone()]).unwrap();
    let clock = Arc::new(TestClock::new());
    let (mut owner, mut logger, socket) =
        setup(Default::default(), store.clone(), clock.clone()).await;
    let c = owner.client();
    within(c.add(Default::default()).unwrap()).await.unwrap();
    let mut key = key();
    key.retry = false;
    within(
        c.subscribe(key.clone(), Interval::Milliseconds(100))
            .unwrap(),
    )
    .await
    .unwrap();
    store.remove(&metric.entry.name).unwrap();
    clock.advance(100);
    let _ = reporter(&c).await;
    assert!(!within(c.subscriptions(name()).unwrap()).await.unwrap()[0].armed);
    none(&socket).await;
    store.put(vec![metric]).unwrap();
    within(async {
        loop {
            if within(c.subscriptions(name()).unwrap()).await.unwrap()[0].armed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    clock.advance(100);
    let _ = reporter(&c).await;
    assert_eq!(packet(&socket).await, b"ae.reporter.value.value:42|g");
    within(c.enable(name(), false).unwrap()).await.unwrap();
    let disabled = within(c.subscriptions(name()).unwrap())
        .await
        .unwrap()
        .remove(0);
    assert!(!disabled.armed && disabled.timer.is_none());
    clock.advance(100);
    let _ = reporter(&c).await;
    none(&socket).await;
    within(c.enable(name(), true).unwrap()).await.unwrap();
    clock.advance(100);
    let _ = reporter(&c).await;
    assert_eq!(packet(&socket).await, b"ae.reporter.value.value:42|g");
    owner.finish().await;
    logger.finish().await;
}

#[tokio::test]
async fn application_static_rules_and_new_entries_match_original_configuration() {
    use serde_json::{Value as J, json};
    let native: J = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_reporter_static.json"
    ))
    .unwrap();
    let native = &native["static_subscriptions"];
    let store = Arc::new(Store::new(Default::default()).unwrap());
    let (mut owner, mut logger, _) = setup(
        Default::default(),
        store.clone(),
        Arc::new(TestClock::new()),
    )
    .await;
    let c = owner.client();
    within(c.add(Default::default()).unwrap()).await.unwrap();
    within(c.subscribe_application(name()).unwrap())
        .await
        .unwrap();
    fn metric(name: Vec<Part>, kind: &str, enabled: bool) -> Metric {
        Metric {
            entry: crate::routing::Entry {
                name,
                kind: kind.into(),
                enabled,
            },
            values: vec![(Part::atom("value"), report::Value::Integer(0.into()))],
        }
    }
    let rows = native["catalog"]["list"].as_array().unwrap();
    let entries = rows
        .iter()
        .map(|r| {
            let t = &r["tuple"];
            metric(
                t[0]["list"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|n| Part::atom(n["atom"].as_str().unwrap()))
                    .collect(),
                t[1]["atom"].as_str().unwrap(),
                t[2]["atom"] == "enabled",
            )
        })
        .collect();
    store.put(entries).unwrap();
    async fn verify(c: &Client, expected: &J) {
        let count = expected.as_array().unwrap().len();
        within(async {
            while c.info().subscriptions != count {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        let subs = within(c.subscriptions(name()).unwrap()).await.unwrap();
        let actual = subs.iter().map(|s| {
            let Selector::Metric(name) = &s.key.metric else {panic!("concrete static subscription")};
            let name = json!({"list":name.iter().map(|p|match p {Part::Atom(s)=>json!({"atom":s}),_=>unreachable!()}).collect::<Vec<_>>()});
            let points = match &s.key.points {Points::One(Part::Atom(s))=>json!({"atom":s}),Points::Many(v)=>json!({"list":v.iter().map(|p|match p {Part::Atom(s)=>json!({"atom":s}),_=>unreachable!()}).collect::<Vec<_>>()}),_=>unreachable!()};
            assert!(s.key.retry && s.key.extra == report::Extra::Undefined);
            json!({"tuple":[name,points,{"atom":"default"},{"atom":"undefined"}]})
        }).collect::<Vec<_>>();
        assert_eq!(json!(actual), *expected);
    }
    verify(&c, &native["initial"]).await;
    store
        .put(vec![metric(
            vec![Part::atom("ae"), Part::atom("epoch"), Part::atom("new")],
            "gauge",
            true,
        )])
        .unwrap();
    verify(&c, &native["after_new_entry"]).await;
    store
        .put(vec![metric(
            vec![
                Part::atom("ae"),
                Part::atom("epoch"),
                Part::atom("disabled"),
            ],
            "gauge",
            true,
        )])
        .unwrap();
    verify(&c, &native["after_enabled_entry"]).await;
    owner.finish().await;
    logger.finish().await;
}

#[tokio::test]
async fn callback_limit_failure_restarts_only_the_joined_reporter_and_owner_panic_closes_receipts()
{
    struct Bad;
    impl Source for Bad {
        fn valid(&self, _: &Selector, _: &Points) -> Result<bool, Error> {
            Ok(true)
        }
        fn read(&self, m: &Selector, _: &Points) -> Result<Vec<Sample>, Error> {
            let Selector::Metric(name) = m else {
                unreachable!()
            };
            if name == &metric() {
                panic!("controlled reporter source panic");
            }
            Ok(vec![Sample {
                metric: name.clone(),
                values: vec![(Part::atom("value"), report::Value::Integer(42.into()))],
            }])
        }
    }
    let (mut owner, mut logger, socket) = setup(
        Default::default(),
        Arc::new(Bad),
        Arc::new(TestClock::new()),
    )
    .await;
    let c = owner.client();
    within(c.add(Default::default()).unwrap()).await.unwrap();
    let mut bad = key();
    bad.metric = Selector::Metric(vec![Part::atom("a"); 64]);
    within(
        c.subscribe(bad.clone(), Interval::Named("default".into()))
            .unwrap(),
    )
    .await
    .unwrap();
    for attempt in 1..=3 {
        within(c.trigger(name(), "default".into()).unwrap())
            .await
            .unwrap();
        let r = within(async {
            loop {
                let r = reporter(&c).await;
                if r.restart_attempts == attempt {
                    break r;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(r.previous_joined);
        assert_eq!(r.last_error, Some(Error::Report(report::Error::Limit)));
        assert_eq!(r.alive, attempt < 3);
    }
    assert_eq!(logger.handle().current().unwrap().number, 1);
    assert!(logger.client().info().running && logger.client().input().unwrap().info().running);
    none(&socket).await;
    within(c.unsubscribe(bad).unwrap()).await.unwrap();
    within(
        c.subscribe(key(), Interval::Named("default".into()))
            .unwrap(),
    )
    .await
    .unwrap();
    within(c.enable(name(), false).unwrap()).await.unwrap();
    within(c.enable(name(), true).unwrap()).await.unwrap();
    assert_eq!(
        within(c.trigger(name(), "default".into()).unwrap()).await,
        Err(Error::Closed)
    );
    within(owner.finish()).await;
    assert!(c.info().joined && !c.info().running);
    assert_eq!(c.info().last_error, Some(Error::Panicked));
    assert_eq!(c.info().pending, 0);
    assert!(logger.client().info().running);
    logger.finish().await;
}

#[tokio::test]
async fn zero_interval_preserves_control_progress_and_store_batch_rejection_is_atomic() {
    let limits = Limits {
        retained_bytes: 1000,
        ..Default::default()
    };
    let store = Store::new(limits).unwrap();
    let mut m = Metric {
        entry: crate::routing::Entry {
            name: metric(),
            kind: "gauge".into(),
            enabled: true,
        },
        values: vec![(Part::atom("value"), report::Value::Integer(1.into()))],
    };
    store.put(vec![m.clone()]).unwrap();
    let revision = store.catalog(None).unwrap().unwrap().revision;
    m.values[0].1 = report::Value::Integer(9.into());
    let mut other = m.clone();
    other.entry.name.push(Part::atom("other"));
    let mut third = other.clone();
    third.entry.name.push(Part::atom("third"));
    assert_eq!(store.put(vec![m, other, third]), Err(Error::Limit));
    assert_eq!(store.catalog(None).unwrap().unwrap().revision, revision);
    assert_eq!(
        store
            .read(&Selector::Metric(metric()), &Points::default_points())
            .unwrap()[0]
            .values[0]
            .1,
        report::Value::Integer(1.into())
    );
    let (mut owner, mut logger, _) = setup(
        Default::default(),
        Arc::new(store),
        Arc::new(TestClock::new()),
    )
    .await;
    let c = owner.client();
    let mut config = Config::default();
    config.intervals[0].period = Period::Milliseconds(0);
    within(c.add(config).unwrap()).await.unwrap();
    within(c.enable(name(), false).unwrap()).await.unwrap();
    assert!(!reporter(&c).await.alive);
    owner.finish().await;
    logger.finish().await;
}

#[tokio::test]
async fn live_scalar_registry_controls_feed_static_subscriptions_and_survive_reporter_replacement()
{
    use crate::observe::registry::{Kind, Value};
    let limits = Limits::default();
    let clock = Arc::new(TestClock::new());
    let store = Arc::new(Store::with_clock(limits, clock.clone()).unwrap());
    let (mut owner, mut logger, socket) = setup(limits, store.clone(), clock).await;
    let c = owner.client();
    let mut config = Config::default();
    config.intervals[0].period = Period::Manual;
    within(c.add(config).unwrap()).await.unwrap();
    within(c.subscribe_application(name()).unwrap())
        .await
        .unwrap();
    let n = vec![Part::atom("ae"), Part::atom("epoch"), Part::atom("scalar")];
    store.create(&n, Kind::Counter, true).unwrap();
    store.update(&n, &Value::Integer(42.into())).unwrap();
    within(async {
        loop {
            if !c.subscriptions(name()).unwrap().await.unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&socket).await, b"ae.epoch.scalar.value:42|g");
    store.set_enabled(&n, false).unwrap();
    store.update(&n, &Value::Integer(999.into())).unwrap();
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    none(&socket).await;
    store.set_enabled(&n, true).unwrap();
    let old = reporter(&c).await;
    within(c.restart(name(), old.generation).unwrap())
        .await
        .unwrap();
    within(async {
        while reporter(&c).await.generation == old.generation {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(reporter(&c).await.previous_joined);
    store.update(&n, &Value::Integer((-2).into())).unwrap();
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&socket).await, b"ae.epoch.scalar.value:40|g");
    store.reset(&n).unwrap();
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&socket).await, b"ae.epoch.scalar.value:0|g");
    store.remove(&n).unwrap();
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    none(&socket).await;
    store.create(&n, Kind::Gauge, true).unwrap();
    store.update(&n, &Value::Atom("retained".into())).unwrap();
    within(c.trigger(name(), "default".into()).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&socket).await, b"ae.epoch.scalar.value:\0|g");
    owner.finish().await;
    logger.finish().await;
    assert!(c.info().joined && logger.client().info().joined);
}
