use super::*;
use crate::observe::{
    registry,
    supervision::{Budget, Child},
};
use serde_json::{Value as Json, json};

fn counter() -> Name {
    ["ae", "epoch", "counter"].map(Part::atom).to_vec()
}
fn value() -> Part {
    Part::atom("value")
}
fn atom(s: &str) -> Json {
    json!({"atom":s})
}
fn tuple(v: Vec<Json>) -> Json {
    json!({"tuple":v})
}
fn list(v: Vec<Json>) -> Json {
    json!({"list":v})
}
fn point(p: &Part) -> Json {
    match p {
        Part::Atom(s) => atom(s),
        _ => panic!("fixture atom"),
    }
}
fn subscription(s: &SubscriptionInfo) -> Json {
    let Selector::Metric(n) = &s.key.metric else {
        panic!("fixture metric")
    };
    let dp = match &s.key.points {
        Points::One(p) => point(p),
        Points::Many(p) => list(p.iter().map(point).collect()),
    };
    let period = match &s.interval {
        Interval::Named(n) => atom(n),
        Interval::Milliseconds(n) => json!({"integer":n.to_string()}),
    };
    tuple(vec![
        list(n.iter().map(point).collect()),
        dp,
        period,
        list(vec![]),
    ])
}
async fn subscriptions(client: &Client, name: &str) -> Json {
    list(
        within(client.subscriptions(name.into()).unwrap())
            .await
            .unwrap()
            .iter()
            .map(subscription)
            .collect(),
    )
}
async fn entries(client: &Client) -> Json {
    let mut result = Vec::new();
    for r in within(client.reporters().unwrap()).await.unwrap() {
        result.push(tuple(vec![
            atom(&r.name),
            atom(if r.alive { "true" } else { "false" }),
            subscriptions(client, &r.name).await,
        ]));
    }
    list(result)
}
fn key_for(name: &str, datapoint: &str) -> Subscription {
    Subscription {
        reporter: name.into(),
        metric: Selector::Metric(counter()),
        points: Points::One(Part::atom(datapoint)),
        retry: true,
        extra: report::Extra::List(vec![]),
    }
}
struct Core {
    reporter: Owner,
    logger: Logger,
    registry: registry::Owner,
    store: Arc<Store>,
    budget: Budget,
    socket: UdpSocket,
}
impl Core {
    async fn new(limits: Limits) -> Self {
        Self::source(limits, |s| s).await
    }
    async fn source(limits: Limits, wrap: impl FnOnce(Arc<Store>) -> Arc<dyn Source>) -> Self {
        let budget = Budget::default();
        let clock = Arc::new(TestClock::new());
        let store = Arc::new(Store::with_clock(limits, clock.clone()).unwrap());
        let registry = registry::Owner::spawn_with_parent(store.clone(), budget.clone()).unwrap();
        registry
            .client()
            .create(&counter(), registry::Kind::Counter, true)
            .unwrap()
            .await
            .unwrap();
        registry
            .client()
            .update(&counter(), &registry::Value::Integer(11.into()))
            .unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut logger = Logger::prepare_with_parent(
            statsd::Settings {
                target: statsd::Config {
                    host: statsd::Host::Name("127.0.0.1".into()),
                    port: socket.local_addr().unwrap().port(),
                    ..Default::default()
                },
                pipeline: Some(pipeline::Config::default()),
                ..Default::default()
            },
            budget.clone(),
        )
        .unwrap();
        logger.activate();
        ready(&mut logger).await;
        let reporter = Owner::spawn_with_clock_and_parent(
            limits,
            wrap(store.clone()),
            logger.client(),
            clock,
            budget.clone(),
        )
        .unwrap();
        Self {
            reporter,
            logger,
            registry,
            store,
            budget,
            socket,
        }
    }
    async fn finish(&mut self) {
        within(self.reporter.finish()).await;
        within(self.logger.finish()).await;
        within(self.registry.finish()).await;
    }
    fn generations(&self) -> Vec<u64> {
        let r = self.registry.client().info();
        let mut ids = r.managers.iter().map(|m| m.generation).collect::<Vec<_>>();
        ids.push(self.reporter.client().info().manager_generation);
        ids.push(self.logger.handle().status().supervisor_generation);
        ids
    }
    async fn add(&self, name: &str, module: &str) {
        let c = self.reporter.client();
        within(
            c.add(Config {
                name: name.into(),
                module: module.into(),
                intervals: vec![NamedInterval {
                    name: "default".into(),
                    period: Period::Milliseconds(60_000),
                    delay_ms: 0,
                }],
                ..Default::default()
            })
            .unwrap(),
        )
        .await
        .unwrap();
        within(
            c.subscribe(key_for(name, "value"), Interval::Named("default".into()))
                .unwrap(),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn native_manager_recovery_retains_subscriptions_budgets_and_distinct_timer_lifetimes() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_report_manager.json"
    ))
    .unwrap();
    let mut core = Core::new(Limits::default()).await;
    core.add("active", "metrics_owner_callback").await;
    core.add("disabled", "metrics_owner_callback").await;
    let c = core.reporter.client();
    within(
        c.subscribe(
            key_for("active", "ms_since_reset"),
            Interval::Milliseconds(60_000),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    within(c.enable("disabled".into(), false).unwrap())
        .await
        .unwrap();
    assert_eq!(entries(&c).await, native["before"]);
    let named = within(c.reporters().unwrap()).await.unwrap()[0].intervals[0].timer;
    let initial_subs = within(c.subscriptions("active".into()).unwrap())
        .await
        .unwrap();
    let individual = initial_subs
        .iter()
        .find(|s| s.interval == Interval::Milliseconds(60_000))
        .unwrap()
        .timer;
    assert_eq!(within(c.restart_manager(1).unwrap()).await, Ok(2));
    assert_eq!(entries(&c).await, native["after_first"]);
    let rows = within(c.reporters().unwrap()).await.unwrap();
    assert!(
        rows.iter()
            .all(|r| r.enabled && r.alive && r.previous_joined && r.generation == 2)
    );
    assert_eq!(
        json!(rows[0].intervals[0].timer == named),
        native["original_named_timer_reference_retained"]
    );
    assert_eq!(
        atom(if rows[0].intervals[0].armed {
            "true"
        } else {
            "false"
        }),
        native["named_timer_after_first_parent_restart"]
    );
    let subs = within(c.subscriptions("active".into()).unwrap())
        .await
        .unwrap();
    let current = subs
        .iter()
        .find(|s| s.interval == Interval::Milliseconds(60_000))
        .unwrap();
    assert_eq!(
        json!(current.timer != individual),
        native["individual_timer_replaced_after_first"]
    );
    assert_eq!(
        json!(current.armed),
        native["individual_timer_active_after_first"]
    );
    assert!(c.info().manager_previous_joined);
    within(c.trigger("active".into(), "default".into()).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&core.socket).await, b"ae.epoch.counter.value:11|g");
    assert_eq!(within(c.restart_manager(2).unwrap()).await, Ok(3));
    assert_eq!(entries(&c).await, native["after_second"]);
    within(c.trigger("active".into(), "default".into()).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&core.socket).await, b"ae.epoch.counter.value:11|g");
    assert_eq!(within(c.restart_manager(3).unwrap()).await, Ok(4));
    assert_eq!(entries(&c).await, native["after_third"]);
    assert_eq!(
        list(vec![
            tuple(vec![atom("active"), subscriptions(&c, "active").await]),
            tuple(vec![atom("disabled"), subscriptions(&c, "disabled").await])
        ]),
        native["subscriptions_after_removal"]
    );
    assert_eq!(core.budget.info().restart_attempts, 3);
    assert_eq!(core.budget.info().attempts_for(Child::Report), 3);
    assert_eq!(core.generations(), vec![1, 1, 1, 4, 1]);
    core.finish().await;
}

#[tokio::test]
async fn native_manager_removal_keeps_subscriptions_even_when_name_equals_module() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_report_manager_same_name.json"
    ))
    .unwrap();
    let mut core = Core::new(Limits::default()).await;
    core.add("metrics_owner_callback", "metrics_owner_callback")
        .await;
    let c = core.reporter.client();
    for (i, row) in native["replacements"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        assert_eq!(
            within(c.restart_manager(i as u64 + 1).unwrap()).await,
            Ok(i as u64 + 2)
        );
        let rows = within(c.reporters().unwrap()).await.unwrap();
        assert_eq!(json!(!rows.is_empty()), row["reporter_present"]);
        assert!(rows.iter().all(|r| r.previous_joined));
        assert_eq!(
            subscriptions(&c, "metrics_owner_callback").await,
            row["subscriptions"]
        );
    }
    assert_eq!(
        subscriptions(&c, "metrics_owner_callback").await,
        native["subscriptions_after_removal"]
    );
    core.finish().await;
}

#[tokio::test]
async fn native_manager_removal_can_target_another_reporter_by_its_module_name() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_report_manager_collision.json"
    ))
    .unwrap();
    let mut core = Core::new(Limits::default()).await;
    core.add("active", "metrics_owner_callback").await;
    let c = core.reporter.client();
    for (i, row) in native["replacements"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        if i == 2 {
            core.add("metrics_owner_callback", "metrics_owner_callback")
                .await;
            let rows = within(c.reporters().unwrap()).await.unwrap();
            assert_eq!(
                list(rows.iter().map(|r| atom(&r.name)).collect()),
                native["before_third"]
            );
        }
        assert_eq!(
            within(c.restart_manager(i as u64 + 1).unwrap()).await,
            Ok(i as u64 + 2)
        );
        let rows = within(c.reporters().unwrap()).await.unwrap();
        assert!(rows.iter().all(|r| r.previous_joined));
        assert_eq!(
            list(rows.iter().map(|r| atom(&r.name)).collect()),
            row["remaining"]
        );
    }
    assert_eq!(
        subscriptions(&c, "active").await,
        native["exhausted_subscriptions"]
    );
    assert_eq!(
        subscriptions(&c, "metrics_owner_callback").await,
        native["other_subscriptions"]
    );
    assert_eq!(core.budget.info().restart_attempts, 3);
    core.finish().await;
}

#[tokio::test]
async fn original_five_child_mixed_restart_trace_exhausts_one_parent_and_joins_every_owner() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_core_parent.json"
    ))
    .unwrap();
    let mut core = Core::new(Limits::default()).await;
    core.add("active", "metrics_owner_callback").await;
    let registry = core.registry.client();
    let alias = registry::Alias::Atom("counter".into());
    registry
        .add_alias(&alias, &counter(), &value())
        .unwrap()
        .await
        .unwrap();
    registry
        .cache_write(
            &counter(),
            &value(),
            &registry::Value::Integer(7.into()),
            60_000,
        )
        .unwrap();
    for row in native["allowed_replacements"].as_array().unwrap() {
        let before = core.generations();
        let changed = match row["child"].as_str().unwrap() {
            "exometer_admin" => {
                assert_eq!(
                    within(registry.restart(registry::Manager::Admin, 1).unwrap()).await,
                    Ok(2)
                );
                0
            }
            "exometer_cache" => {
                assert_eq!(
                    within(registry.restart(registry::Manager::Cache, 1).unwrap()).await,
                    Ok(2)
                );
                1
            }
            "exometer_alias" => {
                assert_eq!(
                    within(registry.restart(registry::Manager::Alias, 1).unwrap()).await,
                    Ok(2)
                );
                2
            }
            "exometer_report" => {
                assert_eq!(
                    within(core.reporter.client().restart_manager(1).unwrap()).await,
                    Ok(2)
                );
                3
            }
            "exometer_report_logger_sup" => {
                let old = core.logger.handle().current().unwrap();
                let ticket = core.logger.handle().restart_supervisor(1).unwrap();
                within(core.logger.poll()).await;
                assert_eq!(within(ticket.wait()).await, Ok(2));
                assert!(old.client.info().joined && old.client.input().unwrap().info().joined);
                assert_eq!(core.logger.handle().status().phase, Phase::Empty);
                4
            }
            other => panic!("unknown native child {other}"),
        };
        let after = core.generations();
        assert_eq!(
            json!(
                before
                    .iter()
                    .zip(&after)
                    .enumerate()
                    .all(|(i, (a, b))| if i == changed { *b == *a + 1 } else { a == b })
            ),
            row["only_target_replaced"]
        );
        assert_eq!(
            registry
                .get_value(&counter(), &Points::One(value()))
                .unwrap(),
            registry::Reading::Values(vec![(
                value(),
                registry::Value::Integer(row["scalar_retained"].as_i64().unwrap().into())
            )])
        );
        assert_eq!(
            registry.alias_value(&alias).unwrap(),
            registry::Reading::Values(vec![(
                value(),
                registry::Value::Integer(row["alias_retained"].as_i64().unwrap().into())
            )])
        );
        assert_eq!(
            registry.cache_read(&counter(), &value()).unwrap(),
            Some(registry::Value::Integer(
                row["cache_retained"].as_i64().unwrap().into()
            ))
        );
    }
    assert_eq!(
        json!(core.budget.info().restart_attempts),
        native["allowed_count"]
    );
    assert_eq!(
        within(registry.restart(registry::Manager::Admin, 2).unwrap()).await,
        Err(registry::Error::RestartLimit)
    );
    assert!(core.budget.info().exhausted);
    assert_eq!(core.budget.info().restart_attempts, 6);
    within(core.logger.poll()).await;
    core.finish().await;
    assert!(core.reporter.client().info().joined && !core.reporter.client().info().running);
    assert_eq!(core.logger.handle().status().phase, Phase::Stopped);
    assert!(registry.info().joined && registry.info().managers.iter().all(|m| m.joined));
    assert_eq!(core.store.retained_bytes(), 0);
    assert_eq!(
        core.store.get_value(&counter(), &Points::One(value())),
        Err(registry::Error::Closed)
    );
}

#[tokio::test]
async fn unread_receipts_keep_the_same_quota_across_joined_manager_generations() {
    let mut core = Core::new(Limits {
        requests: 2,
        ..Default::default()
    })
    .await;
    core.add("active", "metrics_owner_callback").await;
    let c = core.reporter.client();
    let old = c.reporters().unwrap();
    within(async {
        while c.info().completed < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let next = c.restart_manager(1).unwrap();
    within(async {
        while c.info().manager_generation != 2 || !c.info().running {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(c.info().pending, 2);
    assert!(matches!(c.reporters(), Err(Error::Busy)));
    assert_eq!(within(old).await.unwrap()[0].generation, 1);
    assert_eq!(within(next).await, Ok(2));
    assert_eq!(c.info().pending, 0);
    assert_eq!(c.info().pending_bytes, 0);
    assert!(matches!(c.restart_manager(1), Err(Error::Stale)));
    core.finish().await;
}

struct Controlled {
    store: Arc<Store>,
    panic: std::sync::atomic::AtomicBool,
    gate: Mutex<Option<Gate>>,
}
impl Source for Controlled {
    fn watch(&self, signal: Signal) -> Result<(), Error> {
        self.store.watch(signal)
    }
    fn catalog(&self, after: Option<u64>) -> Result<Option<Catalog>, Error> {
        self.store.catalog(after)
    }
    fn valid(&self, metric: &Selector, points: &Points) -> Result<bool, Error> {
        self.store.valid(metric, points)
    }
    fn read(&self, metric: &Selector, points: &Points) -> Result<Vec<Sample>, Error> {
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            let _ = gate.release.recv();
        }
        assert!(
            !self.panic.swap(false, Ordering::AcqRel),
            "controlled manager panic"
        );
        self.store.read(metric, points)
    }
}
async fn controlled() -> (Core, Arc<Controlled>) {
    let mut source = None;
    let core = Core::source(Limits::default(), |store| {
        let wrapped = Arc::new(Controlled {
            store,
            panic: false.into(),
            gate: Mutex::new(None),
        });
        source = Some(wrapped.clone());
        wrapped
    })
    .await;
    (core, source.unwrap())
}

#[tokio::test]
async fn actual_manager_panic_recovers_retained_callbacks_without_polling_the_parent() {
    let (mut core, source) = controlled().await;
    core.add("active", "metrics_owner_callback").await;
    let c = core.reporter.client();
    source.panic.store(true, Ordering::Release);
    let interrupted = c.trigger("active".into(), "default".into()).unwrap();
    assert_eq!(within(interrupted).await, Err(Error::Closed));
    within(async {
        while c.info().manager_generation != 2 || !c.info().running {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(c.info().last_manager_error, Some(Error::Panicked));
    assert!(c.info().manager_previous_joined);
    let r = within(c.reporters().unwrap()).await.unwrap().remove(0);
    assert!(r.alive && r.previous_joined && r.generation == 2);
    assert_eq!(core.generations(), vec![1, 1, 1, 2, 1]);
    within(c.trigger("active".into(), "default".into()).unwrap())
        .await
        .unwrap();
    assert_eq!(packet(&core.socket).await, b"ae.epoch.counter.value:11|g");
    assert_eq!(core.budget.info().attempts_for(Child::Report), 1);
    core.finish().await;
}

#[tokio::test]
async fn cancelled_replacement_joins_inflight_work_discards_old_queue_and_keeps_catalog_watch() {
    let (mut core, source) = controlled().await;
    core.add("active", "metrics_owner_callback").await;
    let c = core.reporter.client();
    within(c.subscribe_application("active".into()).unwrap())
        .await
        .unwrap();
    // Keep only the native application subscription (list datapoints/undefined
    // extra); the explicit scalar subscription has a distinct ordered-set key.
    within(c.unsubscribe(key_for("active", "value")).unwrap())
        .await
        .unwrap();
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    *source.gate.lock().unwrap() = Some(Gate {
        entered,
        release: held,
    });
    let release = Release(Some(release));
    let trigger = c.trigger("active".into(), "default".into()).unwrap();
    within(waiting).await.unwrap();
    let old_queue = c.enable("active".into(), false).unwrap();
    let restart = c.restart_manager(1).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), restart)
            .await
            .is_err()
    );
    assert_eq!(c.info().manager_generation, 1);
    assert_eq!(c.info().pending, 3);
    assert!(c.info().recovering && !c.info().manager_previous_joined);
    assert!(matches!(c.reporters(), Err(Error::Closed)));
    let added = ["ae", "epoch", "added"].map(Part::atom).to_vec();
    core.registry
        .client()
        .create(&added, registry::Kind::Gauge, true)
        .unwrap()
        .await
        .unwrap();
    drop(trigger);
    drop(release);
    assert_eq!(within(old_queue).await, Err(Error::Closed));
    within(async {
        while c.info().manager_generation != 2 || !c.info().running || c.info().pending != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let r = within(c.reporters().unwrap()).await.unwrap().remove(0);
    assert!(r.enabled && r.alive && r.previous_joined);
    assert!(
        within(c.subscriptions("active".into()).unwrap())
            .await
            .unwrap()
            .iter()
            .any(|s| matches!(&s.key.metric, Selector::Metric(n) if n == &added))
    );
    // A second mutation after recovery must still wake the original Store watch.
    let later = ["ae", "epoch", "later"].map(Part::atom).to_vec();
    core.registry
        .client()
        .create(&later, registry::Kind::Gauge, true)
        .unwrap()
        .await
        .unwrap();
    within(async {
        while c.info().subscriptions != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(c.info().pending_bytes, 0);
    assert_eq!(packet(&core.socket).await, b"ae.epoch.counter.value:11|g");
    core.finish().await;
}

#[tokio::test]
async fn callback_restarts_and_manager_recovery_use_one_retained_callback_history() {
    let mut core = Core::new(Limits::default()).await;
    core.add("active", "metrics_owner_callback").await;
    let c = core.reporter.client();
    within(c.restart("active".into(), 1).unwrap())
        .await
        .unwrap();
    assert_eq!(core.budget.info().restart_attempts, 0);
    assert_eq!(within(c.restart_manager(1).unwrap()).await, Ok(2));
    let r = within(c.reporters().unwrap()).await.unwrap().remove(0);
    assert_eq!(r.generation, 3);
    assert_eq!(r.restart_attempts, 2);
    assert_eq!(within(c.restart_manager(2).unwrap()).await, Ok(3));
    assert!(within(c.reporters().unwrap()).await.unwrap().is_empty());
    assert_eq!(
        within(c.subscriptions("active".into()).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(core.budget.info().restart_attempts, 2);
    core.finish().await;
}

#[tokio::test]
async fn logger_children_have_a_separate_budget_until_their_supervisor_is_replaced() {
    let mut core = Core::new(Limits::default()).await;
    for generation in 1..=3 {
        let old = core.logger.handle().current().unwrap();
        let ticket = core.logger.handle().restart(generation).unwrap();
        within(core.logger.poll()).await;
        ready(&mut core.logger).await;
        assert_eq!(within(ticket.wait()).await, Ok(generation + 1));
        assert!(old.client.info().joined);
        assert_eq!(core.budget.info().restart_attempts, 0);
    }
    let ticket = core.logger.handle().restart(4).unwrap();
    within(core.logger.poll()).await;
    assert_eq!(
        within(ticket.wait()).await,
        Err(statsd::supervision::Error::ChildLimit)
    );
    assert_eq!(core.logger.handle().status().phase, Phase::Empty);
    assert_eq!(core.budget.info().attempts_for(Child::LoggerSupervisor), 1);
    assert_eq!(core.generations(), vec![1, 1, 1, 1, 2]);
    let start = core.logger.handle().start(2).unwrap();
    within(core.logger.poll()).await;
    ready(&mut core.logger).await;
    within(start.wait()).await.unwrap();
    assert_eq!(core.budget.info().restart_attempts, 1);
    core.finish().await;
}
