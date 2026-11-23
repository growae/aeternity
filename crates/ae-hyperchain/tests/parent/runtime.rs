use super::{block, block_json, bytes, n, unpack};
use ae_hyperchain::{
    parent_block::ParentBlock,
    parent_connector::{
        self, Consensus,
        fetch::{self, Backend, BackendError, Context, Query, Remote},
        runtime::{self, Config, Error, Health, Runtime, Sink, SinkError},
    },
};
use serde_json::{Value as J, json};
use std::{
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
static DATA: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_hc_parent_runtime.json")));
#[derive(Clone)]
enum Outcome {
    Remote(Remote),
    Crash,
    LocalError,
}
#[derive(Clone)]
struct Plan {
    delay: Duration,
    outcome: Outcome,
}
struct Script {
    plans: Mutex<Vec<Plan>>,
    calls: Mutex<Vec<(J, Instant)>>,
    active: AtomicUsize,
    cancelled: AtomicUsize,
    only_height_ten: AtomicBool,
}
impl Script {
    fn new(plans: Vec<Plan>) -> Arc<Self> {
        Arc::new(Self {
            plans: Mutex::new(plans),
            calls: Mutex::new(vec![]),
            active: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
            only_height_ten: AtomicBool::new(false),
        })
    }
    fn set(&self, plans: Vec<Plan>) {
        *self.plans.lock().unwrap() = plans;
    }
    fn queries(&self) -> J {
        let mut qs: Vec<_> = self
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(q, _)| q.clone())
            .collect();
        qs.sort_by_key(|q| q["host"].as_u64().unwrap());
        json!(qs)
    }
}
impl Backend for Script {
    fn fetch(
        &self,
        host: usize,
        query: &Query,
        seed: &[u8; 16],
        context: &Context,
    ) -> Result<Remote, BackendError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        struct Active<'a>(&'a AtomicUsize);
        impl Drop for Active<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let _active = Active(&self.active);
        let plan = self.plans.lock().unwrap()[host].clone();
        self.calls.lock().unwrap().push((
            json!({"query":query_json(query),"host":host,"seed":hex::encode(seed)}),
            Instant::now(),
        ));
        let delay =
            if self.only_height_ten.load(Ordering::SeqCst) && *query != Query::Height(10.into()) {
                Duration::ZERO
            } else {
                plan.delay
            };
        let due = Instant::now() + delay;
        while Instant::now() < due && !context.cancelled() {
            thread::sleep(
                due.saturating_duration_since(Instant::now())
                    .min(context.remaining())
                    .min(Duration::from_millis(2)),
            );
        }
        if context.cancelled() {
            self.cancelled.fetch_add(1, Ordering::SeqCst);
            return Ok(Remote::OtherError);
        }
        match plan.outcome {
            Outcome::Remote(r) => Ok(r),
            Outcome::Crash => panic!("scripted parent worker failure"),
            Outcome::LocalError => Err(BackendError::Unavailable),
        }
    }
}
#[derive(Clone)]
struct Collector {
    posts: Arc<Mutex<Vec<ParentBlock>>>,
    full: Arc<AtomicBool>,
    fault: Arc<AtomicBool>,
}
impl Collector {
    fn new() -> Self {
        Self {
            posts: Arc::new(Mutex::new(vec![])),
            full: Arc::new(AtomicBool::new(false)),
            fault: Arc::new(AtomicBool::new(false)),
        }
    }
    fn json(&self) -> J {
        json!(
            self.posts
                .lock()
                .unwrap()
                .iter()
                .map(block_json)
                .collect::<Vec<_>>()
        )
    }
}
impl Sink for Collector {
    fn post(&mut self, b: &ParentBlock) -> Result<(), SinkError> {
        if self.full.load(Ordering::SeqCst) {
            return Err(SinkError::Full);
        }
        if self.fault.load(Ordering::SeqCst) {
            return Err(SinkError::ResourceLimit);
        }
        self.posts.lock().unwrap().push(b.clone());
        Ok(())
    }
}
fn query(v: &J) -> Query {
    match v["kind"].as_str().unwrap() {
        "top" => Query::Top,
        "height" => Query::Height(n(&v["height"])),
        "hash" => Query::Hash(bytes(&v["hash"])),
        _ => unreachable!(),
    }
}
fn query_json(q: &Query) -> J {
    match q {
        Query::Top => json!({"kind":"top"}),
        Query::Height(h) => json!({"kind":"height","height":h.to_string()}),
        Query::Hash(h) => json!({"kind":"hash","hash":hex::encode(h)}),
    }
}
fn plan(v: &J) -> Plan {
    Plan {
        delay: Duration::from_millis(v["delay"].as_u64().unwrap()),
        outcome: match v["result"].as_str() {
            Some("crash") => Outcome::Crash,
            Some("not_found") => Outcome::Remote(Remote::NotFound),
            Some("other") => Outcome::Remote(Remote::OtherError),
            None => Outcome::Remote(Remote::Block(block(&v["result"]))),
            _ => unreachable!(),
        },
    }
}
fn plans(b: &ParentBlock, delay: u64) -> Vec<Plan> {
    vec![
        Plan {
            delay: Duration::from_millis(delay),
            outcome: Outcome::Remote(Remote::Block(b.clone()))
        };
        3
    ]
}
fn result(r: Consensus<usize>) -> J {
    match r {
        Consensus::Block { block, .. } => block_json(&block),
        Consensus::NotFound => json!("not_found"),
        Consensus::NoAgreement => json!("no_parent_chain_agreement"),
    }
}
fn snapshot(c: &runtime::Client) -> J {
    let s = c.snapshot();
    json!({"seed":hex::encode(s.seed),"top":s.top.as_ref().map(block_json)})
}
fn wait(mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(3);
    while !f() {
        assert!(Instant::now() < end, "condition did not become true");
        thread::sleep(Duration::from_millis(1));
    }
}
fn config() -> Config {
    Config {
        fetch_interval: None,
        ..Config::default()
    }
}
fn barrier(c: &runtime::Client) {
    c.barrier(Duration::from_secs(3)).unwrap();
}
fn sample_block() -> ParentBlock {
    block(&DATA["timer"]["posts"][0])
}

#[test]
fn original_connector_process_traces_match_queries_posts_seed_and_state() {
    let mut steps = 0;
    for row in DATA["runs"].as_array().unwrap() {
        let backend = Script::new(
            row["steps"][0]["responses"]
                .as_array()
                .unwrap()
                .iter()
                .map(plan)
                .collect(),
        );
        let sink = Collector::new();
        let runtime = Runtime::spawn(
            backend.clone(),
            3,
            bytes(&row["seed"]).try_into().unwrap(),
            sink.clone(),
            config(),
        )
        .unwrap();
        let client = runtime.client();
        for step in row["steps"].as_array().unwrap() {
            backend.set(
                step["responses"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(plan)
                    .collect(),
            );
            backend.calls.lock().unwrap().clear();
            sink.posts.lock().unwrap().clear();
            let input = &step["input"];
            let reply = match input["kind"].as_str().unwrap() {
                "check" => {
                    client.check_top().unwrap();
                    json!("ok")
                }
                "fetch" => result(client.fetch(query(&input["query"])).unwrap()),
                "request" => {
                    client.request(query(&input["query"])).unwrap();
                    json!("ok")
                }
                _ => unreachable!(),
            };
            barrier(&client);
            assert_eq!(reply, step["reply"], "{} {steps}", row["name"]);
            assert_eq!(snapshot(&client), step["state"]);
            assert_eq!(backend.queries(), step["queries"]);
            assert_eq!(sink.json(), step["posts"]);
            steps += 1;
        }
        runtime.shutdown().unwrap();
        assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    }
    assert_eq!(steps, 16);
}

#[test]
fn original_poll_delay_and_manual_check_timer_independence_match() {
    let b = sample_block();
    let backend = Script::new(plans(&b, 30));
    let sink = Collector::new();
    let rt = Runtime::spawn(
        backend.clone(),
        3,
        [0; 16],
        sink.clone(),
        Config {
            fetch_interval: Some(Duration::from_millis(50)),
            ..config()
        },
    )
    .unwrap();
    let c = rt.client();
    wait(|| u128::from_be_bytes(c.snapshot().seed) >= 3);
    assert_eq!(snapshot(&c), DATA["timer"]["state"]);
    rt.shutdown().unwrap();
    assert_eq!(sink.json(), DATA["timer"]["posts"]);
    let starts: Vec<_> = backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(q, _)| q["host"] == 0)
        .map(|(_, t)| *t)
        .collect();
    assert_eq!(starts.len(), 3);
    assert!(
        starts
            .windows(2)
            .all(|w| w[1].duration_since(w[0]) >= Duration::from_millis(75))
    );
    let backend = Script::new(plans(&b, 0));
    let sink = Collector::new();
    let rt = Runtime::spawn(
        backend.clone(),
        3,
        [0; 16],
        sink.clone(),
        Config {
            fetch_interval: Some(Duration::from_millis(400)),
            ..config()
        },
    )
    .unwrap();
    let c = rt.client();
    thread::sleep(Duration::from_millis(200));
    c.check_top().unwrap();
    wait(|| u128::from_be_bytes(c.snapshot().seed) >= 2);
    assert_eq!(snapshot(&c), DATA["manual_timer"]["state"]);
    rt.shutdown().unwrap();
    assert_eq!(sink.json(), DATA["manual_timer"]["posts"]);
    let starts: Vec<_> = backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(q, _)| q["host"] == 0)
        .map(|(_, t)| *t)
        .collect();
    assert_eq!(starts.len(), 2);
    let gap = starts[1].duration_since(starts[0]);
    assert!((Duration::from_millis(100)..Duration::from_millis(300)).contains(&gap));
}

#[test]
fn due_poll_precedes_commands_enqueued_while_an_earlier_fetch_is_blocked() {
    let backend = Script::new(plans(&sample_block(), 200));
    backend.only_height_ten.store(true, Ordering::SeqCst);
    let sink = Collector::new();
    let rt = Runtime::spawn(
        backend.clone(),
        3,
        [0; 16],
        sink.clone(),
        Config {
            fetch_interval: Some(Duration::from_millis(100)),
            ..config()
        },
    )
    .unwrap();
    let c = rt.client();
    let first = c.clone();
    let caller = thread::spawn(move || first.fetch(Query::Height(10.into())).unwrap());
    wait(|| backend.calls.lock().unwrap().len() == 3);
    thread::sleep(Duration::from_millis(120));
    c.request(Query::Height(20.into())).unwrap();
    barrier(&c);
    assert!(matches!(caller.join().unwrap(), Consensus::Block { .. }));
    assert_eq!(snapshot(&c), DATA["mailbox_timer"]["state"]);
    rt.shutdown().unwrap();
    assert_eq!(sink.json(), DATA["mailbox_timer"]["posts"]);
    let sequence: Vec<_> = backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(q, _)| q["host"] == 0)
        .map(|(q, _)| json!({"query":q["query"],"seed":q["seed"]}))
        .collect();
    assert_eq!(json!(sequence), DATA["mailbox_timer"]["sequence"]);
}

#[test]
fn original_default_call_timeout_discards_late_reply_without_cancelling_fetch() {
    let backend = Script::new(plans(&sample_block(), 6000));
    let sink = Collector::new();
    let rt = Runtime::spawn(backend.clone(), 3, [0; 16], sink.clone(), config()).unwrap();
    let c = rt.client();
    let started = Instant::now();
    assert_eq!(c.fetch(Query::Height(10.into())), Err(Error::Timeout));
    assert!(started.elapsed() >= Duration::from_millis(4900));
    barrier(&c);
    assert_eq!(snapshot(&c), DATA["late_call"]["state"]);
    assert_eq!(sink.json(), DATA["late_call"]["posts"]);
    assert_eq!(backend.queries(), DATA["late_call"]["queries"]);
    assert_eq!(backend.cancelled.load(Ordering::SeqCst), 0);
    rt.shutdown().unwrap();
}

#[test]
fn worker_timeouts_preserve_denominator_and_late_jobs_cannot_pollute_next_fetch() {
    assert_eq!(Config::default().fetch_timeout, Duration::from_secs(10));
    let b = sample_block();
    let backend = Script::new(vec![
        plans(&b, 200)[0].clone(),
        Plan {
            delay: Duration::ZERO,
            outcome: Outcome::Remote(Remote::NotFound),
        },
        Plan {
            delay: Duration::ZERO,
            outcome: Outcome::Remote(Remote::NotFound),
        },
    ]);
    let sink = Collector::new();
    let rt = Runtime::spawn(
        backend.clone(),
        3,
        [0; 16],
        sink.clone(),
        Config {
            fetch_timeout: Duration::from_millis(50),
            ..config()
        },
    )
    .unwrap();
    let c = rt.client();
    c.check_top().unwrap();
    barrier(&c);
    assert_eq!(snapshot(&c), DATA["worker_timeout"]["state"]);
    assert_eq!(sink.json(), DATA["worker_timeout"]["posts"]);
    assert_eq!(backend.queries(), DATA["worker_timeout"]["queries"]);
    backend.set(plans(&b, 0));
    c.check_top().unwrap();
    barrier(&c);
    assert_eq!(c.snapshot().seed, 2u128.to_be_bytes());
    assert_eq!(sink.json(), json!([block_json(&b)]));
    rt.shutdown().unwrap();
    assert!(backend.cancelled.load(Ordering::SeqCst) >= 1);
}

#[test]
fn bounded_pool_keeps_reverse_host_order_and_reports_operational_exhaustion() {
    let b = sample_block();
    let mut ps = plans(&b, 0);
    ps[0].delay = Duration::from_millis(30);
    ps[2].delay = Duration::from_millis(10);
    let backend = Script::new(ps);
    let stop = Arc::new(AtomicBool::new(false));
    assert!(fetch::Pool::spawn(backend.clone(), 3, 2, stop.clone()).is_err());
    let mut pool = fetch::Pool::spawn(backend.clone(), 3, 3, stop).unwrap();
    assert_eq!(
        pool.fetch(
            Query::Top,
            [0; 16],
            Duration::from_secs(1),
            10,
            parent_connector::Limits::default()
        )
        .unwrap(),
        Consensus::Block {
            block: b.clone(),
            node: 2
        }
    );
    backend.calls.lock().unwrap().clear();
    assert_eq!(
        pool.fetch(
            Query::Hash(vec![0; 11]),
            [0; 16],
            Duration::from_secs(1),
            10,
            parent_connector::Limits::default()
        ),
        Err(fetch::Error::ResourceLimit)
    );
    assert!(backend.calls.lock().unwrap().is_empty());
    assert_eq!(
        pool.fetch(
            Query::Top,
            [0; 16],
            Duration::from_secs(1),
            10,
            parent_connector::Limits {
                maximum_bytes: 1,
                ..Default::default()
            }
        ),
        Err(fetch::Error::ResourceLimit)
    );
    let mut ps = plans(&b, 0);
    ps[0].outcome = Outcome::LocalError;
    backend.set(ps);
    assert_eq!(
        pool.fetch(
            Query::Top,
            [0; 16],
            Duration::from_secs(1),
            10,
            parent_connector::Limits::default()
        ),
        Err(fetch::Error::Backend(BackendError::Unavailable))
    );
    backend.set(plans(&b, 0));
    assert!(matches!(
        pool.fetch(
            Query::Top,
            [0; 16],
            Duration::from_secs(1),
            10,
            parent_connector::Limits::default()
        ),
        Ok(Consensus::Block { node: 2, .. })
    ));
}

#[test]
fn sink_backpressure_bounds_commands_and_shutdown_releases_pending_post() {
    let b = sample_block();
    let backend = Script::new(plans(&b, 0));
    let sink = Collector::new();
    sink.full.store(true, Ordering::SeqCst);
    let rt = Runtime::spawn(
        backend.clone(),
        3,
        [0; 16],
        sink.clone(),
        Config {
            command_capacity: 1,
            maximum_query_bytes: 2,
            ..config()
        },
    )
    .unwrap();
    let c = rt.client();
    c.check_top().unwrap();
    wait(|| c.snapshot().health == Health::Backpressure);
    assert_eq!(c.snapshot().seed, [0; 16]);
    assert!(c.snapshot().top.is_none());
    c.request(Query::Height(1.into())).unwrap();
    assert_eq!(c.request(Query::Height(2.into())), Err(Error::Full));
    assert_eq!(
        c.request(Query::Hash(vec![0; 3])),
        Err(Error::ResourceLimit)
    );
    sink.full.store(false, Ordering::SeqCst);
    wait(|| sink.posts.lock().unwrap().len() == 2);
    assert_eq!(c.snapshot().seed, 1u128.to_be_bytes());
    assert_eq!(backend.calls.lock().unwrap().len(), 6);
    sink.full.store(true, Ordering::SeqCst);
    c.request(Query::Top).unwrap();
    wait(|| c.snapshot().health == Health::Backpressure);
    let before = Instant::now();
    rt.shutdown().unwrap();
    assert!(before.elapsed() < Duration::from_secs(1));
    assert_eq!(c.snapshot().health, Health::Stopped);
    assert_eq!(c.check_top(), Err(Error::Closed));
    assert_eq!(sink.posts.lock().unwrap().len(), 2);
}

#[test]
fn operational_retry_retains_work_and_completed_reads_are_not_repeated_for_sink_failure() {
    let b = sample_block();
    let mut ps = plans(&b, 0);
    ps[0].outcome = Outcome::LocalError;
    let backend = Script::new(ps);
    let sink = Collector::new();
    let rt = Runtime::spawn(backend.clone(), 3, [0; 16], sink.clone(), config()).unwrap();
    let c = rt.client();
    c.retry(); // A retry while healthy must not arm a future unrelated fault.
    c.check_top().unwrap();
    wait(|| {
        c.snapshot().health
            == Health::WorkStopped(Error::Fetch(fetch::Error::Backend(
                BackendError::Unavailable,
            )))
    });
    assert_eq!(c.snapshot().seed, [0; 16]);
    assert!(sink.posts.lock().unwrap().is_empty());
    assert_eq!(backend.calls.lock().unwrap().len(), 3);
    backend.set(plans(&b, 0));
    sink.fault.store(true, Ordering::SeqCst);
    c.retry();
    wait(|| c.snapshot().health == Health::WorkStopped(Error::Sink(SinkError::ResourceLimit)));
    assert_eq!(c.snapshot().seed, [0; 16]);
    assert_eq!(backend.calls.lock().unwrap().len(), 6);
    sink.fault.store(false, Ordering::SeqCst);
    c.retry();
    barrier(&c);
    assert_eq!(c.snapshot().seed, 1u128.to_be_bytes());
    assert_eq!(backend.calls.lock().unwrap().len(), 6);
    assert_eq!(sink.posts.lock().unwrap().len(), 1);
    rt.shutdown().unwrap();
}

#[test]
fn shutdown_cancels_and_joins_every_fetch_worker() {
    let backend = Script::new(plans(&sample_block(), 60000));
    let rt = Runtime::spawn(backend.clone(), 3, [0; 16], Collector::new(), config()).unwrap();
    let c = rt.client();
    c.check_top().unwrap();
    wait(|| backend.active.load(Ordering::SeqCst) == 3);
    let before = Instant::now();
    rt.shutdown().unwrap();
    assert!(before.elapsed() < Duration::from_secs(1));
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(backend.cancelled.load(Ordering::SeqCst), 3);
    assert_eq!(c.snapshot().seed, [0; 16]);
    assert_eq!(c.snapshot().health, Health::Stopped);
}

#[test]
fn retirement_preserves_operational_failure_after_stop_and_does_not_allow_restart() {
    let backend = Script::new(plans(&sample_block(), 0));
    let sink = Collector::new();
    sink.fault.store(true, Ordering::SeqCst);
    let runtime = Runtime::spawn(backend.clone(), 3, [7; 16], sink, config()).unwrap();
    let client = runtime.client();
    client.check_top().unwrap();
    let failure = Health::WorkStopped(Error::Sink(SinkError::ResourceLimit));
    wait(|| client.snapshot().health == failure);
    assert!(!runtime.replacement_pending());
    client.request_stop();
    wait(|| runtime.is_finished());
    client.retry();
    assert!(!runtime.replacement_pending());
    let retired = runtime.retire();
    assert!(!retired.panicked);
    assert_eq!(retired.snapshot.health, failure);
    assert_eq!(retired.snapshot.seed, [7; 16]);
    assert_eq!(backend.calls.lock().unwrap().len(), 3);
    assert_eq!(client.check_top(), Err(Error::Closed));
}

#[test]
fn panicked_connector_is_joined_and_its_old_client_stays_closed() {
    struct PanickingSink;
    impl Sink for PanickingSink {
        fn post(&mut self, _: &ParentBlock) -> Result<(), SinkError> {
            panic!("controlled connector worker panic")
        }
    }
    let backend = Script::new(plans(&sample_block(), 0));
    let runtime = Runtime::spawn(backend.clone(), 3, [5; 16], PanickingSink, config()).unwrap();
    let client = runtime.client();
    client.check_top().unwrap();
    wait(|| runtime.is_finished());
    assert!(runtime.replacement_pending());
    let retired = runtime.retire();
    assert!(retired.panicked);
    assert_eq!(retired.snapshot.health, Health::Stopped);
    assert_eq!(retired.snapshot.seed, [5; 16]);
    assert!(retired.snapshot.top.is_none());
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(client.check_top(), Err(Error::Closed));
}
