use super::{block_json, bytes, integer, n, otp_order, snapshot, unpack};
use ae_hyperchain::{
    parent_block::ParentBlock,
    parent_cache::runtime::{
        self, Client, ConnectorLink, Error, Health, Mailbox, TargetError, Targets,
    },
    parent_cache::{self, Cache, ChildTop, Entry, Event, LookupError},
    parent_connector::{
        fetch::{Backend, BackendError, Context, Query, Remote},
        runtime as connector,
    },
};
use ae_types::{Hash256, Unsigned};
use num_bigint::BigInt;
use serde_json::{Value as J, json};
use std::{
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
static DATA: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_hc_cache_runtime.json")));
#[derive(Clone, Copy)]
enum Kind {
    Normal,
    Wide,
    Native,
}
#[derive(Clone)]
struct Source {
    kind: Kind,
    calls: usize,
    unavailable: Arc<AtomicBool>,
    gate: Option<(Arc<AtomicBool>, Arc<AtomicBool>)>,
}
impl Source {
    fn new(kind: Kind) -> Self {
        Self {
            kind,
            calls: 0,
            unavailable: Arc::new(AtomicBool::new(false)),
            gate: None,
        }
    }
}
impl Targets for Source {
    fn targets(&mut self, h: &Unsigned) -> Result<Vec<BigInt>, TargetError> {
        if self.calls == 0
            && let Some((entered, released)) = &self.gate
        {
            entered.store(true, Ordering::SeqCst);
            let until = Instant::now() + Duration::from_secs(3);
            while !released.load(Ordering::SeqCst) {
                if Instant::now() >= until {
                    return Err(TargetError::Unavailable);
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(TargetError::Unavailable);
        }
        self.calls += 1;
        Ok(match self.kind {
            Kind::Native if self.calls > 1 => vec![],
            Kind::Wide => (0..33).map(|o| integer(h) + o).collect(),
            _ => vec![integer(h), integer(h) + 2],
        })
    }
}
struct Probe {
    missing: AtomicBool,
    delay12: AtomicU64,
    calls: Mutex<Vec<(J, Instant)>>,
}
impl Probe {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            missing: AtomicBool::new(false),
            delay12: AtomicU64::new(0),
            calls: Mutex::new(vec![]),
        })
    }
    fn queries(&self) -> J {
        let mut qs: Vec<_> = self
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(q, _)| q.clone())
            .collect();
        qs.sort_by(|a, b| {
            a["kind"]
                .as_str()
                .cmp(&b["kind"].as_str())
                .then_with(|| n(&a["height"]).cmp(&n(&b["height"])))
                .then_with(|| a["host"].as_u64().cmp(&b["host"].as_u64()))
                .then_with(|| a["seed"].as_str().cmp(&b["seed"].as_str()))
        });
        json!(qs)
    }
    fn sequence(&self) -> J {
        json!(
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(q, _)| q["host"] == 0)
                .map(|(q, _)| json!({"kind":q["kind"],"height":q["height"],"seed":q["seed"]}))
                .collect::<Vec<_>>()
        )
    }
}
impl Backend for Probe {
    fn fetch(
        &self,
        host: usize,
        q: &Query,
        seed: &[u8; 16],
        ctx: &Context,
    ) -> Result<Remote, BackendError> {
        let missing = self.missing.load(Ordering::SeqCst);
        let delay12 = self.delay12.load(Ordering::SeqCst);
        let (kind, height) = match q {
            Query::Top => ("top", BigInt::from(20)),
            Query::Height(h) => ("height", h.clone()),
            Query::Hash(_) => return Ok(Remote::OtherError),
        };
        self.calls.lock().unwrap().push((
            json!({"kind":kind,"height":height.to_string(),"host":host,"seed":hex::encode(seed)}),
            Instant::now(),
        ));
        if height == BigInt::from(12) {
            let due = Instant::now() + Duration::from_millis(delay12);
            while Instant::now() < due && !ctx.cancelled() {
                thread::sleep(Duration::from_millis(1).min(ctx.remaining()));
            }
        }
        if missing || ctx.cancelled() || height < BigInt::from(1) {
            return Ok(Remote::NotFound);
        }
        let h: u64 = height.to_string().parse().unwrap();
        Ok(Remote::Block(parent(h, h)))
    }
}
fn parent(h: u64, n: u64) -> ParentBlock {
    let mut hash = vec![0; 32];
    hash[24..].copy_from_slice(&n.to_be_bytes());
    let mut previous = vec![0; 32];
    previous[24..].copy_from_slice(&(n - 1).to_be_bytes());
    ParentBlock {
        hash,
        height: h.into(),
        previous_hash: previous,
        time: (h * 100).into(),
    }
}
fn initial() -> Cache {
    Cache::new(
        parent_cache::Config {
            child_start_height: 5.into(),
            max_size: 200.into(),
            retry_interval: 1_000_000_000.into(),
            finality: 2.into(),
        },
        ChildTop {
            height: 10.into(),
            hash: Hash256::try_from(bytes(&DATA["pipeline"]["initial"]["hash"]).as_slice())
                .unwrap(),
        },
    )
}
fn snap(c: &Client) -> J {
    let mut s = snapshot(&c.snapshot().cache);
    for pair in s["blocks"].as_array_mut().unwrap() {
        let e = &pair[1];
        pair[1] = if e.get("requested").is_some() {
            json!("requested")
        } else {
            e["block"].clone()
        };
    }
    s
}
fn reply(r: Result<ParentBlock, LookupError>) -> J {
    match r {
        Ok(b) => block_json(&b),
        Err(LookupError::NotInCache) => json!("not_in_cache"),
        Err(LookupError::NotFinal) => json!("not_final"),
    }
}
fn wait(mut f: impl FnMut() -> bool) {
    wait_for(Duration::from_secs(4), &mut f);
}
fn wait_for(timeout: Duration, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + timeout;
    while !f() {
        assert!(Instant::now() < end, "cache condition did not become true");
        thread::sleep(Duration::from_millis(1));
    }
}
fn has(c: &Client, h: u64) -> bool {
    matches!(
        c.snapshot().cache.blocks().get(&BigInt::from(h)),
        Some(Entry::Block(_))
    )
}
fn ready(c: &Client) {
    wait(|| has(c, 10) && has(c, 12));
}
struct Pair {
    cache: runtime::Runtime,
    parent: connector::Runtime,
    probe: Arc<Probe>,
    client: Client,
}
impl Pair {
    fn start(
        kind: Kind,
        probe: Arc<Probe>,
        cache_config: runtime::Config,
        parent_config: connector::Config,
    ) -> Self {
        let mailbox = Mailbox::new(initial(), cache_config).unwrap();
        let client = mailbox.client();
        let parent =
            connector::Runtime::spawn(probe.clone(), 3, [0; 16], mailbox.sink(), parent_config)
                .unwrap();
        let link = ConnectorLink::new(parent.client());
        let cache = mailbox
            .spawn(
                Source::new(kind),
                link,
                otp_order::profile(&DATA["profile"]),
            )
            .unwrap();
        Self {
            cache,
            parent,
            probe,
            client,
        }
    }
    fn idle(&self) {
        for _ in 0..2 {
            wait(|| self.client.snapshot().pending_requests == 0);
            self.parent
                .client()
                .barrier(Duration::from_secs(3))
                .unwrap();
            self.client.barrier(Duration::from_secs(3)).unwrap();
        }
    }
    fn stop(self) {
        let exit = self.cache.shutdown().unwrap();
        assert!(exit.reason.is_none(), "{exit:?}");
        self.parent.shutdown().unwrap();
    }
}
fn parent_config() -> connector::Config {
    connector::Config {
        fetch_interval: None,
        ..Default::default()
    }
}
fn regular() -> Pair {
    Pair::start(
        Kind::Normal,
        Probe::new(),
        Default::default(),
        parent_config(),
    )
}

#[test]
fn actual_original_cache_connector_pipeline_matches_finality_and_events() {
    let pair = regular();
    let c = &pair.client;
    ready(c);
    assert_eq!(snap(c), DATA["pipeline"]["initial"]);
    for row in DATA["pipeline"]["steps"].as_array().unwrap() {
        let r = match row["name"].as_str().unwrap() {
            "final" => c.get_once(10.into()).unwrap(),
            "nonfinal" => c.get_once(12.into()).unwrap(),
            "parent_top" => {
                pair.parent.client().check_top().unwrap();
                wait(|| c.snapshot().cache.top_height() == &20.into());
                c.get_once(12.into()).unwrap()
            }
            "child_top" => {
                c.child_top(ChildTop {
                    height: 15.into(),
                    hash: Hash256::try_from(parent(15, 15).hash.as_slice()).unwrap(),
                })
                .unwrap();
                wait(|| has(c, 15) && has(c, 17));
                c.get_once(17.into()).unwrap()
            }
            "replacement" => {
                c.post_block(parent(15, 99)).unwrap();
                c.barrier(Duration::from_secs(1)).unwrap();
                c.get_once(15.into()).unwrap()
            }
            "micro_ignored" => {
                c.cast(Event::Ignore).unwrap();
                c.barrier(Duration::from_secs(1)).unwrap();
                c.get_once(15.into()).unwrap()
            }
            "missing" => {
                let r = c.get_once(9.into()).unwrap();
                wait(|| has(c, 9));
                r
            }
            "fetched" => c.get_once(9.into()).unwrap(),
            _ => unreachable!(),
        };
        assert_eq!(reply(r), row["reply"], "{}", row["name"]);
        assert_eq!(snap(c), row["state"], "{}", row["name"]);
    }
    pair.idle();
    assert_eq!(pair.probe.queries(), DATA["pipeline"]["queries"]);
    assert_eq!(pair.probe.sequence(), DATA["pipeline"]["sequence"]);
    pair.stop();
}

#[test]
fn original_lookup_polling_counts_sleep_budget_and_nonfinal_reads_match() {
    let pair = regular();
    let c = &pair.client;
    ready(c);
    pair.probe.missing.store(true, Ordering::SeqCst);
    for row in DATA["polling"]["rows"].as_array().unwrap() {
        let before = c.snapshot().lookup_calls;
        let start = Instant::now();
        let r = c.get_block(99.into(), n(&row["timeout"])).unwrap();
        assert_eq!(reply(r), row["reply"]);
        let calls = c.snapshot().lookup_calls - before;
        assert_eq!(calls, row["calls"].as_u64().unwrap());
        assert!(start.elapsed() >= Duration::from_millis((calls - 1) * 10));
    }
    let before = c.snapshot().lookup_calls;
    assert_eq!(
        reply(c.get_block(12.into(), 21.into()).unwrap()),
        DATA["polling"]["nonfinal"]["reply"]
    );
    assert_eq!(
        c.snapshot().lookup_calls - before,
        DATA["polling"]["nonfinal"]["calls"].as_u64().unwrap()
    );
    pair.idle();
    assert_eq!(snap(c), DATA["polling"]["state"]);
    assert_eq!(pair.probe.queries(), DATA["polling"]["queries"]);
    pair.stop();
}

#[test]
fn original_waiting_lookup_completes_when_parent_finality_advances() {
    let pair = regular();
    let c = &pair.client;
    ready(c);
    let before = c.snapshot().lookup_calls;
    let caller = c.clone();
    let task = thread::spawn(move || caller.get_block(12.into(), 1000.into()).unwrap());
    wait(|| c.snapshot().lookup_calls > before);
    pair.parent.client().check_top().unwrap();
    assert_eq!(reply(task.join().unwrap()), DATA["unblocks"]["reply"]);
    assert!((2..=101).contains(&(c.snapshot().lookup_calls - before)));
    pair.idle();
    assert_eq!(snap(c), DATA["unblocks"]["state"]);
    assert_eq!(pair.probe.queries(), DATA["unblocks"]["queries"]);
    pair.stop();
}

#[test]
fn original_initialization_retries_missing_and_absent_connectors() {
    let probe = Probe::new();
    probe.missing.store(true, Ordering::SeqCst);
    let pair = Pair::start(
        Kind::Normal,
        probe.clone(),
        Default::default(),
        parent_config(),
    );
    let c = &pair.client;
    wait(|| probe.calls.lock().unwrap().len() >= 3);
    c.barrier(Duration::from_secs(1)).unwrap();
    probe.missing.store(false, Ordering::SeqCst);
    ready(c);
    pair.idle();
    assert_eq!(snap(c), DATA["retry"]["state"]);
    assert_eq!(probe.queries(), DATA["retry"]["queries"]);
    assert_eq!(probe.sequence(), DATA["retry"]["sequence"]);
    let starts: Vec<_> = probe
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(q, _)| q["host"] == 0 && q["height"] == "12")
        .map(|(_, t)| *t)
        .collect();
    assert_eq!(starts.len(), 2);
    assert!(starts[1].duration_since(starts[0]) >= Duration::from_millis(990));
    pair.stop();
    let mailbox = Mailbox::new(initial(), Default::default()).unwrap();
    let c = mailbox.client();
    let sink = mailbox.sink();
    let link = ConnectorLink::default();
    let start = Instant::now();
    let cache = mailbox
        .spawn(
            Source::new(Kind::Normal),
            link.clone(),
            otp_order::profile(&DATA["profile"]),
        )
        .unwrap();
    c.barrier(Duration::from_secs(1)).unwrap();
    let probe = Probe::new();
    let parent =
        connector::Runtime::spawn(probe.clone(), 3, [0; 16], sink, parent_config()).unwrap();
    link.replace(Some(parent.client()));
    ready(&c);
    let pair = Pair {
        cache,
        parent,
        probe,
        client: c,
    };
    pair.idle();
    assert!(start.elapsed() >= Duration::from_millis(990));
    assert_eq!(snap(&pair.client), DATA["absent"]["state"]);
    assert_eq!(pair.probe.queries(), DATA["absent"]["queries"]);
    pair.stop();
}

#[test]
fn original_native_cache_failure_preserves_outbound_prefix_and_connector_liveness() {
    let pair = Pair::start(
        Kind::Native,
        Probe::new(),
        Default::default(),
        parent_config(),
    );
    wait(|| matches!(pair.client.snapshot().health, Health::Failed(_)));
    wait(|| pair.probe.calls.lock().unwrap().len() == 6);
    pair.parent
        .client()
        .barrier(Duration::from_secs(1))
        .unwrap();
    assert_eq!(pair.client.get_once(10.into()), Err(Error::Closed));
    assert_eq!(pair.probe.queries(), DATA["native"]["queries"]);
    assert_eq!(pair.probe.sequence(), DATA["native"]["sequence"]);
    assert_eq!(
        pair.parent.client().snapshot().health,
        connector::Health::Running
    );
    let exit = pair.cache.shutdown().unwrap();
    assert_eq!(
        exit.reason,
        Some(Error::Core(parent_cache::Error::Native(
            parent_cache::Native::EmptyMaximumTargets
        )))
    );
    pair.parent.shutdown().unwrap();
}

#[test]
fn original_late_lookup_remains_queued_after_caller_timeout() {
    let probe = Probe::new();
    probe.delay12.store(200, Ordering::SeqCst);
    let pair = Pair::start(Kind::Normal, probe, Default::default(), parent_config());
    wait(|| pair.probe.calls.lock().unwrap().len() >= 3);
    assert_eq!(
        pair.client
            .get_once_timeout(10.into(), Duration::from_millis(20)),
        Err(Error::Timeout)
    );
    ready(&pair.client);
    pair.idle();
    assert_eq!(
        pair.client.snapshot().lookup_calls,
        DATA["late_lookup"]["calls"].as_u64().unwrap()
    );
    assert_eq!(snap(&pair.client), DATA["late_lookup"]["state"]);
    assert_eq!(pair.probe.queries(), DATA["late_lookup"]["queries"]);
    pair.stop();
}

#[test]
fn wide_prefetch_matches_original_through_single_slot_bidirectional_mailboxes() {
    let pair = Pair::start(
        Kind::Wide,
        Probe::new(),
        runtime::Config {
            command_capacity: 1,
            ..Default::default()
        },
        connector::Config {
            command_capacity: 1,
            ..parent_config()
        },
    );
    wait(|| (10..=42).all(|h| has(&pair.client, h)));
    pair.idle();
    assert_eq!(snap(&pair.client), DATA["wide"]["state"]);
    assert_eq!(pair.probe.queries(), DATA["wide"]["queries"]);
    assert_eq!(pair.probe.sequence(), DATA["wide"]["sequence"]);
    assert_eq!(pair.client.snapshot().failed_events, 0);
    pair.stop();
}

#[test]
fn operational_failures_are_retained_without_partial_cache_or_outbound_publication() {
    let limits = runtime::Limits {
        maximum_outbox_requests: 1,
        ..Default::default()
    };
    let pair = Pair::start(
        Kind::Wide,
        Probe::new(),
        runtime::Config {
            limits,
            ..Default::default()
        },
        parent_config(),
    );
    wait(|| pair.client.snapshot().failed_events == 1);
    assert!(pair.client.snapshot().cache.blocks().is_empty());
    assert_eq!(pair.probe.calls.lock().unwrap().len(), 3);
    let failed = pair.client.take_failure().unwrap();
    assert_eq!(failed.error, Error::OutboxFull);
    pair.client.configure(runtime::Limits::default());
    pair.client
        .call(failed.event)
        .unwrap()
        .wait(Duration::from_secs(1))
        .unwrap();
    wait(|| (10..=42).all(|h| has(&pair.client, h)));
    pair.idle();
    assert_eq!(snap(&pair.client), DATA["wide"]["state"]);
    // Fully cached reads and posts that emit no requests remain usable with a
    // disabled outbox. A failed miss does not leave a requested marker behind.
    pair.client.configure(runtime::Limits {
        maximum_outbox_requests: 0,
        ..Default::default()
    });
    assert!(pair.client.get_once(10.into()).unwrap().is_ok());
    assert_eq!(pair.client.get_once(99.into()), Err(Error::OutboxFull));
    assert!(
        !pair
            .client
            .snapshot()
            .cache
            .blocks()
            .contains_key(&99.into())
    );
    pair.client.post_block(parent(10, 99)).unwrap();
    pair.client.barrier(Duration::from_secs(1)).unwrap();
    assert_eq!(
        pair.client.get_once(10.into()).unwrap().unwrap().hash,
        parent(10, 99).hash
    );
    pair.stop();
}

#[test]
fn original_internal_fetch_timeout_schedules_retry_and_discards_late_reply() {
    let probe = Probe::new();
    probe.delay12.store(6000, Ordering::SeqCst);
    let pair = Pair::start(Kind::Normal, probe, Default::default(), parent_config());
    let start = Instant::now();
    wait(|| pair.probe.calls.lock().unwrap().len() >= 3);
    pair.probe.delay12.store(0, Ordering::SeqCst);
    pair.client.barrier(Duration::from_secs(7)).unwrap();
    assert!(start.elapsed() >= Duration::from_millis(4900));
    assert!(pair.client.snapshot().cache.blocks().is_empty());
    ready(&pair.client);
    pair.idle();
    assert_eq!(snap(&pair.client), DATA["internal_timeout"]["state"]);
    assert_eq!(pair.probe.queries(), DATA["internal_timeout"]["queries"]);
    assert_eq!(pair.probe.sequence(), DATA["internal_timeout"]["sequence"]);
    pair.stop();
}

#[test]
fn internal_call_timeout_breaks_a_full_mailbox_cycle_and_prefetch_recovers() {
    let entered = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let mut source = Source::new(Kind::Normal);
    source.gate = Some((entered.clone(), released.clone()));
    let mailbox = Mailbox::new(
        initial(),
        runtime::Config {
            command_capacity: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let c = mailbox.client();
    let probe = Probe::new();
    let parent = connector::Runtime::spawn(
        probe.clone(),
        3,
        [0; 16],
        mailbox.sink(),
        connector::Config {
            command_capacity: 1,
            ..parent_config()
        },
    )
    .unwrap();
    let cache = mailbox
        .spawn(
            source,
            ConnectorLink::new(parent.client()),
            otp_order::profile(&DATA["profile"]),
        )
        .unwrap();
    wait(|| entered.load(Ordering::SeqCst));
    c.post_block(self::parent(8, 8)).unwrap();
    parent.client().request(Query::Height(7.into())).unwrap();
    wait(|| parent.client().snapshot().health == connector::Health::Backpressure);
    let start = Instant::now();
    released.store(true, Ordering::SeqCst);
    wait_for(Duration::from_secs(8), || has(&c, 10) && has(&c, 12));
    assert!(start.elapsed() >= Duration::from_millis(5900));
    assert!(has(&c, 7) && has(&c, 8));
    assert_eq!(c.snapshot().failed_events, 0);
    assert_eq!(
        c.get_once(10.into()).unwrap().unwrap(),
        self::parent(10, 10)
    );
    let pair = Pair {
        cache,
        parent,
        probe,
        client: c,
    };
    pair.idle();
    assert_eq!(
        pair.probe.sequence(),
        json!([
            {"kind":"height","height":"7","seed":hex::encode([0;16])},
            {"kind":"height","height":"12","seed":hex::encode([0;16])},
            {"kind":"height","height":"12","seed":hex::encode([0;16])},
            {"kind":"height","height":"10","seed":hex::encode([0;16])}
        ])
    );
    pair.stop();
}

#[test]
fn failure_retention_admission_and_shutdown_stay_bounded() {
    let source = Source::new(Kind::Normal);
    source.unavailable.store(true, Ordering::SeqCst);
    let mailbox = Mailbox::new(
        initial(),
        runtime::Config {
            limits: runtime::Limits {
                maximum_failures: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let c = mailbox.client();
    let rt = mailbox
        .spawn(
            source,
            ConnectorLink::default(),
            otp_order::profile(&DATA["profile"]),
        )
        .unwrap();
    wait(|| matches!(c.snapshot().health, Health::Failed(Error::FailureBacklog)));
    let exit = rt.shutdown().unwrap();
    assert_eq!(
        exit.unreported.unwrap().error,
        Error::Targets(TargetError::Unavailable)
    );
    let probe = Probe::new();
    probe.delay12.store(60000, Ordering::SeqCst);
    let pair = Pair::start(
        Kind::Normal,
        probe,
        runtime::Config {
            command_capacity: 1,
            limits: runtime::Limits {
                maximum_command_bytes: 128,
                ..Default::default()
            },
        },
        parent_config(),
    );
    wait(|| pair.probe.calls.lock().unwrap().len() >= 3);
    let pending = pair.client.call(Event::Get(1.into())).unwrap();
    assert!(matches!(
        pair.client.call(Event::Get(2.into())),
        Err(Error::Full)
    ));
    assert!(matches!(
        pair.client.call(Event::Get(BigInt::from(1) << 2048)),
        Err(Error::ResourceLimit)
    ));
    let start = Instant::now();
    let exit = pair.cache.shutdown().unwrap();
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(exit.reason.is_none());
    assert!(matches!(
        pending.wait(Duration::from_secs(1)),
        Err(Error::Closed)
    ));
    pair.parent.shutdown().unwrap();
}
