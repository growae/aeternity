use super::*;
use ae_hyperchain::{
    parent_cache::{self, Cache, ChildTop, Event as CacheEvent, runtime as cache},
    parent_connector::{
        fetch::{Backend, BackendError, Context, Query, Remote},
        runtime as connector,
    },
};
use num_bigint::BigInt;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Instant,
};
static ENTROPY: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_hc_entropy.json")));
#[derive(Clone)]
struct Targets(Arc<AtomicBool>);
impl cache::Targets for Targets {
    fn targets(&mut self, _: &Unsigned) -> Result<Vec<BigInt>, cache::TargetError> {
        let until = Instant::now() + Duration::from_secs(10);
        while !self.0.load(Ordering::Acquire) {
            if Instant::now() >= until {
                return Err(cache::TargetError::Unavailable);
            }
            thread::sleep(Duration::from_millis(1));
        }
        Ok(vec![11.into(), 41.into()])
    }
}
struct Probe(AtomicBool);
impl Backend for Probe {
    fn fetch(
        &self,
        _: usize,
        q: &Query,
        _: &[u8; 16],
        _: &Context,
    ) -> Result<Remote, BackendError> {
        if self.0.load(Ordering::Acquire) {
            return Ok(Remote::NotFound);
        }
        let h = match q {
            Query::Height(h) => h.to_string().parse::<u64>().unwrap(),
            _ => 43,
        };
        Ok(Remote::Block(parent(h, h)))
    }
}
fn parent(h: u64, n: u64) -> ParentBlock {
    ParentBlock {
        hash: key(n).to_vec(),
        height: h.into(),
        previous_hash: Some(key(n - 1).to_vec()),
        time: (h * 100).into(),
    }
}
struct Pair {
    cache: Option<cache::Runtime>,
    parent: Option<connector::Runtime>,
    client: cache::Client,
    probe: Arc<Probe>,
    released: Arc<AtomicBool>,
}
impl Pair {
    fn start(ready: bool, capacity: usize) -> Self {
        let released = Arc::new(AtomicBool::new(ready));
        let mailbox = cache::Mailbox::new(
            Cache::new(
                parent_cache::Config {
                    child_start_height: 5.into(),
                    max_size: 200.into(),
                    retry_interval: 1_000_000_000.into(),
                    finality: 2.into(),
                },
                ChildTop {
                    height: 10.into(),
                    hash: ae_types::Hash256([0; 32]),
                },
            ),
            cache::Config {
                command_capacity: capacity,
                ..Default::default()
            },
        )
        .unwrap();
        let probe = Arc::new(Probe(AtomicBool::new(false)));
        let p = connector::Runtime::spawn(
            probe.clone(),
            3,
            [0; 16],
            mailbox.sink(),
            connector::Config::default(),
        )
        .unwrap();
        let client = mailbox.client();
        let c = mailbox
            .spawn(
                Targets(released.clone()),
                cache::ConnectorLink::new(p.client()),
                otp_order::profile(&ENTROPY["profile"]),
            )
            .unwrap();
        let pair = Self {
            cache: Some(c),
            parent: Some(p),
            client,
            probe,
            released,
        };
        if ready {
            pair.ready();
        }
        pair
    }
    fn ready(&self) {
        wait(|| {
            [11, 41].iter().all(|h| {
                matches!(
                    self.client.snapshot().cache.blocks().get(&BigInt::from(*h)),
                    Some(parent_cache::Entry::Block(_))
                )
            })
        });
    }
    fn post(&self, h: u64, n: u64) {
        self.client.post_block(parent(h, n)).unwrap();
        self.client.barrier(Duration::from_secs(2)).unwrap();
    }
    fn coordinator(&self) -> Coordinator {
        let mut h = host(&[1, 2, 3], ServiceLimits::default());
        // This stale manual parent must never bypass the live cache's finality.
        add_parent(&mut h);
        h.set_parent_cache(Some(self.client.clone()));
        Coordinator::new(h, 1000.into(), api::Config::default())
    }
}
impl Drop for Pair {
    fn drop(&mut self) {
        self.released.store(true, Ordering::Release);
        if let Some(c) = self.cache.take() {
            c.shutdown().unwrap();
        }
        if let Some(p) = self.parent.take() {
            p.shutdown().unwrap();
        }
    }
}
fn wait(mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(3);
    while !f() {
        assert!(Instant::now() < end, "wait expired");
        thread::sleep(Duration::from_millis(1));
    }
}
fn drain(c: &mut Coordinator) {
    c.run(&otp_order::profile(&ENTROPY["profile"]), 1000)
        .unwrap();
    assert_eq!(c.queued_events(), 0);
}
fn parent_json(c: &Coordinator) -> J {
    named(
        [("fork", Mode::Fork), ("length", Mode::Length)]
            .into_iter()
            .map(|(n, m)| {
                (
                    n,
                    map(c
                        .machine(m)
                        .parents()
                        .iter()
                        .map(|(e, b)| (num(e), block_json(b)))
                        .collect()),
                )
            })
            .collect(),
    )
}
fn entropy_reply(r: Option<Vec<u8>>) -> J {
    match r {
        Some(b) => tuple(vec![atom("ok"), bin(b)]),
        None => tuple(vec![atom("error"), atom("not_in_cache")]),
    }
}
#[test]
fn block_host_uses_live_entropy_outbox_and_selected_pin_hashes() {
    use ae_hyperchain::{
        block::{
            self, Host,
            host::{LiveHost, PinSource},
        },
        chain,
    };
    #[derive(Clone)]
    struct Pins;
    impl PinSource for Pins {
        type Error = ();
        fn is_operational(_: &()) -> bool {
            true
        }
        fn get_pin(&mut self, hash: &[u8]) -> Result<Option<block::Pin>, ()> {
            Ok((hash == key(777)).then(|| block::Pin {
                epoch: 1.into(),
                child_height: 3.into(),
                child_hash: key(333).to_vec(),
                parent_height: 7.into(),
            }))
        }
    }
    let mut pair = Pair::start(true, 64);
    pair.post(43, 43);
    let selected = chain::Selected::new(chain::Selection {
        heights: BTreeMap::from([(3.into(), key(333).to_vec())]),
        ..Default::default()
    });
    let mut entropy = host(&[1], ServiceLimits::default());
    add_parent(&mut entropy);
    entropy.set_parent_cache(Some(pair.client.clone()));
    let mut live = LiveHost {
        entropy,
        selected: selected.clone(),
        pins: Pins,
    };
    let mut candidate = live.clone();
    assert_eq!(
        candidate.entropy(&9.into()).unwrap(),
        Some(key(41).to_vec())
    );
    assert!(live.entropy.outbox().is_empty());
    assert!(
        matches!(&candidate.entropy.outbox()[0], Output::ParentBlock {epoch,block} if epoch==&9.into() && block.hash==key(41))
    );
    live = candidate;
    let pin = live.pin(&key(777)).unwrap().unwrap();
    assert_eq!(
        live.key_hash(&pin.child_height).unwrap(),
        Some(pin.child_hash)
    );
    selected.replace(chain::Selection {
        heights: BTreeMap::from([(3.into(), key(334).to_vec())]),
        ..Default::default()
    });
    assert_eq!(live.key_hash(&3.into()).unwrap(), Some(key(334).to_vec()));
    assert_eq!(live.key_hash(&4.into()).unwrap(), None);
    pair.post(41, 55);
    assert_eq!(live.entropy(&9.into()).unwrap(), Some(key(55).to_vec()));
    pair.cache.take().unwrap().shutdown().unwrap();
    let before = live.entropy.outbox().len();
    assert!(matches!(
        live.entropy(&9.into()),
        Err(block::host::Error::Entropy(local_host::Error::ParentCache(
            cache::Error::Closed
        )))
    ));
    assert_eq!(live.entropy.outbox().len(), before);
}
#[test]
fn original_entropy_queries_require_finality_and_notify_both_workers() {
    let pair = Pair::start(true, 64);
    let mut c = pair.coordinator();
    for row in ENTROPY["direct"].as_array().unwrap() {
        let name = row["name"].as_str().unwrap();
        if name == "missing" {
            pair.probe.0.store(true, Ordering::Release);
        }
        if name == "final" {
            pair.probe.0.store(false, Ordering::Release);
            pair.post(43, 43);
        }
        if name == "replaced" {
            pair.post(41, 99);
        }
        let before = pair.client.snapshot().lookup_calls;
        let result = c
            .entropy_hash(&Unsigned::from_decimal(row["epoch"].as_str().unwrap()).unwrap())
            .unwrap();
        assert_eq!(entropy_reply(result), row["reply"], "{name}");
        drain(&mut c);
        assert_eq!(
            normalize(&parent_json(&c)),
            normalize(&row["parents"]),
            "{name}"
        );
        assert_eq!(
            pair.client.snapshot().lookup_calls - before,
            row["calls"].as_u64().unwrap()
        );
    }
}
#[test]
fn original_waiting_entropy_delivers_parent_after_finality_advances() {
    let pair = Pair::start(true, 64);
    let mut c = pair.coordinator();
    let t = thread::spawn(move || {
        let r = c.entropy_hash(&9.into()).unwrap();
        drain(&mut c);
        (r, c)
    });
    wait(|| pair.client.snapshot().lookup_calls > 0);
    pair.post(43, 43);
    let (r, c) = t.join().unwrap();
    assert_eq!(entropy_reply(r), ENTROPY["wait"]["reply"]);
    assert_eq!(
        normalize(&parent_json(&c)),
        normalize(&ENTROPY["wait"]["parents"])
    );
    assert!((2..=101).contains(&pair.client.snapshot().lookup_calls));
}
#[test]
fn actual_cache_and_original_eoe_finalization_match_signed_bytes_and_nonces() {
    for ready in [true, false] {
        let pair = Pair::start(true, 64);
        if ready {
            pair.post(43, 43);
        }
        let mut c = pair.coordinator();
        c.negotiate(negotiation(7)).unwrap();
        drain(&mut c);
        c.request_finalize(1, Arc::new(trees(5)), 0).unwrap();
        drain(&mut c);
        let mut reply = None;
        while let Some(o) = c.pop_output() {
            if let ServiceOutput::Finalized { replies, .. } = o {
                reply = Some(pair_json(&replies));
            }
        }
        let row = &ENTROPY[if ready { "final" } else { "nonfinal" }];
        assert_eq!(reply.as_ref(), Some(&row["reply"]));
        assert_eq!(
            normalize(&snapshot(&c, &[1, 2, 3], 1000)),
            normalize(&row["state"])
        );
        assert_eq!(
            pair.client.snapshot().lookup_calls,
            row["calls"].as_u64().unwrap()
        );
    }
}
#[test]
fn original_cache_exit_restarts_only_the_waiting_eoe_worker() {
    let mut pair = Pair::start(true, 64);
    let mut c = pair.coordinator();
    c.negotiate(negotiation(7)).unwrap();
    drain(&mut c);
    pair.cache.take().unwrap().shutdown().unwrap();
    assert_eq!(
        c.entropy_hash(&9.into()),
        Err(api::Error::Host(local_host::Error::ParentCache(
            cache::Error::Closed
        )))
    );
    assert_eq!(ENTROPY["absent"]["direct"], atom("exit_noproc"));
    c.request_finalize(1, Arc::new(trees(5)), 0).unwrap();
    drain(&mut c);
    assert_eq!(
        normalize(&snapshot(&c, &[1, 2, 3], 1000)),
        normalize(&ENTROPY["absent"]["state"])
    );
    let mut failed = false;
    let mut restarted = false;
    while let Some(o) = c.pop_output() {
        match o {
            ServiceOutput::CallFailed {
                request: 1,
                failure: CallFailure::WorkerExited(Mode::Fork),
            } => failed = true,
            ServiceOutput::Restarted {
                mode: Mode::Fork,
                reason: StepError::Host(local_host::Error::ParentCache(cache::Error::Closed)),
            } => restarted = true,
            ServiceOutput::Publication(Output::Vote { .. }) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(failed && restarted);
    assert_eq!(ENTROPY["absent"]["reply"], atom("worker_exit"));
}
#[test]
fn original_five_second_cache_timeout_preserves_accepted_lookup_without_notification() {
    let pair = Pair::start(false, 64);
    let mut c = pair.coordinator();
    let start = Instant::now();
    let r = c.entropy_hash(&9.into());
    assert_eq!(
        r,
        Err(api::Error::Host(local_host::Error::ParentCache(
            cache::Error::Timeout
        )))
    );
    assert!(
        start.elapsed() >= Duration::from_millis(4900)
            && start.elapsed() < Duration::from_millis(5900)
    );
    assert_eq!(ENTROPY["timeout"]["reply"], atom("exit_timeout"));
    pair.released.store(true, Ordering::Release);
    pair.ready();
    pair.client.barrier(Duration::from_secs(2)).unwrap();
    assert_eq!(pair.client.snapshot().lookup_calls, 1);
    assert_eq!(
        normalize(&parent_json(&c)),
        normalize(&ENTROPY["timeout"]["parents"])
    );
    assert!(c.pop_output().is_none());
}
#[test]
fn background_eoe_runtime_waits_for_live_entropy_and_routes_both_finalizations() {
    let pair = Pair::start(true, 64);
    let rt = runtime::Runtime::spawn(
        pair.coordinator(),
        otp_order::profile(&ENTROPY["profile"]),
        runtime::Config::default(),
    )
    .unwrap();
    assert!(
        rt.try_submit(1, runtime::Command::Negotiate(negotiation(7)))
            .is_ok()
    );
    let mut votes = 0;
    while votes < 10 {
        match rt.recv_timeout(Duration::from_secs(2)).unwrap() {
            runtime::Output::Service(ServiceOutput::Publication(Output::Vote { .. })) => votes += 1,
            runtime::Output::Applied { result: Ok(_), .. } => {}
            o => panic!("unexpected {o:?}"),
        }
    }
    assert!(
        rt.try_submit(
            2,
            runtime::Command::Finalize {
                request: 1,
                view: Arc::new(trees(5))
            }
        )
        .is_ok()
    );
    wait(|| pair.client.snapshot().lookup_calls > 0);
    pair.post(43, 43);
    let mut final_reply = None;
    let mut notified = 0;
    while final_reply.is_none() || notified < 2 {
        match rt.recv_timeout(Duration::from_secs(2)).unwrap() {
            runtime::Output::Service(ServiceOutput::Publication(Output::ParentBlock {
                epoch,
                block,
            })) => {
                assert_eq!(epoch, 9.into());
                assert_eq!(block, parent(41, 41));
                notified += 1;
            }
            runtime::Output::Service(ServiceOutput::Finalized { replies, .. }) => {
                final_reply = Some(pair_json(&replies))
            }
            runtime::Output::Applied { result: Ok(_), .. } => {}
            o => panic!("unexpected {o:?}"),
        }
    }
    assert_eq!(final_reply.as_ref(), Some(&ENTROPY["final"]["reply"]));
    rt.shutdown().unwrap();
}
#[test]
fn operational_entropy_failures_keep_callbacks_retryable_and_publications_atomic() {
    let pair = Pair::start(true, 64);
    pair.post(43, 43);
    let mut c = pair.coordinator();
    let mut limits = c.config();
    limits.maximum_outputs = 0;
    c.set_config(limits);
    assert_eq!(c.entropy_hash(&9.into()), Err(api::Error::ResourceLimit));
    assert_eq!(c.queued_events(), 0);
    assert!(c.pop_output().is_none());
    assert!(c.host().outbox().is_empty());
    for mode in [Mode::Fork, Mode::Length] {
        assert!(c.machine(mode).parents().is_empty());
    }
    c.set_config(api::Config::default());
    assert_eq!(c.entropy_hash(&9.into()).unwrap(), Some(key(41).to_vec()));
    drain(&mut c);
    while c.pop_output().is_some() {}
    c.negotiate(negotiation(7)).unwrap();
    drain(&mut c);
    while c.pop_output().is_some() {}
    pair.client.configure(cache::Limits {
        maximum_command_bytes: 0,
        ..Default::default()
    });
    c.request_finalize(1, Arc::new(trees(5)), 0).unwrap();
    let before = c.machine(Mode::Fork).clone();
    assert_eq!(
        c.step(&otp_order::profile(&ENTROPY["profile"])),
        Err(api::Error::Step(StepError::Host(
            local_host::Error::ParentCache(cache::Error::ResourceLimit)
        )))
    );
    assert_eq!(c.machine(Mode::Fork), &before);
    assert_eq!(c.queued_events(), 1);
    assert!(c.pop_output().is_none());
    pair.client.configure(cache::Limits::default());
    drain(&mut c);
    assert_eq!(c.pending_requests(), 0);
    assert!(!c.is_stopped());
    let mut signed = false;
    while let Some(o) = c.pop_output() {
        if let ServiceOutput::Finalized { replies, .. } = o {
            assert!(
                replies
                    .iter()
                    .all(|r| matches!(r, Reply::Finalized(Finalized::Signed(_))))
            );
            signed = true;
        }
    }
    assert!(signed);
}
#[test]
fn cancellation_interrupts_waits_without_removing_accepted_cache_commands() {
    let pair = Pair::start(false, 1);
    let call = pair.client.call(CacheEvent::Get(41.into())).unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let other = cancel.clone();
    let t = thread::spawn(move || call.wait_cancelled(Duration::from_secs(5), &other));
    cancel.store(true, Ordering::Release);
    let start = Instant::now();
    assert_eq!(t.join().unwrap(), Err(cache::Error::Cancelled));
    assert!(start.elapsed() < Duration::from_millis(200));
    pair.released.store(true, Ordering::Release);
    pair.ready();
    wait(|| pair.client.snapshot().lookup_calls == 1);
    let before = pair.client.snapshot().lookup_calls;
    assert_eq!(
        pair.client
            .get_block_cancelled(41.into(), 1000.into(), &cancel),
        Err(cache::Error::Cancelled)
    );
    assert_eq!(pair.client.snapshot().lookup_calls, before);
}
#[test]
fn runtime_shutdown_interrupts_polling_and_cache_link_can_be_replaced() {
    let pair = Pair::start(true, 64);
    let rt = runtime::Runtime::spawn(
        pair.coordinator(),
        otp_order::profile(&ENTROPY["profile"]),
        runtime::Config::default(),
    )
    .unwrap();
    assert!(
        rt.try_submit(1, runtime::Command::Entropy(9.into()))
            .is_ok()
    );
    wait(|| pair.client.snapshot().lookup_calls > 0);
    let start = Instant::now();
    rt.shutdown().unwrap();
    assert!(start.elapsed() < Duration::from_millis(200));
    let rt = runtime::Runtime::spawn(
        Coordinator::new(
            host(&[], ServiceLimits::default()),
            1000.into(),
            api::Config::default(),
        ),
        otp_order::profile(&ENTROPY["profile"]),
        runtime::Config::default(),
    )
    .unwrap();
    assert!(
        rt.try_submit(1, runtime::Command::ParentCache(Some(pair.client.clone())))
            .is_ok()
    );
    assert!(matches!(
        rt.recv_timeout(Duration::from_secs(2)).unwrap(),
        runtime::Output::Applied {
            sequence: 1,
            result: Ok(runtime::Applied::Unit)
        }
    ));
    pair.post(43, 43);
    assert!(
        rt.try_submit(2, runtime::Command::Entropy(9.into()))
            .is_ok()
    );
    assert!(
        matches!(rt.recv_timeout(Duration::from_secs(2)).unwrap(),runtime::Output::Applied{sequence:2,result:Ok(runtime::Applied::Entropy(Some(h)))} if h==key(41))
    );
    rt.shutdown().unwrap();
}

#[test]
fn routed_entropy_selects_a_generation_once_and_never_falls_back_during_a_gap() {
    let mut old = Pair::start(true, 64);
    let next = Pair::start(true, 64);
    next.post(41, 141);
    next.post(43, 43);
    let route = cache::Route::new(old.client.clone());
    let mut host = host(&[1, 2, 3], ServiceLimits::default());
    add_parent(&mut host);
    host.set_parent_route(route.clone());
    let calling = thread::spawn(move || {
        let result = host.entropy_hash(&9.into());
        (result, host)
    });
    wait(|| old.client.snapshot().lookup_calls > 0);
    route.replace(Some(next.client.clone()));
    let before = old.client.snapshot().lookup_calls;
    wait(|| old.client.snapshot().lookup_calls > before);
    assert_eq!(next.client.snapshot().lookup_calls, 0);
    old.cache.take().unwrap().shutdown().unwrap();
    let (result, mut host) = calling.join().unwrap();
    assert_eq!(
        result,
        Err(local_host::Error::ParentCache(cache::Error::Closed))
    );
    assert_eq!(
        host.entropy_hash(&9.into()).unwrap(),
        Some(key(141).to_vec())
    );
    route.replace(None);
    assert_eq!(
        host.entropy_hash(&9.into()),
        Err(local_host::Error::ParentCache(cache::Error::Closed))
    );
}
