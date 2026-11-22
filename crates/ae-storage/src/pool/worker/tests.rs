use super::*;
use crate::pool::Submission;
use crate::pool::tests::{Temp, blocks, config, database, prepare, tx};
use ae_mempool::pool::{self as policy, Event, Location, PushOutcome};
use std::collections::BTreeSet;
use std::sync::mpsc;
mod gossip;

fn settings() -> Settings {
    Settings {
        origins_interval: None,
        ..Default::default()
    }
}
fn spawn(db: Arc<Database>) -> Worker {
    Worker::spawn(db, NetworkProfile::mainnet(), config(), settings()).unwrap()
}
fn until(client: &Client, predicate: impl Fn(&Info) -> bool) -> Info {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut info = client.info();
    while !predicate(&info) {
        assert!(Instant::now() < deadline, "worker did not settle: {info:?}");
        info = client.wait_for_change(info.version, Duration::from_millis(100));
    }
    info
}
fn gate(client: &Client) -> (mpsc::Sender<()>, Ticket<Inspection>) {
    let (started, ready) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let ticket = client
        .submit(BASE_WEIGHT, move |s| {
            started.send(()).unwrap();
            wait.recv().unwrap();
            s.refresh()
        })
        .unwrap();
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    (release, ticket)
}

#[test]
fn async_tickets_wake_the_latest_waiter_and_report_lost_replies() {
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Wake, Waker},
    };
    struct Notify {
        count: AtomicUsize,
        signal: mpsc::Sender<()>,
    }
    impl Wake for Notify {
        fn wake(self: Arc<Self>) {
            self.count.fetch_add(1, Ordering::Relaxed);
            let _ = self.signal.send(());
        }
    }
    let (signal, notified) = mpsc::channel();
    let first = Arc::new(Notify {
        count: AtomicUsize::new(0),
        signal: signal.clone(),
    });
    let latest = Arc::new(Notify {
        count: AtomicUsize::new(0),
        signal,
    });
    let first_waker = Waker::from(first.clone());
    let last_waker = Waker::from(latest.clone());
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db);
    let client = worker.client();
    let (release, gate) = gate(&client);
    let mut ticket = client.push(tx(), Event::Created, false).unwrap();
    assert!(
        Pin::new(&mut ticket)
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        Pin::new(&mut ticket)
            .poll(&mut Context::from_waker(&last_waker))
            .is_pending()
    );
    release.send(()).unwrap();
    gate.wait().unwrap();
    notified.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(first.count.load(Ordering::Relaxed), 0);
    assert_eq!(latest.count.load(Ordering::Relaxed), 1);
    assert!(matches!(
        Pin::new(&mut ticket).poll(&mut Context::from_waker(&last_waker)),
        Poll::Ready(Ok(Submission {
            outcome: PushOutcome::Accepted { .. },
            ..
        }))
    ));
    let (sender, mut lost) = reply::channel::<()>();
    assert!(
        Pin::new(&mut lost)
            .poll(&mut Context::from_waker(&last_waker))
            .is_pending()
    );
    drop(sender);
    notified.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(
        Pin::new(&mut lost).poll(&mut Context::from_waker(&last_waker)),
        Poll::Ready(Err(Error::Disconnected))
    );
    worker.shutdown().unwrap();
}

#[test]
fn queued_configuration_and_owner_drop_preserve_restart_and_reentry_policy() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let old = worker.client();
    old.push(tx(), Event::Created, false)
        .unwrap()
        .wait()
        .unwrap();
    old.candidate(1000000, BTreeSet::new(), Default::default())
        .unwrap()
        .wait()
        .unwrap();
    old.record_failure(tx().hash(), "insufficient_funds".into(), false)
        .unwrap()
        .wait()
        .unwrap();
    drop(worker);
    assert_eq!(old.refresh().err(), Some(Error::Stopped));
    let worker = spawn(db);
    let client = worker.client();
    let info = client.inspect(tx().hash()).unwrap().wait().unwrap();
    assert!(!info.visited);
    assert_eq!(info.failures, 0);
    client.delete(tx().hash()).unwrap().wait().unwrap();
    let mut cfg = config();
    cfg.policy.allow_reentry = true;
    cfg.policy.tx_ttl = 7;
    client
        .reconfigure(cfg.policy, cfg.failures)
        .unwrap()
        .wait()
        .unwrap();
    client
        .push(tx(), Event::Received, false)
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        client.inspect(tx().hash()).unwrap().wait().unwrap().expires,
        7
    );
    client.recover(config()).unwrap().wait().unwrap();
    assert_eq!(
        client.inspect(tx().hash()).unwrap().wait().unwrap().expires,
        20
    );
    worker.shutdown().unwrap();
}

#[test]
fn typed_requests_and_automatic_chain_notifications_preserve_durable_state() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let client = worker.client();
    let t = tx();
    let h = t.hash();
    until(&client, |i| i.background_runs > 0 && !i.in_flight);
    assert!(matches!(
        Service::start(db.clone(), NetworkProfile::mainnet(), config()),
        Err(Failure::AlreadyRunning)
    ));
    assert!(matches!(
        Worker::spawn(db.clone(), NetworkProfile::mainnet(), config(), settings()),
        Err(Error::Service(Failure::AlreadyRunning))
    ));
    assert!(matches!(
        client
            .push(t.clone(), Event::Created, false)
            .unwrap()
            .wait()
            .unwrap()
            .outcome,
        PushOutcome::Accepted { .. }
    ));
    let origin = t.transaction().view().origin().unwrap();
    assert_eq!(
        client.peek(10, Some(origin), None).unwrap().wait().unwrap(),
        vec![t.clone()]
    );
    assert_eq!(
        client.max_nonce(origin).unwrap().wait().unwrap(),
        Some(1.into())
    );
    assert_eq!(
        client
            .candidate(1000000, BTreeSet::new(), Default::default())
            .unwrap()
            .wait()
            .unwrap()
            .transactions,
        vec![t.clone()]
    );
    assert!(client.inspect(h).unwrap().wait().unwrap().visited);
    assert!(
        !client
            .record_failure(h, "insufficient_funds".into(), false)
            .unwrap()
            .wait()
            .unwrap()
    );
    assert_eq!(client.inspect(h).unwrap().wait().unwrap().failures, 1);
    client.sync_target(100).unwrap().wait().unwrap();
    assert_eq!(client.info().pool.gc_height, 100);
    assert!(client.collect(true, 1).unwrap().wait().unwrap().is_empty());
    for block in blocks().iter().take(2) {
        db.commit(prepare(&db, block)).unwrap();
    }
    let revision = db.view().unwrap().status().revision;
    let info = until(&client, |i| {
        i.pool.chain.revision == revision && !i.in_flight
    });
    assert_eq!(info.pool.active + info.pool.visited, 0);
    assert!(matches!(
        db.pool_view().unwrap().location(h).unwrap(),
        Location::Block(_)
    ));
    assert_eq!(
        client.inspect(h).unwrap().wait(),
        Err(Error::Service(Failure::Policy(
            policy::Error::AlreadyAccepted
        )))
    );
    let info = worker.shutdown().unwrap();
    assert!(!info.running && !info.accepting);
    assert_eq!(client.refresh().err(), Some(Error::Stopped));
    let mut service = Service::start(db, NetworkProfile::mainnet(), config()).unwrap();
    assert!(service.peek(10, None, None).unwrap().is_empty());
}

#[test]
fn queue_bounds_include_executing_work_and_cached_received_bypasses_admission() {
    for byte_bound in [false, true] {
        let temp = Temp::new();
        let db = database(&temp);
        let mut limits = settings();
        if byte_bound {
            limits.request_bytes = 2 * BASE_WEIGHT;
        } else {
            limits.requests = 2;
        }
        let worker =
            Worker::spawn(db.clone(), NetworkProfile::mainnet(), config(), limits).unwrap();
        let client = worker.client();
        let (release, first) = gate(&client);
        let second = client.refresh().unwrap();
        assert_eq!(client.info().retained_request_bytes, 2 * BASE_WEIGHT);
        assert_eq!(client.info().queued, 1);
        assert_eq!(client.refresh().err(), Some(Error::Saturated));
        let submitted = client.info().submitted;
        assert_eq!(
            client
                .push(tx(), Event::Received, true)
                .unwrap()
                .wait()
                .unwrap()
                .outcome,
            PushOutcome::Cached
        );
        assert_eq!(client.info().submitted, submitted);
        assert_eq!(
            db.pool_view().unwrap().location(tx().hash()).unwrap(),
            Location::Unknown
        );
        assert_eq!(second.wait_timeout(Duration::ZERO), Err(Error::Timeout));
        release.send(()).unwrap();
        first.wait().unwrap();
        second.wait().unwrap();
        let info = client.info();
        assert_eq!(info.retained_request_bytes, 0);
        assert_eq!(info.completed, 2);
        client.refresh().unwrap().wait().unwrap();
        worker.shutdown().unwrap();
    }
}

#[test]
fn request_payload_and_result_bounds_reject_before_queueing() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = Worker::spawn(
        db.clone(),
        NetworkProfile::mainnet(),
        config(),
        Settings {
            single_request_bytes: BASE_WEIGHT,
            max_peek: 1,
            ..settings()
        },
    )
    .unwrap();
    let client = worker.client();
    let initial = client.info().submitted;
    assert!(matches!(
        client.force_push(tx()),
        Err(Error::Service(Failure::Repository(
            ae_chain::repository::Error::TransactionStorage(ae_tx::storage::Error::ResourceLimit)
        )))
    ));
    assert_eq!(
        client.peek(2, None, None).err(),
        Some(Error::RequestTooLarge)
    );
    assert_eq!(
        client
            .record_failure(tx().hash(), "reason".into(), false)
            .err(),
        Some(Error::RequestTooLarge)
    );
    assert_eq!(
        client
            .candidate(1000000, BTreeSet::from([tx().hash()]), Default::default())
            .err(),
        Some(Error::RequestTooLarge)
    );
    let cfg = config();
    assert_eq!(
        client.reconfigure(cfg.policy, cfg.failures).err(),
        Some(Error::RequestTooLarge)
    );
    assert_eq!(client.info().submitted, initial);
    assert_eq!(
        db.pool_view().unwrap().location(tx().hash()).unwrap(),
        Location::Unknown
    );
    worker.shutdown().unwrap();
    for mode in 0..4 {
        let mut limits = settings();
        match mode {
            0 => limits.requests = 0,
            1 => limits.request_bytes = 0,
            2 => limits.single_request_bytes = 0,
            _ => limits.origins_interval = Some(Duration::ZERO),
        }
        assert!(Worker::spawn(db.clone(), NetworkProfile::mainnet(), config(), limits).is_err());
    }
    spawn(db).shutdown().unwrap();
}

#[test]
fn caller_timeout_and_dropped_reply_do_not_cancel_accepted_mutations_or_shutdown_drain() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let client = worker.client();
    let (release, first) = gate(&client);
    let inserted = client.push(tx(), Event::Created, false).unwrap();
    assert_eq!(inserted.wait_timeout(Duration::ZERO), Err(Error::Timeout));
    drop(inserted);
    let deleted = client.delete(tx().hash()).unwrap();
    let shutting = thread::spawn(move || worker.shutdown());
    until(&client, |i| !i.accepting);
    assert_eq!(client.refresh().err(), Some(Error::Stopped));
    release.send(()).unwrap();
    first.wait().unwrap();
    deleted.wait().unwrap();
    let info = shutting.join().unwrap().unwrap();
    assert_eq!(info.completed, 3);
    assert_eq!(info.retained_request_bytes, 0);
    assert_eq!(info.queued, 0);
    assert!(!info.running);
    assert_eq!(
        db.pool_view().unwrap().location(tx().hash()).unwrap(),
        Location::Deleted
    );
    assert_eq!(
        db.pool_view()
            .unwrap()
            .transaction(tx().hash())
            .unwrap()
            .as_ref(),
        Some(tx().as_ref())
    );
    spawn(db).shutdown().unwrap();
}

#[test]
fn queued_candidate_deadlines_include_time_spent_waiting() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db);
    let client = worker.client();
    client
        .push(tx(), Event::Created, false)
        .unwrap()
        .wait()
        .unwrap();
    let (release, first) = gate(&client);
    let selected = client
        .candidate(
            1000000,
            BTreeSet::new(),
            SelectOptions {
                ceiling: Some(Instant::now()),
                ..Default::default()
            },
        )
        .unwrap();
    release.send(()).unwrap();
    first.wait().unwrap();
    let selected = selected.wait().unwrap();
    // Original selection checks time at bounded chunk/inner-loop boundaries,
    // so an already elapsed ceiling may still return this first transaction.
    assert!(selected.expired);
    assert_eq!(selected.transactions, vec![tx()]);
    assert!(client.inspect(tx().hash()).unwrap().wait().unwrap().visited);
    let selected = client
        .candidate(
            1000000,
            BTreeSet::new(),
            SelectOptions {
                deadline: Some(Instant::now()),
                ..Default::default()
            },
        )
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(selected.transactions, vec![tx()]);
    let selected = client
        .candidate(
            1000000,
            BTreeSet::new(),
            SelectOptions {
                deadline: Some(Instant::now()),
                packed: true,
                ..Default::default()
            },
        )
        .unwrap()
        .wait()
        .unwrap();
    assert!(selected.expired);
    assert_eq!(selected.transactions, vec![tx()]);
    worker.shutdown().unwrap();
}

#[test]
fn automatic_expiry_and_periodic_origin_cleanup_need_no_client_polling() {
    for origins in [false, true] {
        let temp = Temp::new();
        let db = database(&temp);
        let original = tx();
        let pending = Arc::new(ae_tx::SignedTransaction::new(
            original.transaction().clone(),
            vec![],
        ));
        let mut cfg = config();
        let mut limits = settings();
        if origins {
            limits.origins_interval = Some(Duration::from_millis(20));
        } else {
            cfg.policy.tx_ttl = 2;
        }
        let worker = Worker::spawn(db.clone(), NetworkProfile::mainnet(), cfg, limits).unwrap();
        let client = worker.client();
        client.force_push(pending.clone()).unwrap().wait().unwrap();
        for block in blocks().iter().take(if origins { 2 } else { 3 }) {
            db.commit(prepare(&db, block)).unwrap();
        }
        let revision = db.view().unwrap().status().revision;
        until(&client, |i| {
            i.pool.chain.revision == revision && i.pool.active + i.pool.visited == 0
        });
        assert_eq!(
            db.pool_view().unwrap().location(pending.hash()).unwrap(),
            Location::Deleted
        );
        worker.shutdown().unwrap();
    }
}

#[test]
fn coalesced_notifications_report_catchup_limit_and_explicit_recovery_resumes_requests() {
    let temp = Temp::new();
    let db = database(&temp);
    let mut cfg = config();
    cfg.events = 1;
    let worker = Worker::spawn(db.clone(), NetworkProfile::mainnet(), cfg, settings()).unwrap();
    let client = worker.client();
    let (release, first) = gate(&client);
    for block in blocks() {
        db.commit(prepare(&db, &block)).unwrap();
    }
    release.send(()).unwrap();
    assert_eq!(
        first.wait(),
        Err(Error::Service(Failure::Repository(
            ae_chain::repository::Error::ResourceLimit
        )))
    );
    until(&client, |i| !i.in_flight && i.last_error.is_some());
    assert_eq!(
        client.refresh().unwrap().wait(),
        Err(Error::Service(Failure::Repository(
            ae_chain::repository::Error::ResourceLimit
        )))
    );
    client.recover(config()).unwrap().wait().unwrap();
    assert_eq!(client.info().pool.chain, db.view().unwrap().status());
    assert!(
        client
            .peek(10, None, None)
            .unwrap()
            .wait()
            .unwrap()
            .is_empty()
    );
    worker.shutdown().unwrap();
}

#[test]
fn concurrent_clients_have_one_admission_and_shutdown_releases_ownership() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let client = worker.client();
    let barrier = Arc::new(std::sync::Barrier::new(16));
    let jobs: Vec<_> = (0..16)
        .map(|_| {
            let client = client.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                client.push(tx(), Event::Created, false).unwrap().wait()
            })
        })
        .collect();
    let mut accepted = 0;
    for job in jobs {
        match job.join().unwrap() {
            Ok(Submission {
                outcome: PushOutcome::Accepted { .. },
                ..
            }) => accepted += 1,
            Err(Error::Service(Failure::Policy(policy::Error::AlreadyKnown))) => {}
            result => panic!("unexpected admission: {result:?}"),
        }
    }
    assert_eq!(accepted, 1);
    let info = worker.shutdown().unwrap();
    assert_eq!(info.completed, 16);
    assert_eq!(info.pool.active, 1);
    let next = spawn(db);
    assert_eq!(
        next.client().peek(1, None, None).unwrap().wait().unwrap(),
        vec![tx()]
    );
    next.shutdown().unwrap();
}

#[test]
fn worker_panic_resolves_queued_replies_and_can_restore_persisted_state() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let client = worker.client();
    client
        .push(tx(), Event::Created, false)
        .unwrap()
        .wait()
        .unwrap();
    let (started, ready) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let crashing: Ticket<()> = client
        .submit(BASE_WEIGHT, move |_| {
            started.send(()).unwrap();
            wait.recv().unwrap();
            panic!("injected worker panic")
        })
        .unwrap();
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    let pending = client.delete(tx().hash()).unwrap();
    release.send(()).unwrap();
    assert_eq!(crashing.wait(), Err(Error::Disconnected));
    assert_eq!(pending.wait(), Err(Error::Stopped));
    until(&client, |i| !i.running);
    assert_eq!(worker.shutdown().err(), Some(Error::Panicked));
    assert_eq!(client.info().last_error, Some(Error::Panicked));
    assert_eq!(client.info().retained_request_bytes, 0);
    assert_eq!(
        db.pool_view().unwrap().location(tx().hash()).unwrap(),
        Location::Pool
    );
    let next = spawn(db);
    assert_eq!(
        next.client().peek(1, None, None).unwrap().wait().unwrap(),
        vec![tx()]
    );
    next.shutdown().unwrap();
}

#[test]
fn failure_batch_keeps_one_receipt_across_timeout_and_rejects_excessive_requests() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let client = worker.client();
    let h = tx().hash();
    client.force_push(tx()).unwrap().wait().unwrap();
    let (release, first) = gate(&client);
    let report = crate::pool::FailedTransaction {
        hash: h,
        reason: "insufficient_funds".into(),
        tuple: false,
    };
    let batch = client
        .record_failures(vec![report.clone(), report])
        .unwrap();
    assert_eq!(batch.wait_timeout(Duration::ZERO), Err(Error::Timeout));
    release.send(()).unwrap();
    first.wait().unwrap();
    assert_eq!(
        batch.wait_timeout(Duration::from_secs(5)).unwrap(),
        vec![false, false]
    );
    assert_eq!(batch.wait_timeout(Duration::ZERO), Err(Error::Disconnected));
    assert_eq!(client.inspect(h).unwrap().wait().unwrap().failures, 2);
    let submitted = client.info().submitted;
    assert_eq!(
        client
            .record_failures(vec![crate::pool::FailedTransaction {
                hash: h,
                reason: "x".repeat(settings().single_request_bytes),
                tuple: false
            }])
            .err(),
        Some(Error::RequestTooLarge)
    );
    assert_eq!(client.info().submitted, submitted);
    let clock = client.clock();
    let now = clock.now();
    let instant = clock.instant(now).unwrap();
    assert!(Instant::now().saturating_duration_since(instant) < Duration::from_secs(1));
    assert_eq!(
        clock.instant(now + 9).unwrap().duration_since(instant),
        Duration::from_millis(9)
    );
    assert_eq!(
        clock.instant(-1).unwrap() + Duration::from_millis(1),
        clock.instant(0).unwrap()
    );
    worker.shutdown().unwrap();
}
