use super::*;
use crate::test_support::hyperchain::{Node, conductor::start};
use std::sync::atomic::{AtomicBool, Ordering};

fn until(mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < end, "runtime progress timeout");
        thread::sleep(Duration::from_millis(1));
    }
}
fn output<O: MapOrder + Clone + Send + Sync + 'static>(authority: &Authority<O>) -> Output {
    let mut output = None;
    until(|| {
        output = authority.try_output();
        output.is_some()
    });
    output.unwrap()
}
fn settings() -> Settings {
    Settings {
        poll: Duration::from_millis(1),
        outputs: 16,
    }
}
#[derive(Clone)]
struct Manual(Arc<Mutex<(u64, Instant)>>);
impl Clock for Manual {
    fn now(&self) -> Result<(u64, Instant), Error> {
        Ok(*lock(&self.0))
    }
}
struct Blocked(Arc<AtomicBool>, Arc<AtomicBool>);
impl Clock for Blocked {
    fn now(&self) -> Result<(u64, Instant), Error> {
        self.0.store(true, Ordering::Release);
        while !self.1.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(1));
        }
        Ok((1000, Instant::now()))
    }
}
struct Release(Arc<AtomicBool>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
struct Panics;
impl Clock for Panics {
    fn now(&self) -> Result<(u64, Instant), Error> {
        panic!("injected production clock panic");
    }
}

#[test]
fn blocked_clock_does_not_block_key_recovery_controls_or_quiescence() {
    let node = Node::new(0);
    let c = start(&node, true);
    let keys = c.preset_key_binding();
    let old = keys.current().unwrap();
    let pool = c.pool();
    let entered = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let runtime = Runtime::spawn(c, Blocked(entered.clone(), released.clone()), settings())
        .unwrap_or_else(|e| panic!("{}", e.error));
    let release = Release(released);
    let h = runtime.handle();
    until(|| entered.load(Ordering::Acquire));
    let unknown = old.panic_for_test();
    until(|| h.service_status().consensus.key_generation == 2);
    assert!(unknown.try_result().unwrap().is_err());
    assert!(!old.same_worker(&keys.current().unwrap()));
    assert_eq!(h.service_status().consensus.attempts, 1);
    assert_eq!(h.service_status().attempts, 0);
    assert!(pool.info().accepting && !runtime.service_is_finished());
    let client = h.session();
    let tickets: Vec<_> = (0..64)
        .map(|_| client.start_production().unwrap())
        .collect();
    until(|| h.controls().applied == 64);
    assert_eq!(h.usage().controls, 64);
    assert!(matches!(client.stop_production(), Err(RequestError::Busy)));
    h.quiesce();
    until(|| h.quiesced());
    assert!(!h.status().producing && h.status().failure.is_none());
    assert!(matches!(
        client.start_production(),
        Err(RequestError::Closed)
    ));
    assert!(
        tickets
            .iter()
            .all(|t| t.try_result() == Some(Ok(Reply::Applied)))
    );
    drop(release);
    let exit = runtime.shutdown().unwrap();
    assert!(exit.production_driver.unwrap().exit.is_ok());
    assert!(exit.pool.is_ok() && exit.owner.errors.is_empty());
    drop(tickets);
    assert_eq!(h.usage(), Usage::default());
}

#[test]
fn driver_unwind_retires_committed_work_and_fresh_sessions_produce_across_protocols() {
    for profile in [0, 2, 4] {
        let node = Node::new(profile);
        let mut c = start(&node, true);
        let keys = c.preset_key_binding();
        let key = keys.current().unwrap();
        let pool = c.pool();
        let mut wall = 1000;
        let mut now = Instant::now();
        until(|| {
            let p = c.driven_step(Some((wall, now)), Instant::now()).unwrap();
            assert!(c.output().is_none());
            if let Some(d) = c.status().deadline {
                wall += d.saturating_duration_since(now).as_millis() as u64;
                now = d;
            }
            p == Progress::Production(production::Progress::Submitted(production::Kind::Key))
        });
        let hash = c
            .attempt()
            .unwrap()
            .pending_block()
            .unwrap()
            .hash()
            .unwrap();
        until(|| node.db.view().unwrap().status().tip == Some(hash));
        let runtime =
            Runtime::spawn(c, Panics, settings()).unwrap_or_else(|e| panic!("{}", e.error));
        let (authority, driver) = runtime.into_parts();
        let h = authority.handle();
        until(|| driver.is_finished());
        let report = driver.shutdown();
        assert!(report.exit.is_err() && report.pending.is_none());
        assert_eq!(report.failure, Some(Error::DriverPanicked));
        assert!(h.service_status().failure.is_none() && !authority.is_finished());
        let mut stopped = None;
        until(|| {
            if let Some(output) = authority.try_output() {
                match output {
                    Output::Stopped(s) => stopped = Some(s),
                    other => panic!("{other:?}"),
                }
            }
            stopped.is_some()
        });
        let stopped = stopped.unwrap();
        assert_eq!(stopped.pending_hash, Some(hash));
        assert_eq!(
            stopped
                .admission
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .accepted[0]
                .hash,
            hash
        );
        assert_eq!(node.db.view().unwrap().status().tip, Some(hash));
        assert!(pool.info().accepting && key.same_worker(&keys.current().unwrap()));
        let clock = Manual(Arc::new(Mutex::new((wall, now))));
        assert!(Driver::spawn(h.clone(), clock.clone(), settings().poll).is_err());
        let old = h.session();
        let restart = old.restart().unwrap();
        assert_eq!(
            restart.wait_timeout(Duration::from_secs(5)),
            Some(Ok(Reply::Applied))
        );
        until(|| h.service_status().session_generation == 2);
        assert!(matches!(
            output(&authority),
            Output::SessionReplaced { generation: 2, .. }
        ));
        assert!(matches!(
            old.stop_production(),
            Err(RequestError::Stale {
                requested: 1,
                current: 2
            })
        ));
        let driver = Driver::spawn(h.clone(), clock.clone(), settings().poll).unwrap();
        assert!(Driver::spawn(h.clone(), clock.clone(), settings().poll).is_err());
        assert!(h.status().failure.is_none());
        until(|| {
            assert!(h.status().failure.is_none(), "{:?}", h.status());
            if let Some(d) = h.status().deadline {
                let mut t = lock(&clock.0);
                t.0 += d.saturating_duration_since(t.1).as_millis() as u64;
                t.1 = d;
            }
            h.status().completed > 0
        });
        assert!(matches!(
            output(&authority),
            Output::Attempt(production::Completed {
                outcome: production::Outcome::Produced { .. },
                ..
            })
        ));
        assert_eq!(h.service_status().consensus.attempts, 0);
        assert!(key.same_worker(&keys.current().unwrap()));
        h.quiesce();
        until(|| h.quiesced());
        let driver = driver.shutdown();
        let exit = authority.shutdown().unwrap();
        assert!(driver.exit.is_ok() && exit.pool.is_ok() && exit.owner.errors.is_empty());
        node.db.view().unwrap().verify_tip().unwrap();
    }
}

#[test]
fn retained_control_capacity_cannot_starve_the_driver() {
    let node = Node::new(0);
    let authority =
        Authority::spawn(start(&node, false), settings()).unwrap_or_else(|e| panic!("{}", e.error));
    let h = authority.handle();
    let tickets: Vec<_> = (0..64)
        .map(|_| h.session().start_production().unwrap())
        .collect();
    until(|| h.controls().applied == 64);
    assert!(matches!(
        h.session().stop_production(),
        Err(RequestError::Busy)
    ));
    let clock = Manual(Arc::new(Mutex::new((1000, Instant::now()))));
    let driver = Driver::spawn(h.clone(), clock.clone(), settings().poll).unwrap();
    until(|| {
        if let Some(d) = h.status().deadline {
            let mut t = lock(&clock.0);
            t.0 += d.saturating_duration_since(t.1).as_millis() as u64;
            t.1 = d;
        }
        h.status().completed > 0
    });
    assert_eq!(h.usage().controls, 64);
    assert!(matches!(
        output(&authority),
        Output::Attempt(production::Completed {
            outcome: production::Outcome::Produced { .. },
            ..
        })
    ));
    h.quiesce();
    until(|| h.quiesced());
    assert!(driver.shutdown().exit.is_ok());
    assert!(authority.shutdown().unwrap().pool.is_ok());
    drop(tickets);
    assert_eq!(h.usage(), Usage::default());
}

#[test]
fn accepted_advance_survives_driver_join_and_is_observed_without_replay() {
    let node = Node::new(0);
    let mut c = start(&node, false);
    let shared = Shared::new(c.status());
    let h = Handle(shared.clone());
    let driver = Driver::spawn(
        h.clone(),
        Manual(Arc::new(Mutex::new((1000, Instant::now())))),
        settings().poll,
    )
    .unwrap();
    lock(&shared.control).ready = true;
    shared.wake.notify_all();
    until(|| lock(&shared.control).commands.len() == 1);
    let mut report = driver.shutdown();
    assert!(report.exit.is_ok());
    assert!(matches!(report.pending, Some(Observed::Pending(_))));
    assert_eq!(h.usage().driver, 1);
    let mut job = lock(&shared.control).commands.pop_front().unwrap();
    job.executing = true;
    let result = execute(&mut c, &job);
    job.reply.complete(result);
    drop(job);
    report.poll();
    assert!(matches!(
        report.pending,
        Some(Observed::Known {
            sequence: 1,
            generation: 1,
            result: Ok(Reply::Advanced(_))
        })
    ));
    assert_eq!(h.usage(), Usage::default());
    assert!(lock(&shared.control).commands.is_empty());
    assert!(c.shutdown().pool.is_ok());
}

#[test]
fn executing_unwind_closes_admission_before_unknown_and_classifies_queued_work() {
    let node = Node::new(0);
    let c = start(&node, false);
    let shared = Shared::new(c.status());
    let h = Handle(shared.clone());
    let executing = h.session().start_production().unwrap();
    let queued = h.session().stop_production().unwrap();
    let task = thread::spawn(move || {
        let _guard = ServiceGuard(shared.clone());
        let mut job = lock(&shared.control).commands.pop_front().unwrap();
        job.executing = true;
        panic!("injected accepted request unwind");
    });
    assert!(task.join().is_err());
    assert_eq!(executing.try_result(), Some(Err(RequestError::Unknown)));
    assert_eq!(queued.try_result(), Some(Err(RequestError::Closed)));
    assert!(matches!(
        h.session().start_production(),
        Err(RequestError::Closed)
    ));
    assert_eq!(
        (
            h.controls().submitted,
            h.controls().applied,
            h.controls().rejected,
            h.controls().pending
        ),
        (2, 2, 2, 0)
    );
    assert_eq!(h.usage().controls, 2);
    drop((executing, queued));
    assert_eq!(h.usage(), Usage::default());
    assert!(c.shutdown().pool.is_ok());
}

#[test]
fn queued_old_commands_cannot_change_a_replacement_session() {
    let node = Node::new(0);
    let mut c = start(&node, true);
    let shared = Shared::new(c.status());
    let h = Handle(shared.clone());
    let old = h.session();
    let restart = old.restart().unwrap();
    let start = old.start_production().unwrap();
    let stop = old.stop_production().unwrap();
    let mut job = lock(&shared.control).commands.pop_front().unwrap();
    job.executing = true;
    job.reply.complete(execute(&mut c, &job));
    drop(job);
    let mut job = lock(&shared.control).commands.pop_front().unwrap();
    job.executing = true;
    job.reply.complete(execute(&mut c, &job));
    drop(job);
    assert_eq!(restart.try_result(), Some(Ok(Reply::Applied)));
    assert_eq!(start.try_result(), Some(Err(RequestError::Retiring)));
    c.driven_step(None, Instant::now()).unwrap();
    assert_eq!(c.status().session_generation, 2);
    let mut job = lock(&shared.control).commands.pop_front().unwrap();
    job.executing = true;
    job.reply.complete(execute(&mut c, &job));
    drop(job);
    assert_eq!(
        stop.try_result(),
        Some(Err(RequestError::Stale {
            requested: 1,
            current: 2
        }))
    );
    assert!(c.status().producing);
    assert!(c.shutdown().pool.is_ok());
}
