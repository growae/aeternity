use super::super::{Progress, lock};
use super::*;
use crate::{
    chain::hyperchain::eoe::owner::production,
    test_support::hyperchain::{Node, conductor::start},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

fn until(mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < end, "supervision progress timeout");
        thread::sleep(Duration::from_millis(1));
    }
}
fn settings() -> Settings {
    Settings {
        poll: Duration::from_millis(1),
        outputs: 1,
    }
}
#[derive(Clone)]
struct Manual(Arc<Mutex<(u64, Instant)>>, Arc<AtomicBool>);
impl Clock for Manual {
    fn now(&self) -> Result<(u64, Instant), super::super::Error> {
        assert!(!self.1.load(Ordering::Acquire), "injected driver failure");
        Ok(*lock(&self.0))
    }
}
fn manual() -> Manual {
    Manual(
        Arc::new(Mutex::new((1000, Instant::now()))),
        Arc::new(AtomicBool::new(false)),
    )
}

#[test]
fn replacement_preserves_committed_unread_work_and_live_services_across_protocols() {
    for profile in [0, 2, 4] {
        let node = Node::new(profile);
        let mut conductor = start(&node, true);
        let importer = conductor.importer();
        let pool = conductor.pool();
        let keys = conductor.preset_key_binding();
        let key = keys.current().unwrap();
        let clock = manual();
        until(|| {
            let (wall, now) = *lock(&clock.0);
            let progress = conductor
                .driven_step(Some((wall, now)), Instant::now())
                .unwrap();
            assert!(conductor.output().is_none());
            if let Some(deadline) = conductor.status().deadline {
                let mut t = lock(&clock.0);
                t.0 += deadline.saturating_duration_since(t.1).as_millis() as u64;
                t.1 = deadline;
            }
            progress == Progress::Production(production::Progress::Submitted(production::Kind::Key))
        });
        let hash = conductor
            .attempt()
            .unwrap()
            .pending_block()
            .unwrap()
            .hash()
            .unwrap();
        until(|| node.db.view().unwrap().status().tip == Some(hash));
        let clocks = clock.clone();
        let mut group = Group::spawn(
            conductor,
            move |generation| {
                Ok(Manual(
                    clocks.0.clone(),
                    Arc::new(AtomicBool::new(generation == 1)),
                ))
            },
            settings(),
        )
        .unwrap_or_else(|e| panic!("{}", e.error));
        let handle = group.handle();
        let old = handle.session();
        until(|| {
            group.poll(0).unwrap();
            group.status().pending_notice
        });
        // Keeping the retired report unread prevents any replacement, while
        // the independent service settles the already committed block.
        for _ in 0..20 {
            group.poll(0).unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(handle.service_status().session_generation, 1);
        let mut stopped = None;
        until(|| {
            stopped = group.try_output();
            stopped.is_some()
        });
        let Some(Output::Stopped(stopped)) = stopped else {
            panic!("missing stopped receipt")
        };
        assert_eq!(stopped.pending_hash, Some(hash));
        assert_eq!(stopped.admission.unwrap().unwrap().accepted[0].hash, hash);
        let Some(Notice::Retired(report)) = group.take_notice() else {
            panic!("missing driver receipt")
        };
        assert!(report.exit.is_err());
        assert_eq!(report.failure, Some(super::super::Error::DriverPanicked));
        let mut replaced = 0;
        until(|| {
            group.poll(0).unwrap();
            if let Some(Output::SessionReplaced { generation: 2, .. }) = group.try_output() {
                replaced += 1;
            }
            matches!(
                group.take_notice(),
                Some(Notice::Restarted { generation: 2 })
            )
        });
        assert_eq!(replaced, 1);
        assert!(matches!(
            old.stop_production(),
            Err(RequestError::Stale {
                requested: 1,
                current: 2
            })
        ));
        assert!(pool.info().accepting && key.same_worker(&keys.current().unwrap()));
        assert_eq!(handle.service_status().consensus.attempts, 0);
        assert_eq!(group.status().attempts, 1);
        until(|| {
            group.poll(0).unwrap();
            if let Some(deadline) = handle.status().deadline {
                let mut t = lock(&clock.0);
                t.0 += deadline.saturating_duration_since(t.1).as_millis() as u64;
                t.1 = deadline;
            }
            matches!(
                group.try_output(),
                Some(Output::Attempt(production::Completed {
                    outcome: production::Outcome::Produced { .. },
                    ..
                }))
            )
        });
        handle.quiesce();
        until(|| handle.quiesced());
        let exit = group.shutdown();
        assert!(exit.report.driver.unwrap().exit.is_ok());
        let service = exit.service.unwrap();
        assert!(service.pool.is_ok() && service.owner.errors.is_empty());
        assert!(matches!(
            importer.import_block(vec![], 0),
            Err(crate::chain::Error::Closed)
        ));
        node.db.view().unwrap().verify_tip().unwrap();
    }
}

#[test]
fn constructor_unwind_and_failures_consume_the_same_inclusive_restart_budget() {
    for (boundary, allowed) in [(50, false), (51, true)] {
        let node = Node::new(0);
        let calls = Arc::new(AtomicUsize::new(0));
        let captured = calls.clone();
        let clock = manual();
        clock.1.store(true, Ordering::Release);
        let mut group = Group::spawn(
            start(&node, false),
            move |_| {
                let n = captured.fetch_add(1, Ordering::AcqRel);
                if n == 0 || n == 6 {
                    Ok(clock.clone())
                } else if n == 2 {
                    panic!("injected clock constructor failure")
                } else {
                    Err("injected clock constructor error".into())
                }
            },
            settings(),
        )
        .unwrap_or_else(|e| panic!("{}", e.error));
        let mut failed = 0;
        until(|| {
            group.poll(0).unwrap();
            while group.try_output().is_some() {}
            if let Some(Notice::StartFailed { .. }) = group.take_notice() {
                failed += 1;
            }
            failed == 5
        });
        assert_eq!(calls.load(Ordering::Acquire), 6);
        assert_eq!(group.status().attempts, 5);
        group.poll(boundary).unwrap();
        if allowed {
            assert_eq!(group.status().phase, Phase::Starting);
            group.poll(boundary).unwrap();
            assert_eq!(calls.load(Ordering::Acquire), 7);
            assert!(matches!(
                group.take_notice(),
                Some(Notice::Restarted { generation: 2 })
            ));
        } else {
            assert_eq!(group.status().failure, Some(Error::RestartLimit));
            assert_eq!(group.poll(boundary), Err(Error::RestartLimit));
            assert_eq!(calls.load(Ordering::Acquire), 6);
        }
        assert_eq!(group.poll(boundary - 1), Err(Error::Time));
        let exit = group.shutdown();
        assert!(exit.service.unwrap().pool.is_ok());
        assert_eq!(exit.report.status.attempts, 6);
    }
}

#[test]
fn shutdown_retains_unread_failure_notices_and_joined_current_driver() {
    let node = Node::new(0);
    let c = start(&node, false);
    let pool = c.pool();
    let clock = manual();
    clock.1.store(true, Ordering::Release);
    let mut group = Group::spawn(c, move |_| Ok(clock.clone()), settings())
        .unwrap_or_else(|e| panic!("{}", e.error));
    until(|| {
        group.poll(0).unwrap();
        group.status().pending_notice
    });
    let exit = group.shutdown();
    assert!(matches!(
        exit.report.pending,
        Some(Notice::Retired(DriverReport { exit: Err(_), .. }))
    ));
    assert!(exit.report.driver.is_none() && exit.report.restart.is_none());
    assert!(exit.service.unwrap().pool.is_ok() && !pool.info().accepting);
}

#[test]
fn service_failure_is_terminal_and_never_becomes_a_fresh_production_session() {
    let node = Node::new(0);
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = calls.clone();
    let mut group = Group::spawn(
        start(&node, false),
        move |_| {
            captured.fetch_add(1, Ordering::AcqRel);
            Ok(manual())
        },
        settings(),
    )
    .unwrap_or_else(|e| panic!("{}", e.error));
    let handle = group.handle();
    handle.request_shutdown();
    until(|| group.service_is_finished());
    assert_eq!(group.poll(0), Err(Error::ServiceStopped));
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(group.status().attempts, 0);
    let exit = group.shutdown();
    assert!(exit.service.unwrap().pool.is_ok());
    assert!(exit.report.driver.unwrap().exit.is_ok());
}

#[test]
fn unknown_retained_control_result_prevents_replacement_and_is_not_replayed() {
    let node = Node::new(0);
    let mut group = Group::spawn(start(&node, false), |_| Ok(manual()), settings())
        .unwrap_or_else(|e| panic!("{}", e.error));
    // The receipt layer separately exercises an actual accepted-job unwind.
    // This boundary must never reinterpret that receipt as a retryable drive.
    let mut driver = group.driver.take().unwrap().shutdown();
    driver.pending = Some(Observed::Known {
        sequence: 17,
        generation: 1,
        result: Err(RequestError::Unknown),
    });
    group.retired = Some(driver);
    group.status.phase = Phase::Retiring;
    group.poll(0).unwrap();
    assert_eq!(
        group.status().failure,
        Some(Error::Control(RequestError::Unknown))
    );
    let exit = group.shutdown();
    assert!(matches!(
        exit.report.pending,
        Some(Notice::Retired(DriverReport {
            pending: Some(Observed::Known {
                sequence: 17,
                result: Err(RequestError::Unknown),
                ..
            }),
            ..
        }))
    ));
    assert_eq!(exit.report.status.generation, 1);
    assert!(exit.service.unwrap().pool.is_ok());
}

#[test]
fn shutdown_retains_an_accepted_restart_blocked_by_unread_production_results() {
    let node = Node::new(0);
    let clock = manual();
    let clocks = clock.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let mut group = Group::spawn(
        start(&node, true),
        move |_| {
            seen.fetch_add(1, Ordering::AcqRel);
            Ok(clocks.clone())
        },
        settings(),
    )
    .unwrap_or_else(|e| panic!("{}", e.error));
    let handle = group.handle();
    until(|| {
        group.poll(0).unwrap();
        if let Some(deadline) = handle.status().deadline {
            let mut t = lock(&clock.0);
            t.0 += deadline.saturating_duration_since(t.1).as_millis() as u64;
            t.1 = deadline;
        }
        handle.service_status().completed >= 2
    });
    let before = node.db.view().unwrap().status();
    clock.1.store(true, Ordering::Release);
    until(|| {
        group.poll(0).unwrap();
        group.status().pending_notice
    });
    let retired = group.take_notice().unwrap();
    assert!(matches!(retired, Notice::Retired(_)));
    until(|| {
        group.poll(0).unwrap();
        group
            .restart
            .as_ref()
            .is_some_and(|t| t.try_result() == Some(Ok(Reply::Applied)))
    });
    assert!(handle.service_status().session_replacing);
    assert_eq!(handle.service_status().session_generation, 1);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    let exit = group.shutdown();
    assert!(matches!(
        exit.report.restart,
        Some(Observed::Known {
            generation: 1,
            result: Ok(Reply::Applied),
            ..
        })
    ));
    assert!(exit.report.driver.is_none());
    let service = exit.service.unwrap();
    assert!(service.session_retirement.is_some());
    let produced = service
        .queued_outputs
        .iter()
        .chain(service.output.iter())
        .filter(|o| {
            matches!(
                o,
                Output::Attempt(production::Completed {
                    outcome: production::Outcome::Produced { .. },
                    ..
                })
            )
        })
        .count();
    assert_eq!(produced, 2);
    assert_eq!(node.db.view().unwrap().status(), before);
    assert!(service.pool.is_ok() && service.owner.errors.is_empty());
    node.db.view().unwrap().verify_tip().unwrap();
}
