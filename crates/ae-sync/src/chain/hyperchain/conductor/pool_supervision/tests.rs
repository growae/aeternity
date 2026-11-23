use super::*;
use crate::test_support::hyperchain::Node;
use std::{
    sync::{atomic::Ordering, mpsc},
    time::Duration,
};

fn fixture(maximum: usize) -> (Node, Pool) {
    let node = Node::new(1);
    let pool = Pool::start(
        node.db.clone(),
        node.config.profile.network.clone(),
        pool::Config {
            consensus: ae_protocol::ConsensusType::Pos,
            ..Default::default()
        },
        pool::worker::Settings {
            origins_interval: None,
            ..Default::default()
        },
        Default::default(),
        maximum,
    )
    .unwrap();
    (node, pool)
}
fn until(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < end);
        thread::yield_now();
    }
}
#[test]
fn bounded_retirement_outbox_preserves_the_report_and_constructor_holds_the_shared_permit() {
    let (_node, mut pool) = fixture(1);
    let old = pool.client();
    let group = pool.group.clone();
    let binding = pool.binding();
    let (started, entered) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    pool.restore_gate = Some((started, wait));
    // An unexpected permanent worker stop is detected without an administrative request.
    pool.worker.as_ref().unwrap().request_shutdown();
    until(|| {
        pool.poll(|_| Ok(())).unwrap();
        old.info().joined
    });
    assert!(binding.current().is_none() && pool.starting.is_none());
    assert_eq!(group.attempts(), 0);
    assert!(
        matches!(pool.take_notice(), Some(Notice::Retired { generation: 1, worker }) if worker.info.joined && worker.result.is_ok())
    );
    pool.poll(|_| panic!("retirement reported twice")).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    for _ in 0..10 {
        pool.poll(|_| panic!("old worker already joined")).unwrap();
    }
    assert!(group.try_acquire().is_none());
    assert!(binding.current().is_none() && !binding.is_closed());
    release.send(()).unwrap();
    until(|| {
        pool.poll(|_| panic!("duplicate retirement")).unwrap();
        pool.status().generation == 2
    });
    assert!(group.try_acquire().is_some());
    assert!(binding.current().unwrap().client.info().accepting);
    assert!(!old.same_worker(&pool.client()));
    assert!(matches!(
        pool.take_notice(),
        Some(Notice::Restarted { generation: 2 })
    ));
    assert!(pool.shutdown().unwrap().joined && binding.is_closed());
    assert_eq!(pool.poll(|_| Ok(())), Err(Error::Closed));
}

#[test]
fn failed_restore_constructors_consume_the_common_budget_and_do_not_repeat_retirement() {
    let (_node, mut pool) = fixture(2);
    let (group, clock) = Group::controlled();
    pool.set_group(group.clone());
    pool.settings.requests = 0;
    pool.request_restart(1).unwrap();
    let mut reports = Vec::new();
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let result = pool.poll(|_| Ok(()));
        reports.extend(pool.take_notices());
        if result == Err(Error::PoolRestartLimit) {
            break;
        }
        result.unwrap();
        assert!(Instant::now() < end);
        thread::yield_now();
    }
    assert_eq!(
        reports
            .iter()
            .filter(|n| matches!(n, Notice::Retired { .. }))
            .count(),
        1
    );
    assert_eq!(
        reports
            .iter()
            .filter(|n| matches!(
                n,
                Notice::StartFailed {
                    error: pool::worker::Error::InvalidSettings,
                    ..
                }
            ))
            .count(),
        5
    );
    assert_eq!(group.attempts(), 6);
    clock.store(11, Ordering::Release);
    pool.settings.requests = 128;
    assert_eq!(pool.poll(|_| Ok(())), Err(Error::PoolRestartLimit));
    assert_eq!(pool.request_restart(1), Err(ControlError::Closed));
    assert!(pool.shutdown().unwrap().joined);
}

#[test]
fn shutdown_joins_an_unpublished_constructor_and_never_revives_the_binding() {
    let (_node, mut pool) = fixture(2);
    let (started, entered) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    pool.restore_gate = Some((started, wait));
    let group = pool.group.clone();
    let binding = pool.binding();
    pool.request_restart(1).unwrap();
    until(|| {
        pool.poll(|_| Ok(())).unwrap();
        pool.starting.is_some()
    });
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let ending = thread::spawn(move || {
        let info = pool.shutdown().unwrap();
        (info, pool.take_notices())
    });
    until(|| binding.is_closed());
    assert!(!ending.is_finished() && group.try_acquire().is_none());
    release.send(()).unwrap();
    let (info, notices) = ending.join().unwrap();
    assert!(info.joined && !info.accepting);
    assert!(binding.is_closed() && binding.current().is_none());
    assert!(matches!(
        notices.as_slice(),
        [Notice::Retired { generation: 1, .. }]
    ));
    assert!(group.try_acquire().is_some());
}
