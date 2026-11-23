use super::*;
use crate::test_support::{hyperchain::Node, within};
use std::sync::atomic::Ordering;

fn fixture(index: usize) -> (Node, eoe::pool::Hub, votes::independent::Relay, Pool) {
    let node = Node::new(index);
    let hub = eoe::pool::Hub::new(node.config.profile.network.clone(), Default::default()).unwrap();
    let config = votes::Config::default();
    let publications = votes::Publications::new(&node.db, config).unwrap();
    let relay = votes::independent::Relay::new(hub.clone(), publications).unwrap();
    let pool = Pool::start(
        node.db.clone(),
        Default::default(),
        config,
        &relay,
        Default::default(),
        2,
    )
    .unwrap();
    (node, hub, relay, pool)
}

#[tokio::test]
async fn actor_panics_trigger_automatic_empty_pool_replacement_and_retain_the_joined_cause() {
    for index in [1, 3, 5] {
        let (_node, hub, relay, mut pool) = fixture(index);
        let binding = pool.binding();
        let old = pool.client();
        let before = hub.client().unwrap();
        hub.new_epoch(7.into()).unwrap();
        old.fail();
        within(async {
            while pool.status().generation == 1 {
                pool.poll(&relay).unwrap();
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(old.info().joined && before.is_closed());
        let Some(Notice::Retired {
            generation: 1,
            worker,
        }) = pool.take_notice()
        else {
            panic!("retirement");
        };
        assert_eq!(
            worker.result,
            Err(votes::Error::Worker(votes::Fault::Panicked))
        );
        assert!(matches!(
            pool.take_notice(),
            Some(Notice::Restarted { generation: 2 })
        ));
        let stats = within(binding.current().unwrap().stats().unwrap())
            .await
            .unwrap();
        assert_eq!((stats.epoch.clone(), stats.entries), (0.into(), 0));
        assert_eq!(pool.status().restart_attempts, 1);
        assert!(!hub.is_closed());
        pool.shutdown().unwrap();
        assert!(binding.is_closed());
        assert_eq!(pool.poll(&relay), Err(Error::Closed));
        assert!(hub.client().is_none());
    }
}

#[tokio::test]
async fn failed_constructors_share_the_group_permit_budget_and_retirement_is_reported_once() {
    let (_node, hub, relay, mut pool) = fixture(1);
    let (group, clock) = Group::controlled();
    pool.group = group.clone();
    let old = pool.client();
    pool.request_restart(1).unwrap();
    within(async {
        while !pool.worker.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let held = group.try_acquire().unwrap();
    pool.poll(&relay).unwrap();
    assert!(!old.info().joined && pool.notices.is_empty());
    assert_eq!(group.attempts(), 0);
    drop(held);
    pool.config.queue.requests = 0;
    let mut reports = Vec::new();
    for attempt in 1..=6 {
        assert_eq!(
            pool.poll(&relay),
            if attempt < 6 {
                Ok(())
            } else {
                Err(Error::VoteRestartLimit)
            }
        );
        reports.extend(pool.take_notices());
        assert_eq!(group.attempts(), attempt);
        assert!(hub.client().is_none());
        assert!(pool.binding().current().is_none());
    }
    assert!(old.info().joined);
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
                    error: votes::Error::Worker(votes::Fault::Configuration),
                    ..
                }
            ))
            .count(),
        5
    );
    assert!(group.exhausted());
    clock.store(11, Ordering::Release);
    pool.config.queue.requests = 128;
    assert_eq!(pool.poll(&relay), Err(Error::VoteRestartLimit));
    assert!(pool.shutdown().is_ok());
}
