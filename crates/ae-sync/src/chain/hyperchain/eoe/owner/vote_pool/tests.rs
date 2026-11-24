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
async fn owner_observing_worker_budget_exhaustion_keeps_the_consensus_runtime_and_hub() {
    for exhausted_by_vote in [false, true] {
        let node = Node::new(1);
        let source = Source::new(
            node.db.clone(),
            node.config.identity.clone(),
            node.config.profile.network.clone(),
            Default::default(),
        )
        .unwrap();
        let mut owner = Owner::start(
            Inputs {
                source,
                database: node.db.clone(),
                publications: votes::Publications::new(&node.db, Default::default()).unwrap(),
                gossip: Default::default(),
                parent: None,
            },
            Config {
                eoe: super::super::super::Config {
                    host: local_host::Config {
                        network: node.config.profile.network.clone(),
                        election_contract: node.config.profile.contracts.election,
                        parent_epoch_length: 5.into(),
                        parent_start_height: 1.into(),
                        limits: Default::default(),
                    },
                    block_time: 10000.into(),
                    coordinator: Default::default(),
                    runtime: Default::default(),
                    epochs: Default::default(),
                },
                votes: Default::default(),
                pool: Default::default(),
                calls: Default::default(),
            },
            Default::default(),
            node.config.profile.order.clone(),
        )
        .unwrap();
        let (group, _clock) = Group::controlled();
        owner.votes.group = group.clone();
        let eoe = owner.runtime_client();
        let hub = eoe.independent_pool().unwrap().clone();
        let worker = owner.votes();
        let binding = owner.vote_binding();
        // Model a successful conductor preflight, followed by another owner
        // spending the shared budget before the inner owner polls it again.
        owner.poll_votes().unwrap();
        for _ in 0..5 {
            assert!(group.try_acquire().unwrap().record());
        }
        if exhausted_by_vote {
            owner.request_vote_restart(1).unwrap();
            within(async {
                while !owner.votes.worker.as_ref().unwrap().is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await;
        } else {
            assert!(!group.try_acquire().unwrap().record());
        }
        assert_eq!(owner.step(), Err(Error::VoteRestartLimit));
        assert_eq!(owner.step(), Err(Error::VoteRestartLimit));
        assert_eq!(group.attempts(), 6);
        assert!(eoe.same_runtime(&owner.runtime_client()) && !eoe.is_closed());
        assert!(!hub.is_closed());
        assert!(binding.is_closed() && !worker.info().accepting);
        assert_eq!(owner.shutdown().errors, vec![Error::VoteRestartLimit]);
        assert!(worker.info().joined);
        assert!(eoe.is_closed());
    }
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
