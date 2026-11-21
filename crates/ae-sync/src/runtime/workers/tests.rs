use super::*;
use crate::test_support::{Node, within};

pub(super) fn settings() -> Config {
    Config {
        admission: pool::worker::Settings {
            origins_interval: None,
            ..Default::default()
        },
        poll: Duration::from_millis(5),
        ..Default::default()
    }
}
pub(super) async fn ready(handle: &mut Handle) {
    within(async {
        while !handle.status().ready {
            assert!(handle.status().running, "{:?}", handle.status());
            handle.changed().await.unwrap();
        }
    })
    .await;
}
async fn stopped_child(handle: &Handle, child: Child, generation: u64) {
    let (reply, result) = oneshot::channel();
    handle
        .commands
        .send(Restart {
            child,
            generation,
            reply,
            stop_only: true,
        })
        .await
        .unwrap();
    assert_eq!(within(result).await.unwrap(), Ok(generation));
}
#[tokio::test]
async fn actual_worker_exits_restart_only_the_failed_child_and_share_one_native_budget() {
    let mut node = Node::new();
    node.stop_worker();
    let (mut handle, service) = create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        settings(),
        Default::default(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    ready(&mut handle).await;
    let bindings = handle.bindings();
    let original = bindings.admission.current().unwrap().client;
    let original_gc = bindings.gc.as_ref().unwrap().current().unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_worker_owner.json"
    ))
    .unwrap();
    assert_eq!(fixture["policy"]["strategy"], "one_for_one");
    let trace = fixture["lifecycle"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["kind"] == "restart_limit")
        .unwrap()["trace"]
        .as_array()
        .unwrap();
    let attempts = trace
        .iter()
        .filter(|e| **e == serde_json::json!(["exited", "b"]))
        .count();
    assert_eq!(attempts, 6);
    for index in 0..attempts {
        let child = if index % 2 == 0 {
            Child::Admission
        } else {
            Child::GarbageCollection
        };
        let before = handle.status();
        let generation = if child == Child::Admission {
            before.admission_generation
        } else {
            before.gc_generation
        };
        stopped_child(&handle, child, generation).await;
        within(async {
            loop {
                let status = handle.status();
                if index == attempts - 1 && !status.running {
                    break;
                }
                let next = if child == Child::Admission {
                    status.admission_generation
                } else {
                    status.gc_generation
                };
                if next > generation {
                    break;
                }
                handle.changed().await.unwrap();
            }
        })
        .await;
        if index + 1 < attempts {
            let next = handle.status();
            if child == Child::Admission {
                assert_eq!(next.gc_generation, before.gc_generation);
            } else {
                assert_eq!(next.admission_generation, before.admission_generation);
            }
        }
    }
    let status = within(task).await.unwrap();
    assert_eq!(status.error, Some(Error::RestartLimit));
    assert_eq!(status.restart_attempts, attempts as u64);
    assert_eq!(status.admission_generation + status.gc_generation, 7);
    assert!(status.joined && !original.info().running);
    assert!(original_gc.info().is_none_or(|i| !i.running));
    assert!(bindings.admission.is_closed() && bindings.gc.unwrap().is_closed());
}

#[tokio::test]
async fn failed_replacement_starts_consume_budget_without_restarting_the_other_worker() {
    let mut node = Node::new();
    node.stop_worker();
    let mut config = settings();
    config.poll = Duration::from_secs(1);
    let competing_config = config.clone();
    let (mut handle, mut service) = create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        config,
        Default::default(),
    )
    .unwrap();
    let (entered, ready_to_compete) = oneshot::channel();
    let (release, released) = oneshot::channel();
    service.restart_gate = Some((entered, released));
    let task = tokio::spawn(service.run());
    ready(&mut handle).await;
    let old = handle.bindings().admission.current().unwrap().client;
    stopped_child(&handle, Child::Admission, 1).await;
    within(ready_to_compete).await.unwrap();
    assert!(!handle.status().ready);
    assert!(handle.bindings().admission.current().is_none());
    // After the retired worker joins, a competing owner deliberately holds
    // the exact database lease to fail real replacement start attempts.
    let competing = within(async {
        loop {
            match pool::worker::Worker::spawn(
                node.db.clone(),
                NetworkProfile::mainnet(),
                competing_config.pool.clone(),
                competing_config.admission,
            ) {
                Ok(worker) => break worker,
                Err(pool::worker::Error::Service(pool::Failure::AlreadyRunning)) => {
                    tokio::task::yield_now().await
                }
                other => panic!("unexpected competing start: {:?}", other.err()),
            }
        }
    })
    .await;
    assert!(!old.info().running);
    release.send(()).unwrap();
    let status = within(task).await.unwrap();
    assert_eq!(status.error, Some(Error::RestartLimit));
    assert_eq!(status.restart_attempts, 6);
    assert_eq!((status.admission_generation, status.gc_generation), (1, 1));
    assert!(matches!(
        status.last_failure,
        Some(Error::Start {
            child: Child::Admission,
            ..
        })
    ));
    assert!(status.joined && competing.info().running);
    competing.shutdown().unwrap();
}

#[tokio::test]
async fn gossip_drains_retired_accepted_events_and_recovers_skipped_worker_generations() {
    use crate::{gossip, pool as sync_pool, test_support::transaction};
    for skipped in [false, true] {
        let mut node = Node::new();
        node.stop_worker();
        let mut config = settings();
        config.pool.policy.minimum_miner_gas_price = 1.into();
        let (mut handle, service) = create(
            node.db.clone(),
            NetworkProfile::mainnet(),
            config,
            Default::default(),
        )
        .unwrap();
        let worker_task = tokio::spawn(service.run());
        ready(&mut handle).await;
        let binding = handle.bindings().admission;
        let old = binding.current().unwrap().client;
        let (peers, incoming, service) =
            sync_pool::create_bound(node.db.clone(), binding.clone(), Default::default()).unwrap();
        let pool_task = tokio::spawn(service.run());
        let config = gossip::Config {
            feed_batch: 1,
            policy: gossip::Policy {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let (mut gossip, _incoming, dormant) = gossip::create_bound(
            node.db.clone(),
            binding.clone(),
            peers.clone(),
            incoming,
            NetworkProfile::mainnet(),
            ae_protocol::ConsensusType::Pow,
            config,
        )
        .unwrap();
        let tx = transaction();
        for _ in 0..8 {
            old.force_push(tx.clone()).unwrap().await.unwrap();
        }
        handle
            .restart(Child::Admission, 1)
            .unwrap()
            .wait()
            .await
            .unwrap();
        if skipped {
            handle
                .restart(Child::Admission, 2)
                .unwrap()
                .wait()
                .await
                .unwrap();
        }
        let replacement = binding.current().unwrap().client;
        for _ in 0..8 {
            replacement.force_push(tx.clone()).unwrap().await.unwrap();
        }
        assert!(matches!(
            old.force_push(tx),
            Err(pool::worker::Error::Stopped)
        ));
        let task = tokio::spawn(dormant.run());
        within(async {
            let expected = if skipped { 9 } else { 16 };
            while gossip.status().counts.suppressed < expected {
                gossip.changed().await.unwrap();
            }
            assert_eq!(gossip.status().counts.suppressed, expected);
        })
        .await;
        assert_eq!(gossip.status().counts.recovered, u64::from(skipped));
        assert_eq!(gossip.status().counts.gaps, u64::from(skipped));
        gossip.shutdown();
        within(task).await.unwrap();
        peers.shutdown();
        within(pool_task).await.unwrap();
        handle.shutdown();
        assert!(within(worker_task).await.unwrap().joined);
    }
}
