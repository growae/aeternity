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
async fn actual_key_exits_preserve_peer_identity_and_share_all_worker_restart_budget() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory = Directory(
        std::env::temp_dir().join(format!("ae-key-supervisor-budget-{}", std::process::id())),
    );
    std::fs::create_dir(&directory.0).unwrap();
    let mut node = Node::new();
    node.stop_worker();
    let mut config = settings();
    config.keys = Some(crate::keys::Config::new(
        directory.0.join("keys"),
        b"synthetic supervisor password".to_vec(),
    ));
    let (mut handle, service) = create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        config,
        Default::default(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    ready(&mut handle).await;
    let binding = handle.bindings().keys.unwrap();
    let original = binding.current().unwrap();
    let peer = original.info().keys.peer;
    for (index, child) in [Child::Keys, Child::Admission, Child::GarbageCollection]
        .into_iter()
        .cycle()
        .take(6)
        .enumerate()
    {
        let generations = |s: Status| [s.keys_generation, s.admission_generation, s.gc_generation];
        let mut expected = generations(handle.status());
        let child_index = index % 3;
        stopped_child(&handle, child, expected[child_index]).await;
        if index == 5 {
            break;
        }
        expected[child_index] += 1;
        within(async {
            while generations(handle.status()) != expected {
                assert!(handle.status().running, "{:?}", handle.status());
                handle.changed().await.unwrap();
            }
        })
        .await;
        assert_eq!(binding.current().unwrap().info().keys.peer, peer);
        assert_eq!(handle.status().restart_attempts, index as u64 + 1);
    }
    let status = within(task).await.unwrap();
    assert_eq!(status.error, Some(Error::RestartLimit));
    assert_eq!(status.restart_attempts, 6);
    assert_eq!(
        (
            status.keys_generation,
            status.admission_generation,
            status.gc_generation
        ),
        (3, 3, 2)
    );
    assert!(status.joined && binding.is_closed() && !original.info().running);
    assert!(matches!(
        original.peer_identity(),
        Err(crate::keys::Error::Stopped)
    ));
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

#[tokio::test]
async fn actual_routing_exits_retain_parent_cache_and_share_the_native_worker_budget() {
    let mut node = Node::new();
    node.stop_worker();
    let mut config = settings();
    config.routing = Some(Default::default());
    let (mut handle, service) = create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        config,
        Default::default(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    ready(&mut handle).await;
    let binding = handle.bindings().routing.unwrap();
    let old = binding.current().unwrap();
    let key = routing::Key {
        name: vec![routing::Part::atom("ae"), routing::Part::atom("epoch")],
        datapoint: routing::Part::atom("value"),
    };
    old.add(key.clone(), routing::Destination::Send)
        .unwrap()
        .await
        .unwrap();
    for (i, child) in [Child::Routing, Child::Admission, Child::GarbageCollection]
        .into_iter()
        .cycle()
        .take(6)
        .enumerate()
    {
        let generations = |s: Status| {
            [
                s.routing_generation,
                s.admission_generation,
                s.gc_generation,
            ]
        };
        let mut expected = generations(handle.status());
        let index = i % 3;
        stopped_child(&handle, child, expected[index]).await;
        if i == 5 {
            break;
        }
        expected[index] += 1;
        within(async {
            while generations(handle.status()) != expected {
                assert!(handle.status().running);
                handle.changed().await.unwrap();
            }
        })
        .await;
        assert_eq!(
            binding
                .current()
                .unwrap()
                .destinations(key.clone())
                .unwrap()
                .await,
            Ok(Some(vec!["send".into()]))
        );
        assert_eq!(
            old.destinations(key.clone()).unwrap().await,
            Ok(Some(vec!["send".into()]))
        );
        assert!(matches!(
            old.add(key.clone(), routing::Destination::Log),
            Err(routing::Error::Stopped)
        ));
    }
    let status = within(task).await.unwrap();
    assert_eq!(status.error, Some(Error::RestartLimit));
    assert_eq!(status.restart_attempts, 6);
    assert_eq!(
        (
            status.routing_generation,
            status.admission_generation,
            status.gc_generation
        ),
        (3, 3, 2)
    );
    assert!(status.joined && binding.is_closed());
    assert!(matches!(old.destinations(key), Err(routing::Error::Closed)));
}
#[tokio::test]
async fn routing_start_failure_and_later_child_failure_release_the_parent_table() {
    for fail_routing in [true, false] {
        let mut node = Node::new();
        node.stop_worker();
        let mut config = settings();
        let mut routing = routing::Setup::default();
        if fail_routing {
            routing.config.limits.requests = 0;
        } else {
            config.gc.as_mut().unwrap().limits.rows = 0;
        }
        config.routing = Some(routing);
        let (handle, service) = create(
            node.db.clone(),
            NetworkProfile::mainnet(),
            config,
            Default::default(),
        )
        .unwrap();
        let bindings = handle.bindings();
        let status = service.run().await;
        assert!(status.joined);
        assert_eq!(status.restart_attempts, 0);
        assert_eq!(status.routing_generation, u64::from(!fail_routing));
        assert!(bindings.routing.as_ref().unwrap().is_closed());
        if let Some(old) = bindings.routing.unwrap().snapshot().value {
            assert!(matches!(
                old.destinations(routing::Key {
                    name: vec![],
                    datapoint: routing::Part::atom("value")
                }),
                Err(routing::Error::Closed)
            ));
        }
        let (mut handle, service) = create(
            node.db.clone(),
            NetworkProfile::mainnet(),
            Config {
                routing: Some(Default::default()),
                ..settings()
            },
            Default::default(),
        )
        .unwrap();
        let task = tokio::spawn(service.run());
        ready(&mut handle).await;
        handle.shutdown();
        assert!(within(task).await.unwrap().joined);
    }
}

#[tokio::test]
async fn actual_analytics_exits_reset_stats_and_share_worker_restart_budget() {
    let mut node = Node::new();
    node.stop_worker();
    let mut config = settings();
    config.analytics = Some(analytics::Config {
        enabled: true,
        ..Default::default()
    });
    let (mut handle, service) = create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        config,
        Default::default(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    ready(&mut handle).await;
    let binding = handle.bindings().analytics.unwrap();
    let old = binding.current().unwrap();
    for (i, child) in [Child::Analytics, Child::Admission, Child::GarbageCollection]
        .into_iter()
        .cycle()
        .take(6)
        .enumerate()
    {
        let current = binding.current().unwrap();
        current
            .log(
                analytics::Announcement {
                    peer: ae_types::PeerPublicKey([7; 32]),
                    host: analytics::Host::Binary(b"localhost".to_vec()),
                    port: 3015u64.into(),
                    genesis: vec![1; 32],
                    top: vec![2; 32],
                    difficulty: 1u64.into(),
                    temporary: true,
                },
                None,
            )
            .unwrap()
            .await
            .unwrap();
        let generations = |s: Status| {
            [
                s.analytics_generation,
                s.admission_generation,
                s.gc_generation,
            ]
        };
        let mut expected = generations(handle.status());
        let index = i % 3;
        stopped_child(&handle, child, expected[index]).await;
        if i == 5 {
            break;
        }
        expected[index] += 1;
        within(async {
            while generations(handle.status()) != expected {
                assert!(handle.status().running);
                handle.changed().await.unwrap();
            }
        })
        .await;
        assert_eq!(
            binding
                .current()
                .unwrap()
                .snapshot()
                .unwrap()
                .await
                .unwrap()
                .stats
                .len(),
            usize::from(index != 0)
        );
        assert!(matches!(old.snapshot(), Err(analytics::Error::Closed)));
    }
    let status = within(task).await.unwrap();
    assert_eq!(status.error, Some(Error::RestartLimit));
    assert_eq!(status.restart_attempts, 6);
    assert_eq!(
        (
            status.analytics_generation,
            status.admission_generation,
            status.gc_generation
        ),
        (3, 3, 2)
    );
    assert!(status.joined && binding.is_closed());
}
#[tokio::test]
async fn analytics_start_failure_and_later_child_failure_unwind_started_owners() {
    for fail_analytics in [true, false] {
        let mut node = Node::new();
        node.stop_worker();
        let mut config = settings();
        let mut analytics = analytics::Config {
            enabled: true,
            ..Default::default()
        };
        if fail_analytics {
            analytics.limits.requests = 0;
        } else {
            config.gc.as_mut().unwrap().limits.rows = 0;
        }
        config.analytics = Some(analytics);
        let (handle, service) = create(
            node.db.clone(),
            NetworkProfile::mainnet(),
            config,
            Default::default(),
        )
        .unwrap();
        let bindings = handle.bindings();
        let status = service.run().await;
        assert!(status.joined);
        assert_eq!(status.restart_attempts, 0);
        assert_eq!(status.analytics_generation, u64::from(!fail_analytics));
        assert!(bindings.analytics.as_ref().unwrap().is_closed());
        if let Some(old) = bindings.analytics.unwrap().snapshot().value {
            assert!(old.info().joined);
            assert!(matches!(old.snapshot(), Err(analytics::Error::Closed)));
        }
        let (mut handle, service) = create(
            node.db.clone(),
            NetworkProfile::mainnet(),
            Config {
                analytics: Some(Default::default()),
                ..settings()
            },
            Default::default(),
        )
        .unwrap();
        let task = tokio::spawn(service.run());
        ready(&mut handle).await;
        handle.shutdown();
        assert!(within(task).await.unwrap().joined);
    }
}

#[tokio::test]
async fn actual_pool_maintenance_exits_share_restart_budget_without_erasing_pool_metadata() {
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
    let tx = crate::test_support::transaction();
    bindings
        .admission
        .current()
        .unwrap()
        .client
        .force_push(tx.clone())
        .unwrap()
        .await
        .unwrap();
    bindings
        .pool_gc
        .as_ref()
        .unwrap()
        .current()
        .unwrap()
        .shorten_ttl(tx.hash(), -9)
        .unwrap()
        .await
        .unwrap();
    let old = bindings.pool_gc.as_ref().unwrap().current().unwrap();
    for generation in 1..=6 {
        stopped_child(&handle, Child::PoolMaintenance, generation).await;
        if generation == 6 {
            break;
        }
        within(async {
            while handle.status().pool_gc_generation == generation {
                assert!(handle.status().running);
                handle.changed().await.unwrap();
            }
        })
        .await;
        assert_eq!(
            bindings
                .pool_gc
                .as_ref()
                .unwrap()
                .current()
                .unwrap()
                .ttl(tx.hash())
                .unwrap()
                .await
                .unwrap(),
            -9
        );
        assert_eq!(handle.status().admission_generation, 1);
        assert!(old.info().joined);
    }
    let status = within(task).await.unwrap();
    assert!(status.joined);
    assert_eq!(status.error, Some(Error::RestartLimit));
    assert_eq!(status.restart_attempts, 6);
    assert_eq!(
        node.db.pool_view().unwrap().members(10).unwrap(),
        vec![tx.hash()]
    );
}
#[tokio::test]
async fn pool_maintenance_partial_start_unwinds_and_standalone_timer_remains_explicit() {
    for fail_maintenance in [false, true] {
        let mut node = Node::new();
        node.stop_worker();
        let mut config = settings();
        if fail_maintenance {
            config.pool_gc.as_mut().unwrap().requests = 0;
        } else {
            config.gc.as_mut().unwrap().limits.rows = 0;
        }
        let (handle, service) = create(
            node.db.clone(),
            NetworkProfile::mainnet(),
            config,
            Default::default(),
        )
        .unwrap();
        let bindings = handle.bindings();
        let status = service.run().await;
        assert!(status.joined);
        assert_eq!(status.pool_gc_generation, u64::from(!fail_maintenance));
        assert!(bindings.pool_gc.as_ref().unwrap().is_closed());
        if let Some(c) = bindings.pool_gc.as_ref().unwrap().snapshot().value {
            assert!(c.info().joined);
        }
        let config = Config {
            pool_gc: None,
            admission: Default::default(),
            ..settings()
        };
        let (mut handle, service) = create(
            node.db.clone(),
            NetworkProfile::mainnet(),
            config,
            Default::default(),
        )
        .unwrap();
        let task = tokio::spawn(service.run());
        ready(&mut handle).await;
        assert_eq!(
            handle
                .bindings()
                .admission
                .current()
                .unwrap()
                .client
                .origins_interval(),
            Some(Duration::from_secs(30))
        );
        handle.shutdown();
        assert!(within(task).await.unwrap().joined);
    }
}
