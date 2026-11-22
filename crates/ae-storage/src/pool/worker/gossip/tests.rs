use super::*;
use crate::pool::tests::{Temp, config, database, tx};
use ae_mempool::pool::PushOutcome;
use std::sync::mpsc;

#[test]
fn unavailable_cache_fails_open_and_reset_recovers_a_poisoned_cache() {
    let temp = Temp::new();
    let db = database(&temp);
    let cache = GossipCache::default().with_timeout(Duration::from_millis(5));
    let worker = Worker::spawn_with_cache(
        db,
        NetworkProfile::mainnet(),
        config(),
        Settings {
            origins_interval: None,
            ..Default::default()
        },
        cache.clone(),
    )
    .unwrap();
    let client = worker.client();
    let held = cache.clone();
    let (ready, started) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let holder = thread::spawn(move || {
        let instance = held.instance().unwrap();
        let _guard = instance.lock().unwrap();
        ready.send(()).unwrap();
        wait.recv().unwrap();
    });
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let start = Instant::now();
    assert!(matches!(
        client.receive(tx()).unwrap().wait().unwrap().outcome,
        PushOutcome::Accepted { .. }
    ));
    assert_eq!(cache.stats(), Err(Error::CacheUnavailable));
    assert_eq!(cache.reset(), Err(Error::CacheUnavailable));
    assert!(start.elapsed() < Duration::from_secs(10));
    release.send(()).unwrap();
    holder.join().unwrap();
    // A timed-out Rust lock attempt has no queued late cache mutation.
    assert_eq!(cache.stats().unwrap(), Default::default());
    let broken = cache.clone();
    assert!(
        thread::spawn(move || {
            let instance = broken.instance().unwrap();
            let _guard = instance.lock().unwrap();
            panic!("cache failure");
        })
        .join()
        .is_err()
    );
    assert_eq!(cache.stats(), Err(Error::CacheUnavailable));
    assert!(matches!(
        client.receive(tx()).unwrap().wait(),
        Err(Error::Service(Failure::Policy(
            ae_mempool::pool::Error::AlreadyKnown
        )))
    ));
    cache.reset().unwrap();
    assert_eq!(cache.stats().unwrap(), Default::default());
    assert!(client.receive(tx()).unwrap().wait().is_err());
    assert_eq!(
        client.receive(tx()).unwrap().wait().unwrap().outcome,
        PushOutcome::Cached
    );
    worker.shutdown().unwrap();
}

#[test]
fn concurrent_received_duplicates_have_one_admission_and_one_notification() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = Worker::spawn(
        db,
        NetworkProfile::mainnet(),
        config(),
        Settings {
            origins_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    let client = worker.client();
    let cursor = client.event_cursor();
    let barrier = Arc::new(std::sync::Barrier::new(16));
    let threads: Vec<_> = (0..16)
        .map(|_| {
            let client = client.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                client.receive(tx()).unwrap().wait().unwrap().outcome
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, PushOutcome::Accepted { .. }))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == PushOutcome::Cached)
            .count(),
        15
    );
    let stats = client.gossip_cache().stats().unwrap();
    assert_eq!(stats.hit, 15.into());
    assert_eq!(stats.miss, 1.into());
    assert_eq!(client.events_after(&cursor, 10).unwrap().events.len(), 1);
    worker.shutdown().unwrap();
}

#[test]
fn managed_cache_has_exclusive_generations_preserves_configuration_and_rejects_stale_handles() {
    let (cache, mut controller) = GossipCache::managed(3, 8).unwrap();
    let hash = tx().hash();
    assert!(!cache.check(hash));
    assert!(!cache.check(hash));
    assert_eq!(cache.stats(), Err(Error::CacheUnavailable));
    assert_eq!(cache.reset(), Err(Error::CacheUnavailable));
    let first = controller.start().unwrap();
    let stale = first.handle();
    assert!(matches!(
        controller.start(),
        Err(LifecycleError::AlreadyRunning)
    ));
    assert!(!cache.check(hash));
    assert!(cache.check(hash));
    cache.reconfigure(1).unwrap();
    first.shutdown();
    let second = controller.start().unwrap();
    assert_eq!(cache.instance().unwrap().lock().unwrap().capacity(), 1);
    assert_eq!(cache.stats().unwrap(), Default::default());
    drop(first);
    stale.shutdown();
    assert!(!stale.is_running() && second.is_running());
    assert!(!cache.check(hash));
    assert!(cache.check(hash));
    controller.shutdown();
    assert!(!second.is_running());
    assert!(!cache.check(hash));
    assert!(matches!(controller.start(), Err(LifecycleError::Closed)));
}

#[test]
fn captured_cache_operation_cannot_mutate_a_replacement_incarnation() {
    let (cache, mut controller) = GossipCache::managed(3, 8).unwrap();
    let first = controller.start().unwrap();
    let (captured, ready) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let reader = cache.clone();
    let hash = tx().hash();
    assert!(!cache.check(hash));
    let retired = cache.instance().unwrap();
    let operation = thread::spawn(move || {
        // This is the exact capture/lock/check sequence used by check(), with
        // a controlled delay after capture, before the old cache is locked.
        let instance = reader.instance().unwrap();
        captured.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(10)).unwrap();
        reader.check_instance(&instance, hash)
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    first.shutdown();
    let second = controller.start().unwrap();
    release.send(()).unwrap();
    assert!(!operation.join().unwrap());
    assert_eq!(retired.lock().unwrap().stats().hit, 0.into());
    assert_eq!(cache.stats().unwrap(), Default::default());
    assert!(!cache.check(hash));
    assert!(second.is_running());
    drop(controller);
    assert!(!second.is_running());
    assert_eq!(cache.stats(), Err(Error::CacheUnavailable));
}

#[test]
fn poisoned_managed_incarnation_is_dead_and_replacement_uses_new_storage() {
    let (cache, mut controller) = GossipCache::managed(3, 8).unwrap();
    let first = controller.start().unwrap();
    let broken = cache.instance().unwrap();
    assert!(
        thread::spawn(move || {
            let _held = broken.lock().unwrap();
            panic!("cache incarnation failure");
        })
        .join()
        .is_err()
    );
    assert!(!first.is_running());
    assert_eq!(cache.stats(), Err(Error::CacheUnavailable));
    assert_eq!(cache.reset(), Err(Error::CacheUnavailable));
    assert!(!cache.check(tx().hash()));
    first.shutdown();
    let second = controller.start().unwrap();
    drop(first);
    assert!(second.is_running());
    assert_eq!(cache.stats().unwrap(), Default::default());
    assert!(!cache.check(tx().hash()));
    assert!(cache.check(tx().hash()));
}

#[test]
fn admission_uses_current_cache_and_downtime_only_bypasses_the_duplicate_filter() {
    let temp = Temp::new();
    let db = database(&temp);
    let (cache, mut controller) = GossipCache::managed(3, 8).unwrap();
    let worker = Worker::spawn_with_cache(
        db,
        NetworkProfile::mainnet(),
        config(),
        Settings {
            origins_interval: None,
            ..Default::default()
        },
        cache.clone(),
    )
    .unwrap();
    let client = worker.client();
    assert!(matches!(
        client.receive(tx()).unwrap().wait().unwrap().outcome,
        PushOutcome::Accepted { .. }
    ));
    let first = controller.start().unwrap();
    assert!(matches!(
        client.receive(tx()).unwrap().wait(),
        Err(Error::Service(Failure::Policy(
            ae_mempool::pool::Error::AlreadyKnown
        )))
    ));
    assert_eq!(
        client.receive(tx()).unwrap().wait().unwrap().outcome,
        PushOutcome::Cached
    );
    first.shutdown();
    let second = controller.start().unwrap();
    assert!(matches!(
        client.receive(tx()).unwrap().wait(),
        Err(Error::Service(Failure::Policy(
            ae_mempool::pool::Error::AlreadyKnown
        )))
    ));
    assert_eq!(
        cache.stats().unwrap(),
        Stats {
            hit: 0.into(),
            miss: 1.into()
        }
    );
    assert_eq!(
        client.receive(tx()).unwrap().wait().unwrap().outcome,
        PushOutcome::Cached
    );
    drop(second);
    assert!(client.receive(tx()).unwrap().wait().is_err());
    assert!(client.info().running);
    worker.shutdown().unwrap();
}

#[test]
fn managed_cache_lifecycle_matches_original_registered_process() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_cache_owner.json"
    ))
    .unwrap();
    let hash = |n: u64| {
        let mut bytes = [0; 32];
        bytes[24..].copy_from_slice(&n.to_be_bytes());
        ae_types::Hash256(bytes)
    };
    for scenario in fixture["rows"].as_array().unwrap() {
        let (cache, mut controller) =
            GossipCache::managed(scenario["capacity"].as_u64().unwrap() as usize, 100_000).unwrap();
        let mut owner = None;
        for row in scenario["steps"].as_array().unwrap() {
            match row["op"].as_str().unwrap() {
                "absent" => {
                    assert_eq!(cache.check(hash(9)), row["hit"].as_bool().unwrap());
                    assert_eq!(cache.stats(), Err(Error::CacheUnavailable));
                }
                "start" => {
                    owner = Some(controller.start().unwrap());
                }
                "check" => assert_eq!(
                    cache.check(hash(row["hash"].as_u64().unwrap())),
                    row["hit"].as_bool().unwrap()
                ),
                "capacity" => cache
                    .reconfigure(row["capacity"].as_u64().unwrap() as usize)
                    .unwrap(),
                "reset" => cache.reset().unwrap(),
                "stop" | "crash" => {
                    drop(owner.take().unwrap());
                }
                other => panic!("unknown native cache operation {other}"),
            }
            if row.get("state").is_some() {
                let instance = cache.instance().unwrap();
                let current = instance.lock().unwrap();
                let queue: Vec<_> = current
                    .entries()
                    .map(|h| u64::from_be_bytes(h.0[24..].try_into().unwrap()))
                    .collect();
                assert_eq!(
                    serde_json::json!({"size": current.len(), "queue": queue, "hit": current.stats().hit.to_u64().unwrap().to_string(), "miss": current.stats().miss.to_u64().unwrap().to_string()}),
                    row["state"]
                );
            }
        }
        assert!(owner.is_none());
    }
}

#[test]
fn transactional_checks_preserve_fifo_and_stats_on_rollback_and_exclude_concurrent_checks() {
    use ae_mempool::gossip::Transactional;
    let cache = GossipCache::new(2, 4).unwrap();
    let a = ae_types::Hash256([1; 32]);
    let b = ae_types::Hash256([2; 32]);
    let c = ae_types::Hash256([3; 32]);
    assert!(!cache.check(a));
    assert!(!cache.check(b));
    let before = cache.stats().unwrap();
    cache.transact(Box::new(|check| {
        assert!(!check(c));
        assert!(check(b));
        false
    }));
    assert_eq!(cache.stats().unwrap(), before);
    let instance = cache.instance().unwrap();
    assert_eq!(
        instance.lock().unwrap().entries().collect::<Vec<_>>(),
        vec![a, b]
    );
    let (ready, started) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let first = cache.clone();
    let holder = thread::spawn(move || {
        first.transact(Box::new(|check| {
            assert!(!check(c));
            ready.send(()).unwrap();
            wait.recv().unwrap();
            true
        }))
    });
    started.recv_timeout(Duration::from_secs(2)).unwrap();
    let (done, completed) = mpsc::channel();
    let second = cache.clone();
    let reader = thread::spawn(move || done.send(second.check(c)).unwrap());
    assert!(completed.recv_timeout(Duration::from_millis(20)).is_err());
    release.send(()).unwrap();
    holder.join().unwrap();
    assert!(completed.recv_timeout(Duration::from_secs(2)).unwrap());
    reader.join().unwrap();
    assert_eq!(
        instance.lock().unwrap().entries().collect::<Vec<_>>(),
        vec![b, c]
    );
    assert_eq!(cache.stats().unwrap().miss, 3.into());
    assert_eq!(cache.stats().unwrap().hit, 1.into());
    // A callback with no hash lookup never holds the cache lock.
    cache.transact(Box::new(|_| {
        cache.reset().unwrap();
        cache.reconfigure(3).unwrap();
        true
    }));
    assert_eq!(cache.stats().unwrap(), Default::default());
}
#[test]
fn cache_transaction_retirement_never_overwrites_the_replacement_instance() {
    use ae_mempool::gossip::Transactional;
    let (cache, mut controller) = GossipCache::managed(2, 4).unwrap();
    let owner = controller.start().unwrap();
    let a = ae_types::Hash256([1; 32]);
    let b = ae_types::Hash256([2; 32]);
    let (ready, started) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let held = cache.clone();
    let task = thread::spawn(move || {
        held.transact(Box::new(|check| {
            assert!(!check(a));
            assert!(!check(b));
            ready.send(()).unwrap();
            wait.recv().unwrap();
            true
        }))
    });
    started.recv_timeout(Duration::from_secs(2)).unwrap();
    owner.shutdown();
    let replacement = controller.start().unwrap();
    assert!(!cache.check(a));
    release.send(()).unwrap();
    task.join().unwrap();
    assert_eq!(cache.stats().unwrap().miss, 1.into());
    assert_eq!(cache.stats().unwrap().hit, 0.into());
    assert!(!cache.check(b));
    assert!(replacement.is_running());
    replacement.shutdown();
    cache.transact(Box::new(|check| {
        assert!(!check(a));
        assert!(!check(a));
        true
    }));
    let _replacement = controller.start().unwrap();
    assert_eq!(cache.stats().unwrap(), Default::default());
}
#[test]
fn transactional_cache_timeout_fails_open_without_a_late_write() {
    use ae_mempool::gossip::Transactional;
    let cache = GossipCache::default().with_timeout(Duration::from_millis(5));
    let instance = cache.instance().unwrap();
    let guard = instance.lock().unwrap();
    let start = Instant::now();
    cache.transact(Box::new(|check| {
        for _ in 0..100 {
            assert!(!check(ae_types::Hash256([1; 32])));
        }
        true
    }));
    assert!(start.elapsed() < Duration::from_millis(200));
    drop(guard);
    assert_eq!(cache.stats().unwrap(), Default::default());
    assert!(!cache.check(ae_types::Hash256([1; 32])));
}

#[test]
fn reset_and_reconfigure_follow_the_committed_cache_transaction() {
    use ae_mempool::gossip::Transactional;
    let (cache, mut controller) = GossipCache::managed(2, 4).unwrap();
    let owner = controller.start().unwrap();
    let (ready, started) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let held = cache.clone();
    let first = thread::spawn(move || {
        held.transact(Box::new(|check| {
            assert!(!check(ae_types::Hash256([1; 32])));
            ready.send(()).unwrap();
            wait.recv().unwrap();
            true
        }))
    });
    started.recv_timeout(Duration::from_secs(2)).unwrap();
    let later = cache.clone();
    let (done, completed) = mpsc::channel();
    let writer = thread::spawn(move || {
        later.reconfigure(1).unwrap();
        later.reset().unwrap();
        done.send(()).unwrap();
    });
    assert!(completed.recv_timeout(Duration::from_millis(20)).is_err());
    release.send(()).unwrap();
    first.join().unwrap();
    completed.recv_timeout(Duration::from_secs(2)).unwrap();
    writer.join().unwrap();
    assert_eq!(cache.stats().unwrap(), Default::default());
    assert_eq!(cache.instance().unwrap().lock().unwrap().capacity(), 1);
    owner.shutdown();
    let _replacement = controller.start().unwrap();
    assert_eq!(cache.instance().unwrap().lock().unwrap().capacity(), 1);
    assert_eq!(cache.stats().unwrap(), Default::default());
}
