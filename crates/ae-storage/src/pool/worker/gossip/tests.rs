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
        let _guard = held.cache.lock().unwrap();
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
            let _guard = broken.cache.lock().unwrap();
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
