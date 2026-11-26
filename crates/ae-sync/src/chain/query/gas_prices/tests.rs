use super::*;
use serde_json::{Value as Json, json};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64},
        mpsc,
    },
    thread,
};

fn fixture() -> Json {
    serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_gas_cache.json"
    ))
    .unwrap()
}
fn key(n: u64) -> Hash256 {
    let mut h = [0; 32];
    h[24..].copy_from_slice(&n.to_be_bytes());
    Hash256(h)
}
fn stats(n: u64) -> Stats {
    Stats {
        minimum: Some(n.into()),
        used: n.into(),
        total: 6_000_000.into(),
    }
}
fn get(cache: &Cache, n: u64) -> Arc<Value> {
    cache
        .get_or_fill(key(n), || Ok(()), |_| Ok(stats(n)))
        .unwrap()
}
fn value(v: &Value) -> Json {
    match v.data() {
        Data::MissingState => json!(["ok", ["error", "block_not_found"]]),
        Data::Observed(s) => json!([
            "ok",
            [
                1,
                [
                    s.minimum
                        .as_ref()
                        .map(|n| serde_json::from_str::<Json>(&n.to_string()).unwrap()),
                    serde_json::from_str::<Json>(&s.used.to_string()).unwrap(),
                    serde_json::from_str::<Json>(&s.total.to_string()).unwrap()
                ]
            ]
        ]),
    }
}
fn wait(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(Instant::now() < end, "bounded test wait");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn original_cache_keeps_positive_negative_values_and_lru_access_order() {
    let f = fixture();
    let cache = Cache::new(Config {
        entries: 2,
        ..Default::default()
    })
    .unwrap();
    let calls = AtomicU64::new(0);
    let cold = cache
        .get_or_fill(
            key(10),
            || Ok(()),
            |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(gas::Error::MissingState(key(10)))
            },
        )
        .unwrap();
    assert_eq!(value(&cold), f["lru"]["cold"]["value"]);
    let negative = cache
        .get_or_fill(
            key(10),
            || Ok(()),
            |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(stats(10))
            },
        )
        .unwrap();
    assert_eq!(value(&negative), f["lru"]["still_negative"]["value"]);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    drop(cold);
    drop(negative);
    // Evict the sole negative entry before the fresh lookup. Native capture
    // explicitly removed it; the runtime interface deliberately has no put.
    drop(get(&cache, 20));
    drop(get(&cache, 30));
    let fresh = cache
        .get_or_fill(
            key(10),
            || Ok(()),
            |_| {
                Ok(Stats {
                    total: 6_000_000.into(),
                    ..Default::default()
                })
            },
        )
        .unwrap();
    assert_eq!(value(&fresh), f["lru"]["fresh"]["value"]);
    let warm = cache
        .get_or_fill(
            key(10),
            || Ok(()),
            |_| Err(gas::Error::MissingState(key(10))),
        )
        .unwrap();
    assert_eq!(value(&warm), f["lru"]["warm"]["value"]);
    drop(fresh);
    drop(warm);
    drop(get(&cache, 20));
    drop(get(&cache, 10));
    drop(get(&cache, 30));
    let state = cache.state.lock().unwrap();
    for row in f["lru"]["lru"].as_array().unwrap() {
        assert_eq!(
            state.ready.contains_key(&key(row["key"].as_u64().unwrap())),
            row["present"] == "true"
        );
    }
}

#[test]
fn original_same_key_fills_coalesce_without_serializing_different_keys() {
    let f = fixture();
    let cache = Arc::new(Cache::new(Config::default()).unwrap());
    let calls = Arc::new(AtomicU64::new(0));
    let (started, entered) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let c = cache.clone();
    let n = calls.clone();
    let owner = thread::spawn(move || {
        c.get_or_fill(
            key(1),
            || Ok(()),
            |_| {
                n.fetch_add(1, Ordering::Relaxed);
                started.send(()).unwrap();
                blocked.recv().unwrap();
                Ok(stats(10))
            },
        )
    });
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    let c = cache.clone();
    let n = calls.clone();
    let waiter = thread::spawn(move || {
        c.get_or_fill(
            key(1),
            || Ok(()),
            |_| {
                n.fetch_add(1, Ordering::Relaxed);
                Ok(stats(999))
            },
        )
    });
    wait(|| cache.info().waiters == 1);
    let other = cache
        .get_or_fill(key(2), || Ok(()), |_| Ok(stats(20)))
        .unwrap();
    let Data::Observed(other) = other.data() else {
        panic!()
    };
    assert_eq!(
        json!(["ok", other.used.to_string().parse::<u64>().unwrap()]),
        f["concurrent"]["different_key"]
    );
    release.send(()).unwrap();
    let a = owner.join().unwrap().unwrap();
    let b = waiter.join().unwrap().unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    for (actual, expected) in [&a, &b]
        .into_iter()
        .zip(f["concurrent"]["same_key"].as_array().unwrap())
    {
        let Data::Observed(s) = actual.data() else {
            panic!()
        };
        assert_eq!(
            json!(["ok", s.used.to_string().parse::<u64>().unwrap()]),
            *expected
        );
    }
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(f["concurrent"]["one_fill"], true);
    assert_eq!(cache.info().pending, 0);
    assert_eq!(cache.info().requests, 0);
}

#[test]
fn failed_cancelled_and_panicking_fills_wake_waiters_without_negative_caching() {
    let f = fixture();
    assert_eq!(f["concurrent"]["exception"], "notfound");
    assert_eq!(f["concurrent"]["retry"], json!(["ok", "retried"]));
    let cache = Arc::new(Cache::new(Config::default()).unwrap());
    for error in [
        gas::Error::Arithmetic,
        gas::Error::ResourceLimit,
        gas::Error::Backend(ae_chain::repository::Error::RecoveryRequired),
        gas::Error::MissingState(key(99)),
    ] {
        assert!(matches!(
            cache.get_or_fill(key(1), || Ok(()), |_| Err(error)),
            Err(Error::Fill(_))
        ));
        assert_eq!(cache.info().entries, 0);
        assert_eq!(cache.info().pending, 0);
    }
    let (started, entered) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let c = cache.clone();
    let owner = thread::spawn(move || {
        c.get_or_fill(
            key(1),
            || Ok(()),
            |_| {
                started.send(()).unwrap();
                blocked.recv().unwrap();
                panic!("controlled fill panic")
            },
        )
    });
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    let c = cache.clone();
    let waiter =
        thread::spawn(move || c.get_or_fill(key(1), || Ok(()), |_| panic!("duplicate fill")));
    wait(|| cache.info().waiters == 1);
    release.send(()).unwrap();
    assert!(owner.join().is_err());
    assert!(matches!(waiter.join().unwrap(), Err(Error::FillAborted)));
    assert_eq!(cache.info().pending, 0);
    let cancelled = AtomicBool::new(false);
    assert!(matches!(
        cache.get_or_fill(
            key(1),
            || if cancelled.load(Ordering::Acquire) {
                Err(super::super::super::Error::Cancelled)
            } else {
                Ok(())
            },
            |_| {
                cancelled.store(true, Ordering::Release);
                Ok(stats(1))
            }
        ),
        Err(Error::Check(super::super::super::Error::Cancelled))
    ));
    assert_eq!(cache.info().entries, 0);
    drop(get(&cache, 1));
    assert_eq!(cache.info().entries, 1);
}

#[test]
fn timed_out_fill_keeps_ownership_until_return_and_cannot_publish_late() {
    let f = fixture();
    let cache = Arc::new(
        Cache::new(Config {
            timeout: Duration::from_millis(30),
            ..Default::default()
        })
        .unwrap(),
    );
    let (started, entered) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let c = cache.clone();
    let owner = thread::spawn(move || {
        c.get_or_fill(
            key(1),
            || Ok(()),
            |deadline| {
                started.send(deadline).unwrap();
                blocked.recv().unwrap();
                Ok(stats(1))
            },
        )
    });
    let deadline = entered.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        cache.get_or_fill(key(1), || Ok(()), |_| panic!("duplicate")),
        Err(Error::Timeout)
    ));
    assert!(Instant::now() >= deadline);
    assert_eq!(f["concurrent"]["timeout"], "timeout");
    assert_eq!(cache.info().pending, 1);
    assert_eq!(cache.info().entries, 0);
    assert!(matches!(
        cache.get_or_fill(key(1), || Ok(()), |_| panic!("late duplicate")),
        Err(Error::Timeout)
    ));
    release.send(()).unwrap();
    assert!(matches!(owner.join().unwrap(), Err(Error::Timeout)));
    assert_eq!(cache.info().pending, 0);
    assert_eq!(cache.info().entries, 0);
    drop(get(&cache, 1));
    assert_eq!(cache.info().entries, 1);
    assert_eq!(
        f["concurrent"]["after_timeout"],
        json!(["ok", "after_timeout"])
    );
}

#[test]
fn eviction_and_shutdown_keep_memory_charged_until_last_reader_releases_it() {
    let bytes = weight(&Data::Observed(stats(1))).unwrap();
    let cache = Cache::new(Config {
        entries: 1,
        bytes: bytes * 2,
        ..Default::default()
    })
    .unwrap();
    let first = get(&cache, 1);
    let second = get(&cache, 2);
    assert_eq!(cache.info().retained_bytes, bytes * 2);
    assert!(matches!(
        cache.get_or_fill(key(3), || Ok(()), |_| Ok(stats(3))),
        Err(Error::Busy)
    ));
    assert_eq!(cache.info().entries, 1);
    assert_eq!(cache.info().pending, 0);
    assert!(Arc::ptr_eq(&second, &get(&cache, 2)));
    drop(first);
    let third = get(&cache, 3);
    cache.close();
    assert_eq!(cache.info().entries, 0);
    assert_eq!(cache.info().retained_bytes, bytes * 2);
    assert!(matches!(
        cache.get_or_fill(key(4), || Ok(()), |_| panic!("closed")),
        Err(Error::Closed)
    ));
    drop(second);
    assert_eq!(cache.info().retained_bytes, bytes);
    drop(third);
    assert_eq!(cache.info().retained_bytes, 0);
    let fresh = Cache::new(Config::default()).unwrap();
    assert_eq!(fresh.info().entries, 0);
}

#[test]
fn pending_fills_and_requests_are_bounded_and_close_wakes_waiters() {
    let cache = Arc::new(
        Cache::new(Config {
            fills: 1,
            requests: 2,
            ..Default::default()
        })
        .unwrap(),
    );
    let (started, entered) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let c = cache.clone();
    let owner = thread::spawn(move || {
        c.get_or_fill(
            key(1),
            || Ok(()),
            |_| {
                started.send(()).unwrap();
                blocked.recv().unwrap();
                Ok(stats(1))
            },
        )
    });
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        cache.get_or_fill(key(2), || Ok(()), |_| panic!("over fill capacity")),
        Err(Error::Busy)
    ));
    let c = cache.clone();
    let waiter = thread::spawn(move || c.get_or_fill(key(1), || Ok(()), |_| panic!("duplicate")));
    wait(|| cache.info().waiters == 1);
    assert!(matches!(
        cache.get_or_fill(key(1), || Ok(()), |_| panic!("over request capacity")),
        Err(Error::Busy)
    ));
    cache.close();
    assert!(matches!(waiter.join().unwrap(), Err(Error::Closed)));
    assert_eq!(cache.info().requests, 1);
    release.send(()).unwrap();
    assert!(matches!(owner.join().unwrap(), Err(Error::Closed)));
    assert_eq!(cache.info().requests, 0);
    assert_eq!(cache.info().retained_bytes, 0);
}

#[tokio::test]
async fn cancelled_waiter_does_not_abandon_an_owned_fill_or_release_running_job_credits() {
    use crate::blocking;
    let pool = blocking::Pool::new(2, 2, 8192).unwrap();
    let cache = Arc::new(Cache::new(Config::default()).unwrap());
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, blocked) = mpsc::channel();
    let c = cache.clone();
    let owner = pool
        .reserve(4096)
        .unwrap()
        .submit(move |cancel| {
            c.get_or_fill(
                key(1),
                || {
                    if cancel.cancelled() {
                        Err(super::super::super::Error::Cancelled)
                    } else {
                        Ok(())
                    }
                },
                |_| {
                    started.send(()).unwrap();
                    blocked.recv().unwrap();
                    Ok(stats(1))
                },
            )
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    let c = cache.clone();
    let waiter = pool
        .reserve(4096)
        .unwrap()
        .submit(move |cancel| {
            c.get_or_fill(
                key(1),
                || {
                    if cancel.cancelled() {
                        Err(super::super::super::Error::Cancelled)
                    } else {
                        Ok(())
                    }
                },
                |_| panic!("duplicate"),
            )
        })
        .unwrap();
    wait(|| cache.info().waiters == 1);
    drop(waiter);
    wait(|| cache.info().waiters == 0);
    assert_eq!(cache.info().pending, 1);
    drop(owner);
    pool.stop();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), pool.join())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), pool.join())
        .await
        .unwrap();
    assert_eq!(cache.info().pending, 0);
    assert_eq!(cache.info().entries, 0);
    assert_eq!(cache.info().requests, 0);
}

#[test]
fn native_default_capacity_and_lru_counter_rollover_preserve_membership() {
    let cache = Cache::new(Config::default()).unwrap();
    assert_eq!(cache.config.entries, 1200);
    assert_eq!(cache.config.timeout, Duration::from_secs(5));
    for n in 0..1200 {
        drop(get(&cache, n));
    }
    drop(get(&cache, 0));
    drop(get(&cache, 1200));
    {
        let state = cache.state.lock().unwrap();
        assert!(state.ready.contains_key(&key(0)));
        assert!(!state.ready.contains_key(&key(1)));
        assert_eq!(state.ready.len(), 1200);
    }
    cache.state.lock().unwrap().clock = u64::MAX;
    drop(get(&cache, 2));
    drop(get(&cache, 1201));
    let state = cache.state.lock().unwrap();
    assert!(state.ready.contains_key(&key(2)));
    assert!(!state.ready.contains_key(&key(3)));
    assert_eq!(state.ready.len(), 1200);
}

#[test]
fn a_weak_reader_upgrading_during_eviction_cannot_overdraw_cache_memory() {
    let bytes = weight(&Data::Observed(stats(1))).unwrap();
    let cache = Cache::new(Config {
        entries: 1,
        bytes,
        ..Default::default()
    })
    .unwrap();
    let first = get(&cache, 1);
    let weak = Arc::downgrade(&first);
    drop(first);
    let pin = Arc::new(Mutex::new(None));
    let held = pin.clone();
    *cache.before_evict.lock().unwrap() = Some(Box::new(move || {
        // Force an external reader to pin the allocation after the cache has
        // inspected its reference count, immediately before eviction.
        *held.lock().unwrap() = Some(weak.upgrade().unwrap());
    }));
    assert!(matches!(
        cache.get_or_fill(key(2), || Ok(()), |_| Ok(stats(2))),
        Err(Error::Busy)
    ));
    assert_eq!(cache.info().retained_bytes, bytes);
    assert_eq!(cache.info().pending, 0);
    pin.lock().unwrap().take();
    assert_eq!(cache.info().retained_bytes, 0);
    drop(get(&cache, 2));
    assert_eq!(cache.info().retained_bytes, bytes);
}
