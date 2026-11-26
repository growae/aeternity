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

// Pause a registered waiter in its caller check, finish its fill before the
// deadline, and resume the waiter after the deadline. Every worker is joined.
fn completed_before_waiter_resumes(
    kind: &str,
    context_error: Option<super::super::super::Error>,
    close: bool,
) -> (ResultValue, ResultValue, Arc<Cache>) {
    let cache = Arc::new(
        Cache::new(Config {
            timeout: Duration::from_secs(1),
            ..Default::default()
        })
        .unwrap(),
    );
    let (started, entered) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let c = cache.clone();
    let kind = kind.to_owned();
    let owner = thread::spawn(move || {
        c.get_or_fill(
            key(1),
            || Ok(()),
            |deadline| {
                started.send(deadline).unwrap();
                blocked.recv().unwrap();
                match kind.as_str() {
                    "positive" => Ok(stats(7)),
                    "negative" => Err(gas::Error::MissingState(key(1))),
                    "exception" => Err(gas::Error::Arithmetic),
                    _ => panic!("unknown captured outcome"),
                }
            },
        )
    });
    let deadline = entered.recv_timeout(Duration::from_secs(2)).unwrap();
    let (paused, waiting) = mpsc::channel();
    let (resume, suspended) = mpsc::channel();
    let c = cache.clone();
    let waiter = thread::spawn(move || {
        let mut checks = 0;
        c.get_or_fill(
            key(1),
            || {
                checks += 1;
                if checks == 2 {
                    paused.send(()).unwrap();
                    suspended.recv().unwrap();
                    if let Some(error) = context_error.clone() {
                        return Err(error);
                    }
                }
                Ok(())
            },
            |_| panic!("duplicate fill"),
        )
    });
    waiting.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(cache.info().waiters, 1);
    release.send(()).unwrap();
    let owner = owner.join().unwrap();
    let completed_on_time = Instant::now() < deadline;
    thread::sleep(deadline.saturating_duration_since(Instant::now()));
    assert!(Instant::now() >= deadline);
    if close {
        cache.close();
    }
    resume.send(()).unwrap();
    let waiter = waiter.join().unwrap();
    assert!(completed_on_time, "fixture must finish before its deadline");
    assert_eq!(cache.info().pending, 0);
    assert_eq!(cache.info().requests, 0);
    (owner, waiter, cache)
}

#[test]
fn original_completed_outcomes_survive_delayed_waiter_scheduling() {
    let fixture: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_gas_cache_waiters.json"
    ))
    .unwrap();
    for row in fixture["rows"].as_array().unwrap() {
        assert_eq!(row["reply_queued_before_deadline"], true);
        assert_eq!(row["resumed_after_deadline"], true);
        let kind = row["kind"].as_str().unwrap();
        let (owner, waiter, cache) = completed_before_waiter_resumes(kind, None, false);
        let actual = match (owner, waiter) {
            (Ok(owner), Ok(waiter)) => {
                assert!(Arc::ptr_eq(&owner, &waiter));
                assert_eq!(cache.info().entries, 1);
                match waiter.data() {
                    Data::Observed(s) => {
                        assert_eq!(s.minimum, Some(7.into()));
                        json!(["ok", 7])
                    }
                    Data::MissingState => value(&waiter),
                }
            }
            (Err(Error::Fill(owner)), Err(Error::Fill(waiter))) => {
                assert!(Arc::ptr_eq(&owner, &waiter));
                assert!(matches!(waiter.as_ref(), gas::Error::Arithmetic));
                assert_eq!(cache.info().entries, 0);
                // Native removes the pending key and replies notfound on a
                // generator exception; Rust retains the typed fill failure.
                json!("notfound")
            }
            pair => panic!("unexpected delayed results: {pair:?}"),
        };
        assert_eq!(actual, row["result"], "{kind}");
    }
}

#[test]
fn completed_outcomes_do_not_bypass_waiter_cancellation_recovery_or_close() {
    use super::super::super::Error as ChainError;
    for (context, close) in [
        (Some(ChainError::Cancelled), false),
        (Some(ChainError::RecoveryRequired), false),
        (None, true),
        (Some(ChainError::RecoveryRequired), true),
    ] {
        let (owner, waiter, cache) =
            completed_before_waiter_resumes("positive", context.clone(), close);
        let owner = owner.unwrap();
        match context {
            Some(ChainError::Cancelled) => {
                assert!(matches!(waiter, Err(Error::Check(ChainError::Cancelled))));
            }
            Some(ChainError::RecoveryRequired) => assert!(matches!(
                waiter,
                Err(Error::Check(ChainError::RecoveryRequired))
            )),
            None => assert!(matches!(waiter, Err(Error::Closed))),
            _ => unreachable!(),
        }
        assert_eq!(cache.info().entries, usize::from(!close));
        if close {
            assert!(cache.info().retained_bytes > 0);
            drop(owner);
            assert_eq!(cache.info().retained_bytes, 0);
        }
    }
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
