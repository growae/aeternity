use super::*;
use serde_json::Value as Json;
use std::{
    sync::atomic::{AtomicI64, Ordering},
    time::{Duration, Instant},
};

struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now(&self) -> reporter::Time {
        reporter::Time {
            monotonic: Instant::now(),
            unix_microseconds: 1_258_000_000_000_000
                + i128::from(self.0.load(Ordering::Relaxed)) * 1000,
        }
    }
}
fn clock() -> Arc<TestClock> {
    Arc::new(TestClock(AtomicI64::new(0)))
}
fn integer(n: i64) -> Value {
    Value::Integer(n.into())
}
fn wait(predicate: impl Fn() -> bool) {
    let until = Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(Instant::now() < until, "probe transition timeout");
        thread::sleep(Duration::from_millis(1));
    }
}
fn one(client: &Client, point: &str) -> Value {
    client
        .values(&Points::One(Part::atom(point)))
        .unwrap()
        .blocking_wait()
        .unwrap()[0]
        .1
        .clone()
}
fn crash(client: &Client) {
    let ticket: Ticket<()> = client
        .call(128, |_, _, _| panic!("injected histogram probe exit"))
        .unwrap();
    assert_eq!(ticket.blocking_wait(), Err(Error::Closed));
}

#[test]
fn actual_probe_replacements_match_original_restart_and_delete_traces() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/erlang_metrics_probe_recovery.json"
    ))
    .unwrap();
    let cases = native["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 4);
    let mut failures = 0;
    for case in cases {
        let clock = clock();
        let budget = Budget::new(Limits::default()).unwrap();
        let policy = if case["name"] == "configured_delete" {
            Policy(vec![
                Rule {
                    threshold: Threshold::Within {
                        count: 1,
                        milliseconds: 1000,
                    },
                    action: Action::Restart,
                },
                Rule {
                    threshold: Threshold::Always,
                    action: Action::Delete,
                },
            ])
        } else {
            Policy::default()
        };
        let mut owner =
            Owner::spawn(Config::default(), policy, budget.clone(), clock.clone()).unwrap();
        let client = owner.client();
        for row in case["rows"].as_array().unwrap() {
            client
                .update(&integer(12))
                .unwrap()
                .blocking_wait()
                .unwrap();
            assert_eq!(one(&client, "mean"), integer(12));
            let generation = client.info().generation;
            clock
                .0
                .store(row["time_ms"].as_i64().unwrap(), Ordering::Relaxed);
            crash(&client);
            wait(|| client.info().generation > generation || client.info().closing);
            let info = client.info();
            assert!(info.previous_joined);
            assert_eq!(info.last_error, Some(Error::Panicked));
            assert_eq!(row["old_probe_gone"], true);
            if row["action"] == "restart" {
                assert!(info.running && !info.closing);
                assert_eq!(info.generation, generation + 1);
                assert_eq!(one(&client, "n"), integer(0));
                assert_eq!(row["new_live_probe"], true);
            } else {
                assert!(!info.running && info.closing);
                assert_eq!(
                    info.terminal_action,
                    Some(if row["action"] == "delete" {
                        Action::Delete
                    } else {
                        Action::Disable
                    })
                );
                assert_eq!(row["new_live_probe"], false);
                assert!(matches!(client.update(&integer(1)), Err(Error::Closed)));
            }
            failures += 1;
        }
        owner.join();
        assert_eq!(budget.usage(), Usage::default());
        // A fresh registration has a fresh history and fresh histogram.
        let fresh = Owner::spawn(
            Config::default(),
            Policy::default(),
            budget.clone(),
            clock.clone(),
        )
        .unwrap();
        assert_eq!(one(&fresh.client(), "n"), integer(0));
        drop(fresh);
        assert_eq!(budget.usage(), Usage::default());
    }
    assert_eq!(failures, 20);
}

#[test]
fn shared_memory_and_worker_limits_are_released_only_after_owned_work_exits() {
    let budget = Budget::new(Limits {
        workers: 2,
        retained_bytes: 512,
        ..Limits::default()
    })
    .unwrap();
    let config = Config {
        module: super::super::Module::Plain,
        ..Config::default()
    };
    let first = Owner::spawn(config, Policy::default(), budget.clone(), clock()).unwrap();
    let second = Owner::spawn(config, Policy::default(), budget.clone(), clock()).unwrap();
    assert!(matches!(
        Owner::spawn(config, Policy::default(), budget.clone(), clock()),
        Err(Error::Limit)
    ));
    let a = first.client();
    let b = second.client();
    for _ in 0..3 {
        a.update(&integer(1)).unwrap().blocking_wait().unwrap();
    }
    for _ in 0..2 {
        b.update(&integer(2)).unwrap().blocking_wait().unwrap();
    }
    assert_eq!(budget.usage().retained_bytes, 485);
    assert_eq!(
        b.update(&integer(3)).unwrap().blocking_wait(),
        Err(Error::Limit)
    );
    assert_eq!(b.info().failures, 0);
    assert_eq!(one(&b, "mean"), integer(2));
    a.reset().unwrap().blocking_wait().unwrap();
    b.update(&integer(5)).unwrap().blocking_wait().unwrap();
    assert_eq!(budget.usage().retained_bytes, 291);
    drop(first);
    drop(second);
    assert_eq!(budget.usage(), Usage::default());
    assert!(a.info().joined && b.info().joined);
}

#[test]
fn unread_receipts_share_credit_across_workers_and_replacements() {
    let budget = Budget::new(Limits {
        requests: 2,
        ..Limits::default()
    })
    .unwrap();
    let first = Owner::spawn(
        Config::default(),
        Policy::default(),
        budget.clone(),
        clock(),
    )
    .unwrap();
    let second = Owner::spawn(
        Config::default(),
        Policy::default(),
        budget.clone(),
        clock(),
    )
    .unwrap();
    let a = first.client();
    let b = second.client();
    let held = a.update(&integer(1)).unwrap();
    wait(|| a.info().completed == 1);
    let generation = b.info().generation;
    crash(&b);
    wait(|| b.info().generation > generation);
    assert_eq!(budget.usage().pending, 1);
    let other = b.values(&Points::One(Part::atom("n"))).unwrap();
    wait(|| b.info().completed == 1);
    assert!(matches!(a.update(&integer(1)), Err(Error::Busy)));
    assert!(matches!(b.update(&integer(1)), Err(Error::Busy)));
    assert_eq!(budget.usage().pending, 2);
    drop(held);
    a.update(&integer(3)).unwrap().blocking_wait().unwrap();
    assert_eq!(other.blocking_wait().unwrap()[0].1, integer(0));
    drop(first);
    drop(second);
    assert_eq!(budget.usage(), Usage::default());
}

#[test]
fn shutdown_drains_accepted_commands_and_stale_handles_cannot_reopen() {
    let budget = Budget::new(Limits::default()).unwrap();
    let mut owner = Owner::spawn(
        Config::default(),
        Policy::default(),
        budget.clone(),
        clock(),
    )
    .unwrap();
    let client = owner.client();
    let (entered, got) = std::sync::mpsc::sync_channel(1);
    let (release, gate) = std::sync::mpsc::sync_channel(1);
    let blocked = client
        .call(128, move |_, _, _| {
            entered.send(()).unwrap();
            gate.recv().unwrap();
            Ok(())
        })
        .unwrap();
    got.recv_timeout(Duration::from_secs(5)).unwrap();
    let update = client.update(&integer(7)).unwrap();
    let value = client.values(&Points::One(Part::atom("mean"))).unwrap();
    client.shutdown();
    assert!(matches!(client.reset(), Err(Error::Closed)));
    assert_eq!(budget.usage().workers, 1);
    release.send(()).unwrap();
    owner.join();
    assert!(client.info().joined);
    assert_eq!(blocked.blocking_wait(), Ok(()));
    assert_eq!(update.blocking_wait(), Ok(()));
    assert_eq!(value.blocking_wait().unwrap()[0].1, integer(7));
    assert_eq!(budget.usage(), Usage::default());
}

#[test]
fn arithmetic_failures_replace_real_workers_but_admission_errors_do_not() {
    let budget = Budget::new(Limits::default()).unwrap();
    let clock = clock();
    let owner = Owner::spawn(
        Config::default(),
        Policy::default(),
        budget.clone(),
        clock.clone(),
    )
    .unwrap();
    let client = owner.client();
    client
        .update(&Value::Float(1.0e308))
        .unwrap()
        .blocking_wait()
        .unwrap();
    assert_eq!(
        client
            .update(&Value::Float(1.0e308))
            .unwrap()
            .blocking_wait(),
        Err(Error::Arithmetic)
    );
    wait(|| client.info().generation == 2);
    assert_eq!(one(&client, "mean"), integer(0));
    assert!(client.info().previous_joined);
    client
        .update(&Value::Integer(num_bigint::BigInt::from(1) << 1100))
        .unwrap()
        .blocking_wait()
        .unwrap();
    clock.0.store(100_000, Ordering::Relaxed);
    assert_eq!(
        client
            .values(&Points::default_points())
            .unwrap()
            .blocking_wait(),
        Err(Error::Arithmetic)
    );
    wait(|| client.info().generation == 3);
    assert_eq!(one(&client, "n"), integer(0));
    assert!(matches!(
        client.update(&Value::Float(f64::INFINITY)),
        Err(Error::InvalidValue)
    ));
    assert_eq!(client.info().failures, 2);
    drop(owner);
    assert_eq!(budget.usage(), Usage::default());
}

#[test]
fn response_bytes_are_reserved_before_work_and_retained_by_receipts() {
    let budget = Budget::new(Limits {
        pending_bytes: 2000,
        ..Limits::default()
    })
    .unwrap();
    let owner = Owner::spawn(
        Config::default(),
        Policy::default(),
        budget.clone(),
        clock(),
    )
    .unwrap();
    let client = owner.client();
    assert!(matches!(
        client.values(&Points::default_points()),
        Err(Error::Busy)
    ));
    assert_eq!(budget.usage().pending, 0);
    let response = client.values(&Points::One(Part::atom("mean"))).unwrap();
    wait(|| client.info().completed == 1);
    let bytes = budget.usage().pending_bytes;
    assert!(bytes > 512 && bytes < 2000);
    drop(owner);
    assert_eq!(budget.usage().pending_bytes, bytes);
    response.blocking_wait().unwrap();
    assert_eq!(budget.usage(), Usage::default());
}

#[test]
fn ordered_policy_and_history_bounds_preserve_native_prefix_semantics() {
    let policy = Policy(vec![
        Rule {
            threshold: Threshold::Within {
                count: 3,
                milliseconds: 1000,
            },
            action: Action::Restart,
        },
        Rule {
            threshold: Threshold::Total(4),
            action: Action::Disable,
        },
        Rule {
            threshold: Threshold::Always,
            action: Action::Delete,
        },
    ]);
    let mut history = policy::History::new(policy, 64);
    for (time, action) in [
        (100, Action::Restart),
        (200, Action::Restart),
        (300, Action::Restart),
        (400, Action::Disable),
        (500, Action::Delete),
    ] {
        assert_eq!(history.failure(time), Ok(action));
    }
    let mut history = policy::History::new(Policy(vec![]), 1);
    assert_eq!(history.failure(0), Ok(Action::Delete));
    let mut history = policy::History::new(
        Policy(vec![Rule {
            threshold: Threshold::Within {
                count: 0,
                milliseconds: 0,
            },
            action: Action::Restart,
        }]),
        1,
    );
    assert_eq!(history.failure(0), Ok(Action::Restart));
    assert_eq!(history.failure(0), Ok(Action::Restart));
    let mut history = policy::History::new(
        Policy(vec![
            Rule {
                threshold: Threshold::Within {
                    count: 1,
                    milliseconds: 1000,
                },
                action: Action::Restart,
            },
            Rule {
                threshold: Threshold::Always,
                action: Action::Restart,
            },
        ]),
        2,
    );
    assert_eq!(history.failure(0), Ok(Action::Restart));
    assert_eq!(history.failure(0), Ok(Action::Restart));
    assert_eq!(history.failure(0), Err(Error::Limit));
}

#[test]
fn abandoned_receipts_keep_running_credit_and_failed_generation_queues_are_discarded() {
    let budget = Budget::new(Limits {
        requests: 2,
        ..Limits::default()
    })
    .unwrap();
    let owner = Owner::spawn(
        Config::default(),
        Policy::default(),
        budget.clone(),
        clock(),
    )
    .unwrap();
    let client = owner.client();
    let (entered, got) = std::sync::mpsc::sync_channel(1);
    let (release, gate) = std::sync::mpsc::sync_channel(1);
    let doomed: Ticket<()> = client
        .call(128, move |_, _, _| {
            entered.send(()).unwrap();
            gate.recv().unwrap();
            panic!("injected queued probe exit");
        })
        .unwrap();
    got.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(doomed);
    assert_eq!(budget.usage().pending, 1);
    let queued = client.update(&integer(99)).unwrap();
    assert!(matches!(client.update(&integer(1)), Err(Error::Busy)));
    release.send(()).unwrap();
    assert_eq!(queued.blocking_wait(), Err(Error::Closed));
    wait(|| client.info().generation == 2);
    assert_eq!(one(&client, "n"), integer(0));
    drop(owner);
    assert_eq!(budget.usage(), Usage::default());
}

#[tokio::test(flavor = "current_thread")]
async fn asynchronous_finish_joins_workers_and_preserves_accepted_receipts() {
    let budget = Budget::new(Limits::default()).unwrap();
    let mut owner = Owner::spawn(
        Config::default(),
        Policy::default(),
        budget.clone(),
        clock(),
    )
    .unwrap();
    let client = owner.client();
    client.update(&integer(9)).unwrap().await.unwrap();
    let receipt = client.values(&Points::One(Part::atom("mean"))).unwrap();
    owner.finish().await;
    assert!(client.info().joined);
    assert_eq!(budget.usage().workers, 0);
    assert_eq!(budget.usage().retained_bytes, 0);
    assert_eq!(budget.usage().pending, 1);
    assert_eq!(receipt.await.unwrap()[0].1, integer(9));
    assert_eq!(budget.usage(), Usage::default());
}
