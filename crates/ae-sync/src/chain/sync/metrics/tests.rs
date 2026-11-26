use super::*;
use serde_json::{Value as Json, json};
use std::{
    cell::RefCell,
    pin::Pin,
    task::{Context as TaskContext, Waker},
};

fn task(id: u64, suspect: bool, target: u64) -> Task {
    Task {
        task: lifecycle::Task {
            id,
            suspect,
            target,
        },
        pool: 0,
        got: 0,
        adding: 0,
        pending: vec![],
        workers: 0,
    }
}
#[tokio::test]
async fn original_progress_callback_reads_logs_and_failure_prefixes() {
    let rows: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_sync_progress_callback.json"
    ))
    .unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 22);
    for row in rows.as_array().unwrap() {
        let tasks = match row["mode"].as_str().unwrap() {
            "empty" => vec![],
            "active" => vec![task(1, false, 100)],
            "suspect" => vec![task(1, true, 100)],
            "mixed" => vec![task(1, true, 100), task(2, false, 200)],
            "zero" => vec![task(0, false, 0)],
            "pending" => vec![Task {
                pool: 2,
                got: 1,
                adding: 1,
                pending: vec![3, 0, 2],
                workers: 1,
                ..task(1, false, 100)
            }],
            _ => panic!(),
        };
        let snapshot = Snapshot {
            tasks,
            top_target: 0,
            logging: row["logging"].as_bool().unwrap(),
        };
        let order = RefCell::new(vec![]);
        let gauges = RefCell::new(vec![]);
        let mut logs = vec![];
        let mut reads = 0;
        let mut log_attempts = 0;
        let attempt = callback(
            snapshot,
            || {
                reads += 1;
                order.borrow_mut().push(json!(["read", reads]));
                std::future::ready(if row["failure"] == json!(["read", reads]) {
                    Err(Error::Timeout)
                } else {
                    Ok(if reads == 1 { 33 } else { 67 })
                })
            },
            |value| {
                order.borrow_mut().push(json!(["metric", value]));
                gauges.borrow_mut().push(value);
            },
            |event| {
                log_attempts += 1;
                order.borrow_mut().push(json!(["log", log_attempts]));
                if row["failure"] == json!(["log", log_attempts]) {
                    return Err(Error::Events(events::Error::Closed));
                }
                logs.push(event.text());
                Ok(true)
            },
        )
        .await;
        let captured = &row["capture"];
        let native = &captured["result"]["returned"];
        assert_eq!(json!(order.into_inner()), native["order"], "{row}");
        assert_eq!(json!(reads), native["reads"]);
        assert_eq!(json!(log_attempts), native["log_attempts"]);
        assert_eq!(attempt.result.is_ok(), native["call_result"] == "ok");
        assert_eq!(
            attempt.result.is_ok(),
            !captured["timers"].as_array().unwrap().is_empty()
        );
        assert_eq!(
            *gauges.borrow(),
            captured["metrics"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["value"].as_f64().unwrap())
                .collect::<Vec<_>>()
        );
        assert_eq!(attempt.value, gauges.borrow().first().copied());
        assert_eq!(
            logs,
            captured["logs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|l| l["text"]
                    .as_str()
                    .unwrap()
                    .replace("(one)", "(1)")
                    .replace("(two)", "(2)")
                    .replace("(zero)", "(0)"))
                .collect::<Vec<_>>()
        );
        assert_eq!(attempt.logs, logs.len() as u64);
        assert_eq!(attempt.dropped, 0);
    }
}
fn pending<F: Future>(future: Pin<&mut F>) {
    assert!(
        future
            .poll(&mut TaskContext::from_waker(Waker::noop()))
            .is_pending()
    );
}
fn success() -> Attempt {
    Attempt {
        value: Some(100.0),
        logs: 0,
        dropped: 0,
        result: Ok(()),
    }
}
#[tokio::test(start_paused = true)]
async fn progress_clock_rearms_after_completion_without_overlap_or_catchup() {
    use std::sync::atomic::AtomicU64;
    let (mut owner, _, _) = Owner::new(Default::default()).unwrap();
    let delay = owner.status().next.unwrap() - Instant::now();
    assert!((Duration::from_millis(1)..=Duration::from_secs(1)).contains(&delay));
    let calls = Arc::new(AtomicU64::new(0));
    let count = calls.clone();
    pending(std::pin::pin!(owner.next_with(move || async move {
        count.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(5)).await;
        success()
    })));
    tokio::time::advance(delay).await;
    pending(std::pin::pin!(
        owner.next_with(|| async { panic!("replaced pending callback") })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(owner.tasks.len(), 1);
    tokio::time::advance(Duration::from_secs(100)).await;
    owner
        .next_with(|| async { panic!("overlapping callback") })
        .await
        .unwrap();
    assert_eq!(owner.status().ticks, 1);
    assert_eq!(owner.status().next, Some(Instant::now() + PERIOD));
    assert_eq!(owner.status().last, Some(100.0));
    pending(std::pin::pin!(owner.next_with(|| async { success() })));
    tokio::time::advance(PERIOD - Duration::from_millis(1)).await;
    pending(std::pin::pin!(owner.next_with(|| async { panic!() })));
    tokio::time::advance(Duration::from_millis(1)).await;
    owner.next_with(|| async { panic!() }).await.unwrap();
    assert_eq!(owner.status().ticks, 2);
    assert_eq!(owner.status().next, Some(Instant::now() + PERIOD));
}
#[tokio::test(start_paused = true)]
async fn failed_progress_keeps_emitted_prefix_and_never_rearms() {
    let (mut owner, feed, origin) = Owner::new(Default::default()).unwrap();
    owner.status.next = Some(Instant::now());
    assert_eq!(
        owner
            .next_with(|| async {
                Attempt {
                    value: Some(33.0),
                    logs: 1,
                    dropped: 2,
                    result: Err(Error::Timeout),
                }
            })
            .await,
        Err(Error::Timeout)
    );
    let status = owner.status();
    assert_eq!(
        (
            status.ticks,
            status.gauges,
            status.logs,
            status.dropped_logs,
            status.failures
        ),
        (1, 1, 1, 2, 1)
    );
    assert_eq!(status.next, None);
    tokio::time::advance(Duration::from_secs(300)).await;
    pending(std::pin::pin!(
        owner.next_with(|| async { panic!("rearmed failure") })
    ));
    assert_eq!(owner.tasks.len(), 0);
    owner.cancel();
    assert!(feed.subscribe(Some(origin)).unwrap().recv().await.is_none());
}
#[tokio::test(start_paused = true)]
async fn cancelled_progress_drops_callback_and_closes_old_feed() {
    use std::sync::atomic::AtomicU64;
    let (mut owner, feed, origin) = Owner::new(Default::default()).unwrap();
    owner.status.next = Some(Instant::now());
    let count = Arc::new(AtomicU64::new(0));
    let emit = count.clone();
    let (send, recv) = tokio::sync::oneshot::channel::<()>();
    pending(std::pin::pin!(owner.next_with(move || async move {
        let _ = recv.await;
        emit.fetch_add(1, Ordering::SeqCst);
        success()
    })));
    owner.cancel();
    assert!(send.send(()).is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(owner.tasks.len(), 0);
    assert!(
        feed.subscribe(Some(origin.clone()))
            .unwrap()
            .recv()
            .await
            .is_none()
    );
    let (_replacement, next, _) = Owner::new(Default::default()).unwrap();
    assert!(matches!(
        next.subscribe(Some(origin)),
        Err(events::Error::ForeignCursor)
    ));
}
#[tokio::test]
async fn diagnostic_backpressure_retains_gauge_and_reports_drops_and_gaps() {
    let (channel, feed) = events::channel(events::Config {
        records: 1,
        event_bytes: 160,
        bytes: 256,
        ..Default::default()
    })
    .unwrap();
    let origin = feed.cursor();
    let attempt = callback(
        Snapshot {
            tasks: vec![
                Task {
                    pending: vec![1; 100],
                    ..task(1, false, 100)
                },
                task(2, true, 200),
            ],
            top_target: 200,
            logging: true,
        },
        || std::future::ready(Ok(33)),
        |_| (),
        |event| match channel.publish(event) {
            Ok(_) => Ok(true),
            Err(events::Error::EventTooLarge) => Ok(false),
            Err(e) => Err(Error::Events(e)),
        },
    )
    .await;
    assert_eq!(attempt.result, Ok(()));
    assert_eq!(attempt.value, Some(33.0));
    assert_eq!((attempt.logs, attempt.dropped), (2, 1));
    let mut sub = feed.subscribe(Some(origin)).unwrap();
    assert!(matches!(
        sub.recv().await,
        Some(events::Delivery::Gap { missed: 1 })
    ));
    assert!(
        matches!(sub.recv().await, Some(events::Delivery::Event(r)) if matches!(&r.event, Log::Task(t) if t.task.id == 2))
    );
    channel.close();
    assert!(sub.recv().await.is_none());
}

#[tokio::test]
async fn progress_arithmetic_matches_native_integer_multiply_and_decimal_logs() {
    let rows: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_sync_progress_arithmetic.json"
    ))
    .unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 40);
    for row in rows.as_array().unwrap() {
        let selected = row["selected"].as_u64().unwrap();
        let target = row["target"].as_u64().unwrap();
        let mut values = vec![];
        let mut logs = vec![];
        let result = callback(
            Snapshot {
                tasks: vec![task(1, false, target)],
                top_target: target,
                logging: true,
            },
            || std::future::ready(Ok(selected)),
            |value| values.push(value),
            |event| {
                logs.push(event.text());
                Ok(true)
            },
        )
        .await;
        let native = &row["capture"];
        assert_eq!(
            result.result.is_ok(),
            native["result"]["returned"]["call_result"] == "ok"
        );
        assert_eq!(
            values,
            native["metrics"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["value"].as_f64().unwrap())
                .collect::<Vec<_>>(),
            "{selected}/{target}"
        );
        assert_eq!(
            logs,
            native["logs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|l| l["text"].as_str().unwrap().replace("(one)", "(1)"))
                .collect::<Vec<_>>(),
            "{selected}/{target}"
        );
    }
}
