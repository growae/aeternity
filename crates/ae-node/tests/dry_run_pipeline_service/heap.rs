use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[tokio::test]
async fn core_heap_exhaustion_is_native_while_disabled_and_larger_caps_recover() {
    let c = config();
    let (client, owner) = pool(&c);
    let row = selected_row();
    for bytes in [Some(1), None, Some(8 << 20)] {
        let ticket = c
            .reserve(&client, Timeout::After(Duration::from_secs(10)))
            .unwrap()
            .submit(request(row), move |_| {
                Ok(capture(Backend::new(row), row, &FIXTURE["profile"])
                    .with_native_heap(move || Ok(bytes)))
            })
            .unwrap();
        let held = ticket.wait_retained().await.unwrap();
        assert!(held.value.selected.is_some());
        let response = held.value.response.as_ref().unwrap();
        let body: J = serde_json::from_slice(&response.body).unwrap();
        if bytes == Some(1) {
            assert_eq!(response.status, 400);
            assert_eq!(
                body,
                serde_json::json!({"reason":"Bad request: dry-run failed: killed"})
            );
        } else {
            assert_eq!(response.status, 200);
            assert_eq!(body, row["result"]["response"]["body"]);
        }
        assert_eq!(client.available().0, 0);
        drop(held);
        assert_eq!(client.available().0, 1);
    }
    owner.shutdown().await;
    let mut c = config();
    c.process.heap.bytes = 1;
    let (client, owner) = pool(&c);
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(10)))
        .unwrap()
        .submit(request(row), move |_| {
            Ok(capture(Backend::new(row), row, &FIXTURE["profile"]).with_native_heap(|| Ok(None)))
        })
        .unwrap();
    let held = ticket.wait_retained().await.unwrap();
    assert!(
        matches!(&held.value.response, Err(pipeline::Error::Process(process::Error::Exit(s)))
        if s.code().and_then(ae_worker_heap::Exit::from_code) == Some(ae_worker_heap::Exit::ServiceHeap))
    );
    assert_eq!(client.available().0, 0);
    drop(held);
    owner.shutdown().await;
}

#[tokio::test]
async fn actual_child_releases_native_cap_before_large_reporting_reply_and_bounded_rendering() {
    let c = config();
    let (client, owner) = pool(&c);
    let row = selected_row();
    assert_eq!(request(row).profile, Profile::Public);
    let called = Arc::new(AtomicBool::new(false));
    let seen = called.clone();
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(10)))
        .unwrap()
        .submit(request(row), move |_| {
            Ok(configured(&FIXTURE["profile"], |base| {
                capture_with_rendering(
                    Backend::new(row),
                    row,
                    &FIXTURE["profile"],
                    base,
                    dry_run::response::Limits {
                        transaction: http::transaction::Limits {
                            json_bytes: 8192,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
            })
            .with_native_heap(|| Ok(Some(2 << 20)))
            .with_reporting_floor(move || {
                seen.store(true, Ordering::Release);
                // The reply buffer alone exceeds the former native cap.
                // The serializer rejects its price before decimal conversion.
                Ok(Some(Unsigned::from_field_bytes(&vec![1; 3 << 20]).unwrap()))
            }))
        })
        .unwrap();
    let held = ticket.wait_retained().await.unwrap();
    assert!(called.load(Ordering::Acquire));
    assert!(
        matches!(&held.value.response, Err(pipeline::Error::Worker(f))
        if f.stage == worker::Stage::Rendering)
    );
    assert!(held.value.selected.is_some());
    assert_eq!(client.available().0, 0);
    drop(held);
    owner.shutdown().await;
}

#[tokio::test]
async fn heap_policy_runs_after_helper_before_selection_and_remains_owned_on_native_expiry() {
    let c = config();
    let (client, owner) = pool(&c);
    let row = selected_row();
    let bad = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["input"]["name"] == "missing_hash" && r["input"]["protocol"] == 6)
        .unwrap();
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(5)))
        .unwrap()
        .submit(request(bad), move |_| {
            Ok(capture(Backend::new(bad), bad, &FIXTURE["profile"])
                .with_native_heap(|| panic!("invalid helper input must skip heap lookup")))
        })
        .unwrap();
    let held = ticket.wait_retained().await.unwrap();
    assert_eq!(held.value.response.as_ref().unwrap().status, 400);
    assert_eq!(
        serde_json::from_slice::<J>(&held.value.response.as_ref().unwrap().body).unwrap(),
        bad["result"]["response"]["body"]
    );
    assert_eq!(
        held.value.captured.as_ref().unwrap().source.trace,
        bad["trace"].as_array().unwrap().clone()
    );
    assert!(held.value.selected.is_none());
    drop(held);
    for bytes in [0, usize::MAX] {
        let ticket = c
            .reserve(&client, Timeout::After(Duration::from_secs(5)))
            .unwrap()
            .submit(request(row), move |_| {
                Ok(capture(Backend::new(row), row, &FIXTURE["profile"])
                    .with_native_heap(move || Ok(Some(bytes))))
            })
            .unwrap();
        let held = ticket.wait_retained().await.unwrap();
        assert!(matches!(
            &held.value.response,
            Err(pipeline::Error::Configuration(_))
        ));
        assert!(held.value.selected.is_none());
        assert!(
            !held
                .value
                .captured
                .as_ref()
                .unwrap()
                .source
                .trace
                .iter()
                .any(|v| v["call"] == "header")
        );
        drop(held);
    }
    let state = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Release(state.clone());
    let (tx, rx) = mpsc::sync_channel(1);
    let life = Arc::new(());
    let weak = Arc::downgrade(&life);
    let selections = Arc::new(AtomicUsize::new(0));
    let count = selections.clone();
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(5)))
        .unwrap()
        .submit(request(row), move |_| {
            let (unused, _) = mpsc::sync_channel(1);
            Ok(capture(
                Blocking {
                    inner: Backend::new(row),
                    phase: Phase::Capture,
                    gate: Gate {
                        state: Arc::new((Mutex::new(true), Condvar::new())),
                        entered: unused,
                    },
                    _life: life,
                    selections: count,
                },
                row,
                &FIXTURE["profile"],
            )
            .with_native_timeout(Timeout::After(Duration::from_millis(100)))
            .with_native_heap(move || {
                let _ = tx.try_send(());
                drop(
                    state
                        .1
                        .wait_while(state.0.lock().unwrap(), |v| !*v)
                        .unwrap(),
                );
                Ok(None)
            }))
        })
        .unwrap();
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        ticket.wait_retained().await,
        Err(admission::Error::NativeTimeout)
    ));
    assert_eq!(selections.load(Ordering::Acquire), 0);
    assert!(weak.upgrade().is_some());
    assert_eq!(client.available().0, 0);
    drop(release);
    released(&client, &weak).await;
    assert_eq!(selections.load(Ordering::Acquire), 0);
    retry(&c, &client).await;
    owner.shutdown().await;
}
