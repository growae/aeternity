use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Observed {
    inner: Backend<'static>,
    trace: Arc<Mutex<Vec<J>>>,
}
impl Drop for Observed {
    fn drop(&mut self) {
        *self.trace.lock().unwrap() = self.inner.trace.clone();
    }
}
impl dry_run::Source for Observed {
    type Error = &'static str;
    fn top_height(&mut self) -> Result<Option<u64>, Self::Error> {
        self.inner.top_height()
    }
    fn top_hash(&mut self) -> Result<Option<Hash256>, Self::Error> {
        self.inner.top_hash()
    }
    fn protocol(&mut self, h: u64) -> Result<ae_protocol::Protocol, Self::Error> {
        self.inner.protocol(h)
    }
    fn transaction(
        &mut self,
        h: Hash256,
    ) -> Result<dry_run::Location, dry_run::LookupError<Self::Error>> {
        self.inner.transaction(h)
    }
}
impl pipeline::Source for Observed {
    type Accounts = ae_trie::MemoryStore;
    type Cache = ae_trie::MemoryStore;
    type SelectionError = &'static str;
    fn select(
        &mut self,
        h: Hash256,
    ) -> Result<Arc<View>, ae_chain::dry_run::Error<Self::SelectionError>> {
        self.inner.view(h)
    }
}

#[tokio::test]
async fn original_public_deadline_and_internal_unbounded_rows_match_native_pipeline() {
    let c = config();
    let (client, owner) = pool(&c);
    let rows: Vec<_> = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["input"]["name"] == "deadline")
        .collect();
    assert_eq!(rows.len(), 8);
    let mut children = 0;
    for row in rows {
        for profile in [&FIXTURE["profile"], &FIXTURE["repeat_profile"]] {
            let trace = Arc::new(Mutex::new(vec![]));
            let recorded = trace.clone();
            let ticket = c
                .reserve(&client, Timeout::After(Duration::from_secs(5)))
                .unwrap()
                .submit(request(row), move |_| {
                    Ok(capture(
                        Observed {
                            inner: Backend::new(row),
                            trace: recorded,
                        },
                        row,
                        profile,
                    ))
                })
                .unwrap();
            let (status, value) = match ticket.wait_retained().await {
                Ok(held) => {
                    let response = held.value.response.as_ref().unwrap();
                    assert!(held.value.selected.is_some());
                    children += 1;
                    let result = (
                        response.status,
                        serde_json::from_slice::<J>(&response.body).unwrap(),
                    );
                    assert_eq!(client.available().0, 0);
                    drop(held);
                    result
                }
                Err(e) => {
                    assert_eq!(e, admission::Error::NativeTimeout);
                    assert_eq!(client.available().0, 0);
                    let result = dry_run::failure::body(
                        dry_run::failure::native_admission(e).unwrap(),
                        1 << 20,
                    )
                    .unwrap();
                    tokio::time::timeout(Duration::from_secs(3), async {
                        while client.available().0 == 0 {
                            tokio::time::sleep(Duration::from_millis(5)).await
                        }
                    })
                    .await
                    .unwrap();
                    result
                }
            };
            assert_eq!(
                status,
                row["result"]["response"]["code"].as_u64().unwrap() as u16
            );
            assert_eq!(value, row["result"]["response"]["body"]);
            assert_eq!(
                *trace.lock().unwrap(),
                row["trace"].as_array().unwrap().clone()
            );
            assert_eq!(client.available().0, 1);
        }
    }
    assert_eq!(children, 8);
    owner.shutdown().await;
}

#[tokio::test]
async fn explicit_zero_native_timer_starts_after_helper_work_and_before_selection() {
    let c = config();
    let (client, owner) = pool(&c);
    let state = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Release(state.clone());
    let (tx, rx) = mpsc::sync_channel(1);
    let selections = Arc::new(AtomicUsize::new(0));
    let count = selections.clone();
    let row = selected_row();
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(5)))
        .unwrap()
        .submit(request(row), move |_| {
            Ok(capture(
                Blocking {
                    inner: Backend::new(row),
                    phase: Phase::Helper,
                    gate: Gate { state, entered: tx },
                    _life: Arc::new(()),
                    selections: count,
                },
                row,
                &FIXTURE["profile"],
            )
            .with_native_timeout(Timeout::After(Duration::ZERO)))
        })
        .unwrap();
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut waiting = tokio::spawn(ticket.wait_retained());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut waiting)
            .await
            .is_err()
    );
    assert_eq!(selections.load(Ordering::Acquire), 0);
    drop(release);
    assert!(matches!(
        waiting.await.unwrap(),
        Err(admission::Error::NativeTimeout)
    ));
    assert_eq!(selections.load(Ordering::Acquire), 0);
    owner.shutdown().await;
    assert!(dry_run::failure::native_admission(admission::Error::Timeout).is_none());
}

#[tokio::test]
async fn actual_core_acknowledgement_finishes_native_timer_before_reporting() {
    let c = config();
    let (client, owner) = pool(&c);
    let row = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["input"]["name"] == "floor"
                && r["input"]["protocol"] == 6
                && r["operation"] == "ProtectedDryRunTxs"
        })
        .unwrap();
    let state = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Release(state.clone());
    let (tx, rx) = mpsc::sync_channel(1);
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(5)))
        .unwrap()
        .submit(request(row), move |_| {
            Ok(
                capture(Backend::new(row), selected_row(), &FIXTURE["profile"])
                    .with_native_timeout(Timeout::After(Duration::from_secs(1)))
                    .with_reporting_floor(move || {
                        let _ = tx.try_send(());
                        drop(
                            state
                                .1
                                .wait_while(state.0.lock().unwrap(), |v| !*v)
                                .unwrap(),
                        );
                        Ok(Some(2_000_000.into()))
                    }),
            )
        })
        .unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let mut waiting = tokio::spawn(ticket.wait_retained());
    assert!(
        tokio::time::timeout(Duration::from_millis(1100), &mut waiting)
            .await
            .is_err()
    );
    assert_eq!(client.available().0, 0);
    drop(release);
    let held = waiting.await.unwrap().unwrap();
    let response = held.value.response.as_ref().unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(
        serde_json::from_slice::<J>(&response.body).unwrap(),
        row["result"]["response"]["body"]
    );
    drop(held);
    owner.shutdown().await;
}

#[tokio::test]
async fn native_timeout_during_real_execution_keeps_blocked_parent_control_owned() {
    let c = config();
    let (client, owner) = pool(&c);
    let state = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Release(state.clone());
    let (tx, rx) = mpsc::sync_channel(1);
    let life = Arc::new(());
    let weak = Arc::downgrade(&life);
    let row = selected_row();
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(5)))
        .unwrap()
        .submit(request(row), move |_| {
            let control = ae_types::work::Control::new(move || {
                let _ = tx.try_send(());
                drop(
                    state
                        .1
                        .wait_while(state.0.lock().unwrap(), |v| !*v)
                        .unwrap(),
                );
                false
            });
            let (unused, _) = mpsc::sync_channel(1);
            let source = Blocking {
                inner: Backend::new(row),
                phase: Phase::Capture,
                gate: Gate {
                    state: Arc::new((Mutex::new(true), Condvar::new())),
                    entered: unused,
                },
                _life: life,
                selections: Arc::default(),
            };
            Ok(configured(&FIXTURE["profile"], |base| {
                let fate = base.fate.unwrap().with_control(Some(&control));
                capture_with(
                    source,
                    row,
                    &FIXTURE["profile"],
                    &ae_execution::transactions::Config {
                        fate: Some(&fate),
                        ..*base
                    },
                )
            })
            .with_native_timeout(Timeout::After(Duration::from_secs(1))))
        })
        .unwrap();
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        ticket.wait_retained().await,
        Err(admission::Error::NativeTimeout)
    ));
    assert!(weak.upgrade().is_some());
    assert_eq!(client.available().0, 0);
    drop(release);
    released(&client, &weak).await;
    retry(&c, &client).await;
    owner.shutdown().await;
}

#[tokio::test]
async fn reporting_lookup_preserves_native_profile_failure_and_disabled_floor_order() {
    let c = config();
    let (client, owner) = pool(&c);
    let rows = FIXTURE["rows"].as_array().unwrap();
    for row in [
        rows.iter()
            .find(|r| {
                r["input"]["name"] == "floor"
                    && r["input"]["protocol"] == 6
                    && r["operation"] == "DryRunTxs"
            })
            .unwrap(),
        rows.iter()
            .find(|r| {
                r["input"]["name"] == "bad_call"
                    && r["input"]["protocol"] == 6
                    && r["operation"] == "ProtectedDryRunTxs"
            })
            .unwrap(),
    ] {
        let ticket = c
            .reserve(&client, Timeout::After(Duration::from_secs(5)))
            .unwrap()
            .submit(request(row), move |_| {
                Ok(capture(Backend::new(row), row, &FIXTURE["profile"])
                    .with_reporting_floor(|| panic!("native path must skip reporting lookup")))
            })
            .unwrap();
        let held = ticket.wait_retained().await.unwrap();
        let response = held.value.response.as_ref().unwrap();
        assert_eq!(
            response.status,
            row["result"]["response"]["code"].as_u64().unwrap() as u16
        );
        assert_eq!(
            serde_json::from_slice::<J>(&response.body).unwrap(),
            row["result"]["response"]["body"]
        );
        drop(held);
    }
    let row = selected_row();
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(5)))
        .unwrap()
        .submit(request(row), move |_| {
            Ok(configured(&FIXTURE["profile"], |base| {
                capture_with_rendering(
                    Backend::new(row),
                    row,
                    &FIXTURE["profile"],
                    base,
                    dry_run::response::Limits {
                        results: 0,
                        ..Default::default()
                    },
                )
            })
            .with_reporting_floor(|| panic!("rendering failure must skip reporting lookup")))
        })
        .unwrap();
    let held = ticket.wait_retained().await.unwrap();
    assert!(
        matches!(&held.value.response, Err(pipeline::Error::Worker(f))
        if f.stage == worker::Stage::Rendering && f.kind == worker::Kind::Capacity)
    );
    assert_eq!(client.available().0, 0);
    drop(held);
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(5)))
        .unwrap()
        .submit(request(row), move |_| {
            Ok(
                capture(Backend::new(row), row, &FIXTURE["profile"]).with_reporting_floor(|| {
                    Err(ae_node::configuration::resolution::Error::Field(
                        "gas_price",
                    ))
                }),
            )
        })
        .unwrap();
    let held = ticket.wait_retained().await.unwrap();
    assert!(matches!(
        &held.value.response,
        Err(pipeline::Error::Process(process::Error::Broker(
            worker::ReadError::Reporting(_)
        )))
    ));
    assert!(held.value.selected.is_some());
    assert_eq!(client.available().0, 0);
    drop(held);
    let row = rows
        .iter()
        .find(|r| {
            r["input"]["name"] == "floor"
                && r["input"]["protocol"] == 6
                && r["operation"] == "ProtectedDryRunTxs"
        })
        .unwrap();
    let expected = rows
        .iter()
        .find(|r| {
            r["input"]["name"] == "floor"
                && r["input"]["protocol"] == 6
                && r["operation"] == "DryRunTxs"
                && r["module"] == row["module"]
                && r["query"] == row["query"]
        })
        .unwrap();
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(5)))
        .unwrap()
        .submit(request(row), move |_| {
            Ok(capture(Backend::new(row), row, &FIXTURE["profile"])
                .with_reporting_floor(|| Ok(None)))
        })
        .unwrap();
    let held = ticket.wait_retained().await.unwrap();
    let response = held.value.response.as_ref().unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(
        serde_json::from_slice::<J>(&response.body).unwrap(),
        expected["result"]["response"]["body"]
    );
    drop(held);
    owner.shutdown().await;
}
