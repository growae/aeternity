use super::*;
use ae_node::configuration::{
    Configuration,
    resolution::{Context, Directories},
};
use pipeline::configuration::Settings;
use serde_json::json;
use std::sync::LazyLock;

static POLICIES: LazyLock<J> = LazyLock::new(|| {
    let v: J =
        serde_json::from_str(include_str!("../fixtures/erlang_dry_run_policy.json")).unwrap();
    unpack(&v["root"], v["values"].as_array().unwrap())
});

pub(super) fn settings(row: &J) -> Arc<Settings> {
    let input = &row["input"];
    let mut context = Context::new(Directories {
        home: "/release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    let mut user = json!({"http": {
        "external": {"gas_limit": input["gas_limit"]},
        "gas_price": {"min_relay_gas_price": input["floor"]},
        "dry_run": {
            "timeout_ms": input["timeout"],
            "max_heap_words": input["heap"],
            "store_read_gas_metering": input["metering"]
        }
    }});
    for (flag, key) in [
        ("application_gas", "external"),
        ("application_floor", "gas_price"),
        ("application_dry_run", "dry_run"),
    ] {
        if input[flag] == true {
            let value = user["http"].as_object_mut().unwrap().remove(key).unwrap();
            let mut pairs: Vec<_> = value
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| json!([k, v]))
                .collect();
            if key == "gas_price" {
                // Same malformed one-element tuple as the native application
                // capture. Scalar floor lookup must never inspect this pair.
                pairs.push(json!(["reporting_utilization_override"]));
            }
            context
                .set("aehttp".into(), key.into(), J::Array(pairs))
                .unwrap();
        }
    }
    let configuration = Configuration::from_value(user).unwrap();
    Arc::new(Settings::capture(&configuration, Arc::new(context), 8).unwrap())
}

struct Traced {
    inner: Backend<'static>,
    events: Arc<Mutex<Vec<J>>>,
}
impl Traced {
    fn collect(&mut self) {
        self.events.lock().unwrap().append(&mut self.inner.trace);
    }
}
impl dry_run::Source for Traced {
    type Error = &'static str;
    fn top_height(&mut self) -> Result<Option<u64>, Self::Error> {
        let result = self.inner.top_height();
        self.collect();
        result
    }
    fn top_hash(&mut self) -> Result<Option<Hash256>, Self::Error> {
        let result = self.inner.top_hash();
        self.collect();
        result
    }
    fn protocol(&mut self, height: u64) -> Result<ae_protocol::Protocol, Self::Error> {
        let result = self.inner.protocol(height);
        self.collect();
        result
    }
    fn transaction(
        &mut self,
        hash: Hash256,
    ) -> Result<dry_run::Location, dry_run::LookupError<Self::Error>> {
        let result = self.inner.transaction(hash);
        self.collect();
        result
    }
}
impl pipeline::Source for Traced {
    type Accounts = ae_trie::MemoryStore;
    type Cache = ae_trie::MemoryStore;
    type SelectionError = &'static str;
    fn select(
        &mut self,
        hash: Hash256,
    ) -> Result<Arc<View>, ae_chain::dry_run::Error<Self::SelectionError>> {
        let result = self.inner.view(hash);
        self.collect();
        result
    }
}
fn trace(events: &Mutex<Vec<J>>, path: &[&str]) {
    events
        .lock()
        .unwrap()
        .push(json!({"call": "configuration", "path": path}));
}

fn traced(row: &'static J, profile: &J, events: Arc<Mutex<Vec<J>>>) -> pipeline::Captured<Traced> {
    let source = Traced {
        inner: Backend::new(row),
        events: events.clone(),
    };
    let snapshot = settings(row);
    let gas = snapshot.clone();
    let timeout = snapshot.clone();
    let heap = snapshot.clone();
    let metering = snapshot.clone();
    let gas_trace = events.clone();
    let timeout_trace = events.clone();
    let heap_trace = events.clone();
    let metering_trace = events.clone();
    capture(source, row, profile)
        .with_configuration(snapshot.clone())
        .with_gas_limit(move || {
            trace(&gas_trace, &["http", "external", "gas_limit"]);
            gas.gas_limit()
        })
        .with_timeout_policy(move |profile| {
            if profile == ae_execution::dry_run::Profile::Public {
                trace(&timeout_trace, &["http", "dry_run", "timeout_ms"]);
            }
            timeout.timeout(profile)
        })
        .with_native_heap(move || {
            trace(&heap_trace, &["http", "dry_run", "max_heap_words"]);
            heap.heap()
        })
        .with_store_read_metering(move || {
            trace(
                &metering_trace,
                &["http", "dry_run", "store_read_gas_metering"],
            );
            metering.metering()
        })
        .with_reporting_floor(move || {
            trace(&events, &["http", "gas_price", "min_relay_gas_price"]);
            snapshot.reporting_floor()
        })
}

#[tokio::test]
async fn native_configuration_phases_and_raw_limits_match_both_captured_profiles() {
    let c = config();
    let (client, owner) = pool(&c);
    let rows = POLICIES["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 208);
    let mut children = 0;
    for row in rows {
        for profile in [&POLICIES["profile"], &POLICIES["repeat_profile"]] {
            let events = Arc::new(Mutex::new(vec![]));
            let recorded = events.clone();
            let before = Backend::new(row).trees.roots();
            let ticket = c
                .reserve(&client, Timeout::After(Duration::from_secs(10)))
                .unwrap()
                .submit(request(row), move |_| Ok(traced(row, profile, recorded)))
                .unwrap();
            let held = ticket.wait_retained().await.unwrap();
            let label = (
                &row["input"]["name"],
                &row["module"],
                &row["operation"],
                &row["query"],
            );
            let response = held
                .value
                .response
                .as_ref()
                .unwrap_or_else(|e| panic!("{label:?}: {e:?}"));
            assert_eq!(
                u64::from(response.status),
                row["result"]["response"]["code"].as_u64().unwrap(),
                "{label:?}"
            );
            assert_eq!(
                serde_json::from_slice::<J>(&response.body).unwrap(),
                row["result"]["response"]["body"],
                "{label:?}"
            );
            // The native timeout getter internally reads micro_block_cycle.
            // This trace compares entry into the five service policy getters;
            // resolver-specific nested lookup semantics have separate fixtures.
            let expected: Vec<_> = row["trace"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|e| e["path"] != json!(["mining", "micro_block_cycle"]))
                .cloned()
                .collect();
            assert_eq!(*events.lock().unwrap(), expected, "{label:?}");
            if let Some(captured) = &held.value.captured {
                assert_eq!(captured.source.inner.trees.roots(), before);
            }
            let weak = held.value.selected.as_ref().map(Arc::downgrade);
            if let Some(selected) = &held.value.selected {
                children += 1;
                assert_eq!(selected.trees.roots(), before);
            }
            assert_eq!(client.available().0, 0);
            drop(held);
            if let Some(weak) = weak {
                assert!(weak.upgrade().is_none());
            }
            assert_eq!(client.available().0, 1);
        }
    }
    assert_eq!(children, 256);
    owner.shutdown().await;
}

#[tokio::test]
async fn replaced_settings_cannot_change_an_inflight_response_or_release_its_snapshot() {
    let c = config();
    let (client, owner) = pool(&c);
    let base = selected_row();
    let floor = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["input"]["name"] == "floor"
                && r["input"]["protocol"] == base["input"]["protocol"]
                && r["operation"] == base["operation"]
                && r["module"] == base["module"]
                && r["query"] == base["query"]
        })
        .unwrap();
    let old = settings(floor);
    let retained = Arc::downgrade(&old);
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(10)))
        .unwrap()
        .submit(request(floor), move |_| {
            Ok(capture(Backend::new(floor), floor, &FIXTURE["profile"]).with_configuration(old))
        })
        .unwrap();
    // A new owner may now install different settings. The admitted request
    // must still use and retain its original snapshot through response release.
    let replacement = settings(base);
    let held = ticket.wait_retained().await.unwrap();
    let response = held.value.response.as_ref().unwrap();
    assert_eq!(
        serde_json::from_slice::<J>(&response.body).unwrap(),
        floor["result"]["response"]["body"]
    );
    assert!(retained.upgrade().is_some());
    assert_eq!(client.available().0, 0);
    drop(held);
    assert!(retained.upgrade().is_none());
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(10)))
        .unwrap()
        .submit(request(base), move |_| {
            Ok(capture(Backend::new(base), base, &FIXTURE["profile"])
                .with_configuration(replacement))
        })
        .unwrap();
    let held = ticket.wait_retained().await.unwrap();
    let response = held.value.response.as_ref().unwrap();
    assert_eq!(
        serde_json::from_slice::<J>(&response.body).unwrap(),
        base["result"]["response"]["body"]
    );
    drop(held);
    owner.shutdown().await;
}

#[tokio::test]
async fn blocked_policy_callbacks_keep_settings_and_credits_until_work_really_finishes() {
    let c = config();
    let (client, owner) = pool(&c);
    for phase in 0..3 {
        let row = selected_row();
        let snapshot = settings(row);
        let weak = Arc::downgrade(&snapshot);
        let state = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Release(state.clone());
        let (tx, rx) = mpsc::sync_channel(1);
        let ticket = c
            .reserve(
                &client,
                Timeout::After(Duration::from_millis(if phase == 2 { 10_000 } else { 500 })),
            )
            .unwrap()
            .submit(request(row), move |_| {
                let gate = Gate { state, entered: tx };
                let captured = capture(Backend::new(row), row, &FIXTURE["profile"])
                    .with_configuration(snapshot);
                Ok(match phase {
                    0 => captured.with_gas_limit(move || {
                        gate.block();
                        Ok(json!(6_000_000))
                    }),
                    1 => captured.with_timeout_policy(move |_| {
                        gate.block();
                        Ok(Timeout::Unbounded)
                    }),
                    _ => captured
                        .with_native_timeout(Timeout::After(Duration::from_millis(500)))
                        .with_store_read_metering(move || {
                            gate.block();
                            Ok(true)
                        }),
                })
            })
            .unwrap();
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let error = match ticket.wait_retained().await {
            Err(e) => e,
            Ok(_) => panic!("blocked callback completed"),
        };
        assert_eq!(
            error,
            if phase == 2 {
                admission::Error::NativeTimeout
            } else {
                admission::Error::Timeout
            }
        );
        assert!(weak.upgrade().is_some());
        assert_eq!(client.available().0, 0);
        drop(release);
        tokio::time::timeout(Duration::from_secs(3), async {
            while client.available().0 == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(weak.upgrade().is_none());
        retry(&c, &client).await;
    }
    owner.shutdown().await;
}

#[tokio::test]
async fn failed_funding_skips_metering_and_internal_execution_skips_public_policy() {
    let c = config();
    let (client, owner) = pool(&c);
    let row = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["input"]["name"] == "negative_before_bad_call" && r["module"] == "oas_endpoints"
        })
        .unwrap();
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(10)))
        .unwrap()
        .submit(request(row), move |_| {
            Ok(capture(Backend::new(row), row, &FIXTURE["profile"])
                .with_store_read_metering(|| panic!("funding failure must precede metering")))
        })
        .unwrap();
    let held = ticket.wait_retained().await.unwrap();
    assert!(
        matches!(&held.value.response, Err(pipeline::Error::Worker(f)) if f.kind == worker::Kind::NegativeBalance)
    );
    drop(held);
    let row = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["input"]["name"] == "call_success"
                && r["input"]["protocol"] == 6
                && r["operation"] == "DryRunTxs"
        })
        .unwrap();
    let ticket = c
        .reserve(&client, Timeout::After(Duration::from_secs(10)))
        .unwrap()
        .submit(request(row), move |_| {
            Ok(capture(Backend::new(row), row, &FIXTURE["profile"])
                .with_gas_limit(|| panic!("internal path skips public gas limit"))
                .with_reporting_floor(|| panic!("internal path skips public reporting")))
        })
        .unwrap();
    let held = ticket.wait_retained().await.unwrap();
    assert_eq!(
        serde_json::from_slice::<J>(&held.value.response.as_ref().unwrap().body).unwrap(),
        row["result"]["response"]["body"]
    );
    drop(held);
    owner.shutdown().await;
}
