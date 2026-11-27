use ae_node::dry_run_worker::{self as worker, pipeline};
pub use ae_node::http;
use ae_sync::dry_run::{self as admission, Timeout, process};
use ae_types::{Hash256, Unsigned};
use http::dry_run::{self, Profile};
use serde_json::Value as J;
use std::{
    collections::BTreeSet,
    sync::{Arc, Condvar, Mutex, mpsc},
    time::Duration,
};
#[path = "support/dry_run_pipeline.rs"]
mod support;
use support::*;

impl pipeline::Source for Backend<'static> {
    type Accounts = ae_trie::MemoryStore;
    type Cache = ae_trie::MemoryStore;
    type SelectionError = &'static str;
    fn select(
        &mut self,
        hash: Hash256,
    ) -> Result<Arc<View>, ae_chain::dry_run::Error<Self::SelectionError>> {
        self.view(hash)
    }
}
fn config() -> pipeline::Config {
    pipeline::Config {
        process: process::Config::new(
            env!("CARGO_BIN_EXE_ae-dry-run-worker").into(),
            std::env::temp_dir(),
        ),
        body_bytes: 5_000_000,
        query_bytes: 16 << 10,
        work_bytes: 64 << 20,
    }
}
fn request(row: &J) -> pipeline::Request {
    pipeline::Request {
        body: bytes(&row["raw"]),
        more: false,
        query: row["query"]["query"].as_str().unwrap().into(),
        v3: row["module"] == "oas_endpoints",
        profile: if row["operation"] == "ProtectedDryRunTxs" {
            Profile::Public
        } else {
            Profile::Internal
        },
    }
}
fn capture_with<S>(
    source: S,
    row: &J,
    profile: &J,
    base: &ae_execution::transactions::Config<'_>,
) -> pipeline::Captured<S> {
    capture_with_rendering(source, row, profile, base, Default::default())
}
fn capture_with_rendering<S>(
    source: S,
    row: &J,
    profile: &J,
    base: &ae_execution::transactions::Config<'_>,
    rendering: dry_run::response::Limits,
) -> pipeline::Captured<S> {
    let context = ae_node::configuration::resolution::Context::new(
        ae_node::configuration::resolution::Directories {
            home: "/release".into(),
            data: "data".into(),
            log: "log".into(),
        },
    )
    .unwrap();
    let configuration = ae_node::configuration::Configuration::from_value(
        serde_json::json!({"http":{"dry_run":{"timeout_ms":row["input"]["timeout"]}}}),
    )
    .unwrap();
    let resolved = configuration.resolve(&context).unwrap();
    let native = ae_node::configuration::dry_run::timeout(
        &resolved,
        if request(row).profile == Profile::Public {
            ae_execution::dry_run::Profile::Public
        } else {
            ae_execution::dry_run::Profile::Internal
        },
        None,
    )
    .unwrap()
    .runtime()
    .unwrap();
    let keys: BTreeSet<_> = [
        "txs",
        "top",
        "accounts",
        "tx_events",
        "tx",
        "tx_hash",
        "call_req",
        "calldata",
        "contract",
        "amount",
        "gas",
        "caller",
        "nonce",
        "abi_version",
        "context",
        "stateful",
        "pub_key",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let order = otp_order::profile(profile);
    let floor = Unsigned::from(row["input"]["floor"].as_u64().unwrap());
    let gas = row["input"]["gas_limit"]
        .as_number()
        .unwrap()
        .as_str()
        .parse()
        .unwrap();
    pipeline::Captured::new(
        source,
        worker::Parameters {
            execution: base,
            preparation: Default::default(),
            state: Default::default(),
            overlays: Default::default(),
            node_decode: Default::default(),
            rendering,
            reporting_floor: Some(&floor),
        },
        dry_run::Config {
            profile: request(row).profile,
            gas_limit: &gas,
            existing_keys: &keys,
            order: &order,
            state: Default::default(),
            limits: Default::default(),
        },
        ae_execution::dry_run::Options {
            store_read_gas_metering: row["input"]["metering"].as_bool().unwrap(),
            ..Default::default()
        },
    )
    .with_native_timeout(native)
}
fn capture<S>(source: S, row: &J, profile: &J) -> pipeline::Captured<S> {
    configured(profile, |base| capture_with(source, row, profile, base))
}
fn selected_row() -> &'static J {
    FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["input"]["name"] == "call_success" && r["input"]["protocol"] == 6)
        .unwrap()
}
fn pool(c: &pipeline::Config) -> (admission::Client, admission::Worker) {
    admission::create(admission::Config {
        threads: 1,
        requests: 1,
        bytes: c.weight().unwrap(),
    })
    .unwrap()
}

#[tokio::test]
async fn one_admitted_pipeline_preserves_native_responses_traces_and_retained_readers() {
    let c = config();
    let (client, owner) = pool(&c);
    let rows = FIXTURE["rows"].as_array().unwrap();
    assert_eq!(rows.iter().filter(|r| covered(r)).count(), 508);
    let mut selections = 0;
    for row in rows.iter().filter(|r| covered(r)) {
        for profile in [&FIXTURE["profile"], &FIXTURE["repeat_profile"]] {
            let before = Backend::new(row).trees.roots();
            let ticket = c
                .reserve(&client, Timeout::After(Duration::from_secs(10)))
                .unwrap()
                .submit(request(row), move |_| {
                    Ok(capture(Backend::new(row), row, profile))
                })
                .unwrap();
            let held = ticket.wait_retained().await.unwrap();
            let response = held
                .value
                .response
                .as_ref()
                .unwrap_or_else(|e| panic!("{}: {e:?}", row["input"]["name"]));
            assert_eq!(
                response.status,
                row["result"]["response"]["code"].as_u64().unwrap() as u16
            );
            assert_eq!(
                serde_json::from_slice::<J>(&response.body).unwrap(),
                row["result"]["response"]["body"]
            );
            let trace = held
                .value
                .captured
                .as_ref()
                .map(|c| c.source.trace.as_slice())
                .unwrap_or(&[]);
            assert_eq!(trace, row["trace"].as_array().unwrap());
            if let Some(source) = &held.value.captured {
                assert_eq!(source.source.trees.roots(), before)
            }
            let weak = held.value.selected.as_ref().map(Arc::downgrade);
            if let Some(selected) = &held.value.selected {
                assert_eq!(selected.trees.roots(), before);
                selections += 1;
            }
            assert_eq!(client.available().0, 0);
            drop(held);
            if let Some(weak) = weak {
                assert!(weak.upgrade().is_none())
            }
            assert_eq!(client.available().0, 1);
        }
    }
    assert_eq!(selections, 904);
    owner.shutdown().await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Capture,
    Helper,
    Selection,
}
struct Gate {
    state: Arc<(Mutex<bool>, Condvar)>,
    entered: mpsc::SyncSender<()>,
}
impl Gate {
    fn block(&self) {
        let _ = self.entered.try_send(());
        drop(
            self.state
                .1
                .wait_while(self.state.0.lock().unwrap(), |released| !*released)
                .unwrap(),
        );
    }
}
struct Release(Arc<(Mutex<bool>, Condvar)>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all()
    }
}
struct Blocking {
    inner: Backend<'static>,
    phase: Phase,
    gate: Gate,
    _life: Arc<()>,
    selections: Arc<std::sync::atomic::AtomicUsize>,
}
impl dry_run::Source for Blocking {
    type Error = &'static str;
    fn top_height(&mut self) -> Result<Option<u64>, Self::Error> {
        if self.phase == Phase::Helper {
            self.gate.block()
        }
        self.inner.top_height()
    }
    fn protocol(&mut self, h: u64) -> Result<ae_protocol::Protocol, Self::Error> {
        self.inner.protocol(h)
    }
    fn top_hash(&mut self) -> Result<Option<Hash256>, Self::Error> {
        if self.phase == Phase::Helper {
            self.gate.block()
        }
        self.inner.top_hash()
    }
    fn transaction(
        &mut self,
        h: Hash256,
    ) -> Result<dry_run::Location, dry_run::LookupError<Self::Error>> {
        self.inner.transaction(h)
    }
}
impl pipeline::Source for Blocking {
    type Accounts = ae_trie::MemoryStore;
    type Cache = ae_trie::MemoryStore;
    type SelectionError = &'static str;
    fn select(
        &mut self,
        h: Hash256,
    ) -> Result<Arc<View>, ae_chain::dry_run::Error<Self::SelectionError>> {
        self.selections
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if self.phase == Phase::Selection {
            self.gate.block()
        }
        self.inner.view(h)
    }
}
async fn released(client: &admission::Client, weak: &std::sync::Weak<()>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while client.available().0 == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap();
    assert!(weak.upgrade().is_none());
}
fn blocked(
    c: &pipeline::Config,
    client: &admission::Client,
    phase: Phase,
    timeout: Timeout,
) -> (
    admission::Ticket<pipeline::Completed<Blocking>>,
    Release,
    std::sync::Weak<()>,
    mpsc::Receiver<()>,
) {
    let state = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Release(state.clone());
    let (tx, rx) = mpsc::sync_channel(1);
    let life = Arc::new(());
    let weak = Arc::downgrade(&life);
    let row = selected_row();
    let ticket = c
        .reserve(client, timeout)
        .unwrap()
        .submit(request(row), move |_| {
            let gate = Gate { state, entered: tx };
            if phase == Phase::Capture {
                gate.block()
            }
            Ok(capture(
                Blocking {
                    inner: Backend::new(row),
                    phase,
                    gate,
                    _life: life,
                    selections: Arc::default(),
                },
                row,
                &FIXTURE["profile"],
            ))
        })
        .unwrap();
    (ticket, release, weak, rx)
}
async fn retry(c: &pipeline::Config, client: &admission::Client) {
    let row = selected_row();
    let ticket = c
        .reserve(client, Timeout::After(Duration::from_secs(10)))
        .unwrap()
        .submit(request(row), move |_| {
            Ok(capture(Backend::new(row), row, &FIXTURE["profile"]))
        })
        .unwrap();
    let held = ticket.wait_retained().await.unwrap();
    assert_eq!(held.value.response.as_ref().unwrap().status, 200);
    drop(held);
    assert_eq!(client.available().0, 1);
}
#[tokio::test]
async fn deadlines_cover_capture_helper_and_selection_without_releasing_surviving_work() {
    let mut c = config();
    let (client, owner) = pool(&c);
    // No child may start after a blocked phase exceeds its deadline.
    c.process.executable = std::env::temp_dir().join("ae-must-not-start-expired-pipeline");
    for phase in [Phase::Capture, Phase::Helper, Phase::Selection] {
        let (ticket, release, weak, entered) = blocked(
            &c,
            &client,
            phase,
            Timeout::After(Duration::from_millis(350)),
        );
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(matches!(
            c.reserve(&client, Timeout::Unbounded),
            Err(admission::Error::Busy)
        ));
        assert!(matches!(
            ticket.wait_retained().await,
            Err(admission::Error::Timeout)
        ));
        assert!(weak.upgrade().is_some());
        assert_eq!(client.available().0, 0);
        drop(release);
        released(&client, &weak).await;
    }
    retry(&config(), &client).await;
    owner.shutdown().await;
}
#[tokio::test]
async fn disconnect_and_owner_shutdown_join_blocked_selection_and_keep_credits() {
    let c = config();
    let (client, owner) = pool(&c);
    let (ticket, release, weak, entered) =
        blocked(&c, &client, Phase::Selection, Timeout::Unbounded);
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    drop(ticket);
    assert_eq!(client.available().0, 0);
    assert!(weak.upgrade().is_some());
    drop(release);
    released(&client, &weak).await;
    retry(&c, &client).await;
    let (ticket, release, weak, entered) =
        blocked(&c, &client, Phase::Selection, Timeout::Unbounded);
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    owner.stop();
    let joining = tokio::spawn(owner.shutdown());
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!joining.is_finished());
    assert!(weak.upgrade().is_some());
    assert_eq!(client.available().0, 0);
    assert!(matches!(
        c.reserve(&client, Timeout::Unbounded),
        Err(admission::Error::Stopped)
    ));
    drop(release);
    joining.await.unwrap();
    assert!(matches!(
        ticket.wait_retained().await,
        Err(admission::Error::Stopped)
    ));
    assert!(weak.upgrade().is_none());
}

#[path = "dry_run_pipeline_service/native.rs"]
mod native;
#[path = "dry_run_pipeline_service/repository.rs"]
mod repository;
