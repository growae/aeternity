pub use ae_node::http;
use ae_node::{
    dry_run_worker as worker,
    http::dry_run::{self, Config, Error, Profile, schema},
};
use ae_sync::dry_run::{self as admission, Timeout, process};
use ae_types::Unsigned;
use num_bigint::BigInt;
use serde_json::{Value as J, json};
use std::{collections::BTreeSet, time::Duration};
#[path = "support/dry_run_pipeline.rs"]
mod support;
use support::*;

fn process() -> process::Config {
    let mut p = process::Config::new(
        std::path::PathBuf::from(env!("CARGO_BIN_EXE_ae-dry-run-worker")),
        std::env::temp_dir(),
    );
    p.drain = Duration::from_millis(500);
    p
}
fn parameters<'a>(
    base: &'a ae_execution::transactions::Config<'a>,
    floor: Option<&'a Unsigned>,
) -> worker::Parameters<'a> {
    worker::Parameters {
        execution: base,
        preparation: Default::default(),
        state: Default::default(),
        overlays: Default::default(),
        node_decode: Default::default(),
        rendering: Default::default(),
        reporting_floor: floor,
    }
}
async fn compare(row: &J, profile: &J, client: &admission::Client) -> usize {
    let mut source = Backend::new(row);
    let original = source.trees.roots();
    let v3 = row["module"] == "oas_endpoints";
    let wire = bytes(&row["raw"]);
    let parsed = schema::request(
        schema::Body::Complete(&wire),
        v3,
        row["query"]["query"].as_str().unwrap(),
        5_000_000,
    );
    let mut children = 0;
    let (code, body) = match parsed {
        Err(e) => {
            let (status, body) = e.response_parts();
            (status, serde_json::from_slice(&body).unwrap())
        }
        Ok(parsed) => {
            let mode = if row["operation"] == "ProtectedDryRunTxs" {
                Profile::Public
            } else {
                Profile::Internal
            };
            let gas: BigInt = row["input"]["gas_limit"]
                .as_number()
                .unwrap()
                .as_str()
                .parse()
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
            let config = Config {
                profile: mode,
                gas_limit: &gas,
                existing_keys: &keys,
                order: &order,
                state: Default::default(),
                limits: Default::default(),
            };
            match dry_run::prepare(&parsed.input, &mut source, &config) {
                Err(Error::Rejected { code, reason }) => (code, json!({"reason":reason})),
                Err(e) => panic!("unexpected preparation {e:?}"),
                Ok(request) => {
                    let view = source.view(request.top).unwrap();
                    let weak = std::sync::Arc::downgrade(&view);
                    let floor = Unsigned::from(row["input"]["floor"].as_u64().unwrap());
                    let options = ae_execution::dry_run::Options {
                        store_read_gas_metering: row["input"]["metering"].as_bool().unwrap(),
                        control: Some(ae_types::work::Control::new(|| false)),
                        ..Default::default()
                    };
                    let ticket = configured(profile, |base| {
                        worker::submit_http(
                            client,
                            process(),
                            view,
                            request,
                            options,
                            parameters(base, Some(&floor)),
                            Timeout::After(Duration::from_secs(10)),
                        )
                    })
                    .unwrap();
                    let retained =
                        tokio::time::timeout(Duration::from_secs(15), ticket.wait_retained())
                            .await
                            .unwrap()
                            .unwrap();
                    children += 1;
                    assert!(
                        retained
                            .value
                            .observation
                            .as_ref()
                            .unwrap()
                            .status
                            .success()
                    );
                    assert_eq!(retained.value.broker.view.trees.roots(), original);
                    assert_eq!(client.available().0, 0);
                    assert!(weak.upgrade().is_some());
                    let reply =
                        worker::decode_reply(retained.value.result.as_ref().unwrap()).unwrap();
                    let worker::Reply::Done(result) = reply else {
                        panic!("worker failure {reply:?}")
                    };
                    let body = schema::render(200, result.body, v3, parsed.strings).unwrap();
                    drop(retained);
                    assert!(weak.upgrade().is_none());
                    assert_eq!(client.available().0, 1);
                    (200, body)
                }
            }
        }
    };
    assert_eq!(
        code,
        row["result"]["response"]["code"].as_u64().unwrap() as u16
    );
    assert_eq!(body, row["result"]["response"]["body"]);
    assert_eq!(source.trace, row["trace"].as_array().unwrap().clone());
    assert_eq!(source.trees.roots(), original);
    children
}
#[tokio::test]
async fn production_worker_preserves_native_pipeline_results_profiles_and_retained_views() {
    let p = process();
    let (client, owner) = admission::create(admission::Config {
        threads: 1,
        requests: 1,
        bytes: p.reservation(p.input_bytes).unwrap(),
    })
    .unwrap();
    let rows = FIXTURE["rows"].as_array().unwrap();
    assert_eq!(rows.iter().filter(|r| covered(r)).count(), 472);
    let mut children = 0;
    for (index, row) in rows.iter().enumerate().filter(|(_, r)| covered(r)) {
        for profile in [&FIXTURE["profile"], &FIXTURE["repeat_profile"]] {
            eprintln!(
                "isolated native row {index}: {} {}",
                row["input"]["name"], row["input"]["protocol"]
            );
            children += compare(row, profile, &client).await;
        }
    }
    assert_eq!(children, 888);
    println!("native comparisons=944; production children={children}");
    owner.shutdown().await;
}

#[path = "dry_run_worker/aevm.rs"]
mod aevm;
#[path = "dry_run_worker/control.rs"]
mod control;
#[path = "dry_run_worker/repository.rs"]
mod repository;
