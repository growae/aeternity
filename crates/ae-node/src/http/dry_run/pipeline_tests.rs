use super::{Config, Error, Profile, execution, response, schema};
use ae_execution::{Context, contracts, dry_run as core, transactions};
use ae_protocol::Protocol;
use ae_tx::Transaction;
use ae_types::Unsigned;
use num_bigint::BigInt;
use serde_json::{Value as J, json};
use std::collections::BTreeSet;

#[path = "../../../tests/support/dry_run_pipeline.rs"]
mod support;
use support::*;

fn compare(row: &J, profile: &J) {
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
    let (code, body) = match parsed {
        Err(error) => {
            let r = error.response();
            (r.status, serde_json::from_slice(&r.body).unwrap())
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
            match super::prepare(&parsed.input, &mut source, &config) {
                Err(Error::Rejected { code, reason }) => (code, json!({"reason":reason})),
                Err(error) => panic!("unexpected preparation {error:?}"),
                Ok(request) => configured(profile, |base| {
                    let events = request.tx_events;
                    let options = core::Options {
                        store_read_gas_metering: row["input"]["metering"].as_bool().unwrap(),
                        control: Some(ae_types::work::Control::new(|| false)),
                        ..Default::default()
                    };
                    let result = execution::execute(
                        request,
                        |h| source.view(h),
                        base,
                        &options,
                        Default::default(),
                        Default::default(),
                    )
                    .unwrap();
                    assert_eq!(result.view.trees.roots(), original);
                    let prices: Vec<_> = result
                        .output
                        .results
                        .iter()
                        .filter_map(|r| {
                            r.result
                                .as_ref()
                                .ok()?
                                .call
                                .as_ref()
                                .map(|c| c.gas_price.clone())
                        })
                        .collect();
                    let floor = Unsigned::from(row["input"]["floor"].as_u64().unwrap());
                    let rendered = response::body(
                        &result.output,
                        mode,
                        events,
                        Some(&floor),
                        Default::default(),
                    )
                    .unwrap();
                    assert_eq!(
                        prices,
                        result
                            .output
                            .results
                            .iter()
                            .filter_map(|r| r
                                .result
                                .as_ref()
                                .ok()?
                                .call
                                .as_ref()
                                .map(|c| c.gas_price.clone()))
                            .collect::<Vec<_>>()
                    );
                    let rendered = schema::render(200, rendered, v3, parsed.strings).unwrap();
                    // Capacity errors are operational and leave the retained view intact.
                    assert!(matches!(
                        response::body(
                            &result.output,
                            mode,
                            events,
                            Some(&floor),
                            response::Limits {
                                json_bytes: 0,
                                ..Default::default()
                            }
                        ),
                        Err(response::Error::Capacity)
                    ));
                    assert_eq!(result.view.trees.roots(), original);
                    (200, rendered)
                }),
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
}
#[test]
fn schema_preparation_execution_and_rendering_match_native_pipeline() {
    let rows = FIXTURE["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 560);
    assert_eq!(rows.iter().filter(|r| covered(r)).count(), 472);
    for (index, row) in rows.iter().enumerate().filter(|(_, r)| covered(r)) {
        for profile in [&FIXTURE["profile"], &FIXTURE["repeat_profile"]] {
            let result = std::panic::catch_unwind(|| compare(row, profile));
            assert!(
                result.is_ok(),
                "native row {index}: {} {} {} {}",
                row["input"]["name"],
                row["input"]["protocol"],
                row["operation"],
                row["module"]
            );
        }
    }
}

fn native_kind(value: &str) -> ae_tx::TransactionKind {
    ae_tx::TransactionKind::ALL
        .into_iter()
        .find(|kind| {
            format!("{kind:?}").to_lowercase() == value.trim_end_matches("_tx").replace('_', "")
        })
        .unwrap()
}
fn native_event(row: &J) -> transactions::batch::Event {
    use ae_execution::fate_chain::TransferEvent;
    use ae_tx::{CanonicalTransaction, TransactionLeaf};
    let kind = native_kind(row["type"].as_str().unwrap());
    let tx_hash = hash(&row["hash"]);
    let value = match row["kind"].as_str().unwrap() {
        "channel" => transactions::Event::Channel {
            channel: bytes(&row["key"]).try_into().unwrap(),
            transaction_hash: tx_hash,
            transaction_type: kind,
        },
        "delta" => transactions::Event::Primitive(ae_execution::Event::FeeDebit {
            account: ae_types::AccountPublicKey(bytes(&row["key"]).try_into().unwrap()),
            amount: 0.into(),
        }),
        "internal" => {
            let tx = Transaction::decode(&bytes(&row["transaction"])).unwrap();
            let event = match tx.leaf() {
                TransactionLeaf::Spend(tx) => TransferEvent::InternalCall(tx.clone()),
                TransactionLeaf::Name(tx) => {
                    assert_eq!(bytes(&row["key"]), b"Chain.aens_revoke");
                    TransferEvent::NameCall {
                        info: "Chain.aens_revoke",
                        transaction: Some(tx.clone()),
                    }
                }
                TransactionLeaf::Oracle(tx) => {
                    assert_eq!(bytes(&row["key"]), b"Chain.oracle_extend");
                    TransferEvent::OracleCall {
                        info: "Chain.oracle_extend",
                        transaction: tx.clone(),
                    }
                }
                other => panic!("unexpected native event {other:?}"),
            };
            transactions::Event::Contract(contracts::Event::Internal(event))
        }
        other => panic!("unexpected native event kind {other}"),
    };
    transactions::batch::Event {
        transaction_hash: tx_hash,
        transaction_type: kind,
        value,
    }
}
#[test]
fn native_result_types_reasons_event_order_and_callback_serialization() {
    use ae_tx::TransactionKind;
    use transactions::batch::Failure;
    let fixture: J = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_dry_run_rendering.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 34);
    for (index, row) in rows.iter().enumerate() {
        let mut output = core::Output {
            trees: Backend::new(&FIXTURE["rows"][0]).trees,
            context: Context::transaction(50, Protocol::Ceres),
            results: vec![],
            events: row["events"]
                .as_array()
                .unwrap()
                .iter()
                .map(native_event)
                .collect(),
        };
        let failure = match row["error"].as_str() {
            Some("gas_limit_exceeded") => Some((
                native_kind(row["type"].as_str().unwrap()),
                Failure::GasLimitExceeded,
            )),
            Some("tx_nonce_already_used_for_account") => Some((
                TransactionKind::Spend,
                Failure::Transaction(Box::new(transactions::Error::Primitive(
                    ae_execution::Error::NonceAlreadyUsed,
                ))),
            )),
            Some("crash") => Some((
                TransactionKind::Spend,
                Failure::Transaction(Box::new(transactions::Error::MinimumFeeCaseClause(
                    (-1).into(),
                ))),
            )),
            Some("not_allowed_to_pay_for") => Some((
                TransactionKind::PayingFor,
                Failure::Transaction(Box::new(transactions::Error::NotAllowedToPayFor(
                    native_kind(row["type"].as_str().unwrap()),
                ))),
            )),
            None => None,
            other => panic!("unknown captured error {other:?}"),
        };
        if let Some((transaction_type, failure)) = failure {
            output.results.push(core::ResultItem {
                transaction_type,
                result: Err(failure),
            });
        }
        let expected = &row["output"];
        assert_eq!(
            response::body(&output, Profile::Internal, true, None, Default::default()).unwrap(),
            *expected,
            "native rendering row {index}"
        );
        let without =
            response::body(&output, Profile::Public, false, None, Default::default()).unwrap();
        assert_eq!(without, json!({"results":expected["results"]}));
    }
}
