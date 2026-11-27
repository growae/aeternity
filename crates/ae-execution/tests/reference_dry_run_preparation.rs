use ae_execution::{
    Context,
    dry_run::{
        Prepared,
        prepare::{self, Input},
    },
    transactions::{self, batch},
};
use ae_protocol::Protocol;
use ae_tx::{CanonicalTransaction, Transaction, WireTransaction};
use ae_types::{Hash256, Unsigned};
use serde_json::{Value as J, json};
use std::sync::{Arc, LazyLock};

static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_dry_run_preparation.json")).unwrap()
});
fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn auth(v: &J) -> Option<Arc<WireTransaction>> {
    (!v.is_null()).then(|| Arc::new(WireTransaction::decode(&bytes(v)).unwrap()))
}
fn hash(v: &J) -> Option<Hash256> {
    (!v.is_null()).then(|| Hash256(bytes(v).try_into().unwrap()))
}
fn value(v: &prepare::Owned) -> J {
    json!({"transaction":hex::encode_upper(v.transaction.transaction().encode()),"signed":hex::encode_upper(v.transaction.encode()),"stateless":v.stateless,"auth_tx":v.auth_tx.as_ref().map(|tx|hex::encode_upper(tx.encode())),"auth_tx_hash":v.auth_tx_hash.map(|h|hex::encode_upper(h.0))})
}
fn output(result: Result<Vec<prepare::Owned>, prepare::Failure>) -> J {
    match result {
        Ok(v) => json!({"kind":"ok","values":v.iter().map(value).collect::<Vec<_>>()}),
        Err(prepare::Failure {
            error: prepare::Error::BadCallRequest,
            ..
        }) => json!({"kind":"error","class":"error","tag":"bad_dry_run_call_request"}),
        Err(e) => panic!("unexpected local limit {e:?}"),
    }
}
#[test]
fn call_defaults_invalid_values_auth_and_exact_serialization_match_original() {
    let rows = FIXTURE["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 145);
    let mut valid = 0;
    for (i, row) in rows.iter().enumerate() {
        let result = prepare::all(
            vec![Input::CallRequest(&row["request"])],
            Default::default(),
            Default::default(),
        );
        let actual = match result {
            Ok(v) => {
                valid += 1;
                assert_eq!(v.len(), 1);
                json!({"kind":"ok","value":value(&v[0])})
            }
            Err(prepare::Failure {
                index: Some(0),
                error: prepare::Error::BadCallRequest,
            }) => json!({"kind":"error","class":"error","tag":"bad_dry_run_call_request"}),
            Err(e) => panic!("row {i}: {e:?}"),
        };
        assert_eq!(actual, row["result"], "row {i}: {}", row["request"]);
        if row["request"]["gas"] == 0 && row["result"]["kind"] == "ok" {
            let mut request = row["request"].clone();
            request["gas"] = serde_json::from_str("-0").unwrap();
            let prepared = prepare::all(
                vec![Input::CallRequest(&request)],
                Default::default(),
                Default::default(),
            )
            .unwrap();
            assert_eq!(value(&prepared[0]), row["result"]["value"]);
        }
    }
    assert_eq!(valid, 52);
}
#[test]
fn whole_mixed_sequences_prepare_before_exposing_executable_inputs() {
    let rows = FIXTURE["batches"].as_array().unwrap();
    assert_eq!(rows.len(), 6);
    for (i, row) in rows.iter().enumerate() {
        let inputs = row["inputs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|input| match input["kind"].as_str().unwrap() {
                "tx" => Input::transaction(
                    Transaction::decode_with_limits(
                        &bytes(&input["transaction"]),
                        Default::default(),
                    )
                    .unwrap(),
                ),
                "call_req" => Input::CallRequest(&input["request"]),
                _ => panic!(),
            })
            .collect();
        assert_eq!(
            output(prepare::all(inputs, Default::default(), Default::default())),
            row["result"],
            "batch {i}"
        );
    }
}
#[test]
fn authentication_context_persists_and_events_reset_like_original() {
    let rows = FIXTURE["environments"].as_array().unwrap();
    assert_eq!(rows.len(), 64);
    let raw =
        Transaction::decode_with_limits(&bytes(&rows[0]["initial"]["auth_tx"]), Default::default())
            .unwrap();
    let owned = prepare::all(
        vec![Input::transaction(raw)],
        Default::default(),
        Default::default(),
    )
    .unwrap();
    for (i, row) in rows.iter().enumerate() {
        let mut context = Context::transaction(50, Protocol::Ceres);
        context.ga_transaction = auth(&row["initial"]["auth_tx"]);
        context.ga_transaction_hash = hash(&row["initial"]["auth_tx_hash"]);
        let mut events = vec![batch::Event {
            transaction_hash: Hash256::ZERO,
            transaction_type: ae_tx::TransactionKind::Spend,
            value: transactions::Event::Primitive(ae_execution::Event::FeeDebit {
                account: ae_execution::dry_run::MAGIC_ACCOUNT,
                amount: Unsigned::from(1),
            }),
        }];
        assert!(!row["initial"]["events_empty"].as_bool().unwrap());
        let mut trace = vec![];
        for option in row["options"].as_array().unwrap() {
            let prepared = Prepared {
                auth_tx: auth(&option["auth_tx"]),
                auth_tx_hash: hash(&option["auth_tx_hash"]),
                ..owned[0].as_prepared()
            };
            prepared.prepare_environment(&mut context, &mut events);
            trace.push(json!({"auth_tx":context.ga_transaction.as_ref().map(|t|hex::encode_upper(t.encode())),"auth_tx_hash":context.ga_transaction_hash.map(|h|hex::encode_upper(h.0)),"events_empty":events.is_empty()}));
        }
        assert_eq!(json!(trace), row["trace"], "environment sequence {i}");
    }
}
#[test]
fn preparation_capacity_and_decoder_limits_stay_operational() {
    let row = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["request"].is_object()
                && r["request"].as_object().unwrap().len() == 2
                && r["result"]["kind"] == "ok"
        })
        .unwrap();
    let request = &row["request"];
    for limits in [
        prepare::Limits {
            requests: 0,
            ..Default::default()
        },
        prepare::Limits {
            encoded_bytes: 1,
            ..Default::default()
        },
        prepare::Limits {
            work_bytes: 0,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            prepare::all(
                vec![Input::CallRequest(request)],
                limits,
                Default::default()
            ),
            Err(prepare::Failure {
                error: prepare::Error::ResourceLimit,
                ..
            })
        ));
    }
    let mut huge = request.clone();
    huge["gas"] = json!(1000000);
    assert!(matches!(
        prepare::all(
            vec![Input::CallRequest(&huge)],
            prepare::Limits {
                number_chars: 3,
                ..Default::default()
            },
            Default::default()
        ),
        Err(prepare::Failure {
            error: prepare::Error::ResourceLimit,
            ..
        })
    ));
    let nested = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["result"]["kind"] == "ok"
                && r["request"]["context"]["tx"].is_string()
                && WireTransaction::decode(&bytes(&r["result"]["value"]["auth_tx"]))
                    .unwrap()
                    .kind()
                    == ae_tx::TransactionKind::PayingFor
        })
        .unwrap();
    let mut limits = prepare::Limits::default();
    limits.transaction.max_layers = 0;
    assert!(matches!(
        prepare::all(
            vec![Input::CallRequest(&nested["request"])],
            limits,
            Default::default()
        ),
        Err(prepare::Failure {
            error: prepare::Error::Operational(ae_tx::Error::ResourceLimit),
            ..
        })
    ));
    assert_eq!(
        value(
            &prepare::all(
                vec![Input::CallRequest(&nested["request"])],
                Default::default(),
                Default::default()
            )
            .unwrap()[0]
        ),
        nested["result"]["value"]
    );
}
