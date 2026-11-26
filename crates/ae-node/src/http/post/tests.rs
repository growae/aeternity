use super::*;

fn value(response: &Response) -> Value {
    serde_json::from_slice(&response.body).unwrap()
}
#[test]
fn original_post_body_schema_and_integer_query_cases_match() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_post_schema.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 10648);
    let mut rejected = 0;
    for row in rows {
        let input = &fixture["inputs"][row["id"].as_u64().unwrap() as usize];
        let raw = hex::decode(input["raw"].as_str().unwrap()).unwrap();
        let body = match input["mode"].as_str().unwrap() {
            "more" => Body::More(raw),
            "no_body" => Body::Complete(vec![]),
            "fin" => Body::Complete(raw),
            _ => unreachable!(),
        };
        let result = body_value(body, row["v3"].as_bool().unwrap(), 5_000_000);
        if let Some(expected) = row.get("accepted") {
            assert_eq!(
                result.unwrap_or_else(|e| panic!("{row}: {e:?}")),
                *expected,
                "{row}"
            );
        } else {
            rejected += 1;
            let error = result.unwrap_err();
            assert_eq!(
                error.status,
                row["status"].as_u64().unwrap() as u16,
                "{row}"
            );
            assert_eq!(value(&error), row["error"], "{row}");
        }
    }
    assert_eq!(rejected, 116);
}
#[test]
fn original_post_api_canonical_decoding_and_rejections_match() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_post_decode.json"
    ))
    .unwrap();
    let mut accepted = 0;
    for row in fixture["rows"].as_array().unwrap() {
        let decoded = decode(row["tx"].as_str().unwrap(), 5_000_000);
        if row["status"] == 200 {
            accepted += 1;
            let tx = decoded.unwrap_or_else(|e| panic!("{row}: {e:?}"));
            assert_eq!(row["calls"].as_array().unwrap().len(), 1);
            assert_eq!(hex::encode_upper(tx.encode()), row["calls"][0]["canonical"]);
            assert_eq!(
                hex::encode_upper(tx.hash().as_ref()),
                row["calls"][0]["hash"]
            );
            assert_eq!(
                api::encode(ApiType::TxHash, tx.hash().as_ref()),
                row["body"]["tx_hash"]
            );
        } else {
            let response = decoded.unwrap_err();
            assert_eq!(
                response.status,
                row["status"].as_u64().unwrap() as u16,
                "{row}"
            );
            assert_eq!(value(&response), row["body"], "{row}");
        }
    }
    assert_eq!(accepted, 145);
}
#[test]
fn post_service_limits_do_not_turn_resource_failures_into_invalid_transactions() {
    let config = Config::default();
    let service = Service::new(&config).unwrap();
    let mut held = vec![];
    while let Ok(permit) = service.reserve(&config) {
        held.push(permit);
    }
    assert!(!held.is_empty());
    assert_eq!(service.reserve(&config).err().unwrap().status, 503);
    drop(held);
    assert!(service.reserve(&config).is_ok());
    assert_eq!(
        body_value(Body::Complete(vec![b' '; 20]), false, 10)
            .unwrap_err()
            .status,
        503
    );
    let deep = format!("{}0{}", "[".repeat(65), "]".repeat(65));
    assert_eq!(
        body_value(Body::Complete(deep.into_bytes()), false, 1000)
            .unwrap_err()
            .status,
        503
    );
    assert_eq!(
        value(&pool_error(worker::Error::Timeout)),
        json!({"reason":"Internal server error"})
    );
    assert_eq!(
        value(&pool_error(worker::Error::Disconnected)),
        json!({"reason":"Internal server error"})
    );
    assert_eq!(
        value(&policy_error(policy::Error::TooLowMinerGasPrice)),
        json!({"reason":"Invalid tx","error_code":"too_low_gas_price_for_miner"})
    );
    assert_eq!(policy_error(policy::Error::ResourceLimit).status, 503);
}
