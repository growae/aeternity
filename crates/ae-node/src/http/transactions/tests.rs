use super::*;

#[test]
fn original_transaction_request_and_response_schemas_match_both_versions() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_transaction_schemas.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1644);
    let mut compared = 0;
    for (i, row) in rows.iter().enumerate() {
        let hash = row["bindings"]["hash"]
            .as_str()
            .unwrap()
            .as_bytes()
            .to_vec();
        let operation = match row["operation"].as_str().unwrap() {
            "GetTransactionByHash" => Operation::Hash(hash),
            "GetMicroBlockTransactionsByHash" => Operation::Micro(hash),
            "GetMicroBlockTransactionByHashAndIndex" => Operation::Index {
                hash,
                index: row["bindings"]["index"]
                    .as_str()
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
            },
            _ => panic!("unknown operation"),
        };
        for expected in row["expected"].as_array().unwrap() {
            let v3 = expected["v3"].as_bool().unwrap();
            let strings = expected["strings"].as_bool().unwrap();
            let response = match operation.validate_path() {
                Err(response) => response,
                Ok(()) if row["code"] == 200 => {
                    finish(&operation, row["body"].clone(), v3, strings)
                }
                Ok(()) => Response::json(row["code"].as_u64().unwrap() as u16, row["body"].clone()),
            };
            assert_eq!(
                u64::from(response.status),
                expected["code"].as_u64().unwrap(),
                "row {i} {expected}"
            );
            if expected["stream"] == true {
                assert!(response.body.is_empty(), "row {i}");
            } else {
                let mut body = row["body"].clone();
                if let Some(error) = expected.get("path_error") {
                    body = error.clone();
                } else if strings {
                    response::integers_as_strings(&mut body);
                }
                assert_eq!(
                    serde_json::from_slice::<Value>(&response.body).unwrap(),
                    body,
                    "row {i} {expected}"
                );
            }
            compared += 1;
        }
    }
    assert_eq!(compared, 6576);
}

#[test]
fn transaction_paths_preserve_groups_hash_errors_and_unbounded_indices() {
    let hash = ae_codec::api::encode(ApiType::MicroBlockHash, &[9; 32]);
    let operation = Operation::Index {
        hash: hash.clone().into_bytes(),
        index: b"+0001".to_vec(),
    };
    assert!(matches!(
        operation.request().unwrap(),
        Request::MicroIndex { index: Some(1), .. }
    ));
    for raw in [
        "18446744073709551616",
        "99999999999999999999999999999999999999999999999999999999999999999999",
    ] {
        let op = Operation::Index {
            hash: hash.clone().into_bytes(),
            index: raw.as_bytes().to_vec(),
        };
        assert!(op.validate_path().is_ok());
        assert!(matches!(
            op.request().unwrap(),
            Request::MicroIndex { index: None, .. }
        ));
    }
    for op in [
        Operation::Hash(b"bad".to_vec()),
        Operation::Micro(b"bad".to_vec()),
        Operation::Index {
            hash: b"bad".to_vec(),
            index: b"1".to_vec(),
        },
    ] {
        let response = op.request().unwrap_err();
        assert_eq!(response.status, 400);
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            body,
            if matches!(op, Operation::Index { .. }) {
                json!({"reason":"Invalid hash or index"})
            } else {
                json!({"reason":"Invalid hash","error_code":"invalid_encoding"})
            }
        );
    }
    let mut config = Config::default();
    assert!(response::route("/v3/transactions/bad", &config).is_none());
    config.groups.insert("transaction".into());
    assert!(matches!(
        response::route("/v3/transactions/bad", &config),
        Some((true, response::Operation::Transactions { .. }))
    ));
    config.groups.remove("chain");
    assert!(
        response::route(
            &format!("/v2/micro-blocks/hash/{hash}/transactions"),
            &config
        )
        .is_none()
    );
}
