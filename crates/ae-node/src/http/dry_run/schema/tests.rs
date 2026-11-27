use super::*;
use std::sync::LazyLock;

static FIXTURE: LazyLock<Value> = LazyLock::new(|| {
    fn unpack(v: &Value, pool: &[Value]) -> Value {
        match v {
            Value::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                unpack(&pool[m["$ref"].as_u64().unwrap() as usize], pool)
            }
            Value::Object(m) => Value::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), unpack(v, pool)))
                    .collect(),
            ),
            Value::Array(a) => Value::Array(a.iter().map(|v| unpack(v, pool)).collect()),
            v => v.clone(),
        }
    }
    let v: Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_http_dry_run_schema.json"
    ))
    .unwrap();
    unpack(&v["root"], v["values"].as_array().unwrap())
});

#[test]
fn original_body_query_schema_and_response_handler_outcomes_match() {
    let rows = FIXTURE["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5146);
    for (index, row) in rows.iter().enumerate() {
        let input = &FIXTURE["inputs"][row["id"].as_u64().unwrap() as usize];
        let raw = hex::decode(input["raw"].as_str().unwrap()).unwrap();
        let body = match input["mode"].as_str().unwrap() {
            "no_body" => Body::Complete(&[]),
            "more" => Body::More(&raw),
            "fin" => Body::Complete(&raw),
            _ => unreachable!(),
        };
        let v3 = row["module"] == "oas_endpoints";
        let parsed = request(body, v3, row["query"]["query"].as_str().unwrap(), 5_000_000);
        let outcome = match parsed {
            Ok(parsed) => {
                assert_eq!(row["logic_reads"], 1, "row {index}");
                let mut params = json!({"DryRunInput":parsed.input});
                if v3 {
                    params["int-as-string"] = json!(parsed.strings);
                }
                assert_eq!(params, row["logic_params"], "row {index}");
                render(
                    input["code"].as_u64().unwrap() as u16,
                    input["body"].clone(),
                    v3,
                    parsed.strings,
                )
            }
            Err(error) => {
                assert_eq!(row["logic_reads"], 0, "row {index}: {error:?}");
                Err(error)
            }
        };
        let (kind, code, body) = match outcome {
            Ok(value) => ("reply", input["code"].as_u64().unwrap() as u16, value),
            Err(error) => {
                let kind = if matches!(error, Error::Stream(_)) {
                    "stream_error"
                } else {
                    "reply"
                };
                let response = error.response();
                let value = if response.body.is_empty() {
                    json!("")
                } else {
                    serde_json::from_slice(&response.body).unwrap()
                };
                (kind, response.status, value)
            }
        };
        let expected = &row["result"];
        assert_eq!(kind, expected["kind"], "row {index}, input {}", row["id"]);
        assert_eq!(json!(code), expected["response"]["code"], "row {index}");
        assert_eq!(
            body, expected["response"]["body"],
            "row {index}, input {}",
            row["id"]
        );
    }
}

#[test]
fn all_http_pattern_families_match_original_unicode_and_newline_rules() {
    let rows = FIXTURE["pattern_checks"].as_array().unwrap();
    assert_eq!(rows.len(), 504);
    for row in rows {
        assert_eq!(
            model::matches_pattern(
                row["pattern"].as_str().unwrap(),
                row["value"].as_str().unwrap()
            ),
            row["matched"].as_bool().unwrap(),
            "{row}"
        );
    }
}

#[test]
fn body_resource_stops_remain_operational_and_retryable() {
    let valid = br#"{"txs":[]}"#;
    assert!(matches!(
        request(Body::Complete(valid), true, "", 3),
        Err(Error::Capacity)
    ));
    assert!(matches!(
        request(Body::More(valid), true, "", 3),
        Err(Error::Capacity)
    ));
    let deep = format!("{}0{}", "[".repeat(65), "]".repeat(65));
    assert!(matches!(
        request(Body::Complete(deep.as_bytes()), true, "", 1000),
        Err(Error::Capacity)
    ));
    let huge = format!("{{\"txs\":[],\"n\":{}}}", "1".repeat(4097));
    assert!(matches!(
        request(Body::Complete(huge.as_bytes()), true, "", 10000),
        Err(Error::Capacity)
    ));
    assert!(request(Body::Complete(valid), true, "", 1000).is_ok());
}
