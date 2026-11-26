use super::*;
use ae_state::contracts::{Code, Contract};
fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../../ae-chain/tests/fixtures/erlang_http_contracts.json"
    ))
    .unwrap()
}
fn operation(name: &str, key: &str) -> Operation {
    Operation {
        key: key.as_bytes().to_vec(),
        kind: match name {
            "GetContract" => Kind::Metadata,
            "GetContractCode" => Kind::Code,
            _ => Kind::Proof,
        },
    }
}
fn stored(row: &Value, key: [u8; 32]) -> Contract {
    let entry = row["contracts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| hex::decode(p[0].as_str().unwrap()).unwrap() == key)
        .unwrap();
    Contract::deserialize(key, &hex::decode(entry[1].as_str().unwrap()).unwrap()).unwrap()
}
#[test]
fn original_contract_dispatcher_fields_errors_and_encoding_match_all_cases() {
    let f = fixture();
    let rows = f["dispatcher"].as_array().unwrap();
    assert_eq!(rows.len(), 351);
    for row in rows {
        let input = &row["input"];
        let op = operation(
            input["operation"].as_str().unwrap(),
            row["params"]["pubkey"].as_str().unwrap(),
        );
        let response = (|| {
            let scenario = input["scenario"].as_str().unwrap();
            if scenario == "overload" {
                return Response::unavailable(true);
            }
            if scenario == "unstable" {
                return Response::unavailable(false);
            }
            let key = match op.decode() {
                Ok(key) => key,
                Err(r) => return r,
            };
            let error = match scenario {
                "missing" | "deleted" => Some(Error::NotPresent),
                "no_state" | "no_top" => Some(Error::NoStateTrees),
                "missing_account" if op.kind == Kind::Proof => Some(Error::AccountNotPresent),
                "no_store" if op.kind != Kind::Proof => Some(Error::InvalidObject),
                "reference_missing" | "reference_chain" | "reference_no_store"
                    if op.kind == Kind::Code =>
                {
                    Some(Error::InvalidObject)
                }
                "storage_error" => Some(Error::Backend(ae_chain::repository::Error::Backend(
                    "injected".into(),
                ))),
                _ => None,
            };
            if let Some(e) = error {
                return failure(op.kind, e);
            }
            let reply = match op.kind {
                Kind::Metadata => Reply::Metadata(stored(row, key)),
                Kind::Code => {
                    let c = stored(row, key);
                    let code = match c.code {
                        Code::Inline(code) => code,
                        Code::Reference(id) => match stored(row, id.value).code {
                            Code::Inline(code) => code,
                            _ => panic!("reference"),
                        },
                    };
                    Reply::Code(code)
                }
                // The ae-chain native-state test independently constructs and
                // compares the complete proof bytes. Here test the API wrapper.
                Kind::Proof => Reply::Proof(
                    api::decode_as(ApiType::Poi, row["body"]["poi"].as_str().unwrap()).unwrap(),
                ),
            };
            body(reply)
                .map(|body| Response::json(200, body))
                .unwrap_or_else(|r| r)
        })();
        assert_eq!(
            u64::from(response.status),
            row["code"].as_u64().unwrap(),
            "{input}"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap(),
            row["body"],
            "{input}"
        );
    }
}
#[test]
fn original_contract_response_schemas_and_integer_query_versions_match() {
    let f = fixture();
    let inputs = f["inputs"].as_array().unwrap();
    let rows = f["schema"].as_array().unwrap();
    assert_eq!(inputs.len(), 360);
    assert_eq!(rows.len(), 4320);
    for row in rows {
        let input = &inputs[row["id"].as_u64().unwrap() as usize];
        let op = operation(
            input["operation"].as_str().unwrap(),
            input["bindings"]["pubkey"].as_str().unwrap(),
        );
        let v3 = row["module"] == "oas_endpoints";
        let query = row["query"]["query"].as_str().unwrap();
        let result = (|| {
            let strings = if v3 {
                super::super::transport::strings(query)?
            } else {
                false
            };
            let code = input["code"].as_u64().unwrap() as u16;
            Ok::<_, Response>(if code == 200 {
                finish(&op, input["body"].clone(), v3, strings)
            } else {
                Response::json(code, input["body"].clone())
            })
        })()
        .unwrap_or_else(|r| r);
        assert_eq!(
            u64::from(result.status),
            row["result"]["response"]["code"].as_u64().unwrap(),
            "{} {} {query}",
            row["id"],
            row["module"]
        );
        if row["result"]["kind"] == "stream_error" {
            assert!(result.body.is_empty());
        } else {
            assert_eq!(
                serde_json::from_slice::<Value>(&result.body).unwrap(),
                row["result"]["response"]["body"]
            );
        }
    }
}
#[test]
fn contract_routes_and_operational_limits_do_not_truncate_proofs_or_alias_identifiers() {
    let mut config = Config::default();
    config.groups.clear();
    for group in ["contract", "external"] {
        config.groups.insert(group.into());
        for v in ["v2", "v3"] {
            for suffix in ["", "/code", "/poi"] {
                assert!(response::route(&format!("/{v}/contracts/bad{suffix}"), &config).is_some());
            }
        }
        config.groups.clear();
    }
    assert!(response::route("/v3/contracts/bad", &config).is_none());
    for kind in [Kind::Metadata, Kind::Code, Kind::Proof] {
        assert_eq!(
            Operation {
                key: vec![b'a'; 4097],
                kind
            }
            .decode()
            .unwrap_err()
            .status,
            503
        );
        assert_eq!(
            Operation {
                key: api::encode(ApiType::AccountPubkey, &[1; 32]).into_bytes(),
                kind
            }
            .decode()
            .unwrap_err()
            .status,
            400
        );
    }
    for reply in [
        Reply::Code(vec![0; JSON_BYTES / 2]),
        Reply::Proof(vec![0; JSON_BYTES / 2]),
    ] {
        assert_eq!(body(reply).unwrap_err().status, 503);
    }
}
