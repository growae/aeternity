use super::*;
use ae_codec::headers::HeaderMode;

#[test]
fn original_header_client_serialization_matches_saved_mainnet_and_constructed_headers() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_headers.json"
    ))
    .unwrap();
    let rows = corpus["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 365);
    let mut failures = 0;
    for row in rows {
        let bytes = hex::decode(row["wire"].as_str().unwrap()).unwrap();
        let value = Header::decode(&bytes, HeaderMode::Hyperchain).unwrap();
        let previous = if row["input"]["previous_type"] == "key" {
            Kind::Key
        } else {
            Kind::Micro
        };
        let seal = match row["input"]["seal"].as_str().unwrap() {
            "absent" => Some(false),
            "zeros" | "range" => Some(true),
            "decoded" => None,
            _ => unreachable!(),
        };
        match header(
            &ClientHeader {
                header: value,
                previous,
            },
            seal,
        ) {
            Ok(body) => assert_eq!(body, row["body"], "{}", row["input"]),
            Err(error) => {
                assert_eq!(error.status, 500);
                assert_eq!(row["error"]["reason"], "function_clause");
                failures += 1;
            }
        }
    }
    assert_eq!(failures, 10);
}

pub(in crate::http) fn operation(name: &str, params: &Value) -> Option<Operation> {
    let hash = || params["hash"].as_str().unwrap().as_bytes().to_vec();
    let height = || params["height"].as_str().unwrap().as_bytes().to_vec();
    Some(match name {
        "GetTopBlock" => Operation::Top { envelope: true },
        "GetTopHeader" => Operation::Top { envelope: false },
        "GetCurrentKeyBlock" => Operation::CurrentKey,
        "GetKeyBlockByHash" => Operation::KeyHash(hash()),
        "GetKeyBlockByHeight" => Operation::KeyHeight(height()),
        "GetMicroBlockHeaderByHash" => Operation::MicroHeader(hash()),
        "GetMicroBlockTransactionsCountByHash" => Operation::MicroCount(hash()),
        "GetCurrentGeneration" => Operation::CurrentGeneration,
        "GetGenerationByHash" => Operation::GenerationHash(hash()),
        "GetGenerationByHeight" => Operation::GenerationHeight(height()),
        "GetPendingKeyBlock" => return None,
        _ => panic!("unknown native operation {name}"),
    })
}

#[test]
fn original_block_and_generation_callback_responses_match() {
    // Match the controlled providers used by the native dispatcher capture.
    // Actual snapshot selection, branches and body availability are exercised
    // independently by importer and live HTTP integration tests.
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_blocks.json"
    ))
    .unwrap();
    let key = Header::decode(
        &hex::decode(corpus["headers"]["key"].as_str().unwrap()).unwrap(),
        HeaderMode::ProofOfWork,
    )
    .unwrap();
    let micro = Header::decode(
        &hex::decode(corpus["headers"]["micro"].as_str().unwrap()).unwrap(),
        HeaderMode::ProofOfWork,
    )
    .unwrap();
    let mut checked = 0;
    for row in corpus["rows"].as_array().unwrap() {
        let input = &row["input"];
        let Some(operation) = operation(input["operation"].as_str().unwrap(), &row["params"])
        else {
            continue;
        };
        let scenario = input["scenario"].as_str().unwrap();
        let previous = if input["previous_type"] == "key" {
            Kind::Key
        } else {
            Kind::Micro
        };
        let mut key = key.clone();
        let mut micro = micro.clone();
        if scenario == "genesis" {
            let Header::Key(k) = &mut key else { panic!() };
            k.common.height = 0;
        }
        if scenario == "pof" {
            let Header::Micro(m) = &mut micro else {
                panic!()
            };
            let mut h = [0; 32];
            h[31] = 33;
            m.proof_of_fraud = Some(h);
        }
        let result = match scenario {
            "storage" => Err(Response::internal()),
            "overload" => Err(Response::unavailable(true)),
            "unstable" => Err(Response::unavailable(false)),
            _ => operation.request(false).and_then(|request| {
                let reply = if scenario == "missing" {
                    Reply::Missing(match request {
                        Request::GenerationHash(_) => Missing::NotOnChain,
                        Request::KeyHeight(_) | Request::GenerationHeight(_) => {
                            Missing::ChainTooShort
                        }
                        Request::MicroHeader(_) | Request::MicroCount(_) => Missing::Micro,
                        _ => Missing::Block,
                    })
                } else if scenario == "wrong_type" {
                    Reply::Missing(if matches!(request, Request::KeyHash(_)) {
                        Missing::Key
                    } else {
                        Missing::Micro
                    })
                } else if matches!(request, Request::MicroCount(_)) {
                    Reply::Count(input["tx_count"].as_u64().unwrap_or(0) as usize)
                } else if scenario == "missing_prev" {
                    Reply::Missing(Missing::Previous)
                } else if scenario == "missing_block" && matches!(request, Request::KeyHeight(_)) {
                    Reply::Missing(Missing::Block)
                } else if matches!(
                    request,
                    Request::CurrentGeneration
                        | Request::GenerationHash(_)
                        | Request::GenerationHeight(_)
                ) {
                    let mut later = micro.clone();
                    let Header::Micro(m) = &mut later else {
                        panic!()
                    };
                    m.time = 44;
                    Reply::Generation {
                        key: ClientHeader {
                            header: key,
                            previous,
                        },
                        micros: if scenario == "genesis" {
                            vec![]
                        } else {
                            vec![micro.hash().unwrap(), later.hash().unwrap()]
                        },
                    }
                } else {
                    Reply::Header(ClientHeader {
                        header: if matches!(request, Request::MicroHeader(_))
                            || scenario == "micro_top"
                        {
                            micro
                        } else {
                            key
                        },
                        previous: if scenario == "genesis" {
                            Kind::Key
                        } else {
                            previous
                        },
                    })
                };
                body(&operation, reply)
            }),
        };
        let response = match result {
            Ok(body) => Response::json(200, body),
            Err(response) => response,
        };
        // These rows call the dispatcher directly: invalid/negative integer
        // inputs raise there, before the REST validator's separate handling.
        if matches!(
            operation,
            Operation::KeyHeight(_) | Operation::GenerationHeight(_)
        ) && matches!(
            row["params"]["height"].as_str().unwrap(),
            "-1" | "1.0" | "invalid" | "999999999999999999999999999999"
        ) {
            continue;
        }
        assert_eq!(
            response.status,
            row["code"].as_u64().unwrap() as u16,
            "{row}"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap(),
            row["body"],
            "{row}"
        );
        checked += 1;
    }
    assert_eq!(checked, 162);
}
