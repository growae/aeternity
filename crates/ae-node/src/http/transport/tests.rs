use super::*;
use ae_types::Hash256;

#[tokio::test]
async fn original_post_wire_cases_match_request_parsing_and_validation_order() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_post_wire.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 66);
    let mut compared = 0;
    for row in rows {
        // This capture deliberately bypassed CORS middleware. The real node
        // and Rust listener intercept OPTIONS before Cowboy REST routing.
        if row["input"]["name"] == "method_OPTIONS" {
            continue;
        }
        let v3 = row["v3"].as_bool().unwrap();
        let raw = String::from_utf8(hex::decode(row["input"]["raw"].as_str().unwrap()).unwrap())
            .unwrap()
            .replacen(
                "/transactions",
                if v3 {
                    "/v3/transactions"
                } else {
                    "/v2/transactions"
                },
                1,
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (done, wait) = tokio::sync::oneshot::channel();
        let client = tokio::spawn(async move {
            let mut socket = TcpStream::connect(address).await.unwrap();
            socket.write_all(raw.as_bytes()).await.unwrap();
            let _ = wait.await;
            drop(socket);
        });
        let (mut socket, _) = listener.accept().await.unwrap();
        let config = Config {
            groups: ["transaction".into()].into(),
            timeout: std::time::Duration::from_millis(25),
            ..Default::default()
        };
        let outcome = async {
            let read = read(&mut socket, &config).await.map_err(Response::empty)?;
            plan(&read.request, &config)?;
            let data =
                body::read(&mut socket, &read.request.headers, read.prefetched, &config).await?;
            let value = crate::http::post::body_value(data, v3, config.body_bytes)?;
            if v3 {
                strings(&read.request.query)?;
            }
            Ok::<_, Response>(value)
        }
        .await;
        let _ = done.send(());
        client.await.unwrap();
        match outcome {
            Ok(value) => {
                assert_eq!(row["status"], 200, "{row}");
                let expected = &row["logic"][0][if v3 { "EncodedTx" } else { "Tx" }];
                assert_eq!(value, *expected, "{row}");
            }
            Err(response) => {
                assert_eq!(
                    u64::from(response.status),
                    row["status"].as_u64().unwrap(),
                    "{row}"
                );
                let value: serde_json::Value = if response.body.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&response.body).unwrap()
                };
                assert_eq!(value, row["body"], "{row}");
            }
        }
        compared += 1;
    }
    assert_eq!(compared, 64);
}

#[test]
fn original_cowboy_paths_preserve_segment_boundaries_dots_and_error_bodies() {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_paths.json"
    ))
    .unwrap();
    let rows = corpus["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 252);
    for pair in rows.chunks_exact(2) {
        let row = if pair[0]["tokens"][0] == "7633" {
            &pair[1]
        } else {
            &pair[0]
        };
        let raw = row["input"]["path"].as_str().unwrap();
        let parts = path_segments(raw);
        if row["tokens"] == "badrequest" {
            assert!(parts.is_none());
        } else {
            let expected: Vec<_> = row["tokens"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| hex::decode(p.as_str().unwrap()).unwrap())
                .collect();
            assert_eq!(parts, Some(expected), "{row}");
        }
        let request = Request {
            method: "GET".into(),
            path: raw.into(),
            query: String::new(),
            headers: Headers::new(),
        };
        let planned = plan(&request, &Config::default());
        let expected = &row["result"];
        if let Some(error) = expected["route_error"].as_str() {
            assert_eq!(
                planned.unwrap_err().status,
                if error == "badrequest" { 400 } else { 404 },
                "{row}"
            );
            continue;
        }
        let (_, route) = response::route(raw, &Config::default()).unwrap();
        match route {
            Operation::Blocks { operation, .. } => {
                use super::super::blocks::Operation as B;
                let (name, binding) = match operation {
                    B::Top { envelope: true } => ("GetTopBlock", None),
                    B::Top { envelope: false } => ("GetTopHeader", None),
                    B::CurrentKey => ("GetCurrentKeyBlock", None),
                    B::KeyHash(h) => ("GetKeyBlockByHash", Some(("hash", h))),
                    B::KeyHeight(h) => ("GetKeyBlockByHeight", Some(("height", h))),
                    B::MicroHeader(h) => ("GetMicroBlockHeaderByHash", Some(("hash", h))),
                    B::MicroCount(h) => ("GetMicroBlockTransactionsCountByHash", Some(("hash", h))),
                    B::CurrentGeneration => ("GetCurrentGeneration", None),
                    B::GenerationHash(h) => ("GetGenerationByHash", Some(("hash", h))),
                    B::GenerationHeight(h) => ("GetGenerationByHeight", Some(("height", h))),
                };
                assert_eq!(expected["operation"], name);
                if let Some((name, value)) = binding {
                    assert_eq!(
                        value,
                        hex::decode(expected["bindings"][name].as_str().unwrap()).unwrap()
                    );
                }
            }
            Operation::Status { .. } => assert_eq!(expected["operation"], "GetStatus"),
            _ => panic!("unexpected captured route"),
        }
        assert_eq!(planned.is_ok(), expected["logic_reads"] == 1, "{row}");
        let response = super::dispatch(
            planned,
            Err(Response::json(500, json!({"reason":"controlled provider"}))),
        );
        assert_eq!(
            response.status,
            expected["result"]["response"]["code"].as_u64().unwrap() as u16,
            "{row}"
        );
        if expected["result"]["kind"] == "stream_error" {
            assert!(response.body.is_empty());
            assert!(response.headers.is_empty());
        } else {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
                expected["result"]["response"]["body"],
                "{row}"
            );
        }
    }
}

#[test]
fn original_block_request_coercion_and_schema_wrappers_match_both_versions() {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_block_schema.json"
    ))
    .unwrap();
    let mut checked = 0;
    for row in corpus["rows"].as_array().unwrap() {
        let input = corpus["inputs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["id"] == row["id"])
            .unwrap();
        let operation = super::super::blocks::tests::operation(
            input["operation"].as_str().unwrap(),
            &input["bindings"],
        );
        if operation.is_none() && input["operation"] != "GetPendingKeyBlock" {
            continue;
        }
        let v3 = row["module"] == "oas_endpoints";
        // The native capture provides parsed Cowboy bindings directly. This
        // tests schema processing, including empty bindings, independently of
        // path matching (covered by the original Cowboy path fixture above).
        let plan = (|| {
            let strings = if v3 {
                strings(row["query"]["query"].as_str().unwrap())?
            } else {
                false
            };
            if let Some(operation) = operation {
                operation.validate_path(v3)?;
                Ok((Operation::Blocks { operation, v3 }, strings))
            } else {
                Ok((Operation::PendingKey, strings))
            }
        })();
        assert_eq!(plan.is_ok(), row["logic_reads"] == 1, "{row}");
        let code = input["code"].as_u64().unwrap() as u16;
        let body = input["body"].clone();
        let result = if code == 200 {
            Ok(body)
        } else {
            Err(Response::json(code, body))
        };
        let response = super::dispatch(plan, result);
        assert_eq!(
            response.status,
            row["result"]["response"]["code"].as_u64().unwrap() as u16,
            "{row}"
        );
        if row["result"]["kind"] == "stream_error" {
            assert!(response.body.is_empty(), "{row}");
            assert!(response.headers.is_empty());
        } else {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
                row["result"]["response"]["body"],
                "{row}"
            );
        }
        checked += 1;
    }
    assert_eq!(checked, 2616);
}

fn dispatch(request: &Request, config: &Config, selected: Result<Current, Response>) -> Response {
    let plan = plan(request, config);
    let height = matches!(plan, Ok((Operation::Current { height: true }, _)));
    super::dispatch(plan, selected.map(|s| response::current(s, height)))
}

fn corpus() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_surface.json"
    ))
    .unwrap()
}
#[test]
fn original_current_key_callbacks_and_failure_responses_match_both_api_versions() {
    let corpus = corpus();
    let rows = corpus["dispatch"].as_array().unwrap();
    assert_eq!(rows.len(), 96);
    let config = Config::default();
    for row in rows {
        for version in ["v2", "v3"] {
            let height = row["operation"] == "GetCurrentKeyBlockHeight";
            let request = Request {
                method: "GET".into(),
                path: format!(
                    "/{version}/key-blocks/current/{}",
                    if height { "height" } else { "hash" }
                ),
                query: String::new(),
                headers: Headers::new(),
            };
            let selected = match row["failure"].as_str().unwrap() {
                "none" => Ok(Current {
                    revision: 4,
                    top: Hash256([99; 32]),
                    key: Hash256(
                        hex::decode(row["hash"].as_str().unwrap())
                            .unwrap()
                            .try_into()
                            .unwrap(),
                    ),
                    height: row["height"].as_u64().unwrap(),
                }),
                "overload" => Err(failure(chain::Error::Busy)),
                "unstable" => Err(failure(chain::Error::Timeout)),
                "storage" => Err(failure(chain::Error::Storage(
                    ae_chain::repository::Error::Backend("native controlled failure".into()),
                ))),
                _ => unreachable!(),
            };
            let response = dispatch(&request, &config, selected);
            assert_eq!(
                response.status,
                row["code"].as_u64().unwrap() as u16,
                "{row}"
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
                row["body"],
                "{row}"
            );
            let mut headers = response.headers;
            headers.remove("content-type");
            assert_eq!(
                serde_json::to_value(headers).unwrap(),
                row["headers"],
                "{row}"
            );
        }
    }
}
#[test]
fn original_boolean_coercion_and_query_version_scope() {
    for row in corpus()["booleans"].as_array().unwrap() {
        let query = match &row["input"] {
            serde_json::Value::Bool(true) => "int-as-string".into(),
            serde_json::Value::Bool(false) => "int-as-string=false".into(),
            serde_json::Value::String(value) => format!("int-as-string={value}"),
            _ => unreachable!(),
        };
        match strings(&query) {
            Ok(value) => assert_eq!(json!({"value":value}), row["result"]),
            Err(response) => {
                // The standalone coercion fixture reports a parameter error;
                // the actual schema wrapper cannot encode it and emits 500.
                assert!(row["result"]["error"].is_object());
                assert_eq!(response.status, 500);
                assert!(response.body.is_empty());
            }
        }
    }
    assert!(strings("x=1&int%2das%2dstring=TRUE&int-as-string=false").unwrap());
    assert!(!strings("int-as-string=false&int-as-string=true").unwrap());
    assert!(strings("ignored=%zz").is_err());
    let current = Current {
        revision: 1,
        top: Hash256([0; 32]),
        key: Hash256([0; 32]),
        height: u64::MAX,
    };
    for (version, expected) in [
        ("v2", json!({"height":u64::MAX})),
        ("v3", json!({"height":u64::MAX.to_string()})),
    ] {
        let request = Request {
            method: "GET".into(),
            path: format!("/{version}/key-blocks/current/height"),
            query: "int-as-string=true".into(),
            headers: Headers::new(),
        };
        let response = dispatch(&request, &Config::default(), Ok(current));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
            expected
        );
    }
    for v3 in [false, true] {
        for query in ["int-as-string=true", "int-as-string=invalid", "ignored=%zz"] {
            let request = Request {
                method: "GET".into(),
                path: format!("/{}/sync-status", if v3 { "v3" } else { "v2" }),
                query: query.into(),
                headers: Headers::new(),
            };
            assert_eq!(
                plan(&request, &Config::default()).unwrap(),
                (Operation::SyncStatus { v3 }, false)
            );
        }
    }
}
#[test]
fn routing_methods_groups_and_preflight_are_separate() {
    let mut config = Config::default();
    let mut request = Request {
        method: "OPTIONS".into(),
        path: "/unknown".into(),
        query: String::new(),
        headers: Headers::new(),
    };
    assert_eq!(
        dispatch(&request, &config, Err(Response::unavailable(false))).status,
        200
    );
    request.method = "GET".into();
    assert_eq!(
        dispatch(&request, &config, Err(Response::unavailable(false))).status,
        404
    );
    request.path = "/v3/key-blocks/current/height".into();
    request.method = "POST".into();
    assert_eq!(
        dispatch(&request, &config, Err(Response::unavailable(false))).status,
        405
    );
    request.method = "GET".into();
    request.headers.insert("accept".into(), "text/html".into());
    assert_eq!(
        dispatch(&request, &config, Err(Response::unavailable(false))).status,
        406
    );
    request.headers.clear();
    config.target = super::super::Target::Internal;
    assert_eq!(
        dispatch(&request, &config, Err(Response::unavailable(false))).status,
        404
    );
    config.target = super::super::Target::External;
    config.groups.clear();
    assert_eq!(
        dispatch(&request, &config, Err(Response::unavailable(false))).status,
        404
    );
    config.groups.insert("external".into());
    assert_eq!(
        dispatch(&request, &config, Err(Response::unavailable(false))).status,
        503
    );
    for target in [
        super::super::Target::External,
        super::super::Target::Internal,
        super::super::Target::Rosetta,
        super::super::Target::RosettaOffline,
    ] {
        config.target = target;
        for group in ["chain", "node_info", "external"] {
            config.groups = [group.into()].into();
            for path in ["/v3/sync-status", "/v3/peers/pubkey", "/v3/status"] {
                request.path = path.into();
                assert_eq!(
                    plan(&request, &config).is_ok(),
                    target == super::super::Target::External && group != "chain"
                );
            }
        }
    }
}

#[test]
fn original_full_router_groups_match_every_implemented_route_without_exposing_other_targets() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_routing_groups.json"
    ))
    .unwrap();
    // This inventory grows as actual endpoint implementations are connected.
    let implemented = [
        "GetProtocolParameters",
        "GetNodeSettings",
        "GetCurrency",
        "GetAccountByPubkey",
        "GetAccountByPubkeyAndHash",
        "GetAccountByPubkeyAndHeight",
        "GetAccountNextNonce",
        "GetPendingAccountTransactionsByPubkey",
        "GetContract",
        "GetContractCode",
        "GetContractPoI",
        "GetChannelByPubkey",
        "GetNameEntryByName",
        "GetNameEntryByNameHash",
        "GetAuctionEntryByName",
        "GetOracleByPubkey",
        "GetOracleQueriesByPubkey",
        "GetOracleQueryByPubkeyAndQueryId",
        "GetTopBlock",
        "GetTopHeader",
        "GetCurrentGeneration",
        "GetGenerationByHash",
        "GetGenerationByHeight",
        "GetCurrentKeyBlock",
        "GetCurrentKeyBlockHash",
        "GetCurrentKeyBlockHeight",
        "GetKeyBlockByHash",
        "GetKeyBlockByHeight",
        "GetPendingKeyBlock",
        "GetMicroBlockHeaderByHash",
        "GetMicroBlockTransactionsByHash",
        "GetMicroBlockTransactionsCountByHash",
        "GetMicroBlockTransactionByHashAndIndex",
        "GetPeerPubkey",
        "GetStatus",
        "GetSyncStatus",
        "PostTransaction",
        "GetTransactionByHash",
        "GetTransactionInfoByHash",
    ];
    let catalog: Vec<_> = ["swagger2", "oas3"]
        .into_iter()
        .flat_map(|v| fixture["tables"][v].as_array().unwrap())
        .collect();
    assert_eq!(implemented.len(), 39);
    assert_eq!(
        catalog
            .iter()
            .filter(|op| implemented.contains(&op["id"].as_str().unwrap()))
            .count(),
        72
    );
    assert_eq!(fixture["rows"].as_array().unwrap().len(), 280);
    let mut checked = 0;
    for row in fixture["rows"].as_array().unwrap() {
        let target = match row["target"].as_str().unwrap() {
            "external" => super::super::Target::External,
            "internal" => super::super::Target::Internal,
            "rosetta" => super::super::Target::Rosetta,
            "rosetta_offline" => super::super::Target::RosettaOffline,
            "unknown" => {
                assert_eq!(row["paths"].as_array().unwrap().len(), 1);
                continue;
            }
            _ => panic!("target"),
        };
        let config = Config {
            target,
            groups: row["groups"]
                .as_array()
                .unwrap()
                .iter()
                .map(|g| g.as_str().unwrap().to_owned())
                .collect(),
            ..Default::default()
        };
        for op in &catalog {
            let path = op["path"].as_str().unwrap();
            let pattern = path.replace('{', ":").replace('}', "");
            let path = path
                .split('/')
                .map(|p| if p.starts_with('{') { "1" } else { p })
                .collect::<Vec<_>>()
                .join("/");
            let enabled = implemented.contains(&op["id"].as_str().unwrap())
                && row["paths"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|p| p[0] == pattern && p[1] == "aehttp_api_handler");
            let mut request = Request {
                method: op["method"].as_str().unwrap().to_ascii_uppercase(),
                path,
                query: String::new(),
                headers: Headers::from([
                    ("content-type".into(), "application/json".into()),
                    ("content-length".into(), "0".into()),
                ]),
            };
            let result = plan(&request, &config);
            assert_eq!(
                result.is_ok(),
                enabled,
                "{} {} {}: {result:?}",
                row["target"],
                row["groups"],
                op["path"]
            );
            if enabled {
                let method = request.method.clone();
                request.method = "DELETE".into();
                let rejected = plan(&request, &config).unwrap_err();
                assert_eq!(rejected.status, 405);
                assert_eq!(rejected.headers["allow"], method);
            } else {
                assert_eq!(result.unwrap_err().status, 404);
            }
            checked += 1;
        }
    }
    assert_eq!(checked, 33600);
}

#[test]
fn json_accept_uses_the_most_specific_range_and_bounded_quality() {
    for (accept, expected) in [
        ("*/*", true),
        ("application/*;q=0.3", true),
        ("application/json;q=0, */*;q=1", false),
        ("application/*;q=0, application/json;q=0.001", true),
        ("application/json;q=0.0000", false),
        ("application/json;profile=unknown, text/*", false),
    ] {
        assert_eq!(
            accepts_json(&Headers::from([("accept".into(), accept.into())])),
            expected,
            "{accept}"
        );
    }
}

#[test]
fn original_cowboy_queries_match_supported_operations_and_error_headers() {
    let native: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_queries.json"
    ))
    .unwrap();
    let mut count = 0;
    for row in native["rows"].as_array().unwrap() {
        let suffix = match row["id"].as_str().unwrap() {
            "GetCurrentKeyBlockHeight" => "/key-blocks/current/height",
            "GetCurrentKeyBlockHash" => "/key-blocks/current/hash",
            "GetSyncStatus" => "/sync-status",
            "GetPeerPubkey" => "/peers/pubkey",
            "GetStatus" => "/status",
            _ => unreachable!(),
        };
        let input = native["inputs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["id"] == row["id"])
            .unwrap();
        let v3 = row["module"] == "oas_endpoints";
        let request = Request {
            method: "GET".into(),
            path: format!("/{}{suffix}", if v3 { "v3" } else { "v2" }),
            query: row["query"]["query"].as_str().unwrap().into(),
            headers: [("origin".into(), "https://client.example".into())].into(),
        };
        let config = Config::default();
        let planned = plan(&request, &config);
        assert_eq!(
            u64::from(planned.is_ok()),
            row["logic_reads"].as_u64().unwrap(),
            "{row}"
        );
        let body = if row["id"] == "GetPeerPubkey" {
            super::super::key::body(ae_types::PeerPublicKey(
                hex::decode(native["peer_key"].as_str().unwrap())
                    .unwrap()
                    .try_into()
                    .unwrap(),
            ))
        } else {
            input["body"].clone()
        };
        let mut response = super::dispatch(planned, Ok(body));
        response.stamp(&config, &request.headers, Some(99));
        let expected = &row["result"]["response"];
        assert_eq!(
            response.status,
            expected["code"].as_u64().unwrap() as u16,
            "{row}"
        );
        if row["result"]["kind"] == "stream_error" {
            assert!(response.body.is_empty(), "{row}");
            assert!(response.headers.is_empty(), "{row}");
            assert_eq!(expected["headers"], json!({"content-length":"0"}));
        } else {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
                expected["body"],
                "{row}"
            );
            for name in ["x-ae-height", "access-control-allow-origin"] {
                assert_eq!(
                    response.headers[name],
                    expected["headers"][name].as_str().unwrap()
                );
            }
        }
        count += 1;
    }
    assert_eq!(count, 260);
}

#[test]
fn original_oracle_http_wrapper_matches_all_parameters_schemas_versions_and_error_headers() {
    use super::super::oracles::Selection;
    use ae_state::oracles::QueryFilter;
    let f: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_oracle_schema.json"
    ))
    .unwrap();
    let inputs = f["inputs"].as_array().unwrap();
    let rows = f["schema"].as_array().unwrap();
    assert_eq!(inputs.len(), 716);
    assert_eq!(rows.len(), 8592);
    let config = Config {
        groups: ["oracle".into()].into(),
        ..Default::default()
    };
    for row in rows {
        let input = &inputs[row["id"].as_u64().unwrap() as usize];
        let v3 = row["module"] == "oas_endpoints";
        let suffix = match input["operation"].as_str().unwrap() {
            "GetOracleByPubkey" => String::new(),
            "GetOracleQueriesByPubkey" => "/queries".into(),
            _ => format!(
                "/queries/{}",
                input["bindings"]["query-id"].as_str().unwrap()
            ),
        };
        let base = input["query_base"].as_str().unwrap();
        let q = row["query"]["query"].as_str().unwrap();
        let request = Request {
            method: "GET".into(),
            path: format!(
                "/{}/oracles/{}{}",
                if v3 { "v3" } else { "v2" },
                input["bindings"]["pubkey"].as_str().unwrap(),
                suffix
            ),
            query: if base.is_empty() {
                q.into()
            } else {
                format!("{base}&{q}")
            },
            headers: Headers::from([("origin".into(), "https://client.example".into())]),
        };
        let planned = plan(&request, &config);
        assert_eq!(
            usize::from(planned.is_ok()),
            row["logic_reads"].as_u64().unwrap() as usize,
            "{} {} {}",
            row["id"],
            row["module"],
            request.query
        );
        if let Ok((Operation::Oracles { operation, .. }, strings)) = &planned {
            let mut params = json!({"pubkey":super::super::text::binary(&operation.key)});
            if v3 {
                params["int-as-string"] = json!(strings);
            }
            match &operation.selection {
                Selection::Metadata => (),
                Selection::Query(id) => params["query-id"] = json!(super::super::text::binary(id)),
                Selection::Queries {
                    cursor,
                    filter,
                    limit,
                } => {
                    params["from"] = match cursor {
                        None => json!("undefined"),
                        Some(None) => json!(true),
                        Some(Some(v)) => json!(super::super::text::binary(v)),
                    };
                    params["limit"] = json!(limit);
                    params["type"] = json!(match filter {
                        QueryFilter::All => "all",
                        QueryFilter::Open => "open",
                        QueryFilter::Closed => "closed",
                    });
                }
            }
            assert_eq!(params, row["logic_params"], "{}", request.query);
        }
        let code = input["code"].as_u64().unwrap() as u16;
        let controlled = if code == 200 {
            Ok(input["body"].clone())
        } else {
            Err(Response::json(code, input["body"].clone()))
        };
        let mut response = super::dispatch(planned, controlled);
        response.stamp(&config, &request.headers, Some(99));
        let expected = &row["result"]["response"];
        assert_eq!(
            u64::from(response.status),
            expected["code"].as_u64().unwrap(),
            "{} {} {}",
            row["id"],
            row["module"],
            request.query
        );
        if row["result"]["kind"] == "stream_error" {
            assert!(response.body.is_empty());
        } else {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
                expected["body"],
                "{} {} {}",
                row["id"],
                row["module"],
                request.query
            );
        }
        for name in ["access-control-allow-origin", "x-ae-height"] {
            assert_eq!(
                response.headers.get(name).map(String::as_str),
                expected["headers"][name].as_str()
            );
        }
    }
}

#[test]
fn original_name_http_wrapper_matches_all_raw_bindings_schemas_versions_and_errors() {
    use super::super::names;
    use ae_chain::name::Kind;
    let f: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_name_schema.json"
    ))
    .unwrap();
    let inputs = f["inputs"].as_array().unwrap();
    let rows = f["schema"].as_array().unwrap();
    assert_eq!(inputs.len(), 747);
    assert_eq!(rows.len(), 11024);
    let config = Config {
        groups: ["name_service".into()].into(),
        ..Default::default()
    };
    for row in rows {
        let input = &inputs[row["id"].as_u64().unwrap() as usize];
        let v3 = row["module"] == "oas_endpoints";
        let (kind, field, suffix) = match input["operation"].as_str().unwrap() {
            "GetNameEntryByName" => (Kind::Name, "name", "names"),
            "GetNameEntryByNameHash" => (Kind::Hash, "name_hash", "names/hash"),
            _ => (Kind::Auction, "name", "auctions"),
        };
        assert!(v3 || kind != Kind::Hash);
        let raw = hex::decode(input["bindings_hex"][field].as_str().unwrap()).unwrap();
        let encoded: String = raw.iter().map(|b| format!("%{b:02X}")).collect();
        let request = Request {
            method: "GET".into(),
            path: format!("/{}/{suffix}/{encoded}", if v3 { "v3" } else { "v2" }),
            query: row["query"]["query"].as_str().unwrap().into(),
            headers: Headers::from([("origin".into(), "https://client.example".into())]),
        };
        let operation = Operation::Names {
            operation: names::Operation {
                input: raw.clone(),
                kind,
            },
            v3,
        };
        // The native wrapper harness injects bindings after Cowboy routing.
        // Empty/dot bindings cannot be produced by these literal URL paths;
        // test their wrapper behavior without changing router normalization.
        let planned = if matches!(raw.as_slice(), b"" | b"." | b"..") {
            assert!(response::route(&request.path, &config).is_none());
            super::plan_routed(&request, v3, operation)
        } else {
            let routed = response::route(&request.path, &config).unwrap();
            assert_eq!(routed, (v3, operation));
            plan(&request, &config)
        };
        assert_eq!(
            usize::from(planned.is_ok()),
            row["logic_reads"].as_u64().unwrap() as usize,
            "{} {} {}",
            row["id"],
            row["module"],
            request.query
        );
        if let Ok((Operation::Names { operation, .. }, strings)) = &planned {
            let mut params = serde_json::Map::new();
            params.insert(
                field.into(),
                json!(super::super::text::binary(&operation.input)),
            );
            if v3 {
                params.insert("int-as-string".into(), json!(strings));
            }
            assert_eq!(serde_json::Value::Object(params), row["logic_params"]);
        }
        let code = input["code"].as_u64().unwrap() as u16;
        let controlled = if code == 200 {
            Ok(input["body"].clone())
        } else {
            let mut r = Response::json(code, input["body"].clone());
            for (k, v) in input["headers"].as_object().unwrap() {
                r.headers.insert(k.clone(), v.as_str().unwrap().into());
            }
            Err(r)
        };
        let mut response = super::dispatch(planned, controlled);
        response.stamp(&config, &request.headers, Some(99));
        let expected = &row["result"]["response"];
        assert_eq!(
            u64::from(response.status),
            expected["code"].as_u64().unwrap(),
            "{} {} {}",
            row["id"],
            row["module"],
            request.query
        );
        if row["result"]["kind"] == "stream_error" {
            assert!(response.body.is_empty());
        } else {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
                expected["body"],
                "{} {} {}",
                row["id"],
                row["module"],
                request.query
            );
        }
        for name in ["access-control-allow-origin", "x-ae-height", "retry-after"] {
            assert_eq!(
                response.headers.get(name).map(String::as_str),
                expected["headers"][name].as_str()
            );
        }
    }
}

#[test]
fn original_channel_http_wrapper_matches_every_version_schema_integer_mode_and_error() {
    let f: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_channel_schema.json"
    ))
    .unwrap();
    let inputs = f["inputs"].as_array().unwrap();
    let rows = f["schema"].as_array().unwrap();
    assert_eq!(inputs.len(), 338);
    assert_eq!(rows.len(), 5408);
    let cfg = Config {
        groups: ["channel".into()].into(),
        ..Default::default()
    };
    for row in rows {
        let input = &inputs[row["id"].as_u64().unwrap() as usize];
        let v3 = row["module"] == "oas_endpoints";
        let key = hex::decode(input["bindings_hex"]["pubkey"].as_str().unwrap()).unwrap();
        let encoded: String = key.iter().map(|b| format!("%{b:02X}")).collect();
        let req = Request {
            method: "GET".into(),
            path: format!("/{}/channels/{encoded}", if v3 { "v3" } else { "v2" }),
            query: row["query"]["query"].as_str().unwrap().into(),
            headers: Headers::from([("origin".into(), "https://client.example".into())]),
        };
        let planned = plan(&req, &cfg);
        assert_eq!(
            usize::from(planned.is_ok()),
            row["logic_reads"].as_u64().unwrap() as usize,
            "{} {} {}",
            row["id"],
            row["module"],
            req.query
        );
        if let Ok((Operation::Channels { operation, .. }, strings)) = &planned {
            assert_eq!(operation.key, key);
            let mut params = json!({"pubkey":super::super::text::binary(&key)});
            if v3 {
                params["int-as-string"] = json!(strings);
            }
            assert_eq!(params, row["logic_params"]);
        }
        let code = input["code"].as_u64().unwrap() as u16;
        let result = if code == 200 {
            Ok(input["body"].clone())
        } else {
            let mut r = Response::json(code, input["body"].clone());
            for (k, v) in input["headers"].as_object().unwrap() {
                r.headers.insert(k.clone(), v.as_str().unwrap().into());
            }
            Err(r)
        };
        let mut r = super::dispatch(planned, result);
        r.stamp(&cfg, &req.headers, Some(99));
        let expected = &row["result"]["response"];
        assert_eq!(
            u64::from(r.status),
            expected["code"].as_u64().unwrap(),
            "{} {} {}",
            row["id"],
            row["module"],
            req.query
        );
        if row["result"]["kind"] == "stream_error" {
            assert!(r.body.is_empty());
        } else {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&r.body).unwrap(),
                expected["body"],
                "{} {} {}",
                row["id"],
                row["module"],
                req.query
            );
        }
        for name in ["access-control-allow-origin", "x-ae-height", "retry-after"] {
            assert_eq!(
                r.headers.get(name).map(String::as_str),
                expected["headers"][name].as_str()
            );
        }
    }
}

#[test]
fn original_currency_wrapper_matches_integer_modes_required_fields_and_errors() {
    let f: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_currency_schema.json"
    ))
    .unwrap();
    let inputs = f["inputs"].as_array().unwrap();
    let rows = f["schema"].as_array().unwrap();
    assert_eq!(inputs.len(), 105);
    assert_eq!(rows.len(), 840);
    let cfg = Config {
        groups: ["node_info".into()].into(),
        ..Default::default()
    };
    for row in rows {
        let input = &inputs[row["id"].as_u64().unwrap() as usize];
        assert_eq!(row["module"], "oas_endpoints");
        let req = Request {
            method: "GET".into(),
            path: "/v3/currency".into(),
            query: row["query"]["query"].as_str().unwrap().into(),
            headers: Headers::from([("origin".into(), "https://client.example".into())]),
        };
        let planned = plan(&req, &cfg);
        assert_eq!(
            usize::from(planned.is_ok()),
            row["logic_reads"].as_u64().unwrap() as usize,
            "{} {} {}",
            row["id"],
            row["module"],
            req.query
        );
        if let Ok((Operation::Currency, strings)) = &planned {
            assert_eq!(json!({"int-as-string":strings}), row["logic_params"]);
        }
        let code = input["code"].as_u64().unwrap() as u16;
        let result = if code == 200 {
            Ok(input["body"].clone())
        } else {
            let mut r = Response::json(code, input["body"].clone());
            for (k, v) in input["headers"].as_object().unwrap() {
                r.headers.insert(k.clone(), v.as_str().unwrap().into());
            }
            Err(r)
        };
        let mut r = super::dispatch(planned, result);
        r.stamp(&cfg, &req.headers, Some(99));
        let expected = &row["result"]["response"];
        assert_eq!(
            u64::from(r.status),
            expected["code"].as_u64().unwrap(),
            "{} {} {}",
            row["id"],
            row["module"],
            req.query
        );
        if row["result"]["kind"] == "stream_error" {
            assert!(r.body.is_empty());
        } else {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&r.body).unwrap(),
                expected["body"],
                "{} {} {}",
                row["id"],
                row["module"],
                req.query
            );
        }
        for name in ["access-control-allow-origin", "x-ae-height", "retry-after"] {
            assert_eq!(
                r.headers.get(name).map(String::as_str),
                expected["headers"][name].as_str()
            );
        }
    }
}

#[test]
fn original_node_settings_wrapper_matches_integer_modes_required_fields_and_errors() {
    let f: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_node_settings_schema.json"
    ))
    .unwrap();
    let inputs = f["inputs"].as_array().unwrap();
    let rows = f["schema"].as_array().unwrap();
    assert_eq!(inputs.len(), 165);
    assert_eq!(rows.len(), 1320);
    let cfg = Config {
        groups: ["node_settings".into()].into(),
        ..Default::default()
    };
    for row in rows {
        let input = &inputs[row["id"].as_u64().unwrap() as usize];
        assert_eq!(row["module"], "oas_endpoints");
        let req = Request {
            method: "GET".into(),
            path: "/v3/node-settings".into(),
            query: row["query"]["query"].as_str().unwrap().into(),
            headers: Headers::from([("origin".into(), "https://client.example".into())]),
        };
        let planned = plan(&req, &cfg);
        assert_eq!(
            usize::from(planned.is_ok()),
            row["logic_reads"].as_u64().unwrap() as usize,
            "{} {} {}",
            row["id"],
            row["module"],
            req.query
        );
        if let Ok((Operation::Settings, strings)) = &planned {
            assert_eq!(json!({"int-as-string":strings}), row["logic_params"]);
        }
        let code = input["code"].as_u64().unwrap() as u16;
        let result = if code == 200 {
            Ok(input["body"].clone())
        } else {
            let mut r = Response::json(code, input["body"].clone());
            for (k, v) in input["headers"].as_object().unwrap() {
                r.headers.insert(k.clone(), v.as_str().unwrap().into());
            }
            Err(r)
        };
        let mut r = super::dispatch(planned, result);
        r.stamp(&cfg, &req.headers, Some(99));
        let expected = &row["result"]["response"];
        assert_eq!(
            u64::from(r.status),
            expected["code"].as_u64().unwrap(),
            "{} {} {}",
            row["id"],
            row["module"],
            req.query
        );
        if row["result"]["kind"] == "stream_error" {
            assert!(r.body.is_empty());
        } else {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&r.body).unwrap(),
                expected["body"],
                "{} {} {}",
                row["id"],
                row["module"],
                req.query
            );
        }
        for name in ["access-control-allow-origin", "x-ae-height", "retry-after"] {
            assert_eq!(
                r.headers.get(name).map(String::as_str),
                expected["headers"][name].as_str()
            );
        }
    }
}

#[test]
fn original_protocol_parameters_wrapper_matches_integer_modes_required_fields_and_errors() {
    let f: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_protocol_parameters_schema.json"
    ))
    .unwrap();
    let inputs = f["inputs"].as_array().unwrap();
    let rows = f["schema"].as_array().unwrap();
    assert_eq!(inputs.len(), 184);
    assert_eq!(rows.len(), 1472);
    let cfg = Config {
        groups: ["node_info".into()].into(),
        ..Default::default()
    };
    for row in rows {
        let input = &inputs[row["id"].as_u64().unwrap() as usize];
        assert_eq!(row["module"], "oas_endpoints");
        let req = Request {
            method: "GET".into(),
            path: "/v3/protocol-parameters".into(),
            query: row["query"]["query"].as_str().unwrap().into(),
            headers: Headers::from([("origin".into(), "https://client.example".into())]),
        };
        let planned = plan(&req, &cfg);
        assert_eq!(
            usize::from(planned.is_ok()),
            row["logic_reads"].as_u64().unwrap() as usize,
            "{} {} {}",
            row["id"],
            row["module"],
            req.query
        );
        if let Ok((Operation::Parameters, strings)) = &planned {
            assert_eq!(json!({"int-as-string":strings}), row["logic_params"]);
        }
        let code = input["code"].as_u64().unwrap() as u16;
        let result = if code == 200 {
            Ok(input["body"].clone())
        } else {
            let mut r = Response::json(code, input["body"].clone());
            for (k, v) in input["headers"].as_object().unwrap() {
                r.headers.insert(k.clone(), v.as_str().unwrap().into());
            }
            Err(r)
        };
        let mut r = super::dispatch(planned, result);
        r.stamp(&cfg, &req.headers, Some(99));
        let expected = &row["result"]["response"];
        assert_eq!(
            u64::from(r.status),
            expected["code"].as_u64().unwrap(),
            "{} {} {}",
            row["id"],
            row["module"],
            req.query
        );
        if row["result"]["kind"] == "stream_error" {
            assert!(r.body.is_empty());
        } else {
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&r.body).unwrap(),
                expected["body"],
                "{} {} {}",
                row["id"],
                row["module"],
                req.query
            );
        }
        for name in ["access-control-allow-origin", "x-ae-height", "retry-after"] {
            assert_eq!(
                r.headers.get(name).map(String::as_str),
                expected["headers"][name].as_str()
            );
        }
    }
}
