use super::*;
use ae_types::Hash256;

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
            for path in ["/v3/sync-status", "/v3/peers/pubkey"] {
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
            "GetStatus" => continue,
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
    assert_eq!(count, 208);
}
