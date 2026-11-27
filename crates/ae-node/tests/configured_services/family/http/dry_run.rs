use super::*;
use ae_chain::repository::Read;
use ae_node::http::{DryRunConfig, Target};
use ae_tx::CanonicalTransaction;

fn worker() -> DryRunConfig {
    DryRunConfig::new(
        env!("CARGO_BIN_EXE_ae-dry-run-worker").into(),
        std::env::temp_dir(),
    )
}
fn listeners() -> Vec<Config> {
    vec![
        Config {
            groups: ["dry-run".into(), "chain".into()].into(),
            timeout: Duration::from_secs(5),
            ..config()
        },
        Config {
            target: Target::Internal,
            groups: ["debug".into()].into(),
            timeout: Duration::from_secs(5),
            ..config()
        },
    ]
}
async fn post_at(address: SocketAddr, path: &str, body: &Value) -> (u16, Value) {
    let body = body.to_string();
    let (code, headers, bytes) = request(address, &format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nOrigin: https://client.example\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len()
    )).await;
    assert_eq!(
        headers["access-control-allow-origin"],
        "https://client.example"
    );
    (
        code,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        },
    )
}

#[tokio::test]
async fn live_dry_runs_share_runtime_state_without_mutating_chain_or_pool() {
    let lab = Lab::new();
    let prepared = configured(
        &lab,
        &empty(),
        json!({"internal": {"debug_endpoints": true}}),
    );
    let db = prepared.configured().chain().database().clone();
    let tx = transfer(prepared.configured());
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let servers = running
        .listen_http_group(listeners(), Some(worker()))
        .await
        .unwrap();
    assert_eq!(h.http().unwrap().listeners().len(), 2);
    let external = servers[0].status().address;
    let internal = servers[1].status().address;
    assert_eq!(
        request(
            external,
            "GET /v3/dry-run HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        .await
        .0,
        405
    );
    assert_eq!(request(internal, "POST /v3/debug/transactions/dry-run HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n").await.0, 415);
    let body =
        json!({"txs": [{"tx": api::encode(ApiType::Transaction, &tx.transaction().encode())}]});
    let before = db.view().unwrap().source().prefix(&[], 100_000).unwrap();
    for version in ["v2", "v3"] {
        let public = post_at(external, &format!("/{version}/dry-run"), &body).await;
        let private = post_at(
            internal,
            &format!("/{version}/debug/transactions/dry-run"),
            &body,
        )
        .await;
        assert_eq!(public, private);
        assert_eq!(public.0, 200, "{:?}", public.1);
        assert_eq!(public.1["results"][0]["result"], "ok");
        assert_eq!(
            post_at(
                external,
                &format!("/{version}/debug/transactions/dry-run"),
                &body
            )
            .await
            .0,
            404
        );
        assert_eq!(
            post_at(internal, &format!("/{version}/dry-run"), &body)
                .await
                .0,
            404
        );
    }
    assert_eq!(
        db.view().unwrap().source().prefix(&[], 100_000).unwrap(),
        before
    );
    let workers = h.pow().unwrap().workers().unwrap();
    let pool = workers.bindings().admission.current().unwrap().client;
    pool.push(tx.clone(), ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    let body = json!({"txs": [{"tx_hash": api::encode(ApiType::TxHash, tx.hash().as_ref())}]});
    let before = db.view().unwrap().source().prefix(&[], 100_000).unwrap();
    assert_eq!(post_at(external, "/v3/dry-run", &body).await.0, 200);
    assert_eq!(
        db.view().unwrap().source().prefix(&[], 100_000).unwrap(),
        before
    );
    let generation = workers.status().admission_generation;
    workers
        .restart(ae_sync::runtime::workers::Child::Admission, generation)
        .unwrap()
        .wait()
        .await
        .unwrap();
    within(async {
        while !h.status().ready || workers.status().admission_generation <= generation {
            h.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(
        post_at(internal, "/v3/debug/transactions/dry-run", &body)
            .await
            .0,
        200
    );
    let pool = workers.bindings().admission.current().unwrap().client;
    pool.delete(tx.hash()).unwrap().await.unwrap();
    assert_eq!(post_at(external, "/v3/dry-run", &body).await.0, 400);
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(servers.iter().all(|s| s.status().joined));
}

#[tokio::test]
async fn listener_group_shares_body_admission_and_unwinds_failed_startup() {
    let lab = Lab::new();
    let prepared = configured(
        &lab,
        &empty(),
        json!({"internal": {"debug_endpoints": true}}),
    );
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let occupied = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let reusable = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = reusable.local_addr().unwrap();
    drop(reusable);
    let mut configs = listeners();
    configs[0].listen = address;
    configs[1].listen = occupied.local_addr().unwrap();
    assert!(matches!(
        running.listen_http_group(configs, Some(worker())).await,
        Err(Error::Io(_))
    ));
    let rebound = TcpListener::bind(address).await.unwrap();
    drop(rebound);
    let mut missing = worker();
    missing.pipeline.process.executable = lab.0.join("missing-dry-run-worker");
    assert!(matches!(
        running.listen_http_group(listeners(), Some(missing)).await,
        Err(Error::Io(_))
    ));
    let mut options = worker();
    options.admission.requests = 1;
    options.admission.threads = 1;
    let servers = running
        .listen_http_group(listeners(), Some(options))
        .await
        .unwrap();
    let external = servers[0].status().address;
    let internal = servers[1].status().address;
    let mut slow = TcpStream::connect(external).await.unwrap();
    slow.write_all(b"POST /v3/dry-run HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 10\r\nExpect: 100-continue\r\n\r\n").await.unwrap();
    let mut ready = [0; 25];
    slow.read_exact(&mut ready).await.unwrap();
    assert_eq!(&ready, b"HTTP/1.1 100 Continue\r\n\r\n");
    let body = json!({"txs": []});
    assert_eq!(
        post_at(internal, "/v3/debug/transactions/dry-run", &body)
            .await
            .0,
        503
    );
    assert_eq!(get(external, "/v3/key-blocks/current/height").await.0, 200);
    drop(slow);
    within(async {
        loop {
            let result = post_at(internal, "/v3/debug/transactions/dry-run", &body).await;
            if result.0 == 200 {
                break;
            }
            assert_eq!(result.0, 503);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let mut half = TcpStream::connect(external).await.unwrap();
    half.write_all(b"POST /v2/dry-run HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 10\r\n\r\n{\"txs\":[]}").await.unwrap();
    half.shutdown().await.unwrap();
    let mut response = vec![];
    half.read_to_end(&mut response).await.unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 200 "),
        "{}",
        String::from_utf8_lossy(&response)
    );
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && servers.iter().all(|s| s.status().joined));
}

fn configured(lab: &Lab, initial: &Trees, http: Value) -> node::Prepared {
    let config = Configuration::from_value(json!({
        "fork_management": {"network_id": "ae_configured_services"},
        "system": {"dev_mode": true, "custom_prefunded_accs_file": "alloc.json"},
        "chain": {"hard_forks": {"1": 0, "6": 1}, "consensus": {"0": {"type": "on_demand"}}},
        "http": http,
    }))
    .unwrap();
    let context = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    let open = configured::Prepared::load(
        &config.resolve(&context).unwrap(),
        None,
        &lab.0,
        initial,
        &Specification::from_value(&profile::default_value()).unwrap(),
        Default::default(),
        &6_000_000.into(),
        Default::default(),
        Default::default(),
    )
    .unwrap()
    .open("chain", Default::default())
    .unwrap();
    node::Prepared::new(open, Default::default()).unwrap()
}
fn native_fixture() -> Value {
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
        "../../../fixtures/erlang_dry_run_pipeline.json"
    ))
    .unwrap();
    unpack(&v["root"], v["values"].as_array().unwrap())
}

#[tokio::test]
async fn live_fate_sequences_preserve_native_calls_and_public_floor_on_captured_chain() {
    let native = native_fixture();
    let rows = native["rows"].as_array().unwrap();
    let public = rows
        .iter()
        .find(|r| {
            r["input"]["name"] == "floor"
                && r["module"] == "oas_endpoints"
                && r["operation"] == "ProtectedDryRunTxs"
                && r["query"]["query"] == ""
        })
        .unwrap();
    let private = rows
        .iter()
        .find(|r| {
            r["input"]["name"] == "floor"
                && r["module"] == "oas_endpoints"
                && r["operation"] == "DryRunTxs"
                && r["query"]["query"] == ""
        })
        .unwrap();
    let initial = Trees::deserialize_from_binary(
        &hex::decode(public["seed"]["initial"].as_str().unwrap()).unwrap(),
        ae_trie::MemoryStore::default(),
        ae_trie::MemoryStore::default(),
        Default::default(),
    )
    .unwrap();
    let lab = Lab::new();
    let prepared = configured(
        &lab,
        &initial,
        json!({"internal": {"debug_endpoints": true}, "dry_run": {"timeout_ms": 0}, "gas_price": {"min_relay_gas_price": 2_000_000}}),
    );
    let db = prepared.configured().chain().database().clone();
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let done = h
        .commands()
        .unwrap()
        .client
        .submit(Command::Blocks {
            count: 1,
            kind: Kind::Key,
        })
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(done.result.as_ref().unwrap().command.result.is_ok());
    let servers = running
        .listen_http_group(listeners(), Some(worker()))
        .await
        .unwrap();
    let before = db.view().unwrap().source().prefix(&[], 100_000).unwrap();
    for (server, path, row) in [
        (&servers[0], "/v3/dry-run", public),
        (&servers[1], "/v3/debug/transactions/dry-run", private),
    ] {
        let (code, body) = post_at(server.status().address, path, &row["input"]["body"]).await;
        let mut expected = row["result"]["response"]["body"].clone();
        // The captured fixture uses height 50. This tiny real chain executes
        // the identical two stateful calls at height one; all other fields match.
        for result in expected["results"].as_array_mut().unwrap() {
            result["call_obj"]["height"] = 1.into();
        }
        assert_eq!((code, body), (200, expected));
        let string_row = rows
            .iter()
            .find(|r| {
                r["input"]["name"] == "floor"
                    && r["module"] == row["module"]
                    && r["operation"] == row["operation"]
                    && r["query"]["query"] != ""
            })
            .unwrap();
        let body = string_row["input"]["body"].to_string();
        let split = body.len() / 2;
        let raw = format!(
            "POST {path}?{} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{}\r\n{:x}\r\n{}\r\n0\r\n\r\n",
            string_row["query"]["query"].as_str().unwrap(),
            split,
            &body[..split],
            body.len() - split,
            &body[split..]
        );
        let (code, _, bytes) = request(server.status().address, &raw).await;
        let mut expected = string_row["result"]["response"]["body"].clone();
        for result in expected["results"].as_array_mut().unwrap() {
            result["call_obj"]["height"] = "1".into();
        }
        assert_eq!(
            (code, serde_json::from_slice::<Value>(&bytes).unwrap()),
            (200, expected)
        );
    }
    assert_eq!(
        db.view().unwrap().source().prefix(&[], 100_000).unwrap(),
        before
    );
    assert!(within(running.shutdown()).await.unwrap().status().joined);
}

#[tokio::test]
async fn separate_runtime_owners_keep_their_gas_policy_and_internal_bypass() {
    let first = Lab::new();
    let second = Lab::new();
    let a = configured(
        &first,
        &empty(),
        json!({"external": {"gas_limit": 0}, "internal": {"debug_endpoints": true}}),
    );
    let tx = transfer(a.configured());
    let b = configured(
        &second,
        &empty(),
        json!({"external": {"gas_limit": 6_000_000}, "internal": {"debug_endpoints": true}}),
    );
    let mut a = a
        .launch(commands(&first), || Ok(Default::default()))
        .unwrap();
    let mut b = b
        .launch(commands(&second), || Ok(Default::default()))
        .unwrap();
    let mut ah = a.handle();
    let mut bh = b.handle();
    ready(&mut ah).await;
    ready(&mut bh).await;
    let aa = a
        .listen_http_group(listeners(), Some(worker()))
        .await
        .unwrap();
    let bb = b
        .listen_http_group(listeners(), Some(worker()))
        .await
        .unwrap();
    let body =
        json!({"txs": [{"tx": api::encode(ApiType::Transaction, &tx.transaction().encode())}]});
    assert_eq!(
        post_at(aa[0].status().address, "/v3/dry-run", &body).await,
        (403, json!({"reason": "Over the gas limit"}))
    );
    assert_eq!(
        post_at(bb[0].status().address, "/v3/dry-run", &body)
            .await
            .0,
        200
    );
    assert_eq!(
        post_at(
            aa[1].status().address,
            "/v3/debug/transactions/dry-run",
            &body
        )
        .await
        .0,
        200
    );
    assert!(within(a.shutdown()).await.unwrap().status().joined);
    assert!(bh.status().ready);
    assert_eq!(
        post_at(bb[0].status().address, "/v3/dry-run", &body)
            .await
            .0,
        200
    );
    assert!(within(b.shutdown()).await.unwrap().status().joined);
}

#[tokio::test]
async fn internal_debug_permission_precedes_body_admission_and_stays_with_its_runtime() {
    let disabled = Lab::new();
    let enabled = Lab::new();
    let mut a = configured(&disabled, &empty(), json!({}))
        .launch(commands(&disabled), || Ok(Default::default()))
        .unwrap();
    let mut ah = a.handle();
    ready(&mut ah).await;
    let mut options = worker();
    options.admission.requests = 1;
    options.admission.threads = 1;
    let aa = a
        .listen_http_group(listeners(), Some(options))
        .await
        .unwrap();
    let mut slow = TcpStream::connect(aa[0].status().address).await.unwrap();
    slow.write_all(b"POST /v3/dry-run HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 10\r\nExpect: 100-continue\r\n\r\n").await.unwrap();
    let mut continuing = [0; 25];
    slow.read_exact(&mut continuing).await.unwrap();
    assert_eq!(&continuing, b"HTTP/1.1 100 Continue\r\n\r\n");
    let db = ah.pow().unwrap().workers().unwrap();
    let generation = db.status().admission_generation;
    for version in ["v2", "v3"] {
        for (method, suffix, headers, expected) in [
            (
                "POST",
                "",
                "Content-Type: application/json\r\nExpect: 100-continue\r\n",
                403,
            ),
            ("POST", "?int-as-string=bad", "Accept: text/plain\r\n", 403),
            ("GET", "", "", 405),
            ("BREW", "", "", 501),
            ("OPTIONS", "", "", 200),
        ] {
            // Deliberately omit all ten declared body bytes. A forbidden
            // request must finish without sending 100 Continue or waiting.
            let raw = format!(
                "{method} /{version}/debug/transactions/dry-run{suffix} HTTP/1.1\r\nHost: localhost\r\nOrigin: https://client.example\r\n{headers}Content-Length: 10\r\n\r\n"
            );
            let (code, headers, body) = tokio::time::timeout(
                Duration::from_secs(2),
                request(aa[1].status().address, &raw),
            )
            .await
            .unwrap();
            assert_eq!(code, expected);
            assert!(body.is_empty());
            assert_eq!(
                headers["access-control-allow-origin"],
                "https://client.example"
            );
        }
    }
    assert_eq!(db.status().admission_generation, generation);
    let mut b = configured(
        &enabled,
        &empty(),
        json!({"internal": {"debug_endpoints": true}}),
    )
    .launch(commands(&enabled), || Ok(Default::default()))
    .unwrap();
    let mut bh = b.handle();
    ready(&mut bh).await;
    let bb = b
        .listen_http_group(listeners(), Some(worker()))
        .await
        .unwrap();
    let body = json!({"txs": []});
    assert_eq!(
        post_at(
            bb[1].status().address,
            "/v3/debug/transactions/dry-run",
            &body
        )
        .await,
        (200, json!({"results": []}))
    );
    assert_eq!(
        post_at(
            aa[1].status().address,
            "/v3/debug/transactions/dry-run",
            &body
        )
        .await,
        (403, Value::Null)
    );
    drop(slow);
    within(async {
        loop {
            let result = post_at(aa[0].status().address, "/v3/dry-run", &body).await;
            if result.0 == 200 {
                assert_eq!(result.1, json!({"results": []}));
                break;
            }
            assert_eq!(result.0, 503);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(within(a.shutdown()).await.unwrap().status().joined);
    assert!(bh.status().ready);
    assert!(within(b.shutdown()).await.unwrap().status().joined);
}
