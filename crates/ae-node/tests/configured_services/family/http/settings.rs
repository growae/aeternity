use super::*;

fn prepared(lab: &Lab, external: Value, user: Value) -> (node::Prepared, node::Settings) {
    let mut cfg = json!({
        "fork_management":{"network_id":"ae_settings_test"},
        "system":{"dev_mode":true,"custom_prefunded_accs_file":"alloc.json"},
        "chain":{"hard_forks":{"1":0,"6":1},"consensus":{"0":{"type":"on_demand"}}},
        "mining":{"min_miner_gas_price":123,"max_auth_fun_gas":456},
        "mempool":{"tx_ttl":789,"nonce_offset":12}
    });
    if !user.is_null() {
        cfg["http"] = json!({"external":{"gas_limit":user}});
    }
    let cfg = Configuration::from_value(cfg).unwrap();
    let mut context = Context::new(Directories {
        home: lab.0.display().to_string(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    context
        .set("aehttp".into(), "external".into(), external)
        .unwrap();
    let resolved = cfg.resolve(&context).unwrap();
    let open = configured::Prepared::load(
        &resolved,
        None,
        &lab.0,
        &empty(),
        &Specification::from_value(&profile::default_value()).unwrap(),
        Default::default(),
        &7_654_321.into(),
        Default::default(),
        Default::default(),
    )
    .unwrap()
    .open("chain", Default::default())
    .unwrap();
    let p = node::Prepared::new(open, Default::default()).unwrap();
    let mut settings = commands(lab);
    p.configure_core(&resolved, &mut settings).unwrap();
    if let node::Settings::OnDemand(s) = &mut settings {
        s.config.workers.admission.requests = 4;
    }
    (p, settings)
}

#[tokio::test]
async fn node_settings_reads_live_policy_after_reconfiguration_recovery_and_replacement() {
    let lab = Lab::new();
    let (p, s) = prepared(&lab, json!([["gas_limit", 999]]), json!(321));
    let db = p.configured().chain().database().clone();
    let mut running = p.launch(s, || Ok(Default::default())).unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups = ["node_settings".into()].into();
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    let original = json!({"min_miner_gas_price":"123","max_auth_fun_gas":456,"mempool_tx_ttl":789,"mempool_nonce_offset":12,"dry_run_gas_limit":321,"block_gas_limit":7_654_321});
    let revision = db.pool_view().unwrap().revision();
    let result = get(address, "/v3/node-settings").await;
    assert_eq!((result.0, result.2), (200, original.clone()));
    assert_eq!(result.1["x-ae-height"], "0");
    assert_eq!(
        result.1["access-control-allow-origin"],
        "https://client.example"
    );
    assert_eq!(db.pool_view().unwrap().revision(), revision);
    let workers = h.pow().unwrap().workers().unwrap();
    let pool = workers.bindings().admission.current().unwrap().client;
    let mut recovered = ae_storage::pool::Config::default();
    recovered.policy.minimum_miner_gas_price = 987.into();
    recovered.policy.maximum_auth_gas = 654.into();
    recovered.policy.nonce_offset = 32.into();
    recovered.policy.tx_ttl = 10;
    pool.reconfigure(recovered.policy.clone(), recovered.failures.clone())
        .unwrap()
        .await
        .unwrap();
    let changed = json!({"min_miner_gas_price":"987","max_auth_fun_gas":654,"mempool_tx_ttl":10,"mempool_nonce_offset":32,"dry_run_gas_limit":321,"block_gas_limit":7_654_321});
    assert_eq!(get(address, "/v3/node-settings").await.2, changed);
    assert_eq!(
        get(address, "/v3/node-settings?int-as-string=true").await.2,
        json!({"min_miner_gas_price":"987","max_auth_fun_gas":"654","mempool_tx_ttl":"10","mempool_nonce_offset":"32","dry_run_gas_limit":"321","block_gas_limit":"7654321"})
    );
    recovered.policy.tx_ttl = 11;
    pool.recover(recovered).unwrap().await.unwrap();
    assert_eq!(
        get(address, "/v3/node-settings").await.2["mempool_tx_ttl"],
        11
    );
    let mut held = vec![];
    for _ in 0..4 {
        held.push(pool.policy_settings(1024).unwrap().await.unwrap());
    }
    assert_eq!(pool.account_query_usage().0, 4);
    let busy = get(address, "/v3/node-settings").await;
    assert_eq!(busy.0, 503);
    assert_eq!(busy.1["retry-after"], "3");
    for (query, code) in [("ignored=%GG", 400), ("int-as-string=invalid", 500)] {
        let r = request(
            address,
            &format!("GET /v3/node-settings?{query} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
        )
        .await;
        assert_eq!(r.0, code);
        assert!(r.2.is_empty());
        assert!(!r.1.contains_key("x-ae-height"));
    }
    drop(held);
    assert_eq!(pool.account_query_usage(), (0, 0));
    let importer = h.importer().unwrap();
    let capacity = importer.available();
    let held = importer.service_work(96 << 20).unwrap();
    assert_eq!(get(address, "/v3/node-settings").await.0, 503);
    drop(held);
    assert_eq!(importer.available(), capacity);
    let generation = workers.status().admission_generation;
    let command = h.commands().unwrap().number;
    workers
        .restart(ae_sync::runtime::workers::Child::Admission, generation)
        .unwrap()
        .wait()
        .await
        .unwrap();
    within(async {
        while !h.status().ready || h.commands().is_none_or(|c| c.number <= command) {
            h.changed().await.unwrap();
        }
    })
    .await;
    assert!(Arc::ptr_eq(&importer, &h.importer().unwrap()));
    assert!(pool.policy_settings(1024).is_err());
    assert_eq!(get(address, "/v3/node-settings").await.2, original);
    info::mine(&h, 1, Kind::Key).await;
    assert_eq!(
        get(address, "/v3/node-settings").await.1["x-ae-height"],
        "1"
    );
    let current = workers.bindings().admission.current().unwrap().client;
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(http.status().joined);
    assert!(current.policy_settings(1024).is_err());
}

#[tokio::test]
async fn node_settings_preserves_nested_application_and_native_response_validation() {
    for (external, number_code, string_code, expected) in [
        (
            json!([["gas_limit", 765], ["gas_limit", 999]]),
            200,
            200,
            json!(765),
        ),
        (json!({"gas_limit":999}), 200, 200, json!(6_000_000)),
        (json!([["gas_limit", -1]]), 500, 200, json!(-1)),
        (json!([["gas_limit", "bad"]]), 200, 200, json!("bad")),
        (json!([["gas_limit", false]]), 500, 500, json!(false)),
        (json!([["gas_limit", 1, 2]]), 500, 500, Value::Null),
    ] {
        let lab = Lab::new();
        let (p, s) = prepared(&lab, external, Value::Null);
        let mut running = p.launch(s, || Ok(Default::default())).unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        let mut cfg = config();
        cfg.groups = ["external".into()].into();
        let http = running.listen_http(cfg).await.unwrap();
        let address = http.status().address;
        for (path, code) in [
            ("/v3/node-settings", number_code),
            ("/v3/node-settings?int-as-string=true", string_code),
            ("/v2/node-settings", 404),
        ] {
            let r = request(
                address,
                &format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
            )
            .await;
            assert_eq!(r.0, code, "{path}: {:?}", r.2);
            if code == 200 {
                let body: Value = serde_json::from_slice(&r.2).unwrap();
                let expected = if path.contains('?') && expected.is_number() {
                    json!(expected.to_string())
                } else {
                    expected.clone()
                };
                assert_eq!(body["dry_run_gas_limit"], expected);
            }
        }
        let exit = within(running.shutdown()).await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        assert!(http.status().joined);
    }
}
