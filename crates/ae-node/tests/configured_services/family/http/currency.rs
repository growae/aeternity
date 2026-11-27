use super::*;

fn prepared(lab: &Lab, currency: Value, display: Value) -> node::Prepared {
    let config = Configuration::from_value(json!({
        "fork_management":{"network_id":"ae_currency_test"},
        "system":{"dev_mode":true,"custom_prefunded_accs_file":"alloc.json"},
        "chain":{"hard_forks":{"1":0,"6":1},"consensus":{"0":{"type":"on_demand"}},
                 "currency":currency,"display":display}
    }))
    .unwrap();
    let ctx = Context::new(Directories {
        home: lab.0.display().to_string(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    let open = configured::Prepared::load(
        &config.resolve(&ctx).unwrap(),
        None,
        &lab.0,
        &empty(),
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

#[tokio::test]
async fn currency_http_uses_startup_settings_live_files_and_owned_capacity_across_replacement() {
    let lab = Lab::new();
    let prepared = prepared(
        &lab,
        json!({"name":"Grow AE","symbol":"GAE","subunit":"g","subunits_per_unit":123,
               "fiat_converstion_url":"legacy","fiat_conversion_url":"current"}),
        json!({"network_name":"Local currency","primary_colour":"red","secondary_colour":"blue","logo_file":"branding/logo.SVG"}),
    );
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups = ["node_info".into()].into();
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    let expected = json!({"name":"Grow AE","symbol":"GAE","subunit":"g","subunits_per_unit":123,
        "fiat_converstion_url":"current","fiat_conversion_url":"current","network_name":"Local currency",
        "primary_colour":"red","secondary_colour":"blue"});
    let first = get(address, "/v3/currency").await;
    assert_eq!((first.0, first.2), (200, expected.clone()));
    assert_eq!(first.1["x-ae-height"], "0");
    assert_eq!(
        first.1["access-control-allow-origin"],
        "https://client.example"
    );
    lab.write("branding/logo.SVG", [0, 1, 2]);
    let mut with_logo = expected.clone();
    with_logo["logo"] = json!({"type":"SVG","data":"AAEC"});
    assert_eq!(get(address, "/v3/currency").await.2, with_logo);
    let mut strings = with_logo.clone();
    strings["subunits_per_unit"] = json!("123");
    assert_eq!(
        get(address, "/v3/currency?int-as-string=true").await.2,
        strings
    );
    lab.write("branding/logo.SVG", b"new");
    with_logo["logo"]["data"] = json!("bmV3");
    assert_eq!(get(address, "/v3/currency").await.2, with_logo);
    info::mine(&h, 1, Kind::Key).await;
    assert_eq!(get(address, "/v3/currency").await.1["x-ae-height"], "1");
    let workers = h.pow().unwrap().workers().unwrap();
    let generation = workers.status().admission_generation;
    let old_importer = h.importer().unwrap();
    let command_generation = h.commands().unwrap().number;
    workers
        .restart(ae_sync::runtime::workers::Child::Admission, generation)
        .unwrap()
        .wait()
        .await
        .unwrap();
    within(async {
        while !h.status().ready || h.commands().is_none_or(|c| c.number <= command_generation) {
            h.changed().await.unwrap();
        }
    })
    .await;
    // Admission replacement refreshes the producer's pool binding. The
    // importer and its file-work budget belong to the root and stay stable.
    assert!(Arc::ptr_eq(&old_importer, &h.importer().unwrap()));
    drop(old_importer.service_work(1).unwrap());
    assert_eq!(get(address, "/v3/currency").await.2, with_logo);
    let importer = h.importer().unwrap();
    let capacity = importer.available();
    let held = importer
        .service_work(48 << 20)
        .unwrap()
        .submit(|| 7)
        .unwrap()
        .wait_retained()
        .await
        .unwrap();
    assert_eq!(held.value, 7);
    assert!(importer.available().0 < capacity.0);
    drop(held.credits);
    assert_eq!(importer.available(), capacity);
    let held = importer.service_work(96 << 20).unwrap();
    let busy = get(address, "/v3/currency").await;
    assert_eq!(busy.0, 503);
    assert_eq!(busy.1["retry-after"], "3");
    for (query, code) in [("ignored=%GG", 400), ("int-as-string=invalid", 500)] {
        let r = request(
            address,
            &format!(
                "GET /v3/currency?{query} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
            ),
        )
        .await;
        assert_eq!(r.0, code);
        assert!(r.2.is_empty());
        assert!(!r.1.contains_key("x-ae-height"));
    }
    drop(held);
    assert_eq!(importer.available(), capacity);
    // Cancelling a caller does not release a still-running blocking job's
    // capacity. The worker returns the reservation after the bounded work ends.
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let ticket = importer
        .service_work(96 << 20)
        .unwrap()
        .submit(move || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
    within(entered_rx).await.unwrap();
    drop(ticket);
    assert!(importer.available().1 < capacity.1);
    assert_eq!(get(address, "/v3/currency").await.0, 503);
    release_tx.send(()).unwrap();
    within(async {
        while importer.available() != capacity {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(get(address, "/v3/currency").await.2, with_logo);
    lab.write("branding/logo.SVG", vec![0; (1 << 20) + 1]);
    assert_eq!(get(address, "/v3/currency").await.0, 503);
    lab.write("branding/logo.SVG", []);
    with_logo["logo"]["data"] = json!("");
    assert_eq!(get(address, "/v3/currency").await.2, with_logo);
    std::fs::remove_file(lab.0.join("branding/logo.SVG")).unwrap();
    assert_eq!(get(address, "/v3/currency").await.2, expected);
    #[cfg(unix)]
    {
        assert!(
            std::process::Command::new("mkfifo")
                .arg(lab.0.join("branding/logo.SVG"))
                .status()
                .unwrap()
                .success()
        );
        assert_eq!(get(address, "/v3/currency").await.0, 503);
        std::fs::remove_file(lab.0.join("branding/logo.SVG")).unwrap();
    }
    let stale = importer.service_work(1).unwrap();
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(http.status().joined);
    assert!(importer.service_work(1).is_err());
    assert!(
        stale
            .submit(|| panic!("closed owner must not execute file work"))
            .is_err()
    );
}

#[tokio::test]
async fn currency_http_applies_native_integer_schema_and_keeps_metadata_errors_at_request_time() {
    for (currency, display, number_expected, string_expected) in [
        (json!({"subunits_per_unit":-1}), json!({}), 500, 200),
        (
            json!({"subunits_per_unit":18446744073709551616u128}),
            json!({}),
            200,
            200,
        ),
        (
            json!({"subunits_per_unit":serde_json::from_str::<Value>("1606938044258990275541962092341162602522202993782792835301376").unwrap()}),
            json!({}),
            500,
            200,
        ),
    ] {
        let lab = Lab::new();
        let p = prepared(&lab, currency, display);
        let mut running = p.launch(commands(&lab), || Ok(Default::default())).unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        let mut cfg = config();
        cfg.groups = ["external".into()].into();
        let http = running.listen_http(cfg).await.unwrap();
        let address = http.status().address;
        for (path, code) in [
            ("/v3/currency", number_expected),
            ("/v3/currency?int-as-string=true", string_expected),
            ("/v2/currency", 404),
        ] {
            let r = request(
                address,
                &format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
            )
            .await;
            assert_eq!(r.0, code, "{path}: {:?}", r.2);
        }
        let exit = within(running.shutdown()).await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        assert!(http.status().joined);
    }
}
