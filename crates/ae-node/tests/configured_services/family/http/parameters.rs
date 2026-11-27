use super::*;
struct FixedClock;
impl ae_sync::chain::pow::generator::Clock for FixedClock {
    fn unix_millis(&self) -> i128 {
        1_000_000
    }
}

#[tokio::test]
async fn protocol_parameters_include_signalled_fork_only_after_accepted_activation() {
    let lab = Lab::new();
    lab.write("alloc.json", b"{}");
    let config=Configuration::from_value(json!({
        "fork_management":{"network_id":"ae_parameters_signal","fork":{"enabled":true,"signalling_start_height":2,
            "signalling_end_height":4,"signalling_block_count":1,"info_field":17,"version":6}},
        "system":{"dev_mode":true,"custom_prefunded_accs_file":"alloc.json"},
        "chain":{"hard_forks":{"1":0,"2":1},"consensus":{"0":{"type":"on_demand"}}}})).unwrap();
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
    let p = node::Prepared::new(open, Default::default()).unwrap();
    let mut running = p.launch(commands(&lab), || Ok(Default::default())).unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = super::config();
    cfg.groups = ["node_info".into()].into();
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    for height in 1..=4 {
        info::mine(&h, 1, Kind::Key).await;
        let actual = h
            .importer()
            .unwrap()
            .current_header(0, |head| head)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(
            actual.network.scheduled_protocol_at(height),
            ae_protocol::Protocol::Minerva
        );
        let r = get(address, "/v3/protocol-parameters").await;
        assert_eq!(r.0, 200);
        let version = if height == 4 { 6 } else { 2 };
        assert_eq!(actual.version, version);
        assert_eq!(r.2["current_protocol_version"], version);
        let rows = r.2["protocols"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["version"], version);
        assert_eq!(
            rows[0]["effective_at_height"],
            if height == 4 { 4 } else { 1 }
        );
    }
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(http.status().joined);
}

#[tokio::test]
async fn protocol_parameters_follow_accepted_versions_bound_rules_replacement_and_rollback() {
    let lab = Lab::new();
    lab.write("alloc.json",serde_json::to_vec(&json!({api::encode(ApiType::AccountPubkey,payer().public_key().as_ref()):10_000_000_000_000_000_000u64})).unwrap());
    let cfg = Configuration::from_value(
        json!({"fork_management":{"network_id":"ae_parameters_live"},
        "system":{"dev_mode":true,"custom_prefunded_accs_file":"alloc.json"},
        "mining":{"micro_block_cycle":17,"expected_mine_rate":19,"name_claim_bid_timeout":23},
        "chain":{"hard_forks":{"1":0,"6":1,"7":2,"8":3},"consensus":{"0":{"type":"on_demand"}}}}),
    )
    .unwrap();
    let mut context = Context::new(Directories {
        home: lab.0.display().to_string(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    context
        .set("aecore".into(), "micro_block_cycle".into(), json!(99))
        .unwrap();
    let open = configured::Prepared::load(
        &cfg.resolve(&context).unwrap(),
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
    let tx = transfer(&open);
    let second = transfer_with_nonce(&open, 2);
    let db = open.chain().database().clone();
    let p = node::Prepared::new(open, Default::default()).unwrap();
    assert_eq!(p.pow().unwrap().rules().admission.micro_cycle_ms, 17);
    let mut settings = commands(&lab);
    let node::Settings::OnDemand(s) = &mut settings else {
        unreachable!()
    };
    s.clock = Arc::new(FixedClock);
    let mut running = p.launch(settings, || Ok(Default::default())).unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut config = config();
    config.groups = ["external".into()].into();
    let http = running.listen_http(config).await.unwrap();
    let address = http.status().address;
    let first = get(address, "/v3/protocol-parameters").await;
    assert_eq!(first.0, 200, "{}", first.2);
    assert_eq!(first.2["network_id"], "ae_parameters_live");
    assert_eq!(first.2["current_protocol_version"], 1);
    assert_eq!(first.2["micro_block_cycle"], 17);
    assert_eq!(first.2["expected_block_mine_rate"], 19);
    assert!(first.2.get("child_block_time").is_none());
    let versions = |value: &Value| {
        value["protocols"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["version"].as_u64().unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(versions(&first.2), vec![1, 6, 7, 8]);
    let importer = h.importer().unwrap();
    for (height, version) in [(1, 6), (2, 7), (3, 8)] {
        info::mine(&h, 1, Kind::Key).await;
        let r = get(address, "/v3/protocol-parameters").await;
        assert_eq!(r.0, 200);
        assert_eq!(r.1["x-ae-height"], height.to_string());
        assert_eq!(r.2["current_protocol_version"], version);
        assert_eq!(versions(&r.2), (version as u64..=8).collect::<Vec<_>>());
        for row in r.2["protocols"].as_array().unwrap() {
            assert_eq!(row["name_auction_timeouts"][0]["bid_timeout"], 23);
            assert_eq!(row["name_auction_timeouts"][0]["bid_extension"], 240);
        }
        if height == 1 {
            h.pow()
                .unwrap()
                .workers()
                .unwrap()
                .bindings()
                .admission
                .current()
                .unwrap()
                .client
                .push(tx.clone(), ae_mempool::pool::Event::Created, false)
                .unwrap()
                .await
                .unwrap();
            info::mine(&h, 1, Kind::Micro).await;
            let selected = importer
                .current_header(0, |head| head)
                .unwrap()
                .wait()
                .await
                .unwrap();
            assert_ne!(selected.selected.top, selected.selected.key);
            assert_eq!(selected.version, 6);
            assert_eq!(get(address, "/v3/protocol-parameters").await.2, r.2);
            assert!(db.view().unwrap().location(tx.hash()).unwrap().is_some());
            let ae_codec::headers::Header::Micro(first_header) = db
                .view()
                .unwrap()
                .require_header(selected.selected.top)
                .unwrap()
            else {
                panic!()
            };
            h.pow()
                .unwrap()
                .workers()
                .unwrap()
                .bindings()
                .admission
                .current()
                .unwrap()
                .client
                .push(second.clone(), ae_mempool::pool::Event::Created, false)
                .unwrap()
                .await
                .unwrap();
            info::mine(&h, 1, Kind::Micro).await;
            let next = importer
                .current_header(0, |h| h)
                .unwrap()
                .wait()
                .await
                .unwrap();
            let ae_codec::headers::Header::Micro(second_header) = db
                .view()
                .unwrap()
                .require_header(next.selected.top)
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(second_header.time - first_header.time, 17);
            assert!(
                db.view()
                    .unwrap()
                    .location(second.hash())
                    .unwrap()
                    .is_some()
            );
        }
    }
    let strings = get(address, "/v3/protocol-parameters?int-as-string=true").await;
    assert_eq!(strings.0, 200);
    assert_eq!(strings.2["current_protocol_version"], "8");
    assert_eq!(
        strings.2["protocols"][0]["name_auction_timeouts"][0]["bid_timeout"],
        "23"
    );
    assert_eq!(
        request(
            address,
            "GET /v2/protocol-parameters HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )
        .await
        .0,
        404
    );
    let capacity = importer.available();
    let held = importer.service_work(96 << 20).unwrap();
    let busy = get(address, "/v3/protocol-parameters").await;
    assert_eq!(busy.0, 503);
    assert_eq!(busy.1["retry-after"], "3");
    drop(held);
    assert_eq!(importer.available(), capacity);
    let workers = h.pow().unwrap().workers().unwrap();
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
    assert_eq!(
        get(address, "/v3/protocol-parameters").await.2["current_protocol_version"],
        8
    );
    db.rollback(
        ae_chain::repository::rollback::Target::Height(0),
        Default::default(),
    )
    .unwrap();
    let reset = get(address, "/v3/protocol-parameters").await;
    assert_eq!(reset.0, 200);
    assert_eq!(reset.2, first.2);
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(http.status().joined);
    assert!(importer.current_header(0, |h| h).is_err());
}
