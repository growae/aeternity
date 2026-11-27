use super::*;
use ae_protocol::Protocol;
use num_bigint::BigInt;
use num_traits::ToPrimitive;

struct RecentClock(AtomicU64);
impl generator::Clock for RecentClock {
    fn unix_millis(&self) -> i128 {
        self.0.load(Ordering::Relaxed).into()
    }
}
fn prepared(lab: &Lab, price: Option<u64>, malformed: bool) -> node::Prepared {
    let mut cfg = json!({"fork_management":{"network_id":"ae_gas_http"},
        "system":{"dev_mode":true,"custom_prefunded_accs_file":"alloc.json"},
        "chain":{"hard_forks":{"1":0,"5":1,"6":2},"consensus":{"0":{"type":"on_demand"}}}});
    if let Some(price) = price {
        cfg["http"] =
            json!({"gas_price":{"min_relay_gas_price":price,"reporting_utilization_override":37}});
    }
    let cfg = Configuration::from_value(cfg).unwrap();
    let mut ctx = Context::new(Directories {
        home: lab.0.display().to_string(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    if malformed {
        ctx.set(
            "aehttp".into(),
            "gas_price".into(),
            json!([
                ["min_relay_gas_price", 9],
                ["reporting_utilization_override", 1, 2]
            ]),
        )
        .unwrap();
    }
    let open = configured::Prepared::load(
        &cfg.resolve(&ctx).unwrap(),
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
fn settings(lab: &Lab, clock: Arc<RecentClock>) -> node::Settings {
    let mut settings = commands(lab);
    let node::Settings::OnDemand(s) = &mut settings else {
        unreachable!()
    };
    s.clock = clock;
    settings
}
fn http_config() -> Config {
    let mut cfg = config();
    cfg.groups.insert("transaction".into());
    cfg
}
async fn check(address: SocketAddr, expected: &Value) {
    for version in ["v2", "v3"] {
        let response = get(address, &format!("/{version}/recent-gas-prices")).await;
        assert_eq!(response.0, 200, "{}", response.2);
        assert_eq!(&response.2, expected);
        assert!(response.1.contains_key("x-ae-height"));
    }
    let mut strings = expected.clone();
    for row in strings.as_array_mut().unwrap() {
        for value in row.as_object_mut().unwrap().values_mut() {
            *value = Value::String(value.to_string());
        }
    }
    let response = get(address, "/v3/recent-gas-prices?int-as-string=true").await;
    assert_eq!((response.0, response.2), (200, strings));
}

#[tokio::test]
async fn recent_gas_http_uses_admitted_historical_windows_and_warm_cache_survives_pruning() {
    let lab = Lab::new();
    let p = prepared(&lab, None, false);
    let db = p.configured().chain().database().clone();
    let first = info::contract_at(p.configured(), 5, 1, 3_000_000);
    let second = info::contract_at(p.configured(), 6, 2, 3_000_000);
    let third = transfer_with_nonce(p.configured(), 3);
    let now: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    let clock = Arc::new(RecentClock(AtomicU64::new(now - 40 * 60_000)));
    let mut running = p
        .launch(settings(&lab, clock.clone()), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let importer = h.importer().unwrap();
    let http = running.listen_http(http_config()).await.unwrap();
    let address = http.status().address;
    let empty = json!(
        [1, 5, 15, 60].map(|minutes| json!({"minutes":minutes,"min_gas_price":0,"utilization":0}))
    );
    check(address, &empty).await;
    let mut minimum = Vec::new();
    let mut used = Vec::new();
    let mut hashes = Vec::new();
    for (index, (tx, age)) in [(first, 40), (second, 10), (third, 3)]
        .into_iter()
        .enumerate()
    {
        clock.0.store(now - age * 60_000, Ordering::Relaxed);
        info::mine(&h, 1, Kind::Key).await;
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
        let view = db.view().unwrap();
        let hash = view.status().tip.unwrap();
        assert_eq!(view.location(tx.hash()).unwrap(), Some(hash));
        let header = view.require_header(hash).unwrap();
        let height = header.common().height.into();
        let protocol = Protocol::try_from(header.common().version).unwrap();
        let pricing = tx.transaction().view().pricing(&height, protocol);
        let gas = if index == 0 {
            assert_eq!(protocol, Protocol::Iris);
            assert!(pricing.gas_limit > BigInt::from(3_000_000));
            pricing.gas_limit
        } else {
            assert_eq!(protocol, Protocol::Ceres);
            let gas = ae_execution::transactions::accounting::used_gas(
                tx.transaction().view(),
                &height,
                protocol,
                &view.trees(hash, Default::default()).unwrap(),
            )
            .unwrap();
            if index == 1 {
                assert!(gas < BigInt::from(100_000) && gas < pricing.gas_limit);
            }
            gas
        };
        minimum.push(pricing.minimum_gas_price);
        used.push(gas);
        hashes.push(hash);
    }
    clock.0.store(now - 20_000, Ordering::Relaxed);
    info::mine(&h, 1, Kind::Key).await;
    let mut expected = vec![json!({"minutes":1,"min_gas_price":0,"utilization":0})];
    for (minutes, start) in [(5, 2), (15, 1), (60, 0)] {
        let price = minimum[start..].iter().min().unwrap();
        let sum: BigInt = used[start..].iter().sum();
        let utilization = ((&sum * BigInt::from(100)).to_f64().unwrap()
            / (6_000_000 * (3 - start)) as f64)
            .round() as u64;
        expected.push(json!({"minutes":minutes,"min_gas_price":serde_json::from_str::<Value>(&price.to_string()).unwrap(),"utilization":utilization}));
    }
    let expected = Value::Array(expected);
    check(address, &expected).await;
    assert!(expected[3]["utilization"].as_u64().unwrap() > 10);
    let capacity = importer.available();
    let held = importer.service_work(32 << 20).unwrap();
    let busy = get(address, "/v3/recent-gas-prices").await;
    assert_eq!(busy.0, 503);
    assert_eq!(busy.1["retry-after"], "3");
    drop(held);
    assert_eq!(importer.available(), capacity);
    assert!(db.begin_gc(Default::default()).unwrap());
    for _ in 0..1024 {
        if !db.gc_step(Default::default()).unwrap().active() {
            break;
        }
    }
    assert!(!db.gc_state().unwrap().active());
    assert!(matches!(
        db.view().unwrap().trees(hashes[0], Default::default()),
        Err(ae_chain::repository::Error::Pruned { .. })
    ));
    check(address, &expected).await;
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
    check(address, &expected).await;
    drop(workers);
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(http.status().joined);
    drop(exit);
    drop(h);
    drop(importer);
    drop(db);
    let p = prepared(&lab, None, false);
    let mut running = p
        .launch(settings(&lab, clock), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let http = running.listen_http(http_config()).await.unwrap();
    // A new runtime must not reuse the old positive cache across reopen.
    let cold = get(http.status().address, "/v3/recent-gas-prices").await;
    assert_eq!(
        (cold.0, cold.2),
        (500, json!({"reason":"Internal server error"}))
    );
    assert!(within(running.shutdown()).await.unwrap().status().joined);
}

#[tokio::test]
async fn recent_gas_http_captures_reporting_override_and_defers_configuration_errors() {
    for malformed in [false, true] {
        let lab = Lab::new();
        let p = prepared(&lab, (!malformed).then_some(9), malformed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let clock = Arc::new(RecentClock(AtomicU64::new(now - 20_000)));
        let tx = info::contract(p.configured());
        let mut running = p
            .launch(settings(&lab, clock), || Ok(Default::default()))
            .unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        // Activate Ceres for the independent contract fixture.
        info::mine(&h, 2, Kind::Key).await;
        h.pow()
            .unwrap()
            .workers()
            .unwrap()
            .bindings()
            .admission
            .current()
            .unwrap()
            .client
            .push(tx, ae_mempool::pool::Event::Created, false)
            .unwrap()
            .await
            .unwrap();
        info::mine(&h, 1, Kind::Micro).await;
        let http = running.listen_http(http_config()).await.unwrap();
        if malformed {
            let response = get(http.status().address, "/v3/recent-gas-prices").await;
            assert_eq!(
                (response.0, response.2),
                (500, json!({"reason":"Internal server error"}))
            );
        } else {
            let expected = json!(
                [1, 5, 15, 60]
                    .map(|minutes| json!({"minutes":minutes,"min_gas_price":9,"utilization":37}))
            );
            check(http.status().address, &expected).await;
        }
        assert!(within(running.shutdown()).await.unwrap().status().joined);
    }
}

#[tokio::test]
async fn recent_gas_http_distinguishes_missing_state_records_and_missing_ancestor_headers() {
    use ae_chain::repository as repo;
    for missing_state in [true, false] {
        let lab = Lab::new();
        let p = prepared(&lab, None, false);
        let tx = transfer(p.configured());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let clock = Arc::new(RecentClock(AtomicU64::new(now - 20_000)));
        let mut running = p
            .launch(settings(&lab, clock.clone()), || Ok(Default::default()))
            .unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        info::mine(&h, 1, Kind::Key).await;
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
        let micro = h
            .importer()
            .unwrap()
            .current_header(0, |header| header.selected.top)
            .unwrap()
            .wait()
            .await
            .unwrap();
        // The missing row is an ancestor, not the selected tip. Startup's tip
        // validation remains intact; the HTTP traversal must encounter it.
        info::mine(&h, 2, Kind::Key).await;
        let http = running.listen_http(http_config()).await.unwrap();
        let healthy = get(http.status().address, "/v3/recent-gas-prices").await;
        assert_eq!(healthy.0, 200);
        assert!(healthy.2[0]["min_gas_price"].as_u64().unwrap() > 0);
        let exit = within(running.shutdown()).await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        drop(exit);
        drop(h);
        let key = repo::key(
            if missing_state {
                repo::RECORDS
            } else {
                repo::BLOCKS
            },
            micro.0,
        );
        let saved = {
            // Only this stopped, isolated test database is modified. No
            // production mutation hook or second live writer is introduced.
            let raw = rocksdb::DB::open(&rocksdb::Options::default(), lab.0.join("chain")).unwrap();
            let bytes = raw.get(&key).unwrap().expect("admitted ancestor row");
            raw.delete(&key).unwrap();
            raw.flush().unwrap();
            bytes
        };
        let p = prepared(&lab, None, false);
        let mut running = p
            .launch(settings(&lab, clock.clone()), || Ok(Default::default()))
            .unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        let http = running.listen_http(http_config()).await.unwrap();
        for attempt in 0..2 {
            for path in [
                "/v2/recent-gas-prices",
                "/v3/recent-gas-prices",
                "/v3/recent-gas-prices?int-as-string=true",
            ] {
                let (status, headers, body) = request(http.status().address, &format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nOrigin: https://client.example\r\n\r\n")).await;
                assert_eq!(
                    status, 500,
                    "{path}, missing_state={missing_state}, attempt={attempt}"
                );
                if missing_state {
                    assert_eq!(
                        serde_json::from_slice::<Value>(&body).unwrap(),
                        json!({"reason":"Internal server error"})
                    );
                    assert_eq!(headers["x-ae-height"], "3");
                    assert_eq!(
                        headers["access-control-allow-origin"],
                        "https://client.example"
                    );
                } else {
                    assert!(body.is_empty());
                    assert!(!headers.contains_key("x-ae-height"));
                    assert!(!headers.contains_key("access-control-allow-origin"));
                }
            }
            assert!(h.status().ready);
        }
        let exit = within(running.shutdown()).await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        drop(exit);
        drop(h);
        {
            let raw = rocksdb::DB::open(&rocksdb::Options::default(), lab.0.join("chain")).unwrap();
            assert!(raw.get(&key).unwrap().is_none());
            raw.put(&key, &saved).unwrap();
            raw.flush().unwrap();
        }
        let p = prepared(&lab, None, false);
        let mut running = p
            .launch(settings(&lab, clock), || Ok(Default::default()))
            .unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        let http = running.listen_http(http_config()).await.unwrap();
        check(http.status().address, &healthy.2).await;
        assert!(within(running.shutdown()).await.unwrap().status().joined);
    }
}
