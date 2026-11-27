use super::*;
use ae_node::http::{ConfiguredLimits, DryRunConfig};

fn prepared(lab: &Lab, http: Value, maintenance: bool) -> (node::Prepared, Context) {
    let config = Configuration::from_value(json!({
        "fork_management":{"network_id":"ae_configured_services"},
        "system":{"dev_mode":true,"maintenance_mode":maintenance,"custom_prefunded_accs_file":"alloc.json"},
        "chain":{"hard_forks":{"1":0,"6":1},"consensus":{"0":{"type":"on_demand"}}},
        "http":http
    })).unwrap();
    let mut context = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    context
        .set(
            "aehttp".into(),
            "http".into(),
            json!([["websocket", [["listen_address", "127.1"]]]]),
        )
        .unwrap();
    let open = configured::Prepared::load(
        &config.resolve(&context).unwrap(),
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
    (
        node::Prepared::new(open, Default::default()).unwrap(),
        context,
    )
}
fn settings() -> Value {
    json!({"external":{"port":0,"acceptors":1},"internal":{"port":0,"acceptors":2},"rosetta":{"port":0,"acceptors":3},"rosetta_offline":{"port":0,"acceptors":4}})
}

#[tokio::test]
async fn configured_http_startup_keeps_captured_groups_cors_and_debug_permission() {
    let lab = Lab::new();
    let mut http = settings();
    http["endpoints"] = json!({"dry-run":true});
    http["cors"] = json!({"allow_domains":["https://client.example"],"allow_methods":["GET","POST"],"allow_headers":["x-one","x-two"],"max_age":-1});
    let (prepared, mut context) = prepared(&lab, http, true);
    // Changes to the caller's context after preparation cannot change a node.
    context
        .set(
            "aehttp".into(),
            "http".into(),
            json!([["websocket", [["listen_address", "192.0.2.1"]]]]),
        )
        .unwrap();
    context
        .set(
            "aehttp".into(),
            "internal".into(),
            json!([["debug_endpoints", true]]),
        )
        .unwrap();
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let worker = DryRunConfig::new(
        env!("CARGO_BIN_EXE_ae-dry-run-worker").into(),
        std::env::temp_dir(),
    );
    let servers = running
        .listen_configured_http(ConfiguredLimits::default(), Some(worker))
        .await
        .unwrap();
    assert_eq!(servers.len(), 4);
    assert_eq!(h.http().unwrap().listeners().len(), 4);
    assert!(matches!(
        running
            .listen_configured_http(ConfiguredLimits::default(), None)
            .await,
        Err(Error::AlreadyListening)
    ));
    let addresses: Vec<_> = servers.iter().map(|s| s.status().address).collect();
    assert!(
        addresses
            .iter()
            .all(|a| a.ip().is_loopback() && a.port() != 0)
    );
    assert_eq!(get(addresses[0], "/v3/status").await.0, 200);
    assert_eq!(
        request(
            addresses[0],
            "GET /v3/key-blocks/current/height HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        .await
        .0,
        404
    );
    assert_eq!(request(addresses[1], "POST /v3/debug/transactions/dry-run HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000\r\nExpect: 100-continue\r\n\r\n").await.0, 403);
    let body = r#"{"txs":[]}"#;
    let (code, _, bytes) = request(addresses[0], &format!("POST /v3/dry-run HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len())).await;
    assert_eq!(code, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["results"],
        json!([])
    );
    for address in &addresses {
        let (code, headers, _) = request(*address, "OPTIONS /v3/status HTTP/1.1\r\nHost: localhost\r\nOrigin: https://client.example\r\nAccess-Control-Request-Headers: x-requested\r\n\r\n").await;
        assert_eq!(code, 200);
        assert_eq!(headers["access-control-allow-methods"], "GET, POST");
        assert_eq!(headers["access-control-allow-headers"], "x-onex-two");
        assert_eq!(headers["access-control-max-age"], "-1");
    }
    for address in &addresses[2..] {
        assert_eq!(
            request(
                *address,
                "GET /v3/status HTTP/1.1\r\nHost: localhost\r\n\r\n"
            )
            .await
            .0,
            404
        );
    }
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(
        servers
            .iter()
            .all(|s| s.status().joined && s.status().active == 0)
    );
    for address in addresses {
        let rebound = TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }
}

#[tokio::test]
async fn configured_http_startup_unwinds_all_sockets_and_bounds_acceptors() {
    let lab = Lab::new();
    let mut reservations = Vec::new();
    let mut http = settings();
    for target in ["external", "internal", "rosetta", "rosetta_offline"] {
        let socket = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        http[target]["port"] = json!(socket.local_addr().unwrap().port());
        http[target]["acceptors"] = json!(10);
        reservations.push(socket);
    }
    let addresses: Vec<_> = reservations
        .iter()
        .map(|s| s.local_addr().unwrap())
        .collect();
    let occupied = reservations.pop().unwrap();
    drop(reservations);
    let (prepared, _) = prepared(&lab, http, false);
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let limits = ConfiguredLimits {
        connections: 2,
        timeout: Duration::from_secs(10),
        ..Default::default()
    };
    assert!(matches!(
        running.listen_configured_http(limits.clone(), None).await,
        Err(Error::Io(_))
    ));
    assert!(h.http().is_none());
    for address in &addresses[..3] {
        let rebound = TcpListener::bind(*address).await.unwrap();
        drop(rebound);
    }
    drop(occupied);
    let servers = running.listen_configured_http(limits, None).await.unwrap();
    let mut first = TcpStream::connect(addresses[0]).await.unwrap();
    let mut second = TcpStream::connect(addresses[0]).await.unwrap();
    first
        .write_all(b"GET /v3/status HTTP/1.1\r\n")
        .await
        .unwrap();
    second
        .write_all(b"GET /v3/status HTTP/1.1\r\n")
        .await
        .unwrap();
    within(async {
        while servers[0].status().active != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let mut third = TcpStream::connect(addresses[0]).await.unwrap();
    third
        .write_all(b"GET /v3/status HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut byte = [0];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), third.read(&mut byte))
            .await
            .is_err()
    );
    assert_eq!(servers[0].status().accepted, 2);
    drop(first);
    let mut response = Vec::new();
    within(third.read_to_end(&mut response)).await.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200 "));
    assert!(servers[0].status().active <= 2);
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert_eq!(within(second.read(&mut byte)).await.unwrap(), 0);
    assert!(
        servers
            .iter()
            .all(|s| s.status().joined && s.status().active == 0)
    );
    for address in addresses {
        let rebound = TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }
}
