use super::*;
#[path = "http/accounts.rs"]
mod accounts;
#[path = "http/channels.rs"]
mod channels;
#[path = "http/contracts.rs"]
mod contracts;
#[path = "http/currency.rs"]
mod currency;
#[path = "http/dry_run.rs"]
mod dry_run;
#[path = "http/ends.rs"]
mod ends;
#[path = "http/gas.rs"]
mod gas;
#[path = "http/info.rs"]
mod info;
#[path = "http/names.rs"]
mod names;
#[path = "http/oracles.rs"]
mod oracles;
#[path = "http/parameters.rs"]
mod parameters;
#[path = "http/post.rs"]
mod post;
#[path = "http/settings.rs"]
mod settings;
#[path = "http/startup.rs"]
mod startup;
use ae_node::http::{Config, Error};
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn config() -> Config {
    Config {
        listen: ([127, 0, 0, 1], 0).into(),
        timeout: Duration::from_secs(2),
        ..Default::default()
    }
}
async fn request(
    address: SocketAddr,
    raw: &str,
) -> (u16, std::collections::BTreeMap<String, String>, Vec<u8>) {
    within(async {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.write_all(raw.as_bytes()).await.unwrap();
        let mut bytes = vec![];
        socket.read_to_end(&mut bytes).await.unwrap();
        let mut fields = [httparse::EMPTY_HEADER; 64];
        let mut response = httparse::Response::new(&mut fields);
        let end = response.parse(&bytes).unwrap().unwrap();
        let headers = response
            .headers
            .iter()
            .map(|h| {
                (
                    h.name.to_owned(),
                    std::str::from_utf8(h.value).unwrap().to_owned(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            headers["content-length"].parse::<usize>().unwrap(),
            bytes.len() - end
        );
        assert_eq!(headers["connection"], "close");
        (response.code.unwrap(), headers, bytes[end..].to_vec())
    })
    .await
}
pub(super) async fn get(
    address: SocketAddr,
    path: &str,
) -> (u16, std::collections::BTreeMap<String, String>, Value) {
    let (code, headers, body) = request(
        address,
        &format!(
            "GET {path} HTTP/1.1\r\nHost: localhost\r\nOrigin: https://client.example\r\n\r\n"
        ),
    )
    .await;
    (
        code,
        headers,
        serde_json::from_slice(&body)
            .unwrap_or_else(|error| panic!("GET {path}: status {code}, invalid JSON: {error}")),
    )
}

#[tokio::test]
async fn http_transaction_queries_follow_pool_mining_rollback_and_worker_replacement() {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = prepared.configured().chain().database().clone();
    let tx = transfer(prepared.configured());
    let mut settings = commands(&lab);
    if let node::Settings::OnDemand(s) = &mut settings {
        s.config.workers.pool.policy.allow_reentry = true;
    }
    let mut running = prepared
        .launch(settings, || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups.insert("transaction".into());
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    let encoded_hash = api::encode(ApiType::TxHash, tx.hash().as_ref());
    let expected = json!({
        "block_hash":"none", "block_height":-1,
        "hash":encoded_hash, "encoded_tx":api::encode(ApiType::Transaction,&tx.encode()),
        "signatures":tx.signatures().iter().map(|s|api::encode(ApiType::Signature,s.as_ref())).collect::<Vec<_>>(),
        "tx":{"type":"SpendTx","version":1,"sender_id":api::encode(ApiType::AccountPubkey,payer().public_key().as_ref()),"recipient_id":api::encode(ApiType::AccountPubkey,&[43;32]),"nonce":1,"amount":7,"fee":100_000_000_000_000u64,"payload":api::encode(ApiType::Bytearray,&[])}
    });
    for version in ["v2", "v3"] {
        let (code, _, body) =
            get(address, &format!("/{version}/transactions/{encoded_hash}")).await;
        assert_eq!(
            (code, body),
            (404, json!({"reason":"Transaction not found"}))
        );
    }
    let workers = h.pow().unwrap().workers().unwrap();
    let pool = workers.bindings().admission.current().unwrap().client;
    pool.push(tx.clone(), ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    for version in ["v2", "v3"] {
        let (code, _, body) =
            get(address, &format!("/{version}/transactions/{encoded_hash}")).await;
        assert_eq!((code, body), (200, expected.clone()));
    }
    pool.delete(tx.hash()).unwrap().await.unwrap();
    assert_eq!(
        get(address, &format!("/v3/transactions/{encoded_hash}"))
            .await
            .0,
        404
    );
    pool.push(tx.clone(), ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    let generation = workers.status().admission_generation;
    let command_generation = h.commands().unwrap().number;
    workers
        .restart(ae_sync::runtime::workers::Child::Admission, generation)
        .unwrap()
        .wait()
        .await
        .unwrap();
    within(async {
        loop {
            assert!(h.status().running && h.status().failure.is_none());
            if h.status().ready && h.commands().is_some_and(|c| c.number > command_generation) {
                break;
            }
            h.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(
        get(address, &format!("/v3/transactions/{encoded_hash}"))
            .await
            .2,
        expected
    );
    let importer = h.importer().unwrap();
    let capacity = importer.available();
    let mut unread = vec![];
    while let Ok(ticket) = importer.current() {
        unread.push(ticket);
        assert!(unread.len() < 64);
    }
    for path in [
        format!("/v3/transactions/{encoded_hash}"),
        format!("/v3/transactions/{encoded_hash}/info"),
        "/v3/transactions/bad/info".into(),
        "/v3/transactions/bad".into(),
        "/v3/micro-blocks/hash/bad/transactions".into(),
    ] {
        let (code, headers, body) = get(address, &path).await;
        assert_eq!((code, body), (503, json!({"reason":"Temporary overload"})));
        assert_eq!(headers["retry-after"], "3");
    }
    assert_eq!(
        get(address, "/v3/micro-blocks/hash/bad/transactions/index/0")
            .await
            .0,
        400
    );
    drop(unread);
    within(async {
        while importer.available() != capacity {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let completion = h
        .commands()
        .unwrap()
        .client
        .submit(Command::Blocks {
            count: 2,
            kind: Kind::Any,
        })
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(completion.result.as_ref().unwrap().command.result.is_ok());
    let (header, micro, key) = {
        let view = db.view().unwrap();
        let block = view.location(tx.hash()).unwrap().unwrap();
        let header = view.require_header(block).unwrap();
        let key = api::encode(ApiType::MicroBlockHash, &header.common().previous_key_hash);
        let micro = api::encode(ApiType::MicroBlockHash, block.as_ref());
        (header, micro, key)
    };
    let mut mined = expected.clone();
    mined["block_height"] = json!(header.common().height);
    mined["block_hash"] = json!(micro);
    for version in ["v2", "v3"] {
        for suffix in [
            format!("transactions/{encoded_hash}"),
            format!("micro-blocks/hash/{micro}/transactions/index/+0001"),
        ] {
            let (code, headers, body) = get(address, &format!("/{version}/{suffix}")).await;
            assert_eq!((code, body), (200, mined.clone()));
            assert_eq!(
                headers["access-control-allow-origin"],
                "https://client.example"
            );
        }
        assert_eq!(
            get(
                address,
                &format!("/{version}/micro-blocks/hash/{micro}/transactions")
            )
            .await
            .2,
            json!({"transactions":[mined]})
        );
        for index in [
            "2",
            "18446744073709551616",
            "9999999999999999999999999999999999999999999999999999999999999",
        ] {
            let (code, _, body) = get(
                address,
                &format!("/{version}/micro-blocks/hash/{micro}/transactions/index/{index}"),
            )
            .await;
            assert_eq!(
                (code, body),
                (400, json!({"reason":"Invalid hash or index"}))
            );
        }
        let (code, _, body) = get(
            address,
            &format!("/{version}/micro-blocks/hash/{key}/transactions"),
        )
        .await;
        assert_eq!(
            (code, body),
            (
                404,
                json!({"reason":"Block not found","error_code":"block_not_found"})
            )
        );
        let (code, _, body) = get(
            address,
            &format!("/{version}/micro-blocks/hash/{key}/transactions/index/1"),
        )
        .await;
        assert_eq!((code, body), (404, json!({"reason":"Block not found"})));
        let (code, _, body) = get(
            address,
            &format!("/{version}/micro-blocks/hash/{micro}/transactions/index/%ED%A0%41"),
        )
        .await;
        assert_eq!(
            (code, body),
            (
                400,
                json!({"reason":"validation_error","parameter":"index","info":{"error":"wrong_type","data":"�"}})
            )
        );
    }
    let (code, _, string_body) = get(
        address,
        &format!("/v3/transactions/{encoded_hash}?int-as-string=true"),
    )
    .await;
    assert_eq!(code, 200);
    assert_eq!(string_body["tx"]["amount"], "7");
    assert_eq!(string_body["tx"]["version"], "1");
    assert_eq!(
        string_body["block_height"],
        header.common().height.to_string()
    );
    db.rollback(
        ae_chain::repository::rollback::Target::Height(0),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        get(address, &format!("/v3/transactions/{encoded_hash}"))
            .await
            .0,
        404
    );
    assert_eq!(
        get(
            address,
            &format!("/v3/micro-blocks/hash/{micro}/transactions")
        )
        .await
        .0,
        404
    );
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined);
    assert!(http.status().joined);
    assert!(TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn http_query_errors_and_public_key_survive_key_worker_replacement() {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let genesis = prepared
        .configured()
        .chain()
        .database()
        .view()
        .unwrap()
        .identity()
        .genesis;
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut handle = running.handle();
    ready(&mut handle).await;
    let workers = handle.pow().unwrap().workers().unwrap();
    let old = workers.bindings().keys.unwrap().current().unwrap();
    let peer = old.peer_public_key().unwrap().await.unwrap();
    let expected_peer = api::encode(ApiType::PeerPubkey, peer.as_ref());
    let http = running.listen_http(config()).await.unwrap();
    let address = http.status().address;
    let (status_code, _, initial_status) = get(address, "/v2/status").await;
    assert_eq!(status_code, 200);
    assert_eq!(initial_status["difficulty"], 1338);
    assert_eq!(initial_status["peer_pubkey"], expected_peer);
    let mut last_uptime = super::http_checks::uptime_ms(initial_status["uptime"].as_str().unwrap());
    let native: Value =
        serde_json::from_str(include_str!("../../fixtures/erlang_http_queries.json")).unwrap();
    let mut checked = 0;
    for row in native["rows"].as_array().unwrap() {
        let (suffix, mut expected) = match row["id"].as_str().unwrap() {
            "GetCurrentKeyBlockHeight" => ("/key-blocks/current/height", json!({"height":0})),
            "GetCurrentKeyBlockHash" => (
                "/key-blocks/current/hash",
                json!({"hash":api::encode(ApiType::KeyBlockHash, genesis.as_ref())}),
            ),
            "GetPeerPubkey" => ("/peers/pubkey", json!({"pubkey":expected_peer})),
            "GetSyncStatus" => ("/sync-status", json!({"reason":"Node is not syncing"})),
            "GetStatus" => ("/status", initial_status.clone()),
            _ => unreachable!(),
        };
        let version = if row["module"] == "oas_endpoints" {
            "v3"
        } else {
            "v2"
        };
        let path = format!(
            "/{version}{suffix}?{}",
            row["query"]["query"].as_str().unwrap()
        );
        let (code, headers, body) = request(
            address,
            &format!(
                "GET {path} HTTP/1.1\r\nHost: localhost\r\nOrigin: https://client.example\r\n\r\n"
            ),
        )
        .await;
        let expected_code = if row["id"] == "GetSyncStatus" {
            404
        } else {
            row["result"]["response"]["code"].as_u64().unwrap() as u16
        };
        assert_eq!(code, expected_code, "{row}");
        if row["result"]["kind"] == "stream_error" {
            assert!(body.is_empty(), "{row}");
            assert!(!headers.contains_key("x-ae-height"));
            assert!(!headers.contains_key("access-control-allow-origin"));
            assert!(!headers.contains_key("content-type"));
        } else {
            if row["id"] == "GetCurrentKeyBlockHeight"
                && row["result"]["response"]["body"]["height"].is_string()
            {
                expected["height"] = json!("0");
            }
            let actual = serde_json::from_slice::<Value>(&body).unwrap();
            if row["id"] == "GetStatus" {
                let uptime = super::http_checks::uptime_ms(actual["uptime"].as_str().unwrap());
                assert!(uptime >= last_uptime);
                last_uptime = uptime;
                expected["uptime"] = actual["uptime"].clone();
                if row["result"]["response"]["body"]["difficulty"].is_string() {
                    fn strings(value: &mut Value) {
                        match value {
                            Value::Number(n) if !n.as_str().contains(['.', 'e', 'E']) => {
                                *value = json!(n.to_string())
                            }
                            Value::Array(a) => a.iter_mut().for_each(strings),
                            Value::Object(m) => m.values_mut().for_each(strings),
                            _ => (),
                        }
                    }
                    strings(&mut expected);
                }
            }
            assert_eq!(actual, expected, "{row}");
            assert_eq!(headers["x-ae-height"], "0");
            assert_eq!(
                headers["access-control-allow-origin"],
                "https://client.example"
            );
        }
        checked += 1;
    }
    assert_eq!(checked, 260);
    let generation = workers.status().keys_generation;
    let command_generation = handle.commands().unwrap().number;
    assert_eq!(
        workers
            .restart(ae_sync::runtime::workers::Child::Keys, generation)
            .unwrap()
            .wait()
            .await
            .unwrap(),
        generation + 1
    );
    // Worker replacement is acknowledged before the root necessarily notices
    // it. Wait for the dependent command owner to rebind as well; a retained
    // ready publication from the previous generation is not this barrier.
    within(async {
        loop {
            let status = handle.status();
            assert!(status.running && status.failure.is_none(), "{status:?}");
            if status.ready
                && handle
                    .commands()
                    .is_some_and(|c| c.number > command_generation)
            {
                break;
            }
            handle.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(handle.commands().unwrap().number, command_generation + 1);
    let current = workers.bindings().keys.unwrap().current().unwrap();
    assert!(!old.same_worker(&current));
    assert!(matches!(
        old.peer_public_key(),
        Err(ae_sync::keys::Error::Stopped)
    ));
    for version in ["v2", "v3"] {
        let (code, _, body) = get(address, &format!("/{version}/peers/pubkey")).await;
        assert_eq!((code, body), (200, json!({"pubkey":expected_peer})));
        let (code, _, body) = get(address, &format!("/{version}/status")).await;
        assert_eq!(code, 200);
        assert_eq!(body["peer_pubkey"], expected_peer);
        assert!(super::http_checks::uptime_ms(body["uptime"].as_str().unwrap()) >= last_uptime);
    }
    assert!(running.shutdown().await.unwrap().status().joined);
    assert!(matches!(
        current.peer_public_key(),
        Err(ae_sync::keys::Error::Stopped)
    ));
}

#[tokio::test]
async fn http_sync_status_uses_live_peer_progress_statistics_and_replacement_ownership() {
    use ae_p2p::{message::Kind as WireKind, noise, ping, runtime as net};
    use ae_sync::{chain::sync, peer::snapshot};
    let source_lab = Lab::new();
    let source =
        prepare(source_lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let source_db = source.configured().chain().database().clone();
    let source = source
        .launch(commands(&source_lab), || Ok(Default::default()))
        .unwrap();
    let mut source_handle = source.handle();
    ready(&mut source_handle).await;
    let made = source_handle
        .commands()
        .unwrap()
        .client
        .submit(Command::Blocks {
            count: 3,
            kind: Kind::Key,
        })
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(made.result.as_ref().unwrap().command.result.is_ok());
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({}), Default::default()));
    let db = prepared.configured().chain().database().clone();
    let mut settings = commands(&lab);
    if let node::Settings::OnDemand(s) = &mut settings {
        s.config.connections.sync.headers = 1;
        s.config.connections.sync.concurrent = 1;
        s.config.connections.sync.read_bytes = 256 << 20;
    }
    let mut running = prepared
        .launch(settings, || Ok(Default::default()))
        .unwrap();
    let mut handle = running.handle();
    ready(&mut handle).await;
    let http = running.listen_http(config()).await.unwrap();
    let address = http.status().address;
    assert_eq!(get(address, "/v2/sync-status").await.0, 404);
    let mut connections = handle.connections().unwrap();
    let mut generation = connection_ready(&mut connections, 1).await;
    while !generation.network.status().ready {
        generation.network.changed().await.unwrap();
    }
    let identity = handle
        .pow()
        .unwrap()
        .workers()
        .unwrap()
        .bindings()
        .keys
        .unwrap()
        .current()
        .unwrap()
        .peer_identity()
        .unwrap()
        .await
        .unwrap();
    let binding = db.view().unwrap().identity().clone();
    assert_eq!(
        binding.genesis,
        source_db.view().unwrap().identity().genesis
    );
    let network = noise::Network::new(1, binding.genesis, &binding.network).unwrap();
    let socket = TcpStream::connect(generation.network.status().listening.unwrap())
        .await
        .unwrap();
    let (client, mut events, driver) = net::establish(
        socket,
        &noise::Identity::from_secret([98; 32]),
        &network,
        noise::Role::Initiator(identity.public_key()),
        Default::default(),
    )
    .await
    .unwrap();
    let driver = tokio::spawn(driver.run());
    let ping = ping::encode(
        &snapshot::local(&source_db.view().unwrap(), false).unwrap(),
        vec![],
        &[],
        ping::Encoding {
            version: 1,
            minimum_version: 1,
            port: 4321,
            share: 0.into(),
            limits: Default::default(),
        },
    )
    .unwrap();
    within(client.request(&ping, None).unwrap()).await.unwrap();
    let mut requests = 0;
    let held = within(async {
        loop {
            let Some(net::Event::Message(received)) = events.recv().await else {
                continue;
            };
            if received.message().kind() == WireKind::GetGeneration {
                requests += 1;
                if requests == 2 {
                    break received;
                }
            }
            if let Some(reply) = snapshot::serve(
                &source_db.pool_view().unwrap(),
                received.message(),
                Default::default(),
            )
            .unwrap()
            {
                client.send(&reply).unwrap().await.unwrap();
            }
        }
    })
    .await;
    let progress = within(async {
        loop {
            let progress = generation.sync.progress().unwrap().wait().await.unwrap();
            if progress.percent > 0.0 && generation.sync.estimate(progress.task.unwrap()).is_ok() {
                break progress;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(progress.target, 3);
    let metrics = generation.network.metrics().unwrap().wait().await.unwrap();
    assert!(metrics.verified + metrics.unverified > 0);
    assert!(metrics.registry.counts.inbound + metrics.registry.counts.outbound > 0);
    let mut status_uptime = 0;
    for version in ["v2", "v3"] {
        let (code, _, status) = get(address, &format!("/{version}/status")).await;
        assert_eq!(code, 200, "{status}");
        assert_eq!(status["syncing"], true);
        assert_eq!(status["sync_progress"], json!(progress.percent));
        assert_eq!(
            status["peer_count"],
            json!(metrics.verified + metrics.unverified)
        );
        assert_eq!(
            status["peer_connections"],
            json!({"inbound":metrics.registry.counts.inbound,"outbound":metrics.registry.counts.outbound})
        );
        status_uptime = super::http_checks::uptime_ms(status["uptime"].as_str().unwrap());
        let (code, headers, body) = get(
            address,
            &format!("/{version}/sync-status?int-as-string=true"),
        )
        .await;
        assert_eq!(code, 200, "{body}");
        assert_eq!(body["progress"], json!(progress.percent));
        assert!(body["speed"].as_f64().unwrap() > 0.0);
        assert_eq!(body["target"], 3);
        assert!(body["estimate"].is_number());
        assert_eq!(
            headers["x-ae-height"],
            generation
                .sync
                .selected_height()
                .unwrap()
                .wait()
                .await
                .unwrap()
                .to_string()
        );
    }
    let reply = snapshot::serve(
        &source_db.pool_view().unwrap(),
        held.message(),
        Default::default(),
    )
    .unwrap()
    .unwrap();
    client.send(&reply).unwrap().await.unwrap();
    within(async {
        loop {
            if generation.sync.status().counts.finished > 0 { break; }
            tokio::select! {
                result = generation.sync.changed() => { result.unwrap(); },
                event = events.recv() => {
                    if let Some(net::Event::Message(received)) = event
                        && let Some(reply) = snapshot::serve(&source_db.pool_view().unwrap(),received.message(),Default::default()).unwrap() {
                        client.send(&reply).unwrap().await.unwrap();
                    }
                }
            }
        }
    }).await;
    assert_eq!(get(address, "/v3/sync-status").await.0, 404);
    let mut unread = vec![];
    loop {
        match generation.sync.progress() {
            Ok(ticket) => unread.push(ticket),
            Err(sync::Error::Busy) => break,
            _ => panic!("live sync"),
        }
        assert!(unread.len() <= 16);
    }
    let (code, headers, body) = get(address, "/v3/sync-status").await;
    assert_eq!((code, body), (503, json!({"reason":"Temporary overload"})));
    assert_eq!(headers["retry-after"], "3");
    assert_eq!(headers["x-ae-height"], "3");
    let (code, headers, status) = get(address, "/v3/status").await;
    assert_eq!(
        (code, status),
        (503, json!({"reason":"Temporary overload"}))
    );
    assert_eq!(headers["retry-after"], "3");
    drop(unread);
    generation.cache.as_ref().unwrap().shutdown();
    let mut replacement = connection_ready(&mut connections, generation.number + 1).await;
    assert!(!generation.sync.same_owner(&replacement.sync));
    assert!(matches!(
        generation.sync.progress(),
        Err(sync::Error::Closed)
    ));
    while !replacement.network.status().ready {
        replacement.network.changed().await.unwrap();
    }
    ready(&mut handle).await;
    assert_eq!(get(address, "/v3/sync-status").await.0, 404);
    let (code, _, status) = get(address, "/v3/status").await;
    assert_eq!(code, 200);
    assert_eq!(status["syncing"], false);
    assert_eq!(status["top_block_height"], 3);
    assert!(super::http_checks::uptime_ms(status["uptime"].as_str().unwrap()) >= status_uptime);
    client.shutdown();
    within(driver).await.unwrap();
    assert!(running.shutdown().await.unwrap().status().joined);
    assert!(source.shutdown().await.unwrap().status().joined);
}

#[tokio::test]
async fn http_current_chain_queries_follow_real_key_and_micro_commits() {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = prepared.configured().chain().database().clone();
    let tx = transfer(prepared.configured());
    let genesis = db.view().unwrap().identity().genesis;
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    assert!(h.connections().is_none());
    let http = running.listen_http(config()).await.unwrap();
    let address = http.status().address;
    assert!(running.handle().http().is_some());
    super::http_checks::verify_blocks(&http, &db).await;
    for path in [
        "/v3/key-blocks/current/",
        "/v3/a/../key-blocks/current",
        "/%76%33/key-blocks/current",
    ] {
        let (code, _, body) = get(address, path).await;
        assert_eq!(code, 200, "{path}: {body}");
        assert_eq!(
            body["hash"],
            api::encode(ApiType::KeyBlockHash, genesis.as_ref())
        );
    }
    for path in [
        "/v3/key-blocks/hash/a%2Fb",
        "/v3/key-blocks/hash/%FF",
        "/v3/key-blocks/hash//",
    ] {
        let (code, _, body) = get(address, path).await;
        assert_eq!(
            (code, body),
            (400, json!({"reason":"Invalid hash"})),
            "{path}"
        );
    }
    for path in ["/v3%2Fstatus", "/v2/headers/top", "/v3/blocks/top"] {
        let (code, _, body) = request(
            address,
            &format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        )
        .await;
        assert_eq!(code, 404);
        assert!(body.is_empty());
    }
    for v3 in [false, true] {
        for (encoded, expected) in [
            ("%FF", "�"),
            ("%C0%80", "�"),
            ("%ED%A0%41", "�"),
            ("%ED%A0%80", "�"),
            ("%F5%80%80%80", "�"),
            ("%ED%FF%2F", "�"),
            ("1%ED%A0%32", "1�"),
        ] {
            let path = format!(
                "/{}/key-blocks/height/{encoded}",
                if v3 { "v3" } else { "v2" }
            );
            let (code, headers, body) = request(
                address,
                &format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"),
            )
            .await;
            if v3 {
                assert_eq!(code, 500);
                assert!(body.is_empty());
                assert!(!headers.contains_key("x-ae-height"));
            } else {
                assert_eq!(code, 400);
                let body: Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(
                    body,
                    json!({"reason":"validation_error","parameter":"height","info":{"error":"wrong_type","data":expected}})
                );
            }
        }
    }
    for version in ["v2", "v3"] {
        let (code, headers, body) =
            get(address, &format!("/{version}/key-blocks/current/height")).await;
        assert_eq!((code, body), (200, json!({"height":0})));
        assert_eq!(headers["x-ae-height"], "0");
        assert_eq!(
            headers["access-control-allow-origin"],
            "https://client.example"
        );
        let (_, _, body) = get(address, &format!("/{version}/key-blocks/current/hash")).await;
        assert_eq!(
            body,
            json!({"hash":api::encode(ApiType::KeyBlockHash,genesis.as_ref())})
        );
    }
    let importer = h.importer().unwrap();
    let mut unread = vec![];
    loop {
        match importer.current() {
            Ok(ticket) => unread.push(ticket),
            Err(ae_sync::chain::Error::Busy) => break,
            _ => panic!("live importer"),
        }
        assert!(unread.len() <= 64);
    }
    assert!(!unread.is_empty());
    let (code, headers, body) = get(address, "/v3/status").await;
    assert_eq!((code, body), (503, json!({"reason":"Temporary overload"})));
    assert_eq!(headers["retry-after"], "3");
    let (code, headers, body) = get(address, "/v3/key-blocks/current/height").await;
    assert_eq!((code, body), (503, json!({"reason":"Temporary overload"})));
    assert_eq!(headers["retry-after"], "3");
    assert_eq!(headers["x-ae-height"], "0");
    let (code, headers, body) = get(address, "/v3/key-blocks/hash/invalid").await;
    assert_eq!((code, body), (503, json!({"reason":"Temporary overload"})));
    assert_eq!(headers["retry-after"], "3");
    let (code, headers, _) =
        request(address, "OPTIONS / HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
    assert_eq!(code, 200);
    assert_eq!(headers["x-ae-height"], "0");
    drop(unread);
    within(async {
        while importer.available().0 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
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
    let (code, _, status) = get(address, "/v3/status").await;
    assert_eq!(code, 200);
    assert_eq!(status["pending_transactions_count"], 1);
    assert_eq!(status["difficulty"], 1338);
    assert_eq!(status["top_block_height"], 0);
    let completion = h
        .commands()
        .unwrap()
        .client
        .submit(Command::Blocks {
            count: 2,
            kind: Kind::Any,
        })
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(completion.result.as_ref().unwrap().command.result.is_ok());
    let (height, key) = {
        let view = db.view().unwrap();
        let top = view.status().tip.unwrap();
        let ae_chain::block::Block::Micro(micro) = view.require_block(top).unwrap() else {
            panic!("committed micro tip")
        };
        assert!(view.location(tx.hash()).unwrap().is_some());
        (
            micro.header.common.height,
            Hash256(micro.header.common.previous_key_hash),
        )
    };
    assert!(height > 0);
    super::http_checks::verify_blocks(&http, &db).await;
    let pool = h
        .pow()
        .unwrap()
        .workers()
        .unwrap()
        .bindings()
        .admission
        .current()
        .unwrap()
        .client;
    within(async {
        loop {
            let current = pool.inspection().unwrap().await.unwrap();
            if current.active + current.visited == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let (code, _, status) = get(address, "/v3/status?int-as-string=true").await;
    assert_eq!(code, 200);
    assert_eq!(status["pending_transactions_count"], "0");
    assert_eq!(status["top_block_height"], height.to_string());
    assert_eq!(
        status["top_key_block_hash"],
        api::encode(ApiType::KeyBlockHash, key.as_ref())
    );
    let (code, headers, body) =
        get(address, "/v3/key-blocks/current/height?int-as-string=TRUE").await;
    assert_eq!((code, body), (200, json!({"height":height.to_string()})));
    assert_eq!(headers["x-ae-height"], height.to_string());
    let (_, _, body) = get(address, "/v3/key-blocks/current/hash").await;
    assert_eq!(
        body,
        json!({"hash":api::encode(ApiType::KeyBlockHash,key.as_ref())})
    );
    let completion = h
        .commands()
        .unwrap()
        .client
        .submit(Command::Blocks {
            count: 70,
            kind: Kind::Key,
        })
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(completion.result.as_ref().unwrap().command.result.is_ok());
    let height = height + 70;
    super::http_checks::verify_blocks(&http, &db).await;
    let (code, headers, body) = get(address, "/v3/key-blocks/current/height").await;
    assert_eq!((code, body), (200, json!({"height":height})));
    assert_eq!(headers["x-ae-height"], height.to_string());
    let (code,headers,body)=request(address,"OPTIONS /unknown HTTP/1.1\r\nHost: localhost\r\nOrigin: https://client.example\r\nAccess-Control-Request-Headers: x-token\r\n\r\n").await;
    assert_eq!(code, 200);
    assert!(body.is_empty());
    assert_eq!(headers["x-ae-height"], height.to_string());
    assert_eq!(headers["access-control-allow-headers"], "x-token");
    let (code, headers, _) =
        request(address, "GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
    assert_eq!(code, 404);
    assert_eq!(headers["x-ae-height"], height.to_string());
    assert!(!headers.contains_key("access-control-allow-origin"));
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined);
    let status = http.status();
    assert!(status.joined && !status.running);
    assert_eq!(status.active, 0);
    assert!(TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn http_limits_bind_failure_and_idle_clients_are_owned_until_shutdown() {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let occupied = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let mut c = config();
    c.listen = occupied.local_addr().unwrap();
    assert!(matches!(running.listen_http(c).await, Err(Error::Io(_))));
    assert!(h.status().ready);
    let mut c = config();
    c.connections = 2;
    c.header_bytes = 1024;
    c.body_bytes = 16;
    let mut http = running.listen_http(c).await.unwrap();
    let address = http.status().address;
    assert!(matches!(
        running.listen_http(config()).await,
        Err(Error::AlreadyListening)
    ));
    for (raw,code) in [
        ("GET /v3/key-blocks/current/height HTTP/1.1\r\n\r\n".to_owned(),400),
        ("GET /v3/key-blocks/current/height HTTP/1.1\r\nHost: localhost\r\nContent-Length: 17\r\n\r\n".to_owned(),413),
        ("GET / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n".to_owned(),400),
        (format!("GET / HTTP/1.1\r\nHost: localhost\r\nX-Large: {}", "a".repeat(1500)),431),
        ("POST /v3/key-blocks/current/height HTTP/1.1\r\nHost: localhost\r\n\r\n".to_owned(),405),
        ("GET /v3/key-blocks/current/height HTTP/1.1\r\nHost: localhost\r\nAccept: text/html\r\n\r\n".to_owned(),406),
    ] { let (actual,headers,_)=request(address,&raw).await;assert_eq!(actual,code);assert_eq!(headers["x-ae-height"],"0"); }
    let mut slow = TcpStream::connect(address).await.unwrap();
    slow.write_all(b"GET /").await.unwrap();
    let mut slow2 = TcpStream::connect(address).await.unwrap();
    slow2.write_all(b"GET /").await.unwrap();
    within(async {
        while http.status().active != 2 {
            http.changed().await.unwrap();
        }
    })
    .await;
    let accepted = http.status().accepted;
    let mut queued = TcpStream::connect(address).await.unwrap();
    queued.write_all(b"GET /").await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(http.status().accepted, accepted);
    assert_eq!(http.status().active, 2);
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined);
    assert_eq!(http.status().cancelled, 2);
    assert_eq!(http.status().active, 0);
    assert!(http.status().joined);
    let mut byte = [0];
    assert!(matches!(slow.read(&mut byte).await, Ok(0) | Err(_)));
}

#[tokio::test]
async fn http_owner_drop_closes_listener_and_parent_without_claiming_a_root_receipt() {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut http = running.listen_http(config()).await.unwrap();
    let address = http.status().address;
    drop(running);
    within(async {
        while !http.status().joined {
            http.changed().await.unwrap();
        }
    })
    .await;
    within(async {
        while !h.status().joined {
            h.changed().await.unwrap();
        }
    })
    .await;
    assert!(TcpStream::connect(address).await.is_err());
}
