use super::*;
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
async fn get(
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
    (code, headers, serde_json::from_slice(&body).unwrap())
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
    let (code, headers, body) = get(address, "/v3/key-blocks/current/height").await;
    assert_eq!((code, body), (503, json!({"reason":"Temporary overload"})));
    assert_eq!(headers["retry-after"], "3");
    assert_eq!(headers["x-ae-height"], "0");
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
