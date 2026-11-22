use ae_hyperchain::block::Pin;
use ae_sync::parent_pins::{Chain, Config, Error, Node, Service};
use serde_json::{Value as J, json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn term(v: &J) -> J {
    if let Some(b) = v.get("binary") {
        return J::String(String::from_utf8(bytes(b)).unwrap());
    }
    if let Some(n) = v.get("integer") {
        return serde_json::from_str(n.as_str().unwrap()).unwrap();
    }
    if let Some(a) = v.get("atom") {
        return match a.as_str().unwrap() {
            "true" => json!(true),
            "false" => json!(false),
            "null" => J::Null,
            a => json!(a),
        };
    }
    if let Some(l) = v.get("list") {
        return J::Array(l.as_array().unwrap().iter().map(term).collect());
    }
    if let Some(m) = v.get("map") {
        return J::Object(
            m.as_array()
                .unwrap()
                .iter()
                .map(|r| (term(&r[0]).as_str().unwrap().to_owned(), term(&r[1])))
                .collect(),
        );
    }
    panic!("unknown native term {v}");
}
fn read_request(s: &mut impl Read) -> (String, J) {
    let mut raw = Vec::new();
    let mut b = [0; 2048];
    loop {
        let n = s.read(&mut b).unwrap();
        assert_ne!(n, 0);
        raw.extend_from_slice(&b[..n]);
        assert!(raw.len() < 65536);
        if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = std::str::from_utf8(&raw[..end]).unwrap();
            let len = head
                .lines()
                .find_map(|l| l.strip_prefix("Content-Length: "))
                .map_or(0, |s| s.parse::<usize>().unwrap());
            if raw.len() >= end + 4 + len {
                return (
                    head.to_owned(),
                    if len == 0 {
                        J::Null
                    } else {
                        serde_json::from_slice(&raw[end + 4..]).unwrap()
                    },
                );
            }
        }
    }
}
fn listener() -> TcpListener {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.set_nonblocking(true).unwrap();
    l
}
fn accept(l: &TcpListener) -> std::net::TcpStream {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        match l.accept() {
            Ok((s, _)) => {
                s.set_nonblocking(false).unwrap();
                s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                s.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
                return s;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < end);
                thread::sleep(Duration::from_millis(1));
            }
            Err(e) => panic!("{e}"),
        }
    }
}
fn node(l: &TcpListener, c: Chain) -> Node {
    Node::new(
        c,
        &format!("http://{}", l.local_addr().unwrap()),
        (c == Chain::Dogecoin).then(|| (b"u".to_vec(), b"p".to_vec())),
    )
    .unwrap()
}
fn config() -> Config {
    Config {
        request_timeout: Duration::from_secs(2),
        lookup_timeout: Duration::from_secs(4),
        ..Default::default()
    }
}
fn expected(row: &J) -> Option<Pin> {
    let p = &row["result"];
    if p.is_null() {
        None
    } else {
        Some(Pin {
            epoch: p["epoch"].as_str().unwrap().parse().unwrap(),
            child_height: p["height"].as_str().unwrap().parse().unwrap(),
            child_hash: bytes(&p["hash"]),
            parent_height: p["parent_height"].as_str().unwrap().parse().unwrap(),
        })
    }
}
#[test]
fn original_first_parent_callbacks_over_real_http() {
    let data: J = serde_json::from_str(include_str!("fixtures/hc_pin_rpc.json")).unwrap();
    assert_eq!(data["rows"].as_array().unwrap().len(), 15);
    for row in data["rows"].as_array().unwrap() {
        let chain = match row["chain"].as_str().unwrap() {
            "aeternity" => Chain::Aeternity,
            "doge" => Chain::Dogecoin,
            "btc" => Chain::Bitcoin,
            _ => panic!(),
        };
        let l = listener();
        let fallback = listener();
        let service =
            Service::spawn(vec![node(&l, chain), node(&fallback, chain)], config()).unwrap();
        let requests = row["requests"].as_array().unwrap().clone();
        let replies = row["replies"].as_array().unwrap().clone();
        let server = thread::spawn(move || {
            for (i, request) in requests.iter().enumerate() {
                let mut s = accept(&l);
                let (head, body) = read_request(&mut s);
                let url = String::from_utf8(bytes(&request["url"])).unwrap();
                let path = url.strip_prefix("http://primary.example:1234").unwrap();
                let method = request["method"].as_str().unwrap().to_ascii_uppercase();
                assert!(head.starts_with(&format!("{method} {path} HTTP/1.1\r\n")));
                if method == "POST" {
                    assert_eq!(body, term(&request["body"]));
                    assert!(head.contains("Authorization: Basic dTpw\r\n"));
                } else {
                    assert!(!head.contains("Authorization:"));
                }
                let json = serde_json::to_vec(&term(&replies[i]["body"])).unwrap();
                write!(
                    s,
                    "HTTP/1.1 {} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    replies[i]["status"],
                    json.len()
                )
                .unwrap();
                s.write_all(&json).unwrap();
            }
        });
        assert_eq!(
            service.client().fetch(b"th_opaque").unwrap(),
            expected(row),
            "{}",
            row["name"]
        );
        drop(service);
        server.join().unwrap();
        assert!(fallback.accept().is_err());
    }
}
fn one_reply(raw: Vec<u8>, cfg: Config) -> Result<Option<Pin>, Error> {
    let l = listener();
    let service = Service::spawn(vec![node(&l, Chain::Aeternity)], cfg).unwrap();
    let server = thread::spawn(move || {
        let mut s = accept(&l);
        read_request(&mut s);
        let _ = s.write_all(&raw);
    });
    let result = service.client().fetch(b"th_opaque");
    drop(service);
    server.join().unwrap();
    result
}
fn payload() -> String {
    let p = ae_hyperchain::pin::Parent {
        epoch: 3.into(),
        height: 12.into(),
        block_hash: vec![42; 32],
    };
    let encoded = ae_codec::api::encode(
        ae_codec::api::ApiType::Bytearray,
        &ae_hyperchain::pin::encode_parent(&p, 4096).unwrap(),
    );
    json!({"tx":{"payload":encoded},"block_height":17}).to_string()
}
#[test]
fn bounded_framing_and_failed_lookups_keep_operational_errors_separate() {
    assert_eq!(
        one_reply(
            b"HTTP/1.1 302 FOUND\r\nLocation: /elsewhere\r\nContent-Length: 0\r\n\r\n".to_vec(),
            config()
        ),
        Err(Error::Redirect)
    );
    let b = payload();
    let raw = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{b}", b.len()).into_bytes();
    assert_eq!(
        one_reply(
            raw.clone(),
            Config {
                maximum_payload_bytes: 8,
                ..config()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        one_reply(
            raw,
            Config {
                maximum_number_digits: 1,
                ..config()
            }
        ),
        Err(Error::ResourceLimit)
    );
    let chunk = format!(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:X}\r\n{}\r\n0\r\n\r\n",
        b.len(),
        b
    );
    assert!(one_reply(chunk.into_bytes(), config()).unwrap().is_some());
    assert!(
        one_reply(format!("HTTP/1.1 200 OK\r\n\r\n{b}").into_bytes(), config())
            .unwrap()
            .is_some()
    );
    assert_eq!(
        one_reply(
            b"HTTP/1.1 200 OK\r\nContent-Length: 4194305\r\n\r\n".to_vec(),
            config()
        ),
        Err(Error::ResourceLimit)
    );
    for raw in [
        b"HTTP/1.1 404 NOT FOUND\r\nContent-Length: 0\r\n\r\n".to_vec(),
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nxx".to_vec(),
        b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx".to_vec(),
        vec![],
    ] {
        assert_eq!(one_reply(raw, config()).unwrap(), None);
    }
    let deep = format!("{}0{}", "[".repeat(130), "]".repeat(130));
    assert_eq!(
        one_reply(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{deep}",
                deep.len()
            )
            .into_bytes(),
            config()
        ),
        Err(Error::ResourceLimit)
    );
}
#[test]
fn worker_lifetime_cancellation_deadline_and_queue_are_bounded() {
    let l = listener();
    let service = Service::spawn(
        vec![node(&l, Chain::Aeternity)],
        Config {
            maximum_pending: 1,
            lookup_timeout: Duration::from_secs(2),
            ..config()
        },
    )
    .unwrap();
    let (seen, received) = mpsc::sync_channel(1);
    let (done, hold) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let mut s = accept(&l);
        read_request(&mut s);
        seen.send(()).unwrap();
        let _ = hold.recv_timeout(Duration::from_secs(5));
    });
    let c = service.client();
    let first = thread::spawn(move || c.fetch(b"one"));
    received.recv_timeout(Duration::from_secs(3)).unwrap();
    let (reply, replies) = mpsc::sync_channel(2);
    let mut waiting = vec![];
    for hash in [b"two", b"tri"] {
        let c = service.client();
        let reply = reply.clone();
        waiting.push(thread::spawn(move || reply.send(c.fetch(hash)).unwrap()));
    }
    assert_eq!(
        replies.recv_timeout(Duration::from_secs(2)).unwrap(),
        Err(Error::Busy)
    );
    assert_eq!(
        service.client().fetch(&vec![0; 4097]),
        Err(Error::ResourceLimit)
    );
    let old = service.client();
    let before = Instant::now();
    drop(service);
    assert!(before.elapsed() < Duration::from_secs(1));
    assert_eq!(first.join().unwrap(), Err(Error::Closed));
    assert_eq!(
        replies.recv_timeout(Duration::from_secs(2)).unwrap(),
        Err(Error::Closed)
    );
    for thread in waiting {
        thread.join().unwrap();
    }
    assert_eq!(old.check(), Err(Error::Closed));
    done.send(()).unwrap();
    server.join().unwrap();
    let l = listener();
    let service = Service::spawn(
        vec![node(&l, Chain::Aeternity)],
        Config {
            lookup_timeout: Duration::from_millis(50),
            ..config()
        },
    )
    .unwrap();
    let server = thread::spawn(move || {
        let mut s = accept(&l);
        read_request(&mut s);
        thread::sleep(Duration::from_millis(100));
    });
    assert_eq!(service.client().fetch(b"one"), Err(Error::Deadline));
    drop(service);
    server.join().unwrap();
}
#[test]
fn tls_checks_parent_identity_and_uses_explicit_trust() {
    const CA: &[u8] = include_bytes!("fixtures/upnp_test_ca.der");
    const CERT: &[u8] = include_bytes!("fixtures/upnp_test_server.der");
    const KEY: &[u8] = include_bytes!("fixtures/upnp_test_server_key.der");
    for trusted in [true, false] {
        let l = listener();
        let n = Node::new(
            Chain::Aeternity,
            &format!(
                "https://{}:{}",
                if trusted { "localhost" } else { "127.0.0.1" },
                l.local_addr().unwrap().port()
            ),
            None,
        )
        .unwrap();
        let service = Service::spawn(
            vec![n],
            Config {
                public_roots: false,
                roots: if trusted { vec![CA.to_vec()] } else { vec![] },
                ..config()
            },
        )
        .unwrap();
        let server = thread::spawn(move || {
            let cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![rustls::pki_types::CertificateDer::from(CERT.to_vec())],
                rustls::pki_types::PrivateKeyDer::try_from(KEY.to_vec()).unwrap(),
            )
            .unwrap();
            let mut s = rustls::StreamOwned::new(
                rustls::ServerConnection::new(Arc::new(cfg)).unwrap(),
                accept(&l),
            );
            if trusted {
                read_request(&mut s);
                let b = payload();
                write!(
                    s,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{b}",
                    b.len()
                )
                .unwrap();
                s.flush().unwrap();
            } else {
                let _ = s.read(&mut [0; 128]);
            }
        });
        assert_eq!(
            service.client().fetch(b"th_opaque").unwrap().is_some(),
            trusted
        );
        drop(service);
        server.join().unwrap();
    }
}
