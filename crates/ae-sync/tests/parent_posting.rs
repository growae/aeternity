use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{pin, signing::Keys};
use ae_sync::parent_pins::posting::{Failure, Outcome, Post, Request, Signing};
use ae_sync::parent_pins::{Chain, Config, Error, Node, Service};
use ae_types::{AccountPublicKey, Unsigned};
use serde_json::{Value as J, json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
#[path = "parent_posting/durable.rs"]
mod durable;
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
    if let Some(n) = v.get("float") {
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
fn signing() -> Signing {
    let mut seed = [0; 32];
    seed[31] = 7;
    Signing {
        network: b"parent_network".to_vec(),
        keys: Keys::new([Ed25519SecretKey::from_seed(&seed)]).into(),
    }
}
fn post() -> Post {
    let mut seed = [0; 32];
    seed[31] = 7;
    let mut hash = [0; 32];
    hash[31] = 42;
    Post {
        pin: pin::Parent {
            epoch: (-16).into(),
            height: num_bigint::BigInt::from(1) << 130,
            block_hash: hash.to_vec(),
        },
        account: Ed25519SecretKey::from_seed(&seed).public_key(),
        amount: 1.into(),
        fee: 1.into(),
    }
}
fn wait(r: &mut Request) -> Outcome {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(result) = r.try_result() {
            assert!(r.try_result().is_none());
            return result;
        }
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
}
fn reply(s: &mut impl Write, status: u16, value: &J) {
    let body = serde_json::to_vec(value).unwrap();
    write!(
        s,
        "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    s.write_all(&body).unwrap();
}
#[test]
fn original_posting_callbacks_and_canonical_signatures() {
    original_callbacks(|nodes, config, signing| {
        Service::spawn_with_signing(nodes, config, signing).unwrap()
    });
}
fn original_callbacks(mut spawn: impl FnMut(Vec<Node>, Config, Option<Signing>) -> Service) {
    let data: J = serde_json::from_str(include_str!("fixtures/hc_post.json")).unwrap();
    assert_eq!(data["rows"].as_array().unwrap().len(), 20);
    assert_eq!(
        post().account,
        AccountPublicKey(bytes(&data["account"]).try_into().unwrap())
    );
    for row in data["rows"].as_array().unwrap() {
        let chain = match row["chain"].as_str().unwrap() {
            "ae" => Chain::Aeternity,
            "doge" => Chain::Dogecoin,
            "btc" => Chain::Bitcoin,
            _ => panic!(),
        };
        let l = listener();
        let fallback = listener();
        let service = spawn(
            vec![node(&l, chain), node(&fallback, chain)],
            config(),
            Some(signing()),
        );
        let requests = row["requests"].as_array().unwrap().clone();
        let replies = row["replies"].as_array().unwrap().clone();
        let server = thread::spawn(move || {
            for (i, expected) in requests.iter().enumerate() {
                let mut stream = accept(&l);
                let (head, body) = read_request(&mut stream);
                let url = String::from_utf8(bytes(&expected["url"])).unwrap();
                let path = url.strip_prefix("http://primary.example:1234").unwrap();
                let method = expected["method"].as_str().unwrap().to_ascii_uppercase();
                assert!(head.starts_with(&format!("{method} {path} HTTP/1.1\r\n")));
                if method == "POST" {
                    assert_eq!(body, term(&expected["body"]));
                }
                assert_eq!(
                    head.contains("Authorization: Basic dTpw\r\n"),
                    chain == Chain::Dogecoin
                );
                reply(
                    &mut stream,
                    replies[i]["status"].as_u64().unwrap() as u16,
                    &term(&replies[i]["body"]),
                );
            }
            l
        });
        let mut p = post();
        p.amount = Unsigned::from_decimal(row["amount"].as_str().unwrap()).unwrap();
        p.fee = Unsigned::from_decimal(row["fee"].as_str().unwrap()).unwrap();
        let result = wait(&mut service.client().post(p).unwrap());
        let sent = row["requests"].as_array().unwrap().last().is_some_and(|r| {
            String::from_utf8(bytes(&r["url"]))
                .unwrap()
                .ends_with("/v3/transactions")
                || r.get("body")
                    .is_some_and(|v| term(v)["method"] == "sendrawtransaction")
        });
        match (&row["result"].get("hash"), result) {
            (
                Some(hash),
                Outcome::Submitted {
                    hash: actual,
                    submission,
                },
            ) => {
                assert_eq!(actual, bytes(hash), "{}", row["name"]);
                assert_eq!(submission.chain, chain);
                if chain == Chain::Aeternity {
                    let tx =
                        ae_tx::SignedTransaction::<ae_tx::Spend>::decode(&submission.transaction)
                            .unwrap();
                    use ae_tx::CanonicalTransaction;
                    let mut message = b"parent_network".to_vec();
                    message.extend_from_slice(&tx.transaction().encode());
                    assert_eq!(message, bytes(&row["signed_message"]));
                    assert!(ae_crypto::verify_ed25519(
                        &post().account,
                        &message,
                        &tx.signatures()[0]
                    ));
                } else {
                    assert_eq!(&*submission.transaction, b"signed");
                }
            }
            (None, Outcome::Unknown { .. }) => assert!(sent, "{}", row["name"]),
            (None, Outcome::NotSubmitted(_)) => assert!(!sent, "{}", row["name"]),
            (_, other) => panic!("{}: {other:?}", row["name"]),
        }
        service.shutdown().unwrap();
        let l = server.join().unwrap();
        assert!(l.accept().is_err());
        assert!(fallback.accept().is_err());
    }
}
#[test]
fn cancellation_preserves_possible_broadcast_and_queue_backpressure() {
    let l = listener();
    let mut cfg = config();
    cfg.maximum_pending = 1;
    let service =
        Service::spawn_with_signing(vec![node(&l, Chain::Aeternity)], cfg, Some(signing()))
            .unwrap();
    let client = service.client();
    let (sent, received) = mpsc::sync_channel(1);
    let (release, hold) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let mut s = accept(&l);
        read_request(&mut s);
        reply(&mut s, 200, &json!({"next_nonce":1}));
        let mut s = accept(&l);
        let (_, body) = read_request(&mut s);
        sent.send(body).unwrap();
        hold.recv_timeout(Duration::from_secs(5)).unwrap();
        l
    });
    let mut active = client.post(post()).unwrap();
    let body = received.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut queued = client.post(post()).unwrap();
    assert!(matches!(client.post(post()), Err(Error::Busy)));
    assert!(matches!(client.request(b"hash"), Err(Error::Busy)));
    queued.cancel();
    active.cancel();
    let result = wait(&mut active);
    let Outcome::Unknown {
        submission,
        reason: Failure::Local(Error::Closed),
    } = result
    else {
        panic!("{result:?}")
    };
    assert_eq!(
        ae_codec::api::encode(ae_codec::api::ApiType::Transaction, &submission.transaction),
        body["tx"]
    );
    assert_eq!(
        wait(&mut queued),
        Outcome::NotSubmitted(Failure::Local(Error::Closed))
    );
    service.shutdown().unwrap();
    assert!(matches!(client.post(post()), Err(Error::Closed)));
    release.send(()).unwrap();
    let l = server.join().unwrap();
    assert!(l.accept().is_err());
}
#[test]
fn shutdown_receipts_and_deadlines_do_not_erase_success() {
    let l = listener();
    let mut cfg = config();
    cfg.lookup_timeout = Duration::from_millis(1000);
    let service =
        Service::spawn_with_signing(vec![node(&l, Chain::Aeternity)], cfg, Some(signing()))
            .unwrap();
    let server = thread::spawn(move || {
        let mut s = accept(&l);
        read_request(&mut s);
        reply(&mut s, 200, &json!({"next_nonce":1}));
        let mut s = accept(&l);
        read_request(&mut s);
        reply(&mut s, 200, &json!({"tx_hash":"accepted"}));
        l
    });
    let mut r = service.client().post(post()).unwrap();
    let l = server.join().unwrap();
    // Queue a lookup barrier: its HTTP request proves the posting receipt exists.
    let mut barrier = service.client().request(b"barrier").unwrap();
    let mut s = accept(&l);
    read_request(&mut s);
    reply(&mut s, 404, &json!({}));
    let end = Instant::now() + Duration::from_secs(2);
    while barrier.try_result().is_none() {
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(1020));
    r.cancel();
    service.shutdown().unwrap();
    assert!(matches!(wait(&mut r),Outcome::Submitted{hash,..}if hash==b"accepted"));
    assert!(l.accept().is_err());
}
#[test]
fn stop_before_broadcast_and_missing_keys_leave_no_submission() {
    for stop in [false, true] {
        let l = listener();
        let service = Service::spawn(vec![node(&l, Chain::Aeternity)], config()).unwrap();
        let mut r = service.client().post(post()).unwrap();
        let mut s = accept(&l);
        read_request(&mut s);
        if stop {
            service.shutdown().unwrap();
            assert_eq!(
                wait(&mut r),
                Outcome::NotSubmitted(Failure::Local(Error::Closed))
            );
        } else {
            reply(&mut s, 200, &json!({"next_nonce":1}));
            assert_eq!(wait(&mut r), Outcome::NotSubmitted(Failure::MissingKey));
            service.shutdown().unwrap();
        }
        assert!(l.accept().is_err());
    }
}
#[test]
fn broadcast_failures_remain_unknown_without_retries() {
    for mode in [0, 1, 2, 3, 4] {
        let l = listener();
        let mut cfg = config();
        cfg.request_timeout = Duration::from_millis(500);
        cfg.lookup_timeout = Duration::from_millis(1000);
        let service =
            Service::spawn_with_signing(vec![node(&l, Chain::Aeternity)], cfg, Some(signing()))
                .unwrap();
        let mut r = service.client().post(post()).unwrap();
        let mut s = accept(&l);
        read_request(&mut s);
        reply(&mut s, 200, &json!({"next_nonce":1}));
        let mut s = accept(&l);
        read_request(&mut s);
        match mode {
            0 => drop(s),
            1 => reply(&mut s, 500, &json!({})),
            2 => reply(&mut s, 302, &json!({})),
            3 => {
                service.stop();
            }
            _ => {
                thread::sleep(Duration::from_millis(1050));
            }
        }
        assert!(
            matches!(wait(&mut r), Outcome::Unknown { .. }),
            "mode {mode}"
        );
        service.shutdown().unwrap();
        assert!(l.accept().is_err());
    }
}
#[test]
fn dropping_receipt_cancels_and_releases_the_worker() {
    let l = listener();
    let service =
        Service::spawn_with_signing(vec![node(&l, Chain::Aeternity)], config(), Some(signing()))
            .unwrap();
    let r = service.client().post(post()).unwrap();
    let mut s = accept(&l);
    read_request(&mut s);
    drop(r);
    let mut next = service.client().request(b"next").unwrap();
    let mut s = accept(&l);
    let (head, _) = read_request(&mut s);
    assert!(head.starts_with("GET /v3/transactions/next "));
    reply(&mut s, 404, &json!({}));
    let end = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(r) = next.try_result() {
            assert_eq!(r, Ok(None));
            break;
        }
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
    service.shutdown().unwrap();
    assert!(l.accept().is_err());
}
#[test]
fn quotas_and_unsupported_bitcoin_prevent_network_writes() {
    let l = listener();
    let mut cfg = config();
    cfg.maximum_payload_bytes = 1;
    let service = Service::spawn(vec![node(&l, Chain::Aeternity)], cfg).unwrap();
    assert!(matches!(
        service.client().post(post()),
        Err(Error::ResourceLimit)
    ));
    service.shutdown().unwrap();
    assert!(l.accept().is_err());
    let mut cfg = config();
    cfg.maximum_number_digits = 1;
    let service = Service::spawn(vec![node(&l, Chain::Aeternity)], cfg).unwrap();
    let mut p = post();
    p.fee = 100.into();
    assert!(matches!(
        service.client().post(p),
        Err(Error::ResourceLimit)
    ));
    service.shutdown().unwrap();
    assert!(l.accept().is_err());
    let service = Service::spawn(vec![node(&l, Chain::Bitcoin)], config()).unwrap();
    assert_eq!(
        wait(&mut service.client().post(post()).unwrap()),
        Outcome::NotSubmitted(Failure::Unsupported)
    );
    service.shutdown().unwrap();
    assert!(l.accept().is_err());
}
