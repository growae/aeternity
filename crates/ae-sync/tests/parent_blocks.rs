use ae_hyperchain::{
    parent_block::ParentBlock,
    parent_connector::{
        Consensus,
        fetch::{self, BackendError, Query},
    },
};
use ae_sync::parent_blocks::{Backend, Chain, Config, Error, Node};
use ae_types::Unsigned;
use serde_json::{Value as J, json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn term(v: &J) -> J {
    if let Some(b) = v.get("binary") {
        return json!(String::from_utf8(bytes(b)).unwrap());
    }
    if let Some(n) = v.get("integer") {
        return serde_json::from_str(n.as_str().unwrap()).unwrap();
    }
    if let Some(a) = v.get("atom") {
        return match a.as_str().unwrap() {
            "true" => json!(true),
            "false" => json!(false),
            "null" => J::Null,
            s => json!(s),
        };
    }
    if let Some(l) = v.get("list").or_else(|| v.get("tuple")) {
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
                        serde_json::from_slice(&raw[end + 4..end + 4 + len]).unwrap()
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
fn reply(s: &mut impl Write, status: u16, body: &[u8]) {
    let _ = write!(
        s,
        "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = s.write_all(body);
    let _ = s.flush();
}
fn node(l: &TcpListener, c: Chain) -> Node {
    Node::new(
        c,
        &format!("http://{}", l.local_addr().unwrap()),
        (c != Chain::Aeternity).then(|| (b"u".to_vec(), b"p".to_vec())),
    )
    .unwrap()
}
fn config() -> Config {
    Config {
        request_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}
fn pool(backend: Arc<Backend>, stop: Arc<AtomicBool>) -> fetch::Pool {
    let hosts = backend.hosts();
    fetch::Pool::spawn(backend, hosts, 32, stop).unwrap()
}
fn query(p: &mut fetch::Pool, q: Query, seed: [u8; 16]) -> Result<Consensus<usize>, fetch::Error> {
    p.fetch(q, seed, Duration::from_secs(3), 65536, Default::default())
}
fn number(v: &J) -> Unsigned {
    Unsigned::from_decimal(v["integer"].as_str().unwrap()).unwrap()
}
fn expected(row: &J) -> Consensus<usize> {
    let r = &row["result"];
    match r["kind"].as_str().unwrap() {
        "not_found" => Consensus::NotFound,
        "other" => Consensus::NoAgreement,
        "block" => Consensus::Block {
            node: 0,
            block: ParentBlock {
                hash: bytes(&r["hash"]["binary"]),
                height: number(&r["height"]),
                time: number(&r["time"]),
                previous_hash: r["previous"].get("binary").map(bytes),
            },
        },
        _ => panic!(),
    }
}
#[test]
fn original_header_callbacks_match_real_http_requests_and_quorum_results() {
    let data: J = serde_json::from_str(include_str!("fixtures/hc_parent_rpc.json")).unwrap();
    assert_eq!(data["rows"].as_array().unwrap().len(), 112);
    let mut redirects = 0;
    for row in data["rows"].as_array().unwrap() {
        let chain = match row["chain"].as_str().unwrap() {
            "aeternity" => Chain::Aeternity,
            "btc" => Chain::Bitcoin,
            "doge" => Chain::Dogecoin,
            _ => panic!(),
        };
        let l = listener();
        let backend = Arc::new(Backend::new(vec![node(&l, chain)], config()).unwrap());
        let requests = row["requests"].as_array().unwrap().clone();
        let replies = row["replies"].as_array().unwrap().clone();
        let server = thread::spawn(move || {
            for (i, r) in requests.iter().enumerate() {
                let mut s = accept(&l);
                let (head, body) = read_request(&mut s);
                let url = String::from_utf8(bytes(&r["url"])).unwrap();
                let path = url.strip_prefix("http://primary.example:1234").unwrap();
                let method = r["method"].as_str().unwrap().to_ascii_uppercase();
                assert!(head.starts_with(&format!("{method} {path} HTTP/1.1\r\n")));
                if method == "POST" {
                    assert_eq!(body, term(&r["body"]));
                    assert!(head.contains("Authorization: Basic dTpw\r\n"));
                } else {
                    assert!(!head.contains("Authorization:"));
                }
                let r = &replies[i];
                if r.get("error").is_some() {
                    continue;
                }
                let body = if let Some(raw) = r.get("raw") {
                    bytes(raw)
                } else {
                    serde_json::to_vec(&term(&r["body"])).unwrap()
                };
                reply(&mut s, r["status"].as_u64().unwrap() as u16, &body);
            }
            l
        });
        let q = term(&row["query"]);
        let q = if q == json!("top") {
            Query::Top
        } else if q[0] == "height" {
            Query::Height(q[1].to_string().parse().unwrap())
        } else {
            Query::Hash(q[1].as_str().unwrap().as_bytes().to_vec())
        };
        let mut p = pool(backend, Arc::new(AtomicBool::new(false)));
        let result = query(&mut p, q, bytes(&row["seed"]).try_into().unwrap());
        if row["name"] == "http_redirect" {
            // Redirect refusal is an explicit local acquisition policy, not a
            // negative parent vote. The native fixture stubs the httpc boundary.
            redirects += 1;
            assert_eq!(
                result,
                Err(fetch::Error::Backend(BackendError::Unavailable))
            );
        } else {
            assert_eq!(
                result,
                Ok(expected(row)),
                "{} {}",
                row["chain"],
                row["name"]
            );
        }
        drop(p);
        let l = server.join().unwrap();
        assert!(l.accept().is_err());
    }
    assert_eq!(redirects, 9);
}
fn ae_block(previous: J) -> J {
    json!({"hash":"kh_parent","prev_key_hash":previous,"height":7,"time":17000})
}
fn once(raw: Vec<u8>, c: Config) -> Result<Consensus<usize>, fetch::Error> {
    let l = listener();
    let backend = Arc::new(Backend::new(vec![node(&l, Chain::Aeternity)], c).unwrap());
    let server = thread::spawn(move || {
        let mut s = accept(&l);
        read_request(&mut s);
        reply(&mut s, 200, &raw);
    });
    let result = query(
        &mut pool(backend, Arc::new(AtomicBool::new(false))),
        Query::Top,
        [0; 16],
    );
    server.join().unwrap();
    result
}
#[test]
fn response_limits_are_local_failures_and_do_not_vote_that_a_block_is_missing() {
    for (c, body) in [
        (
            Config {
                maximum_body_bytes: 1024,
                ..config()
            },
            vec![b' '; 2048],
        ),
        (
            Config {
                maximum_hash_bytes: 3,
                ..config()
            },
            serde_json::to_vec(&ae_block(json!("previous"))).unwrap(),
        ),
        (
            Config {
                maximum_number_digits: 2,
                ..config()
            },
            serde_json::to_vec(&ae_block(json!("previous"))).unwrap(),
        ),
    ] {
        assert_eq!(
            once(body, c),
            Err(fetch::Error::Backend(BackendError::ResourceLimit))
        );
    }
}
#[test]
fn null_previous_hash_is_not_an_empty_binary_in_real_parent_quorums() {
    for majority in [false, true] {
        let mut nodes = vec![];
        let mut servers = vec![];
        for i in 0..3 {
            let l = listener();
            nodes.push(node(&l, Chain::Aeternity));
            let previous = if i == 0 || (i == 1 && majority) {
                J::Null
            } else {
                json!("")
            };
            servers.push(thread::spawn(move || {
                let mut s = accept(&l);
                read_request(&mut s);
                reply(
                    &mut s,
                    200,
                    &serde_json::to_vec(&ae_block(previous)).unwrap(),
                );
            }));
        }
        let b = Arc::new(Backend::new(nodes, config()).unwrap());
        let Consensus::Block { block, .. } = query(
            &mut pool(b, Arc::new(AtomicBool::new(false))),
            Query::Top,
            [7; 16],
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(
            block.previous_hash,
            if majority { None } else { Some(vec![]) }
        );
        for s in servers {
            s.join().unwrap();
        }
    }
}
#[test]
fn invalid_host_configurations_are_rejected_before_starting_fetch_workers() {
    assert!(matches!(
        Backend::new(vec![], config()),
        Err(Error::Configuration)
    ));
    let l = listener();
    assert!(matches!(
        Backend::new(
            vec![node(&l, Chain::Aeternity), node(&l, Chain::Dogecoin)],
            config()
        ),
        Err(Error::Configuration)
    ));
    let no_auth = Node::new(Chain::Bitcoin, "http://127.0.0.1:1", None).unwrap();
    assert!(matches!(
        Backend::new(vec![no_auth], config()),
        Err(Error::Configuration)
    ));
    for c in [
        Config {
            maximum_hosts: 0,
            ..config()
        },
        Config {
            maximum_number_digits: 0,
            ..config()
        },
        Config {
            request_timeout: Duration::ZERO,
            ..config()
        },
        Config {
            roots: vec![vec![0]],
            ..config()
        },
    ] {
        assert!(matches!(
            Backend::new(vec![node(&l, Chain::Aeternity)], c),
            Err(Error::Configuration)
        ));
    }
    assert!(l.accept().is_err());
}
#[test]
fn native_http_timeouts_preserve_chain_specific_negative_vote_classification() {
    for chain in [Chain::Aeternity, Chain::Bitcoin, Chain::Dogecoin] {
        let l = listener();
        let backend = Arc::new(
            Backend::new(
                vec![node(&l, chain)],
                Config {
                    request_timeout: Duration::from_millis(50),
                    ..config()
                },
            )
            .unwrap(),
        );
        let server = thread::spawn(move || {
            let mut s = accept(&l);
            read_request(&mut s);
            thread::sleep(Duration::from_millis(150));
            drop(s);
        });
        let actual = query(
            &mut pool(backend, Arc::new(AtomicBool::new(false))),
            Query::Top,
            [0; 16],
        );
        assert_eq!(
            actual,
            Ok(if chain == Chain::Aeternity {
                Consensus::NotFound
            } else {
                Consensus::NoAgreement
            })
        );
        server.join().unwrap();
    }
}
#[test]
fn connector_cancellation_joins_pending_http_without_waiting_for_server_timeout() {
    let l = listener();
    let backend = Arc::new(Backend::new(vec![node(&l, Chain::Aeternity)], config()).unwrap());
    let (ready, seen) = mpsc::sync_channel(1);
    let (release, done) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let mut s = accept(&l);
        read_request(&mut s);
        ready.send(()).unwrap();
        done.recv_timeout(Duration::from_secs(3)).unwrap();
    });
    let stop = Arc::new(AtomicBool::new(false));
    let p = pool(backend, stop.clone());
    let worker = thread::spawn(move || {
        let mut p = p;
        let result = query(&mut p, Query::Top, [0; 16]);
        drop(p);
        result
    });
    seen.recv_timeout(Duration::from_secs(3)).unwrap();
    let start = Instant::now();
    stop.store(true, Ordering::Release);
    assert_eq!(worker.join().unwrap(), Err(fetch::Error::Stopped));
    assert!(start.elapsed() < Duration::from_secs(1));
    release.send(()).unwrap();
    server.join().unwrap();
}
#[test]
fn same_host_overlap_is_bounded_and_does_not_cancel_the_original_request() {
    let l = listener();
    let b = Arc::new(Backend::new(vec![node(&l, Chain::Aeternity)], config()).unwrap());
    let (ready, seen) = mpsc::sync_channel(1);
    let (release, done) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let mut s = accept(&l);
        read_request(&mut s);
        ready.send(()).unwrap();
        done.recv_timeout(Duration::from_secs(3)).unwrap();
        reply(
            &mut s,
            200,
            &serde_json::to_vec(&ae_block(J::Null)).unwrap(),
        );
    });
    let mut first = pool(b.clone(), Arc::new(AtomicBool::new(false)));
    let worker = thread::spawn(move || query(&mut first, Query::Top, [0; 16]));
    seen.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(
        query(
            &mut pool(b, Arc::new(AtomicBool::new(false))),
            Query::Top,
            [0; 16]
        ),
        Err(fetch::Error::Backend(BackendError::ResourceLimit))
    );
    release.send(()).unwrap();
    assert!(matches!(
        worker.join().unwrap(),
        Ok(Consensus::Block { .. })
    ));
    server.join().unwrap();
}
#[test]
fn tls_verifies_configured_trust_and_parent_hostname() {
    const CA: &[u8] = include_bytes!("fixtures/upnp_test_ca.der");
    const CERT: &[u8] = include_bytes!("fixtures/upnp_test_server.der");
    const KEY: &[u8] = include_bytes!("fixtures/upnp_test_server_key.der");
    for trusted in [true, false] {
        let l = listener();
        let n = Node::new(
            Chain::Aeternity,
            &format!("https://localhost:{}", l.local_addr().unwrap().port()),
            None,
        )
        .unwrap();
        let b = Arc::new(
            Backend::new(
                vec![n],
                Config {
                    public_roots: false,
                    roots: if trusted { vec![CA.to_vec()] } else { vec![] },
                    ..config()
                },
            )
            .unwrap(),
        );
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
                reply(
                    &mut s,
                    200,
                    &serde_json::to_vec(&ae_block(J::Null)).unwrap(),
                );
            } else {
                let _ = s.read(&mut [0; 128]);
            }
        });
        assert_eq!(
            matches!(
                query(
                    &mut pool(b, Arc::new(AtomicBool::new(false))),
                    Query::Top,
                    [0; 16]
                )
                .unwrap(),
                Consensus::Block { .. }
            ),
            trusted
        );
        server.join().unwrap();
    }
}
