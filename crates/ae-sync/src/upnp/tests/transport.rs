use super::*;
use std::{future::poll_fn, pin::Pin};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const CA: &[u8] = include_bytes!("../../../tests/fixtures/upnp_test_ca.der");
const CERT: &[u8] = include_bytes!("../../../tests/fixtures/upnp_test_server.der");
const KEY: &[u8] = include_bytes!("../../../tests/fixtures/upnp_test_server_key.der");

fn trusted_settings() -> Settings {
    Settings {
        request_timeout: Duration::from_secs(2),
        https: Https {
            public_roots: false,
            roots: vec![CA.to_vec()],
        },
        ..settings()
    }
}
fn transport(s: Settings) -> http::Transport {
    http::Transport::new(Arc::new(s)).unwrap()
}
fn acceptor() -> tokio_rustls::TlsAcceptor {
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
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
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}
async fn read_some<S: AsyncRead + Unpin>(stream: &mut S, bytes: &mut [u8]) -> usize {
    let mut buffer = ReadBuf::new(bytes);
    poll_fn(|cx| Pin::new(&mut *stream).poll_read(cx, &mut buffer))
        .await
        .unwrap();
    buffer.filled().len()
}
async fn request<S: AsyncRead + Unpin>(stream: &mut S) -> String {
    let mut raw = Vec::new();
    loop {
        let mut buffer = [0; 2048];
        let n = read_some(stream, &mut buffer).await;
        assert_ne!(n, 0, "request closed early");
        raw.extend_from_slice(&buffer[..n]);
        assert!(raw.len() <= 16384);
        if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = std::str::from_utf8(&raw[..end]).unwrap();
            let length = head
                .lines()
                .find_map(|line| line.strip_prefix("Content-Length: "))
                .map_or(0, |n| n.parse::<usize>().unwrap());
            if raw.len() == end + 4 + length {
                return String::from_utf8(raw).unwrap();
            }
        }
    }
}
async fn send_bytes<S: AsyncWrite + Unpin>(stream: &mut S, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        let n = poll_fn(|cx| Pin::new(&mut *stream).poll_write(cx, bytes))
            .await
            .unwrap();
        assert_ne!(n, 0);
        bytes = &bytes[n..];
    }
    poll_fn(|cx| Pin::new(&mut *stream).poll_flush(cx))
        .await
        .unwrap();
}
async fn reply<S: AsyncWrite + Unpin>(stream: &mut S, body: &str) {
    send_bytes(
        stream,
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    )
    .await;
}

#[test]
fn original_httpc_capture_records_url_repairs_tls_and_post_redirect_differences() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_upnp_http.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["source_commit"],
        "da8b260ba34ecb307561d190d402c145a9b4003c"
    );
    assert_eq!(fixture["loopback_only"], true);
    assert_eq!(fixture["joined_servers"], true);
    assert_eq!(fixture["full_node"], false);
    assert!(
        fixture["otp_httpc"]
            .as_str()
            .unwrap()
            .contains("inets-9.1.0.7/ebin/httpc.beam")
    );
    assert_eq!(fixture["descriptions"].as_array().unwrap().len(), 6);
    assert_eq!(fixture["redirects"].as_array().unwrap().len(), 12);
    assert_eq!(fixture["tls"].as_array().unwrap().len(), 3);
    assert_eq!(fixture["hostname"]["result"]["status"], 200);
    assert_eq!(fixture["tls"][0]["result"]["kind"], "error");
    assert!(
        fixture["tls"][0]["result"]["term"]
            .as_str()
            .unwrap()
            .contains("unknown_ca")
    );
    for index in [1, 2] {
        assert_eq!(fixture["tls"][index]["result"]["status"], 200);
    }
    let requests = fixture["requests"].as_array().unwrap();
    assert_eq!(requests.len(), 33);
    for (index, status) in [300, 301, 302, 303, 307, 308].into_iter().enumerate() {
        let offset = 6 + index * 4;
        assert_eq!(requests[offset]["path"], format!("/redirect/{status}"));
        assert_eq!(requests[offset]["method"], "GET");
        assert_eq!(requests[offset + 1]["path"], "/plain");
        assert_eq!(requests[offset + 2]["method"], "POST");
        let final_request = &requests[offset + 3];
        let changed = matches!(status, 301..=303);
        assert_eq!(
            final_request["method"],
            if changed { "GET" } else { "POST" }
        );
        assert_eq!(final_request["content_length"], "269");
        assert_eq!(
            final_request["body"].as_str().unwrap().len(),
            if changed { 0 } else { 269 }
        );
        assert_eq!(
            final_request["soap_action"],
            requests[offset + 2]["soap_action"]
        );
    }
    let root = http::Url::parse("http://localhost:1234/description/relative").unwrap();
    let native = &fixture["descriptions"][1]["result"]["value"];
    assert_eq!(native, "http://localhost:HTTP_PORTcontrol");
    assert_eq!(
        root.control("control").unwrap().path,
        "/description/control"
    );
}

#[test]
fn control_urls_resolve_directory_absolute_authority_and_urlbase_without_downgrade() {
    let root = http::Url::parse("https://router.example:8443/folder/device.xml").unwrap();
    for (reference, host, path) in [
        (
            "control?x=1&amp;y=2",
            "router.example",
            "/folder/control?x=1&y=2",
        ),
        ("../control#unused", "router.example", "/control"),
        ("/control", "router.example", "/control"),
        (
            "https://other.example:9443/control",
            "other.example",
            "/control",
        ),
        ("//other.example/control", "other.example", "/control"),
    ] {
        let xml = description(1).replacen("/control", reference, 1);
        let (url, _) = xml::control(xml.as_bytes(), &root, 1, &settings()).unwrap();
        assert_eq!(url.host(), host);
        assert_eq!(url.path, path);
    }
    let based = description(1)
        .replace(
            "<device>",
            "<URLBase>https://base.example:9443/base/</URLBase><device>",
        )
        .replacen("/control", "control", 1);
    let (url, _) = xml::control(based.as_bytes(), &root, 1, &settings()).unwrap();
    assert_eq!(url.host(), "base.example");
    assert_eq!(url.path, "/base/control");
    for invalid in [
        "<URLBase>/relative/</URLBase>",
        "<URLBase>http://router.example/</URLBase>",
        "<URLBase>https://router.example/</URLBase><URLBase>https://other.example/</URLBase>",
        "<URLBase><nested>https://router.example/</nested></URLBase>",
    ] {
        let xml = description(1).replacen("<device>", &format!("{invalid}<device>"), 1);
        assert!(xml::control(xml.as_bytes(), &root, 1, &settings()).is_err());
    }
    assert_eq!(
        root.control("http://router.example/control"),
        Err(Error::Redirect)
    );
    for bad in [
        "https://user:pass@router.example/",
        "https://[::1]/",
        "https://router.example\\@evil/",
        "https://router.example/\nheader",
        "https://router.example/ space",
        "https://255.255.255.255/",
    ] {
        assert!(http::Url::parse(bad).is_err(), "{bad}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn hostname_get_follows_all_native_redirect_statuses_and_preserves_host() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        for status in [300, 301, 302, 303, 307, 308] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let text = request(&mut stream).await;
            assert!(text.starts_with(&format!("GET /redirect/{status} HTTP/1.1\r\n")));
            assert!(text.contains(&format!("\r\nHost: localhost:{port}\r\n")));
            send_bytes(&mut stream, format!("HTTP/1.1 {status} Redirect\r\nLocation: ../final?ok=1#ignored\r\nContent-Length: 0\r\n\r\n").as_bytes()).await;
            let (mut stream, _) = listener.accept().await.unwrap();
            let text = request(&mut stream).await;
            assert!(text.starts_with("GET /final?ok=1 HTTP/1.1\r\n"));
            assert!(!text.contains("SOAPAction"));
            reply(&mut stream, "fixture response").await;
        }
    });
    let transport = transport(trusted_settings());
    for status in [300, 301, 302, 303, 307, 308] {
        let url = http::Url::parse(&format!("http://localhost:{port}/redirect/{status}")).unwrap();
        let result = transport.fetch(&url, Ipv4Addr::LOCALHOST).await.unwrap();
        assert_eq!(result.response.status, 200);
        assert_eq!(result.response.body, b"fixture response");
        assert_eq!(result.endpoint.url.path, "/final?ok=1");
        assert_eq!(result.endpoint.address, Ipv4Addr::LOCALHOST);
    }
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
    drop(transport); // Joins the owned hostname resolver.
}

#[tokio::test(flavor = "current_thread")]
async fn redirect_limit_duplicate_location_and_303_do_not_acknowledge_soap_writes() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert!(request(&mut stream).await.starts_with("GET /loop "));
            send_bytes(
                &mut stream,
                b"HTTP/1.1 302 Found\r\nLocation: /loop\r\nContent-Length: 0\r\n\r\n",
            )
            .await;
        }
        let (mut stream, _) = listener.accept().await.unwrap();
        request(&mut stream).await;
        send_bytes(
            &mut stream,
            b"HTTP/1.1 302 Found\r\nLocation: /a\r\nlocation: /b\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        {
            let status = 303;
            let (mut stream, _) = listener.accept().await.unwrap();
            assert!(request(&mut stream).await.starts_with("POST /control "));
            send_bytes(
                &mut stream,
                format!(
                    "HTTP/1.1 {status} Redirect\r\nLocation: /never\r\nContent-Length: 0\r\n\r\n"
                )
                .as_bytes(),
            )
            .await;
        }
        assert!(
            timeout(Duration::from_millis(40), listener.accept())
                .await
                .is_err()
        );
    });
    let transport = transport(Settings {
        redirects: 2,
        ..trusted_settings()
    });
    let url = http::Url::parse(&format!("http://127.0.0.1:{port}/loop")).unwrap();
    assert!(matches!(
        transport.fetch(&url, Ipv4Addr::LOCALHOST).await,
        Err(Error::Redirect)
    ));
    assert!(matches!(
        transport.fetch(&url, Ipv4Addr::LOCALHOST).await,
        Err(Error::Http)
    ));
    let endpoint = transport
        .resolve(&url.control("/control").unwrap())
        .await
        .unwrap();
    let action = xml::Action::Add {
        ports: ports(),
        lifetime: 7200,
    };
    let (header, body) = xml::request(action, 1, Ipv4Addr::LOCALHOST);
    {
        let status = 303;
        let result = transport
            .post(&endpoint, Ipv4Addr::LOCALHOST, &header, &body)
            .await
            .unwrap();
        assert_eq!(
            xml::response(&result, action, 1, &settings()),
            Err(Error::HttpStatus(status))
        );
    }
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn verified_tls_accepts_explicit_ca_dns_and_ip_but_rejects_unknown_ca_and_wrong_name() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let acceptor = acceptor();
        for valid in [false, false, true, true] {
            let (stream, _) = listener.accept().await.unwrap();
            match acceptor.accept(stream).await {
                Ok(mut stream) => {
                    assert!(valid);
                    assert_eq!(
                        stream.get_ref().1.alpn_protocol(),
                        Some(b"http/1.1".as_slice())
                    );
                    let text = request(&mut stream).await;
                    assert!(text.starts_with("POST /control HTTP/1.1\r\n"));
                    assert!(text.contains(&format!(
                        "\r\nHost: {}:{port}\r\n",
                        if text.contains("localhost") {
                            "localhost"
                        } else {
                            "127.0.0.1"
                        }
                    )));
                    reply(&mut stream, "verified").await;
                }
                Err(_) => assert!(!valid),
            }
        }
    });
    for (host, trusted, success) in [
        ("localhost", false, false),
        ("wrong.invalid", true, false),
        ("localhost", true, true),
        ("127.0.0.1", true, true),
    ] {
        let transport = transport(if trusted {
            trusted_settings()
        } else {
            Settings {
                request_timeout: Duration::from_secs(2),
                ..settings()
            }
        });
        // Pin the actual socket so wrong-name rejection does not need external DNS.
        let endpoint = http::Endpoint {
            url: http::Url::parse(&format!("https://{host}:{port}/control")).unwrap(),
            address: Ipv4Addr::LOCALHOST,
        };
        let result = transport
            .post(&endpoint, Ipv4Addr::LOCALHOST, "test#Probe", "probe")
            .await;
        if success {
            assert_eq!(result.unwrap().body, b"verified");
        } else {
            assert!(matches!(result, Err(Error::Tls(_))), "{result:?}");
        }
    }
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn tls_handshake_timeout_and_cancellation_close_the_owned_socket() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (observed, mut handshakes) = tokio::sync::mpsc::channel(2);
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 4096];
            assert_ne!(read_some(&mut stream, &mut bytes).await, 0);
            observed.send(()).await.unwrap();
            while read_some(&mut stream, &mut bytes).await != 0 {}
        }
    });
    let url = http::Url::parse(&format!("https://127.0.0.1:{port}/root")).unwrap();
    let transport = transport(Settings {
        request_timeout: Duration::from_millis(100),
        ..trusted_settings()
    });
    let first = transport.fetch(&url, Ipv4Addr::LOCALHOST);
    tokio::pin!(first);
    tokio::select! {
        result = &mut first => panic!("premature completion: {:?}", result.err()),
        _ = handshakes.recv() => (),
    }
    assert!(matches!(first.await, Err(Error::Timeout)));
    let mut second = Box::pin(transport.fetch(&url, Ipv4Addr::LOCALHOST));
    tokio::select! {
        result = &mut second => panic!("premature completion: {:?}", result.err()),
        _ = handshakes.recv() => (),
    }
    drop(second);
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn https_redirect_cannot_downgrade_and_same_origin_controls_retain_resolved_address() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let forbidden = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let plain_port = forbidden.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = acceptor().accept(stream).await.unwrap();
        request(&mut stream).await;
        send_bytes(&mut stream, format!("HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{plain_port}/never\r\nContent-Length: 0\r\n\r\n").as_bytes()).await;
        assert!(
            timeout(Duration::from_millis(50), forbidden.accept())
                .await
                .is_err()
        );
    });
    let transport = transport(trusted_settings());
    let url = http::Url::parse(&format!("https://localhost:{port}/root")).unwrap();
    assert!(matches!(
        transport.fetch(&url, Ipv4Addr::LOCALHOST).await,
        Err(Error::Redirect)
    ));
    let origin = http::Endpoint {
        url,
        address: Ipv4Addr::new(192, 0, 2, 1),
    };
    let control = transport
        .endpoint(origin.url.control("control").unwrap(), &origin)
        .await
        .unwrap();
    assert_eq!(control.address, origin.address);
    let other = transport
        .endpoint(http::Url::parse("http://localhost/other").unwrap(), &origin)
        .await
        .unwrap();
    assert_eq!(other.address, Ipv4Addr::LOCALHOST);
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn invalid_trust_anchors_and_excess_redirect_limits_fail_before_network_io() {
    for s in [
        Settings {
            redirects: 17,
            ..settings()
        },
        Settings {
            https: Https {
                public_roots: false,
                roots: vec![vec![1]],
            },
            ..settings()
        },
        Settings {
            https: Https {
                public_roots: false,
                roots: vec![CA.to_vec(); 33],
            },
            ..settings()
        },
    ] {
        assert!(matches!(Network::new(s), Err(Error::Configuration)));
    }
    assert!(Network::new(trusted_settings()).is_ok());
}

async fn secure_gateway(version: u8) {
    let ssdp = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let plain = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let secure = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let plain_port = plain.local_addr().unwrap().port();
    let secure_port = secure.local_addr().unwrap().port();
    let network = Network::new(Settings {
        ssdp: address(ssdp.local_addr().unwrap()),
        ..trusted_settings()
    })
    .unwrap();
    let discovery = tokio::spawn(async move {
        let mut bytes = [0; 8192];
        loop {
            let (n, source) = ssdp.recv_from(&mut bytes).await.unwrap();
            if std::str::from_utf8(&bytes[..n])
                .unwrap()
                .contains(&xml::target(version))
            {
                ssdp.send_to(format!("HTTP/1.1 200 OK\r\nST: {}\r\nLocation: http://localhost:{plain_port}/initial\r\n\r\n", xml::target(version)).as_bytes(), source).await.unwrap();
                break;
            }
        }
        let (mut stream, _) = plain.accept().await.unwrap();
        assert!(request(&mut stream).await.starts_with("GET /initial "));
        send_bytes(&mut stream, format!("HTTP/1.1 308 Redirect\r\nLocation: https://localhost:{secure_port}/folder/root.xml\r\nContent-Length: 0\r\n\r\n").as_bytes()).await;
    });
    let server = tokio::spawn(async move {
        let acceptor = acceptor();
        let (stream, _) = secure.accept().await.unwrap();
        let mut stream = acceptor.accept(stream).await.unwrap();
        assert!(
            request(&mut stream)
                .await
                .starts_with("GET /folder/root.xml ")
        );
        let mut xml = description(version).replacen("/control", "control", 1);
        if version == 2 {
            xml = xml.replacen(
                "<device>",
                &format!("<URLBase>https://localhost:{secure_port}/base/</URLBase><device>"),
                1,
            );
        }
        reply(&mut stream, &xml).await;
        drop(stream);
        let methods = if version == 1 {
            vec!["AddPortMapping", "DeletePortMapping"]
        } else {
            vec!["GetNATRSIPStatus", "AddAnyPortMapping", "DeletePortMapping"]
        };
        for method in methods {
            let (stream, _) = secure.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let text = request(&mut stream).await;
            assert!(text.starts_with(&format!(
                "POST /{}/control HTTP/1.1\r\n",
                if version == 1 { "folder" } else { "base" }
            )));
            assert!(text.contains(&format!("\r\nHost: localhost:{secure_port}\r\n")));
            assert!(text.contains(&format!(
                "SOAPAction: \"{}#{method}\"",
                xml::service(version)
            )));
            let body = match method {
                "GetNATRSIPStatus" => "<NewNATEnabled>1</NewNATEnabled>",
                "AddAnyPortMapping" => "<NewReservedPort>4016</NewReservedPort>",
                _ => "",
            };
            if method == "DeletePortMapping" {
                assert!(text.contains(&format!(
                    "<NewExternalPort>{}</NewExternalPort>",
                    if version == 1 { 4015 } else { 4016 }
                )));
            }
            reply(&mut stream, &response(version, method, body)).await;
        }
    });
    let gateway = network.discover().await.unwrap();
    assert_eq!(
        gateway.protocol(),
        if version == 1 {
            Protocol::Upnp1
        } else {
            Protocol::Upnp2
        }
    );
    let mapped = network.add(&gateway, ports()).await.unwrap();
    network
        .delete(
            &gateway,
            Ports {
                external: mapped.external,
                ..ports()
            },
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(3), discovery)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn hostname_redirect_https_v1_gateway_maps_and_deletes_at_final_description_relative_url() {
    timeout(Duration::from_secs(10), secure_gateway(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn hostname_redirect_https_v2_gateway_uses_urlbase_and_actual_assigned_port() {
    timeout(Duration::from_secs(10), secure_gateway(2))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn soap_redirect_upgrades_to_verified_tls_and_journals_each_exact_destination() {
    let plain = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let secure = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let plain_port = plain.local_addr().unwrap().port();
    let secure_port = secure.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = plain.accept().await.unwrap();
        let original = request(&mut stream).await;
        send_bytes(&mut stream, format!("HTTP/1.1 308 Redirect\r\nLocation: https://localhost:{secure_port}/control\r\nContent-Length: 0\r\n\r\n").as_bytes()).await;
        let (stream, _) = secure.accept().await.unwrap();
        let mut stream = acceptor().accept(stream).await.unwrap();
        let redirected = request(&mut stream).await;
        assert!(redirected.starts_with("POST /control HTTP/1.1"));
        assert!(redirected.contains(&format!("Host: localhost:{secure_port}")));
        assert_eq!(
            original.split_once("\r\n\r\n").unwrap().1,
            redirected.split_once("\r\n\r\n").unwrap().1
        );
        reply(&mut stream, "SOAP result").await;
    });
    let transport = transport(trusted_settings());
    let endpoint = transport
        .resolve(&http::Url::parse(&format!("http://127.0.0.1:{plain_port}/control")).unwrap())
        .await
        .unwrap();
    let mut intents = Vec::new();
    let result = transport
        .post_tracked(
            &endpoint,
            Ipv4Addr::LOCALHOST,
            "test#Write",
            "<operation/>",
            &mut |e| {
                intents.push(e.clone());
                Ok(())
            },
        )
        .await
        .unwrap();
    assert_eq!(result.response.body, b"SOAP result");
    assert_eq!(intents.len(), 2);
    assert_eq!(intents[0], endpoint);
    assert_eq!(intents[1], result.endpoint);
    assert!(result.endpoint.url.secure());
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn soap_hop_limit_and_deadline_stop_forwarding_and_close_the_last_socket() {
    for delayed in [false, true] {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert!(request(&mut stream).await.starts_with("POST /0 "));
            if delayed {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            send_bytes(
                &mut stream,
                b"HTTP/1.1 307 Redirect\r\nLocation: /1\r\nContent-Length: 0\r\n\r\n",
            )
            .await;
            let (mut stream, _) = listener.accept().await.unwrap();
            assert!(request(&mut stream).await.starts_with("POST /1 "));
            if delayed {
                // The first hop spent part of the one shared deadline. The
                // second connection must be dropped while awaiting its reply.
                assert_eq!(read_some(&mut stream, &mut [0; 1]).await, 0);
            } else {
                send_bytes(
                    &mut stream,
                    b"HTTP/1.1 308 Redirect\r\nLocation: /2\r\nContent-Length: 0\r\n\r\n",
                )
                .await;
                assert!(
                    timeout(Duration::from_millis(40), listener.accept())
                        .await
                        .is_err()
                );
            }
        });
        let transport = transport(Settings {
            redirects: 1,
            request_timeout: Duration::from_millis(800),
            ..trusted_settings()
        });
        let endpoint = transport
            .resolve(&http::Url::parse(&format!("http://127.0.0.1:{port}/0")).unwrap())
            .await
            .unwrap();
        let mut intents = Vec::new();
        let started = tokio::time::Instant::now();
        let result = transport
            .post_tracked(
                &endpoint,
                Ipv4Addr::LOCALHOST,
                "test#Write",
                "x",
                &mut |e| {
                    intents.push(e.clone());
                    Ok(())
                },
            )
            .await;
        assert_eq!(
            result.err(),
            Some(if delayed {
                Error::Timeout
            } else {
                Error::Redirect
            })
        );
        if delayed {
            assert!(started.elapsed() < Duration::from_millis(1100));
        }
        assert_eq!(intents.len(), 2);
        timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }
}
