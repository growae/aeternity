use super::*;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex},
};
use tokio::{
    net::{TcpListener, TcpStream, UdpSocket},
    time::timeout,
};

mod leases;
mod ownership;
mod transport;

fn settings() -> Settings {
    Settings {
        bind: Ipv4Addr::LOCALHOST,
        initial_retry: Duration::from_millis(10),
        request_timeout: Duration::from_millis(500),
        discovery_timeout: Duration::from_secs(2),
        ..Settings::default()
    }
}
fn ports() -> Ports {
    Ports {
        internal: 3015,
        external: 4015,
    }
}
fn address(a: std::net::SocketAddr) -> SocketAddrV4 {
    match a {
        std::net::SocketAddr::V4(a) => a,
        _ => panic!("IPv4 test"),
    }
}
fn description(version: u8) -> String {
    format!(
        "<root xmlns=\"urn:schemas-upnp-org:device-1-0\"><device><deviceType>{}</deviceType><deviceList><device><deviceType>urn:schemas-upnp-org:device:WANDevice:{version}</deviceType><deviceList><device><deviceType>urn:schemas-upnp-org:device:WANConnectionDevice:{version}</deviceType><serviceList><service><serviceType>{}</serviceType><controlURL>/control</controlURL></service></serviceList></device></deviceList></device></deviceList></device></root>",
        xml::target(version),
        xml::service(version)
    )
}
fn envelope(body: &str) -> String {
    format!(
        "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body>{body}</s:Body></s:Envelope>"
    )
}
fn response(version: u8, method: &str, body: &str) -> String {
    envelope(&format!(
        "<u:{method}Response xmlns:u=\"{}\">{body}</u:{method}Response>",
        xml::service(version)
    ))
}
fn fault(code: u16) -> String {
    envelope(&format!(
        "<s:Fault><detail><UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\"><errorCode>{code}</errorCode></UPnPError></detail></s:Fault>"
    ))
}

#[test]
fn native_protocol_fixture_preserves_v1_and_records_v2_repairs() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_upnp_protocol.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["source_commit"],
        "da8b260ba34ecb307561d190d402c145a9b4003c"
    );
    let s = settings();
    for row in fixture["pmp"].as_array().unwrap() {
        let name = row["name"].as_str().unwrap();
        let bytes = hex::decode(row["bytes"].as_str().unwrap()).unwrap();
        if row["opcode"] == 0 {
            assert_eq!(
                pmp::external_response(&bytes).unwrap().to_string(),
                row["result"]["address"].as_str().unwrap()
            );
        } else {
            let result = pmp::mapping_response(&bytes, ports(), name == "deletion");
            match name {
                "mapping" | "deletion" => {
                    let m = result.unwrap();
                    assert_eq!(
                        u64::from(m.internal),
                        row["result"]["internal"].as_u64().unwrap()
                    );
                    assert_eq!(
                        u64::from(m.external),
                        row["result"]["external"].as_u64().unwrap()
                    );
                    assert_eq!(
                        u64::from(m.lifetime.unwrap()),
                        row["result"]["lifetime"].as_u64().unwrap()
                    );
                    assert_eq!(
                        u64::from(m.gateway_epoch.unwrap()),
                        row["result"]["since"].as_u64().unwrap()
                    );
                }
                "version_ignored" | "unexpected_internal_unchecked" => {
                    assert_eq!(row["result"]["kind"], "mapping");
                    assert_eq!(result, Err(Error::Response));
                }
                "status_6" => {
                    assert_eq!(row["result"]["reason"], "function_clause");
                    assert_eq!(result, Err(Error::NatPmp(6)));
                }
                "wrong_opcode" => {
                    // This fixture calls the native parser with UDP opcode 1
                    // while Rust's TCP API never accepts that operation.
                    assert_eq!(row["result"]["error"], "bad_response");
                    assert!(result.is_ok());
                }
                _ => assert!(result.is_err(), "{name}"),
            }
        }
    }
    for row in fixture["soap"].as_array().unwrap() {
        let name = row["name"].as_str().unwrap();
        let version = if name.starts_with("v1") { 1 } else { 2 };
        for (index, request) in row["requests"].as_array().unwrap().iter().enumerate() {
            let action = if name.ends_with("delete") {
                xml::Action::Delete { external: 4015 }
            } else if name.ends_with("nat_status") {
                xml::Action::NatStatus
            } else {
                xml::Action::Add {
                    ports: ports(),
                    lifetime: if index == 0 { 7200 } else { 0 },
                }
            };
            let (header, body) = xml::request(action, version, Ipv4Addr::LOCALHOST);
            let native = request["body"].as_str().unwrap();
            let corrected = if version == 2 {
                native
                    .replace("WANIPConnection:1", "WANIPConnection:2")
                    .replace("</u:AddPortMapping>", "</u:AddAnyPortMapping>")
            } else {
                native.to_string()
            };
            assert_eq!(body, corrected, "{name}");
            assert_eq!(
                format!("\"{header}\""),
                request["headers"]["SOAPAction"]
                    .as_str()
                    .unwrap()
                    .replace("WANIPConnection:1", &format!("WANIPConnection:{version}"))
            );
            if version == 2 && matches!(action, xml::Action::Add { .. }) {
                assert!(xml::document(native.as_bytes(), &s).is_err());
            }
        }
    }
    for row in fixture["descriptions"].as_array().unwrap() {
        let url = http::Url::parse(row["root"].as_str().unwrap()).unwrap();
        let version = if row["backend"] == "natupnp_v2" { 2 } else { 1 };
        let result = xml::control(row["xml"].as_str().unwrap().as_bytes(), &url, version, &s);
        match row["name"].as_str().unwrap() {
            "v1_relative" => assert_eq!(result.unwrap().0.path, "/folder/control"),
            "v1_absolute" => assert_eq!(result.unwrap().0.path, "/control"),
            _ => assert_eq!(
                result.unwrap().0,
                http::Url::parse(row["result"]["url"].as_str().unwrap()).unwrap()
            ),
        }
    }
}

#[test]
fn pmp_packets_validate_opcode_version_status_ports_and_deletion() {
    assert_eq!(
        pmp::mapping_request(ports(), 7200),
        [0, 2, 0, 0, 11, 199, 15, 175, 0, 0, 28, 32]
    );
    let mut raw = vec![0, 130, 0, 0, 0, 0, 0, 42, 11, 199, 15, 175, 0, 0, 28, 32];
    let mapping = pmp::mapping_response(&raw, ports(), false).unwrap();
    assert_eq!(mapping.gateway_epoch, Some(42));
    assert_eq!(mapping.lifetime, Some(7200));
    raw[0] = 1;
    assert_eq!(
        pmp::mapping_response(&raw, ports(), false),
        Err(Error::Response)
    );
    raw[0] = 0;
    raw[9] = 198;
    assert_eq!(
        pmp::mapping_response(&raw, ports(), false),
        Err(Error::Response)
    );
    raw[9] = 199;
    for status in 1u16..=6 {
        raw[2..4].copy_from_slice(&status.to_be_bytes());
        assert_eq!(
            pmp::mapping_response(&raw, ports(), false),
            Err(Error::NatPmp(status))
        );
    }
    raw[2..4].fill(0);
    assert_eq!(
        pmp::mapping_response(&raw, ports(), true),
        Err(Error::Response)
    );
    raw[10..].fill(0);
    assert!(pmp::mapping_response(&raw, ports(), true).is_ok());
    assert_eq!(
        pmp::mapping_response(&raw, ports(), false),
        Err(Error::Response)
    );
    assert_eq!(
        pmp::external_response(&[0, 128, 0, 0, 0, 0, 0, 1, 203, 0, 113, 4]).unwrap(),
        Ipv4Addr::new(203, 0, 113, 4)
    );
    assert_eq!(
        pmp::external_response(&[0, 128, 0, 0, 0, 0, 0, 1]),
        Err(Error::Response)
    );
    assert_eq!(
        pmp::external_response(&[0, 128, 0, 2, 0, 0, 0, 1]),
        Err(Error::NatPmp(2))
    );
}

#[test]
fn http_urls_framing_and_chunk_limits() {
    let s = settings();
    let url = http::Url::parse("http://127.0.0.1:1234/root?a=b#anchor").unwrap();
    assert_eq!(url.path, "/root?a=b");
    assert_eq!(url.control("/control").unwrap().path, "/control");
    for bad in [
        "ftp://127.0.0.1/",
        "http://127.0.0.1:0/",
        "http://user@127.0.0.1/",
        "http://127.0.0.1/\r\nX: x",
        "http://0.0.0.0/",
        "http://224.0.0.1/",
    ] {
        assert!(http::Url::parse(bad).is_err(), "{bad}");
    }
    assert_eq!(url.control("relative").unwrap().path, "/relative");
    assert_eq!(
        url.control("//192.0.2.1/control").unwrap().host(),
        "192.0.2.1"
    );
    for bad in [
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: +2\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n",
    ] {
        assert!(http::headers(bad, &s).is_err());
    }
    let raw = b"3;ext=1\r\nabc\r\n2\r\nde\r\n0\r\nX-Test: yes\r\n\r\n";
    let mut chunks = http::Chunks::default();
    for n in 0..raw.len() {
        assert!(chunks.advance(&raw[..n], &s).unwrap().is_none());
    }
    assert_eq!(chunks.advance(raw, &s).unwrap().unwrap(), b"abcde");
    assert!(http::Chunks::default().advance(b"100001\r\n", &s).is_err());
    assert!(
        http::Chunks::default()
            .advance(b"0\r\nContent-Length: 0\r\n\r\n", &s)
            .is_err()
    );
}

#[test]
fn xml_hierarchy_entities_nodes_faults_and_correct_v2_messages() {
    let s = settings();
    let url = http::Url::parse("http://127.0.0.1/root").unwrap();
    for v in [1, 2] {
        let (_, parsed) = xml::control(description(v).as_bytes(), &url, v, &s).unwrap();
        assert_eq!(parsed, v);
        let action = xml::Action::Add {
            ports: ports(),
            lifetime: 7200,
        };
        let (header, body) = xml::request(action, v, Ipv4Addr::LOCALHOST);
        assert_eq!(header, format!("{}#{}", xml::service(v), action.name(v)));
        assert!(xml::document(body.as_bytes(), &s).is_ok());
        let reply = http::Response {
            status: 200,
            body: response(
                v,
                action.name(v),
                if v == 2 {
                    "<NewReservedPort>5000</NewReservedPort>"
                } else {
                    ""
                },
            )
            .into_bytes(),
        };
        assert_eq!(
            xml::response(&reply, action, v, &s).unwrap(),
            if v == 2 { Some(5000) } else { None }
        );
        let error = http::Response {
            status: 500,
            body: fault(725).into_bytes(),
        };
        assert_eq!(xml::response(&error, action, v, &s), Err(Error::Soap(725)));
    }
    assert!(xml::control(description(2).as_bytes(), &url, 1, &s).is_err());
    assert_eq!(
        xml::control(description(1).as_bytes(), &url, 2, &s)
            .unwrap()
            .1,
        1
    );
    assert!(
        xml::document(
            b"<!DOCTYPE root [<!ENTITY x SYSTEM 'file:///etc/passwd'>]><root>&x;</root>",
            &s
        )
        .is_err()
    );
    let mut limits = s.clone();
    limits.xml_nodes = 4;
    assert!(xml::document(description(1).as_bytes(), &limits).is_err());
    let hidden = description(1)
        .replace("<serviceList>", "<unrelated><serviceList>")
        .replace("</serviceList>", "</serviceList></unrelated>");
    assert!(xml::control(hidden.as_bytes(), &url, 1, &s).is_err());
}

async fn read_request(stream: &TcpStream) -> String {
    let mut raw = Vec::new();
    loop {
        stream.readable().await.unwrap();
        let mut buffer = [0; 2048];
        match stream.try_read(&mut buffer) {
            Ok(0) => panic!("closed request"),
            Ok(n) => raw.extend_from_slice(&buffer[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("{e}"),
        }
        assert!(raw.len() < 16384);
        if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            let text = std::str::from_utf8(&raw[..end]).unwrap();
            let len = text
                .lines()
                .find_map(|line| line.strip_prefix("Content-Length: "))
                .map(|n| n.parse::<usize>().unwrap())
                .unwrap_or(0);
            if raw.len() == end + 4 + len {
                return String::from_utf8(raw).unwrap();
            }
        }
    }
}
async fn send(stream: &TcpStream, raw: &[u8]) {
    let mut sent = 0;
    while sent < raw.len() {
        stream.writable().await.unwrap();
        match stream.try_write(&raw[sent..]) {
            Ok(n) => {
                assert_ne!(n, 0);
                sent += n;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("{e}"),
        }
    }
}
async fn answer(stream: &TcpStream, status: u16, body: &str) {
    let raw = format!(
        "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    send(stream, raw.as_bytes()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_pmp_discovery_mapping_deletion_and_retry() {
    let gateway = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let unused = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let mut s = settings();
    s.gateways = vec![address(gateway.local_addr().unwrap())];
    s.ssdp = address(unused.local_addr().unwrap());
    let network = Network::new(s).unwrap();
    let server = tokio::spawn(async move {
        let mut buffer = [0; 64];
        // Drop the first discovery datagram to exercise retransmission.
        let (n, _) = gateway.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..n], &[0, 0]);
        let (n, source) = gateway.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..n], &[0, 0]);
        gateway
            .send_to(&[0, 128, 0, 0, 0, 0, 0, 9, 203, 0, 113, 5], source)
            .await
            .unwrap();
        let (n, source) = gateway.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..n], &[0, 2, 0, 0, 11, 199, 15, 175, 0, 0, 28, 32]);
        gateway
            .send_to(
                &[0, 130, 0, 0, 0, 0, 0, 10, 11, 199, 15, 176, 0, 0, 14, 16],
                source,
            )
            .await
            .unwrap();
        let (n, source) = gateway.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..n], &[0, 2, 0, 0, 11, 199, 0, 0, 0, 0, 0, 0]);
        gateway
            .send_to(
                &[0, 130, 0, 0, 0, 0, 0, 11, 11, 199, 0, 0, 0, 0, 0, 0],
                source,
            )
            .await
            .unwrap();
    });
    let client = async {
        let gateway = network.discover().await.unwrap();
        assert_eq!(gateway.protocol(), Protocol::NatPmp);
        let mapping = network.add(&gateway, ports()).await.unwrap();
        assert_eq!(mapping.external, 4016);
        assert_eq!(mapping.lifetime, Some(3600));
        network.delete(&gateway, ports()).await.unwrap();
    };
    timeout(Duration::from_secs(5), client).await.unwrap();
    server.await.unwrap();
}

async fn simulated_upnp(version: u8) {
    let ssdp = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let http = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let location = format!("http://{}/description.xml", http.local_addr().unwrap());
    let mut s = settings();
    s.ssdp = address(ssdp.local_addr().unwrap());
    let network = Network::new(s).unwrap();
    let ssdp_task = tokio::spawn(async move {
        let mut buffer = [0; 8192];
        loop {
            let (n, source) = ssdp.recv_from(&mut buffer).await.unwrap();
            let request = std::str::from_utf8(&buffer[..n]).unwrap();
            if request.contains(&xml::target(version)) {
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nst: {}\r\nLOCATION: {location}\r\n\r\n",
                    xml::target(version)
                );
                ssdp.send_to(reply.as_bytes(), source).await.unwrap();
                break;
            }
        }
    });
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let http_task = tokio::spawn(async move {
        let count = if version == 1 { 4 } else { 5 };
        let mut adds = 0;
        for _ in 0..count {
            let (stream, _) = http.accept().await.unwrap();
            let request = read_request(&stream).await;
            if request.starts_with("GET ") {
                answer(&stream, 200, &description(version)).await;
            } else {
                assert!(request.contains(&format!("SOAPAction: \"{}#", xml::service(version))));
                let body = request.split_once("\r\n\r\n").unwrap().1;
                assert!(xml::document(body.as_bytes(), &settings()).is_ok());
                if request.contains("#GetNATRSIPStatus") {
                    answer(
                        &stream,
                        200,
                        &response(
                            version,
                            "GetNATRSIPStatus",
                            "<NewNATEnabled>1</NewNATEnabled>",
                        ),
                    )
                    .await;
                } else if request.contains("#DeletePortMapping") {
                    assert!(body.contains(if version == 1 {
                        "<NewExternalPort>4015</NewExternalPort>"
                    } else {
                        "<NewExternalPort>5000</NewExternalPort>"
                    }));
                    answer(&stream, 200, &response(version, "DeletePortMapping", "")).await;
                } else {
                    adds += 1;
                    if adds == 1 {
                        assert!(body.contains("<NewLeaseDuration>7200</NewLeaseDuration>"));
                        answer(&stream, 500, &fault(725)).await;
                    } else {
                        assert!(body.contains("<NewLeaseDuration>0</NewLeaseDuration>"));
                        answer(
                            &stream,
                            200,
                            &response(
                                version,
                                if version == 1 {
                                    "AddPortMapping"
                                } else {
                                    "AddAnyPortMapping"
                                },
                                if version == 1 {
                                    ""
                                } else {
                                    "<NewReservedPort>5000</NewReservedPort>"
                                },
                            ),
                        )
                        .await;
                    }
                }
            }
            recorded.lock().unwrap().push(request);
        }
    });
    timeout(Duration::from_secs(5), async {
        let gateway = network.discover().await.unwrap();
        assert_eq!(
            gateway.protocol(),
            if version == 1 {
                Protocol::Upnp1
            } else {
                Protocol::Upnp2
            }
        );
        let mapping = network.add(&gateway, ports()).await.unwrap();
        assert_eq!(mapping.lifetime, None);
        assert_eq!(mapping.external, if version == 1 { 4015 } else { 5000 });
        network
            .delete(
                &gateway,
                Ports {
                    external: mapping.external,
                    ..ports()
                },
            )
            .await
            .unwrap();
        ssdp_task.await.unwrap();
        http_task.await.unwrap();
    })
    .await
    .unwrap();
    assert_eq!(
        requests.lock().unwrap().len(),
        if version == 1 { 4 } else { 5 }
    );
}
#[tokio::test(flavor = "current_thread")]
async fn real_upnp_v1_permanent_lease_fallback_and_cleanup() {
    simulated_upnp(1).await;
}
#[tokio::test(flavor = "current_thread")]
async fn real_upnp_v2_nat_status_reserved_port_and_cleanup() {
    simulated_upnp(2).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_closes_http_and_timeouts_bound_discovery() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let url = http::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let s = settings();
    let server = async {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = read_request(&stream).await;
        loop {
            stream.readable().await.unwrap();
            match stream.try_read(&mut [0; 32]) {
                Ok(0) => break,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                other => panic!("expected closure: {other:?}"),
            }
        }
    };
    let client = async {
        assert!(
            timeout(
                Duration::from_millis(40),
                http::request(&url, Ipv4Addr::LOCALHOST, None, &s)
            )
            .await
            .is_err()
        );
    };
    timeout(Duration::from_secs(2), async {
        tokio::join!(server, client);
    })
    .await
    .unwrap();
    let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let mut s = settings();
    s.ssdp = address(silent.local_addr().unwrap());
    s.gateways = vec![s.ssdp];
    s.discovery_timeout = Duration::from_millis(40);
    assert!(matches!(
        Network::new(s).unwrap().discover().await,
        Err(Error::NoGateway)
    ));
}

async fn wait_info(client: &Client, predicate: impl Fn(&Info) -> bool) -> Info {
    timeout(Duration::from_secs(5), async {
        loop {
            let info = client.info();
            if predicate(&info) {
                return info;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test(flavor = "current_thread")]
async fn worker_samples_live_ports_after_discovery_and_cleans_owned_ports_after_config_change() {
    let native: serde_json::Value =
        serde_json::from_str(include_str!("../../tests/fixtures/erlang_upnp_worker.json")).unwrap();
    let events = native["events"].as_array().unwrap();
    assert_eq!(
        events.iter().find(|e| e["kind"] == "add").unwrap(),
        &serde_json::json!({"kind":"add","internal":6000,"external":7000,"lifetime":7200})
    );
    assert_eq!(
        events.iter().find(|e| e["kind"] == "delete").unwrap(),
        &serde_json::json!({"kind":"delete","internal":6001,"external":7001})
    );
    assert_eq!(native["mapping_lifetime"], MAPPING_LIFETIME);
    assert_eq!(
        native["refresh_milliseconds"].as_u64().unwrap(),
        RENEWAL_INTERVAL.as_millis() as u64
    );
    let gateway = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let unused = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let mut network = settings();
    network.ssdp = address(unused.local_addr().unwrap());
    network.gateways = vec![address(gateway.local_addr().unwrap())];
    let source = PortsSource::new(ports()).unwrap();
    let config = Config {
        network,
        system_gateways: false,
        ports: source.clone(),
        requests: 4,
        listener: None,
    };
    let worker = Worker::with_timing(config, Duration::from_secs(60), RENEWAL_INTERVAL).unwrap();
    let client = worker.client();
    let (seen, discovered) = tokio::sync::oneshot::channel();
    let (release, proceed) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut packet = [0; 64];
        let (n, from) = gateway.recv_from(&mut packet).await.unwrap();
        assert_eq!(&packet[..n], &[0, 0]);
        seen.send(()).unwrap();
        proceed.await.unwrap();
        gateway
            .send_to(&[0, 128, 0, 0, 0, 0, 0, 1, 203, 0, 113, 5], from)
            .await
            .unwrap();
        let (n, from) = gateway.recv_from(&mut packet).await.unwrap();
        assert_eq!(
            &packet[..n],
            &pmp::mapping_request(
                Ports {
                    internal: 6000,
                    external: 7000
                },
                7200
            )
        );
        gateway
            .send_to(
                &[0, 130, 0, 0, 0, 0, 0, 2, 23, 112, 27, 88, 0, 0, 28, 32],
                from,
            )
            .await
            .unwrap();
        // Rust deletes the retained mapping's original internal port, even
        // after configuration changes. It does not delete the new unowned port.
        let (n, from) = gateway.recv_from(&mut packet).await.unwrap();
        assert_eq!(
            &packet[..n],
            &pmp::mapping_request(
                Ports {
                    internal: 6000,
                    external: 0
                },
                0
            )
        );
        gateway
            .send_to(&[0, 130, 0, 0, 0, 0, 0, 4, 23, 112, 0, 0, 0, 0, 0, 0], from)
            .await
            .unwrap();
    });
    assert_eq!(client.refresh().unwrap().await.unwrap(), 1);
    timeout(Duration::from_secs(2), discovered)
        .await
        .unwrap()
        .unwrap();
    source
        .set(Ports {
            internal: 6000,
            external: 7000,
        })
        .unwrap();
    release.send(()).unwrap();
    let info = wait_info(&client, |i| i.completed == 1).await;
    assert_eq!(info.mapping.unwrap().external, 7000);
    source
        .set(Ports {
            internal: 6001,
            external: 7001,
        })
        .unwrap();
    client.prepare_stop().unwrap().await.unwrap();
    assert!(matches!(client.refresh(), Err(Error::Closed)));
    assert!(client.info().prepared && client.info().running);
    worker.shutdown().unwrap();
    assert!(client.info().joined && !client.info().running);
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn worker_renewal_retires_pending_attempt_and_cleanup_stops_future_renewal() {
    let native: serde_json::Value =
        serde_json::from_str(include_str!("../../tests/fixtures/erlang_upnp_worker.json")).unwrap();
    assert_eq!(native["refresh_after_prepare"], true);
    assert_eq!(native["terminate_leaves_attempt_alive"], true);
    let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let mut network = settings();
    network.ssdp = address(silent.local_addr().unwrap());
    network.discovery_timeout = Duration::from_millis(80);
    let config = Config {
        network,
        system_gateways: false,
        requests: 2,
        ..Config::default()
    };
    let worker =
        Worker::with_timing(config, Duration::from_millis(1), Duration::from_millis(20)).unwrap();
    let client = worker.client();
    let before = wait_info(&client, |i| i.attempts >= 3).await;
    assert!(before.cancelled >= 2);
    let cleanup = client.prepare_stop().unwrap();
    drop(cleanup);
    let after = wait_info(&client, |i| i.prepared).await;
    assert_eq!(after.last_error, None);
    assert!(after.running && !after.active);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(client.info().attempts, after.attempts);
    worker.shutdown().unwrap();
    assert!(client.info().joined);
}

#[tokio::test(flavor = "current_thread")]
async fn worker_reserves_cleanup_capacity_and_bounds_accepted_refreshes() {
    let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let mut network = settings();
    network.ssdp = address(silent.local_addr().unwrap());
    network.discovery_timeout = Duration::from_millis(30);
    let config = Config {
        network,
        system_gateways: false,
        requests: 2,
        ..Config::default()
    };
    let worker = Worker::with_timing(config, Duration::from_secs(60), RENEWAL_INTERVAL).unwrap();
    let client = worker.client();
    let (entered, release) = client.pause();
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    let first = client.refresh().unwrap();
    let second = client.refresh().unwrap();
    assert!(matches!(client.refresh(), Err(Error::Busy)));
    assert_eq!(client.info().pending, 2);
    let cleanup = client.prepare_stop().unwrap();
    drop(first);
    release.send(()).unwrap();
    assert_eq!(cleanup.await, Ok(()));
    assert_eq!(second.await, Err(Error::Closed));
    assert_eq!(client.info().attempts, 0);
    assert_eq!(client.info().pending, 0);
    worker.shutdown().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn worker_failure_fences_old_client_and_shutdown_cancels_cleanup() {
    let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let mut network = settings();
    network.ssdp = address(silent.local_addr().unwrap());
    network.discovery_timeout = Duration::from_secs(10);
    let config = Config {
        network,
        system_gateways: false,
        ..Config::default()
    };
    let worker =
        Worker::with_timing(config.clone(), Duration::from_secs(60), RENEWAL_INTERVAL).unwrap();
    let old = worker.client();
    old.fail();
    wait_info(&old, |i| !i.running).await;
    assert_eq!(worker.shutdown(), Err(Error::Panicked));
    assert!(matches!(old.refresh(), Err(Error::Closed)));
    let worker = Worker::with_timing(config, Duration::from_secs(60), RENEWAL_INTERVAL).unwrap();
    let client = worker.client();
    let ticket = client.prepare_stop().unwrap();
    worker.request_shutdown();
    worker.shutdown().unwrap();
    assert!(matches!(ticket.await, Ok(()) | Err(Error::Closed)));
    assert!(client.info().joined && old.info().joined);
}

#[tokio::test(flavor = "current_thread")]
async fn withdrawing_listener_cancels_discovery_and_prevents_manual_or_timed_restarts() {
    let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let mut network = settings();
    network.ssdp = address(silent.local_addr().unwrap());
    network.discovery_timeout = Duration::from_secs(10);
    let gate = ListenerSource::default();
    let lease = gate.acquire().unwrap();
    let worker = Worker::with_timing(
        Config {
            network,
            system_gateways: false,
            listener: Some(gate),
            ..Default::default()
        },
        Duration::from_millis(1),
        Duration::from_millis(20),
    )
    .unwrap();
    let client = worker.client();
    lease.publish(Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6000)), 6000);
    wait_info(&client, |i| i.active).await;
    // Observe a real discovery datagram, rather than only a scheduled task.
    let mut bytes = [0; 4096];
    timeout(Duration::from_secs(2), silent.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    lease.clear();
    let stopped = wait_info(&client, |i| !i.active && i.cancelled > 0).await;
    assert_eq!(client.refresh().unwrap().await, Err(Error::NotReady));
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(client.info().attempts, stopped.attempts);
    assert_eq!(client.info().retained_mappings, 0);
    assert_eq!(client.prepare_stop().unwrap().await, Ok(()));
    worker.shutdown().unwrap();
}
