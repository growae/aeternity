use super::*;
use ae_p2p::{
    message::Message,
    registry::{Command, Outcome, Registry},
};
use ae_types::{Hash256, Unsigned};
use std::future::Future;
use tokio::{net::TcpListener, task::JoinHandle};

fn identity(n: u8) -> Arc<noise::Identity> {
    Arc::new(noise::Identity::from_secret([n; 32]))
}
fn network() -> Arc<noise::Network> {
    Arc::new(noise::Network::new(1, Hash256([0; 32]), b"ae_dev").unwrap())
}
fn tokens(n: usize) -> Vec<Token> {
    let mut registry = Registry::new(identity(100).public_key(), Default::default()).unwrap();
    (0..n)
        .map(|_| {
            let plan = registry
                .prepare(
                    0,
                    Command::ReserveInbound {
                        address: Ipv4Addr::LOCALHOST,
                    },
                )
                .unwrap();
            let Outcome::Reserved(token) = registry.commit(plan).unwrap().result else {
                panic!()
            };
            token
        })
        .collect()
}
fn sockets(settings: Settings) -> Sockets {
    Sockets::new(identity(1), network(), settings).unwrap()
}
fn peer(id: PeerPublicKey, listener: &TcpListener) -> Peer {
    Peer {
        id,
        // A deliberate unusable hostname proves dials use the resolved socket.
        host: b"not-resolved-by-the-transport.invalid".to_vec(),
        port: listener.local_addr().unwrap().port(),
        address: Ipv4Addr::LOCALHOST,
        source: Ipv4Addr::new(10, 2, 3, 4),
        trusted: false,
    }
}
async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("bounded loopback test")
}
fn ping(port: Unsigned, version: u16) -> Message {
    let mut fields = BTreeMap::from([
        ("port".into(), Value::Integer(port)),
        ("share".into(), Value::Integer(0.into())),
        ("genesis_hash".into(), Value::Binary(vec![0; 32])),
        ("difficulty".into(), Value::Integer(1.into())),
        ("best_hash".into(), Value::Binary(vec![1; 32])),
        ("sync_allowed".into(), Value::Boolean(true)),
        ("peers".into(), Value::List(vec![])),
    ]);
    if version == 2 {
        fields.insert("versions".into(), Value::List(vec![]));
        fields.insert("height".into(), Value::Integer(1.into()));
        fields.insert("capabilities".into(), Value::Binary(vec![0xc0]));
    }
    Message::new(Kind::Ping, version, fields, Default::default()).unwrap()
}
async fn incoming(
    sockets: &mut Sockets,
    token: Token,
) -> (net::Client, net::Events, JoinHandle<net::Exit>, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let ephemeral = remote.local_addr().unwrap().port();
    let (local, _) = listener.accept().await.unwrap();
    sockets.inbound(token, local).unwrap();
    let identity = identity(2);
    let network = network();
    let established = net::establish(
        remote,
        &identity,
        &network,
        noise::Role::Initiator(sockets.identity.public_key()),
        Default::default(),
    );
    let (client, events, driver) = within(async {
        tokio::select! {
            _ = sockets.next() => panic!("inbound must wait for first ping"),
            result = established => result.unwrap(),
        }
    })
    .await;
    (client, events, tokio::spawn(driver.run()), ephemeral)
}
async fn eof(socket: &TcpStream) {
    within(async {
        loop {
            socket.readable().await.unwrap();
            match socket.try_read(&mut [0; 1]) {
                Ok(0) => return,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                value => panic!("expected EOF without protocol bytes, got {value:?}"),
            }
        }
    })
    .await;
}

#[test]
fn configuration_and_advertised_ports_are_checked_without_truncation() {
    for settings in [
        Settings {
            connections: 0,
            ..Default::default()
        },
        Settings {
            connections: 65_537,
            ..Default::default()
        },
        Settings {
            pre_admission_events: 0,
            ..Default::default()
        },
        Settings {
            pre_admission_events: 65_537,
            ..Default::default()
        },
        Settings {
            connect_timeout: Duration::ZERO,
            ..Default::default()
        },
        Settings {
            first_ping_timeout: Duration::MAX,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            Sockets::new(identity(1), network(), settings),
            Err(Error::Configuration)
        ));
    }
    let settings = Settings {
        runtime: net::Config {
            minimum_ping: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(matches!(
        Sockets::new(identity(1), network(), settings),
        Err(Error::Runtime(_))
    ));
    for port in [0u64, 1, 3015, 65_535] {
        assert_eq!(advertised_port(&ping(port.into(), 1)), Ok(port as u16));
    }
    for port in [
        65_536u64.into(),
        u64::MAX.into(),
        Unsigned::from_field_bytes(&[1; 9]).unwrap(),
    ] {
        assert_eq!(advertised_port(&ping(port, 1)), Err(Error::InvalidPort));
    }
}

#[tokio::test]
async fn probes_are_tcp_only_and_reservations_cover_queued_work_and_results() {
    let mut sockets = sockets(Settings {
        connections: 1,
        ..Default::default()
    });
    let tokens = tokens(3);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = peer(identity(2).public_key(), &listener);
    sockets.dial(tokens[0], peer.clone(), true).unwrap();
    assert_eq!(sockets.occupied(), 1);
    assert_eq!(
        sockets.dial(tokens[0], peer.clone(), true),
        Err(Error::Duplicate)
    );
    assert_eq!(
        sockets.dial(tokens[1], peer.clone(), true),
        Err(Error::Busy)
    );
    // Merely registering a job does not start a detached connect task.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    let (event, accepted) = within(async { tokio::join!(sockets.next(), listener.accept()) }).await;
    assert!(
        matches!(event, Some(Event::Probe { token, peer: id, result: Ok(()) }) if token == tokens[0] && id == peer.id)
    );
    eof(&accepted.unwrap().0).await;
    assert_eq!(sockets.occupied(), 0);
    drop(listener);
    sockets.dial(tokens[1], peer.clone(), true).unwrap();
    assert!(
        matches!(within(sockets.next()).await, Some(Event::Probe { token, result: Err(Error::Io(io::ErrorKind::ConnectionRefused)), .. }) if token == tokens[1])
    );
    sockets.dial(tokens[2], peer, true).unwrap();
    assert!(sockets.cancel(tokens[2]));
    assert!(!sockets.cancel(tokens[2]));
    sockets.stop();
    assert_eq!(sockets.occupied(), 0);
    assert!(sockets.next().await.is_none());
}

#[test]
fn original_admission_callbacks_match_dialable_ports_and_expose_oversized_port_boundary() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_peer_transport.json"
    ))
    .unwrap();
    assert_eq!(fixture["full_node_started"], false);
    assert_eq!(fixture["callback_only"], true);
    assert_eq!(fixture["sockets_opened"], 0);
    let rows = fixture["admission"].as_array().unwrap();
    assert_eq!(rows.len(), 8);
    let mut supported = 0;
    for row in rows {
        let port = Unsigned::from_decimal(row["port"].as_str().unwrap()).unwrap();
        let Ok(port) = advertised_port(&ping(port, 1)) else {
            assert!(row["port"] == "65536" || row["port"] == "1208925819614629174706176");
            assert_eq!(row["accepted"], true);
            continue;
        };
        supported += 1;
        let info = Info {
            id: PeerPublicKey::try_from(
                hex::decode(row["peer"].as_str().unwrap())
                    .unwrap()
                    .as_slice(),
            )
            .unwrap(),
            host: hex::decode(row["host"].as_str().unwrap()).unwrap(),
            port,
        };
        let mut config = ae_p2p::registry::Config::default();
        if row["kind"] == "temporary" {
            config.maximum_inbound = 0;
        }
        let mut registry = Registry::new(identity(100).public_key(), config).unwrap();
        let mut apply = |command| {
            let plan = registry.prepare(0, command).unwrap();
            registry.commit(plan).unwrap().result
        };
        if row["stop_cast"] == true {
            apply(Command::Block(info.clone()));
        }
        let Outcome::Reserved(token) = apply(Command::ReserveInbound {
            address: Ipv4Addr::LOCALHOST,
        }) else {
            panic!()
        };
        let outcome = apply(Command::Accept {
            token,
            peer: info.clone(),
        });
        if row["accepted"] == true {
            let expected = if row["kind"] == "temporary" {
                ae_p2p::registry::Kind::Temporary
            } else {
                ae_p2p::registry::Kind::Permanent
            };
            assert_eq!(outcome, Outcome::Accepted(expected));
            assert_eq!(registry.connection(info.id).unwrap().peer.port, port);
            assert_eq!(registry.connection(info.id).unwrap().peer.host, info.host);
        } else {
            assert_eq!(
                outcome,
                Outcome::Rejected(ae_p2p::registry::Rejection::Blocked)
            );
        }
        assert_eq!(registry.counts().pending_inbound, 0);
        assert_eq!(row["timer_removed"], true);
        assert_eq!(row["repeated_ping_keeps_original_port"], true);
    }
    assert_eq!(supported, 6);
    assert_eq!(fixture["missing_port_raises"], true);
    assert!(Message::new(Kind::Ping, 1, BTreeMap::new(), Default::default()).is_err());
    for (row, expected) in fixture["probes"]
        .as_array()
        .unwrap()
        .iter()
        .zip(["alive", "dead"])
    {
        assert_eq!(row["status"], expected);
        assert_eq!(row["noise_calls"], 0);
        assert_eq!(row["stopped"], true);
    }
    println!(
        "Original transport callbacks: 6 supported admission rows, 2 oversized-port boundaries, 2 TCP-only probe outcomes"
    );
}

#[tokio::test]
async fn inbound_uses_authenticated_identity_and_first_ping_listening_port() {
    for version in [1, 2] {
        let mut sockets = sockets(Default::default());
        let token = tokens(1)[0];
        let (remote, _events, task, ephemeral) = incoming(&mut sockets, token).await;
        // Noise alone is not admission, and cancelling observation is harmless.
        assert!(
            tokio::time::timeout(Duration::from_millis(20), sockets.next())
                .await
                .is_err()
        );
        assert_eq!(sockets.occupied(), 1);
        let port = if ephemeral == 3015 { 3016 } else { 3015 };
        let message = ping(port.into(), version);
        let ticket = remote.send(&message).unwrap();
        let (event, sent) = within(async { tokio::join!(sockets.next(), ticket) }).await;
        assert!(matches!(sent, Ok(net::Reply::Sent)));
        let Some(Event::Ready(ready)) = event else {
            panic!()
        };
        assert!(ready.inbound);
        assert_eq!(ready.token, token);
        assert_eq!(ready.peer.id, identity(2).public_key());
        assert_eq!(ready.peer.host, b"127.0.0.1");
        assert_eq!(ready.peer.port, port as u16);
        assert_ne!(ready.peer.port, ephemeral);
        assert_eq!(ready.address, Ipv4Addr::LOCALHOST);
        let monitor = ready.connection.client().monitor();
        assert_eq!(sockets.occupied(), 1);
        drop(ready);
        let Some(Event::Closed {
            token: actual,
            peer,
            exit,
        }) = within(sockets.next()).await
        else {
            panic!()
        };
        assert_eq!(actual, token);
        assert_eq!(peer, identity(2).public_key());
        assert_eq!(exit, monitor.wait().await);
        assert_eq!(exit.reason, net::Error::Cancelled);
        assert_eq!(sockets.occupied(), 0);
        within(task).await.unwrap();
        sockets.stop();
        assert!(sockets.next().await.is_none());
    }
}

#[tokio::test]
async fn first_ping_deadline_limit_and_large_port_drop_the_actual_driver() {
    for case in 0..3 {
        let settings = Settings {
            first_ping_timeout: Duration::from_millis(100),
            pre_admission_events: if case == 1 { 1 } else { 64 },
            ..Default::default()
        };
        let mut sockets = sockets(settings);
        let token = tokens(1)[0];
        let (remote, _events, task, _) = incoming(&mut sockets, token).await;
        let ticket = match case {
            1 => Some(
                remote
                    .send(
                        &Message::new(Kind::GetNodeInfo, 1, BTreeMap::new(), Default::default())
                            .unwrap(),
                    )
                    .unwrap(),
            ),
            2 => Some(remote.send(&ping(65_536u64.into(), 1)).unwrap()),
            _ => None,
        };
        let (event, _) = within(async {
            tokio::join!(sockets.next(), async {
                if let Some(ticket) = ticket {
                    let _ = ticket.await;
                }
            })
        })
        .await;
        let Some(Event::Failed {
            token: actual,
            peer: None,
            error,
        }) = event
        else {
            panic!()
        };
        assert_eq!(actual, token);
        assert_eq!(
            error,
            match case {
                0 => Error::Timeout(Stage::FirstPing),
                1 => Error::AdmissionLimit,
                _ => Error::InvalidPort,
            }
        );
        assert_eq!(sockets.occupied(), 0);
        within(task).await.unwrap();
    }
}

#[tokio::test]
async fn cancelling_or_dropping_pending_admission_closes_socket_without_detached_work() {
    for stopped in [false, true] {
        let mut sockets = sockets(Default::default());
        let token = tokens(1)[0];
        let (remote, _events, task, _) = incoming(&mut sockets, token).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), sockets.next())
                .await
                .is_err()
        );
        if stopped {
            drop(sockets);
        } else {
            assert!(sockets.cancel(token));
            assert_eq!(sockets.occupied(), 0);
            sockets.stop();
            assert!(sockets.next().await.is_none());
        }
        let exit = within(task).await.unwrap();
        assert_eq!(remote.monitor().wait().await, exit);
        assert!(matches!(
            exit.reason,
            net::Error::Io(io::ErrorKind::UnexpectedEof)
        ));
    }
}

#[tokio::test]
async fn outbound_authentication_uses_expected_key_and_network_and_times_out() {
    for case in 0..4 {
        let mut sockets = sockets(Settings {
            runtime: net::Config {
                handshake_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            ..Default::default()
        });
        let token = tokens(1)[0];
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let actual = identity(2);
        let expected = if case == 1 {
            identity(3).public_key()
        } else {
            actual.public_key()
        };
        sockets
            .dial(token, peer(expected, &listener), false)
            .unwrap();
        let remote = async {
            let (socket, _) = listener.accept().await.unwrap();
            if case == 3 {
                // Retain a silent TCP endpoint until the bounded handshake fails.
                tokio::time::sleep(Duration::from_millis(200)).await;
                drop(socket);
                return None;
            }
            let network = if case == 2 {
                Arc::new(noise::Network::new(1, Hash256([1; 32]), b"ae_dev").unwrap())
            } else {
                network()
            };
            Some(
                net::establish(
                    socket,
                    &actual,
                    &network,
                    noise::Role::Responder,
                    Default::default(),
                )
                .await,
            )
        };
        let (event, remote) = within(async { tokio::join!(sockets.next(), remote) }).await;
        if case == 0 {
            let Some(Event::Ready(ready)) = event else {
                panic!()
            };
            let (client, _events, driver) = remote.unwrap().unwrap();
            let task = tokio::spawn(driver.run());
            assert!(!ready.inbound);
            assert_eq!(ready.peer.id, expected);
            assert_eq!(ready.connection.client().peer(), expected);
            sockets.stop();
            assert_eq!(sockets.occupied(), 1);
            assert!(ready.connection.client().is_closed());
            assert!(ready.connection.client().monitor().exit().is_none());
            drop(ready);
            assert!(
                matches!(within(sockets.next()).await, Some(Event::Closed { token: t, .. }) if t == token)
            );
            assert!(sockets.next().await.is_none());
            within(task).await.unwrap();
            assert!(client.monitor().exit().is_some());
        } else {
            assert!(
                matches!(event, Some(Event::Failed { token: t, peer: Some(p), error: Error::Runtime(_) }) if t == token && p == expected)
            );
            if let Some(result) = remote {
                assert!(result.is_err());
            }
            assert_eq!(sockets.occupied(), 0);
        }
    }
}
