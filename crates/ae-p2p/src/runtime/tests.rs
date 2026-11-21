use super::*;
use crate::pool;
use ae_types::Hash256;
use message::{Kind, Message, Value};
use std::collections::BTreeMap;
use tokio::net::TcpListener;

fn init() -> Message {
    pool::Request::Init
        .into_message(message::Limits::default())
        .unwrap()
}

#[tokio::test]
async fn terminal_monitor_distinguishes_shutdown_intent_and_retains_one_completed_generation() {
    let (client, _events, driver, raw) = raw_pair(Config::default()).await;
    let monitor = client.monitor();
    assert_eq!(monitor.peer(), client.peer());
    assert_eq!(monitor.generation(), client.generation());
    assert!(monitor.exit().is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(1), monitor.wait())
            .await
            .is_err()
    );
    assert!(!client.is_closed());
    let mut queued = client.request(&init(), None).unwrap();
    client.shutdown();
    assert!(client.is_closed());
    assert!(monitor.exit().is_none());
    let second = monitor.clone();
    let task = tokio::spawn(driver.run());
    let (a, b) = within(async { tokio::join!(monitor.wait(), second.wait()) }).await;
    assert_eq!(a, b);
    assert_eq!(a.reason, Error::Shutdown);
    assert_eq!(&a.generation, client.generation());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        std::future::Future::poll(std::pin::Pin::new(&mut queued), &mut cx),
        std::task::Poll::Ready(Err(Error::Closed))
    ));
    assert_eq!(within(task).await.unwrap(), a);
    assert_eq!(monitor.exit(), Some(a.clone()));
    assert_eq!(within(client.monitor().wait()).await, a);
    let mut byte = [0];
    assert_eq!(
        within(wire::read(&raw.socket, &mut byte))
            .await
            .unwrap_err(),
        Error::Io(io::ErrorKind::UnexpectedEof)
    );
}

#[tokio::test]
async fn terminal_monitor_covers_unstarted_cancelled_and_unwinding_driver_owners() {
    for mode in ["driver", "future", "abort", "unwind"] {
        let (client, mut events, driver, mut raw) = raw_pair(Config::default()).await;
        let monitor = client.monitor();
        let queued = client.request(&init(), None).unwrap();
        match mode {
            "driver" => drop(driver),
            "future" => drop(driver.run()),
            "abort" => {
                let task = tokio::spawn(driver.run());
                raw.send(&init()).await;
                assert!(matches!(
                    within(events.recv()).await,
                    Some(Event::Message(_))
                ));
                task.abort();
                assert!(within(task).await.unwrap_err().is_cancelled());
            }
            "unwind" => {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                        let _owned = driver;
                        panic!("injected driver owner panic");
                    }))
                    .is_err()
                );
            }
            _ => unreachable!(),
        }
        let exit = within(monitor.wait()).await;
        assert_eq!(exit.reason, Error::Cancelled, "{mode}");
        assert_eq!(&exit.generation, client.generation());
        assert!(client.is_closed());
        assert!(matches!(within(queued).await, Err(Error::Closed)));
    }
}

#[tokio::test]
async fn terminal_observation_does_not_keep_a_connection_alive_or_depend_on_event_capacity() {
    let (client, _events, driver, _raw) = raw_pair(Config::default()).await;
    let monitor = client.monitor();
    drop(client);
    let task = tokio::spawn(driver.run());
    assert_eq!(within(monitor.wait()).await.reason, Error::Closed);
    within(task).await.unwrap();

    let config = Config {
        events: 1,
        ..Default::default()
    };
    let (client, mut events, driver, mut raw) = raw_pair(config).await;
    let monitor = client.monitor();
    let task = tokio::spawn(driver.run());
    raw.send(&init()).await;
    raw.send(&init()).await;
    assert_eq!(within(monitor.wait()).await.reason, Error::Backpressure);
    assert_eq!(within(task).await.unwrap(), monitor.exit().unwrap());
    // Queued external messages retain their own lifetime and old generation.
    let Event::Message(held) = events.recv().await.unwrap() else {
        panic!()
    };
    assert_eq!(&held.generation, monitor.generation());
    assert!(events.recv().await.is_none());
    assert_eq!(within(monitor.wait()).await.reason, Error::Backpressure);
}
fn reply() -> Message {
    pool::Reply::Init
        .into_response(message::Limits::default())
        .unwrap()
}
fn transactions(size: usize) -> Message {
    Message::new(
        Kind::Transactions,
        1,
        BTreeMap::from([(
            "txs".into(),
            Value::List(vec![Value::Binary(vec![42; size])]),
        )]),
        message::Limits::default(),
    )
    .unwrap()
}
fn node_info() -> Message {
    Message::new(
        Kind::GetNodeInfo,
        1,
        BTreeMap::new(),
        message::Limits::default(),
    )
    .unwrap()
}
async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("bounded loopback test")
}
async fn sockets() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (second, _) = listener.accept().await.unwrap();
    (first, second)
}
fn network() -> noise::Network {
    noise::Network::new(1, Hash256([0; 32]), b"ae_dev").unwrap()
}
async fn pair(config: Config) -> ((Client, Events, Driver), (Client, Events, Driver)) {
    let (a, b) = sockets().await;
    let ka = noise::Identity::from_secret([1; 32]);
    let kb = noise::Identity::from_secret([2; 32]);
    let network = network();
    let (a, b) = tokio::join!(
        establish(
            a,
            &ka,
            &network,
            noise::Role::Initiator(kb.public_key()),
            config
        ),
        establish(b, &kb, &network, noise::Role::Responder, config)
    );
    (a.unwrap(), b.unwrap())
}
struct Raw {
    socket: TcpStream,
    crypto: noise::Transport,
}
impl Raw {
    fn record(&mut self, packet: &[u8]) -> Vec<u8> {
        noise::record::encode(&self.crypto.seal(packet).unwrap()).unwrap()
    }
    async fn send(&mut self, message: &Message) {
        let record = self.record(&message.encode_packet(message::Limits::default()).unwrap());
        wire::write(&self.socket, &record).await.unwrap();
    }
    async fn receive(&mut self) -> Message {
        let mut prefix = [0; 2];
        wire::read(&self.socket, &mut prefix).await.unwrap();
        let mut body = vec![0; usize::from(u16::from_be_bytes(prefix))];
        wire::read(&self.socket, &mut body).await.unwrap();
        Message::decode_packet(
            &self.crypto.open(&body).unwrap(),
            message::Limits::default(),
        )
        .unwrap()
    }
}
async fn raw_pair(config: Config) -> (Client, Events, Driver, Raw) {
    let (a, b) = sockets().await;
    raw_streams(a, b, config).await
}
async fn raw_streams(a: TcpStream, b: TcpStream, config: Config) -> (Client, Events, Driver, Raw) {
    let ka = noise::Identity::from_secret([1; 32]);
    let kb = noise::Identity::from_secret([2; 32]);
    let network = network();
    let (a, b_crypto) = tokio::join!(
        establish(
            a,
            &ka,
            &network,
            noise::Role::Initiator(kb.public_key()),
            config
        ),
        wire::handshake(
            &b,
            &kb,
            &network,
            noise::Role::Responder,
            Duration::from_secs(2)
        )
    );
    let (client, events, driver) = a.unwrap();
    (
        client,
        events,
        driver,
        Raw {
            socket: b,
            crypto: b_crypto.unwrap(),
        },
    )
}

#[tokio::test]
async fn blocked_writes_do_not_block_reads_or_deadlines() {
    let listener = tokio::net::TcpSocket::new_v4().unwrap();
    listener.set_recv_buffer_size(4096).unwrap();
    listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = listener.listen(8).unwrap();
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_send_buffer_size(4096).unwrap();
    let a = socket
        .connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (b, _) = listener.accept().await.unwrap();
    let config = Config {
        write_timeout: Duration::from_millis(500),
        ..Config::default()
    };
    let (client, mut events, driver, mut raw) = raw_streams(a, b, config).await;
    let task = tokio::spawn(driver.run());
    let sending = client.send(&transactions(2_000_000)).unwrap();
    raw.send(&init()).await;
    assert_eq!(message(&mut events).await.message(), &init());
    assert_eq!(within(task).await.unwrap().reason, Error::Timeout);
    assert!(matches!(within(sending).await, Err(Error::Closed)));
    assert!(client.is_closed());
}

#[tokio::test]
async fn queued_expiry_event_consumer_loss_and_unstarted_driver_release_tickets() {
    let config = Config {
        write_timeout: Duration::from_millis(10),
        ..Config::default()
    };
    let (client, events, driver, _raw) = raw_pair(config).await;
    let queued = client.send(&init()).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let task = tokio::spawn(driver.run());
    assert!(matches!(within(queued).await, Err(Error::Timeout)));
    assert!(!client.is_closed());
    drop(events);
    assert_eq!(within(task).await.unwrap().reason, Error::Closed);
    let (client, _events, driver, _raw) = raw_pair(Config::default()).await;
    let queued = client.request(&init(), None).unwrap();
    drop(driver);
    assert!(matches!(within(queued).await, Err(Error::Closed)));
    assert!(client.is_closed());
}

fn ping(version: u16) -> Message {
    let mut fields = BTreeMap::from([
        ("port".into(), Value::Integer(3015.into())),
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
    Message::new(Kind::Ping, version, fields, message::Limits::default()).unwrap()
}

#[tokio::test]
async fn ping_probe_fallback_is_driven_by_runtime_timer_without_closing_connection() {
    let (client, mut events, driver, mut raw) = raw_pair(Config::default()).await;
    let task = tokio::spawn(driver.run());
    let first = client.request(&ping(1), None).unwrap();
    assert_eq!(within(raw.receive()).await, ping(1));
    raw.send(&Message::response_ok(&ping(1), message::Limits::default()).unwrap())
        .await;
    assert!(matches!(within(first).await, Ok(Reply::Response(_))));
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping {
            action: requests::PingAction::ProbeV2,
            ..
        })
    ));
    let probe = client.probe(&ping(2)).unwrap();
    assert_eq!(within(raw.receive()).await, ping(2));
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(matches!(
        within(probe).await,
        Err(Error::RequestFailed(requests::Failure::Timeout))
    ));
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping {
            action: requests::PingAction::Selected(1),
            ..
        })
    ));
    tokio::time::resume();
    assert_eq!(client.selected_ping_version(), Some(1));
    assert_eq!(client.ping_version(), 1);
    assert!(!client.is_closed());
    let next = client.request(&init(), None).unwrap();
    assert_eq!(within(raw.receive()).await, init());
    raw.send(&reply()).await;
    assert!(matches!(within(next).await, Ok(Reply::Response(_))));
    client.shutdown();
    assert_eq!(within(task).await.unwrap().reason, Error::Shutdown);
}

#[tokio::test]
async fn configured_v2_accepts_incoming_v1_without_changing_initial_outbound_version() {
    let (client, mut events, driver, mut raw) = raw_pair(Config {
        minimum_ping: 2,
        ..Default::default()
    })
    .await;
    let task = tokio::spawn(driver.run());
    raw.send(&ping(1)).await;
    let received = message(&mut events).await;
    assert_eq!(received.message(), &ping(1));
    assert_eq!(received.ping.unwrap().action, requests::PingAction::None);
    assert_eq!(client.minimum_ping_version(), 2);
    assert_eq!(client.selected_ping_version(), None);
    assert_eq!(client.ping_version(), 2);
    let response = Message::response_ok(received.message(), Default::default()).unwrap();
    within(client.send(&response).unwrap()).await.unwrap();
    assert_eq!(within(raw.receive()).await, response);
    raw.send(&init()).await;
    assert_eq!(message(&mut events).await.message(), &init());
    client.shutdown();
    assert_eq!(within(task).await.unwrap().reason, Error::Shutdown);
}

#[tokio::test]
async fn configured_v2_timeout_sends_and_completes_v1_fallback_on_same_encrypted_stream() {
    let (client, mut events, driver, mut raw) = raw_pair(Config {
        minimum_ping: 2,
        ..Default::default()
    })
    .await;
    let monitor = client.monitor();
    let task = tokio::spawn(driver.run());
    let first = client.request(&ping(client.ping_version()), None).unwrap();
    assert_eq!(within(raw.receive()).await, ping(2));
    tokio::time::pause();
    tokio::time::advance(requests::DEFAULT_TIMEOUT + Duration::from_secs(1)).await;
    assert!(matches!(
        within(first).await,
        Err(Error::RequestFailed(requests::Failure::Timeout))
    ));
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping {
            action: requests::PingAction::Selected(1),
            ..
        })
    ));
    tokio::time::resume();
    assert!(!client.is_closed());
    assert!(monitor.exit().is_none());
    assert_eq!(client.minimum_ping_version(), 2);
    assert_eq!(client.ping_version(), 1);
    let retry = client.request(&ping(client.ping_version()), None).unwrap();
    assert_eq!(within(raw.receive()).await, ping(1));
    let response = Message::response_ok(&ping(1), Default::default()).unwrap();
    raw.send(&response).await;
    let Reply::Response(received) = within(retry).await.unwrap() else {
        panic!()
    };
    assert_eq!(received.message(), &response);
    assert_eq!(&received.generation, client.generation());
    assert_eq!(received.ping.unwrap().previous, Some(1));
    assert_eq!(client.ping_version(), 1);
    // A later v2 exchange can renegotiate; fallback is not a new minimum.
    raw.send(&ping(2)).await;
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping {
            action: requests::PingAction::Selected(2),
            ..
        })
    ));
    assert_eq!(message(&mut events).await.message(), &ping(2));
    assert_eq!(client.ping_version(), 2);
    client.shutdown();
    assert_eq!(within(task).await.unwrap().reason, Error::Shutdown);
}
async fn message(events: &mut Events) -> Received {
    match within(events.recv()).await.unwrap() {
        Event::Message(message) => message,
        _ => panic!("expected message"),
    }
}

#[tokio::test]
async fn bidirectional_requests_and_fragmented_messages_use_owned_async_tcp() {
    let ((a, mut ae, ad), (b, mut be, bd)) = pair(Config::default()).await;
    assert_eq!(a.peer(), noise::Identity::from_secret([2; 32]).public_key());
    assert_ne!(a.generation(), b.generation());
    let at = tokio::spawn(ad.run());
    let bt = tokio::spawn(bd.run());
    let ar = a.request(&init(), None).unwrap();
    let br = b.request(&init(), None).unwrap();
    assert_eq!(message(&mut be).await.message(), &init());
    assert_eq!(message(&mut ae).await.message(), &init());
    let asend = a.send(&reply()).unwrap();
    let bsend = b.send(&reply()).unwrap();
    assert!(matches!(within(asend).await, Ok(Reply::Sent)));
    assert!(matches!(within(bsend).await, Ok(Reply::Sent)));
    for (received, generation) in [
        (within(ar).await.unwrap(), a.generation()),
        (within(br).await.unwrap(), b.generation()),
    ] {
        let Reply::Response(received) = received else {
            panic!()
        };
        assert_eq!(&received.generation, generation);
        assert_eq!(received.message(), &reply());
    }
    let large = transactions(200_000);
    let sent = a.send(&large).unwrap();
    assert_eq!(message(&mut be).await.message(), &large);
    assert!(matches!(within(sent).await, Ok(Reply::Sent)));
    a.shutdown();
    b.shutdown();
    assert_eq!(within(at).await.unwrap().reason, Error::Shutdown);
    assert_eq!(within(bt).await.unwrap().reason, Error::Shutdown);
    assert!(a.is_closed() && b.is_closed());
}

#[tokio::test]
async fn partial_records_survive_other_selected_work_and_coalesced_input() {
    let (client, mut events, driver, mut raw) = raw_pair(Config::default()).await;
    let task = tokio::spawn(driver.run());
    let first = raw.record(&init().encode_packet(message::Limits::default()).unwrap());
    wire::write(&raw.socket, &first[..1]).await.unwrap();
    let ticket = client.request(&init(), None).unwrap();
    assert_eq!(within(raw.receive()).await, init());
    wire::write(&raw.socket, &first[1..3]).await.unwrap();
    let other = client.send(&transactions(2)).unwrap();
    assert_eq!(within(raw.receive()).await, transactions(2));
    assert!(matches!(within(other).await, Ok(Reply::Sent)));
    let mut rest = first[3..].to_vec();
    rest.extend(raw.record(&reply().encode_packet(message::Limits::default()).unwrap()));
    rest.extend(
        raw.record(
            &transactions(3)
                .encode_packet(message::Limits::default())
                .unwrap(),
        ),
    );
    wire::write(&raw.socket, &rest).await.unwrap();
    assert_eq!(message(&mut events).await.message(), &init());
    assert_eq!(message(&mut events).await.message(), &transactions(3));
    assert!(matches!(within(ticket).await, Ok(Reply::Response(_))));
    client.shutdown();
    assert_eq!(within(task).await.unwrap().reason, Error::Shutdown);
}

#[tokio::test]
async fn unfinished_ticket_cancellation_retires_generation_and_settles_other_requests() {
    let (client, _events, driver, mut raw) = raw_pair(Config::default()).await;
    let task = tokio::spawn(driver.run());
    let abandoned = client.request(&init(), None).unwrap();
    assert_eq!(within(raw.receive()).await, init());
    let other = client.request(&node_info(), None).unwrap();
    assert_eq!(within(raw.receive()).await, node_info());
    drop(abandoned);
    let exit = within(task).await.unwrap();
    assert_eq!(exit.reason, Error::Cancelled);
    assert_eq!(&exit.generation, client.generation());
    assert!(matches!(
        within(other).await,
        Err(Error::RequestFailed(requests::Failure::Disconnected))
    ));
    assert!(matches!(client.request(&init(), None), Err(Error::Closed)));
    let (new, _events, driver, _raw) = raw_pair(Config::default()).await;
    assert_ne!(new.generation(), client.generation());
    new.shutdown();
    assert_eq!(within(driver.run()).await.reason, Error::Shutdown);
}

#[tokio::test]
async fn request_timeout_closes_connection_and_wrong_response_does_not_release_slot() {
    let (client, _events, driver, mut raw) = raw_pair(Config::default()).await;
    let task = tokio::spawn(driver.run());
    let pending = client.request(&init(), None).unwrap();
    assert_eq!(within(raw.receive()).await, init());
    raw.send(
        &pool::Reply::Finished(true)
            .into_response(message::Limits::default())
            .unwrap(),
    )
    .await;
    let duplicate = client.request(&init(), None).unwrap();
    assert!(matches!(
        within(duplicate).await,
        Err(Error::Request(requests::Error::AlreadyPending))
    ));
    let expiring = client
        .request(&node_info(), Some(Duration::from_secs(10)))
        .unwrap();
    assert_eq!(within(raw.receive()).await, node_info());
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11)).await;
    assert!(matches!(
        within(expiring).await,
        Err(Error::RequestFailed(requests::Failure::Timeout))
    ));
    assert!(matches!(
        within(pending).await,
        Err(Error::RequestFailed(requests::Failure::Disconnected))
    ));
    assert_eq!(within(task).await.unwrap().reason, Error::Closed);
}

#[tokio::test]
async fn command_and_byte_budgets_reject_before_accepting_or_writing() {
    let config = Config {
        commands: 1,
        outgoing_bytes: 32,
        ..Config::default()
    };
    let (client, _events, driver, mut raw) = raw_pair(config).await;
    assert!(matches!(client.send(&transactions(100)), Err(Error::Busy)));
    let pending = client.send(&init()).unwrap();
    assert!(matches!(client.send(&init()), Err(Error::Busy)));
    let task = tokio::spawn(driver.run());
    assert_eq!(within(raw.receive()).await, init());
    assert!(matches!(within(pending).await, Ok(Reply::Sent)));
    // The completed ticket can be dropped unpolled without cancelling a live connection.
    let completed = client.send(&init()).unwrap();
    assert_eq!(within(raw.receive()).await, init());
    tokio::task::yield_now().await;
    drop(completed);
    let another = client.send(&init()).unwrap();
    assert_eq!(within(raw.receive()).await, init());
    assert!(matches!(within(another).await, Ok(Reply::Sent)));
    client.shutdown();
    assert_eq!(within(task).await.unwrap().reason, Error::Shutdown);
}

#[tokio::test]
async fn held_inbound_messages_and_full_event_queue_apply_bounded_backpressure() {
    for byte_limit in [true, false] {
        let config = if byte_limit {
            Config {
                incoming_bytes: init()
                    .encode_packet(message::Limits::default())
                    .unwrap()
                    .len(),
                ..Config::default()
            }
        } else {
            Config {
                events: 1,
                ..Config::default()
            }
        };
        let (client, mut events, driver, mut raw) = raw_pair(config).await;
        let task = tokio::spawn(driver.run());
        raw.send(&init()).await;
        let held = if byte_limit {
            Some(message(&mut events).await)
        } else {
            None
        };
        raw.send(&init()).await;
        assert_eq!(within(task).await.unwrap().reason, Error::Backpressure);
        assert!(client.is_closed());
        drop(held);
    }
}

#[tokio::test]
async fn absolute_partial_deadlines_cover_records_and_fragments() {
    for fragmented in [false, true] {
        let config = Config {
            partial_timeout: Duration::from_millis(30),
            ..Config::default()
        };
        let (_client, _events, driver, mut raw) = raw_pair(config).await;
        let task = tokio::spawn(driver.run());
        if fragmented {
            let frames = framing::fragment(
                &transactions(100_000)
                    .encode_packet(message::Limits::default())
                    .unwrap(),
                config.fragments,
            )
            .unwrap();
            let first = raw.record(&frames[0]);
            wire::write(&raw.socket, &first).await.unwrap();
        } else {
            let first = raw.record(&init().encode_packet(config.messages).unwrap());
            wire::write(&raw.socket, &first[..1]).await.unwrap();
        }
        assert_eq!(within(task).await.unwrap().reason, Error::Timeout);
    }
}

#[tokio::test]
async fn authentication_eof_and_driver_cancellation_release_every_owner() {
    for tamper in [false, true] {
        let (client, _events, driver, mut raw) = raw_pair(Config::default()).await;
        let task = tokio::spawn(driver.run());
        let pending = client.request(&init(), None).unwrap();
        within(raw.receive()).await;
        let expected = if tamper {
            let mut record =
                raw.record(&reply().encode_packet(message::Limits::default()).unwrap());
            *record.last_mut().unwrap() ^= 1;
            wire::write(&raw.socket, &record).await.unwrap();
            Error::Noise(noise::Error::Crypto)
        } else {
            drop(raw);
            Error::Io(io::ErrorKind::UnexpectedEof)
        };
        assert_eq!(within(task).await.unwrap().reason, expected);
        assert!(matches!(
            within(pending).await,
            Err(Error::RequestFailed(requests::Failure::Disconnected))
        ));
    }
    let (client, _events, driver, mut raw) = raw_pair(Config::default()).await;
    let task = tokio::spawn(driver.run());
    let pending = client.request(&init(), None).unwrap();
    within(raw.receive()).await;
    task.abort();
    assert!(within(task).await.unwrap_err().is_cancelled());
    assert!(matches!(within(pending).await, Err(Error::Closed)));
    let mut byte = [0];
    assert_eq!(
        within(wire::read(&raw.socket, &mut byte)).await,
        Err(Error::Io(io::ErrorKind::UnexpectedEof))
    );
    assert!(client.is_closed());
}

#[tokio::test]
async fn handshake_deadlines_wrong_network_and_invalid_configuration_close_sockets() {
    let (a, b) = sockets().await;
    let identity = noise::Identity::from_secret([1; 32]);
    assert!(matches!(
        establish(
            a,
            &identity,
            &network(),
            noise::Role::Responder,
            Config {
                handshake_timeout: Duration::from_millis(20),
                ..Config::default()
            }
        )
        .await,
        Err(Error::Timeout)
    ));
    let mut byte = [0];
    assert_eq!(
        within(wire::read(&b, &mut byte)).await,
        Err(Error::Io(io::ErrorKind::UnexpectedEof))
    );
    let (a, b) = sockets().await;
    let other = noise::Identity::from_secret([2; 32]);
    let network = network();
    let wrong = noise::Network::new(1, Hash256([0; 32]), b"wrong").unwrap();
    let (a, b) = tokio::join!(
        establish(
            a,
            &identity,
            &network,
            noise::Role::Initiator(other.public_key()),
            Config::default()
        ),
        establish(b, &other, &wrong, noise::Role::Responder, Config::default())
    );
    assert!(a.is_err() && b.is_err());
    for config in [
        Config {
            commands: 0,
            ..Config::default()
        },
        Config {
            events: 0,
            ..Config::default()
        },
        Config {
            incoming_bytes: 1,
            ..Config::default()
        },
        Config {
            partial_timeout: Duration::ZERO,
            ..Config::default()
        },
        Config {
            write_timeout: Duration::MAX,
            ..Config::default()
        },
    ] {
        assert_eq!(config.validate(), Err(Error::Configuration));
    }
}

#[tokio::test]
async fn prepared_gossip_is_shared_but_each_peer_enforces_its_own_limits_and_budget() {
    let message = transactions(64);
    let prepared = Prepared::new(&message, message::Limits::default()).unwrap();
    let config = Config {
        outgoing_bytes: prepared.bytes(),
        messages: message::Limits {
            body_bytes: 128,
            work_bytes: 4096,
            items: 100,
            depth: 8,
        },
        ..Config::default()
    };
    let ((first, _first_events, first_driver), (_remote_a, mut events_a, remote_a_driver)) =
        pair(config).await;
    let ((second, _second_events, second_driver), (_remote_b, mut events_b, remote_b_driver)) =
        pair(config).await;
    let a = first.send_prepared(&prepared).unwrap();
    let b = second.send_prepared(&prepared).unwrap();
    assert!(matches!(first.send_prepared(&prepared), Err(Error::Busy)));
    let larger = Prepared::new(&transactions(256), message::Limits::default()).unwrap();
    assert!(matches!(
        second.send_prepared(&larger),
        Err(Error::Message(_))
    ));
    drop(prepared);
    let drivers = [
        first_driver,
        remote_a_driver,
        second_driver,
        remote_b_driver,
    ]
    .map(|d| tokio::spawn(d.run()));
    assert!(matches!(within(a).await, Ok(Reply::Sent)));
    assert!(matches!(within(b).await, Ok(Reply::Sent)));
    for events in [&mut events_a, &mut events_b] {
        let Event::Message(received) = within(events.recv()).await.unwrap() else {
            panic!()
        };
        assert_eq!(received.message(), &message);
    }
    first.shutdown();
    second.shutdown();
    for driver in drivers {
        within(driver).await.unwrap();
    }
}

#[tokio::test]
async fn ping_observation_carries_pre_message_state_despite_a_newer_current_selection() {
    let (client, mut events, driver, mut raw) = raw_pair(Config::default()).await;
    let task = tokio::spawn(driver.run());
    assert_eq!(client.selected_ping_version(), None);
    assert_eq!(client.ping_version(), 1);
    raw.send(&ping(2)).await;
    raw.send(&ping(2)).await;
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping {
            action: requests::PingAction::Selected(2),
            ..
        })
    ));
    let first = message(&mut events).await;
    let second = message(&mut events).await;
    assert_eq!(
        first.ping,
        Some(PingObservation {
            previous: None,
            action: requests::PingAction::Selected(2),
            busy: false
        })
    );
    assert_eq!(
        second.ping,
        Some(PingObservation {
            previous: Some(2),
            action: requests::PingAction::None,
            busy: false
        })
    );
    assert_eq!(client.selected_ping_version(), Some(2));
    assert_eq!(client.ping_version(), 2);
    assert_eq!(first.generation, second.generation);
    raw.send(&init()).await;
    assert_eq!(message(&mut events).await.ping, None);
    client.shutdown();
    assert_eq!(within(task).await.unwrap().reason, Error::Shutdown);
}

#[tokio::test]
async fn ping_response_metadata_and_current_version_do_not_depend_on_event_consumption() {
    let (client, mut events, driver, mut raw) = raw_pair(Config::default()).await;
    let task = tokio::spawn(driver.run());
    let first = client.request(&ping(1), None).unwrap();
    assert_eq!(within(raw.receive()).await, ping(1));
    raw.send(&Message::response_ok(&ping(1), Default::default()).unwrap())
        .await;
    let Reply::Response(first) = within(first).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        first.ping,
        Some(PingObservation {
            previous: None,
            action: requests::PingAction::ProbeV2,
            busy: false
        })
    );
    assert_eq!(client.ping_version(), 1);
    // Keep the earlier ProbeV2 event queued while completing the real upgrade.
    let probe = client.probe(&ping(2)).unwrap();
    assert_eq!(within(raw.receive()).await, ping(2));
    raw.send(&Message::response_ok(&ping(2), Default::default()).unwrap())
        .await;
    let Reply::Response(upgraded) = within(probe).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        upgraded.ping,
        Some(PingObservation {
            previous: None,
            action: requests::PingAction::Selected(2),
            busy: false
        })
    );
    assert_eq!(client.selected_ping_version(), Some(2));
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping {
            action: requests::PingAction::ProbeV2,
            ..
        })
    ));
    assert_eq!(client.ping_version(), 2);
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping {
            action: requests::PingAction::Selected(2),
            ..
        })
    ));
    let refused = client.request(&ping(1), None).unwrap();
    assert_eq!(within(raw.receive()).await, ping(1));
    raw.send(
        &Message::response_error(Kind::Ping, b"refused".to_vec(), Default::default()).unwrap(),
    )
    .await;
    let Reply::Response(refused) = within(refused).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        refused.ping,
        Some(PingObservation {
            previous: Some(2),
            action: requests::PingAction::None,
            busy: false
        })
    );
    client.shutdown();
    within(task).await.unwrap();
}
