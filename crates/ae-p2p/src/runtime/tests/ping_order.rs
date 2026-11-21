use super::*;

fn managed() -> Config {
    Config {
        defer_pings: true,
        ..Default::default()
    }
}
fn response(ping: &Message) -> Message {
    Message::response_ok(ping, Default::default()).unwrap()
}
fn incompatible() -> Message {
    let mut fields = ping(2).fields().clone();
    fields.insert(
        "versions".into(),
        Value::List(vec![Value::Record(BTreeMap::from([
            ("protocol".into(), Value::Binary(b"ping".to_vec())),
            ("vsns".into(), Value::List(vec![])),
        ]))]),
    );
    Message::new(Kind::Ping, 2, fields, Default::default()).unwrap()
}
async fn selected(events: &mut Events, version: u16) {
    assert!(
        matches!(within(events.recv()).await, Some(Event::Ping { action: requests::PingAction::Selected(v), .. }) if v == version)
    );
}

#[tokio::test]
async fn completion_bypasses_full_command_queue_and_serializes_coalesced_pings() {
    let (client, mut events, driver, mut raw) = raw_pair(Config {
        commands: 1,
        ..managed()
    })
    .await;
    let task = tokio::spawn(driver.run());
    let packet = ping(2).encode_packet(Default::default()).unwrap();
    let packets = [
        raw.record(&init().encode_packet(Default::default()).unwrap()),
        raw.record(&packet),
        raw.record(&packet),
        raw.record(&init().encode_packet(Default::default()).unwrap()),
    ]
    .concat();
    wire::write(&raw.socket, &packets).await.unwrap();
    assert_eq!(message(&mut events).await.message(), &init());
    let first = message(&mut events).await;
    assert_eq!(first.ping.unwrap().previous, None);
    assert_eq!(client.selected_ping_version(), None);
    let waiting = client.request(&init(), None).unwrap();
    assert!(matches!(client.send(&init()), Err(Error::Busy)));
    assert!(events.try_recv().is_err());
    let earlier_reply = Prepared::new(&super::reply(), Default::default()).unwrap();
    within(client.send_prepared(&earlier_reply).unwrap())
        .await
        .unwrap();
    assert_eq!(within(raw.receive()).await, super::reply());
    assert_eq!(client.selected_ping_version(), None);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), raw.receive())
            .await
            .is_err()
    );
    let reply = response(first.message());
    assert!(matches!(
        within(client.finish_ping(&first, Some(&reply)).unwrap()).await,
        Ok(Reply::Sent)
    ));
    assert_eq!(within(raw.receive()).await, reply);
    selected(&mut events, 2).await;
    let second = message(&mut events).await;
    assert_eq!(second.ping.unwrap().previous, Some(2));
    assert_eq!(second.ping.unwrap().action, requests::PingAction::None);
    assert!(events.try_recv().is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), raw.receive())
            .await
            .is_err()
    );
    within(client.finish_ping(&second, Some(&reply)).unwrap())
        .await
        .unwrap();
    assert_eq!(within(raw.receive()).await, reply);
    assert_eq!(message(&mut events).await.message(), &init());
    assert_eq!(within(raw.receive()).await, init());
    raw.send(&super::reply()).await;
    assert!(matches!(within(waiting).await, Ok(Reply::Response(_))));
    client.shutdown();
    assert_eq!(within(task).await.unwrap().reason, Error::Shutdown);
}

#[tokio::test]
async fn invalid_negotiation_sends_incoming_response_before_retiring_stream() {
    let (client, mut events, driver, mut raw) = raw_pair(managed()).await;
    let monitor = client.monitor();
    let task = tokio::spawn(driver.run());
    raw.send(&incompatible()).await;
    let received = message(&mut events).await;
    assert_eq!(received.message(), &incompatible());
    assert!(monitor.exit().is_none());
    assert!(!client.is_closed());
    assert_eq!(client.selected_ping_version(), None);
    let queued = client.request(&init(), None).unwrap();
    let response = response(&ping(2));
    assert!(matches!(
        within(client.finish_ping(&received, Some(&response)).unwrap()).await,
        Err(Error::Request(requests::Error::NoCommonPingVersion))
    ));
    assert_eq!(within(raw.receive()).await, response);
    let mut byte = [0];
    assert_eq!(
        within(wire::read(&raw.socket, &mut byte))
            .await
            .unwrap_err(),
        Error::Io(io::ErrorKind::UnexpectedEof)
    );
    assert!(matches!(within(queued).await, Err(Error::Closed)));
    assert_eq!(
        within(task).await.unwrap().reason,
        Error::Request(requests::Error::NoCommonPingVersion)
    );
}

#[tokio::test]
async fn invalid_solicited_ping_reaches_caller_before_failure_is_committed() {
    let (client, _events, driver, mut raw) = raw_pair(managed()).await;
    let task = tokio::spawn(driver.run());
    let request = client.request(&ping(2), None).unwrap();
    assert_eq!(within(raw.receive()).await, ping(2));
    let envelope = response(&incompatible());
    raw.send(&envelope).await;
    let Reply::Response(received) = within(request).await.unwrap() else {
        panic!()
    };
    assert_eq!(received.message(), &envelope);
    assert!(client.monitor().exit().is_none());
    assert!(!client.is_closed());
    assert_eq!(client.selected_ping_version(), None);
    assert!(matches!(
        within(client.finish_ping(&received, None).unwrap()).await,
        Err(Error::Request(requests::Error::NoCommonPingVersion))
    ));
    assert_eq!(
        within(task).await.unwrap().reason,
        Error::Request(requests::Error::NoCommonPingVersion)
    );
}

#[tokio::test]
async fn ping_receipts_reject_wrong_generation_shape_and_reuse_without_consuming_valid_owner() {
    let (client, mut events, driver, mut raw) = raw_pair(managed()).await;
    let (other, _events, other_driver, _raw) = raw_pair(managed()).await;
    let task = tokio::spawn(driver.run());
    raw.send(&ping(2)).await;
    let received = message(&mut events).await;
    let response = response(received.message());
    assert!(matches!(client.send(&response), Err(Error::InvalidPing)));
    assert!(matches!(
        client.send_prepared(&Prepared::new(&response, Default::default()).unwrap()),
        Err(Error::InvalidPing)
    ));
    assert!(matches!(
        other.finish_ping(&received, Some(&response)),
        Err(Error::InvalidPing)
    ));
    assert!(matches!(
        client.finish_ping(&received, None),
        Err(Error::InvalidPing)
    ));
    assert!(matches!(
        client.finish_ping(&received, Some(&init())),
        Err(Error::InvalidPing)
    ));
    let ticket = client.finish_ping(&received, Some(&response)).unwrap();
    assert!(matches!(
        client.finish_ping(&received, Some(&response)),
        Err(Error::Busy | Error::InvalidPing)
    ));
    within(ticket).await.unwrap();
    assert_eq!(within(raw.receive()).await, response);
    selected(&mut events, 2).await;
    assert!(matches!(
        client.finish_ping(&received, Some(&response)),
        Err(Error::InvalidPing)
    ));
    raw.send(&ping(2)).await;
    let next = message(&mut events).await;
    assert!(matches!(
        client.finish_ping(&received, Some(&response)),
        Err(Error::InvalidPing)
    ));
    within(client.finish_ping(&next, Some(&response)).unwrap())
        .await
        .unwrap();
    assert_eq!(within(raw.receive()).await, response);
    assert!(!client.is_closed());
    client.shutdown();
    within(task).await.unwrap();
    drop(other_driver);
    assert_eq!(
        within(other.monitor().wait()).await.reason,
        Error::Cancelled
    );
}

#[tokio::test]
async fn abandoned_ping_work_and_expired_callback_deadline_close_and_join_owner() {
    for mode in ["receipt", "ticket", "driver", "deadline"] {
        let (client, mut events, driver, mut raw) = raw_pair(Config {
            ping_timeout: Duration::from_secs(1),
            ..managed()
        })
        .await;
        let monitor = client.monitor();
        let task = tokio::spawn(driver.run());
        raw.send(&ping(2)).await;
        let received = message(&mut events).await;
        let queued = client.request(&init(), None).unwrap();
        match mode {
            "receipt" => drop(received),
            "ticket" => drop(
                client
                    .finish_ping(&received, Some(&response(received.message())))
                    .unwrap(),
            ),
            "driver" => {
                task.abort();
                drop(received);
            }
            "deadline" => {
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(2)).await;
                tokio::time::resume();
                assert_eq!(within(monitor.wait()).await.reason, Error::Timeout);
                drop(received);
            }
            _ => unreachable!(),
        }
        let exit = within(monitor.wait()).await;
        assert_eq!(
            exit.reason,
            if mode == "deadline" {
                Error::Timeout
            } else {
                Error::Cancelled
            },
            "{mode}"
        );
        assert!(matches!(within(queued).await, Err(Error::Closed)));
        assert!(client.is_closed());
        if mode == "driver" {
            assert!(within(task).await.unwrap_err().is_cancelled());
        } else {
            assert_eq!(within(task).await.unwrap(), exit);
        }
    }
}

#[tokio::test]
async fn busy_response_finishes_before_coalesced_solicited_ping_negotiation() {
    let (client, mut events, driver, mut raw) = raw_pair(managed()).await;
    let task = tokio::spawn(driver.run());
    let request = client.request(&ping(2), None).unwrap();
    assert_eq!(within(raw.receive()).await, ping(2));
    let frames = [
        raw.record(&incompatible().encode_packet(Default::default()).unwrap()),
        raw.record(
            &response(&ping(2))
                .encode_packet(Default::default())
                .unwrap(),
        ),
    ]
    .concat();
    wire::write(&raw.socket, &frames).await.unwrap();
    let busy = message(&mut events).await;
    assert!(busy.ping.unwrap().busy);
    assert_eq!(busy.ping.unwrap().action, requests::PingAction::None);
    let error =
        Message::response_error(Kind::Ping, b"already_pinging".to_vec(), Default::default())
            .unwrap();
    within(client.finish_ping(&busy, Some(&error)).unwrap())
        .await
        .unwrap();
    assert_eq!(within(raw.receive()).await, error);
    let Reply::Response(received) = within(request).await.unwrap() else {
        panic!()
    };
    assert_eq!(received.ping.unwrap().previous, None);
    assert_eq!(client.selected_ping_version(), None);
    within(client.finish_ping(&received, None).unwrap())
        .await
        .unwrap();
    selected(&mut events, 2).await;
    assert_eq!(client.selected_ping_version(), Some(2));
    client.shutdown();
    within(task).await.unwrap();
}

#[tokio::test]
async fn automatic_probe_is_written_and_registered_before_following_ping() {
    let (client, mut events, driver, mut raw) = raw_pair(managed()).await;
    let task = tokio::spawn(driver.run());
    let request = client.request(&ping(1), None).unwrap();
    assert_eq!(within(raw.receive()).await, ping(1));
    let frames = [
        raw.record(
            &response(&ping(1))
                .encode_packet(Default::default())
                .unwrap(),
        ),
        raw.record(&ping(2).encode_packet(Default::default()).unwrap()),
    ]
    .concat();
    wire::write(&raw.socket, &frames).await.unwrap();
    let Reply::Response(received) = within(request).await.unwrap() else {
        panic!()
    };
    assert_eq!(received.ping.unwrap().action, requests::PingAction::ProbeV2);
    assert_eq!(client.selected_ping_version(), None);
    assert!(client.monitor().exit().is_none());
    assert!(matches!(
        client.finish_ping(&received, None),
        Err(Error::InvalidPing)
    ));
    assert!(matches!(
        client.finish_ping_with_probe(&received, &ping(1)),
        Err(Error::InvalidPing)
    ));
    let (finished, probe) = client.finish_ping_with_probe(&received, &ping(2)).unwrap();
    within(finished).await.unwrap();
    assert_eq!(within(raw.receive()).await, ping(2));
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping {
            action: requests::PingAction::ProbeV2,
            ..
        })
    ));
    let busy = message(&mut events).await;
    assert!(busy.ping.unwrap().busy);
    assert_eq!(busy.ping.unwrap().previous, None);
    assert_eq!(client.selected_ping_version(), None);
    let error =
        Message::response_error(Kind::Ping, b"already_pinging".to_vec(), Default::default())
            .unwrap();
    within(client.finish_ping(&busy, Some(&error)).unwrap())
        .await
        .unwrap();
    assert_eq!(within(raw.receive()).await, error);
    raw.send(&response(&ping(2))).await;
    let Reply::Response(received) = within(probe).await.unwrap() else {
        panic!()
    };
    within(client.finish_ping(&received, None).unwrap())
        .await
        .unwrap();
    selected(&mut events, 2).await;
    assert_eq!(client.selected_ping_version(), Some(2));
    client.shutdown();
    assert_eq!(within(task).await.unwrap().reason, Error::Shutdown);
}

#[tokio::test]
async fn ordinary_request_deadline_waits_until_incoming_callback_finishes() {
    let (client, mut events, driver, mut raw) = raw_pair(managed()).await;
    let task = tokio::spawn(driver.run());
    let info = client
        .request(&node_info(), Some(Duration::from_secs(5)))
        .unwrap();
    assert_eq!(within(raw.receive()).await, node_info());
    raw.send(&ping(2)).await;
    let received = message(&mut events).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    tokio::time::resume();
    assert!(client.monitor().exit().is_none());
    assert!(!client.is_closed());
    let response = response(received.message());
    within(client.finish_ping(&received, Some(&response)).unwrap())
        .await
        .unwrap();
    assert_eq!(within(raw.receive()).await, response);
    assert!(matches!(
        within(info).await,
        Err(Error::RequestFailed(requests::Failure::Timeout))
    ));
    assert_eq!(within(task).await.unwrap().reason, Error::Closed);
}

#[tokio::test]
async fn blocked_ping_write_expires_without_committing_negotiation() {
    for probe in [false, true] {
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
        let (client, mut events, driver, mut raw) = raw_streams(
            a,
            b,
            Config {
                write_timeout: Duration::from_millis(500),
                ..managed()
            },
        )
        .await;
        let task = tokio::spawn(driver.run());
        let received = if probe {
            let request = client.request(&ping(1), None).unwrap();
            assert_eq!(within(raw.receive()).await, ping(1));
            raw.send(&response(&ping(1))).await;
            let Reply::Response(received) = within(request).await.unwrap() else {
                panic!()
            };
            received
        } else {
            raw.send(&ping(2)).await;
            message(&mut events).await
        };
        let mut fields = ping(2).fields().clone();
        fields.insert("best_hash".into(), Value::Binary(vec![9; 2_000_000]));
        let large = Message::new(Kind::Ping, 2, fields, Default::default()).unwrap();
        let (finishing, pending_probe) = if probe {
            let (finished, response) = client.finish_ping_with_probe(&received, &large).unwrap();
            (finished, Some(response))
        } else {
            (
                client
                    .finish_ping(&received, Some(&response(&large)))
                    .unwrap(),
                None,
            )
        };
        // Observe the first byte of ciphertext, then deliberately stop draining.
        within(wire::read(&raw.socket, &mut [0])).await.unwrap();
        assert_eq!(client.selected_ping_version(), None);
        assert_eq!(within(task).await.unwrap().reason, Error::Timeout);
        assert!(matches!(within(finishing).await, Err(Error::Closed)));
        assert_eq!(client.selected_ping_version(), None);
        if let Some(pending_probe) = pending_probe {
            assert!(matches!(
                within(pending_probe).await,
                Err(Error::RequestFailed(requests::Failure::Disconnected))
            ));
        }
    }
}

#[tokio::test]
async fn failed_response_reservation_leaves_receipt_retryable_and_refunds_after_write() {
    let (client, mut events, driver, mut raw) = raw_pair(Config {
        outgoing_bytes: 64,
        ..managed()
    })
    .await;
    let task = tokio::spawn(driver.run());
    raw.send(&ping(2)).await;
    let received = message(&mut events).await;
    assert!(matches!(
        client.finish_ping(&received, Some(&response(received.message()))),
        Err(Error::Busy)
    ));
    assert_eq!(client.selected_ping_version(), None);
    let error =
        Message::response_error(Kind::Ping, b"refused".to_vec(), Default::default()).unwrap();
    within(client.finish_ping(&received, Some(&error)).unwrap())
        .await
        .unwrap();
    assert_eq!(within(raw.receive()).await, error);
    selected(&mut events, 2).await;
    let ordinary_reply =
        Message::response_error(Kind::NodeInfo, b"refused".to_vec(), Default::default()).unwrap();
    for _ in 0..8 {
        within(client.send(&ordinary_reply).unwrap()).await.unwrap();
        assert_eq!(within(raw.receive()).await, ordinary_reply);
    }
    client.shutdown();
    within(task).await.unwrap();
}
