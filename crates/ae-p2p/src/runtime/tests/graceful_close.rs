use super::*;

fn close() -> Message {
    Message::new(Kind::Close, 1, BTreeMap::new(), Default::default()).unwrap()
}
fn config() -> Config {
    Config {
        defer_pings: true,
        ..Default::default()
    }
}
async fn silent(raw: &mut Raw) {
    assert!(
        tokio::time::timeout(Duration::from_millis(20), raw.receive())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn native_close_packet_deadline_and_terminal_paths_keep_owned_resources_until_exit() {
    let native: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_connection_lifecycle.json"
    ))
    .unwrap();
    let cases = native["closing"].as_array().unwrap();
    let disconnect = cases.iter().find(|c| c["name"] == "disconnect").unwrap();
    assert_eq!(
        hex::encode(close().encode_packet(Default::default()).unwrap()),
        disconnect["effects"][0]["packet"].as_str().unwrap()
    );
    assert_eq!(
        Config::default().close_timeout.as_millis(),
        u128::from(disconnect["effects"][1]["milliseconds"].as_u64().unwrap())
    );
    for mode in ["crossing_close", "disconnecting_eof", "close_timeout"] {
        let native = cases.iter().find(|c| c["name"] == mode).unwrap();
        assert_eq!(native["outcome"]["kind"], "stop");
        let (client, _events, driver, mut raw) = raw_pair(Config {
            close_timeout: Duration::from_millis(100),
            ..config()
        })
        .await;
        let monitor = client.monitor();
        assert!(!client.is_disconnecting());
        client.disconnect();
        client.disconnect();
        assert!(client.is_disconnecting());
        assert!(!client.is_closed());
        assert!(monitor.exit().is_none());
        let task = tokio::spawn(driver.run());
        assert_eq!(within(raw.receive()).await, close());
        assert!(monitor.exit().is_none());
        match mode {
            "crossing_close" => raw.send(&close()).await,
            "disconnecting_eof" => drop(raw),
            "close_timeout" => silent(&mut raw).await,
            _ => unreachable!(),
        }
        let exit = within(monitor.wait()).await;
        assert_eq!(exit.reason, Error::LocalClose, "{mode}");
        assert_eq!(&exit.generation, client.generation());
        assert_eq!(within(task).await.unwrap(), exit);
        assert!(client.is_closed());
    }
}

#[tokio::test]
async fn repeated_disconnect_does_not_restart_deadline_or_depend_on_command_capacity() {
    let (client, _events, driver, mut raw) = raw_pair(Config {
        commands: 1,
        outgoing_bytes: init().encode_packet(Default::default()).unwrap().len(),
        close_timeout: Duration::from_millis(150),
        ..config()
    })
    .await;
    let queued = client.send(&init()).unwrap();
    assert!(matches!(client.send(&init()), Err(Error::Busy)));
    client.disconnect();
    let monitor = client.monitor();
    let task = tokio::spawn(driver.run());
    assert_eq!(within(raw.receive()).await, close());
    assert!(matches!(within(queued).await, Ok(Reply::Sent)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    client.disconnect();
    client.disconnect();
    let exit = tokio::time::timeout(Duration::from_millis(100), monitor.wait())
        .await
        .expect("repeated disconnect must not restart the 150ms grace period");
    assert_eq!(exit.reason, Error::LocalClose);
    assert_eq!(within(task).await.unwrap(), exit);
}

#[tokio::test]
async fn closing_still_delivers_callbacks_and_responses_but_suppresses_all_new_writes() {
    let (client, mut events, driver, mut raw) = raw_pair(config()).await;
    let task = tokio::spawn(driver.run());
    let request = client.request(&init(), None).unwrap();
    assert_eq!(within(raw.receive()).await, init());
    client.disconnect();
    assert_eq!(within(raw.receive()).await, close());
    raw.send(&reply()).await;
    assert!(matches!(within(request).await, Ok(Reply::Response(_))));
    raw.send(&init()).await;
    assert_eq!(message(&mut events).await.message(), &init());
    within(client.send(&reply()).unwrap()).await.unwrap();
    within(client.send(&transactions(64)).unwrap())
        .await
        .unwrap();
    within(
        client
            .send_prepared(&Prepared::new(&reply(), Default::default()).unwrap())
            .unwrap(),
    )
    .await
    .unwrap();
    raw.send(&ping(2)).await;
    let received = message(&mut events).await;
    within(
        client
            .finish_ping(
                &received,
                Some(&Message::response_ok(&ping(2), Default::default()).unwrap()),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping {
            action: requests::PingAction::Selected(2),
            ..
        })
    ));
    assert_eq!(client.selected_ping_version(), Some(2));
    silent(&mut raw).await;
    // Like native prepare_and_send_request, a request still acquires its slot
    // although send_msg suppresses bytes. A crossing response can complete it.
    let suppressed = client.request(&init(), None).unwrap();
    let duplicate = client.request(&init(), None).unwrap();
    assert!(matches!(
        within(duplicate).await,
        Err(Error::Request(requests::Error::AlreadyPending))
    ));
    raw.send(&reply()).await;
    assert!(matches!(within(suppressed).await, Ok(Reply::Response(_))));
    silent(&mut raw).await;
    raw.send(&close()).await;
    assert_eq!(within(task).await.unwrap().reason, Error::LocalClose);
}

#[tokio::test]
async fn disconnect_waits_for_deferred_ping_response_and_negotiation_before_close() {
    let (client, mut events, driver, mut raw) = raw_pair(config()).await;
    let task = tokio::spawn(driver.run());
    let packets = [
        raw.record(&ping(2).encode_packet(Default::default()).unwrap()),
        raw.record(&init().encode_packet(Default::default()).unwrap()),
    ]
    .concat();
    wire::write(&raw.socket, &packets).await.unwrap();
    let received = message(&mut events).await;
    client.disconnect();
    silent(&mut raw).await;
    assert!(events.try_recv().is_err());
    let response = Message::response_ok(&ping(2), Default::default()).unwrap();
    within(client.finish_ping(&received, Some(&response)).unwrap())
        .await
        .unwrap();
    assert_eq!(within(raw.receive()).await, response);
    assert_eq!(within(raw.receive()).await, close());
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping {
            action: requests::PingAction::Selected(2),
            ..
        })
    ));
    assert_eq!(message(&mut events).await.message(), &init());
    within(client.send(&reply()).unwrap()).await.unwrap();
    silent(&mut raw).await;
    raw.send(&close()).await;
    assert_eq!(within(task).await.unwrap().reason, Error::LocalClose);
}

#[tokio::test]
async fn closing_solicited_ping_preserves_probe_registration_without_sending_probe() {
    let (client, _events, driver, mut raw) = raw_pair(config()).await;
    let task = tokio::spawn(driver.run());
    let request = client.request(&ping(1), None).unwrap();
    assert_eq!(within(raw.receive()).await, ping(1));
    client.disconnect();
    assert_eq!(within(raw.receive()).await, close());
    raw.send(&Message::response_ok(&ping(1), Default::default()).unwrap())
        .await;
    let Reply::Response(received) = within(request).await.unwrap() else {
        panic!()
    };
    let (finish, probe) = client.finish_ping_with_probe(&received, &ping(2)).unwrap();
    within(finish).await.unwrap();
    silent(&mut raw).await;
    raw.send(&Message::response_ok(&ping(2), Default::default()).unwrap())
        .await;
    let Reply::Response(received) = within(probe).await.unwrap() else {
        panic!()
    };
    within(client.finish_ping(&received, None).unwrap())
        .await
        .unwrap();
    assert_eq!(client.selected_ping_version(), Some(2));
    raw.send(&close()).await;
    assert_eq!(within(task).await.unwrap().reason, Error::LocalClose);
}

#[tokio::test]
async fn closing_deadline_bounds_held_callbacks_and_settles_pending_requests() {
    let (client, mut events, driver, mut raw) = raw_pair(Config {
        close_timeout: Duration::from_millis(100),
        ..config()
    })
    .await;
    let task = tokio::spawn(driver.run());
    let request = client.request(&init(), None).unwrap();
    assert_eq!(within(raw.receive()).await, init());
    client.disconnect();
    assert_eq!(within(raw.receive()).await, close());
    raw.send(&ping(2)).await;
    let held = message(&mut events).await;
    let exit = within(client.monitor().wait()).await;
    assert_eq!(exit.reason, Error::LocalClose);
    assert!(matches!(
        within(request).await,
        Err(Error::RequestFailed(requests::Failure::Disconnected))
    ));
    assert_eq!(within(task).await.unwrap(), exit);
    drop(held);
    assert_eq!(client.monitor().exit(), Some(exit));
}

#[tokio::test]
async fn abrupt_shutdown_and_abandonment_remain_distinct_from_graceful_close() {
    for mode in ["shutdown", "cancel", "drop", "eof", "remote"] {
        let (client, _events, driver, mut raw) = raw_pair(config()).await;
        let monitor = client.monitor();
        let expected = match mode {
            "shutdown" => Error::Shutdown,
            "cancel" | "drop" => Error::Cancelled,
            "eof" => Error::Io(io::ErrorKind::UnexpectedEof),
            "remote" => Error::RemoteClose,
            _ => unreachable!(),
        };
        if mode == "drop" {
            client.disconnect();
            drop(driver);
        } else {
            let task = tokio::spawn(driver.run());
            match mode {
                "shutdown" => {
                    client.disconnect();
                    client.shutdown();
                }
                "cancel" => {
                    client.disconnect();
                    drop(client.request(&init(), None).unwrap());
                }
                "eof" => drop(raw),
                "remote" => raw.send(&close()).await,
                _ => unreachable!(),
            }
            assert_eq!(within(task).await.unwrap().reason, expected);
        }
        assert_eq!(within(monitor.wait()).await.reason, expected);
    }
    for close_timeout in [Duration::ZERO, Duration::MAX] {
        assert_eq!(
            Config {
                close_timeout,
                ..config()
            }
            .validate(),
            Err(Error::Configuration)
        );
    }
}

#[tokio::test]
async fn graceful_intent_survives_last_client_drop_until_terminal_monitor_completion() {
    let (client, _events, driver, mut raw) = raw_pair(config()).await;
    let monitor = client.monitor();
    client.disconnect();
    drop(client);
    let task = tokio::spawn(driver.run());
    assert_eq!(within(raw.receive()).await, close());
    assert!(monitor.exit().is_none());
    raw.send(&close()).await;
    let exit = within(monitor.wait()).await;
    assert_eq!(exit.reason, Error::LocalClose);
    assert_eq!(within(task).await.unwrap(), exit);
}

async fn plaintext(raw: &mut Raw) -> Vec<u8> {
    let mut prefix = [0; 2];
    within(wire::read(&raw.socket, &mut prefix)).await.unwrap();
    let mut bytes = vec![0; usize::from(u16::from_be_bytes(prefix))];
    within(wire::read(&raw.socket, &mut bytes)).await.unwrap();
    raw.crypto.open(&bytes).unwrap()
}

#[tokio::test]
async fn disconnect_keeps_fragmented_packet_and_cipher_nonces_contiguous_before_close() {
    // Exercise both ready-branch choices when a queued command and close
    // intent wake the same select after its pre-await intent check.
    for _ in 0..16 {
        let (client, _events, driver, mut raw) = raw_pair(config()).await;
        let task = tokio::spawn(driver.run());
        let gossip = transactions(200_000);
        let sending = client.send(&gossip).unwrap();
        let first = plaintext(&mut raw).await;
        assert_eq!(&first[..2], &[0, 0]);
        let queued = client.send(&init()).unwrap();
        client.disconnect();
        let mut assembler = framing::Assembler::new(Default::default());
        let mut packet = assembler.push(&first).unwrap();
        while packet.is_none() {
            packet = assembler.push(&plaintext(&mut raw).await).unwrap();
        }
        assert_eq!(
            Message::decode_packet(&packet.unwrap(), Default::default()).unwrap(),
            gossip
        );
        assert!(matches!(within(sending).await, Ok(Reply::Sent)));
        assert_eq!(within(raw.receive()).await, close());
        assert!(matches!(within(queued).await, Ok(Reply::Sent)));
        silent(&mut raw).await;
        raw.send(&close()).await;
        assert_eq!(within(task).await.unwrap().reason, Error::LocalClose);
    }
}

#[tokio::test]
async fn disconnect_does_not_discard_active_ciphertext_or_extend_its_write_deadline() {
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
    let (client, _events, driver, mut raw) = raw_streams(
        a,
        b,
        Config {
            write_timeout: Duration::from_millis(300),
            ..config()
        },
    )
    .await;
    let task = tokio::spawn(driver.run());
    let sending = client.send(&transactions(2_000_000)).unwrap();
    assert_eq!(&plaintext(&mut raw).await[..2], &[0, 0]);
    client.disconnect();
    assert_eq!(within(task).await.unwrap().reason, Error::Timeout);
    assert!(matches!(within(sending).await, Err(Error::Closed)));
    assert!(client.is_closed());
}
