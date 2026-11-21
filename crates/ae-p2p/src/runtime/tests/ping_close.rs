use super::*;

fn config() -> Config {
    Config {
        defer_pings: true,
        ..Default::default()
    }
}
fn close() -> Message {
    Message::new(Kind::Close, 1, BTreeMap::new(), Default::default()).unwrap()
}
fn response(message: &Message) -> Message {
    Message::response_ok(message, Default::default()).unwrap()
}
async fn silent(raw: &mut Raw) {
    assert!(
        tokio::time::timeout(Duration::from_millis(20), raw.receive())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn notification_continuation_holds_coalesced_input_and_close_intent_after_response() {
    let (client, mut events, driver, mut raw) = raw_pair(Config {
        commands: 1,
        ..config()
    })
    .await;
    let task = tokio::spawn(driver.run());
    let bytes = [&ping(2), &init(), &ping(2)]
        .into_iter()
        .flat_map(|m| raw.record(&m.encode_packet(Default::default()).unwrap()))
        .collect::<Vec<_>>();
    wire::write(&raw.socket, &bytes).await.unwrap();
    let received = message(&mut events).await;
    let queued = client.send(&init()).unwrap();
    let (finished, continuation) = client
        .finish_ping_before_close(&received, &response(received.message()))
        .unwrap();
    assert_eq!(continuation.generation(), client.generation());
    assert!(!continuation.is_ready());
    within(finished).await.unwrap();
    assert_eq!(within(raw.receive()).await, response(received.message()));
    assert!(continuation.is_ready());
    assert!(!continuation.was_closing());
    assert_eq!(client.selected_ping_version(), Some(2));
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping {
            action: requests::PingAction::Selected(2),
            ..
        })
    ));
    // Even a concurrent ordinary disconnect must wait for policy notification.
    client.disconnect();
    silent(&mut raw).await;
    assert!(events.try_recv().is_err());
    continuation.close().unwrap();
    assert_eq!(within(raw.receive()).await, close());
    assert_eq!(message(&mut events).await.message(), &init());
    let next = message(&mut events).await;
    assert_eq!(next.message(), &ping(2));
    within(
        client
            .finish_ping(&next, Some(&response(next.message())))
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(within(queued).await, Ok(Reply::Sent)));
    silent(&mut raw).await;
    raw.send(&close()).await;
    assert_eq!(within(task).await.unwrap().reason, Error::LocalClose);
}

#[tokio::test]
async fn already_closing_ping_continuation_does_not_send_another_close_or_extend_grace() {
    let (client, mut events, driver, mut raw) = raw_pair(Config {
        close_timeout: Duration::from_millis(150),
        ..config()
    })
    .await;
    let task = tokio::spawn(driver.run());
    client.disconnect();
    assert_eq!(within(raw.receive()).await, close());
    raw.send(&ping(2)).await;
    let received = message(&mut events).await;
    let (finished, continuation) = client
        .finish_ping_before_close(&received, &response(received.message()))
        .unwrap();
    within(finished).await.unwrap();
    assert!(continuation.is_ready() && continuation.was_closing());
    silent(&mut raw).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    continuation.close().unwrap();
    let exit = tokio::time::timeout(Duration::from_millis(100), client.monitor().wait())
        .await
        .unwrap();
    assert_eq!(exit.reason, Error::LocalClose);
    assert_eq!(within(task).await.unwrap(), exit);
}

#[tokio::test]
async fn failed_negotiation_sends_response_but_never_authorizes_policy_close() {
    let (client, mut events, driver, mut raw) = raw_pair(config()).await;
    let task = tokio::spawn(driver.run());
    let mut fields = ping(2).fields().clone();
    fields.insert(
        "versions".into(),
        Value::List(vec![Value::Record(BTreeMap::from([
            ("protocol".into(), Value::Binary(b"ping".to_vec())),
            ("vsns".into(), Value::List(vec![])),
        ]))]),
    );
    let incompatible = Message::new(Kind::Ping, 2, fields, Default::default()).unwrap();
    raw.send(&incompatible).await;
    let received = message(&mut events).await;
    let reply = response(&ping(2));
    let (finished, continuation) = client.finish_ping_before_close(&received, &reply).unwrap();
    assert!(matches!(
        within(finished).await,
        Err(Error::Request(requests::Error::NoCommonPingVersion))
    ));
    assert_eq!(within(raw.receive()).await, reply);
    assert!(!continuation.is_ready());
    assert_eq!(continuation.close(), Err(Error::InvalidPing));
    let exit = within(task).await.unwrap();
    assert_eq!(
        exit.reason,
        Error::Request(requests::Error::NoCommonPingVersion)
    );
    assert_eq!(
        within(wire::read(&raw.socket, &mut [0])).await,
        Err(Error::Io(io::ErrorKind::UnexpectedEof))
    );
}

#[tokio::test]
async fn continuation_abandonment_early_release_shutdown_and_deadline_retire_exact_owner() {
    for mode in ["drop", "early", "shutdown", "timeout"] {
        let (client, mut events, driver, mut raw) = raw_pair(Config {
            ping_timeout: Duration::from_millis(100),
            ..config()
        })
        .await;
        let task = tokio::spawn(driver.run());
        raw.send(&ping(2)).await;
        let received = message(&mut events).await;
        let (finished, continuation) = client
            .finish_ping_before_close(&received, &response(received.message()))
            .unwrap();
        let expected = match mode {
            "early" => {
                assert_eq!(continuation.close(), Err(Error::InvalidPing));
                assert!(within(finished).await.is_err());
                Error::Cancelled
            }
            _ => {
                within(finished).await.unwrap();
                assert_eq!(within(raw.receive()).await, response(received.message()));
                match mode {
                    "drop" => {
                        drop(continuation);
                        Error::Cancelled
                    }
                    "shutdown" => {
                        client.shutdown();
                        drop(continuation);
                        Error::Shutdown
                    }
                    "timeout" => {
                        let exit = within(client.monitor().wait()).await;
                        assert_eq!(exit.reason, Error::Timeout);
                        drop(continuation);
                        Error::Timeout
                    }
                    _ => unreachable!(),
                }
            }
        };
        let exit = within(task).await.unwrap();
        assert_eq!(exit.reason, expected, "{mode}");
        assert_eq!(client.monitor().exit(), Some(exit));
    }
}

#[tokio::test]
async fn closing_receipts_reject_foreign_generation_and_busy_ping_without_consuming_callback() {
    let (client, mut events, driver, mut raw) = raw_pair(config()).await;
    let (other, _events, other_driver, _raw) = raw_pair(config()).await;
    let task = tokio::spawn(driver.run());
    raw.send(&ping(2)).await;
    let received = message(&mut events).await;
    assert!(matches!(
        other.finish_ping_before_close(&received, &response(received.message())),
        Err(Error::InvalidPing)
    ));
    assert!(!other.is_closed());
    within(
        client
            .finish_ping(&received, Some(&response(received.message())))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(within(raw.receive()).await, response(received.message()));
    assert!(matches!(
        within(events.recv()).await,
        Some(Event::Ping { .. })
    ));
    let pending = client.request(&ping(2), None).unwrap();
    assert_eq!(within(raw.receive()).await, ping(2));
    raw.send(&ping(2)).await;
    let received = message(&mut events).await;
    assert!(received.ping.unwrap().busy);
    let reply =
        Message::response_error(Kind::Ping, b"already_pinging".to_vec(), Default::default())
            .unwrap();
    assert!(matches!(
        client.finish_ping_before_close(&received, &reply),
        Err(Error::InvalidPing)
    ));
    within(client.finish_ping(&received, Some(&reply)).unwrap())
        .await
        .unwrap();
    assert_eq!(within(raw.receive()).await, reply);
    client.shutdown();
    assert!(within(pending).await.is_err());
    assert_eq!(within(task).await.unwrap().reason, Error::Shutdown);
    drop(other_driver);
}
