use super::*;

#[test]
fn original_socket_open_retry_and_send_error_transitions_preserve_native_state() {
    let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    receiver
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    struct FailOnce(bool);
    impl Backend for FailOnce {
        fn hostname(&mut self) -> io::Result<String> {
            Ok("localhost".into())
        }
        fn resolve(&mut self, _: &str) -> io::Result<IpAddr> {
            Ok("127.0.0.1".parse().unwrap())
        }
        fn open(&mut self, a: SocketAddr) -> io::Result<UdpSocket> {
            if std::mem::replace(&mut self.0, false) {
                Err(io::ErrorKind::PermissionDenied.into())
            } else {
                System.open(a)
            }
        }
    }
    for delay in [0, 5] {
        let mut udp = Udp::new(Target::Configured(Config {
            port: receiver.local_addr().unwrap().port(),
            reconnect_interval: Duration::from_millis(delay),
            ..Default::default()
        }));
        udp.backend = Box::new(FailOnce(true));
        udp.resolve().unwrap();
        assert_eq!(
            udp.connect().unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(udp.next.is_some());
        assert!(!udp.write(&[b"ignored".to_vec()]).unwrap());
        udp.connect().unwrap();
        assert!(udp.connected() && udp.next.is_none());
        let local = udp.socket.as_ref().unwrap().local_addr().unwrap();
        assert!(
            udp.write(&[vec![0; 65536], b"ae.metric:42|g".to_vec()])
                .is_err()
        );
        assert_eq!(udp.socket.as_ref().unwrap().local_addr().unwrap(), local);
        assert!(udp.next.is_none());
        let mut bytes = [0; 64];
        let (n, from) = receiver.recv_from(&mut bytes).unwrap();
        assert_eq!(&bytes[..n], b"ae.metric:42|g");
        assert_eq!(from.port(), local.port());
    }
}
#[tokio::test]
async fn owned_exporter_initializes_sends_and_joins_without_a_connection_group() {
    let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut worker = Worker::prepare(Settings {
        target: Config {
            host: Host::Name("127.0.0.1".into()),
            port: receiver.local_addr().unwrap().port(),
            ..Default::default()
        },
        ..Default::default()
    })
    .unwrap();
    let mut client = worker.client();
    assert!(!client.info().ready && client.info().open_attempts == 0);
    assert!(client.submit(vec![b"ae.metric:42|g".to_vec()]));
    worker.activate();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !client.info().ready {
            client.changed().await;
        }
    })
    .await
    .unwrap();
    let mut bytes = [0; 64];
    let (n, _) = tokio::time::timeout(Duration::from_secs(3), receiver.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..n], b"ae.metric:42|g");
    worker.finish().await;
    assert!(client.info().joined && !client.info().running && !client.info().connected);
    assert!(!client.submit(vec![b"closed".to_vec()]));
}

#[tokio::test]
async fn original_zero_port_callback_resolves_but_never_opens_a_sender() {
    let native: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_transport.json"
    ))
    .unwrap();
    assert_eq!(native["cases"].as_array().unwrap().len(), 7);
    let result = &native["cases"][0]["result"];
    let mut worker = Worker::spawn(Settings {
        target: Config {
            host: Host::Name(result["address"].as_str().unwrap().into()),
            port: 0,
            reconnect_interval: Duration::ZERO,
        },
        ..Default::default()
    })
    .unwrap();
    let mut client = worker.client();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !client.info().ready {
            assert!(client.info().running);
            client.changed().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(client.info().connected, result["socket"].as_bool().unwrap());
    assert_eq!(
        client.info().target.unwrap().port(),
        result["port"].as_u64().unwrap() as u16
    );
    assert_eq!(client.info().open_attempts, 0);
    assert!(client.submit(vec![
            native["cases"][1]["result"]["line"]
                .as_str()
                .unwrap()
                .as_bytes()
                .to_vec()
        ]));
    worker.finish().await;
    assert_eq!(client.info().skipped, 1);
    assert_eq!(client.info().written, 0);
    assert!(client.info().joined);
}
