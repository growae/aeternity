use ae_p2p::noise::{Identity, MAX_PLAINTEXT, Network, Role, connection::Connection};
use ae_types::Hash256;
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

#[test]
#[ignore = "requires the isolated pinned Erlang Noise peer"]
fn native_loopback_worker() {
    let role = std::env::var("AE_NOISE_ROLE").unwrap();
    let ready = std::env::var("AE_NOISE_READY").unwrap();
    let (socket, identity, role) = if role == "initiator" {
        let address = std::env::var("AE_NOISE_ADDRESS").unwrap().parse().unwrap();
        (
            TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap(),
            Identity::from_secret([1; 32]),
            Role::Initiator(Identity::from_secret([2; 32]).public_key()),
        )
    } else {
        assert_eq!(role, "responder");
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        std::fs::write(ready, listener.local_addr().unwrap().port().to_string()).unwrap();
        listener.set_nonblocking(true).unwrap();
        let end = Instant::now() + Duration::from_secs(10);
        let socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < end => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(e) => panic!("accept: {e}"),
            }
        };
        (socket, Identity::from_secret([2; 32]), Role::Responder)
    };
    let net = Network::new(1, Hash256::ZERO, b"ae_dev").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut connection = Connection::handshake(socket, &identity, &net, role, deadline).unwrap();
    let remote_secret = if matches!(role, Role::Initiator(_)) {
        [2; 32]
    } else {
        [1; 32]
    };
    assert_eq!(
        connection.peer(),
        Identity::from_secret(remote_secret).public_key()
    );
    for message in [
        vec![],
        vec![42],
        vec![0, 20, 193, 1],
        vec![7; MAX_PLAINTEXT],
    ] {
        if matches!(role, Role::Initiator(_)) {
            connection.send(&message, deadline).unwrap();
            assert_eq!(connection.receive(deadline).unwrap(), message);
        } else {
            assert_eq!(connection.receive(deadline).unwrap(), message);
            connection.send(&message, deadline).unwrap();
        }
    }
    connection.close();
    assert!(connection.is_closed());
}
