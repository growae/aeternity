use super::*;
use serde_json::Value;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../tests/fixtures/erlang_noise.json")).unwrap()
}
fn bytes(value: &Value) -> Vec<u8> {
    hex::decode(value.as_str().unwrap()).unwrap()
}
fn network(row: &Value) -> Network {
    Network::new(
        row["protocol_version"].as_u64().unwrap(),
        Hash256(bytes(&row["genesis"]).try_into().unwrap()),
        &bytes(&row["network"]),
    )
    .unwrap()
}
fn pair(row: &Value) -> (Handshake, Handshake) {
    let net = network(row);
    assert_eq!(net.prologue(), bytes(&row["prologue"]));
    let initiator = Identity::from_secret(bytes(&row["initiator_secret"]).try_into().unwrap());
    let responder = Identity::from_secret(bytes(&row["responder_secret"]).try_into().unwrap());
    assert_eq!(
        initiator.public_key().as_ref(),
        bytes(&row["initiator_public"])
    );
    assert_eq!(
        responder.public_key().as_ref(),
        bytes(&row["responder_public"])
    );
    let ir = Role::Initiator(responder.public_key());
    let rr = Role::Responder;
    let ei = bytes(&row["initiator_ephemeral"]);
    let er = bytes(&row["responder_ephemeral"]);
    let i = Handshake::build(
        Handshake::builder(&initiator, &net, &ir)
            .unwrap()
            .fixed_ephemeral_key_for_testing_only(&ei),
        ir,
    )
    .unwrap();
    let r = Handshake::build(
        Handshake::builder(&responder, &net, &rr)
            .unwrap()
            .fixed_ephemeral_key_for_testing_only(&er),
        rr,
    )
    .unwrap();
    (i, r)
}
fn complete(row: &Value) -> (Transport, Transport) {
    let (mut i, mut r) = pair(row);
    let first = i.write().unwrap();
    assert_eq!(first, bytes(&row["messages"][0]));
    r.read(&first).unwrap();
    let second = r.write().unwrap();
    assert_eq!(second, bytes(&row["messages"][1]));
    i.read(&second).unwrap();
    let third = i.write().unwrap();
    assert_eq!(third, bytes(&row["messages"][2]));
    r.read(&third).unwrap();
    assert!(i.is_finished() && r.is_finished());
    assert!(!i.is_my_turn() && !r.is_my_turn());
    let i = i.finish().unwrap();
    let r = r.finish().unwrap();
    assert_eq!(i.peer().as_ref(), bytes(&row["responder_public"]));
    assert_eq!(r.peer().as_ref(), bytes(&row["initiator_public"]));
    assert_eq!(i.handshake_hash().as_slice(), bytes(&row["handshake_hash"]));
    assert_eq!(i.handshake_hash(), r.handshake_hash());
    (i, r)
}

#[test]
fn original_xk_transcripts_identities_hashes_and_bidirectional_ciphertexts_match() {
    for row in fixture()["sessions"].as_array().unwrap() {
        let (mut i, mut r) = complete(row);
        for direction in ["initiator_to_responder", "responder_to_initiator"] {
            let (writer, reader) = if direction == "initiator_to_responder" {
                (&mut i, &mut r)
            } else {
                (&mut r, &mut i)
            };
            for vector in row[direction].as_array().unwrap() {
                let plain = bytes(&vector["plain"]);
                let cipher = writer.seal(&plain).unwrap();
                assert_eq!(cipher, bytes(&vector["cipher"]));
                assert_eq!(record::encode(&cipher).unwrap(), bytes(&vector["packet"]));
                assert_eq!(reader.open(&cipher).unwrap(), plain);
                assert_eq!(vector["tamper_rejected"], true);
            }
        }
    }
}

#[test]
fn original_handshake_failures_close_without_reusable_crypto_state() {
    for row in fixture()["sessions"].as_array().unwrap() {
        for failure in row["failures"].as_array().unwrap() {
            let (mut i, mut r) = pair(row);
            let step = failure["step"].as_u64().unwrap();
            let receiver = if step == 1 {
                &mut r
            } else {
                let m = i.write().unwrap();
                r.read(&m).unwrap();
                let m = r.write().unwrap();
                if step == 2 {
                    &mut i
                } else {
                    i.read(&m).unwrap();
                    &mut r
                }
            };
            let result = receiver.read(&bytes(&failure["bytes"]));
            assert_eq!(failure["rejected"], true);
            assert!(result.is_err());
            assert!(receiver.is_closed());
            assert_eq!(
                receiver.read(&bytes(&row["messages"][step as usize - 1])),
                Err(Error::Closed)
            );
        }
        assert_eq!(row["early_finalize"], true);
        assert!(matches!(pair(row).0.finish(), Err(Error::Incomplete)));
        let initiator = Identity::from_secret(bytes(&row["initiator_secret"]).try_into().unwrap());
        let responder = Identity::from_secret(bytes(&row["responder_secret"]).try_into().unwrap());
        let net = network(row);
        let wrong_net = Network::new(1, Hash256([7; 32]), b"wrong_network").unwrap();
        let mut wrong = Handshake::new(&responder, &wrong_net, Role::Responder).unwrap();
        let mut right =
            Handshake::new(&initiator, &net, Role::Initiator(responder.public_key())).unwrap();
        assert_eq!(row["wrong_prologue"], true);
        assert!(wrong.read(&right.write().unwrap()).is_err());
        let other = Identity::from_secret([99; 32]);
        let mut wrong =
            Handshake::new(&initiator, &net, Role::Initiator(other.public_key())).unwrap();
        let mut right = Handshake::new(&responder, &net, Role::Responder).unwrap();
        assert_eq!(row["wrong_static"], true);
        assert!(right.read(&wrong.write().unwrap()).is_err());
    }
}

#[test]
fn native_tcp_chunking_matches_bounded_incremental_record_decoder() {
    for row in fixture()["sessions"].as_array().unwrap() {
        let (_, mut receiver) = complete(row);
        let mut decoder = record::Decoder::new(MAX_CIPHERTEXT);
        for chunk in row["chunks"].as_array().unwrap() {
            let input = bytes(&chunk["input"]);
            let mut rest = input.as_slice();
            let mut out = vec![];
            while !rest.is_empty() {
                let progress = decoder.push(rest).unwrap();
                assert!(progress.consumed > 0);
                rest = &rest[progress.consumed..];
                if let Some(record) = progress.record {
                    out.push(hex::encode(receiver.open(&record).unwrap()));
                }
            }
            assert_eq!(serde_json::json!(out), chunk["output"]);
        }
        assert!(!decoder.is_pending());
        assert_eq!(decoder.buffered_bytes(), 0);
    }
    for length in [0u16, 1, 15, 65] {
        let mut decoder = record::Decoder::new(64);
        assert!(decoder.push(&length.to_be_bytes()).is_err());
        assert_eq!(decoder.buffered_bytes(), 0);
        assert!(matches!(decoder.push(&[0, 16]), Err(Error::Closed)));
    }
    let mut decoder = record::Decoder::new(MAX_CIPHERTEXT);
    assert_eq!(decoder.push(&[0]).unwrap().consumed, 1);
    assert!(decoder.is_pending());
    assert_eq!(decoder.push(&[]).unwrap().consumed, 0);
    assert!(decoder.push(&[16, 1, 2, 3]).unwrap().record.is_none());
    assert_eq!(decoder.buffered_bytes(), 5);
    decoder.close();
    assert_eq!(decoder.buffered_bytes(), 0);
}

#[test]
fn transport_limits_tamper_replay_and_nonce_exhaustion_are_terminal() {
    let row = &fixture()["sessions"][0];
    let (mut i, mut r) = complete(row);
    assert_eq!(
        i.seal(&vec![0; MAX_PLAINTEXT + 1]),
        Err(Error::ResourceLimit)
    );
    let plain = vec![42; MAX_PLAINTEXT];
    let cipher = i.seal(&plain).unwrap();
    assert_eq!(r.open(&cipher).unwrap(), plain);
    assert_eq!(r.open(&cipher), Err(Error::Crypto));
    assert!(r.is_closed());
    assert_eq!(r.open(&cipher), Err(Error::Closed));
    let (mut i, mut r) = complete(row);
    let mut cipher = i.seal(b"authenticated").unwrap();
    cipher[0] ^= 1;
    assert_eq!(r.open(&cipher), Err(Error::Crypto));
    assert!(r.is_closed());
    let (mut i, _) = complete(row);
    i.state.as_mut().unwrap().set_receiving_nonce(u64::MAX);
    assert!(
        i.open(&bytes(&row["responder_to_initiator"][0]["cipher"]))
            .is_err()
    );
    assert!(i.is_closed());
    let (mut i, _) = complete(row);
    assert_eq!(i.open(&[0; 15]), Err(Error::InvalidRecord));
    assert_eq!(i.seal(&[]), Err(Error::Closed));
    assert!(Network::new(1, Hash256::ZERO, &vec![0; MAX_NETWORK_ID + 1]).is_err());
    let identity = Identity::generate().unwrap();
    let other = Identity::generate().unwrap();
    assert_ne!(identity.public_key(), other.public_key());
    assert!(!format!("{identity:?}").contains("secret"));
}

#[test]
fn real_loopback_handshake_encrypted_records_and_joined_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server_id = Identity::from_secret([2; 32]);
    let server_public = server_id.public_key();
    let client_id = Identity::from_secret([1; 32]);
    let client_public = client_id.public_key();
    let worker = std::thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let net = Network::new(1, Hash256::ZERO, b"ae_dev").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut connection =
            connection::Connection::handshake(socket, &server_id, &net, Role::Responder, deadline)
                .unwrap();
        assert_eq!(connection.peer(), client_public);
        for expected in [vec![], b"hello".to_vec(), vec![7; MAX_PLAINTEXT]] {
            assert_eq!(connection.receive(deadline).unwrap(), expected);
            connection.send(&expected, deadline).unwrap();
        }
        connection.close();
        assert!(connection.is_closed());
    });
    let net = Network::new(1, Hash256::ZERO, b"ae_dev").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut connection = connection::Connection::handshake(
        TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap(),
        &client_id,
        &net,
        Role::Initiator(server_public),
        deadline,
    )
    .unwrap();
    assert!(matches!(
        connection.send(&vec![0; MAX_PLAINTEXT + 1], deadline),
        Err(connection::Error::Noise(Error::ResourceLimit))
    ));
    assert!(!connection.is_closed());
    for message in [vec![], b"hello".to_vec(), vec![7; MAX_PLAINTEXT]] {
        connection.send(&message, deadline).unwrap();
        assert_eq!(connection.receive(deadline).unwrap(), message);
    }
    assert!(connection.receive(deadline).is_err());
    assert!(connection.is_closed());
    worker.join().unwrap();
}

#[test]
fn real_handshake_deadline_and_partial_record_eof_close_socket() {
    for bytes in [vec![], vec![0], vec![0, 48, 1, 2, 3]] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let writer = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream.write_all(&bytes).unwrap();
        });
        let (socket, _) = listener.accept().unwrap();
        let identity = Identity::from_secret([2; 32]);
        let network = Network::new(1, Hash256::ZERO, b"ae_dev").unwrap();
        assert!(
            connection::Connection::handshake(
                socket,
                &identity,
                &network,
                Role::Responder,
                Instant::now() + Duration::from_secs(2)
            )
            .is_err()
        );
        writer.join().unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (socket, _) = listener.accept().unwrap();
    let identity = Identity::from_secret([2; 32]);
    let network = Network::new(1, Hash256::ZERO, b"ae_dev").unwrap();
    assert!(matches!(
        connection::Connection::handshake(
            socket,
            &identity,
            &network,
            Role::Responder,
            Instant::now() + Duration::from_millis(30)
        ),
        Err(connection::Error::Timeout)
    ));
    drop(client);
}

#[test]
fn native_low_order_dh_rejections_are_preserved_by_the_resolver() {
    use snow::resolvers::CryptoResolver;
    let mut dh = resolver::Resolver
        .resolve_dh(&snow::params::DHChoice::Curve25519)
        .unwrap();
    let mut key = [0; 32];
    key[31] = 7;
    dh.set(&key);
    for row in fixture()["low_order"].as_array().unwrap() {
        assert_eq!(row["rejected"], true);
        let mut out = [0; 32];
        assert!(dh.dh(&bytes(&row["public"]), &mut out).is_err());
    }
    assert!(dh.dh(&[0; 31], &mut [0; 32]).is_err());
    let valid = Identity::from_secret([8; 32]);
    assert!(dh.dh(valid.public_key().as_ref(), &mut [0; 31]).is_err());
    let mut out = [0; 32];
    dh.dh(valid.public_key().as_ref(), &mut out).unwrap();
    assert_ne!(out, [0; 32]);
}

#[test]
fn authenticated_connection_failure_and_expired_send_retire_both_directions() {
    for tamper in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let mut raw = socket.try_clone().unwrap();
            let id = Identity::from_secret([2; 32]);
            let net = Network::new(1, Hash256::ZERO, b"ae_dev").unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut peer =
                connection::Connection::handshake(socket, &id, &net, Role::Responder, deadline)
                    .unwrap();
            if tamper {
                raw.write_all(&record::encode(&[0; 16]).unwrap()).unwrap();
            }
            assert!(peer.receive(deadline).is_err());
            assert!(peer.is_closed());
        });
        let socket = TcpStream::connect(address).unwrap();
        let id = Identity::from_secret([1; 32]);
        let net = Network::new(1, Hash256::ZERO, b"ae_dev").unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut peer = connection::Connection::handshake(
            socket,
            &id,
            &net,
            Role::Initiator(Identity::from_secret([2; 32]).public_key()),
            deadline,
        )
        .unwrap();
        if tamper {
            assert!(matches!(
                peer.receive(deadline),
                Err(connection::Error::Noise(Error::Crypto))
            ));
        } else {
            assert!(matches!(
                peer.send(b"expired", Instant::now() - Duration::from_secs(1)),
                Err(connection::Error::Timeout)
            ));
        }
        assert!(peer.is_closed());
        assert!(matches!(
            peer.send(b"no reuse", deadline),
            Err(connection::Error::Noise(Error::Closed))
        ));
        worker.join().unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let mut socket = TcpStream::connect(address).unwrap();
        if socket.write_all(&[0, 48]).is_ok() {
            for _ in 0..20 {
                std::thread::sleep(Duration::from_millis(10));
                if socket.write_all(&[0]).is_err() {
                    break;
                }
            }
        }
    });
    let (socket, _) = listener.accept().unwrap();
    let id = Identity::from_secret([2; 32]);
    let net = Network::new(1, Hash256::ZERO, b"ae_dev").unwrap();
    assert!(matches!(
        connection::Connection::handshake(
            socket,
            &id,
            &net,
            Role::Responder,
            Instant::now() + Duration::from_millis(50)
        ),
        Err(connection::Error::Timeout)
    ));
    worker.join().unwrap();
}
