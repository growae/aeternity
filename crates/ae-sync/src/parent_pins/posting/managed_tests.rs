use super::*;
use crate::{
    bindings, preset_keys,
    test_support::hyperchain::{Node as Child, conductor::signer},
};
use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::Duration,
};

fn until(mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !f() {
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn interrupted_parent_signing_retains_old_receipt_without_broadcast_or_replay() {
    for deadline in [false, true] {
        for panic in [false, true] {
            let child = Child::new(0);
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let worker =
                preset_keys::Worker::spawn(Keys::new([signer(7)]), Default::default()).unwrap();
            let old = worker.client();
            let (publisher, binding) = bindings::Owner::new(&child.db);
            publisher.install(old.clone(), 1);
            let service = super::super::Service::spawn_with_signing(
                vec![
                    Node::new(
                        Chain::Aeternity,
                        &format!("http://{}", listener.local_addr().unwrap()),
                        None,
                    )
                    .unwrap(),
                ],
                super::super::Config {
                    lookup_timeout: Duration::from_millis(if deadline { 250 } else { 5000 }),
                    ..Default::default()
                },
                Some(Signing {
                    network: b"parent_network".to_vec(),
                    keys: SigningKeys::Worker(binding.clone()),
                }),
            )
            .unwrap();
            let (gate, release) = old.pause_for_test();
            let unknown = panic.then(|| old.panic_for_test());
            let mut request = service
                .client()
                .post(Post {
                    pin: pin::Parent {
                        epoch: 1.into(),
                        height: 3.into(),
                        block_hash: vec![42; 32],
                    },
                    account: signer(7).public_key(),
                    amount: 1.into(),
                    fee: 1.into(),
                })
                .unwrap();
            let mut socket = None;
            until(|| {
                socket = listener.accept().ok().map(|p| p.0);
                socket.is_some()
            });
            let mut socket = socket.unwrap();
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut head = vec![];
            loop {
                let mut byte = [0];
                socket.read_exact(&mut byte).unwrap();
                head.push(byte[0]);
                assert!(head.len() < 4096);
                if head.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            assert!(String::from_utf8(head).unwrap().contains("/next-nonce "));
            let body = b"{\"next_nonce\":7}";
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            socket.write_all(body).unwrap();
            drop(socket);
            until(|| request.progress.signing.lock().unwrap().is_some());
            if !deadline {
                request.cancel();
            }
            let mut outcome = None;
            until(|| {
                outcome = request.try_result();
                outcome.is_some()
            });
            let mut outcome = outcome.unwrap();
            match &outcome {
                Outcome::NotSubmitted(Failure::SigningInterrupted { cause, receipt }) => {
                    assert_eq!(
                        **cause,
                        Failure::Local(if deadline {
                            Error::Deadline
                        } else {
                            Error::Closed
                        })
                    );
                    assert!(receipt.try_result().is_none());
                }
                other => panic!("{other:?}"),
            }
            assert!(listener.accept().is_err());
            publisher.withdraw();
            worker.request_shutdown();
            release.send(()).unwrap();
            assert!(worker.retire().joined);
            assert_eq!(gate.try_result(), Some(Ok(())));
            if let Some(unknown) = unknown {
                assert_eq!(
                    unknown.try_result(),
                    Some(Err(preset_keys::Error::Disconnected))
                );
            }
            drop(gate);
            let replacement =
                preset_keys::Worker::spawn_with_capacity(Keys::new([signer(7)]), old.capacity())
                    .unwrap();
            publisher.install(replacement.client(), 2);
            outcome.poll_signing();
            let Outcome::NotSubmitted(Failure::SigningInterrupted { receipt, .. }) = outcome else {
                panic!()
            };
            assert!(matches!(*receipt, preset_keys::Observed::Known(_)));
            assert_eq!(receipt.try_result().unwrap().is_ok(), !panic);
            if panic {
                assert_eq!(receipt.try_result(), Some(Err(preset_keys::Error::Stopped)));
            }
            service.shutdown().unwrap();
            assert!(listener.accept().is_err());
            assert_eq!(replacement.client().info().retained_requests, 0);
            assert_eq!(old.capacity().usage().requests, 0);
            publisher.close();
            assert!(replacement.retire().joined);
        }
    }
}
