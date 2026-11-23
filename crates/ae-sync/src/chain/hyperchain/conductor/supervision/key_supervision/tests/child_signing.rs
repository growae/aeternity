use super::*;
use crate::{parent_pins, test_support::hyperchain::conductor as fixture};
use ae_codec::api::{self, ApiType};
use ae_hyperchain::signing::Keys;
use ae_tx::{CanonicalTransaction, TransactionLeaf};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
};

fn request(socket: &mut std::net::TcpStream) -> (String, Vec<u8>) {
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
    let head = String::from_utf8(head).unwrap();
    let size = head
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length: "))
        .map_or(0, |v| v.parse::<usize>().unwrap());
    assert!(size < 65536);
    let mut body = vec![0; size];
    socket.read_exact(&mut body).unwrap();
    (head, body)
}
fn reply(socket: &mut std::net::TcpStream, body: &str) {
    write!(
        socket,
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
}
#[tokio::test]
async fn cancelled_child_signing_keeps_exact_nonce_and_old_receipt_without_admission() {
    for panic in [false, true] {
        let node = Node::new(0);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = parent_pins::Node::new(
            parent_pins::Chain::Aeternity,
            &format!("http://{}", listener.local_addr().unwrap()),
            None,
        )
        .unwrap();
        let mut config = fixture::config(&node, false);
        config.parent.pins = Some(PinConfig {
            chain: parent_pins::Chain::Aeternity,
            network: b"parent_network".to_vec(),
            accounts: [12, 13, 14]
                .into_iter()
                .map(|n| (signer(n).public_key(), signer(99).public_key()))
                .collect(),
            signing: Some(parent_pins::posting::Signing {
                network: b"parent_network".to_vec(),
                keys: Keys::new([signer(99)]).into(),
            }),
            agent: Some(Default::default()),
            nodes: vec![endpoint],
            service: Default::default(),
            journal: Default::default(),
        });
        let mut c = fixture::new(&node, config).unwrap();
        c.parent()
            .post_block(crate::test_support::hyperchain::parents::parent(8, 1008))
            .unwrap();
        c.parent().barrier(Duration::from_secs(5)).unwrap();
        let old = c.preset_key_binding().current().unwrap();
        let keys = old.clone();
        let (send, receive) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut payload = vec![];
            for index in 0..3 {
                let mut socket = None;
                until(|| {
                    socket = listener.accept().ok().map(|p| p.0);
                    socket.is_some()
                });
                let mut socket = socket.unwrap();
                socket.set_nonblocking(false).unwrap();
                let (head, body) = request(&mut socket);
                if index == 0 {
                    assert!(head.contains("/next-nonce "));
                    reply(&mut socket, "{\"next_nonce\":1}");
                } else if index == 1 {
                    assert!(head.starts_with("POST /v3/transactions "));
                    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let (_, bytes) = api::decode(body["tx"].as_str().unwrap()).unwrap();
                    let tx = ae_tx::SignedTransaction::<ae_tx::Spend>::decode(&bytes).unwrap();
                    payload = tx.transaction().payload().to_vec();
                    let (gate, release) = keys.pause_for_test();
                    let unknown = panic.then(|| keys.panic_for_test());
                    send.send((gate, release, unknown)).unwrap();
                    reply(
                        &mut socket,
                        &serde_json::json!({"tx_hash":api::encode(ApiType::TxHash,&[42;32])})
                            .to_string(),
                    );
                } else {
                    assert!(head.starts_with("GET /v3/transactions/"));
                    reply(&mut socket,&serde_json::json!({"tx":{"payload":api::encode(ApiType::Bytearray,&payload)},"block_height":7}).to_string());
                }
            }
            listener
        });
        for (i, block) in node.blocks().into_iter().take(6).enumerate() {
            c.importer()
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait()
                .await
                .unwrap();
            if i < 5 {
                let revision = node.db.view().unwrap().status().revision;
                until(|| {
                    c.step(1_000_000, Instant::now()).unwrap();
                    c.take_output();
                    c.pinning().unwrap().status().cursor.revision() >= revision
                });
            }
        }
        let (gate, release, unknown) = receive.recv_timeout(Duration::from_secs(5)).unwrap();
        // This committed batch contains an epoch followed by a key-top.
        // The top checks parent inclusion before submitting the child note.
        // Its event cursor cannot advance while that signature is held.
        let end = Instant::now() + Duration::from_secs(5);
        while old.info().retained_requests < if panic { 3 } else { 2 } {
            assert!(
                Instant::now() < end,
                "keys: {:?}; pinning: {:?}",
                old.info(),
                c.pinning().unwrap().status()
            );
            thread::sleep(Duration::from_millis(1));
        }
        let leader = c.pinning().unwrap().status().state.active.unwrap().leader;
        let nonce = c
            .pool()
            .next_nonce(leader)
            .unwrap()
            .wait()
            .unwrap()
            .unwrap_or_else(|| 1.into());
        let mut stopped = c.services.pinning.take().unwrap().shutdown().unwrap();
        let reports: Vec<_> = stopped
            .outputs
            .iter()
            .chain(stopped.retained.iter())
            .collect();
        let (tx, receipt) = reports
            .iter()
            .flat_map(|r| &r.effects)
            .find_map(|e| match e {
                pinning::Effect::Signing {
                    kind: pinning::ChildKind::Note,
                    transaction,
                    receipt,
                } => Some((transaction.clone(), receipt.clone())),
                _ => None,
            })
            .expect("accepted child signing receipt");
        assert!(receipt.try_result().is_none());
        assert!(
            reports
                .iter()
                .any(|r| r.error == Some(pinning::Error::Cancelled))
        );
        assert!(
            !reports
                .iter()
                .flat_map(|r| &r.effects)
                .any(|e| matches!(e, pinning::Effect::Child { .. }))
        );
        let TransactionLeaf::Spend(note) = tx.leaf() else {
            panic!()
        };
        assert_eq!(note.nonce(), &nonce);
        assert!(
            node.db
                .pinning_children()
                .unwrap()
                .records(Default::default())
                .unwrap()
                .is_empty()
        );
        assert!(
            c.pool()
                .peek(100, None, None)
                .unwrap()
                .wait()
                .unwrap()
                .is_empty()
        );
        c.services
            .owner
            .as_ref()
            .unwrap()
            .request_keys_restart()
            .unwrap();
        release.send(()).unwrap();
        let now = Instant::now();
        until(|| {
            c.services
                .consensus
                .poll_keys(c.services.owner.as_mut().unwrap(), now)
                .unwrap();
            c.services.consensus.take_notice();
            c.services.consensus.status().key_generation == 2
        });
        assert_eq!(gate.try_result(), Some(Ok(())));
        drop(gate);
        if let Some(unknown) = unknown {
            assert_eq!(
                unknown.try_result(),
                Some(Err(preset_keys::Error::Disconnected))
            );
        }
        stopped.poll_children();
        if panic {
            assert_eq!(receipt.try_result(), Some(Err(preset_keys::Error::Stopped)));
        } else {
            let signature = receipt.try_result().unwrap().unwrap();
            let mut message = node.config.profile.network.network_id().to_vec();
            message.extend_from_slice(&tx.encode());
            assert!(ae_crypto::verify_ed25519(
                &note.sender(),
                &message,
                &signature
            ));
        }
        drop(receipt);
        assert_eq!(old.capacity().usage().requests, 0);
        assert_eq!(
            c.preset_key_binding()
                .current()
                .unwrap()
                .info()
                .retained_requests,
            0
        );
        assert!(
            node.db
                .pinning_children()
                .unwrap()
                .records(Default::default())
                .unwrap()
                .is_empty()
        );
        assert!(
            c.pool()
                .peek(100, None, None)
                .unwrap()
                .wait()
                .unwrap()
                .is_empty()
        );
        let listener = server.join().unwrap();
        assert!(listener.accept().is_err());
        assert!(c.shutdown().owner.keys.unwrap().joined);
    }
}
