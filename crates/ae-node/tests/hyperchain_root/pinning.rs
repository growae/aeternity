use super::*;
use ae_codec::api::{self, ApiType};
use ae_hyperchain::signing::Keys;
use ae_sync::{chain::hyperchain::pinning, parent_pins};
use ae_tx::{SignedTransaction, Spend};
use common::within;
use serde_json::json;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};
struct Wallet {
    node: parent_pins::Node,
    mode: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    posted: Arc<Mutex<Vec<SignedTransaction<Spend>>>>,
    requests: Arc<AtomicUsize>,
    worker: Option<thread::JoinHandle<()>>,
}
fn request(s: &mut TcpStream) -> (String, serde_json::Value) {
    s.set_nonblocking(false).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut data = vec![];
    let mut buf = [0; 2048];
    loop {
        let n = s.read(&mut buf).unwrap();
        assert!(n > 0);
        data.extend_from_slice(&buf[..n]);
        assert!(data.len() < 65536);
        if let Some(i) = data.windows(4).position(|p| p == b"\r\n\r\n") {
            let head = std::str::from_utf8(&data[..i]).unwrap();
            let len = head
                .lines()
                .find_map(|s| s.strip_prefix("Content-Length: "))
                .map_or(0, |s| s.parse::<usize>().unwrap());
            if data.len() >= i + 4 + len {
                return (
                    head.to_owned(),
                    if len == 0 {
                        serde_json::Value::Null
                    } else {
                        serde_json::from_slice(&data[i + 4..i + 4 + len]).unwrap()
                    },
                );
            }
        }
    }
}
impl Wallet {
    fn new() -> Self {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.set_nonblocking(true).unwrap();
        let node = parent_pins::Node::new(
            parent_pins::Chain::Aeternity,
            &format!("http://{}", l.local_addr().unwrap()),
            None,
        )
        .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mode = Arc::new(AtomicUsize::new(3));
        let posted = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(AtomicUsize::new(0));
        let (end, m, txs, count) = (stop.clone(), mode.clone(), posted.clone(), requests.clone());
        let worker = thread::spawn(move || {
            while !end.load(Ordering::Acquire) {
                let Ok((mut s, _)) = l.accept() else {
                    thread::sleep(Duration::from_millis(1));
                    continue;
                };
                let (head, body) = request(&mut s);
                count.fetch_add(1, Ordering::AcqRel);
                let response = if head.starts_with("GET /v3/key-blocks/") {
                    let path = head.split_whitespace().nth(1).unwrap();
                    let h = if path == "/v3/key-blocks/current" {
                        16
                    } else {
                        path.strip_prefix("/v3/key-blocks/height/")
                            .unwrap()
                            .parse::<i64>()
                            .unwrap()
                    };
                    if h < 0 {
                        let _ = s.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}");
                        continue;
                    }
                    let h = h as u64;
                    json!({"hash":api::encode(ApiType::KeyBlockHash,&key(1000+h)),"prev_key_hash":api::encode(ApiType::KeyBlockHash,&key(999+h)),"height":h,"time":h*100})
                } else if head.starts_with("POST /v3/transactions ") {
                    let (ApiType::Transaction, bytes) =
                        api::decode(body["tx"].as_str().unwrap()).unwrap()
                    else {
                        panic!()
                    };
                    let tx = SignedTransaction::<Spend>::decode(&bytes).unwrap();
                    let hash = api::encode(ApiType::TxHash, &tx.hash().0);
                    txs.lock().unwrap().push(tx);
                    while m.load(Ordering::Acquire) == 3 && !end.load(Ordering::Acquire) {
                        thread::sleep(Duration::from_millis(1));
                    }
                    if end.load(Ordering::Acquire) || m.load(Ordering::Acquire) == 2 {
                        continue;
                    }
                    json!({"tx_hash":hash})
                } else if head.contains("/next-nonce ") {
                    json!({"next_nonce":1})
                } else if head.starts_with("GET /v3/transactions/") {
                    let txs = txs.lock().unwrap();
                    let tx = txs.last().unwrap();
                    json!({"tx":{"payload":api::encode(ApiType::Bytearray,tx.transaction().payload())},"block_height":if m.load(Ordering::Acquire)==1{-1}else{7}})
                } else {
                    panic!("{head}")
                };
                let bytes = serde_json::to_vec(&response).unwrap();
                let _ = write!(
                    s,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    bytes.len()
                );
                let _ = s.write_all(&bytes);
            }
        });
        Self {
            node,
            mode,
            stop,
            posted,
            requests,
            worker: Some(worker),
        }
    }
}
impl Drop for Wallet {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.mode.store(0, Ordering::Release);
        let joined = self.worker.take().unwrap().join();
        if !thread::panicking() {
            joined.unwrap();
        }
    }
}

#[tokio::test]
async fn root_shutdown_and_child_failure_retain_an_uncertain_parent_broadcast_once() {
    for failed in [false, true] {
        let node = Node::new(0);
        let wallet = Wallet::new();
        let input = inputs(&node, fixed(121), |cfg| {
            cfg.parent.pins = Some(conductor::PinConfig {
                journal: Default::default(),
                chain: parent_pins::Chain::Aeternity,
                network: b"parent_network".to_vec(),
                accounts: [12, 13, 14]
                    .into_iter()
                    .map(|n| (signer(n).public_key(), signer(99).public_key()))
                    .collect(),
                nodes: vec![wallet.node.clone()],
                service: parent_pins::Config {
                    request_timeout: Duration::from_secs(10),
                    lookup_timeout: Duration::from_secs(10),
                    ..Default::default()
                },
                signing: Some(parent_pins::posting::Signing {
                    network: b"parent_network".to_vec(),
                    keys: Keys::new([signer(99)]).into(),
                }),
                agent: Some(Default::default()),
            });
        });
        let importer = input.conductor.importer();
        assert!(input.conductor.parent_pins().unwrap().durable_posts());
        let agent = input.conductor.pinning().unwrap();
        let running = launch(input, config(), Clock::new());
        let mut h = running.handle();
        ready(&mut h).await;
        let mut consumed = vec![];
        for (i, block) in node.blocks().into_iter().take(6).enumerate() {
            within(
                importer
                    .import_block(block.encode().unwrap(), 1_000_000)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
            node.check(i);
            if i < 5 {
                let revision = node.db.view().unwrap().status().revision;
                within(async {
                    while agent.status().cursor.revision() < revision {
                        while let Some(o) = h.try_output() {
                            consumed.push(o);
                        }
                        assert!(h.status().failure.is_none());
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await;
            }
        }
        within(async {
            while wallet.posted.lock().unwrap().is_empty() {
                assert!(h.status().failure.is_none());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        if failed {
            h.workers().unwrap().shutdown();
        } else {
            h.request_shutdown();
        }
        let exit = within(running.wait()).await.unwrap();
        clean(&exit);
        assert_eq!(exit.status.failure.is_some(), failed);
        let report = exit.producer.as_ref().unwrap().as_ref().unwrap();
        let pin = report.pinning.as_ref().unwrap().as_ref().unwrap();
        let reports = pin.retained.iter().chain(&pin.outputs).chain(
            consumed
                .iter()
                .chain(&exit.retained)
                .chain(&report.queued_outputs)
                .chain(report.output.iter())
                .filter_map(|o| match o {
                    Output::Pinning(r) => Some(r.as_ref()),
                    _ => None,
                }),
        );
        let effects: Vec<_> = reports.flat_map(|r| &r.effects).collect();
        assert_eq!(
            effects
                .iter()
                .filter(|e| matches!(
                    e,
                    pinning::Effect::Parent(parent_pins::posting::Outcome::Unknown { .. })
                ))
                .count(),
            1
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, pinning::Effect::Child { .. }))
        );
        assert_eq!(wallet.posted.lock().unwrap().len(), 1);
        assert_eq!(wallet.requests.load(Ordering::Acquire), 2);
        let journal = node
            .db
            .parent_posts()
            .unwrap()
            .records(Default::default())
            .unwrap();
        assert_eq!(journal.len(), 1);
        assert!(journal[0].response.is_none());
        assert_eq!(
            journal[0].transaction,
            wallet.posted.lock().unwrap()[0].encode()
        );
        assert!(!agent.status().running);
    }
}
