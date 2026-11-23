use super::*;
use ae_codec::api::{self, ApiType};
use ae_hyperchain::signing::Keys;
use ae_sync::{chain::hyperchain::pinning, parent_pins};
use ae_tx::{SignedTransaction, Spend, TransactionLeaf};
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
        let mode = Arc::new(AtomicUsize::new(0));
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
fn configured(
    node: &Node,
    wallet: &Wallet,
    configure: impl FnOnce(&mut conductor::Config),
) -> Conductor<Otp26> {
    let mut cfg = configuration(node, wallet);
    configure(&mut cfg);
    let c = new(node, cfg).unwrap();
    c.parent().post_block(parent(8, 1008)).unwrap();
    c.parent().barrier(Duration::from_secs(5)).unwrap();
    c
}
fn configuration(node: &Node, wallet: &Wallet) -> conductor::Config {
    configuration_at(node, &wallet.node)
}
fn configuration_at(node: &Node, endpoint: &parent_pins::Node) -> conductor::Config {
    let mut cfg = config(node, false);
    cfg.parent.pins = Some(conductor::PinConfig {
        journal: Default::default(),
        chain: parent_pins::Chain::Aeternity,
        network: b"parent_network".to_vec(),
        accounts: [12, 13, 14]
            .into_iter()
            .map(|n| (signer(n).public_key(), signer(99).public_key()))
            .collect(),
        nodes: vec![endpoint.clone()],
        service: parent_pins::Config {
            request_timeout: Duration::from_secs(2),
            lookup_timeout: Duration::from_secs(3),
            ..Default::default()
        },
        signing: Some(parent_pins::posting::Signing {
            network: b"parent_network".to_vec(),
            keys: Keys::new([signer(99)]),
        }),
        agent: Some(pinning::Config::default()),
    });
    cfg
}
fn drain(c: &mut Conductor<Otp26>, reports: &mut Vec<pinning::Report>) {
    for _ in 0..1024 {
        let progress = c.step(1_000_000, Instant::now()).unwrap();
        while let Some(output) = c.take_output() {
            if let Output::Pinning(report) = output {
                assert!(report.error.is_none(), "{report:?}");
                reports.push(*report);
            }
        }
        if progress != conductor::Progress::Output {
            return;
        }
    }
    panic!("diagnostic output did not quiesce");
}
fn settled(c: &mut Conductor<Otp26>, reports: &mut Vec<pinning::Report>, revision: u64) {
    let end = Instant::now() + Duration::from_secs(10);
    while c.pinning().unwrap().status().cursor.revision() < revision {
        assert!(Instant::now() < end, "{:?}", c.pinning().unwrap().status());
        drain(c, reports);
        thread::sleep(Duration::from_millis(1));
    }
    drain(c, reports);
}
async fn prefix(
    node: &Node,
    c: &mut Conductor<Otp26>,
    reports: &mut Vec<pinning::Report>,
    count: usize,
) {
    for (i, block) in node.blocks().into_iter().take(count).enumerate() {
        within(
            c.importer()
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        node.check(i);
        settled(c, reports, node.db.view().unwrap().status().revision);
    }
}
fn children(
    reports: &[pinning::Report],
) -> Vec<(pinning::ChildKind, Arc<ae_mempool::pool::Signed>)> {
    reports
        .iter()
        .flat_map(|r| r.effects.iter())
        .filter_map(|e| match e {
            pinning::Effect::Child {
                kind,
                transaction,
                result,
            } => {
                assert!(
                    matches!(result, pinning::ChildResult::Known(Ok(_))),
                    "{result:?}"
                );
                Some((*kind, transaction.clone()))
            }
            _ => None,
        })
        .collect()
}
#[tokio::test]
async fn conductor_agent_posts_parent_and_admits_note_and_exact_height_proof() {
    for index in [0, 2, 4] {
        let node = Node::new(index);
        let wallet = Wallet::new();
        let mut c = configured(&node, &wallet, |_| {});
        let mut reports = vec![];
        prefix(&node, &mut c, &mut reports, 9).await;
        let posted = wallet.posted.lock().unwrap();
        assert_eq!(posted.len(), 1);
        let parent = &posted[0];
        assert_eq!(parent.transaction().sender(), signer(99).public_key());
        let pin = ae_hyperchain::pin::decode_parent(parent.transaction().payload(), 4096).unwrap();
        assert_eq!(pin.epoch, 1.into());
        assert_eq!(pin.height, 3.into());
        assert_eq!(
            pin.block_hash,
            node.db.view().unwrap().canonical_key(3).unwrap().unwrap().0
        );
        let child = children(&reports);
        assert_eq!(child.len(), 2);
        assert_eq!(child[0].0, pinning::ChildKind::Note);
        assert_eq!(child[1].0, pinning::ChildKind::Proof);
        let stored = node
            .db
            .pinning_children()
            .unwrap()
            .records(Default::default())
            .unwrap();
        assert_eq!(stored.len(), 2);
        for (_, tx) in &child {
            assert!(stored.iter().any(|r| r.transaction == tx.encode()
                && matches!(
                    r.receipt,
                    Some(ae_storage::pinning_children::Receipt::Submitted(_))
                )));
        }
        let parent_hash = api::encode(ApiType::TxHash, &parent.hash().0);
        drop(posted);
        let TransactionLeaf::Spend(note) = child[0].1.transaction().leaf() else {
            panic!()
        };
        assert_eq!(
            note.payload(),
            [b"pin".as_slice(), parent_hash.as_bytes()].concat()
        );
        let TransactionLeaf::Contract(proof) = child[1].1.transaction().leaf() else {
            panic!()
        };
        assert_eq!(proof.nonce(), &note.nonce().add(&1.into()));
        let proof_event = reports
            .iter()
            .find(|r| {
                r.effects.iter().any(|e| {
                    matches!(
                        e,
                        pinning::Effect::Child {
                            kind: pinning::ChildKind::Proof,
                            ..
                        }
                    )
                })
            })
            .unwrap();
        assert_eq!(
            proof_event.event,
            ae_hyperchain::pinning::Event::Top { height: 5.into() }
        );
        assert!(!c.pinning().unwrap().status().state.enabled);
        assert_eq!(wallet.requests.load(Ordering::Acquire), 3);
        for (_, tx) in &child {
            assert!(
                c.pool()
                    .peek(100, None, None)
                    .unwrap()
                    .wait()
                    .unwrap()
                    .iter()
                    .any(|t| t.hash() == tx.hash())
            );
        }
        let handle = c.pinning().unwrap();
        let report = shutdown(c);
        assert!(!handle.status().running);
        assert!(report.pinning.unwrap().is_ok());
    }
}
#[tokio::test]
async fn pending_parent_bows_out_at_last_minus_one_without_child_transactions() {
    let node = Node::new(0);
    let wallet = Wallet::new();
    wallet.mode.store(1, Ordering::Release);
    let mut c = configured(&node, &wallet, |_| {});
    let mut reports = vec![];
    prefix(&node, &mut c, &mut reports, 9).await;
    assert_eq!(wallet.posted.lock().unwrap().len(), 1);
    assert!(children(&reports).is_empty());
    assert!(!c.pinning().unwrap().status().state.enabled);
    assert_eq!(wallet.requests.load(Ordering::Acquire), 6);
    shutdown(c);
}
#[tokio::test]
async fn uncertain_parent_is_reported_once_and_shutdown_retains_blocked_broadcast() {
    for mode in [2, 3] {
        let node = Node::new(0);
        let wallet = Wallet::new();
        wallet.mode.store(mode, Ordering::Release);
        let mut c = configured(&node, &wallet, |_| {});
        let mut reports = vec![];
        prefix(&node, &mut c, &mut reports, 5).await;
        within(
            c.importer()
                .import_block(node.blocks()[5].encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        wait(|| wallet.posted.lock().unwrap().len() == 1);
        if mode == 2 {
            settled(
                &mut c,
                &mut reports,
                node.db.view().unwrap().status().revision,
            );
            assert!(reports.iter().flat_map(|r| &r.effects).any(|e| matches!(
                e,
                pinning::Effect::Parent(parent_pins::posting::Outcome::Unknown { .. })
            )));
            assert!(children(&reports).is_empty());
            assert_eq!(wallet.requests.load(Ordering::Acquire), 2);
            shutdown(c);
        } else {
            let at = Instant::now();
            c.stop_production().unwrap();
            assert!(at.elapsed() < Duration::from_millis(100));
            let mut report = shutdown(c);
            assert!(at.elapsed() < Duration::from_secs(2));
            let exit = report.pinning.take().unwrap().unwrap();
            assert!(
                exit.retained
                    .iter()
                    .chain(exit.outputs.iter())
                    .flat_map(|r| &r.effects)
                    .any(|e| matches!(
                        e,
                        pinning::Effect::Parent(parent_pins::posting::Outcome::Unknown { .. })
                    ))
            );
            assert_eq!(wallet.requests.load(Ordering::Acquire), 2);
        }
    }
}

#[tokio::test]
async fn bounded_reports_retain_exact_events_without_reposting_when_consumers_pause() {
    for stop in [false, true] {
        let node = Node::new(0);
        let wallet = Wallet::new();
        let mut c = configured(&node, &wallet, |cfg| {
            cfg.parent
                .pins
                .as_mut()
                .unwrap()
                .agent
                .as_mut()
                .unwrap()
                .outputs = 1;
        });
        let mut reports = vec![];
        prefix(&node, &mut c, &mut reports, 5).await;
        let before = c.pinning().unwrap().status().cursor;
        within(
            c.importer()
                .import_block(node.blocks()[5].encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        // Complete the current-epoch query at key 3 before importing micro 4.
        // Native Top resolves a key at the selected height; micro 4 alone has
        // no key 4 yet. That independent query failure is not output pressure.
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            let status = c.pinning().unwrap().status();
            if status.state.active.as_ref().is_some_and(|a| a.child_note) {
                break;
            }
            assert!(
                status.failure.is_none() && Instant::now() < end,
                "{status:?}; requests {}; posted {}",
                wallet.requests.load(Ordering::Acquire),
                wallet.posted.lock().unwrap().len()
            );
            thread::sleep(Duration::from_millis(1));
        }
        for block in node.blocks().into_iter().skip(6).take(3) {
            within(
                c.importer()
                    .import_block(block.encode().unwrap(), 1_000_000)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
        }
        let blocked = c.pinning().unwrap().status();
        assert_eq!(blocked.cursor, before);
        assert!(blocked.state.enabled);
        assert_eq!(wallet.posted.lock().unwrap().len(), 1);
        assert_eq!(wallet.requests.load(Ordering::Acquire), 3);
        if stop {
            let mut result = shutdown(c);
            let exit = result.pinning.take().unwrap().unwrap();
            assert_eq!(exit.outputs.len(), 1);
            assert!(exit.retained.is_some());
            assert!(exit.pending.is_some());
            let all: Vec<_> = exit.outputs.into_iter().chain(exit.retained).collect();
            assert_eq!(children(&all).len(), 1);
        } else {
            settled(
                &mut c,
                &mut reports,
                node.db.view().unwrap().status().revision,
            );
            assert_eq!(children(&reports).len(), 2);
            assert_eq!(wallet.posted.lock().unwrap().len(), 1);
            assert_eq!(wallet.requests.load(Ordering::Acquire), 3);
            shutdown(c);
        }
    }
}

#[tokio::test]
async fn disabled_unmapped_and_failed_startup_do_not_leak_owners_or_post_transactions() {
    let node = Node::new(0);
    let wallet = Wallet::new();
    let mut invalid = configuration(&node, &wallet);
    invalid
        .parent
        .pins
        .as_mut()
        .unwrap()
        .agent
        .as_mut()
        .unwrap()
        .outputs = 0;
    assert!(matches!(
        new(&node, invalid),
        Err(conductor::Error::Pinning(pinning::Error::Configuration))
    ));
    let c = configured(&node, &wallet, |cfg| {
        cfg.parent.pins.as_mut().unwrap().agent = None
    });
    assert!(c.pinning().is_none());
    shutdown(c);
    let mut c = configured(&node, &wallet, |cfg| {
        cfg.parent.pins.as_mut().unwrap().accounts.clear()
    });
    let mut reports = vec![];
    prefix(&node, &mut c, &mut reports, 9).await;
    assert!(!reports.is_empty());
    assert!(
        reports
            .iter()
            .all(|r| r.effects.is_empty() && !r.state.enabled)
    );
    assert_eq!(wallet.requests.load(Ordering::Acquire), 0);
    let stale = c.pinning().unwrap();
    shutdown(c);
    assert!(!stale.status().running);
    // A replacement starts at the current cursor and never replays old epochs.
    let mut next = configured(&node, &wallet, |_| {});
    stale.request_stop();
    drain(&mut next, &mut vec![]);
    assert!(next.pinning().unwrap().status().running);
    assert_eq!(wallet.requests.load(Ordering::Acquire), 0);
    shutdown(next);
}

#[tokio::test]
async fn event_quota_and_rollback_fail_closed_without_replaying_external_effects() {
    for mode in [0, 1, 2] {
        let node = Node::new(0);
        let wallet = Wallet::new();
        let mut c = configured(&node, &wallet, |cfg| {
            let agent = cfg.parent.pins.as_mut().unwrap().agent.as_mut().unwrap();
            if mode == 0 {
                agent.maximum_event_bytes = 1;
            }
            if mode == 1 {
                agent.maximum_events = 1;
            }
        });
        let mut reports = vec![];
        if mode != 0 {
            prefix(&node, &mut c, &mut reports, 5).await;
        }
        if mode == 2 {
            node.db
                .rollback(
                    ae_chain::repository::rollback::Target::Height(1),
                    Default::default(),
                )
                .unwrap();
        } else {
            within(
                c.importer()
                    .import_block(
                        node.blocks()[if mode == 0 { 0 } else { 5 }]
                            .encode()
                            .unwrap(),
                        1_000_000,
                    )
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
        }
        let agent = c.pinning().unwrap();
        wait(|| !agent.status().running);
        let failure = agent.status().failure.unwrap();
        assert!(if mode == 2 {
            matches!(failure, pinning::Error::Rollback(_))
        } else {
            failure == pinning::Error::ResourceLimit
        });
        assert_eq!(
            c.step(1_000_000, Instant::now()),
            Err(conductor::Error::Pinning(failure))
        );
        assert_eq!(wallet.requests.load(Ordering::Acquire), 0);
        shutdown(c);
    }
}

#[tokio::test]
async fn event_gap_while_parent_post_is_pending_recovers_durable_events_without_reposting() {
    let node = Node::with_events(ae_storage::events::Config {
        records: 1,
        bytes: 16 << 20,
    });
    let wallet = Wallet::new();
    let mut c = configured(&node, &wallet, |_| {});
    let mut reports = vec![];
    prefix(&node, &mut c, &mut reports, 5).await;
    let before = c.pinning().unwrap().status().cursor;
    wallet.mode.store(3, Ordering::Release);
    for (i, block) in node.blocks().into_iter().enumerate().skip(5).take(3) {
        within(
            c.importer()
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        if i == 5 {
            wait(|| wallet.posted.lock().unwrap().len() == 1);
        }
    }
    assert!(matches!(
        node.db.events().after(&before, 1),
        Err(ae_storage::events::Error::Gap { .. })
    ));
    wallet.mode.store(0, Ordering::Release);
    // Observe the independently owned pinning worker directly. The conductor
    // keeps its separate gap-failure policy and is shut down without stepping.
    let head = node.db.view().unwrap().status().revision;
    let handle = c.pinning().unwrap();
    wait(|| handle.status().cursor.revision() == head || handle.status().failure.is_some());
    assert!(c.pinning().unwrap().status().running);
    assert!(c.pinning().unwrap().status().failure.is_none());
    assert_eq!(wallet.posted.lock().unwrap().len(), 1);
    let exit = shutdown(c).pinning.take().unwrap().unwrap();
    reports.extend(exit.outputs.into_iter().chain(exit.retained));
    assert!(reports.iter().flat_map(|r| &r.effects).any(|e| matches!(
        e,
        pinning::Effect::Parent(parent_pins::posting::Outcome::Submitted { .. })
    )));
    assert_eq!(children(&reports).len(), 1);
    assert_eq!(wallet.requests.load(Ordering::Acquire), 3);
}

#[path = "pinning/utilities.rs"]
mod utilities;

#[path = "pinning/children.rs"]
mod durable_children;

#[path = "pinning/checkpoint.rs"]
mod checkpoint;

#[path = "pinning/pool_recovery.rs"]
mod pool_recovery;
#[path = "pinning/supervision.rs"]
mod supervision;
