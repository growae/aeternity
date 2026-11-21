use super::chain_sync::{seed, synchronize_with_importer};
use super::*;
use ae_sync::chain;
use tokio::sync::mpsc;

struct Fleet {
    peers: Vec<net::Client>,
    incoming: mpsc::Receiver<(usize, net::Received)>,
    drivers: Vec<JoinHandle<net::Exit>>,
    collectors: Vec<JoinHandle<()>>,
}
impl Fleet {
    async fn new(node: &Node, app: &App, count: usize) -> Self {
        let (send, incoming) = mpsc::channel(32);
        let mut fleet = Self {
            peers: vec![],
            incoming,
            drivers: vec![],
            collectors: vec![],
        };
        for index in 0..count {
            let identity = noise::Identity::from_secret([index as u8 + 9; 32]);
            let socket = TcpStream::connect(app.network.status().listening.unwrap())
                .await
                .unwrap();
            let (peer, mut events, driver) = establish(
                node,
                socket,
                &identity,
                noise::Role::Initiator(app.identity.public_key()),
            )
            .await;
            let send = send.clone();
            fleet.collectors.push(tokio::spawn(async move {
                while let Some(event) = events.recv().await {
                    if let net::Event::Message(received) = event
                        && send.send((index, received)).await.is_err()
                    {
                        break;
                    }
                }
            }));
            fleet.peers.push(peer);
            fleet.drivers.push(driver);
        }
        fleet
    }
    async fn reply(&self, source: &Node, index: usize, request: &net::Received) {
        if let Some(reply) = snapshot::serve(
            &source.db.pool_view().unwrap(),
            request.message(),
            Default::default(),
        )
        .unwrap()
        {
            self.peers[index].send(&reply).unwrap().await.unwrap();
        }
    }
    async fn join(self) {
        drop(self.incoming);
        for driver in self.drivers {
            within(driver).await.unwrap();
        }
        for collector in self.collectors {
            within(collector).await.unwrap();
        }
    }
}
fn hash(message: &Message) -> Hash256 {
    let Value::Binary(hash) = &message.fields()["hash"] else {
        panic!()
    };
    Hash256(hash.as_slice().try_into().unwrap())
}
fn backward(message: &Message) -> bool {
    message.kind() == Kind::GetGeneration && message.fields()["forward"] == Value::Boolean(false)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    Ordered,
    LostHead,
    InvalidBody,
    Shutdown,
}

async fn shared_downloads(scenario: Scenario, count: usize) {
    let node = Node::new();
    let source = Node::new();
    let blocks = seed(&source, 3).await;
    let first = blocks[0].hash().unwrap();
    let last = blocks[2].hash().unwrap();
    let before = node.db.view().unwrap().status();
    let app = start(&node, config(), Default::default()).await;
    let mut fleet = Fleet::new(&node, &app, count).await;
    let (mut sync, _events, finish) = synchronize_with_importer(
        &node,
        app,
        Default::default(),
        chain::Config {
            bytes: 3 * (80 << 20),
            jobs: 3,
            ..Default::default()
        },
    );
    for peer in &fleet.peers {
        let _ = response(peer, &ping(&source, 1, false)).await;
    }
    let mut head: Option<(usize, net::Received)> = None;
    let mut head_peer = None;
    let mut tail_peer = None;
    let mut backward_requests = vec![];
    let mut early_empty = false;
    let mut released = false;
    within(async {
        loop {
            let status = sync.status();
            if !released && head.is_some() && status.active.iter().any(|a| a.progress.buffered > 0)
                && (count < 3 || early_empty) {
                let tasks: std::collections::BTreeSet<_> = status.active.iter().filter_map(|a| a.progress.task).collect();
                assert_eq!(tasks.len(), 1, "{status:?}");
                assert!(status.active.iter().any(|a| a.progress.members == count));
                assert_eq!(status.counts.finished + status.counts.failed, 0);
                assert_eq!(node.db.view().unwrap().status(), before, "receipt is not admission");
                let (index, request) = head.take().unwrap();
                assert_ne!(Some(index), tail_peer);
                match scenario {
                    Scenario::LostHead => fleet.peers[index].shutdown(),
                    Scenario::Shutdown => { sync.shutdown(); break; },
                    _ => fleet.reply(&source, index, &request).await,
                }
                released = true;
            }
            if status.counts.finished + status.counts.failed == count as u64 { break; }
            tokio::select! {
                _=sync.changed()=>(),
                received=fleet.incoming.recv()=>{
                    let (index, request) = received.unwrap();
                    let message = request.message();
                    if backward(message) {
                        let requested = hash(message);
                        backward_requests.push((index,requested));
                        if requested == first && head_peer.is_none() {
                            head_peer = Some(index);
                            head = Some((index, request));
                            continue;
                        }
                        if requested == last && tail_peer.is_none() {
                            tail_peer = Some(index);
                            if scenario == Scenario::InvalidBody {
                                // Preserve the signed header/hash and generation links,
                                // but corrupt the micro body for actual ledger admission.
                                let mut micro = blocks[1].clone();
                                let ae_chain::block::Block::Micro(body) = &mut micro else { panic!() };
                                body.transactions.clear();
                                let reply = Message::response_ok(&common::import::message(&blocks[2], &[micro], chain::Direction::Backward), Default::default()).unwrap();
                                fleet.peers[index].send(&reply).unwrap().await.unwrap();
                                continue;
                            }
                        }
                    }
                    if message.kind() == Kind::GetSuccessors {
                        let reply = snapshot::serve(&source.db.pool_view().unwrap(), message, Default::default()).unwrap().unwrap();
                        if let Details::Response { message: Some(Ok(body)), .. } = reply.details()
                            && body.fields()["header_hashes"] == Value::List(vec![]) && !released {
                            early_empty = true;
                        }
                    }
                    fleet.reply(&source, index, &request).await;
                }
            }
        }
    }).await;
    if scenario == Scenario::Shutdown {
        assert!(within(finish).await.joined);
        assert_eq!(node.db.view().unwrap().status(), before);
    } else {
        // Completion schedules a policy update on the shared import worker.
        // Observe its acknowledgment before checking the database state.
        within(async {
            loop {
                let status = sync.status();
                assert!(status.running, "{scenario:?}: {status:?}");
                if status
                    .lifecycle
                    .policy
                    .is_some_and(|p| p.state.synchronized())
                {
                    break;
                }
                sync.changed().await.unwrap();
            }
        })
        .await;
        let status = sync.status();
        assert_eq!(
            status.counts.failed,
            u64::from(scenario != Scenario::Ordered),
            "{scenario:?}: {status:?}"
        );
        assert!(status.counts.finished > 0);
        assert_eq!(node.db.view().unwrap().status().tip, Some(last));
        assert!(node.db.view().unwrap().resilience().state.synchronized());
        assert_eq!(
            node.db.view().unwrap().resilience().state.active_depth(),
            Some(100)
        );
        // Exhausted coordinated tasks do not set the initial-fetch latch.
        assert!(!status.lifecycle.last_generation_in_sync);
        assert_eq!(
            backward_requests
                .iter()
                .filter(|(_, h)| *h == first)
                .count(),
            if scenario == Scenario::LostHead { 2 } else { 1 }
        );
        assert_eq!(
            backward_requests.iter().filter(|(_, h)| *h == last).count(),
            if scenario == Scenario::InvalidBody {
                2
            } else {
                1
            }
        );
        sync.shutdown();
        assert!(within(finish).await.joined);
    }
    fleet.join().await;
}
#[tokio::test]
async fn shared_peers_buffer_out_of_order_and_import_once_after_head_arrives() {
    shared_downloads(Scenario::Ordered, 2).await;
}
#[tokio::test]
async fn early_successor_exhaustion_waits_for_all_buffered_generation_commits() {
    shared_downloads(Scenario::Ordered, 3).await;
}
#[tokio::test]
async fn disconnected_head_supplier_is_replaced_without_losing_buffered_tail() {
    shared_downloads(Scenario::LostHead, 3).await;
}
#[tokio::test]
async fn invalid_buffered_micro_body_is_refetched_after_ordered_admission_failure() {
    shared_downloads(Scenario::InvalidBody, 3).await;
}
#[tokio::test]
async fn shared_shutdown_drops_buffered_receipts_and_joins_pending_drivers() {
    shared_downloads(Scenario::Shutdown, 3).await;
}

#[tokio::test]
async fn missing_chain_hint_waits_for_existing_header_slot_then_merges_live_tasks() {
    let node = Node::new();
    let source = Node::new();
    let blocks = seed(&source, 3).await;
    seed(&node, 1).await;
    let before = node.db.view().unwrap().status();
    let app = start(&node, config(), Default::default()).await;
    let mut fleet = Fleet::new(&node, &app, 2).await;
    let (mut sync, _events, finish) = synchronize_with_importer(
        &node,
        app,
        chain::sync::Config {
            read_bytes: 3 * (80 << 20),
            ..Default::default()
        },
        Default::default(),
    );
    let send_header = |header: ae_codec::headers::Header| {
        Message::response_ok(
            &message(
                Kind::Header,
                1,
                &[("hdr", Value::Binary(header.encode().unwrap()))],
            ),
            Default::default(),
        )
        .unwrap()
    };
    let mut high = blocks[2].header();
    let ae_codec::headers::Header::Key(key) = &mut high else {
        panic!()
    };
    key.common.height = 4;
    key.common.previous_key_hash = [7; 32];
    let _ = response(&fleet.peers[0], &ping(&source, 1, false)).await;
    within(async {
        loop {
            let (index, request) = fleet.incoming.recv().await.unwrap();
            assert_eq!(index, 0);
            if request.message().kind() == Kind::GetHeaderByHeight {
                assert_eq!(
                    request.message().fields()["height"],
                    Value::Integer(1.into())
                );
                break;
            }
            if request.message().kind() == Kind::GetHeaderByHash {
                fleet.peers[0]
                    .send(&send_header(high.clone()))
                    .unwrap()
                    .await
                    .unwrap();
            } else {
                fleet.reply(&source, index, &request).await;
            }
        }
    })
    .await;
    let _ = response(&fleet.peers[1], &ping(&source, 1, false)).await;
    within(async {
        loop {
            let (index, request) = fleet.incoming.recv().await.unwrap();
            if request.message().kind() == Kind::GetHeaderByHash {
                assert_eq!(index, 1);
                fleet.reply(&source, index, &request).await;
                break;
            }
            fleet.reply(&source, index, &request).await;
        }
        while !sync
            .status()
            .active
            .iter()
            .any(|a| a.progress.phase == chain::sync::Phase::Matching)
        {
            sync.changed().await.unwrap();
        }
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), async {
            while let Some((index, request)) = fleet.incoming.recv().await {
                assert_ne!(
                    request.message().kind(),
                    Kind::GetHeaderByHeight,
                    "overlapping header request on peer {index}"
                );
                fleet.reply(&source, index, &request).await;
            }
        })
        .await
        .is_err()
    );
    assert_eq!(sync.status().counts.failed, 0);
    fleet.peers[0]
        .send(&send_header(blocks[0].header()))
        .unwrap()
        .await
        .unwrap();
    let mut looked_up = false;
    within(async {
        loop {
            let status = sync.status();
            assert_eq!(status.counts.failed, 0, "{status:?}");
            if looked_up && status.active.iter().any(|a| a.progress.members == 2) {
                let ids: std::collections::BTreeSet<_> = status.active.iter().filter_map(|a| a.progress.task).collect();
                assert_eq!(ids.len(), 1);
                break;
            }
            tokio::select! {
                _=sync.changed()=>(),
                received=fleet.incoming.recv()=>{
                    let (index,request)=received.unwrap();
                    if request.message().kind()==Kind::GetHeaderByHeight {
                        assert_eq!(index,0);
                        assert_eq!(request.message().fields()["height"],Value::Integer(2.into()));
                        fleet.peers[0].send(&send_header(blocks[2].header())).unwrap().await.unwrap();
                        looked_up=true;
                    }
                    // Leave successor requests pending; matching a chain must
                    // not imply admission of the unsealed height-four hint.
                }
            }
        }
    }).await;
    assert_eq!(node.db.view().unwrap().status(), before);
    sync.shutdown();
    assert!(within(finish).await.joined);
    fleet.join().await;
}
