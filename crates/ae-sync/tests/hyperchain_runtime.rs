mod common;
use ae_chain::block::{Block, Light};
use ae_p2p::{
    message::{Kind, Message, Value},
    noise, ping, runtime as net,
};
use ae_protocol::ConsensusType;
use ae_storage::pool::worker;
use ae_sync::{
    chain::{self, Direction, Importer, sync},
    gossip, network, peer,
};
use common::{hyperchain as fixture, within};
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
};
use tokio::{net::TcpStream, task::JoinHandle};
const NOW: u64 = 1_000_000;
struct App {
    network: network::Handle,
    gossip: gossip::Handle,
    peer: peer::Handle,
    events: peer::Events,
    network_task: JoinHandle<network::Exit>,
    gossip_task: JoinHandle<gossip::Status>,
    peer_task: JoinHandle<peer::Status>,
    identity: Arc<noise::Identity>,
    _pool: worker::Worker,
}
fn message(kind: Kind, fields: Vec<(&str, Value)>) -> Message {
    Message::new(
        kind,
        1,
        fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        Default::default(),
    )
    .unwrap()
}
async fn start(node: &fixture::Node) -> App {
    let profile = node.config.profile.network.clone();
    let pool = worker::Worker::spawn(
        node.db.clone(),
        profile.clone(),
        Default::default(),
        worker::Settings {
            origins_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    let identity = Arc::new(noise::Identity::from_secret([71; 32]));
    let mut config = network::Config {
        listen: Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
        ..Default::default()
    };
    config.startup.include_default_peers = false;
    config.registry.policy.maximum_outbound = 0;
    config.registry.policy.maximum_probes = 0;
    let (mut network, scheduled, incoming, service) = network::create(
        node.db.clone(),
        pool.client(),
        identity.clone(),
        profile.clone(),
        config,
    )
    .await
    .unwrap();
    let network_task = tokio::spawn(service.run());
    let (gossip, incoming, service) = gossip::create(
        node.db.clone(),
        pool.client(),
        network.peers(),
        incoming,
        profile,
        ConsensusType::Pos,
        Default::default(),
    )
    .unwrap();
    let gossip_task = tokio::spawn(service.run());
    let (peer, events, service) = peer::create(
        node.db.clone(),
        network.clone(),
        scheduled,
        incoming,
        Default::default(),
    )
    .unwrap();
    let peer_task = tokio::spawn(service.run());
    within(async {
        while !network.status().ready {
            assert!(network.status().failure.is_none());
            network.changed().await.unwrap();
        }
    })
    .await;
    App {
        network,
        gossip,
        peer,
        events,
        network_task,
        gossip_task,
        peer_task,
        identity,
        _pool: pool,
    }
}
async fn connect(
    node: &fixture::Node,
    app: &App,
) -> (net::Client, net::Events, JoinHandle<net::Exit>) {
    let socket = TcpStream::connect(app.network.status().listening.unwrap())
        .await
        .unwrap();
    let identity = noise::Identity::from_secret([72; 32]);
    let binding = noise::Network::new(
        1,
        node.config.identity.genesis,
        &node.config.identity.network,
    )
    .unwrap();
    let (remote, events, driver) = within(net::establish(
        socket,
        &identity,
        &binding,
        noise::Role::Initiator(app.identity.public_key()),
        Default::default(),
    ))
    .await
    .unwrap();
    let driver = tokio::spawn(driver.run());
    let local = peer::snapshot::local(&node.db.view().unwrap(), false).unwrap();
    let ping = ping::encode(
        &local,
        vec![],
        &[],
        ping::Encoding {
            version: 2,
            minimum_version: 1,
            port: 4321,
            share: 0.into(),
            limits: Default::default(),
        },
    )
    .unwrap();
    assert!(matches!(
        within(remote.request(&ping, None).unwrap()).await.unwrap(),
        net::Reply::Response(_)
    ));
    within(async {
        loop {
            if app
                .network
                .peers()
                .peers()
                .unwrap()
                .wait()
                .await
                .unwrap()
                .iter()
                .any(|p| p.peer() == identity.public_key())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    (remote, events, driver)
}
async fn incoming(events: &mut net::Events, kind: Kind) -> net::Received {
    within(async {
        loop {
            if let net::Event::Message(m) = events.recv().await.unwrap()
                && m.message().kind() == kind
            {
                return m;
            }
        }
    })
    .await
}
fn importer(node: &fixture::Node) -> Importer {
    Importer::new_hyperchain(
        node.db.clone(),
        node.config.clone(),
        fixture::Inputs::default(),
        Default::default(),
    )
    .unwrap()
}
async fn stop_parts(app: App) {
    app.peer.shutdown();
    assert!(within(app.peer_task).await.unwrap().joined);
    app.gossip.shutdown();
    app.network.shutdown();
    assert!(!within(app.gossip_task).await.unwrap().running);
    assert!(within(app.network_task).await.unwrap().status.joined);
}
#[tokio::test]
async fn encrypted_hyperchain_generations_execute_finalization_and_reject_stale_clients() {
    let node = fixture::Node::new(6);
    let app = start(&node).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let client = within(async {
        loop {
            if let Some(client) = app
                .network
                .peers()
                .peers()
                .unwrap()
                .wait()
                .await
                .unwrap()
                .into_iter()
                .next()
            {
                return client;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let importer = importer(&node);
    let mut micros = vec![];
    for (index, block) in node.blocks().into_iter().enumerate() {
        if matches!(block, Block::Micro(_)) {
            micros.push(block);
            continue;
        }
        let hash = block.hash().unwrap();
        let fetch = importer.fetch(&app.network, client.clone(), hash, Direction::Backward, NOW);
        let serve = async {
            let request = incoming(&mut events, Kind::GetGeneration).await;
            assert_eq!(
                request.message().fields()["hash"],
                Value::Binary(hash.0.to_vec())
            );
            assert_eq!(request.message().fields()["forward"], Value::Boolean(false));
            remote
                .send(
                    &Message::response_ok(
                        &common::import::message(&block, &micros, Direction::Backward),
                        Default::default(),
                    )
                    .unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
        };
        let (result, ()) = within(async { tokio::join!(fetch, serve) }).await;
        assert_eq!(result.unwrap().accepted.len(), micros.len() + 1);
        node.check(index);
        micros.clear();
    }
    remote.shutdown();
    within(driver).await.unwrap();
    within(client.monitor().wait()).await;
    assert_eq!(
        importer
            .fetch(
                &app.network,
                client,
                node.config.identity.genesis,
                Direction::Backward,
                NOW
            )
            .await
            .unwrap_err()
            .error,
        chain::Error::Stale
    );
    importer.stop();
    importer.join().await;
    stop_parts(app).await;
}
fn gossip(block: &Block, light: bool) -> Message {
    match block {
        Block::Key(_) => message(
            Kind::KeyBlock,
            vec![("key_block", Value::Binary(block.encode().unwrap()))],
        ),
        Block::Micro(m) => message(
            Kind::MicroBlock,
            vec![
                (
                    "micro_block",
                    Value::Binary(if light {
                        Light::from_micro(m).encode().unwrap()
                    } else {
                        block.encode().unwrap()
                    }),
                ),
                ("light", Value::Boolean(light)),
            ],
        ),
    }
}
async fn wait_blocks(handle: &mut sync::Handle, accepted: u64, ignored: u64) {
    within(async {
        loop {
            let status = handle.status();
            assert!(status.running, "{status:?}");
            if status.blocks.counts.accepted == accepted && status.blocks.counts.ignored == ignored
            {
                break;
            }
            assert_eq!(status.blocks.counts.failed, 0, "{status:?}");
            handle.changed().await.unwrap();
        }
    })
    .await;
}
#[tokio::test]
async fn encrypted_full_and_light_hc_blocks_reconstruct_and_commit_both_epoch_ends() {
    for (index, light, pins) in [
        (0, false, false),
        (0, true, false),
        (6, true, false),
        (0, false, true),
        (0, true, true),
        (1, true, true),
        (6, true, true),
    ] {
        let node = if pins {
            fixture::Node::from_corpus(&fixture::pinned::DATA, index)
        } else {
            fixture::Node::new(index)
        };
        let parent = pins.then(|| fixture::pinned::Parent::new(index));
        let (importer, mut accepted) = if let Some(p) = &parent {
            let (i, a) = p.importer(&node);
            (i, Some(a))
        } else {
            (importer(&node), None)
        };
        let app = start(&node).await;
        let (remote, mut events, driver) = connect(&node, &app).await;
        let (handle, _forwarded, service) = sync::create(
            importer,
            app.network.clone(),
            app.peer.clone(),
            app.events,
            Default::default(),
        )
        .unwrap();
        let mut handle = handle;
        let finish = tokio::spawn(async move {
            let status = service.run().await;
            app.peer.shutdown();
            assert!(within(app.peer_task).await.unwrap().joined);
            app.gossip.shutdown();
            app.network.shutdown();
            assert!(!within(app.gossip_task).await.unwrap().running);
            assert!(within(app.network_task).await.unwrap().status.joined);
            drop(app._pool);
            status
        });
        let mut ignored = 0;
        let mut leader_version = node
            .db
            .view()
            .unwrap()
            .require_block(node.config.identity.genesis)
            .unwrap()
            .header()
            .common()
            .version;
        for (i, block) in node.blocks().into_iter().enumerate() {
            let boundary =
                matches!(&block,Block::Micro(m)if m.header.common.version!=leader_version);
            if light && boundary {
                // Native light preassembly uses the leader's version. A full
                // block still uses the effective activation schedule.
                remote.send(&gossip(&block, true)).unwrap().await.unwrap();
                ignored += 1;
                wait_blocks(&mut handle, i as u64, ignored).await;
            }
            let expand = light && !boundary;
            remote.send(&gossip(&block, expand)).unwrap().await.unwrap();
            if expand
                && let Block::Micro(m) = &block
                && !m.transactions.is_empty()
            {
                let request = incoming(&mut events, Kind::GetBlockTransactions).await;
                assert_eq!(
                    request.message().fields()["hash"],
                    Value::Binary(block.hash().unwrap().0.to_vec())
                );
                assert_eq!(
                    request.message().fields()["tx_hashes"],
                    Value::List(
                        m.transactions
                            .iter()
                            .map(|t| Value::Binary(t.hash().0.to_vec()))
                            .collect()
                    )
                );
                let reply = message(
                    Kind::BlockTransactions,
                    vec![
                        ("hash", Value::Binary(block.hash().unwrap().0.to_vec())),
                        (
                            "txs",
                            Value::List(
                                m.transactions
                                    .iter()
                                    .map(|t| Value::Binary(t.encode()))
                                    .collect(),
                            ),
                        ),
                    ],
                );
                remote
                    .send(&Message::response_ok(&reply, Default::default()).unwrap())
                    .unwrap()
                    .await
                    .unwrap();
            }
            wait_blocks(&mut handle, i as u64 + 1, ignored).await;
            node.check(i);
            if let Some(a) = &mut accepted {
                fixture::pinned::check_pin(&node, i);
                a.take().unwrap().drain();
            }
            if let Block::Key(k) = &block {
                leader_version = k.common.version;
            }
            if i == 0
                && let Block::Micro(m) = &block
            {
                remote.send(&gossip(&block, true)).unwrap().await.unwrap();
                ignored += 1;
                wait_blocks(&mut handle, 1, ignored).await;
                let mut bad = m.clone();
                bad.transactions.clear();
                remote
                    .send(&gossip(&Block::Micro(bad), false))
                    .unwrap()
                    .await
                    .unwrap();
                ignored += 1;
                wait_blocks(&mut handle, 1, ignored).await;
                assert_eq!(node.db.view().unwrap().status().revision, 2);
            }
        }
        if let Some(p) = &parent {
            assert_eq!(p.requests.load(std::sync::atomic::Ordering::Acquire), 1);
        }
        handle.shutdown();
        let status = within(finish).await.unwrap();
        assert!(status.joined);
        assert_eq!(status.blocks.counts.committed, 13);
        assert_eq!(status.blocks.active, 0);
        within(driver).await.unwrap();
    }
}

#[tokio::test]
async fn encrypted_hc_disconnect_during_parent_capture_cannot_publish_the_download() {
    let node = fixture::Node::new(0);
    let app = start(&node).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let client = within(async {
        loop {
            if let Some(client) = app
                .network
                .peers()
                .peers()
                .unwrap()
                .wait()
                .await
                .unwrap()
                .into_iter()
                .next()
            {
                return client;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let inputs = fixture::Inputs::default();
    let importer = Importer::new_hyperchain(
        node.db.clone(),
        node.config.clone(),
        inputs.clone(),
        Default::default(),
    )
    .unwrap();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    inputs.hook(move || {
        let _ = entered.send(());
        resume
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
    });
    let blocks = node.blocks();
    let hash = blocks[1].hash().unwrap();
    let fetch = importer.fetch(&app.network, client.clone(), hash, Direction::Backward, NOW);
    let serve = async {
        incoming(&mut events, Kind::GetGeneration).await;
        remote
            .send(
                &Message::response_ok(
                    &common::import::message(&blocks[1], &blocks[..1], Direction::Backward),
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap()
            .await
            .unwrap();
        within(ready).await.unwrap();
        remote.shutdown();
        within(client.monitor().wait()).await;
        release.send(()).unwrap();
    };
    let (result, ()) = within(async { tokio::join!(fetch, serve) }).await;
    let failure = result.unwrap_err();
    assert_eq!(failure.error, chain::Error::Stale);
    assert!(failure.progress.unwrap().accepted.is_empty());
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    assert!(!importer.recovery_required());
    within(
        importer
            .import_block(blocks[0].encode().unwrap(), NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    node.check(0);
    importer.stop();
    importer.join().await;
    within(driver).await.unwrap();
    stop_parts(app).await;
}

#[tokio::test]
async fn encrypted_pin_generations_preserve_native_rewards_and_incorrect_proofs() {
    use fixture::pinned;
    use std::sync::atomic::Ordering;
    for index in [0, 1, 6] {
        let node = fixture::Node::from_corpus(&pinned::DATA, index);
        let parent = pinned::Parent::new(index);
        let (importer, mut accepted) = parent.importer(&node);
        let app = start(&node).await;
        let (remote, mut events, driver) = connect(&node, &app).await;
        let client = app
            .network
            .peers()
            .peers()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .pop()
            .unwrap();
        let mut micros = vec![];
        for (i, block) in node.blocks().into_iter().enumerate() {
            if matches!(block, Block::Micro(_)) {
                micros.push(block);
                continue;
            }
            let hash = block.hash().unwrap();
            let fetch =
                importer.fetch(&app.network, client.clone(), hash, Direction::Backward, NOW);
            let serve = async {
                let request = incoming(&mut events, Kind::GetGeneration).await;
                assert_eq!(
                    request.message().fields()["hash"],
                    Value::Binary(hash.0.to_vec())
                );
                remote
                    .send(
                        &Message::response_ok(
                            &common::import::message(&block, &micros, Direction::Backward),
                            Default::default(),
                        )
                        .unwrap(),
                    )
                    .unwrap()
                    .await
                    .unwrap();
            };
            let (result, ()) = within(async { tokio::join!(fetch, serve) }).await;
            assert_eq!(result.unwrap().accepted.len(), micros.len() + 1);
            node.check(i);
            pinned::check_pin(&node, i);
            accepted.take().unwrap().drain();
            micros.clear();
        }
        assert_eq!(parent.requests.load(Ordering::Acquire), 1);
        remote.shutdown();
        within(driver).await.unwrap();
        importer.stop();
        importer.join().await;
        stop_parts(app).await;
    }
}

#[tokio::test]
async fn encrypted_disconnect_during_pin_rpc_discards_reward_and_retries_on_fresh_peer() {
    use fixture::pinned;
    use std::sync::atomic::Ordering;
    let node = fixture::Node::from_corpus(&pinned::DATA, 6);
    let parent = pinned::Parent::new(6);
    let (importer, mut accepted) = parent.importer(&node);
    let blocks = node.blocks();
    for b in &blocks[..10] {
        within(
            importer
                .import_block(b.encode().unwrap(), NOW)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        accepted.take().unwrap().drain();
    }
    let before = node.db.view().unwrap();
    let status = before.status();
    let checkpoint = before.hyperchain_checkpoint().unwrap();
    let app = start(&node).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let client = app
        .network
        .peers()
        .peers()
        .unwrap()
        .wait()
        .await
        .unwrap()
        .pop()
        .unwrap();
    parent.mode.store(3, Ordering::Release);
    let hash = blocks[10].hash().unwrap();
    let fetch = importer.fetch(&app.network, client.clone(), hash, Direction::Backward, NOW);
    let serve = async {
        incoming(&mut events, Kind::GetGeneration).await;
        remote
            .send(
                &Message::response_ok(
                    &common::import::message(&blocks[10], &blocks[9..10], Direction::Backward),
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap()
            .await
            .unwrap();
        within(async {
            while parent.requests.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await;
        remote.shutdown();
        within(client.monitor().wait()).await;
        parent.mode.store(0, Ordering::Release);
    };
    let (result, ()) = within(async { tokio::join!(fetch, serve) }).await;
    let failure = result.unwrap_err();
    assert_eq!(failure.error, chain::Error::Stale);
    let prefix = failure.progress.unwrap();
    assert_eq!(prefix.accepted.len(), 1);
    assert_eq!(prefix.accepted[0].outcome, chain::Outcome::Present);
    assert_eq!(node.db.view().unwrap().status(), status);
    assert_eq!(
        node.db.view().unwrap().hyperchain_checkpoint().unwrap(),
        checkpoint
    );
    assert!(accepted.take().unwrap().entries().is_empty());
    assert!(!importer.recovery_required());
    within(driver).await.unwrap();
    within(async {
        while !app
            .network
            .peers()
            .peers()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let client = app
        .network
        .peers()
        .peers()
        .unwrap()
        .wait()
        .await
        .unwrap()
        .pop()
        .unwrap();
    let fetch = importer.fetch(&app.network, client, hash, Direction::Backward, NOW);
    let serve = async {
        incoming(&mut events, Kind::GetGeneration).await;
        remote
            .send(
                &Message::response_ok(
                    &common::import::message(&blocks[10], &blocks[9..10], Direction::Backward),
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap()
            .await
            .unwrap();
    };
    let (result, ()) = within(async { tokio::join!(fetch, serve) }).await;
    let report = result.unwrap();
    assert_eq!(report.accepted.len(), 2);
    assert_eq!(
        report.accepted[1].outcome,
        chain::Outcome::Committed {
            revision: status.revision + 1
        }
    );
    node.check(10);
    pinned::check_pin(&node, 10);
    accepted.take().unwrap().drain();
    assert_eq!(before.status(), status);
    assert_eq!(parent.requests.load(Ordering::Acquire), 2);
    remote.shutdown();
    within(driver).await.unwrap();
    importer.stop();
    importer.join().await;
    stop_parts(app).await;
}
