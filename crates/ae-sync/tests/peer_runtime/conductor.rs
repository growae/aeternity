use super::*;
use ae_chain::block::{Block, Light};
use ae_sync::{
    chain::sync,
    conductor::{self, Config, Error},
};
struct Running {
    network: network::Handle,
    sync: sync::Handle,
    finish: JoinHandle<sync::Status>,
    drain: JoinHandle<()>,
    peers: Vec<net::Client>,
    events: Vec<net::Events>,
    drivers: Vec<JoinHandle<net::Exit>>,
}
impl Running {
    async fn start(node: &Node, count: usize) -> Self {
        let app = start(
            node,
            config(),
            peer::Config {
                events: 256,
                ..Default::default()
            },
        )
        .await;
        let mut peers = vec![];
        let mut events = vec![];
        let mut drivers = vec![];
        for index in 0..count {
            let identity = noise::Identity::from_secret([index as u8 + 11; 32]);
            let socket = TcpStream::connect(app.network.status().listening.unwrap())
                .await
                .unwrap();
            let (peer, incoming, driver) = establish(
                node,
                socket,
                &identity,
                noise::Role::Initiator(app.identity.public_key()),
            )
            .await;
            response(&peer, &ping(node, 2, true)).await;
            admitted_client(&app, identity.public_key()).await;
            peers.push(peer);
            events.push(incoming);
            drivers.push(driver);
        }
        let network = app.network.clone();
        let (sync, mut forwarded, service) = sync::create(
            download_importer(node, Default::default()),
            network.clone(),
            app.application.clone(),
            app.events,
            sync::Config {
                forwarded: 256,
                ..Default::default()
            },
        )
        .unwrap();
        let drain = tokio::spawn(async move { while forwarded.recv().await.is_some() {} });
        let finish = tokio::spawn(async move {
            let status = service.run().await;
            app.application.shutdown();
            assert!(within(app.application_task).await.unwrap().joined);
            app.gossip.shutdown();
            app.network.shutdown();
            assert!(!within(app.gossip_task).await.unwrap().running);
            assert!(within(app.network_task).await.unwrap().status.joined);
            status
        });
        Self {
            network,
            sync,
            finish,
            drain,
            peers,
            events,
            drivers,
        }
    }
    fn conductor(
        &self,
        node: &Node,
        after: Option<ae_storage::events::Cursor>,
        config: Config,
    ) -> Result<(conductor::Handle, conductor::Service), Error> {
        conductor::create(
            node.db.clone(),
            self.network.clone(),
            self.sync.clone(),
            after,
            config,
        )
    }
    async fn stop(self) {
        self.sync.shutdown();
        assert!(within(self.finish).await.unwrap().joined);
        assert_eq!(self.sync.peers_in_sync(), Err(sync::Error::Closed));
        for driver in self.drivers {
            within(driver).await.unwrap();
        }
        within(self.drain).await.unwrap();
    }
    async fn drain_blocks(&mut self, node: &Node) -> Vec<Vec<Message>> {
        let mut result = vec![];
        for (peer, events) in self.peers.iter().zip(&mut self.events) {
            // The response crosses the same encrypted connection after every
            // publication send ticket has completed, providing a read barrier.
            response(peer, &ping(node, 2, true)).await;
            let mut messages = vec![];
            while let Ok(event) = events.try_recv() {
                if let net::Event::Message(m) = event
                    && matches!(m.message().kind(), Kind::KeyBlock | Kind::MicroBlock)
                {
                    messages.push(m.message().clone());
                }
            }
            result.push(messages);
        }
        result
    }
}
async fn observed(handle: &mut conductor::Handle, records: u64) -> conductor::Status {
    within(async {
        loop {
            let status = handle.status();
            assert!(status.last_error.is_none(), "{status:?}");
            if status.records >= records {
                return status;
            }
            handle.changed().await.unwrap();
        }
    })
    .await
}
async fn stop(handle: conductor::Handle, task: JoinHandle<conductor::Status>) {
    handle.shutdown();
    let status = within(task).await.unwrap();
    assert!(!status.running && status.joined);
    assert_eq!(status.last_error, None);
}
#[tokio::test]
async fn committed_created_and_received_blocks_use_native_wire_forms_and_fanout() {
    for (created, limit, recipients) in [(false, 1, 1), (false, 0, 0), (true, 0, 2)] {
        let node = Node::new();
        let mut running = Running::start(&node, 2).await;
        let (mut handle, service) = running
            .conductor(
                &node,
                None,
                Config {
                    received_peers: limit,
                    ..Default::default()
                },
            )
            .unwrap();
        let task = tokio::spawn(service.run());
        let importer = download_importer(&node, Default::default());
        let all = common::ledger_fixtures::blocks(&NetworkProfile::mainnet());
        for (index, block) in all.iter().take(2).enumerate() {
            let bytes = block.encode().unwrap();
            let ticket = if created {
                importer.import_created_block(bytes, u64::MAX - 600_000)
            } else {
                importer.import_block(bytes, u64::MAX - 600_000)
            };
            ticket.unwrap().wait().await.unwrap();
            let status = observed(&mut handle, index as u64 + 1).await;
            assert_eq!(status.sent, (index as u64 + 1) * recipients);
            assert_eq!(status.failed, 0);
            let messages = running.drain_blocks(&node).await;
            assert_eq!(
                messages.iter().map(Vec::len).sum::<usize>(),
                recipients as usize
            );
            for message in messages.into_iter().flatten() {
                match block {
                    Block::Key(_) => {
                        assert_eq!(message.kind(), Kind::KeyBlock);
                        assert_eq!(
                            message.fields()["key_block"],
                            Value::Binary(block.encode().unwrap())
                        );
                    }
                    Block::Micro(m) => {
                        assert_eq!(message.kind(), Kind::MicroBlock);
                        assert_eq!(message.fields()["light"], Value::Boolean(true));
                        assert_eq!(
                            message.fields()["micro_block"],
                            Value::Binary(Light::from_micro(m).encode().unwrap())
                        );
                    }
                }
            }
        }
        let status = handle.status();
        assert_eq!(status.top_changes, 2);
        assert!(status.transaction_events > 0);
        assert_eq!(status.created, if created { 2 } else { 0 });
        let before = node.db.events().cursor();
        assert!(
            importer
                .import_block(vec![1, 2, 3], u64::MAX - 600_000)
                .unwrap()
                .wait()
                .await
                .is_err()
        );
        importer
            .import_block(all[0].encode().unwrap(), u64::MAX - 600_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(node.db.events().cursor(), before);
        importer.stop();
        importer.join().await;
        stop(handle, task).await;
        running.stop().await;
    }
}
#[tokio::test]
async fn synchronized_blocks_and_late_consumers_do_not_republish_history() {
    let node = Node::new();
    let mut running = Running::start(&node, 1).await;
    let (mut handle, service) = running.conductor(&node, None, Config::default()).unwrap();
    let task = tokio::spawn(service.run());
    chain_sync::seed(&node, 3).await;
    let status = observed(&mut handle, 3).await;
    assert_eq!(
        (status.sent, status.broadcasts, status.transaction_events),
        (0, 0, 0)
    );
    assert_eq!(status.top_changes, 3);
    assert!(
        running
            .drain_blocks(&node)
            .await
            .into_iter()
            .flatten()
            .next()
            .is_none()
    );
    stop(handle, task).await;
    let (handle, service) = running.conductor(&node, None, Config::default()).unwrap();
    assert_eq!(handle.status().cursor, node.db.events().cursor());
    handle.shutdown();
    let status = service.run().await;
    assert!(status.joined);
    assert_eq!(status.records, 0);
    running.stop().await;
}
#[tokio::test]
async fn received_blocks_exclude_current_sync_members_but_created_blocks_reach_them() {
    let node = Node::new();
    let source = Node::new();
    let all = chain_sync::seed(&source, 3).await;
    let mut running = Running::start(&node, 1).await;
    response(&running.peers[0], &ping(&source, 2, false)).await;
    within(async {
        while running.sync.peers_in_sync().unwrap().is_empty() {
            tokio::select! {
                _ = running.sync.changed() => (),
                event = running.events[0].recv() => {
                    if let net::Event::Message(request) = event.unwrap() {
                        // The owner fetches the advertised header before it
                        // can register this peer in a shared chain task.
                        if let Some(reply) = snapshot::serve(&source.db.pool_view().unwrap(),request.message(),Default::default()).unwrap() {
                            running.peers[0].send(&reply).unwrap().await.unwrap();
                        }
                    }
                }
            }
        }
    })
    .await;
    let (mut handle, service) = running.conductor(&node, None, Config::default()).unwrap();
    let task = tokio::spawn(service.run());
    let importer = download_importer(&node, Default::default());
    importer
        .import_block(all[0].encode().unwrap(), u64::MAX - 600_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(observed(&mut handle, 1).await.sent, 0);
    importer
        .import_created_block(all[1].encode().unwrap(), u64::MAX - 600_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(observed(&mut handle, 2).await.sent, 1);
    let incoming = within(async {
        loop {
            let message = incoming(&mut running.events[0]).await;
            if message.message().kind() == Kind::MicroBlock {
                break message;
            }
        }
    })
    .await;
    assert_eq!(incoming.message().fields()["light"], Value::Boolean(true));
    importer.stop();
    importer.join().await;
    stop(handle, task).await;
    running.stop().await;
}
#[tokio::test]
async fn conductor_rejects_foreign_sources_and_joins_after_dependency_stop_or_abort() {
    let node = Node::new();
    let other = Node::new();
    let running = Running::start(&node, 0).await;
    assert!(matches!(
        running.conductor(&other, None, Config::default()),
        Err(Error::Configuration)
    ));
    assert!(matches!(
        running.conductor(&node, Some(other.db.events().cursor()), Config::default()),
        Err(Error::Source(ae_storage::events::Error::WrongSource))
    ));
    for config in [
        Config {
            peer_limit: 0,
            ..Default::default()
        },
        Config {
            read_bytes: 1,
            ..Default::default()
        },
        Config {
            poll: Duration::ZERO,
            ..Default::default()
        },
        Config {
            timeout: Duration::MAX,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            running.conductor(&node, None, config),
            Err(Error::Configuration)
        ));
    }
    let (mut handle, service) = running.conductor(&node, None, Config::default()).unwrap();
    let task = tokio::spawn(service.run());
    task.abort();
    assert!(within(task).await.unwrap_err().is_cancelled());
    assert!(!handle.status().running);
    assert_eq!(handle.status().last_error, Some(Error::Closed));
    let (_, service) = running.conductor(&node, None, Config::default()).unwrap();
    running.sync.shutdown();
    // Wait for the dependency status to become terminal rather than racing it.
    while running.sync.status().running {
        tokio::task::yield_now().await;
    }
    let status = within(service.run()).await;
    assert!(status.joined);
    assert_eq!(status.last_error, Some(Error::SyncClosed));
    assert!(handle.changed().await.is_ok());
    running.stop().await;
}

#[tokio::test]
async fn conductor_replays_retained_commits_and_fails_explicitly_on_gaps() {
    for retained in [true, false] {
        let node = Node::with_events(ae_storage::events::Config {
            records: if retained { 4 } else { 1 },
            ..Default::default()
        });
        let mut running = Running::start(&node, 1).await;
        let before = node.db.events().cursor();
        let (mut handle, service) = running
            .conductor(&node, Some(before.clone()), Config::default())
            .unwrap();
        let importer = download_importer(&node, Default::default());
        for block in common::ledger_fixtures::blocks(&NetworkProfile::mainnet())
            .iter()
            .take(2)
        {
            importer
                .import_block(block.encode().unwrap(), u64::MAX - 600_000)
                .unwrap()
                .wait()
                .await
                .unwrap();
        }
        if retained {
            let task = tokio::spawn(service.run());
            assert_eq!(observed(&mut handle, 2).await.sent, 2);
            assert_eq!(running.drain_blocks(&node).await[0].len(), 2);
            stop(handle, task).await;
        } else {
            let status = within(service.run()).await;
            assert!(status.joined);
            assert_eq!(status.records, 0);
            assert!(matches!(
                status.last_error,
                Some(Error::Source(ae_storage::events::Error::Gap {
                    missed: 1,
                    ..
                }))
            ));
            assert!(matches!(
                running.conductor(&node, Some(before), Config::default()),
                Err(Error::Source(ae_storage::events::Error::Gap {
                    missed: 1,
                    ..
                }))
            ));
        }
        importer.stop();
        importer.join().await;
        running.stop().await;
    }
}
