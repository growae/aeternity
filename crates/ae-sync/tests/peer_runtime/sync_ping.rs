use super::*;
use ae_sync::{
    chain::sync,
    observe::{
        registry::{self, predefined},
        reporter::Points,
    },
    routing::Part,
};

fn counter(store: &registry::Store, suffix: &str, expected: u64) {
    assert_eq!(
        store
            .get_value(&predefined::name(suffix), &Points::One(Part::atom("value")))
            .unwrap(),
        registry::Reading::Values(vec![(
            Part::atom("value"),
            registry::Value::Integer(expected.into())
        )])
    );
}

#[tokio::test]
async fn scheduled_ping_suppression_reschedules_once_and_wire_pings_resume_after_sync() {
    let node = Node::new();
    let source = Node::new();
    chain_sync::seed(&source, 3).await;
    let store = Arc::new(registry::Store::new(Default::default()).unwrap());
    let _registry_owner = registry::Owner::spawn(store.clone()).unwrap();
    let feed = predefined::Feed::install(store.clone()).unwrap();
    let _binding = node.db.instrumentation().bind(feed.clone()).unwrap();
    let mut cfg = config();
    cfg.registry.policy.ping_interval = 1000;
    cfg.transport.runtime.minimum_ping = 2;
    let app = start(&node, cfg, Default::default()).await;
    let (remote, mut incoming, driver) = connect(&node, &app).await;
    let App {
        mut network,
        application,
        gossip,
        events,
        network_task,
        gossip_task,
        application_task,
        ..
    } = app;
    let (mut sync, mut forwarded, service) = sync::create(
        download_importer(&node, Default::default()),
        network.clone(),
        application.clone(),
        events,
        Default::default(),
    )
    .unwrap();
    let sync_task = tokio::spawn(service.run());
    let drain = tokio::spawn(async move { while forwarded.recv().await.is_some() {} });
    // The first v2 ping negotiates only; the second carries chain effects.
    response(&remote, &ping(&source, 2, false)).await;
    response(&remote, &ping(&source, 2, false)).await;
    let key = identities().1.public_key();
    let mut pending = vec![];
    within(async {
        while !sync.peers_in_sync().unwrap().contains(&key) {
            tokio::select! {
                result = sync.changed() => { result.unwrap(); },
                event = incoming.recv() => {
                    if let net::Event::Message(request) = event.unwrap() {
                        assert_ne!(request.message().kind(), Kind::Ping);
                        if request.message().kind() == Kind::GetHeaderByHash {
                            let reply = snapshot::serve(&source.db.pool_view().unwrap(), request.message(), Default::default()).unwrap().unwrap();
                            remote.send(&reply).unwrap().await.unwrap();
                        } else {
                            pending.push(request);
                        }
                    }
                },
            }
        }
        // Leave agreement/download requests unanswered while two independent
        // policy timers fire. A missing completion would lose the second timer.
        while network.status().counts.pings_completed < 2 {
            tokio::select! {
                result = network.changed() => { result.unwrap(); },
                event = incoming.recv() => {
                    if let net::Event::Message(request) = event.unwrap() {
                        assert_ne!(request.message().kind(), Kind::Ping, "sync member was pinged on wire");
                        pending.push(request);
                    }
                },
            }
        }
    }).await;
    assert_eq!(network.status().counts.pings_completed, 2);
    assert_eq!(network.status().counts.pings_failed, 0);
    assert_eq!(application.status().counts.outgoing_pings, 0);
    assert_eq!(application.status().counts.probes, 0);
    counter(&store, "peers.ping.success", 2);
    counter(&store, "peers.ping.failure", 0);
    assert!(!remote.is_closed());
    for request in pending {
        let reply = snapshot::serve(
            &source.db.pool_view().unwrap(),
            request.message(),
            Default::default(),
        )
        .unwrap()
        .unwrap();
        remote.send(&reply).unwrap().await.unwrap();
    }
    chain_sync::serve_until_finished(&mut sync, &remote, &mut incoming, &source, false, false)
        .await;
    within(async {
        while !sync.peers_in_sync().unwrap().is_empty() {
            sync.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(sync.status().counts.failed, 0);
    // The same connection sends an ordinary ping once it leaves the task.
    let before = network.status().counts.pings_completed;
    let request = within(async {
        loop {
            let net::Event::Message(request) = incoming.recv().await.unwrap() else {
                continue;
            };
            if request.message().kind() == Kind::Ping {
                break request;
            }
            if let Some(reply) = snapshot::serve(
                &source.db.pool_view().unwrap(),
                request.message(),
                Default::default(),
            )
            .unwrap()
            {
                remote.send(&reply).unwrap().await.unwrap();
            }
        }
    })
    .await;
    assert_eq!(request.message().version(), 2);
    let reply = Message::response_ok(&ping(&node, 2, true), Default::default()).unwrap();
    remote.send(&reply).unwrap().await.unwrap();
    within(async {
        while network.status().counts.pings_completed <= before {
            network.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(network.status().counts.pings_completed, before + 1);
    assert_eq!(network.status().counts.pings_failed, 0);
    counter(&store, "peers.ping.success", before + 1);
    counter(&store, "peers.ping.failure", 0);
    assert_eq!(application.status().counts.outgoing_pings, 1);
    // Closing sync also closes its application event receiver. The dependent
    // peer service joins; it must not retain the owner or invent a success.
    sync.shutdown();
    assert!(within(sync_task).await.unwrap().joined);
    within(drain).await.unwrap();
    assert_eq!(sync.peers_in_sync(), Err(sync::Error::Closed));
    assert_eq!(feed.info().rejected, 0);
    application.shutdown();
    let stopped = within(application_task).await.unwrap();
    assert!(stopped.joined);
    assert_eq!(stopped.counts.outgoing_pings, 1);
    counter(&store, "peers.ping.success", before + 1);
    gossip.shutdown();
    network.shutdown();
    assert!(!within(gossip_task).await.unwrap().running);
    assert!(within(network_task).await.unwrap().status.joined);
    within(driver).await.unwrap();
}
