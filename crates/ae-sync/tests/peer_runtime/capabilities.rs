use super::*;
use ae_sync::peer::capabilities::{self, CHAIN_POI, Value as Capability};

fn advertised(received: &net::Received) -> Vec<message::ChainPoi> {
    let Details::Ping {
        capabilities: Some(Ok(caps)),
        ..
    } = body(received).details()
    else {
        panic!()
    };
    caps.clone()
}
fn announce(node: &Node, caps: &[message::ChainPoi]) -> Message {
    alter(
        &ping(node, 2, true),
        &[(
            "capabilities",
            Value::Binary(message::encode_capabilities(caps, Default::default()).unwrap()),
        )],
    )
}
async fn registered(app: &mut App, expected: message::ChainPoi) -> net::Client {
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Status { .. }
    ));
    let event = observed(&mut app.events).await;
    let peer::EventKind::Capabilities { client, chain_poi } = event.kind() else {
        panic!()
    };
    assert_eq!(*chain_poi, expected);
    let records = app
        .application
        .peers_with_capability(CHAIN_POI)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].peer, client.peer());
    assert_eq!(&records[0].generation, client.generation());
    assert_eq!(records[0].value, Capability::ChainPoi(vec![expected]));
    client.clone()
}
async fn empty(app: &mut App) {
    within(async {
        while app.application.status().connections != 0 {
            app.application.changed().await.unwrap();
        }
    })
    .await;
    assert!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn mutable_capabilities_reach_encrypted_pings_and_remote_queries_and_close_cleanup() {
    let node = Node::new();
    let mut app = start(
        &node,
        config(),
        peer::Config {
            capabilities: vec![cap(0), cap(1)],
            capability_limits: capabilities::Limits {
                value_bytes: 400,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    let (client, _events, remote_task) = connect(&node, &app).await;
    let request = announce(&node, &[cap(3), cap(4)]);
    assert_eq!(advertised(&response(&client, &request).await), vec![cap(1)]);
    // Initial-v2 negotiation does not register the remote advertisement.
    assert!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap()
            .is_empty()
    );
    app.application
        .set_capability(CHAIN_POI, &Capability::ChainPoi(vec![cap(2)]))
        .await
        .unwrap();
    assert!(matches!(
        app.application
            .set_capability(CHAIN_POI, &Capability::Opaque(vec![0; 401]))
            .await,
        Err(peer::Error::Capabilities(capabilities::Error::Capacity))
    ));
    assert_eq!(advertised(&response(&client, &request).await), vec![cap(2)]);
    let owner = registered(&mut app, cap(4)).await;
    app.application.delete_capability(CHAIN_POI).await.unwrap();
    assert!(advertised(&response(&client, &announce(&node, &[])).await).is_empty());
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Status { .. }
    ));
    assert_eq!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap()[0]
            .value,
        Capability::ChainPoi(vec![cap(4)])
    );
    // A valid unknown-only payload decodes to empty and retains registration.
    let unknown = alter(
        &ping(&node, 2, true),
        &[(
            "capabilities",
            Value::Binary(hex::decode("cac98666757475726581c0").unwrap()),
        )],
    );
    response(&client, &unknown).await;
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Status { .. }
    ));
    assert_eq!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap()[0]
            .value,
        Capability::ChainPoi(vec![cap(4)])
    );
    let extension = Capability::Opaque(b"application-defined".to_vec());
    app.application
        .set_capability("future", &extension)
        .await
        .unwrap();
    assert_eq!(
        app.application.capability("future").await.unwrap(),
        Some(extension.clone())
    );
    assert_eq!(
        app.application.capabilities().await.unwrap()["future"],
        extension
    );
    // Version one does not consult the provider; production v2 fails explicitly.
    assert_eq!(
        body(&response(&client, &ping(&node, 1, true)).await).version(),
        1
    );
    let failed = client.request(&ping(&node, 2, true), None).unwrap();
    assert!(within(failed).await.is_err());
    within(remote_task).await.unwrap();
    empty(&mut app).await;
    assert!(owner.is_closed());
    assert_eq!(
        app.application.status().last_error,
        Some(peer::Error::Capabilities(
            capabilities::Error::UnsupportedKey("future".into())
        ))
    );
    assert!(app.application.status().running);
    app.application.delete_capability("future").await.unwrap();
    stop(app).await;
}

#[tokio::test]
async fn capability_registration_replaced_connection_and_owner_abort_release_values() {
    let node = Node::new();
    let mut app = start(&node, config(), Default::default()).await;
    let (first, _events, task) = connect(&node, &app).await;
    response(&first, &announce(&node, &[cap(1)])).await;
    response(&first, &announce(&node, &[cap(1)])).await;
    let old = registered(&mut app, cap(1)).await;
    first.shutdown();
    within(task).await.unwrap();
    empty(&mut app).await;
    let (second, _events, task) = connect(&node, &app).await;
    response(&second, &announce(&node, &[cap(2)])).await;
    assert!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap()
            .is_empty()
    );
    response(&second, &announce(&node, &[cap(2)])).await;
    let replacement = registered(&mut app, cap(2)).await;
    assert_ne!(old.generation(), replacement.generation());
    old.shutdown();
    assert_eq!(
        &app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap()[0]
            .generation,
        replacement.generation()
    );
    app.application_task.abort();
    assert!(
        within(&mut app.application_task)
            .await
            .unwrap_err()
            .is_cancelled()
    );
    assert!(matches!(
        app.application.peers_with_capability(CHAIN_POI).await,
        Err(peer::Error::Capabilities(capabilities::Error::Closed))
    ));
    assert!(matches!(
        app.application
            .set_capability("future", &Capability::Opaque(vec![]))
            .await,
        Err(peer::Error::Capabilities(capabilities::Error::Closed))
    ));
    within(task).await.unwrap();
    app.gossip.shutdown();
    app.network.shutdown();
    assert!(!within(app.gossip_task).await.unwrap().running);
    assert!(within(app.network_task).await.unwrap().status.joined);
}

#[tokio::test]
async fn capability_malformed_unknown_after_known_closes_without_partial_registration() {
    let node = Node::new();
    let mut app = start(&node, config(), Default::default()).await;
    let (client, _events, task) = connect(&node, &app).await;
    response(&client, &announce(&node, &[cap(1)])).await;
    response(&client, &announce(&node, &[cap(1)])).await;
    registered(&mut app, cap(1)).await;
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/erlang_capabilities.json")).unwrap();
    let case = fixture["decodings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "known_then_malformed")
        .unwrap();
    let malformed = alter(
        &ping(&node, 2, true),
        &[(
            "capabilities",
            Value::Binary(hex::decode(case["wire"].as_str().unwrap()).unwrap()),
        )],
    );
    assert!(
        within(client.request(&malformed, None).unwrap())
            .await
            .is_err()
    );
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Status { .. }
    ));
    within(task).await.unwrap();
    empty(&mut app).await;
    assert_eq!(
        app.application.status().last_error,
        Some(peer::Error::MalformedCapabilities)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(25), app.events.recv())
            .await
            .is_err()
    );
    stop(app).await;
}

#[tokio::test]
async fn independent_capabilities_restart_clears_maps_without_restarting_encrypted_connections() {
    use ae_sync::runtime::{connection as owner, workers};
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/erlang_capabilities_worker.json")).unwrap();
    let native = &fixture["rows"][0];
    assert_eq!(native["cause"], "normal");
    let mut node = Node::new();
    node.stop_worker();
    let (mut workers, service) = workers::create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        worker_owner::worker_config(),
        Default::default(),
    )
    .unwrap();
    let workers_task = tokio::spawn(service.run());
    within(async {
        while !workers.status().ready {
            assert!(workers.status().running);
            workers.changed().await.unwrap();
        }
    })
    .await;
    let bindings = workers.bindings();
    let binding = bindings.capabilities.as_ref().unwrap();
    let (mut owner, service) = owner::create_with_workers(
        worker_owner::inputs(&node, &bindings),
        connection_owner::settings(),
        || Ok(Default::default()),
        bindings.clone(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let generation = connection_owner::ready(&mut owner, 1).await;
    let (remote, _events, driver) = connection_owner::remote(&node, &generation).await;
    generation
        .peer
        .set_capability(CHAIN_POI, &Capability::ChainPoi(vec![cap(1)]))
        .await
        .unwrap();
    let request = announce(&node, &[cap(2), cap(3)]);
    response(&remote, &request).await;
    assert_eq!(advertised(&response(&remote, &request).await), vec![cap(1)]);
    let before = generation
        .peer
        .peers_with_capability(CHAIN_POI)
        .await
        .unwrap();
    assert_eq!(
        before.len() as u64,
        native["before"]["peer_entries"].as_u64().unwrap()
    );
    assert_eq!(before[0].value, Capability::ChainPoi(vec![cap(3)]));
    let old = binding.current().unwrap();
    assert_eq!(
        workers
            .restart(workers::Child::Capabilities, 1)
            .unwrap()
            .wait()
            .await,
        Ok(2)
    );
    assert!(old.info().joined);
    assert!(matches!(
        old.local(),
        Err(capabilities::worker::Error::Worker(
            capabilities::worker::Fault::Stopped
        ))
    ));
    assert_eq!(
        generation.peer.capabilities().await.unwrap().len() as u64,
        native["after"]["local_entries"].as_u64().unwrap()
    );
    assert!(
        generation
            .peer
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(owner.status().generation, 1);
    assert!(!remote.is_closed());
    // A real accepted announcement registers again. No old advertisement is
    // restored merely because the independent worker has been replaced.
    assert!(advertised(&response(&remote, &announce(&node, &[cap(4)])).await).is_empty());
    let after = generation
        .peer
        .peers_with_capability(CHAIN_POI)
        .await
        .unwrap();
    assert_eq!(after[0].generation, before[0].generation);
    assert_eq!(after[0].value, Capability::ChainPoi(vec![cap(4)]));
    generation
        .peer
        .set_capability(CHAIN_POI, &Capability::ChainPoi(vec![cap(5)]))
        .await
        .unwrap();
    assert_eq!(advertised(&response(&remote, &request).await), vec![cap(5)]);
    owner.shutdown();
    assert!(within(task).await.unwrap().joined);
    connection_owner::closed(&generation);
    within(driver).await.unwrap();
    assert!(
        binding
            .current()
            .unwrap()
            .peers_with(CHAIN_POI)
            .unwrap()
            .await
            .unwrap()
            .is_empty()
    );
    // Connection-owner shutdown does not clear independent local capabilities.
    assert_eq!(
        binding
            .current()
            .unwrap()
            .get(CHAIN_POI)
            .unwrap()
            .await
            .unwrap(),
        Some(Capability::ChainPoi(vec![cap(5)]))
    );
    workers.shutdown();
    assert!(within(workers_task).await.unwrap().joined);
}

#[tokio::test]
async fn empty_capability_announcements_remain_valid_below_minimum_stored_value_size() {
    let node = Node::new();
    let mut app = start(
        &node,
        config(),
        peer::Config {
            capability_limits: capabilities::Limits {
                value_bytes: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    let (client, _events, task) = connect(&node, &app).await;
    let request = announce(&node, &[]);
    for _ in 0..3 {
        assert!(advertised(&response(&client, &request).await).is_empty());
    }
    assert!(!client.is_closed());
    assert!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(app.application.status().counts.failed_connections, 0);
    client.shutdown();
    within(task).await.unwrap();
    empty(&mut app).await;
    stop(app).await;
}
