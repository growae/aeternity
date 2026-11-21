use super::*;
use ae_storage::gc::worker::Worker;
use ae_sync::{observe, runtime::connection as owner};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn settings() -> owner::Config {
    owner::Config {
        network: config(),
        health_interval: Duration::from_millis(5),
        observer: observe::Config {
            interval: Duration::from_millis(10),
            ..Default::default()
        },
        maintenance: ae_sync::maintenance::Config {
            poll_interval: Duration::from_millis(5),
        },
        ..Default::default()
    }
}
fn inputs(node: &Node, gc: Option<&Worker>) -> owner::Inputs {
    owner::Inputs {
        database: node.db.clone(),
        admission: node.client(),
        gc: gc.map(Worker::client),
        identity: identities().0,
        execution: common::ledger_fixtures::execution(&NetworkProfile::mainnet()),
        rules: common::ledger_fixtures::rules(node.db.view().unwrap().identity().genesis),
    }
}
fn gc(node: &Node) -> Worker {
    Worker::spawn(
        node.db.clone(),
        ae_chain::repository::retention::Config {
            enabled: false,
            history: 50,
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
}
async fn ready(handle: &mut owner::Handle, number: u64) -> owner::Generation {
    within(async {
        loop {
            let status = handle.status();
            assert!(status.running, "{status:?}");
            if let Some(generation) = handle.current()
                && generation.number >= number
            {
                return generation;
            }
            handle.changed().await.unwrap();
        }
    })
    .await
}
async fn remote(
    node: &Node,
    generation: &owner::Generation,
) -> (net::Client, net::Events, JoinHandle<net::Exit>) {
    let socket = TcpStream::connect(generation.network.status().listening.unwrap())
        .await
        .unwrap();
    establish(
        node,
        socket,
        &identities().1,
        noise::Role::Initiator(identities().0.public_key()),
    )
    .await
}
fn closed(g: &owner::Generation) {
    assert!(g.network.status().joined);
    assert!(g.peer.status().joined);
    assert!(g.sync.status().joined);
    assert!(g.propagation.status().joined);
    assert!(g.observer.status().joined);
    assert!(!g.gossip.status().running);
    assert!(g.maintenance.as_ref().is_none_or(|m| m.status().joined));
    assert!(matches!(
        g.network
            .command(Command::IsBlocked(PeerPublicKey([44; 32]))),
        Err(network::Error::Closed)
    ));
    assert!(matches!(g.gossip.retry_feed(), Err(gossip::Error::Closed)));
    assert_eq!(
        g.sync.peers_in_sync(),
        Err(ae_sync::chain::sync::Error::Closed)
    );
}

#[tokio::test]
async fn composed_owner_restarts_all_connections_preserves_commits_and_independent_workers() {
    let node = Node::new();
    let gc = gc(&node);
    let factories = Arc::new(AtomicUsize::new(0));
    let count = factories.clone();
    let (mut handle, service) = owner::create(inputs(&node, Some(&gc)), settings(), move || {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(Default::default())
    })
    .unwrap();
    let task = tokio::spawn(service.run());
    let mut first = ready(&mut handle, 1).await;
    let (client, _events, driver) = remote(&node, &first).await;
    response(&client, &ping(&node, 1, false)).await;
    within(async {
        while !gc.info().synced {
            first.maintenance.as_mut().unwrap().changed().await.unwrap();
        }
    })
    .await;
    let blocks = chain_sync::seed(&node, 1).await;
    within(async {
        while first.propagation.status().records == 0 {
            first.propagation.changed().await.unwrap();
        }
    })
    .await;
    let cursor = first.propagation.status().cursor;
    let old_events = first.sync.events();
    let old_cursor = old_events.cursor();
    first.peer.shutdown();
    let second = ready(&mut handle, 2).await;
    within(driver).await.unwrap();
    assert!(client.is_closed());
    closed(&first);
    assert!(old_events.info().closed);
    assert!(matches!(
        second.sync.events().subscribe(Some(old_cursor)),
        Err(ae_sync::events::Error::ForeignCursor)
    ));
    assert_eq!(second.propagation.status().cursor, cursor);
    assert_eq!(second.propagation.status().records, 0);
    assert_eq!(
        node.db.view().unwrap().status().tip,
        Some(blocks[0].hash().unwrap())
    );
    assert!(node.client().info().running);
    assert!(gc.info().running && gc.info().synced && !gc.info().enabled);
    let (client, _events, driver) = remote(&node, &second).await;
    response(&client, &ping(&node, 2, false)).await;
    handle.shutdown();
    let status = within(task).await.unwrap();
    within(driver).await.unwrap();
    closed(&second);
    assert!(status.joined && !status.running);
    assert_eq!(status.error, None);
    assert_eq!(status.restart_attempts, 1);
    assert_eq!(status.starts, 2);
    assert_eq!(status.terminated_generations, 2);
    assert_eq!(factories.load(Ordering::SeqCst), 2);
    assert!(node.client().info().running && gc.info().running);
    gc.shutdown().unwrap();
}

#[tokio::test]
async fn partial_construction_failures_join_every_constructed_owner_and_release_storage_lease() {
    let node = Node::new();
    let gc = gc(&node);
    for child in [
        owner::Child::Importer,
        owner::Child::Network,
        owner::Child::Gossip,
        owner::Child::Peer,
        owner::Child::Sync,
        owner::Child::Propagation,
        owner::Child::Maintenance,
        owner::Child::Observer,
    ] {
        let mut config = settings();
        match child {
            owner::Child::Importer => config.importer.blocks = 0,
            owner::Child::Network => config.network.commands = 0,
            owner::Child::Gossip => config.gossip.operations = 0,
            owner::Child::Peer => config.peer.connections = 0,
            owner::Child::Sync => config.sync.peers = 0,
            owner::Child::Propagation => config.propagation.peer_limit = 0,
            owner::Child::Maintenance => config.maintenance.poll_interval = Duration::ZERO,
            owner::Child::Observer => config.observer.records = 0,
        }
        let (handle, service) =
            owner::create(inputs(&node, Some(&gc)), config, || Ok(Default::default())).unwrap();
        let status = within(service.run()).await;
        assert!(status.joined && !status.running, "{status:?}");
        assert!(
            matches!(status.error, Some(owner::Error::Startup(ref f)) if f.child == child),
            "{status:?}"
        );
        assert_eq!(status.restart_attempts, 0);
        assert_eq!(status.starts, 1);
        assert!(handle.current().is_none());
        assert!(node.client().info().running && gc.info().running);
        let (mut healthy, service) = owner::create(inputs(&node, Some(&gc)), settings(), || {
            Ok(Default::default())
        })
        .unwrap();
        let task = tokio::spawn(service.run());
        let generation = ready(&mut healthy, 1).await;
        healthy.shutdown();
        assert!(within(task).await.unwrap().joined);
        closed(&generation);
    }
    gc.shutdown().unwrap();
}

#[tokio::test]
async fn listener_failure_is_initial_start_failure_and_failed_restarts_consume_native_budget() {
    let node = Node::new();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
        panic!()
    };
    let mut config = settings();
    config.network.listen = Some(address);
    let (_handle, service) = owner::create(inputs(&node, None), config.clone(), || {
        Ok(Default::default())
    })
    .unwrap();
    let status = within(service.run()).await;
    assert!(status.joined);
    assert!(
        matches!(status.error, Some(owner::Error::Startup(ref f)) if f.child == owner::Child::Network)
    );
    assert_eq!(status.starts, 1);
    assert_eq!(status.restart_attempts, 0);
    drop(listener);
    let occupied = Arc::new(Mutex::new(None));
    let hold = occupied.clone();
    let factories = Arc::new(AtomicUsize::new(0));
    let count = factories.clone();
    let (mut handle, service) = owner::create(inputs(&node, None), config, move || {
        if count.fetch_add(1, Ordering::SeqCst) == 1 {
            *hold.lock().unwrap() = Some(std::net::TcpListener::bind(address).unwrap());
        }
        Ok(Default::default())
    })
    .unwrap();
    let task = tokio::spawn(service.run());
    let first = ready(&mut handle, 1).await;
    first.network.shutdown();
    let status = within(task).await.unwrap();
    assert_eq!(status.error, Some(owner::Error::RestartLimit));
    assert!(status.joined);
    let reference: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/erlang_connection_owner.json")).unwrap();
    let trace = reference["lifecycle"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["kind"] == "restart_limit")
        .unwrap()["trace"]
        .as_array()
        .unwrap();
    let starts = trace
        .iter()
        .filter(|event| **event == serde_json::json!(["started", "a"]))
        .count();
    let attempts = trace
        .iter()
        .filter(|event| **event == serde_json::json!(["exited", "b"]))
        .count();
    assert_eq!(status.restart_attempts, attempts as u64);
    assert_eq!(status.starts, starts as u64);
    assert_eq!(status.terminated_generations, starts as u64);
    assert_eq!(factories.load(Ordering::SeqCst), starts);
    closed(&first);
    drop(occupied.lock().unwrap().take());
    assert!(std::net::TcpListener::bind(address).is_ok());
}

#[tokio::test]
async fn shutdown_before_start_and_during_replacement_never_exposes_a_new_generation() {
    let node = Node::new();
    let count = Arc::new(AtomicUsize::new(0));
    let factories = count.clone();
    let (handle, service) = owner::create(inputs(&node, None), settings(), move || {
        factories.fetch_add(1, Ordering::SeqCst);
        Ok(Default::default())
    })
    .unwrap();
    handle.shutdown();
    let status = within(service.run()).await;
    assert!(status.joined);
    assert_eq!(status.starts, 0);
    assert_eq!(count.load(Ordering::SeqCst), 0);

    let (handle, service) =
        owner::create(inputs(&node, None), settings(), || Ok(Default::default())).unwrap();
    drop(handle);
    let status = within(service.run()).await;
    assert!(status.joined && status.error.is_none());
    assert_eq!(status.starts, 0);

    let (mut handle, service) =
        owner::create(inputs(&node, None), settings(), || Ok(Default::default())).unwrap();
    let task = tokio::spawn(service.run());
    let generation = ready(&mut handle, 1).await;
    drop(handle);
    let status = within(task).await.unwrap();
    assert!(status.joined && status.error.is_none());
    assert_eq!(status.restart_attempts, 0);
    closed(&generation);

    let control = Arc::new(Mutex::new(None::<owner::Handle>));
    let stop = control.clone();
    let factories = count.clone();
    let (mut handle, service) = owner::create(inputs(&node, None), settings(), move || {
        if factories.fetch_add(1, Ordering::SeqCst) == 1 {
            stop.lock().unwrap().as_ref().unwrap().shutdown();
        }
        Ok(Default::default())
    })
    .unwrap();
    *control.lock().unwrap() = Some(handle.clone());
    let task = tokio::spawn(service.run());
    let first = ready(&mut handle, 1).await;
    first.sync.shutdown();
    let status = within(task).await.unwrap();
    assert_eq!(status.starts, 2);
    assert_eq!(status.restart_attempts, 1);
    assert!(status.joined && status.error.is_none());
    assert!(handle.current().is_none());
    closed(&first);
}

#[tokio::test]
async fn independent_gc_exit_stops_the_connection_owner_without_restarting_or_reenabling_it() {
    let node = Node::new();
    let gc = gc(&node);
    let (mut handle, service) = owner::create(inputs(&node, Some(&gc)), settings(), || {
        Ok(Default::default())
    })
    .unwrap();
    let task = tokio::spawn(service.run());
    let generation = ready(&mut handle, 1).await;
    assert!(gc.info().running && !gc.info().enabled);
    assert_eq!(handle.status().restart_attempts, 0);
    gc.shutdown().unwrap();
    let status = within(task).await.unwrap();
    assert_eq!(
        status.error,
        Some(owner::Error::Dependency(owner::Child::Maintenance))
    );
    assert!(status.joined);
    assert_eq!(status.restart_attempts, 0);
    closed(&generation);
    assert!(node.client().info().running);
}

#[tokio::test]
async fn aborting_the_owner_retires_old_handles_but_never_claims_joined_shutdown() {
    let node = Node::new();
    let (mut handle, service) =
        owner::create(inputs(&node, None), settings(), || Ok(Default::default())).unwrap();
    let task = tokio::spawn(service.run());
    let mut generation = ready(&mut handle, 1).await;
    task.abort();
    assert!(within(task).await.unwrap_err().is_cancelled());
    let status = handle.status();
    assert!(!status.joined && !status.running && !status.ready);
    assert_eq!(status.error, Some(owner::Error::Aborted));
    assert!(handle.current().is_none());
    within(async {
        while generation.network.status().running {
            let _ = generation.network.changed().await;
        }
    })
    .await;
    assert!(matches!(
        generation.network.metrics(),
        Err(network::Error::Closed)
    ));
    let (mut next, service) =
        owner::create(inputs(&node, None), settings(), || Ok(Default::default())).unwrap();
    let task = tokio::spawn(service.run());
    ready(&mut next, 1).await;
    next.shutdown();
    assert!(within(task).await.unwrap().joined);
}
