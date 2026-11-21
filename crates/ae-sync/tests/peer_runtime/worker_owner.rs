use super::*;
use ae_storage::pool::worker;
use ae_sync::runtime::{connection as owner, workers};
use connection_owner::{closed, ready, remote, settings};

pub(super) fn worker_config() -> workers::Config {
    workers::Config {
        admission: worker::Settings {
            origins_interval: None,
            ..Default::default()
        },
        pool: ae_storage::pool::Config {
            policy: ae_mempool::pool::Config {
                minimum_miner_gas_price: 1.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        poll: Duration::from_millis(5),
        ..Default::default()
    }
}
async fn running(node: &Node) -> (workers::Handle, JoinHandle<workers::Status>) {
    let (mut handle, service) = workers::create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        worker_config(),
        Default::default(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    within(async {
        while !handle.status().ready {
            assert!(handle.status().running, "{:?}", handle.status());
            handle.changed().await.unwrap();
        }
    })
    .await;
    (handle, task)
}
pub(super) fn inputs(node: &Node, bindings: &workers::Bindings) -> owner::Inputs {
    owner::Inputs {
        database: node.db.clone(),
        admission: bindings.admission.current().unwrap().client,
        gc: bindings.gc.as_ref().map(|g| g.current().unwrap()),
        identity: identities().0,
        execution: common::ledger_fixtures::execution(&NetworkProfile::mainnet()),
        rules: common::ledger_fixtures::rules(node.db.view().unwrap().identity().genesis),
    }
}
async fn transaction(events: &mut net::Events) -> Vec<u8> {
    within(async {
        loop {
            let net::Event::Message(received) = events.recv().await.unwrap() else {
                continue;
            };
            if received.message().kind() != Kind::Transactions {
                continue;
            }
            let Value::List(values) = &received.message().fields()["txs"] else {
                panic!()
            };
            let [Value::Binary(tx)] = values.as_slice() else {
                panic!()
            };
            return tx.clone();
        }
    })
    .await
}

#[tokio::test]
async fn independent_pool_and_gc_replacements_preserve_the_same_encrypted_connection_group() {
    let mut node = Node::new();
    node.stop_worker();
    let (workers, worker_task) = running(&node).await;
    let bindings = workers.bindings();
    let old_pool = bindings.admission.current().unwrap().client;
    let old_gc = bindings.gc.as_ref().unwrap().current().unwrap();
    let (mut connection, service) = owner::create_with_workers(
        inputs(&node, &bindings),
        settings(),
        || Ok(Default::default()),
        bindings.clone(),
    )
    .unwrap();
    let connection_task = tokio::spawn(service.run());
    let mut generation = ready(&mut connection, 1).await;
    let (peer, mut events, driver) = remote(&node, &generation).await;
    response(&peer, &ping(&node, 1, false)).await;
    within(async {
        while !old_gc.info().unwrap().synced {
            generation
                .maintenance
                .as_mut()
                .unwrap()
                .changed()
                .await
                .unwrap();
        }
    })
    .await;
    let tx = common::transaction();
    old_pool.force_push(tx.clone()).unwrap().await.unwrap();
    assert_eq!(transaction(&mut events).await, tx.encode());
    assert!(!old_pool.gossip_cache().check(tx.hash()));
    let mut accepted = vec![];
    for _ in 0..8 {
        accepted.push(old_pool.force_push(tx.clone()).unwrap());
    }
    assert_eq!(
        workers
            .restart(workers::Child::Admission, 1)
            .unwrap()
            .wait()
            .await,
        Ok(2)
    );
    for ticket in accepted {
        ticket.await.unwrap();
    }
    let pool = bindings.admission.current().unwrap().client;
    assert!(!old_pool.info().running);
    assert!(matches!(
        old_pool.force_push(tx.clone()),
        Err(worker::Error::Stopped)
    ));
    assert!(pool.gossip_cache().check(tx.hash()));
    assert_eq!(workers.status().gc_generation, 1);
    assert!(old_gc.info().unwrap().synced && !old_gc.info().unwrap().enabled);
    pool.force_push(tx.clone()).unwrap().await.unwrap();
    for _ in 0..9 {
        assert_eq!(transaction(&mut events).await, tx.encode());
    }
    assert_eq!(
        node.db.pool_view().unwrap().members(10).unwrap(),
        vec![tx.hash()]
    );
    assert_eq!(
        workers
            .restart(workers::Child::Admission, 1)
            .unwrap()
            .wait()
            .await,
        Err(workers::Error::Stale)
    );
    assert_eq!(workers.status().restart_attempts, 1);
    assert_eq!(
        workers
            .restart(workers::Child::GarbageCollection, 1)
            .unwrap()
            .wait()
            .await,
        Ok(2)
    );
    let gc = bindings.gc.as_ref().unwrap().current().unwrap();
    within(async {
        while !gc.info().unwrap().synced
            || generation
                .maintenance
                .as_ref()
                .unwrap()
                .status()
                .notifications
                < 2
        {
            generation
                .maintenance
                .as_mut()
                .unwrap()
                .changed()
                .await
                .unwrap();
        }
    })
    .await;
    assert!(!gc.info().unwrap().enabled);
    assert!(old_gc.info().is_none_or(|i| !i.running));
    assert!(!old_gc.on_sync_done(false));
    assert_eq!(workers.status().admission_generation, 2);
    assert!(pool.info().running);
    response(&peer, &ping(&node, 2, false)).await;
    assert!(!peer.is_closed());
    assert_eq!(connection.status().starts, 1);
    assert_eq!(connection.status().restart_attempts, 0);
    assert_eq!(connection.current().unwrap().number, generation.number);
    connection.shutdown();
    assert!(within(connection_task).await.unwrap().joined);
    within(driver).await.unwrap();
    closed(&generation);
    assert!(pool.info().running && gc.info().unwrap().running);
    workers.shutdown();
    assert!(within(worker_task).await.unwrap().joined);
    assert!(!pool.info().running && bindings.admission.is_closed());
}

#[tokio::test]
async fn failed_worker_startup_joins_prior_workers_and_allows_a_fresh_owner() {
    let mut node = Node::new();
    node.stop_worker();
    for child in [workers::Child::Admission, workers::Child::GarbageCollection] {
        let mut config = worker_config();
        if child == workers::Child::Admission {
            config.admission.requests = 0;
        } else {
            config.gc.as_mut().unwrap().limits.rows = 0;
        }
        let (handle, service) = workers::create(
            node.db.clone(),
            NetworkProfile::mainnet(),
            config,
            Default::default(),
        )
        .unwrap();
        let status = within(service.run()).await;
        assert!(matches!(status.error, Some(workers::Error::Start { child: c, .. }) if c == child));
        assert!(status.joined && !status.running);
        assert_eq!(status.restart_attempts, 0);
        assert!(handle.bindings().admission.is_closed());
        let (healthy, task) = running(&node).await;
        healthy.shutdown();
        assert!(within(task).await.unwrap().joined);
    }
}

#[tokio::test]
async fn worker_owner_shutdown_closes_bindings_and_terminates_consumers_without_retrying() {
    let mut node = Node::new();
    node.stop_worker();
    let (workers, worker_task) = running(&node).await;
    let bindings = workers.bindings();
    let (mut connection, service) = owner::create_with_workers(
        inputs(&node, &bindings),
        settings(),
        || Ok(Default::default()),
        bindings.clone(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let generation = ready(&mut connection, 1).await;
    drop(workers);
    assert!(within(worker_task).await.unwrap().joined);
    let status = within(task).await.unwrap();
    assert_eq!(
        status.error,
        Some(owner::Error::Dependency(owner::Child::Gossip))
    );
    assert!(status.joined);
    assert_eq!(status.restart_attempts, 0);
    assert!(bindings.admission.current().is_none());
    closed(&generation);
}

#[tokio::test]
async fn waiting_for_initial_workers_can_be_cancelled_without_opening_networking() {
    let mut node = Node::new();
    let initial_client = node.client();
    node.stop_worker();
    let mut config = worker_config();
    config.gc = None;
    let (workers, service) = workers::create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        config,
        Default::default(),
    )
    .unwrap();
    let inputs = owner::Inputs {
        database: node.db.clone(),
        admission: initial_client,
        gc: None,
        identity: identities().0,
        execution: common::ledger_fixtures::execution(&NetworkProfile::mainnet()),
        rules: common::ledger_fixtures::rules(node.db.view().unwrap().identity().genesis),
    };
    let (connection, network_service) = owner::create_with_workers(
        inputs,
        settings(),
        || Ok(Default::default()),
        workers.bindings(),
    )
    .unwrap();
    let task = tokio::spawn(network_service.run());
    tokio::task::yield_now().await;
    assert_eq!(connection.status().starts, 0);
    assert!(connection.current().is_none());
    connection.shutdown();
    assert!(within(task).await.unwrap().joined);
    workers.shutdown();
    let status = within(service.run()).await;
    assert!(status.joined && status.error.is_none());
    assert_eq!(status.admission_generation, 0);
}
