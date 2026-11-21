use super::*;
use ae_sync::runtime::{connection as owner, workers};
use connection_owner::{closed, ready, remote, settings};
#[tokio::test]
async fn pool_maintenance_and_admission_replace_without_restarting_encrypted_connection() {
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
    let (mut owner, service) = owner::create_with_workers(
        worker_owner::inputs(&node, &bindings),
        settings(),
        || Ok(Default::default()),
        bindings.clone(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let generation = ready(&mut owner, 1).await;
    let (remote, _events, driver) = remote(&node, &generation).await;
    response(&remote, &ping(&node, 1, false)).await;
    let tx = common::transaction();
    bindings
        .admission
        .current()
        .unwrap()
        .client
        .force_push(tx.clone())
        .unwrap()
        .await
        .unwrap();
    let old = bindings.pool_gc.as_ref().unwrap().current().unwrap();
    old.shorten_ttl(tx.hash(), -1).unwrap().await.unwrap();
    assert_eq!(
        workers
            .restart(workers::Child::PoolMaintenance, 1)
            .unwrap()
            .wait()
            .await,
        Ok(2)
    );
    assert!(old.info().joined);
    let gc = bindings.pool_gc.as_ref().unwrap().current().unwrap();
    assert_eq!(gc.ttl(tx.hash()).unwrap().await.unwrap(), -1);
    assert_eq!(
        workers
            .restart(workers::Child::Admission, 1)
            .unwrap()
            .wait()
            .await,
        Ok(2)
    );
    assert!(gc.ttl(tx.hash()).unwrap().await.unwrap() >= 0);
    assert_eq!(gc.info().last_admission_generation, Some(2));
    assert_eq!(owner.status().generation, 1);
    assert!(!remote.is_closed());
    response(&remote, &ping(&node, 1, false)).await;
    owner.shutdown();
    assert!(within(task).await.unwrap().joined);
    closed(&generation);
    within(driver).await.unwrap();
    workers.shutdown();
    assert!(within(workers_task).await.unwrap().joined);
    assert!(gc.info().joined);
    assert_eq!(
        node.db.pool_view().unwrap().members(10).unwrap(),
        vec![tx.hash()]
    );
}
