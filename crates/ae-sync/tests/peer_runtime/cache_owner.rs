use super::*;
use ae_mempool::pool::PushOutcome;
use ae_storage::pool::worker;
use ae_sync::runtime::{connection as owner, workers};
use connection_owner::{closed, ready, remote, settings};
use worker_owner::{inputs, worker_config};

async fn running(
    node: &Node,
    cache: worker::GossipCache,
) -> (workers::Handle, JoinHandle<workers::Status>) {
    let (mut handle, service) = workers::create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        worker_config(),
        cache,
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

#[tokio::test]
async fn connection_cache_restarts_with_its_group_and_survives_independent_admission_replacement() {
    let mut node = Node::new();
    node.stop_worker();
    let (cache, controller) = worker::GossipCache::managed(200, 100_000).unwrap();
    let (workers, worker_task) = running(&node, cache.clone()).await;
    let bindings = workers.bindings();
    let old_pool = bindings.admission.current().unwrap().client;
    assert_eq!(cache.stats(), Err(worker::Error::CacheUnavailable));
    let (mut handle, service) = owner::create_with_owned_cache(
        inputs(&node, &bindings),
        settings(),
        || Ok(Default::default()),
        bindings.clone(),
        controller,
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let first = ready(&mut handle, 1).await;
    let (peer, _events, driver) = remote(&node, &first).await;
    response(&peer, &ping(&node, 1, false)).await;
    let tx = common::transaction();
    assert!(matches!(
        old_pool.receive(tx.clone()).unwrap().await.unwrap().outcome,
        PushOutcome::Accepted { .. }
    ));
    assert_eq!(
        old_pool.receive(tx.clone()).unwrap().await.unwrap().outcome,
        PushOutcome::Cached
    );
    let stats = cache.stats().unwrap();
    workers
        .restart(workers::Child::Admission, 1)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let replacement = bindings.admission.current().unwrap().client;
    assert_eq!(cache.stats().unwrap(), stats);
    assert_eq!(
        replacement
            .receive(tx.clone())
            .unwrap()
            .await
            .unwrap()
            .outcome,
        PushOutcome::Cached
    );
    assert!(first.cache.as_ref().unwrap().is_running());
    assert_eq!(handle.status().starts, 1);
    response(&peer, &ping(&node, 1, false)).await;

    first.cache.as_ref().unwrap().shutdown();
    let second = ready(&mut handle, 2).await;
    closed(&first);
    within(driver).await.unwrap();
    assert_eq!(handle.status().restart_attempts, 1);
    assert_eq!(
        handle.status().last_failure.as_ref().unwrap().child,
        owner::Child::GossipCache
    );
    assert_eq!(workers.status().admission_generation, 2);
    assert!(replacement.info().running);
    assert_eq!(cache.stats().unwrap(), Default::default());
    first.cache.as_ref().unwrap().shutdown();
    assert!(second.cache.as_ref().unwrap().is_running());
    assert!(matches!(
        replacement.receive(tx.clone()).unwrap().await,
        Err(worker::Error::Service(ae_storage::pool::Failure::Policy(
            ae_mempool::pool::Error::AlreadyKnown
        )))
    ));
    assert_eq!(
        replacement.receive(tx).unwrap().await.unwrap().outcome,
        PushOutcome::Cached
    );
    let (peer2, _events2, driver2) = remote(&node, &second).await;
    response(&peer2, &ping(&node, 2, false)).await;
    handle.shutdown();
    let status = within(task).await.unwrap();
    assert!(status.joined && status.error.is_none());
    within(driver2).await.unwrap();
    assert_eq!(cache.stats(), Err(worker::Error::CacheUnavailable));
    assert!(replacement.info().running);
    workers.shutdown();
    assert!(within(worker_task).await.unwrap().joined);
}

#[tokio::test]
async fn partial_start_failure_and_cancelled_start_release_cache_without_restarting_workers() {
    for case in 0..3 {
        let cancelled = case == 1;
        let mut node = Node::new();
        node.stop_worker();
        let (cache, mut controller) = worker::GossipCache::managed(3, 8).unwrap();
        let prior = (case == 2).then(|| controller.start().unwrap());
        let (workers, worker_task) = running(&node, cache.clone()).await;
        let bindings = workers.bindings();
        let pool = bindings.admission.current().unwrap().client;
        let mut config = settings();
        if case == 0 {
            config.gossip.operations = 0;
        }
        let (handle, service) = owner::create_with_owned_cache(
            inputs(&node, &bindings),
            config,
            || Ok(Default::default()),
            bindings,
            controller,
        )
        .unwrap();
        if cancelled {
            handle.shutdown();
        }
        let status = within(service.run()).await;
        assert!(status.joined && status.restart_attempts == 0);
        if cancelled {
            assert!(status.error.is_none());
            assert_eq!(status.starts, 0);
        } else {
            assert!(matches!(status.error, Some(owner::Error::Startup(_))));
        }
        assert_eq!(cache.stats(), Err(worker::Error::CacheUnavailable));
        assert!(prior.as_ref().is_none_or(|p| !p.is_running()));
        if case == 2 {
            assert_eq!(
                status.last_failure.unwrap().child,
                owner::Child::GossipCache
            );
        }
        assert_eq!(workers.status().admission_generation, 1);
        assert!(matches!(
            pool.receive(common::transaction())
                .unwrap()
                .await
                .unwrap()
                .outcome,
            PushOutcome::Accepted { .. }
        ));
        workers.shutdown();
        assert!(within(worker_task).await.unwrap().joined);
    }
}

#[tokio::test]
async fn abort_closes_cache_and_foreign_cache_controller_is_rejected() {
    let mut node = Node::new();
    node.stop_worker();
    let (cache, controller) = worker::GossipCache::managed(3, 8).unwrap();
    let (workers, worker_task) = running(&node, cache.clone()).await;
    let bindings = workers.bindings();
    let (foreign, foreign_controller) = worker::GossipCache::managed(3, 8).unwrap();
    assert!(matches!(
        owner::create_with_owned_cache(
            inputs(&node, &bindings),
            settings(),
            || Ok(Default::default()),
            bindings.clone(),
            foreign_controller
        ),
        Err(owner::Error::Configuration)
    ));
    assert_eq!(foreign.stats(), Err(worker::Error::CacheUnavailable));
    let (mut handle, service) = owner::create_with_owned_cache(
        inputs(&node, &bindings),
        settings(),
        || Ok(Default::default()),
        bindings,
        controller,
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let generation = ready(&mut handle, 1).await;
    task.abort();
    assert!(within(task).await.unwrap_err().is_cancelled());
    assert_eq!(handle.status().error, Some(owner::Error::Aborted));
    assert!(!handle.status().joined);
    assert!(!generation.cache.unwrap().is_running());
    assert_eq!(cache.stats(), Err(worker::Error::CacheUnavailable));
    assert!(workers.status().ready);
    workers.shutdown();
    assert!(within(worker_task).await.unwrap().joined);
}
