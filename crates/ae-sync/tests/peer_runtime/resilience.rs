use super::*;
use ae_chain::resilience::Config as Policy;
use ae_sync::{
    chain::sync,
    runtime::{connection as owner, workers},
};

async fn policy(sync: &mut sync::Handle, wanted: impl Fn(&sync::Status) -> bool) {
    within(async {
        loop {
            let state = sync.status();
            assert!(state.running, "{state:?}");
            if wanted(&state) {
                return;
            }
            sync.changed().await.unwrap();
        }
    })
    .await;
}

#[tokio::test]
async fn resilience_restart_keeps_encrypted_connections_and_group_recreation_keeps_live_config() {
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
    let binding = bindings.resilience.as_ref().unwrap();
    let old = binding.current().unwrap();
    let source = old.source();
    let (mut owner, service) = owner::create_with_workers(
        worker_owner::inputs(&node, &bindings),
        connection_owner::settings(),
        || Ok(Default::default()),
        bindings.clone(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let mut generation = connection_owner::ready(&mut owner, 1).await;
    let config = Policy {
        depth: 9,
        from_start: false,
    };
    generation.sync.set_resilience(config).unwrap();
    policy(&mut generation.sync, |s| {
        s.lifecycle.policy_config == Some(config)
    })
    .await;
    let cached = generation.sync.status().lifecycle.policy.unwrap().state;
    assert_eq!(cached.configured_depth(), Some(100));
    assert_eq!(cached.recorded_active(), None);
    let (remote, _events, driver) = connection_owner::remote(&node, &generation).await;
    response(&remote, &ping(&node, 1, false)).await;
    policy(&mut generation.sync, |s| {
        s.lifecycle.peer_completions > 0
            && s.lifecycle
                .policy
                .is_some_and(|p| p.state.active_depth() == Some(9))
    })
    .await;
    let activated = old.snapshot().unwrap().await.unwrap();
    assert_eq!(activated.sync_callbacks, 1);
    assert!(!activated.sync_subscribed);
    generation.sync.set_resilience(Policy::default()).unwrap();
    policy(&mut generation.sync, |s| {
        s.lifecycle
            .policy
            .is_some_and(|p| p.state.recorded_active() == Some(0))
    })
    .await;
    assert_eq!(
        workers
            .restart(workers::Child::Resilience, 1)
            .unwrap()
            .wait()
            .await,
        Ok(2)
    );
    assert!(old.info().joined);
    let next = binding.current().unwrap();
    let restarted = next.snapshot().unwrap().await.unwrap();
    assert_eq!(restarted.sync_callbacks, 0);
    assert!(restarted.sync_subscribed);
    assert_eq!(restarted.policy.state.recorded_active(), Some(0));
    assert!(!remote.is_closed());
    assert_eq!(
        (owner.status().starts, owner.status().restart_attempts),
        (1, 0)
    );
    // Refresh the lifecycle while its old completion count remains nonzero.
    // The retained count must not become a new callback on the replacement.
    let live = Policy {
        depth: 7,
        from_start: false,
    };
    generation.sync.set_resilience(live).unwrap();
    policy(&mut generation.sync, |s| {
        s.lifecycle
            .policy
            .is_some_and(|p| p.state.active_depth() == Some(7))
    })
    .await;
    assert!(generation.sync.status().lifecycle.peer_completions > 0);
    let retained = next.snapshot().unwrap().await.unwrap();
    assert_eq!(retained.sync_callbacks, 0);
    assert!(retained.sync_subscribed);
    owner.shutdown();
    assert!(within(task).await.unwrap().joined);
    connection_owner::closed(&generation);
    within(driver).await.unwrap();
    assert_eq!(
        generation.sync.set_resilience(Policy::default()),
        Err(sync::Error::Closed)
    );
    assert_eq!(source.configuration().unwrap().value, live);
    let (mut owner, service) = owner::create_with_workers(
        worker_owner::inputs(&node, &bindings),
        connection_owner::settings(),
        || Ok(Default::default()),
        bindings.clone(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let mut generation = connection_owner::ready(&mut owner, 1).await;
    policy(&mut generation.sync, |s| {
        s.lifecycle.policy_config == Some(live) && s.lifecycle.policy.is_some()
    })
    .await;
    assert_eq!(generation.sync.status().lifecycle.peer_completions, 0);
    assert_eq!(next.snapshot().unwrap().await.unwrap(), retained);
    let (remote, _events, driver) = connection_owner::remote(&node, &generation).await;
    // This argument is the ping protocol version, not a sequence number.
    // An initial v2 ping deliberately skips chain effects.
    response(&remote, &ping(&node, 1, false)).await;
    policy(&mut generation.sync, |s| s.lifecycle.peer_completions > 0).await;
    let completed = next.snapshot().unwrap().await.unwrap();
    assert_eq!(completed.sync_callbacks, 1);
    assert!(!completed.sync_subscribed);
    assert_eq!(completed.policy.state.active_depth(), Some(7));
    owner.shutdown();
    assert!(within(task).await.unwrap().joined);
    within(driver).await.unwrap();
    workers.shutdown();
    assert!(within(workers_task).await.unwrap().joined);
    assert!(source.is_closed() && next.info().joined);
    assert_eq!(
        node.db.view().unwrap().resilience().state,
        Default::default()
    );
}
