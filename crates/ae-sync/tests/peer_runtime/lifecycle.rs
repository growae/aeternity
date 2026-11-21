use super::*;
use ae_chain::resilience;
use ae_sync::chain::sync::{self, lifecycle::DoneReason};
async fn wait(handle: &mut sync::Handle, predicate: impl Fn(&sync::Status) -> bool) {
    within(async {
        loop {
            let status = handle.status();
            assert!(status.running, "{status:?}");
            if predicate(&status) {
                break;
            }
            handle.changed().await.unwrap();
        }
    })
    .await;
}
#[tokio::test]
async fn peer_completion_activates_policy_before_forward_success_and_success_suppresses_later_fetches()
 {
    let native: serde_json::Value =
        serde_json::from_slice(include_bytes!("../fixtures/erlang_sync_lifecycle.json")).unwrap();
    let node = Node::new();
    let source = Node::new();
    let all = chain_sync::seed(&source, 2).await;
    chain_sync::seed(&node, 1).await;
    let app = start(&node, config(), Default::default()).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let (mut sync, _forwarded, finish) = chain_sync::synchronize(&node, app, Default::default());
    let mut business = sync.events().subscribe(None).unwrap();
    response(&remote, &ping(&source, 1, false)).await;
    let request = within(generation_request(&mut events)).await;
    assert_eq!(native["latch"][0]["scheduled"], true);
    wait(&mut sync, |s| {
        s.lifecycle.policy.is_some_and(|p| p.state.synchronized())
    })
    .await;
    assert_eq!(sync.status().lifecycle.peer_completions, 1);
    let Some(ae_sync::events::Delivery::Event(record)) = business.recv().await else {
        panic!()
    };
    assert!(
        matches!(&record.event, sync::lifecycle::Event::PeerDone { completion, syncing: false } if completion.reason == DoneReason::Ping)
    );
    assert_eq!(
        sync.status().lifecycle.gc_ready,
        sync.status().lifecycle.last_peer_done
    );
    assert_eq!(
        sync.status().lifecycle.last_peer_done.unwrap().reason,
        DoneReason::Ping
    );
    assert_eq!(sync.status().counts.finished, 0);
    assert!(!sync.status().lifecycle.last_generation_in_sync);
    assert_eq!(
        node.db.view().unwrap().status().tip,
        Some(all[0].hash().unwrap())
    );
    let response = snapshot::serve(
        &source.db.pool_view().unwrap(),
        request.message(),
        Default::default(),
    )
    .unwrap()
    .unwrap();
    remote.send(&response).unwrap().await.unwrap();
    wait(&mut sync, |s| s.lifecycle.last_generation_in_sync).await;
    assert_eq!(native["latch"][0]["after_success"], true);
    assert_eq!(sync.status().counts.finished, 1);
    assert_eq!(
        node.db.view().unwrap().status().tip,
        Some(all[1].hash().unwrap())
    );
    super::response(&remote, &ping(&source, 1, false)).await;
    wait(&mut sync, |s| s.lifecycle.suppressed_generations == 1).await;
    assert_eq!(native["latch"][1]["scheduled"], false);
    assert_eq!(sync.status().counts.started, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(40), generation_request(&mut events))
            .await
            .is_err()
    );
    sync.shutdown();
    assert!(within(finish).await.joined);
    within(driver).await.unwrap();
}
#[tokio::test]
async fn exhausted_peer_with_failed_final_fetch_reports_completion_without_admission() {
    let node = Node::new();
    let source = Node::new();
    chain_sync::seed(&source, usize::MAX).await;
    let before = node.db.view().unwrap().status();
    let app = start(&node, config(), Default::default()).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let (mut sync, _forwarded, finish) = chain_sync::synchronize(&node, app, Default::default());
    let mut business = sync.events().subscribe(None).unwrap();
    response(&remote, &ping(&source, 1, false)).await;
    within(async {
        loop {
            if sync.status().counts.failed > 0 { break; }
            tokio::select! {
                _ = sync.changed() => (),
                event = events.recv() => if let Some(net::Event::Message(received)) = event {
                    let reply = match received.message().kind() {
                        Kind::GetSuccessors => Some(Message::response_ok(&message(Kind::HeaderHashes, 1, &[("header_hashes", Value::List(vec![]))]), Default::default()).unwrap()),
                        Kind::GetGeneration => Some(Message::response_error(Kind::Generation, b"not_found".to_vec(), Default::default()).unwrap()),
                        _ => snapshot::serve(&source.db.pool_view().unwrap(), received.message(), Default::default()).unwrap(),
                    };
                    if let Some(reply) = reply { remote.send(&reply).unwrap().await.unwrap(); }
                }
            }
        }
    }).await;
    wait(&mut sync, |s| {
        s.lifecycle.policy.is_some_and(|p| p.state.synchronized())
    })
    .await;
    let status = sync.status();
    assert_eq!(status.counts.finished, 0);
    assert_eq!(status.lifecycle.peer_completions, 1);
    assert_eq!(status.lifecycle.gc_ready, status.lifecycle.last_peer_done);
    assert_eq!(
        status.lifecycle.last_peer_done.unwrap().reason,
        DoneReason::Exhausted
    );
    assert!(!status.lifecycle.last_generation_in_sync);
    assert!(!status.lifecycle.progress.unwrap().syncing);
    assert!(matches!(
        status.last.unwrap().result,
        Err(sync::Error::Import(_))
    ));
    assert_eq!(node.db.view().unwrap().status(), before);
    sync.shutdown();
    assert!(within(finish).await.joined);
    let mut completion_seen = false;
    while let Some(delivery) = business.recv().await {
        let ae_sync::events::Delivery::Event(record) = delivery else {
            panic!()
        };
        if let sync::lifecycle::Event::PeerDone {
            completion,
            syncing,
        } = &record.event
        {
            assert!(!syncing);
            assert_eq!(completion.reason, DoneReason::Exhausted);
            completion_seen = true;
        }
    }
    assert!(completion_seen);
    within(driver).await.unwrap();
}
#[tokio::test]
async fn runtime_resilience_configuration_and_restart_retain_database_owned_activation() {
    let node = Node::new();
    let remote_node = Node::new();
    let all = chain_sync::seed(&node, 1).await;
    let app = start(&node, config(), Default::default()).await;
    let (remote, _events, driver) = connect(&node, &app).await;
    let config = sync::Config {
        resilience: resilience::Config {
            depth: 2,
            from_start: true,
        },
        ..Default::default()
    };
    let (mut sync, _forwarded, finish) = chain_sync::synchronize(&node, app, config);
    wait(&mut sync, |s| {
        s.lifecycle
            .policy
            .is_some_and(|p| p.state.active_depth() == Some(2))
    })
    .await;
    let update = resilience::Config {
        depth: 9,
        from_start: false,
    };
    sync.set_resilience(update).unwrap();
    wait(&mut sync, |s| s.lifecycle.policy_config == Some(update)).await;
    assert_eq!(
        sync.status().lifecycle.policy.unwrap().state.active_depth(),
        Some(2)
    );
    assert_eq!(
        sync.status()
            .lifecycle
            .policy
            .unwrap()
            .state
            .configured_depth(),
        Some(2)
    );
    response(&remote, &ping(&remote_node, 1, false)).await;
    wait(&mut sync, |s| {
        s.lifecycle
            .policy
            .is_some_and(|p| p.state.active_depth() == Some(9))
    })
    .await;
    assert_eq!(sync.status().counts.started, 0);
    let disabled = resilience::Config::default();
    sync.set_resilience(disabled).unwrap();
    wait(&mut sync, |s| s.lifecycle.policy_config == Some(disabled)).await;
    assert_eq!(
        sync.status()
            .lifecycle
            .policy
            .unwrap()
            .state
            .recorded_active(),
        Some(0)
    );
    sync.shutdown();
    assert!(within(finish).await.joined);
    remote.shutdown();
    within(driver).await.unwrap();
    let app = start(&node, super::config(), Default::default()).await;
    let config = sync::Config {
        resilience: resilience::Config {
            depth: 3,
            from_start: true,
        },
        ..Default::default()
    };
    let (mut sync, _forwarded, finish) = chain_sync::synchronize(&node, app, config);
    wait(&mut sync, |s| {
        s.lifecycle.policy.is_some() && s.lifecycle.selected.is_some()
    })
    .await;
    let status = sync.status();
    let policy = status.lifecycle.policy.unwrap().state;
    assert!(policy.synchronized());
    assert_eq!(policy.recorded_active(), Some(0));
    assert_eq!(policy.configured_depth(), Some(3));
    assert!(!status.lifecycle.last_generation_in_sync);
    assert_eq!(
        status.lifecycle.selected.unwrap().hash,
        all[0].hash().unwrap()
    );
    let update = resilience::Config {
        depth: 4,
        from_start: false,
    };
    sync.set_resilience(update).unwrap();
    wait(&mut sync, |s| {
        s.lifecycle
            .policy
            .is_some_and(|p| p.state.active_depth() == Some(4))
    })
    .await;
    sync.shutdown();
    assert!(within(finish).await.joined);
}
