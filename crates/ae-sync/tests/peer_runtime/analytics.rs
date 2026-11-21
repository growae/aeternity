use super::*;
use ae_sync::{
    analytics,
    runtime::{connection as owner, workers},
};
use connection_owner::{closed, ready, remote, settings};
async fn start_owned(
    node: &Node,
    enabled: bool,
) -> (
    workers::Handle,
    JoinHandle<workers::Status>,
    owner::Handle,
    JoinHandle<owner::Status>,
    owner::Generation,
    analytics::Client,
) {
    let mut config = worker_owner::worker_config();
    config.analytics = Some(analytics::Config {
        enabled,
        ..Default::default()
    });
    let (mut workers, service) = workers::create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        config,
        Default::default(),
    )
    .unwrap();
    let worker_task = tokio::spawn(service.run());
    within(async {
        while !workers.status().ready {
            assert!(workers.status().running);
            workers.changed().await.unwrap();
        }
    })
    .await;
    let bindings = workers.bindings();
    let client = bindings.analytics.as_ref().unwrap().current().unwrap();
    let (mut owner, service) = owner::create_with_workers(
        worker_owner::inputs(node, &bindings),
        settings(),
        || Ok(Default::default()),
        bindings,
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let generation = ready(&mut owner, 1).await;
    (workers, worker_task, owner, task, generation, client)
}
async fn probe(events: &mut net::Events) {
    within(async {
        loop {
            let m = incoming(events).await;
            if m.message().kind() == Kind::GetNodeInfo {
                return;
            }
        }
    })
    .await
}
async fn snapshot(client: &analytics::Client, pending: usize) -> analytics::Snapshot {
    within(async {
        loop {
            let s = client.snapshot().unwrap().await.unwrap();
            if s.pending == pending {
                return s;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
}
fn metadata() -> peer::info::Metadata {
    peer::info::Metadata {
        version: b"native-test".to_vec(),
        revision: b"rev".to_vec(),
        vendor: b"test-peer".to_vec(),
        os: b"test-os".to_vec(),
    }
}
#[tokio::test]
async fn analytics_probes_coalesce_and_restart_drains_before_rebinding_same_encrypted_socket() {
    let mut node = Node::new();
    node.stop_worker();
    let (workers, worker_task, owner, task, generation, old) = start_owned(&node, true).await;
    let (remote, mut events, driver) = remote(&node, &generation).await;
    response(&remote, &ping(&node, 1, false)).await;
    probe(&mut events).await;
    let first = snapshot(&old, 1).await;
    let peer = identities().1.public_key();
    let before = &first.stats[&peer];
    assert_eq!(before.host, analytics::Host::Binary(b"127.0.0.1".to_vec()));
    assert_eq!(before.port, 4321u64.into());
    assert_eq!(before.info, analytics::PeerInfo::Unresolved);
    response(&remote, &ping(&node, 1, false)).await;
    assert_eq!(snapshot(&old, 1).await.stats.len(), 1);
    let restart = workers.restart(workers::Child::Analytics, 1).unwrap();
    within(async {
        while old.info().accepting {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(workers.status().analytics_generation, 1);
    assert!(workers.bindings().analytics.unwrap().current().is_none());
    assert!(!remote.is_closed());
    let reply = peer::info::response(
        Some(&metadata()),
        b"remote-network",
        &1u64.into(),
        &2u64.into(),
        Default::default(),
    )
    .unwrap()
    .unwrap();
    remote.send(&reply).unwrap().await.unwrap();
    assert_eq!(within(restart.wait()).await, Ok(2));
    assert!(old.info().joined);
    assert_eq!(old.info().probe_completions, 1);
    assert!(matches!(old.snapshot(), Err(analytics::Error::Closed)));
    let next = workers.bindings().analytics.unwrap().current().unwrap();
    assert!(snapshot(&next, 0).await.stats.is_empty());
    assert_eq!(owner.status().generation, 1);
    assert!(!remote.is_closed());
    response(&remote, &ping(&node, 1, false)).await;
    probe(&mut events).await;
    remote
        .send(
            &Message::response_error(Kind::NodeInfo, b"refused".to_vec(), Default::default())
                .unwrap(),
        )
        .unwrap()
        .await
        .unwrap();
    let second = snapshot(&next, 0).await;
    assert_eq!(second.stats[&peer].info, analytics::PeerInfo::Unknown);
    response(&remote, &ping(&node, 1, false)).await;
    assert_eq!(
        snapshot(&next, 0).await.stats[&peer].info,
        analytics::PeerInfo::Unknown
    );
    assert_eq!(next.info().probe_completions, 1);
    assert_eq!(
        within(
            workers
                .restart(workers::Child::Analytics, 2)
                .unwrap()
                .wait()
        )
        .await,
        Ok(3)
    );
    let final_client = workers.bindings().analytics.unwrap().current().unwrap();
    response(&remote, &ping(&node, 1, false)).await;
    probe(&mut events).await;
    remote.send(&reply).unwrap().await.unwrap();
    let resolved = snapshot(&final_client, 0).await;
    let expected = metadata();
    assert_eq!(
        resolved.stats[&peer].info,
        analytics::PeerInfo::Resolved(analytics::NodeInfo {
            network_id: b"remote-network".to_vec(),
            version: expected.version,
            revision: expected.revision,
            vendor: expected.vendor,
            os: expected.os
        })
    );
    assert_eq!(owner.status().generation, 1);
    assert!(!remote.is_closed());
    assert_eq!(workers.status().admission_generation, 1);
    assert_eq!(workers.status().gc_generation, 1);
    workers.shutdown();
    assert!(within(worker_task).await.unwrap().joined);
    let status = within(task).await.unwrap();
    assert_eq!(status.error, Some(owner::Error::AnalyticsClosed));
    assert_eq!(status.restart_attempts, 0);
    closed(&generation);
    within(driver).await.unwrap();
    assert!(remote.is_closed());
}
#[tokio::test]
async fn analytics_temporary_pings_record_without_probes_and_disabled_owner_ignores_them() {
    for enabled in [false, true] {
        let mut node = Node::new();
        node.stop_worker();
        let (workers, worker_task, owner, task, generation, client) =
            start_owned(&node, enabled).await;
        let (remote, _events, driver) = remote(&node, &generation).await;
        response(&remote, &ping(&node, 1, true)).await;
        let s = snapshot(&client, 0).await;
        assert_eq!(s.enabled, enabled);
        assert_eq!(s.stats.len(), usize::from(enabled));
        assert_eq!(client.info().probe_completions, 0);
        if enabled {
            assert_eq!(
                s.stats[&identities().1.public_key()].info,
                analytics::PeerInfo::Unresolved
            );
        }
        owner.shutdown();
        assert!(within(task).await.unwrap().joined);
        within(driver).await.unwrap();
        workers.shutdown();
        assert!(within(worker_task).await.unwrap().joined);
        closed(&generation);
    }
}
#[tokio::test]
async fn analytics_shutdown_drains_actual_native_timeout_before_joining_owner() {
    let mut node = Node::new();
    node.stop_worker();
    let (workers, worker_task, _owner, task, generation, client) = start_owned(&node, true).await;
    let (remote, mut events, driver) = remote(&node, &generation).await;
    response(&remote, &ping(&node, 1, false)).await;
    probe(&mut events).await;
    assert_eq!(snapshot(&client, 1).await.stats.len(), 1);
    // Keep the actual remote driver alive without replying to GetNodeInfo.
    workers.shutdown();
    let status = within(worker_task).await.unwrap();
    assert!(status.joined);
    assert_eq!(client.info().probe_completions, 1);
    assert_eq!(client.info().probes, 0);
    assert!(client.info().joined);
    assert!(within(task).await.unwrap().joined);
    within(driver).await.unwrap();
    assert!(remote.is_closed());
    closed(&generation);
}
