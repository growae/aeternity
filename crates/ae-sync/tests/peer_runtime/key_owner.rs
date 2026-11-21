use super::*;
use ae_storage::pool::worker::GossipCache;
use ae_sync::{
    keys,
    runtime::{connection as owner, workers},
};
use connection_owner::{ready, settings};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "ae-key-runtime-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn config(&self) -> keys::Config {
        keys::Config::new(self.0.join("keys"), b"runtime synthetic password".to_vec())
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
async fn start(
    node: &Node,
    keys: keys::Config,
    cache: GossipCache,
) -> (workers::Handle, JoinHandle<workers::Status>) {
    let mut config = worker_owner::worker_config();
    config.keys = Some(keys);
    let (mut handle, service) =
        workers::create(node.db.clone(), NetworkProfile::mainnet(), config, cache).unwrap();
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
async fn key_replacement_keeps_persisted_identity_and_same_encrypted_connection() {
    let mut node = Node::new();
    node.stop_worker();
    let directory = Directory::new();
    let (cache, controller) = GossipCache::managed(200, 100_000).unwrap();
    let (workers, worker_task) = start(&node, directory.config(), cache).await;
    let bindings = workers.bindings();
    let old = bindings.keys.as_ref().unwrap().current().unwrap();
    let identity = old.peer_identity().unwrap().await.unwrap();
    let mut input = worker_owner::inputs(&node, &bindings);
    input.identity = identity.clone();
    let (mut handle, service) = owner::create_with_owned_cache(
        input,
        settings(),
        || Ok(Default::default()),
        bindings.clone(),
        controller,
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let first = ready(&mut handle, 1).await;
    let socket = TcpStream::connect(first.network.status().listening.unwrap())
        .await
        .unwrap();
    let (peer, _events, driver) = establish(
        &node,
        socket,
        &identities().1,
        noise::Role::Initiator(identity.public_key()),
    )
    .await;
    response(&peer, &ping(&node, 1, false)).await;
    let candidate = old.info().keys.candidate;
    old.promote_candidate(candidate).unwrap().await.unwrap();
    assert_eq!(old.info().keys.signing, candidate);
    assert_eq!(
        workers
            .restart(workers::Child::Keys, 1)
            .unwrap()
            .wait()
            .await,
        Ok(2)
    );
    let next = bindings.keys.as_ref().unwrap().current().unwrap();
    assert_eq!(next.info().keys.peer, identity.public_key());
    assert_ne!(next.info().keys.signing, candidate);
    assert!(matches!(
        old.promote_candidate(candidate),
        Err(keys::Error::Stopped)
    ));
    assert_eq!(
        next.promote_candidate(candidate).unwrap().await,
        Err(keys::Error::KeyNotFound)
    );
    response(&peer, &ping(&node, 1, false)).await;
    assert_eq!(handle.status().starts, 1);
    assert_eq!(handle.status().restart_attempts, 0);
    assert_eq!(
        (
            workers.status().keys_generation,
            workers.status().admission_generation,
            workers.status().gc_generation
        ),
        (2, 1, 1)
    );
    handle.shutdown();
    assert!(within(task).await.unwrap().joined);
    within(driver).await.unwrap();
    assert!(next.info().running);
    workers.shutdown();
    assert!(within(worker_task).await.unwrap().joined);
}

#[tokio::test]
async fn startup_failure_unwinds_keys_and_storage_workers_in_reverse_order() {
    for stage in 0..3 {
        let mut node = Node::new();
        node.stop_worker();
        let directory = Directory::new();
        let keys = directory.config();
        let mut config = worker_owner::worker_config();
        config.keys = Some(keys.clone());
        match stage {
            0 => config.keys.as_mut().unwrap().requests = 0,
            1 => config.admission.requests = 0,
            _ => config.gc.as_mut().unwrap().limits.rows = 0,
        }
        let (handle, service) = workers::create(
            node.db.clone(),
            NetworkProfile::mainnet(),
            config,
            Default::default(),
        )
        .unwrap();
        let status = within(service.run()).await;
        assert!(status.joined && status.error.is_some());
        assert_eq!(status.restart_attempts, 0);
        assert!(handle.bindings().keys.as_ref().unwrap().is_closed());
        assert_eq!(status.keys_generation, u64::from(stage != 0));
        let (healthy, task) = start(&node, keys, Default::default()).await;
        healthy.shutdown();
        assert!(within(task).await.unwrap().joined);
    }
}

#[tokio::test]
async fn closed_key_source_or_changed_peer_identity_stops_consumers_without_retries() {
    for changed in [false, true] {
        let mut node = Node::new();
        node.stop_worker();
        let directory = Directory::new();
        let key_config = directory.config();
        let (workers, worker_task) = start(&node, key_config.clone(), Default::default()).await;
        let bindings = workers.bindings();
        let client = bindings.keys.as_ref().unwrap().current().unwrap();
        let mut input = worker_owner::inputs(&node, &bindings);
        input.identity = client.peer_identity().unwrap().await.unwrap();
        let (mut handle, service) =
            owner::create_with_workers(input, settings(), || Ok(Default::default()), bindings)
                .unwrap();
        let task = tokio::spawn(service.run());
        ready(&mut handle, 1).await;
        if changed {
            // Deliberate out-of-band corruption/reconfiguration of this test's
            // synthetic directory; production writers honor the owner lease.
            let other = Directory::new();
            let alternate = other.config();
            keys::Worker::spawn(alternate.clone())
                .unwrap()
                .shutdown()
                .unwrap();
            for name in ["peer_key.pub", "peer_key"] {
                fs::copy(
                    alternate.directory.join(name),
                    key_config.directory.join(name),
                )
                .unwrap();
            }
            workers
                .restart(workers::Child::Keys, 1)
                .unwrap()
                .wait()
                .await
                .unwrap();
        } else {
            workers.shutdown();
        }
        let status = within(task).await.unwrap();
        assert!(status.joined);
        assert_eq!(status.restart_attempts, 0);
        assert_eq!(
            status.error,
            Some(if changed {
                owner::Error::IdentityChanged
            } else {
                owner::Error::KeysClosed
            })
        );
        workers.shutdown();
        assert!(within(worker_task).await.unwrap().joined);
    }
}
