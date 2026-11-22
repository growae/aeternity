use super::*;
use ae_sync::{
    peer::capabilities::{CHAIN_POI, Value},
    runtime::workers::{self as supervisor, Child, hyperchain as optional},
};
use std::{path::PathBuf, sync::atomic::AtomicU64};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Resources {
    keys: PathBuf,
    _ssdp: tokio::net::UdpSocket,
}
impl Drop for Resources {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.keys);
    }
}
async fn all_services() -> (optional::Config, Resources) {
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let std::net::SocketAddr::V4(address) = socket.local_addr().unwrap() else {
        panic!()
    };
    let keys = std::env::temp_dir().join(format!(
        "ae-hc-optional-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let config = optional::Config {
        upnp: Some(ae_sync::upnp::Config {
            system_gateways: false,
            network: ae_sync::upnp::Settings {
                bind: Ipv4Addr::LOCALHOST,
                ssdp: address,
                discovery_timeout: Duration::from_millis(20),
                request_timeout: Duration::from_millis(20),
                ..Default::default()
            },
            ..Default::default()
        }),
        keys: Some(ae_sync::keys::Config::new(keys.clone(), b"test".to_vec())),
        routing: Some(Default::default()),
        analytics: Some(ae_sync::analytics::Config {
            enabled: true,
            ..Default::default()
        }),
        poll: Duration::from_millis(5),
        ..Default::default()
    };
    (
        config,
        Resources {
            keys,
            _ssdp: socket,
        },
    )
}
async fn worker_ready(h: &mut optional::Handle<Otp26>) {
    within(async {
        while !h.status().ready {
            assert!(h.status().running, "{:?}", h.status());
            h.changed().await.unwrap();
        }
    })
    .await;
}
fn policy_producer(node: &Node) -> Conductor<Otp26> {
    let mut cfg = config(node, false);
    cfg.resilience = Some(Default::default());
    new(node, cfg).unwrap()
}
async fn admission_live(node: &Node, importer: &ae_sync::chain::Importer<Otp26>) {
    let block = node
        .db
        .view()
        .unwrap()
        .require_block(node.config.identity.genesis)
        .unwrap();
    importer
        .import_block(block.encode().unwrap(), 1_000_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
}
fn generation(s: &supervisor::Status, child: Child) -> u64 {
    match child {
        Child::Upnp => s.upnp_generation,
        Child::Routing => s.routing_generation,
        Child::Keys => s.keys_generation,
        Child::Admission => s.admission_generation,
        Child::Analytics => s.analytics_generation,
        Child::PoolMaintenance => s.pool_gc_generation,
        Child::Resilience => s.resilience_generation,
        Child::Capabilities => s.capabilities_generation,
        Child::GarbageCollection => s.gc_generation,
        Child::Votes => s.votes_generation,
    }
}
fn old_stopped(bindings: &supervisor::Bindings, child: Child) -> Box<dyn Fn() -> bool> {
    match child {
        Child::Upnp => {
            let c = bindings.upnp.as_ref().unwrap().current().unwrap();
            Box::new(move || c.info().joined)
        }
        Child::Routing => {
            let c = bindings.routing.as_ref().unwrap().current().unwrap();
            Box::new(move || !c.info().running)
        }
        Child::Keys => {
            let c = bindings.keys.as_ref().unwrap().current().unwrap();
            Box::new(move || !c.info().running)
        }
        Child::Analytics => {
            let c = bindings.analytics.as_ref().unwrap().current().unwrap();
            Box::new(move || c.info().joined)
        }
        Child::PoolMaintenance => {
            let c = bindings.pool_gc.as_ref().unwrap().current().unwrap();
            Box::new(move || c.info().joined)
        }
        Child::Resilience => {
            let c = bindings.resilience.as_ref().unwrap().current().unwrap();
            Box::new(move || c.info().joined)
        }
        Child::Capabilities => {
            let c = bindings.capabilities.as_ref().unwrap().current().unwrap();
            Box::new(move || c.info().joined)
        }
        Child::GarbageCollection => {
            let c = bindings.gc.as_ref().unwrap().current().unwrap();
            Box::new(move || c.info().is_none_or(|s| !s.running))
        }
        _ => panic!(),
    }
}
struct Composed {
    peer: Running,
    workers: optional::Handle<Otp26>,
    task: JoinHandle<supervisor::Status>,
    _resources: Resources,
}
async fn composed(node: &Node) -> Composed {
    let (mut c, controller) = prepared_with_policy(node, Some(Default::default()));
    let (config, resources) = all_services().await;
    let (mut workers, service) = optional::create(&mut c, config).unwrap();
    let task = tokio::spawn(service.run());
    worker_ready(&mut workers).await;
    let identity = workers
        .bindings()
        .keys
        .unwrap()
        .current()
        .unwrap()
        .peer_identity()
        .unwrap()
        .await
        .unwrap();
    let (mut handle, service) = connections::create_hyperchain_with_workers(
        &c,
        &workers,
        identity.clone(),
        controller,
        settings(),
        || Ok(Default::default()),
    )
    .unwrap();
    let importer = c.importer();
    let pool = c.pool();
    let votes = c.votes();
    let peer_task = tokio::spawn(service.run());
    ready(&mut handle, 1).await;
    let clock = ManualClock(Arc::new(Mutex::new((1000, Instant::now()))));
    let producer = match producer_runtime::Runtime::spawn(
        c,
        clock.clone(),
        producer_runtime::Settings {
            poll: Duration::from_millis(1),
            outputs: 16,
        },
    ) {
        Ok(p) => p,
        Err(e) => panic!("{}", e.error),
    };
    Composed {
        peer: Running {
            producer,
            handle,
            task: peer_task,
            clock,
            identity,
            importer,
            pool,
            votes,
        },
        workers,
        task,
        _resources: resources,
    }
}
async fn stop_all(c: Composed) {
    let g = c.peer.handle.current().unwrap();
    c.peer.handle.shutdown();
    let s = within(c.peer.task).await.unwrap();
    assert!(s.joined && s.error.is_none(), "{s:?}");
    closed(&g);
    assert!(g.maintenance.unwrap().status().joined);
    c.workers.shutdown();
    let s = within(c.task).await.unwrap();
    assert!(s.joined && s.error.is_none(), "{s:?}");
    assert!(c.peer.pool.info().accepting && c.peer.votes.info().accepting);
    let p = c.peer.producer.shutdown().unwrap();
    assert!(p.status.failure.is_none(), "{:?}", p.status);
}

#[tokio::test]
async fn all_optional_workers_share_producer_peers_across_independent_replacements() {
    let a_node = Node::new(1);
    let b_node = Node::new(1);
    let mut a = composed(&a_node).await;
    let b = composed(&b_node).await;
    produce(&a.peer, 2).await;
    link(&b.peer, &a.peer).await;
    same_tip(&a_node, &b_node, &a.peer, &b.peer).await;
    let policy = b.workers.bindings().resilience.unwrap().current().unwrap();
    let policy_source = policy.source();
    within(async {
        while !policy
            .snapshot()
            .unwrap()
            .await
            .unwrap()
            .policy
            .state
            .synchronized()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    policy_source
        .configure(ae_chain::resilience::Config {
            depth: 17,
            from_start: true,
        })
        .unwrap();
    let before_policy = policy.snapshot().unwrap().await.unwrap();
    assert_eq!(before_policy.policy.state.recorded_active(), Some(17));
    for (c, children) in [
        (
            &a,
            vec![
                Child::Keys,
                Child::Routing,
                Child::Analytics,
                Child::PoolMaintenance,
                Child::Capabilities,
            ],
        ),
        (
            &b,
            vec![Child::GarbageCollection, Child::Resilience, Child::Upnp],
        ),
    ] {
        let initial = c.peer.handle.current().unwrap().number;
        for child in children {
            let old = old_stopped(&c.workers.bindings(), child);
            let before = generation(&c.workers.status(), child);
            assert_eq!(
                c.workers
                    .restart(child, before)
                    .unwrap()
                    .wait()
                    .await
                    .unwrap(),
                before + 1
            );
            assert!(old());
            assert_eq!(c.workers.status().admission_generation, 1);
            assert_eq!(c.workers.status().votes_generation, 1);
            assert!(c.peer.pool.info().accepting && c.peer.votes.info().accepting);
            assert_eq!(c.peer.handle.current().unwrap().number, initial);
        }
        assert!(c.workers.status().borrowed_producer);
        let pool_gc = c.workers.bindings().pool_gc.unwrap().current().unwrap();
        pool_gc.collect_origins().unwrap().await.unwrap();
        assert_eq!(pool_gc.info().last_admission_generation, Some(1));
        let identity = c
            .workers
            .bindings()
            .keys
            .unwrap()
            .current()
            .unwrap()
            .peer_identity()
            .unwrap()
            .await
            .unwrap();
        assert_eq!(identity.public_key(), c.peer.identity.public_key());
    }
    let after_policy = b
        .workers
        .bindings()
        .resilience
        .unwrap()
        .current()
        .unwrap()
        .snapshot()
        .unwrap()
        .await
        .unwrap();
    assert_eq!(after_policy.configuration.value.depth, 17);
    assert_eq!(
        after_policy.policy.state.recorded_active(),
        before_policy.policy.state.recorded_active()
    );
    assert!(!policy_source.is_closed());
    let old = a.peer.handle.current().unwrap();
    old.peer.shutdown();
    ready(&mut a.peer.handle, 2).await;
    closed(&old);
    assert!(old.maintenance.unwrap().status().joined);
    link(&b.peer, &a.peer).await;
    produce(&a.peer, 2).await;
    same_tip(&a_node, &b_node, &a.peer, &b.peer).await;
    admission_live(&a_node, &a.peer.importer).await;
    stop_all(b).await;
    stop_all(a).await;
}

#[tokio::test]
async fn borrowed_pool_and_votes_cannot_be_restarted_and_optional_children_share_one_budget() {
    let node = Node::new(1);
    let mut c = policy_producer(&node);
    let (config, _resources) = all_services().await;
    let (mut h, s) = optional::create(&mut c, config).unwrap();
    let task = tokio::spawn(s.run());
    worker_ready(&mut h).await;
    for child in [Child::Admission, Child::Votes] {
        assert_eq!(
            h.restart(child, 1).unwrap().wait().await,
            Err(supervisor::Error::Missing)
        );
    }
    let capabilities = h.bindings().capabilities.unwrap();
    let old = capabilities.current().unwrap();
    old.set(CHAIN_POI, &Value::ChainPoi(vec![]))
        .unwrap()
        .await
        .unwrap();
    let children = [
        Child::Capabilities,
        Child::Routing,
        Child::PoolMaintenance,
        Child::Keys,
        Child::GarbageCollection,
        Child::Analytics,
    ];
    for (i, child) in children.into_iter().enumerate() {
        let g = generation(&h.status(), child);
        let result = h.restart(child, g).unwrap().wait().await;
        if i == 5 {
            assert_eq!(result, Err(supervisor::Error::RestartLimit));
        } else {
            assert_eq!(result, Ok(g + 1));
            assert!(c.pool().info().accepting && c.votes().info().accepting);
        }
        if i == 0 {
            assert!(old.info().joined);
            assert_eq!(
                capabilities
                    .current()
                    .unwrap()
                    .get(CHAIN_POI)
                    .unwrap()
                    .await
                    .unwrap(),
                None
            );
        }
    }
    let s = within(task).await.unwrap();
    assert_eq!(s.error, Some(supervisor::Error::RestartLimit));
    assert!(
        s.joined && h.bindings().admission.is_closed() && h.bindings().votes.unwrap().is_closed()
    );
    assert!(c.pool().info().accepting && c.votes().info().accepting);
    admission_live(&node, &c.importer()).await;
    c.shutdown();
}

#[tokio::test]
async fn every_optional_start_failure_unwinds_without_retiring_producer_authorities() {
    for child in [
        Child::Upnp,
        Child::Routing,
        Child::Keys,
        Child::Analytics,
        Child::PoolMaintenance,
        Child::Resilience,
        Child::Capabilities,
        Child::GarbageCollection,
    ] {
        let node = Node::new(1);
        if child == Child::Resilience {
            let mut cfg = config(&node, false);
            let mut policy = ae_sync::resilience::Config::default();
            policy.queue.requests = 0;
            cfg.resilience = Some(policy);
            assert!(matches!(
                new(&node, cfg),
                Err(conductor::Error::Resilience(_))
            ));
            let c = policy_producer(&node);
            admission_live(&node, &c.importer()).await;
            assert!(c.shutdown().resilience.unwrap().is_ok());
            continue;
        }
        let mut c = policy_producer(&node);
        let (mut cfg, _resources) = all_services().await;
        match child {
            Child::Upnp => cfg.upnp.as_mut().unwrap().requests = 0,
            Child::Routing => cfg.routing.as_mut().unwrap().config.limits.requests = 0,
            Child::Keys => cfg.keys.as_mut().unwrap().requests = 0,
            Child::Analytics => cfg.analytics.as_mut().unwrap().limits.requests = 0,
            Child::PoolMaintenance => cfg.pool_gc.as_mut().unwrap().requests = 0,
            Child::Capabilities => cfg.capabilities.as_mut().unwrap().queue.requests = 0,
            Child::GarbageCollection => cfg.gc.as_mut().unwrap().limits.rows = 0,
            _ => panic!(),
        }
        let (h, service) = optional::create(&mut c, cfg).unwrap();
        let s = within(service.run()).await;
        assert!(
            matches!(s.error, Some(supervisor::Error::Start { child: failed, .. }) if failed == child),
            "{s:?}"
        );
        assert!(
            s.joined
                && h.bindings().admission.is_closed()
                && h.bindings().votes.unwrap().is_closed()
        );
        assert!(c.pool().info().accepting && c.votes().info().accepting);
        admission_live(&node, &c.importer()).await;
        // The policy parent survives a partial optional start; its transferred
        // child is joined and can be reacquired by a fresh optional owner.
        let (mut fresh, service) = optional::create(&mut c, Default::default()).unwrap();
        let task = tokio::spawn(service.run());
        worker_ready(&mut fresh).await;
        fresh.shutdown();
        assert!(within(task).await.unwrap().joined);
        admission_live(&node, &c.importer()).await;
        c.shutdown();
    }
}

#[tokio::test]
async fn constructor_rejects_duplicate_pool_timers_and_stale_producer_generations() {
    let node = Node::new(1);
    let mut cfg = config(&node, false);
    cfg.pool_worker.origins_interval = Some(Duration::from_secs(30));
    let mut c = new(&node, cfg).unwrap();
    assert!(matches!(
        optional::create(&mut c, Default::default()),
        Err(supervisor::Error::Configuration)
    ));
    assert!(c.pool().info().accepting && c.votes().info().accepting);
    let (old, old_service) = optional::create(
        &mut c,
        optional::Config {
            pool_gc: None,
            ..Default::default()
        },
    )
    .unwrap();
    c.shutdown();
    let (c, controller) = prepared(&node);
    assert!(matches!(
        connections::create_hyperchain_with_workers(
            &c,
            &old,
            Arc::new(noise::Identity::from_secret([99; 32])),
            controller,
            settings(),
            || Ok(Default::default())
        ),
        Err(connections::Error::Configuration)
    ));
    assert!(c.pool().info().accepting && c.votes().info().accepting);
    let s = within(old_service.run()).await;
    assert_eq!(
        s.error,
        Some(supervisor::Error::Producer(ae_sync::chain::Error::Closed))
    );
    assert!(s.joined);
    admission_live(&node, &c.importer()).await;
    c.shutdown();
}

#[tokio::test]
async fn peers_wait_for_optional_startup_and_managed_gc_publication() {
    let node = Node::new(1);
    let (mut c, cache) = prepared_with_policy(&node, Some(Default::default()));
    let (mut h, workers) = optional::create(&mut c, Default::default()).unwrap();
    let (mut peer, service) = connections::create_hyperchain_with_workers(
        &c,
        &h,
        Arc::new(noise::Identity::from_secret([98; 32])),
        cache,
        settings(),
        || Ok(Default::default()),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(peer.current().is_none() && peer.status().running);
    let workers = tokio::spawn(workers.run());
    worker_ready(&mut h).await;
    let g = ready(&mut peer, 1).await;
    assert!(g.maintenance.is_some());
    peer.shutdown();
    assert!(within(task).await.unwrap().joined);
    h.shutdown();
    assert!(within(workers).await.unwrap().joined);
    assert!(c.pool().info().accepting && c.votes().info().accepting);
    c.shutdown();
}

#[tokio::test]
async fn optional_shutdown_or_abort_closes_peer_bindings_but_preserves_producer() {
    for abort in [false, true] {
        let node = Node::new(1);
        let c = composed(&node).await;
        let bindings = c.workers.bindings();
        let policy = bindings
            .resilience
            .as_ref()
            .unwrap()
            .current()
            .unwrap()
            .source();
        if abort {
            c.task.abort();
            assert!(within(c.task).await.unwrap_err().is_cancelled());
            assert!(!c.workers.status().joined);
        } else {
            c.workers.shutdown();
            let s = within(c.task).await.unwrap();
            assert!(s.joined && s.error.is_none(), "{s:?}");
        }
        let s = within(c.peer.task).await.unwrap();
        assert!(s.joined && s.error.is_some(), "{s:?}");
        assert!(bindings.admission.is_closed() && bindings.votes.unwrap().is_closed());
        assert!(c.peer.pool.info().accepting && c.peer.votes.info().accepting);
        assert!(!policy.is_closed());
        admission_live(&node, &c.peer.importer).await;
        c.peer.producer.shutdown().unwrap();
        assert!(policy.is_closed());
    }
}

#[tokio::test]
async fn retired_producer_terminates_optional_workers_without_replacement() {
    let node = Node::new(1);
    let mut c = policy_producer(&node);
    let (cfg, _resources) = all_services().await;
    let (mut h, s) = optional::create(&mut c, cfg).unwrap();
    let task = tokio::spawn(s.run());
    worker_ready(&mut h).await;
    let old = old_stopped(&h.bindings(), Child::Capabilities);
    c.shutdown();
    let s = within(task).await.unwrap();
    assert_eq!(
        s.error,
        Some(supervisor::Error::Producer(ae_sync::chain::Error::Closed))
    );
    assert!(s.joined && old());
    assert_eq!(s.restart_attempts, 0);
}

#[tokio::test]
async fn optional_peer_keys_must_match_the_connection_identity() {
    let node = Node::new(1);
    let (mut c, cache) = prepared_with_policy(&node, Some(Default::default()));
    let (cfg, _resources) = all_services().await;
    let (mut h, s) = optional::create(&mut c, cfg).unwrap();
    let task = tokio::spawn(s.run());
    worker_ready(&mut h).await;
    let (_handle, service) = connections::create_hyperchain_with_workers(
        &c,
        &h,
        Arc::new(noise::Identity::from_secret([97; 32])),
        cache,
        settings(),
        || Ok(Default::default()),
    )
    .unwrap();
    let s = within(service.run()).await;
    assert_eq!(s.error, Some(connections::Error::IdentityChanged));
    assert!(s.joined);
    assert_eq!(s.starts, 0);
    assert!(c.pool().info().accepting && c.votes().info().accepting);
    h.shutdown();
    assert!(within(task).await.unwrap().joined);
    c.shutdown();
}
