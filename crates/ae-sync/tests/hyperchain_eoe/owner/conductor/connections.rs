use super::*;
use ae_p2p::{discovery::Peer, noise, registry::Command};
use ae_sync::{
    chain::hyperchain::conductor::runtime as producer_runtime, runtime::connection as connections,
};
use common::within;
use std::net::Ipv4Addr;
use tokio::task::JoinHandle;

fn settings() -> connections::Config {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let std::net::SocketAddr::V4(address) = listener.local_addr().unwrap() else {
        panic!()
    };
    let mut c = connections::Config::default();
    c.network.listen = Some(address);
    c.network.startup.include_default_peers = false;
    c.network.registry.policy.maximum_outbound = 1;
    c.network.registry.policy.maximum_probes = 0;
    c.peer.port = address.port();
    c.health_interval = Duration::from_millis(5);
    c.propagation.poll = Duration::from_millis(5);
    c.sync.refresh_interval = Duration::from_millis(5);
    c
}
fn prepared(node: &Node) -> (Conductor<Otp26>, pool::worker::CacheController) {
    let (cache, controller) = pool::worker::GossipCache::managed(200, 10000).unwrap();
    let mut cfg = config(node, false);
    cfg.gossip = cache;
    let c = new(node, cfg).unwrap();
    c.parent().post_block(parent(8, 1008)).unwrap();
    c.parent().barrier(Duration::from_secs(5)).unwrap();
    wait(|| {
        [1, 6].into_iter().all(|h| {
            matches!(
                c.parent().snapshot().cache.blocks().get(&h.into()),
                Some(parent_cache::Entry::Block(_))
            )
        })
    });
    (c, controller)
}
async fn ready(h: &mut connections::Handle, number: u64) -> connections::Generation {
    within(async {
        loop {
            assert!(h.status().running, "{:?}", h.status());
            if let Some(g) = h.current()
                && g.number >= number
            {
                break g;
            }
            h.changed().await.unwrap();
        }
    })
    .await
}
struct Running {
    producer: producer_runtime::Runtime<Otp26>,
    handle: connections::Handle,
    task: JoinHandle<connections::Status>,
    clock: ManualClock,
    identity: Arc<noise::Identity>,
    importer: Arc<ae_sync::chain::Importer<Otp26>>,
    pool: pool::worker::Client,
    votes: votes::Client,
}
async fn running(node: &Node, identity: Arc<noise::Identity>) -> Running {
    let (c, controller) = prepared(node);
    let (mut handle, service) =
        connections::create_hyperchain(&c, identity.clone(), controller, settings(), || {
            Ok(Default::default())
        })
        .unwrap();
    let importer = c.importer();
    let pool = c.pool();
    let votes = c.votes();
    let task = tokio::spawn(service.run());
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
    Running {
        producer,
        handle,
        task,
        clock,
        identity,
        importer,
        pool,
        votes,
    }
}
async fn link(from: &Running, to: &Running) {
    let a = from.handle.current().unwrap();
    let b = to.handle.current().unwrap();
    let addr = b.network.status().listening.unwrap();
    a.network
        .command(Command::Add(Peer {
            id: to.identity.public_key(),
            host: b"127.0.0.1".to_vec(),
            port: addr.port(),
            address: Ipv4Addr::LOCALHOST,
            source: Ipv4Addr::LOCALHOST,
            trusted: true,
        }))
        .unwrap()
        .wait()
        .await
        .unwrap();
    a.network
        .command(Command::Connect(Some(to.identity.public_key())))
        .unwrap()
        .wait()
        .await
        .unwrap();
    within(async {
        loop {
            let peers = a.network.peers().peers().unwrap().wait().await.unwrap();
            if peers.iter().any(|p| p.peer() == to.identity.public_key())
                && b.peer.status().counts.incoming_pings > 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}
async fn produce(r: &Running, count: usize) {
    r.producer.handle().start_production().unwrap();
    let end = Instant::now() + Duration::from_secs(25);
    let mut keys = 0;
    while keys < count {
        let s = r.producer.handle().status();
        assert!(Instant::now() < end && s.failure.is_none(), "{s:?}");
        if let Some(d) = s.deadline {
            let mut t = r.clock.0.lock().unwrap();
            t.0 += d.saturating_duration_since(t.1).as_millis() as u64;
            t.1 = d;
        }
        if let Some(output) = r.producer.try_output() {
            match output {
                Output::Attempt(done) => {
                    keys +=
                        usize::from(matches!(done.outcome, production::Outcome::Produced { .. }))
                }
                Output::Stopped(_) => (),
                other => panic!("{other:?}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
async fn same_tip(source: &Node, target: &Node, a: &Running, b: &Running) {
    within(async {
        loop {
            let expected = source.db.view().unwrap().status().tip;
            if target.db.view().unwrap().status().tip == expected {
                break;
            }
            assert!(
                a.handle.status().running && b.handle.status().running,
                "{:?} {:?}",
                a.handle.status(),
                b.handle.status()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let top = source.db.view().unwrap().status().tip.unwrap();
    assert_eq!(
        source.db.view().unwrap().require_record(top).unwrap().roots,
        target.db.view().unwrap().require_record(top).unwrap().roots
    );
}
fn closed(g: &connections::Generation) {
    assert!(
        g.network.status().joined
            && g.peer.status().joined
            && g.sync.status().joined
            && g.propagation.status().joined
            && g.observer.status().joined
    );
    assert!(!g.cache.as_ref().unwrap().is_running());
}
async fn stop(r: Running) {
    let g = r.handle.current().unwrap();
    r.handle.shutdown();
    let s = within(r.task).await.unwrap();
    assert!(s.joined && s.error.is_none(), "{s:?}");
    closed(&g);
    assert!(r.pool.info().accepting && r.votes.info().accepting);
    let p = r.producer.shutdown().unwrap();
    assert!(p.status.failure.is_none(), "{:?}", p.status);
    assert!(p.pool.is_ok() && p.parent.is_ok() && p.connector_joined);
}
#[tokio::test]
async fn managed_peers_share_live_producer_admission_and_sync_across_connection_restarts() {
    let source = Node::new(1);
    let target = Node::new(1);
    let mut a = running(&source, Arc::new(noise::Identity::from_secret([111; 32]))).await;
    let mut b = running(&target, Arc::new(noise::Identity::from_secret([112; 32]))).await;
    // Catch up through generation download before testing live propagation.
    produce(&a, 2).await;
    link(&b, &a).await;
    same_tip(&source, &target, &a, &b).await;
    within(async {
        while b.handle.current().unwrap().sync.status().counts.finished == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let old_a = a.handle.current().unwrap();
    let old_b = b.handle.current().unwrap();
    old_a.peer.shutdown();
    old_b.peer.shutdown();
    ready(&mut a.handle, 2).await;
    ready(&mut b.handle, 2).await;
    closed(&old_a);
    closed(&old_b);
    assert!(
        a.pool.info().accepting
            && b.pool.info().accepting
            && a.votes.info().accepting
            && b.votes.info().accepting
    );
    // A peer-generation retirement must not close the producer's importer.
    let genesis = source
        .db
        .view()
        .unwrap()
        .require_block(source.config.identity.genesis)
        .unwrap();
    a.importer
        .import_block(genesis.encode().unwrap(), 1_000_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    link(&b, &a).await;
    produce(&a, 2).await;
    same_tip(&source, &target, &a, &b).await;
    assert_eq!(a.handle.status().starts, 2);
    assert_eq!(b.handle.status().starts, 2);
    assert!(a.handle.current().unwrap().propagation.status().sent > 0);
    stop(b).await;
    stop(a).await;
}

#[tokio::test]
async fn failed_peer_construction_keeps_producer_authorities_and_releases_started_children() {
    for child in [
        connections::Child::Network,
        connections::Child::Gossip,
        connections::Child::Peer,
        connections::Child::Sync,
        connections::Child::Propagation,
        connections::Child::Observer,
    ] {
        let node = Node::new(1);
        let (c, controller) = prepared(&node);
        let importer = c.importer();
        let mut config = settings();
        match child {
            connections::Child::Network => config.network.commands = 0,
            connections::Child::Gossip => config.gossip.operations = 0,
            connections::Child::Peer => config.peer.connections = 0,
            connections::Child::Sync => config.sync.peers = 0,
            connections::Child::Propagation => config.propagation.peer_limit = 0,
            connections::Child::Observer => config.observer.records = 0,
            _ => unreachable!(),
        }
        let (_handle, service) = connections::create_hyperchain(
            &c,
            Arc::new(noise::Identity::from_secret([113; 32])),
            controller,
            config,
            || Ok(Default::default()),
        )
        .unwrap();
        let status = within(service.run()).await;
        assert!(
            status.joined
                && matches!(status.error, Some(connections::Error::Startup(ref f)) if f.child == child),
            "{child:?}: {status:?}"
        );
        let genesis = node
            .db
            .view()
            .unwrap()
            .require_block(node.config.identity.genesis)
            .unwrap();
        importer
            .import_created_block(genesis.encode().unwrap(), 1_000_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(c.pool().info().accepting && c.votes().info().accepting);
        shutdown(c);
    }
}
#[tokio::test]
async fn foreign_cache_constructor_rejection_cannot_retire_the_conductor_importer() {
    let node = Node::new(1);
    let (c, controller) = prepared(&node);
    let (_, foreign) = pool::worker::GossipCache::managed(200, 10000).unwrap();
    assert!(matches!(
        connections::create_hyperchain(
            &c,
            Arc::new(noise::Identity::from_secret([114; 32])),
            foreign,
            settings(),
            || Ok(Default::default())
        ),
        Err(connections::Error::Configuration)
    ));
    let (mut handle, service) = connections::create_hyperchain(
        &c,
        Arc::new(noise::Identity::from_secret([114; 32])),
        controller,
        settings(),
        || Ok(Default::default()),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let g = ready(&mut handle, 1).await;
    handle.shutdown();
    assert!(within(task).await.unwrap().joined);
    closed(&g);
    let genesis = node
        .db
        .view()
        .unwrap()
        .require_block(node.config.identity.genesis)
        .unwrap();
    c.importer()
        .import_block(genesis.encode().unwrap(), 1_000_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    shutdown(c);
}
#[tokio::test]
async fn retiring_producer_closes_connections_and_aborting_connections_preserves_producer() {
    for retire_producer in [true, false] {
        let node = Node::new(1);
        let mut r = running(&node, Arc::new(noise::Identity::from_secret([115; 32]))).await;
        let g = r.handle.current().unwrap();
        if retire_producer {
            let p = r.producer.shutdown().unwrap();
            assert!(p.pool.is_ok() && p.connector_joined);
            let status = within(r.task).await.unwrap();
            assert!(status.joined);
            assert_eq!(
                status.error,
                Some(connections::Error::Dependency(connections::Child::Importer))
            );
            closed(&g);
        } else {
            r.task.abort();
            assert!(within(&mut r.task).await.unwrap_err().is_cancelled());
            assert!(!r.handle.status().joined);
            assert_eq!(r.handle.status().error, Some(connections::Error::Aborted));
            let genesis = node
                .db
                .view()
                .unwrap()
                .require_block(node.config.identity.genesis)
                .unwrap();
            r.importer
                .import_block(genesis.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait()
                .await
                .unwrap();
            assert!(r.pool.info().accepting && r.votes.info().accepting);
            let p = r.producer.shutdown().unwrap();
            assert!(p.pool.is_ok() && p.connector_joined);
        }
    }
}
