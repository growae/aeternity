use super::*;
use ae_node::runtime as node;
use ae_sync::chain::hyperchain::eoe::owner::production;
use producer::supervision::{self, Notice};
use std::sync::atomic::Ordering;

#[path = "family/core.rs"]
mod core;
#[path = "../../common/network_settings.rs"]
mod network_settings;

fn prepare(lab: &Hc, resources: Resources) -> node::Prepared {
    let mut options = node::Preparation::default();
    options.hyperchain.micro_cycle_ms = 1;
    node::Prepared::new(lab.open(resources, "chain"), options).unwrap()
}
fn settings(
    lab: &Hc,
    p: &node::Prepared,
    clocks: Arc<dyn supervision::Factory + Sync>,
) -> node::Settings {
    let (gossip, cache) = ae_storage::pool::worker::GossipCache::managed(200, 10000).unwrap();
    let mut runtime = p.hyperchain().unwrap().runtime();
    runtime.gossip = gossip;
    runtime.pool_worker.origins_interval = None;
    node::Settings::Hyperchain(Box::new(node::Hyperchain {
        config: lab.root_config(),
        runtime,
        services: Default::default(),
        cache,
        identity: root::Identity::Managed,
        clocks,
    }))
}
async fn ready(h: &mut node::Handle) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !h.status().ready {
            assert!(
                h.status().running && h.status().failure.is_none(),
                "{:?}",
                h.status()
            );
            h.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn configured_network_reaches_hyperchain_owner_and_respects_application_modes() {
    let parent = Parent::start();
    for mode in [
        json!({}),
        json!({"dev_mode":true}),
        json!({"offline_mode":true}),
        json!({"maintenance_mode":true}),
    ] {
        let normal = mode == json!({});
        let lab = Hc::new(8, mode, &parent);
        let mut prepared = prepare(&lab, Default::default());
        let database = prepared.configured().chain().database().clone();
        let clock = Clock::new();
        let mut settings = settings(&lab, &prepared, Arc::new(move |_| Ok(clock.clone())));
        network_settings::configure(&mut prepared, &mut settings);
        let running = prepared
            .launch(settings, || Ok(Default::default()))
            .unwrap();
        let mut handle = running.handle();
        ready(&mut handle).await;
        assert_eq!(handle.connections().is_some(), normal);
        let generation = if normal {
            let local = handle
                .hyperchain()
                .unwrap()
                .workers()
                .unwrap()
                .bindings()
                .keys
                .as_ref()
                .unwrap()
                .current()
                .unwrap()
                .peer_identity()
                .unwrap()
                .await
                .unwrap()
                .public_key();
            Some(network_settings::verify(handle.connections().unwrap(), &database, local).await)
        } else {
            None
        };
        let exit = running.shutdown().await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        if let Some(generation) = generation {
            assert!(generation.network.status().joined && generation.observer.status().joined);
            assert_eq!(generation.network.status().listener.unwrap().active, 0);
        }
    }
}

#[tokio::test]
async fn dispatch_hyperchain_keeps_real_transfers_modes_and_exact_opened_chain() {
    let parent = Parent::start();
    for mode in [
        json!({}),
        json!({"dev_mode":true}),
        json!({"offline_mode":true}),
        json!({"maintenance_mode":true}),
    ] {
        let normal = mode == json!({});
        let lab = Hc::new(8, mode, &parent);
        let p = prepare(&lab, Default::default());
        assert_eq!(p.family(), node::Family::Hyperchain);
        assert!(p.pow().is_none());
        let tx = transaction(p.hyperchain().unwrap());
        let db = p.configured().chain().database().clone();
        let identity = db.view().unwrap().identity().clone();
        let clock = Clock::new();
        let clocks = clock.clone();
        let s = settings(&lab, &p, Arc::new(move |_| Ok(clocks.clone())));
        let running = p.launch(s, || Ok(Default::default())).unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        assert_eq!(h.connections().is_some(), normal);
        assert!(h.commands().is_none() && h.pow().is_none());
        assert!(matches!(h.status().details, node::Details::Hyperchain(_)));
        let hc = h.hyperchain().unwrap();
        let pool = hc
            .workers()
            .unwrap()
            .bindings()
            .admission
            .current()
            .unwrap()
            .client;
        pool.push(tx.clone(), ae_mempool::pool::Event::Created, false)
            .unwrap()
            .await
            .unwrap();
        h.set_producing(true).unwrap();
        let mut count = 0;
        tokio::time::timeout(Duration::from_secs(25), async {
            while count < 3 {
                assert!(h.status().failure.is_none(), "{:?}", h.status());
                clock.tick(&hc);
                while let Some(out) = h.try_output() {
                    if let node::Output::Hyperchain(out) = out
                        && matches!(
                            *out,
                            conductor::Output::Attempt(production::Completed {
                                outcome: production::Outcome::Produced { .. },
                                ..
                            })
                        )
                    {
                        count += 1;
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(db.view().unwrap().location(tx.hash()).unwrap().is_some());
        h.set_producing(false).unwrap();
        let exit = running.shutdown().await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        assert_eq!(exit.configured().unwrap().chain().identity(), &identity);
        let node::Exit::Hyperchain(inner) = &exit else {
            panic!("wrong family receipt")
        };
        clean(inner);
        let status = db.view().unwrap().status();
        assert!(h.take_exit().is_none());
        drop((h, hc, pool, exit, db));
        assert_eq!(
            lab.open(Default::default(), "chain")
                .chain()
                .database()
                .view()
                .unwrap()
                .status(),
            status
        );
    }
    println!(
        "Family dispatch Hyperchain: configured supervised root, funded transfer, four modes, typed outputs and exact durable reopen"
    );
}

struct Failing(Clock, Arc<AtomicBool>);
impl producer::Clock for Failing {
    fn now(&self) -> Result<(u64, Instant), conductor::Error> {
        if self.1.load(Ordering::Acquire) {
            Err(conductor::Error::Time)
        } else {
            producer::Clock::now(&self.0)
        }
    }
}
#[tokio::test]
async fn dispatch_alternates_block_and_restart_outputs_without_replacing_the_importer() {
    let parent = Parent::start();
    let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
    let p = prepare(&lab, Default::default());
    let clock = Clock::new();
    let clocks = clock.clone();
    let fail = Arc::new(AtomicBool::new(false));
    let first = fail.clone();
    let mut s = settings(
        &lab,
        &p,
        Arc::new(move |generation| {
            Ok(Failing(
                clocks.clone(),
                if generation == 1 {
                    first.clone()
                } else {
                    Arc::new(AtomicBool::new(false))
                },
            ))
        }),
    );
    let node::Settings::Hyperchain(cfg) = &mut s else {
        unreachable!()
    };
    cfg.config.outputs = 1;
    cfg.config.producer.outputs = 1;
    let running = p.launch(s, || Ok(Default::default())).unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let hc = h.hyperchain().unwrap();
    let importer = h.importer().unwrap();
    h.set_producing(true).unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while hc.status().producer.unwrap().completed < 2 {
            assert!(h.status().failure.is_none(), "{:?}", h.status());
            clock.tick(&hc);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    fail.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(h.status().failure.is_none(), "{:?}", h.status());
            let s = hc.status().production.unwrap();
            if s.phase == supervision::Phase::Replacing && !s.pending_notice {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    // Both original queues are occupied. Cloned handles share the selector.
    assert!(matches!(h.try_output(), Some(node::Output::Hyperchain(_))));
    let other = h.clone();
    assert!(matches!(
        other.try_output(),
        Some(node::Output::HyperchainProduction(Notice::Retired(_)))
    ));
    let mut outputs = vec![];
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(h.status().failure.is_none(), "{:?}", h.status());
            if let Some(o) = h.try_output() {
                let done = matches!(
                    o,
                    node::Output::HyperchainProduction(Notice::Restarted { generation: 2 })
                );
                outputs.push(o);
                if done {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    ready(&mut h).await;
    assert!(Arc::ptr_eq(&importer, &h.importer().unwrap()));
    assert!(
        outputs
            .iter()
            .any(|o| matches!(o, node::Output::Hyperchain(_)))
    );
    let exit = running.shutdown().await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    let node::Exit::Hyperchain(e) = exit else {
        unreachable!()
    };
    assert_eq!(e.production.as_ref().unwrap().status.attempts, 1);
    assert!(e.production.unwrap().driver.unwrap().exit.is_ok());
    println!(
        "Family dispatch outputs: bounded real block and supervision queues alternate across cloned clients, permit driver replacement and keep one importer"
    );
}

#[tokio::test]
async fn dispatch_hyperchain_failures_retry_with_the_same_bound_chain_and_drop_retains_exit() {
    let parent = Parent::start();
    let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
    let open = lab.open(Default::default(), "chain");
    let db = open.chain().database().clone();
    let mut options = node::Preparation::default();
    options.hyperchain.ledger.history = 0;
    let failure = match node::Prepared::new(open, options) {
        Err(f) => f,
        Ok(_) => panic!("invalid HC limit accepted"),
    };
    assert!(matches!(
        failure.error,
        node::PrepareError::Hyperchain(startup::Error::Binding)
    ));
    assert!(Arc::ptr_eq(&db, failure.open.chain().database()));
    let mut options = node::Preparation::default();
    options.hyperchain.micro_cycle_ms = 1;
    let p = node::Prepared::new(failure.open, options).unwrap();
    let clocks: Arc<dyn supervision::Factory + Sync> = Arc::new(|_| Ok(Clock::new()));
    let mut s = settings(&lab, &p, clocks.clone());
    let node::Settings::Hyperchain(cfg) = &mut s else {
        unreachable!()
    };
    cfg.config.poll = Duration::ZERO;
    let mut failure = p
        .launch(s, || panic!("invalid launch output factory invoked"))
        .err()
        .unwrap();
    assert_eq!(
        failure.error,
        node::Error::Hyperchain(Box::new(root::Error::Configuration))
    );
    assert!(Arc::ptr_eq(
        &db,
        failure.prepared.configured().chain().database()
    ));
    let node::Settings::Hyperchain(cfg) = &mut failure.settings else {
        unreachable!()
    };
    assert!(Arc::ptr_eq(&cfg.clocks, &clocks));
    cfg.config.poll = Duration::from_millis(2);
    let running = failure
        .prepared
        .launch(failure.settings, || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    drop(running);
    tokio::time::timeout(Duration::from_secs(10), async {
        while h.status().running {
            h.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let exit = h.take_exit().unwrap();
    assert!(h.take_exit().is_none());
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(Arc::ptr_eq(
        &db,
        exit.configured().unwrap().chain().database()
    ));
    let node::Exit::Hyperchain(e) = &exit else {
        unreachable!()
    };
    clean(e);
}
