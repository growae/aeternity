use super::*;
use ae_node::configuration::runtime::Error;
use ae_storage::pool::worker::{CacheConfigurationError, LifecycleError};

#[tokio::test]
async fn resolved_core_hyperchain_preserves_owned_services_and_applies_policies_in_all_modes() {
    let parent = Parent::start();
    for mode in [
        json!({}),
        json!({"dev_mode":true}),
        json!({"offline_mode":true}),
        json!({"maintenance_mode":true}),
    ] {
        let normal = mode == json!({});
        let mut lab = Hc::new(8, mode, &parent);
        let mut value = lab.config.user_map().clone();
        value["mempool"] = json!({"tx_ttl":73,"cache_size":2,"allow_reentry_of_txs":true,"tx_failures":{"spend_tx":{"fallback":2}}});
        value["mining"] = json!({"min_miner_gas_price":99});
        value["keys"] = json!({"dir":"relative-keys","peer_password":"configured-password"});
        value["sync"] = json!({"sync_allowed_height_from_top":7,"resist_forks_from_start":true});
        value["chain"]["garbage_collection"] = json!({"enabled":true,"minimum_height":1000,"history":50,"during_sync":true,"trees":["oracles"]});
        lab.config = Configuration::from_value(value).unwrap();
        let ctx = lab.temp.context();
        let resolved = lab.config.resolve(&ctx).unwrap();
        let p = prepare(&lab, Default::default());
        let tx = transaction(p.hyperchain().unwrap());
        let db = p.configured().chain().database().clone();
        let identity = db.view().unwrap().identity().clone();
        let clock = Clock::new();
        let clocks = clock.clone();
        let mut s = settings(&lab, &p, Arc::new(move |_| Ok(clocks.clone())));
        let node::Settings::Hyperchain(hc) = &mut s else {
            unreachable!()
        };
        let retained = hc.runtime.gossip.clone();
        hc.runtime.pool_worker.origins_interval = Some(Duration::from_secs(30));
        let original = std::mem::take(&mut hc.runtime.gossip);
        assert_eq!(
            p.configure_core(&resolved, &mut s),
            Err(Error::CacheBinding)
        );
        let node::Settings::Hyperchain(hc) = &mut s else {
            unreachable!()
        };
        hc.runtime.gossip = original;
        let cache_owner = hc.cache.start().unwrap();
        assert_eq!(
            p.configure_core(&resolved, &mut s),
            Err(Error::CacheConfiguration(
                CacheConfigurationError::Lifecycle(LifecycleError::AlreadyRunning)
            ))
        );
        drop(cache_owner);
        p.configure_core(&resolved, &mut s).unwrap();
        let node::Settings::Hyperchain(hc) = &s else {
            unreachable!()
        };
        assert!(hc.cache.uses_cache(&retained));
        assert!(hc.runtime.pool_worker.origins_interval.is_none());
        assert_eq!(hc.runtime.pool.consensus, ConsensusType::Pos);
        let running = p.launch(s, || Ok(Default::default())).unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        let hc = h.hyperchain().unwrap();
        let w = hc.workers().unwrap();
        let bindings = w.bindings();
        assert!(w.status().gc.as_ref().unwrap().enabled);
        let resistance = bindings
            .resilience
            .as_ref()
            .unwrap()
            .current()
            .unwrap()
            .snapshot()
            .unwrap()
            .await
            .unwrap();
        assert_eq!(resistance.configuration.value.depth, 7);
        assert_eq!(resistance.policy.state.active_depth(), Some(7));
        let peer = bindings
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
        let pool = bindings.admission.current().unwrap().client;
        assert_eq!(
            pool.minimum_miner_gas_price().unwrap().await.unwrap(),
            99.into()
        );
        pool.push(tx.clone(), ae_mempool::pool::Event::Created, false)
            .unwrap()
            .await
            .unwrap();
        assert_eq!(pool.ttl(tx.hash()).unwrap().await.unwrap(), 73);
        assert!(
            !pool
                .record_failure(tx.hash(), "custom-runtime-failure".into(), false)
                .unwrap()
                .await
                .unwrap()
        );
        assert!(
            pool.record_failure(tx.hash(), "custom-runtime-failure".into(), false)
                .unwrap()
                .await
                .unwrap()
        );
        pool.push(tx.clone(), ae_mempool::pool::Event::Created, false)
            .unwrap()
            .await
            .unwrap();
        if normal {
            assert!(!retained.check(Hash256([11; 32])));
            assert!(retained.check(Hash256([11; 32])));
            for n in [12, 13, 11] {
                assert!(!retained.check(Hash256([n; 32])));
            }
        } else {
            assert!(!retained.check(Hash256([11; 32])));
            assert!(!retained.check(Hash256([11; 32])));
            assert_eq!(
                retained.stats(),
                Err(ae_storage::pool::worker::Error::CacheUnavailable)
            );
        }
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
        assert_eq!(exit.configured().unwrap().startup_directory(), lab.temp.0);
        let before = db.view().unwrap().status();
        drop((h, hc, w, bindings, pool, retained, exit, db));
        let opened = lab.open(Default::default(), "chain");
        assert_eq!(opened.chain().database().view().unwrap().status(), before);
        assert_eq!(opened.chain().identity(), &identity);
        let worker = keys::Worker::spawn(keys::Config::new(
            lab.temp.0.join("relative-keys"),
            b"configured-password".to_vec(),
        ))
        .unwrap();
        assert_eq!(worker.info().keys.peer, peer);
        worker.shutdown().unwrap();
    }
    println!(
        "Core configuration Hyperchain: one retained cache/admission binding, actual TTL/failure/reentry policy, configured resilience/GC, real produced transfers and encrypted-key reopen in all modes"
    );
}
