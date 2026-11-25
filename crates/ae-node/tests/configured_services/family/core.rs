use super::*;

#[tokio::test]
async fn resolved_core_settings_reach_real_pow_owners_and_survive_reopen() {
    for kind in ["pow_cuckoo", "on_demand"] {
        for mode in [
            json!({}),
            json!({"dev_mode":true}),
            json!({"offline_mode":true}),
            json!({"maintenance_mode":true}),
        ] {
            let lab = Lab::new();
            let mut system = mode.as_object().unwrap().clone();
            system.insert("custom_prefunded_accs_file".into(), json!("alloc.json"));
            let cfg=Configuration::from_value(json!({
                "fork_management":{"network_id":"ae_configured_services"},"system":system,
                "chain":{"hard_forks":{"1":0,"6":1},"consensus":{"0":{"type":kind}},
                    "garbage_collection":{"enabled":true,"history":50,"minimum_height":1000,"during_sync":true,"trees":["ns"]}},
                "mempool":{"tx_ttl":73,"cache_size":2,"allow_reentry_of_txs":true,"tx_failures":{"spend_tx":{"fallback":2}}},
                "mining":{"min_miner_gas_price":99},
                "keys":{"dir":"relative-keys","peer_password":"configured-password"},
                "sync":{"sync_allowed_height_from_top":7,"resist_forks_from_start":true}
            })).unwrap();
            let ctx = Context::new(Directories {
                home: "release".into(),
                data: "data".into(),
                log: "log".into(),
            })
            .unwrap();
            let resolved = cfg.resolve(&ctx).unwrap();
            let load = || {
                configured::Prepared::load(
                    &resolved,
                    None,
                    &lab.0,
                    &empty(),
                    &Specification::from_value(&profile::default_value()).unwrap(),
                    Default::default(),
                    &6_000_000.into(),
                    Default::default(),
                    Default::default(),
                )
                .unwrap()
                .open("chain", Default::default())
                .unwrap()
            };
            let p = node::Prepared::new(load(), Default::default()).unwrap();
            assert_eq!(p.configured().startup_directory(), lab.0);
            let identity = p.configured().chain().identity().clone();
            let db = p.configured().chain().database().clone();
            let tx = transfer(p.configured());
            let mut s = if kind == "on_demand" {
                commands(&lab)
            } else {
                cuckoo(&lab)
            };
            p.configure_core(&resolved, &mut s).unwrap();
            let running = p.launch(s, || Ok(Default::default())).unwrap();
            let mut h = running.handle();
            ready(&mut h).await;
            let w = h.pow().unwrap().workers().unwrap();
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
            let key_client = bindings.keys.as_ref().unwrap().current().unwrap();
            let peer = key_client
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
            let cache = pool.gossip_cache();
            if mode == json!({}) {
                assert!(!cache.check(Hash256([11; 32])));
                assert!(cache.check(Hash256([11; 32])));
                for n in [12, 13, 11] {
                    assert!(!cache.check(Hash256([n; 32])), "{kind}/{mode}/{n}");
                }
            } else {
                assert!(!cache.check(Hash256([11; 32])));
                assert!(!cache.check(Hash256([11; 32])));
                assert_eq!(cache.stats(), Err(worker::Error::CacheUnavailable));
            }
            if let Some(c) = h.commands() {
                let done = c
                    .client
                    .submit(Command::Blocks {
                        count: 2,
                        kind: Kind::Any,
                    })
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
                assert!(done.result.as_ref().unwrap().command.result.is_ok());
                assert!(db.view().unwrap().location(tx.hash()).unwrap().is_some());
            }
            let before = db.view().unwrap().status();
            let exit = running.shutdown().await.unwrap();
            assert!(exit.status().joined && exit.status().failure.is_none());
            assert_eq!(exit.configured().unwrap().chain().identity(), &identity);
            assert_eq!(exit.configured().unwrap().startup_directory(), lab.0);
            drop((h, w, bindings, key_client, pool, cache, exit, db));
            let reopened = load();
            assert_eq!(reopened.chain().identity(), &identity);
            assert_eq!(reopened.chain().database().view().unwrap().status(), before);
            let worker = keys::Worker::spawn(keys::Config::new(
                lab.0.join("relative-keys"),
                b"configured-password".to_vec(),
            ))
            .unwrap();
            assert_eq!(worker.info().keys.peer, peer);
            worker.shutdown().unwrap();
        }
    }
    println!(
        "Core configuration PoW: real admission TTL/failure/reentry/cache policies, active resilience, GC, relative encrypted keys and durable reopen in both families and four modes"
    );
}

#[test]
fn configured_core_rejects_wrong_family_without_mutating_settings() {
    let lab = Lab::new();
    let p = prepare(lab.services("on_demand", json!({}), Default::default()));
    let mut s = cuckoo(&lab);
    let c = Configuration::empty();
    let ctx = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    assert_eq!(
        p.configure_core(&c.resolve(&ctx).unwrap(), &mut s),
        Err(ae_node::configuration::runtime::Error::Family)
    );
    let node::Settings::Cuckoo(s) = s else {
        unreachable!()
    };
    assert_eq!(s.config.cache_entries, 1000);
    assert_eq!(s.config.workers.keys.unwrap().directory, lab.0.join("keys"));
}
