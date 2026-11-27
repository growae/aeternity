use super::*;
use ae_node::runtime as node;
use ae_sync::chain::hyperchain::eoe::owner::production;
use producer::supervision::{self, Notice};
use std::sync::atomic::Ordering;

#[path = "family/core.rs"]
mod core;
#[path = "../../common/http_checks.rs"]
mod http_checks;
#[path = "../../common/network_settings.rs"]
mod network_settings;

#[tokio::test]
async fn application_assembly_starts_hyperchain_from_files_and_joins_all_owners() {
    use ae_node::application::{Options, Prepared};
    let parent = Parent::start();
    let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
    let data = unpack(include_str!(
        "../../../../ae-hyperchain/tests/fixtures/erlang_hc_genesis.json"
    ));
    let row = data["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == 8)
        .unwrap();
    let mut accounts = serde_json::Map::new();
    for account in row["initial"]["accounts"].as_array().unwrap() {
        let key = AccountPublicKey(native_bytes(&account["key"]).try_into().unwrap());
        let account = Account::deserialize(key, &native_bytes(&account["encoded"])).unwrap();
        accounts.insert(
            api::encode(ApiType::AccountPubkey, &key.0),
            serde_json::from_str(
                &num_bigint::BigInt::from_bytes_be(
                    num_bigint::Sign::Plus,
                    account.balance().as_bytes(),
                )
                .to_string(),
            )
            .unwrap(),
        );
    }
    let allocation = lab.temp.write(
        "application-accounts.json",
        serde_json::to_vec(&accounts).unwrap(),
    );
    let mut value = lab.config.user_map().clone();
    value["chain"]["hard_forks"]["8"]["accounts_file"] = json!(allocation);
    value["keys"] = json!({"dir":"application-keys","peer_password":"local-test"});
    value["include_default_peers"] = json!(false);
    value["sync"] =
        json!({"listen_address":"127.0.0.1","port":0,"upnp_enabled":false,"peer_analytics":false});
    value["metrics"] = json!({"host":"127.0.0.1","rules":[{"name":"ae.**","actions":"none"}]});
    value["http"] = json!({"external":{"listen_address":"127.0.0.1","port":0},"internal":{"port":0},"rosetta":{"port":0},"rosetta_offline":{"port":0}});
    let config = Configuration::from_value(value).unwrap();
    let options = Options::new(
        lab.temp.0.clone(),
        "application-chain".into(),
        ae_node::http::DryRunConfig::new(
            env!("CARGO_BIN_EXE_ae-dry-run-worker").into(),
            std::env::temp_dir(),
        ),
    );
    let prepared =
        Prepared::load(&config, Arc::new(lab.temp.context()), &lab.profile, options).unwrap();
    let db = prepared.configured().chain().database().clone();
    let mut running = prepared.launch().unwrap();
    let h = running.handle();
    let servers = tokio::time::timeout(Duration::from_secs(20), running.start_http())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(h.status().family, node::Family::Hyperchain);
    assert_eq!(servers.len(), 4);
    assert!(h.status().ready);
    assert!(h.metrics().is_some());
    assert!(ae_storage::snapshots::Owner::current(&db).is_ok());
    let workers = h.hyperchain().unwrap().workers().unwrap();
    let keys = workers.bindings().keys.as_ref().unwrap().current().unwrap();
    assert!(keys.peer_identity().unwrap().await.is_ok());
    let before = db.view().unwrap().status();
    let exit = tokio::time::timeout(Duration::from_secs(20), running.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert_eq!(db.view().unwrap().status(), before);
    assert!(servers.iter().all(|s| s.status().joined));
    assert!(!keys.info().running && !keys.info().accepting);
}

fn prepare(lab: &Hc, resources: Resources) -> node::Prepared {
    let mut options = node::Preparation::default();
    options.hyperchain.micro_cycle_ms = Some(1);
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
async fn live_hyperchain_dry_runs_share_existing_snapshot_authority_and_join_listeners() {
    use ae_node::http::{Config, DryRunConfig, Target};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };
    let parent = Parent::start();
    let mut lab = Hc::new(8, json!({"dev_mode": true}), &parent);
    let mut value = lab.config.user_map().clone();
    value["http"] = json!({"internal": {"debug_endpoints": true}});
    lab.config = Configuration::from_value(value).unwrap();
    let p = prepare(&lab, Default::default());
    let db = p.configured().chain().database().clone();
    let clock = Clock::new();
    let s = settings(&lab, &p, Arc::new(move |_| Ok(clock.clone())));
    let mut running = p.launch(s, || Ok(Default::default())).unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    assert!(ae_storage::snapshots::Owner::current(&db).is_ok());
    assert!(matches!(
        ae_storage::snapshots::Owner::start(db.clone(), Default::default()),
        Err(ae_storage::snapshots::Error::AlreadyRunning)
    ));
    let before = db.view().unwrap().status();
    let listeners = running
        .listen_http_group(
            vec![
                Config {
                    listen: ([127, 0, 0, 1], 0).into(),
                    groups: ["dry-run".into()].into(),
                    ..Default::default()
                },
                Config {
                    listen: ([127, 0, 0, 1], 0).into(),
                    target: Target::Internal,
                    groups: ["debug".into()].into(),
                    ..Default::default()
                },
            ],
            Some(DryRunConfig::new(
                env!("CARGO_BIN_EXE_ae-dry-run-worker").into(),
                std::env::temp_dir(),
            )),
        )
        .await
        .unwrap();
    for (listener, path) in [
        (&listeners[0], "/v3/dry-run"),
        (&listeners[1], "/v3/debug/transactions/dry-run"),
    ] {
        let mut socket = TcpStream::connect(listener.status().address).await.unwrap();
        socket.write_all(format!("POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 10\r\n\r\n{{\"txs\":[]}}").as_bytes()).await.unwrap();
        let mut bytes = vec![];
        socket.read_to_end(&mut bytes).await.unwrap();
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut response = httparse::Response::new(&mut headers);
        let end = response.parse(&bytes).unwrap().unwrap();
        assert_eq!(
            response.code,
            Some(200),
            "{}",
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes[end..]).unwrap(),
            json!({"results": []})
        );
    }
    assert_eq!(db.view().unwrap().status(), before);
    assert_eq!(h.http().unwrap().listeners().len(), 2);
    let exit = running.shutdown().await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(listeners.iter().all(|listener| listener.status().joined));
}

#[tokio::test]
async fn protocol_parameters_report_configured_hyperchain_timing_from_active_rules() {
    let parent = Parent::start();
    let mut lab = Hc::new(8, json!({"dev_mode":true}), &parent);
    let mut value = lab.config.user_map().clone();
    value["mining"] = json!({"micro_block_cycle":17,"name_claim_bid_timeout":23});
    lab.config = Configuration::from_value(value).unwrap();
    let p = node::Prepared::new(lab.open(Default::default(), "chain"), Default::default()).unwrap();
    assert_eq!(p.hyperchain().unwrap().execution().rules.micro_cycle_ms, 17);
    let clock = Clock::new();
    let s = settings(&lab, &p, Arc::new(move |_| Ok(clock.clone())));
    let mut running = p.launch(s, || Ok(Default::default())).unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let http = http_checks::start(&mut running).await;
    let (code, body) = http_checks::block_get(&http, "/v3/protocol-parameters").await;
    assert_eq!(code, 200);
    assert_eq!(body["micro_block_cycle"], 17);
    assert_eq!(body["child_block_time"], 4);
    assert!(body.get("expected_block_mine_rate").is_none());
    assert_eq!(body["current_protocol_version"], 8);
    assert_eq!(body["protocols"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["protocols"][0]["name_auction_timeouts"][0]["bid_timeout"],
        23
    );
    let exit = running.shutdown().await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    http_checks::joined(&http).await;
}

#[tokio::test]
async fn pending_hyperchain_requests_preserve_native_errors_and_root_availability() {
    use ae_sync::chain::pending::service::Error;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };
    let parent = Parent::start();
    for block_producer in [false, true] {
        let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
        let p = prepare(&lab, Default::default());
        let db = p.configured().chain().database().clone();
        let initial = db.view().unwrap().status();
        let clock = Clock::new();
        let mut s = settings(&lab, &p, Arc::new(move |_| Ok(clock.clone())));
        let node::Settings::Hyperchain(config) = &mut s else {
            unreachable!()
        };
        config.runtime.block_producer = block_producer;
        config.runtime.producing = false;
        let mut running = p.launch(s, || Ok(Default::default())).unwrap();
        let mut handle = running.handle();
        ready(&mut handle).await;
        let pending = handle.pending_key().unwrap();
        let expected = if block_producer {
            Error::BeneficiaryNotApplicable
        } else {
            Error::BeneficiaryNotConfigured
        };
        for _ in 0..2 {
            let receipt = pending.get().unwrap().await.unwrap();
            assert_eq!(receipt.result, Err(expected.clone()));
        }
        let http = http_checks::start(&mut running).await;
        for version in ["v2", "v3"] {
            tokio::time::timeout(Duration::from_secs(5), async {
                let mut stream = TcpStream::connect(http.status().address).await.unwrap();
                stream.write_all(format!("GET /{version}/key-blocks/pending HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes()).await.unwrap();
                let mut bytes = vec![];
                stream.read_to_end(&mut bytes).await.unwrap();
                let mut fields = [httparse::EMPTY_HEADER;64];
                let mut response = httparse::Response::new(&mut fields);
                let end = response.parse(&bytes).unwrap().unwrap();
                assert_eq!(response.code, Some(if block_producer { 500 } else { 400 }));
                assert_eq!(serde_json::from_slice::<Value>(&bytes[end..]).unwrap(), if block_producer {
                    json!({"reason":"Internal server error"})
                } else {
                    json!({"reason":"Beneficiary not configured","error_code":"beneficiary_not_configured"})
                });
            }).await.unwrap();
        }
        assert!(handle.status().ready && handle.status().failure.is_none());
        assert_eq!(pending.status().accepted, 4);
        assert_eq!(pending.status().completed, 4);
        assert!(pending.status().cached_parent.is_none());
        assert_eq!(db.view().unwrap().status(), initial);
        let exit = running.shutdown().await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        assert!(pending.status().joined && http.status().joined);
    }
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
        let optional = network_settings::Services::start().await;
        let mut prepared = prepare(&lab, Default::default());
        let database = prepared.configured().chain().database().clone();
        let clock = Clock::new();
        let mut settings = settings(&lab, &prepared, Arc::new(move |_| Ok(clock.clone())));
        network_settings::configure(&mut prepared, &mut settings, &optional);
        let running = prepared
            .launch(settings, || Ok(Default::default()))
            .unwrap();
        let mut handle = running.handle();
        ready(&mut handle).await;
        let workers = handle.hyperchain().unwrap().workers().unwrap();
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
            Some(
                network_settings::verify(
                    handle.connections().unwrap(),
                    &database,
                    local,
                    &workers,
                    &optional,
                )
                .await,
            )
        } else {
            optional.no_listener(&workers).await;
            None
        };
        let exit = running.shutdown().await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        if let Some(generation) = generation {
            assert!(generation.network.status().joined && generation.observer.status().joined);
            assert_eq!(generation.network.status().listener.unwrap().active, 0);
        }
        optional.finish(&workers, normal).await;
    }
}

#[tokio::test]
async fn configured_optional_network_flags_control_actual_hyperchain_workers_and_application_limits()
 {
    let parent = Parent::start();
    for enabled in [false, true] {
        let lab = Hc::new(8, json!({}), &parent);
        let p = prepare(&lab, Default::default());
        let clock = Clock::new();
        let mut settings = settings(&lab, &p, Arc::new(move |_| Ok(clock.clone())));
        network_settings::configure_policy_only(&p, &mut settings, enabled);
        let running = p.launch(settings, || Ok(Default::default())).unwrap();
        let mut handle = running.handle();
        ready(&mut handle).await;
        let analytics = network_settings::verify_policy_only(&handle, enabled).await;
        let exit = running.shutdown().await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        assert!(analytics.info().joined);
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
    options.hyperchain.micro_cycle_ms = Some(1);
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

#[path = "../../common/metrics_settings.rs"]
mod metrics_settings;

#[tokio::test]
async fn application_metrics_outlive_hyperchain_connections_in_all_modes() {
    let parent = Parent::start();
    for mode in [
        json!({}),
        json!({"dev_mode":true}),
        json!({"offline_mode":true}),
        json!({"maintenance_mode":true}),
    ] {
        let normal = mode == json!({});
        let lab = Hc::new(8, mode, &parent);
        let mut p = prepare(&lab, Default::default());
        let clock = Clock::new();
        let mut s = settings(&lab, &p, Arc::new(move |_| Ok(clock.clone())));
        let metrics = metrics_settings::Metrics::new().await;
        metrics.configure(&mut p, &mut s);
        let database = p.configured().chain().database().clone();
        let mut running = p.launch(s, || Ok(Default::default())).unwrap();
        let mut handle = running.handle();
        ready(&mut handle).await;
        let http = http_checks::start(&mut running).await;
        http_checks::verify(&http, &database, &handle).await;
        let client = metrics.verify(&handle, normal).await;
        metrics.replace_logger(&handle).await;
        http_checks::verify(&http, &database, &handle).await;
        let exit = running.shutdown().await.unwrap();
        http_checks::joined(&http).await;
        assert!(exit.status().joined && exit.status().failure.is_none());
        assert!(client.info().joined && !client.info().running);
        assert!(exit.status().metrics.unwrap().joined);
        metrics_settings::Metrics::assert_registry_joined(&client);
    }
}
