use super::*;
use ae_node::runtime as node;
use ae_sync::chain::on_demand::control::{Command, Kind};

#[path = "family/core.rs"]
mod core;
#[path = "family/discovery.rs"]
mod discovery;
#[path = "../common/network_settings.rs"]
mod network_settings;

fn prepare(p: Prepared) -> node::Prepared {
    node::Prepared::new(p.into_open(), Default::default()).unwrap()
}
fn cuckoo(lab: &Lab) -> node::Settings {
    node::Settings::Cuckoo(Box::new(node::Cuckoo {
        config: root_config(lab),
        clocks: Arc::new(group::SystemClocks::default()),
    }))
}
fn commands(lab: &Lab) -> node::Settings {
    let mut config = root::Config::with_production(root::Commands::new(AccountPublicKey([42; 32])));
    config.workers = lab.worker_config();
    config.connections = network_config();
    config.poll = Duration::from_millis(2);
    node::Settings::OnDemand(Box::new(node::OnDemand {
        config,
        clock: Arc::new(Clock),
    }))
}
async fn ready(h: &mut node::Handle) {
    within(async {
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
}
fn transfer(open: &configured::Open) -> Arc<ae_tx::SignedTransaction<ae_tx::Transaction>> {
    use ae_tx::CanonicalTransaction;
    let mut tx = ae_tx::Spend::new(
        payer().public_key(),
        ae_types::Id::new(ae_types::IdTag::Account, [43; 32]),
    )
    .unwrap();
    tx.set_nonce(1.into());
    tx.set_amount(7.into());
    tx.set_fee(100_000_000_000_000u64.into());
    let mut bytes = open.chain().settings().profile().signing_prefix(false);
    bytes.extend(tx.encode());
    Arc::new(ae_tx::SignedTransaction::new(
        ae_tx::Transaction::new(ae_tx::TransactionLeaf::Spend(tx)),
        vec![payer().sign(&bytes)],
    ))
}

#[tokio::test]
async fn configured_network_reaches_both_pow_families_and_respects_application_modes() {
    for kind in ["pow_cuckoo", "on_demand"] {
        for mode in [
            json!({}),
            json!({"dev_mode":true}),
            json!({"offline_mode":true}),
            json!({"maintenance_mode":true}),
        ] {
            let normal = mode == json!({});
            let lab = Lab::new();
            let optional = network_settings::Services::start().await;
            let mut prepared = prepare(lab.services(kind, mode, Default::default()));
            let database = prepared.configured().chain().database().clone();
            let mut settings = if kind == "on_demand" {
                commands(&lab)
            } else {
                cuckoo(&lab)
            };
            network_settings::configure(&mut prepared, &mut settings, &optional);
            let running = prepared
                .launch(settings, || Ok(Default::default()))
                .unwrap();
            let mut handle = running.handle();
            ready(&mut handle).await;
            let workers = handle.pow().unwrap().workers().unwrap();
            assert_eq!(handle.connections().is_some(), normal);
            let generation = if normal {
                let local = handle
                    .pow()
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
                assert!(generation.network.status().joined);
                assert!(generation.observer.status().joined);
                assert_eq!(generation.network.status().listener.unwrap().active, 0);
            }
            optional.finish(&workers, normal).await;
        }
    }
}

#[test]
fn configured_network_rejects_wrong_family_before_changing_settings() {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({}), Default::default()));
    let mut settings = cuckoo(&lab);
    let cfg = Configuration::empty();
    let context = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    assert_eq!(
        prepared.configure_network(&cfg.resolve(&context).unwrap(), &mut settings),
        Err(ae_node::configuration::network::Error::Family)
    );
    assert_eq!(
        prepared.configure_optional_network(&cfg.resolve(&context).unwrap(), &mut settings),
        Err(ae_node::configuration::network::Error::Family)
    );
    let node::Settings::Cuckoo(settings) = settings else {
        unreachable!()
    };
    assert_eq!(
        settings.config.connections.network.listen.unwrap().port(),
        0
    );
}

#[tokio::test]
async fn configured_optional_network_flags_control_actual_pow_workers_and_application_limits() {
    for kind in ["pow_cuckoo", "on_demand"] {
        for enabled in [false, true] {
            let lab = Lab::new();
            let p = prepare(lab.services(kind, json!({}), Default::default()));
            let mut settings = if kind == "on_demand" {
                commands(&lab)
            } else {
                cuckoo(&lab)
            };
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
}

#[tokio::test]
async fn dispatch_cuckoo_preserves_mainnet_admission_modes_and_single_terminal_receipt() {
    for mode in [
        json!({}),
        json!({"dev_mode":true}),
        json!({"offline_mode":true}),
        json!({"maintenance_mode":true}),
    ] {
        let lab = Lab::new();
        let normal = mode == json!({});
        let p = if normal {
            lab.public()
        } else {
            lab.services("pow_cuckoo", mode, Default::default())
        };
        let block = normal.then(|| first_key(&p));
        let p = prepare(p);
        assert_eq!(p.family(), node::Family::Cuckoo);
        assert!(p.hyperchain().is_none());
        let db = p.configured().chain().database().clone();
        let identity = db.view().unwrap().identity().clone();
        let running = p.launch(cuckoo(&lab), || Ok(Default::default())).unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        assert_eq!(h.connections().is_some(), normal);
        assert!(h.commands().is_none() && h.hyperchain().is_none());
        assert!(matches!(h.status().details, node::Details::Cuckoo(_)));
        h.set_producing(true).unwrap();
        h.set_producing(false).unwrap();
        let importer = h.importer().unwrap();
        let ticket = block
            .as_ref()
            .map(|(bytes, time, _)| importer.import_synced_block(bytes.clone(), *time).unwrap());
        let exit = within(running.shutdown()).await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        assert_eq!(exit.configured().unwrap().chain().identity(), &identity);
        assert!(matches!(&exit, node::Exit::Cuckoo(e) if e.status.joined));
        assert!(h.take_exit().is_none());
        assert!(matches!(
            h.set_producing(true),
            Err(node::Error::Pow(error)) if *error == root::Error::Closed
        ));
        if let Some(ticket) = ticket {
            ticket.wait().await.unwrap();
            let (bytes, time, hash) = block.unwrap();
            assert_eq!(db.view().unwrap().status().tip, Some(hash));
            assert!(matches!(
                importer.import_synced_block(bytes, time),
                Err(ae_sync::chain::Error::Closed)
            ));
            drop((importer, db, h, exit));
            assert_eq!(
                lab.public()
                    .configured()
                    .chain()
                    .database()
                    .view()
                    .unwrap()
                    .status()
                    .tip,
                Some(hash)
            );
        }
    }
    println!(
        "Family dispatch Cuckoo: actual mainnet first-key admission and four application modes retain the bound chain and one joined terminal receipt"
    );
}

#[tokio::test]
async fn dispatch_on_demand_keeps_real_transfer_and_unread_command_receipts_in_all_modes() {
    for mode in [
        json!({}),
        json!({"dev_mode":true}),
        json!({"offline_mode":true}),
        json!({"maintenance_mode":true}),
    ] {
        let normal = mode == json!({});
        let lab = Lab::new();
        let p = prepare(lab.services("on_demand", mode.clone(), Default::default()));
        assert_eq!(p.family(), node::Family::OnDemand);
        let tx = transfer(p.configured());
        let db = p.configured().chain().database().clone();
        let running = p.launch(commands(&lab), || Ok(Default::default())).unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        assert_eq!(h.connections().is_some(), normal);
        assert!(matches!(h.status().details, node::Details::OnDemand(_)));
        assert_eq!(h.set_producing(true), Err(node::Error::UnsupportedControl));
        let c = h.commands().unwrap();
        let pool = h
            .pow()
            .unwrap()
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
        let first = c
            .client
            .submit(Command::Blocks {
                count: 2,
                kind: Kind::Any,
            })
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(first.result.as_ref().unwrap().command.result.is_ok());
        assert!(db.view().unwrap().location(tx.hash()).unwrap().is_some());
        let unread = c.client.submit(Command::Key).unwrap();
        within(async {
            while c.client.status().completed != 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        let before = db.view().unwrap().status();
        let exit = within(running.shutdown()).await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        assert!(exit.configured().is_some());
        let node::Exit::OnDemand(report) = &exit else {
            panic!("wrong terminal family")
        };
        assert_root_joined(report);
        let unread = unread.wait().await.unwrap();
        // Shutdown can precede the root's next output collection. The joined
        // child then retains its completions directly in the terminal report.
        // Both accepted commands must be retained exactly once across owners.
        let held: Vec<_> = report
            .retained
            .iter()
            .filter_map(|output| match output {
                root::Output::Command { completion, .. } => Some(completion),
                _ => None,
            })
            .chain(&report.commands.as_ref().unwrap().as_ref().unwrap().outputs)
            .collect();
        assert_eq!(held.len(), 2);
        for receipt in [&first, &unread] {
            assert_eq!(
                held.iter()
                    .filter(|held| Arc::ptr_eq(held, receipt))
                    .count(),
                1
            );
        }
        assert!(unread.result.as_ref().unwrap().command.result.is_ok());
        assert!(h.take_exit().is_none());
        drop((first, c, pool, h, exit, db));
        assert_eq!(
            lab.services("on_demand", mode, Default::default())
                .configured()
                .chain()
                .database()
                .view()
                .unwrap()
                .status(),
            before
        );
    }
    println!(
        "Family dispatch on-demand: funded transfers, separate command capability, four modes, unread accepted results and exact database reopen"
    );
}

#[test]
fn dispatch_preparation_failures_retain_open_and_cannot_bypass_live_snapshot_limits() {
    let lab = Lab::new();
    let open = lab
        .services("pow_cuckoo", json!({"dev_mode":true}), Default::default())
        .into_open();
    let db = open.chain().database().clone();
    let fingerprint = open.execution().fingerprint();
    let mut rules = open.pow_rules(3000, Default::default());
    rules.admission.configured_gas_limit += 1;
    let mut config = node::Preparation::default();
    config.pow.rules = Some(rules);
    let failure = match node::Prepared::new(open, config) {
        Err(f) => f,
        Ok(_) => panic!("mismatched rules accepted"),
    };
    assert!(matches!(
        failure.error,
        node::PrepareError::Pow(pow::Error::Binding)
    ));
    assert!(Arc::ptr_eq(&db, failure.open.chain().database()));
    assert_eq!(failure.open.execution().fingerprint(), fingerprint);
    let mut config = node::Preparation::default();
    config.pow.snapshots.maximum_snapshots = 0;
    let failure = match node::Prepared::new(failure.open, config) {
        Err(f) => f,
        Ok(_) => panic!("invalid snapshot limits accepted"),
    };
    assert!(matches!(
        failure.error,
        node::PrepareError::Pow(pow::Error::Snapshots(ae_storage::snapshots::Error::Config))
    ));
    let prepared = node::Prepared::new(failure.open, Default::default()).unwrap();
    let live = prepared.pow().unwrap().snapshots().view().unwrap();
    let open = prepared.into_open();
    let failure = match node::Prepared::new(open, Default::default()) {
        Err(f) => f,
        Ok(_) => panic!("live snapshot lease bypassed"),
    };
    assert!(matches!(
        failure.error,
        node::PrepareError::Pow(pow::Error::Snapshots(
            ae_storage::snapshots::Error::AlreadyRunning
        ))
    ));
    drop(live);
    let prepared = node::Prepared::new(failure.open, Default::default()).unwrap();
    assert!(Arc::ptr_eq(&db, prepared.configured().chain().database()));
    assert_eq!(prepared.configured().execution().fingerprint(), fingerprint);
}

#[test]
fn dispatch_without_async_runtime_returns_the_same_prepared_binding_and_clock_factory() {
    let lab = Lab::new();
    let p = prepare(lab.public());
    let identity = p.configured().chain().identity().clone();
    let clocks: Arc<dyn group::Clocks> = Arc::new(group::SystemClocks::default());
    let settings = node::Settings::Cuckoo(Box::new(node::Cuckoo {
        config: root_config(&lab),
        clocks: clocks.clone(),
    }));
    let failure = p.launch(settings, || Ok(Default::default())).err().unwrap();
    assert_eq!(
        failure.error,
        node::Error::Pow(Box::new(root::Error::NoRuntime))
    );
    assert_eq!(failure.prepared.configured().chain().identity(), &identity);
    let node::Settings::Cuckoo(s) = failure.settings else {
        panic!("lost family settings")
    };
    assert!(Arc::ptr_eq(&s.clocks, &clocks));
}

#[tokio::test]
async fn dispatch_wrong_family_and_invalid_launch_are_retryable_without_reopening() {
    let lab = Lab::new();
    let p = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = p.configured().chain().database().clone();
    let failure = p
        .launch(cuckoo(&lab), || {
            panic!("wrong-family output factory called")
        })
        .err()
        .unwrap();
    assert_eq!(
        failure.error,
        node::Error::Family {
            configured: node::Family::OnDemand,
            requested: node::Family::Cuckoo
        }
    );
    assert!(Arc::ptr_eq(
        &db,
        failure.prepared.configured().chain().database()
    ));
    let mut settings = commands(&lab);
    let node::Settings::OnDemand(s) = &mut settings else {
        unreachable!()
    };
    s.config.poll = Duration::ZERO;
    let mut failure = failure
        .prepared
        .launch(settings, || panic!("invalid launch output factory called"))
        .err()
        .unwrap();
    assert_eq!(
        failure.error,
        node::Error::Pow(Box::new(root::Error::Configuration))
    );
    let node::Settings::OnDemand(s) = &mut failure.settings else {
        panic!("lost settings")
    };
    s.config.poll = Duration::from_millis(2);
    let running = failure
        .prepared
        .launch(failure.settings, || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(Arc::ptr_eq(
        &db,
        exit.configured().unwrap().chain().database()
    ));
}

#[tokio::test]
async fn dispatch_partial_startup_and_dropped_running_owner_keep_the_single_exit() {
    for command in [false, true] {
        let lab = Lab::new();
        let kind = if command { "on_demand" } else { "pow_cuckoo" };
        let p = prepare(lab.services(kind, json!({"dev_mode":true}), Default::default()));
        let mut settings = if command {
            commands(&lab)
        } else {
            cuckoo(&lab)
        };
        match &mut settings {
            node::Settings::Cuckoo(s) => s.config.workers.poll = Duration::ZERO,
            node::Settings::OnDemand(s) => s.config.workers.poll = Duration::ZERO,
            _ => unreachable!(),
        }
        let exit = within(
            p.launch(settings, || Ok(Default::default()))
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        assert!(exit.status().failure.is_some() && exit.status().joined);
        assert!(exit.configured().is_some());
        drop(exit);
        let p = prepare(lab.services(kind, json!({"dev_mode":true}), Default::default()));
        let running = p
            .launch(
                if command {
                    commands(&lab)
                } else {
                    cuckoo(&lab)
                },
                || Ok(Default::default()),
            )
            .unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        let receipt = if command {
            Some(h.commands().unwrap().client.submit(Command::Key).unwrap())
        } else {
            None
        };
        drop(running);
        within(async {
            while h.status().running {
                h.changed().await.unwrap();
            }
        })
        .await;
        let exit = h.take_exit().unwrap();
        assert!(exit.status().joined && exit.configured().is_some());
        assert!(h.take_exit().is_none());
        if let Some(receipt) = receipt {
            assert!(receipt.wait().await.is_ok());
        }
    }
}

#[test]
fn dispatch_never_silently_reduces_a_future_consensus_schedule_to_genesis() {
    let lab = Lab::new();
    let config = Configuration::from_value(json!({
        "fork_management":{"network_id":"ae_dispatch_schedule"},
        "system":{"custom_prefunded_accs_file":"alloc.json"},
        "chain":{"hard_forks":{"1":0,"6":1},"consensus":{
            "0":{"type":"pow_cuckoo"},"100":{"type":"on_demand"}
        }}
    }))
    .unwrap();
    let context = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    let prepared = configured::Prepared::load(
        &config.resolve(&context).unwrap(),
        None,
        &lab.0,
        &empty(),
        &Specification::from_value(&profile::default_value()).unwrap(),
        Default::default(),
        &6_000_000.into(),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let error = prepared
        .open("unsupported", Default::default())
        .err()
        .unwrap();
    assert!(matches!(
        error,
        configured::Error::Genesis(ae_node::bootstrap::genesis::Error::Database(
            ae_node::bootstrap::Error::UnsupportedSchedule
        ))
    ));
    assert!(!lab.0.join("unsupported").exists());
}

#[path = "../common/metrics_settings.rs"]
mod metrics_settings;

#[tokio::test]
async fn application_metrics_outlive_connection_generations_in_both_pow_families_and_all_modes() {
    for kind in ["pow_cuckoo", "on_demand"] {
        for mode in [
            json!({}),
            json!({"dev_mode":true}),
            json!({"offline_mode":true}),
            json!({"maintenance_mode":true}),
        ] {
            let normal = mode == json!({});
            let lab = Lab::new();
            let mut p = prepare(lab.services(kind, mode, Default::default()));
            let mut s = if kind == "on_demand" {
                commands(&lab)
            } else {
                cuckoo(&lab)
            };
            let metrics = metrics_settings::Metrics::new().await;
            metrics.configure(&mut p, &mut s);
            let running = p.launch(s, || Ok(Default::default())).unwrap();
            let mut handle = running.handle();
            ready(&mut handle).await;
            let client = metrics.verify(&handle, normal).await;
            metrics.replace_logger(&handle).await;
            let exit = running.shutdown().await.unwrap();
            assert!(exit.status().joined && exit.status().failure.is_none());
            assert!(client.info().joined && !client.info().running);
            assert!(exit.status().metrics.unwrap().joined);
        }
    }
}
#[tokio::test]
async fn metrics_resolution_failure_including_port_zero_keeps_the_actual_application_running() {
    for port in [0, 8125] {
        let lab = Lab::new();
        let mut p = prepare(lab.services(
            "on_demand",
            json!({"offline_mode":true}),
            Default::default(),
        ));
        let mut s = commands(&lab);
        let cfg = Configuration::from_value(json!({"metrics":{"host":"bad\u{0}host","port":port}}))
            .unwrap();
        let ctx = Context::new(Directories {
            home: "release".into(),
            data: "data".into(),
            log: "log".into(),
        })
        .unwrap();
        p.configure_metrics(&cfg.resolve(&ctx).unwrap(), &mut s)
            .unwrap();
        let running = p.launch(s, || Ok(Default::default())).unwrap();
        let mut handle = running.handle();
        let client = handle.metrics().unwrap();
        ready(&mut handle).await;
        within(async {
            while handle.status().metrics_error.is_none() || !client.info().joined {
                handle.changed().await.unwrap();
            }
        })
        .await;
        assert!(
            handle.status().ready && handle.status().running && handle.status().failure.is_none()
        );
        assert!(!client.info().ready);
        assert_eq!(client.info().open_attempts, 0);
        let exit = running.shutdown().await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        assert!(exit.status().metrics_error.is_some());
    }
}

#[tokio::test]
async fn metrics_owner_recovers_failed_launch_and_drop_does_not_wait_for_a_stalled_root_executor() {
    let lab = Lab::new();
    let mut p = prepare(lab.services(
        "on_demand",
        json!({"offline_mode":true}),
        Default::default(),
    ));
    let mut s = commands(&lab);
    let metrics = metrics_settings::Metrics::new().await;
    metrics.configure(&mut p, &mut s);
    if let node::Settings::OnDemand(s) = &mut s {
        s.config.poll = Duration::ZERO;
    }
    let error = match p.launch(s, || Ok(Default::default())) {
        Ok(_) => panic!("invalid root accepted"),
        Err(error) => error,
    };
    assert!(matches!(error.error, node::Error::Pow(_)));
    let mut s = error.settings;
    if let node::Settings::OnDemand(s) = &mut s {
        s.config.poll = Duration::from_millis(2);
    }
    let running = error.prepared.launch(s, || Ok(Default::default())).unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let client = h.metrics().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let dropper = std::thread::spawn(move || {
        drop(running);
        let _ = tx.send(());
    });
    // Deliberately prevent this current-thread Tokio executor from advancing
    // the root while the other thread drops the application owner.
    let returned = rx.recv_timeout(Duration::from_secs(3)).is_ok();
    within(async {
        while !h.status().joined {
            h.changed().await.unwrap();
        }
    })
    .await;
    dropper.join().unwrap();
    assert!(returned, "Drop waited for the stalled family executor");
    assert!(client.info().joined && !client.info().running);
}

#[tokio::test]
async fn metrics_supervisor_recreation_leaves_an_empty_slot_and_preserves_the_family() {
    use ae_sync::observe::statsd::supervision::{Error, Phase};
    let lab = Lab::new();
    let mut prepared = prepare(lab.services(
        "on_demand",
        json!({"offline_mode":true}),
        Default::default(),
    ));
    let mut settings = commands(&lab);
    let metrics = metrics_settings::Metrics::new().await;
    metrics.configure(&mut prepared, &mut settings);
    let running = prepared
        .launch(settings, || Ok(Default::default()))
        .unwrap();
    let mut handle = running.handle();
    ready(&mut handle).await;
    let client = handle.metrics().unwrap();
    let supervisor = client.supervisor().unwrap();
    let routing_generation = handle
        .pow()
        .unwrap()
        .workers()
        .unwrap()
        .status()
        .routing_generation;
    for _ in 0..3 {
        let old = supervisor.current().unwrap();
        let ticket = supervisor.restart(old.number).unwrap();
        assert_eq!(within(ticket.wait()).await, Ok(old.number + 1));
        assert!(old.client.info().joined);
    }
    let last = supervisor.current().unwrap();
    let ticket = supervisor.restart(last.number).unwrap();
    assert_eq!(within(ticket.wait()).await, Err(Error::ChildLimit));
    assert!(last.client.info().joined);
    assert_eq!(supervisor.status().phase, Phase::Empty);
    assert_eq!(supervisor.status().supervisor_generation, 2);
    assert!(supervisor.current().is_none());
    assert!(!client.submit(vec![b"no-logger:0|g".to_vec()]));
    assert!(handle.status().ready && handle.status().running && handle.status().failure.is_none());
    assert_eq!(
        handle
            .pow()
            .unwrap()
            .workers()
            .unwrap()
            .status()
            .routing_generation,
        routing_generation
    );
    let ticket = supervisor.start(2).unwrap();
    assert_eq!(within(ticket.wait()).await, Ok(last.number + 1));
    assert!(client.same_owner(&handle.metrics().unwrap()));
    let exit = running.shutdown().await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(client.info().joined && !client.info().running);
}
