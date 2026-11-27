use ae_codec::api::{self, ApiType};
use ae_crypto::Ed25519SecretKey;
use ae_node::{
    bootstrap::configured::{
        self,
        pow::{self, Prepared},
    },
    configuration::{
        Configuration,
        execution::{Resources, Specification},
        resolution::{ApplicationMode, Context, Directories},
    },
};
use ae_state::Trees;
use ae_storage::pool::worker;
use ae_sync::{
    chain::{on_demand, pow::generator},
    keys,
    runtime::{connection, workers},
};
use ae_types::{AccountPublicKey, Hash256};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
#[path = "../../ae-sync/tests/common/ledger_fixtures.rs"]
mod fixtures;
#[path = "common/execution_profile.rs"]
mod profile;

struct Lab(PathBuf);
impl Lab {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "ae-configured-services-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(p.join("data/aecore")).unwrap();
        let lab = Self(p);
        lab.write("alloc.json",serde_json::to_vec(&json!({api::encode(ApiType::AccountPubkey,&payer().public_key().0):1_000_000_000_000_000_000u64})).unwrap());
        lab
    }
    fn write(&self, name: &str, bytes: impl AsRef<[u8]>) {
        let p = self.0.join(name);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, bytes).unwrap();
    }
    fn prepare(
        &self,
        kind: &str,
        system: Value,
        resources: Resources,
        gas: u64,
    ) -> configured::Prepared {
        self.prepare_with_mine_rate(kind, system, resources, gas, None)
    }
    fn prepare_with_mine_rate(
        &self,
        kind: &str,
        system: Value,
        resources: Resources,
        gas: u64,
        mine_rate: Option<Value>,
    ) -> configured::Prepared {
        let mut sys = system.as_object().unwrap().clone();
        sys.insert("custom_prefunded_accs_file".into(), json!("alloc.json"));
        let cfg = Configuration::from_value(
            json!({"fork_management":{"network_id":"ae_configured_services"},
            "system":sys,"chain":{"hard_forks":{"1":0,"6":1},"consensus":{"0":{"type":kind}}}}),
        )
        .unwrap();
        let mut ctx = Context::new(Directories {
            home: "release".into(),
            data: "data".into(),
            log: "log".into(),
        })
        .unwrap();
        if let Some(rate) = mine_rate {
            ctx.set("aecore".into(), "expected_mine_rate".into(), rate)
                .unwrap();
        }
        configured::Prepared::load(
            &cfg.resolve(&ctx).unwrap(),
            None,
            &self.0,
            &empty(),
            &Specification::from_value(&profile::default_value()).unwrap(),
            resources,
            &gas.into(),
            Default::default(),
            Default::default(),
        )
        .unwrap()
    }
    fn services(&self, kind: &str, system: Value, resources: Resources) -> Prepared {
        let open = self
            .prepare(kind, system, resources, 6_000_000)
            .open("chain", Default::default())
            .unwrap();
        let rules = open.pow_rules(3000, Default::default());
        Prepared::new(open, rules, Default::default(), Default::default()).unwrap()
    }
    fn public(&self) -> Prepared {
        self.write(
            "data/aecore/.genesis/accounts.json",
            include_str!("../../ae-state/data/mainnet_accounts.json"),
        );
        let cfg = Configuration::empty();
        let ctx = Context::new(Directories {
            home: "release".into(),
            data: "data".into(),
            log: "log".into(),
        })
        .unwrap();
        let open = configured::Prepared::load(
            &cfg.resolve(&ctx).unwrap(),
            Some(b"ae_mainnet"),
            &self.0,
            &empty(),
            &Specification::from_value(&profile::default_value()).unwrap(),
            Default::default(),
            &1.into(),
            Default::default(),
            Default::default(),
        )
        .unwrap()
        .open("chain", Default::default())
        .unwrap();
        let rules = open.pow_rules(3000, Default::default());
        Prepared::new(open, rules, Default::default(), Default::default()).unwrap()
    }
    fn worker_config(&self) -> workers::Config {
        workers::Config {
            keys: Some(keys::Config::new(
                self.0.join("keys"),
                b"isolated test".to_vec(),
            )),
            admission: worker::Settings {
                origins_interval: None,
                ..Default::default()
            },
            poll: Duration::from_millis(5),
            ..Default::default()
        }
    }
}
impl Drop for Lab {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn payer() -> Ed25519SecretKey {
    Ed25519SecretKey::from_seed(&[41; 32])
}
fn empty() -> Trees {
    Trees {
        accounts: Default::default(),
        contracts: Default::default(),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    }
}
async fn within<F: std::future::Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), f)
        .await
        .expect("bounded completion")
}
async fn start_workers(
    lab: &Lab,
    s: &Prepared,
    cache: worker::GossipCache,
) -> (workers::Handle, tokio::task::JoinHandle<workers::Status>) {
    let (mut h, svc) = s.workers(lab.worker_config(), cache).unwrap();
    let t = tokio::spawn(svc.run());
    within(async {
        while !h.status().ready {
            assert!(h.status().running, "{:?}", h.status());
            h.changed().await.unwrap();
        }
    })
    .await;
    (h, t)
}
fn network_config() -> connection::Config {
    let mut c = connection::Config::default();
    c.network.listen = Some(std::net::SocketAddrV4::new(
        std::net::Ipv4Addr::LOCALHOST,
        0,
    ));
    c.network.startup.include_default_peers = false;
    c.network.registry.policy.maximum_outbound = 0;
    c.network.registry.policy.maximum_probes = 0;
    c.health_interval = Duration::from_millis(5);
    c
}
async fn connection_ready(h: &mut connection::Handle, n: u64) -> connection::Generation {
    within(async {
        loop {
            assert!(h.status().running, "{:?}", h.status());
            if let Some(g) = h.current()
                && g.number >= n
            {
                return g;
            }
            h.changed().await.unwrap();
        }
    })
    .await
}
struct Clock;
impl generator::Clock for Clock {
    fn unix_millis(&self) -> i128 {
        static NOW: AtomicU64 = AtomicU64::new(1_000_000);
        NOW.fetch_add(10_000, Ordering::Relaxed).into()
    }
}

#[test]
fn all_startup_files_and_relative_database_use_one_explicit_working_directory() {
    let cwd = std::env::current_dir().unwrap();
    let lab = Lab::new();
    lab.write("data/aecore/.pre_iris_map_ordering_test.json", b"{}");
    lab.write(
        "data/aecore/.block_whitelist_test.json",
        serde_json::to_vec(&json!({"1":api::encode(ApiType::KeyBlockHash,&[7;32])})).unwrap(),
    );
    let prepared = lab.prepare("pow_cuckoo", json!({}), Default::default(), 123456);
    assert!(
        prepared
            .genesis()
            .trees()
            .accounts
            .lookup(payer().public_key())
            .unwrap()
            .is_some()
    );
    assert_eq!(prepared.auxiliary().inputs()[1].bytes, 2);
    let open = prepared.open("nested/chain", Default::default()).unwrap();
    assert!(lab.0.join("nested/chain/CURRENT").is_file());
    assert_eq!(open.checkpoints().get(&1), Some(&Hash256([7; 32])));
    assert_eq!(
        open.pow_rules(3000, Default::default())
            .admission
            .configured_gas_limit,
        123456.into()
    );
    assert_eq!(std::env::current_dir().unwrap(), cwd);
    println!(
        "Configured paths: relative allocation, ordering, whitelist and database use one explicit cwd without changing process cwd"
    );
}
#[test]
fn configured_application_modes_match_all_original_boolean_combinations() {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/erlang_root_modes.json")).unwrap();
    assert_eq!(rows.len(), 8);
    for row in rows {
        let cfg=Configuration::from_value(json!({"system":{"maintenance_mode":row["maintenance"],"offline_mode":row["offline"],"dev_mode":row["development"]}})).unwrap();
        let ctx = Context::new(Directories {
            home: "/release".into(),
            data: "/data".into(),
            log: "/log".into(),
        })
        .unwrap();
        let mode = cfg.resolve(&ctx).unwrap().mode().unwrap().application();
        let name = match mode {
            ApplicationMode::Normal => "normal",
            ApplicationMode::Offline => "offline",
            ApplicationMode::Maintenance => "maintenance",
            ApplicationMode::Development => "dev_mode",
        };
        assert_eq!(row["mode"], name);
    }
    println!(
        "Configured modes: all eight original maintenance/offline/development precedence cases"
    );
}
#[test]
fn configured_service_rules_reject_mismatched_gas_genesis_and_checkpoints() {
    for field in 0..3 {
        let lab = Lab::new();
        let open = lab
            .prepare("pow_cuckoo", json!({}), Default::default(), 17)
            .open("chain", Default::default())
            .unwrap();
        let mut rules = open.pow_rules(3000, Default::default());
        match field {
            0 => rules.admission.configured_gas_limit = 18.into(),
            1 => rules.genesis_hash = Hash256([1; 32]),
            _ => {
                rules.checkpoints.insert(1, Hash256([2; 32]));
            }
        }
        assert!(matches!(
            Prepared::new(open, rules, Default::default(), Default::default()),
            Err(pow::Error::Binding)
        ));
        let open = lab
            .prepare("pow_cuckoo", json!({}), Default::default(), 17)
            .open("chain", Default::default())
            .unwrap();
        assert_eq!(open.chain().database().view().unwrap().status().revision, 1);
    }
}
#[test]
fn configured_retarget_rules_are_bound_and_reopen_without_changing_chain_identity() {
    use ae_consensus::target::MineRate;
    for kind in ["pow_cuckoo", "on_demand"] {
        let lab = Lab::new();
        let mut identity = None;
        for rate in [
            json!(1000),
            serde_json::from_str("1000000000000000000000000000000").unwrap(),
            json!(0),
            json!(-1),
            json!("invalid"),
            json!(7.0),
        ] {
            let expected = rate
                .as_number()
                .and_then(|n| n.as_str().parse().ok())
                .map_or(MineRate::NonInteger, MineRate::Integer);
            for _ in 0..2 {
                let open = lab
                    .prepare_with_mine_rate(
                        kind,
                        json!({}),
                        Default::default(),
                        6_000_000,
                        Some(rate.clone()),
                    )
                    .open("chain", Default::default())
                    .unwrap();
                let current = open.chain().identity().clone();
                assert_eq!(current, *identity.get_or_insert_with(|| current.clone()));
                assert_eq!(open.chain().database().view().unwrap().status().revision, 1);
                let rules = open.pow_rules(3000, Default::default());
                assert_eq!(rules.expected_mine_rate, expected);
                let mut wrong = rules.clone();
                wrong.expected_mine_rate = MineRate::default();
                let Err(failure) =
                    Prepared::try_new(open, wrong, Default::default(), Default::default())
                else {
                    panic!("mismatched retarget interval was accepted")
                };
                assert!(matches!(failure.error, pow::Error::Binding));
                let prepared =
                    Prepared::new(failure.open, rules, Default::default(), Default::default())
                        .unwrap();
                assert_eq!(prepared.rules().expected_mine_rate, expected);
            }
        }
    }
}
#[tokio::test]
async fn shared_configured_importer_survives_connection_restart_and_joined_shutdown() {
    let lab = Lab::new();
    let s = lab.public();
    let blocks = fixtures::blocks(s.configured().chain().settings().profile());
    let block = &blocks[0];
    let time = match block {
        ae_chain::block::Block::Key(k) => k.time + 60_000,
        _ => unreachable!(),
    };
    let (cache, controller) = worker::GossipCache::managed(200, 100_000).unwrap();
    let (w, wt) = start_workers(&lab, &s, cache).await;
    let s = s.activate(&w).unwrap();
    let identity = w
        .bindings()
        .keys
        .unwrap()
        .current()
        .unwrap()
        .peer_identity()
        .unwrap()
        .await
        .unwrap();
    let (mut c, svc) = s
        .connections(&w, identity, controller, network_config(), || {
            Ok(Default::default())
        })
        .unwrap();
    let ct = tokio::spawn(svc.run());
    let first = connection_ready(&mut c, 1).await;
    s.importer()
        .import_synced_block(block.encode().unwrap(), time)
        .unwrap()
        .wait()
        .await
        .unwrap();
    first.cache.as_ref().unwrap().shutdown();
    let second = connection_ready(&mut c, 2).await;
    assert!(first.network.status().joined);
    assert!(second.cache.as_ref().unwrap().is_running());
    assert_eq!(c.status().restart_attempts, 1);
    s.importer()
        .import_synced_block(block.encode().unwrap(), time)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let accepted = s
        .importer()
        .import_synced_block(block.encode().unwrap(), time)
        .unwrap();
    c.shutdown();
    let status = within(ct).await.unwrap();
    assert!(status.joined, "{status:?}");
    accepted.wait().await.unwrap();
    s.importer()
        .import_synced_block(block.encode().unwrap(), time)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        s.configured()
            .chain()
            .database()
            .view()
            .unwrap()
            .status()
            .tip,
        Some(block.hash().unwrap())
    );
    w.shutdown();
    assert!(within(wt).await.unwrap().joined);
    s.stop_and_join().await;
    println!(
        "Configured importer: original first mainnet key admitted, connection cache replaced, joined connections leave the same importer live"
    );
}
#[tokio::test]
async fn configured_on_demand_owner_produces_and_reopens_real_chain() {
    let lab = Lab::new();
    let open = lab
        .prepare_with_mine_rate(
            "on_demand",
            json!({"dev_mode":true}),
            Default::default(),
            6_000_000,
            Some(json!("invalid")),
        )
        .open("chain", Default::default())
        .unwrap();
    let rules = open.pow_rules(3000, Default::default());
    let s = Prepared::new(open, rules, Default::default(), Default::default()).unwrap();
    let (w, wt) = start_workers(&lab, &s, Default::default()).await;
    let s = s.activate(&w).unwrap();
    let bindings = w.bindings();
    let pool = bindings.admission.current().unwrap().client;
    let keys = bindings.keys.unwrap().current().unwrap();
    let controller = s
        .on_demand(
            pool.clone(),
            keys,
            on_demand::runtime::Config::new(AccountPublicKey([42; 32])),
            Arc::new(Clock),
        )
        .unwrap();
    let result = tokio::task::spawn_blocking(move || {
        let mut controller = controller;
        let r = controller.execute(on_demand::control::Command::Blocks {
            count: 20,
            kind: on_demand::control::Kind::Key,
        });
        (r, controller.retire())
    })
    .await
    .unwrap();
    let (report, retired) = result;
    assert!(report.command.result.is_ok(), "{:?}", report.command.result);
    assert_eq!(report.admissions.len(), 20);
    assert!(report.admissions.iter().all(|r| r.result.is_ok()));
    assert!(!retired.pending_pool());
    let tip = s
        .configured()
        .chain()
        .database()
        .view()
        .unwrap()
        .status()
        .tip;
    let (_, cache) = worker::GossipCache::managed(10, 1000).unwrap();
    assert!(matches!(
        s.connections(
            &w,
            Arc::new(ae_p2p::noise::Identity::from_secret([1; 32])),
            cache,
            network_config(),
            || Ok(Default::default())
        ),
        Err(pow::Error::Mode)
    ));
    drop(retired);
    w.shutdown();
    assert!(within(wt).await.unwrap().joined);
    s.stop_and_join().await;
    drop(pool);
    drop(report);
    drop(w);
    drop(s);
    let reopened = lab.services("on_demand", json!({"dev_mode":true}), Default::default());
    assert_eq!(
        reopened
            .configured()
            .chain()
            .database()
            .view()
            .unwrap()
            .status()
            .tip,
        tip
    );

    println!(
        "Configured on-demand: actual key generation, ordinary admission, explicit network-mode gate and retained-chain reopen"
    );
}

#[tokio::test]
async fn configured_generator_retains_signature_budget_without_mutating_chain_or_pool() {
    for restricted in [true, false] {
        let lab = Lab::new();
        let mut resources = Resources::default();
        if restricted {
            resources.signatures.max_encoded_bytes = 0;
        }
        let s = lab.services("on_demand", json!({"dev_mode":true}), resources);
        let (w, wt) = start_workers(&lab, &s, Default::default()).await;
        let s = s.activate(&w).unwrap();
        let bindings = w.bindings();
        let pool = bindings.admission.current().unwrap().client;
        let keys = bindings.keys.unwrap().current().unwrap();
        let controller = s
            .on_demand(
                pool.clone(),
                keys,
                on_demand::runtime::Config::new(AccountPublicKey([42; 32])),
                Arc::new(Clock),
            )
            .unwrap();
        let (report, retired) = tokio::task::spawn_blocking(move || {
            let mut c = controller;
            let r = c.execute(on_demand::control::Command::Key);
            (r, c.retire())
        })
        .await
        .unwrap();
        assert!(report.command.result.is_ok(), "{:?}", report.command.result);
        assert!(!retired.pending_pool());
        drop(retired);
        let before = s.configured().chain().database().view().unwrap().status();
        let mut spend = ae_tx::Spend::new(
            payer().public_key(),
            ae_types::Id::new(ae_types::IdTag::Account, [43; 32]),
        )
        .unwrap();
        spend.set_nonce(1.into());
        spend.set_amount(7.into());
        spend.set_fee(100_000_000_000_000u64.into());
        let mut signed = s
            .configured()
            .chain()
            .settings()
            .profile()
            .signing_prefix(false);
        use ae_tx::CanonicalTransaction;
        signed.extend_from_slice(&spend.encode());
        let tx = Arc::new(ae_tx::SignedTransaction::new(
            ae_tx::Transaction::new(ae_tx::TransactionLeaf::Spend(spend)),
            vec![payer().sign(&signed)],
        ));
        pool.push(tx.clone(), ae_mempool::pool::Event::Created, false)
            .unwrap()
            .await
            .unwrap();
        let generator = s
            .generator(pool.clone(), Default::default(), Arc::new(Clock))
            .unwrap();
        let client = generator.client();
        let request = client.start().unwrap();
        within(async {
            loop {
                if let Some(r) = request.try_result() {
                    r.unwrap();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        within(async {
            loop {
                assert!(client.status().running, "{:?}", client.status());
                if restricted {
                    if client.status().last_error.is_some() {
                        assert!(client.candidate().is_none());
                        break;
                    }
                } else if let Some(c) = client.candidate() {
                    assert_eq!(c.prepared().value.candidate.block().transactions.len(), 1);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        assert_eq!(
            s.configured().chain().database().view().unwrap().status(),
            before
        );
        let report = tokio::task::spawn_blocking(move || generator.shutdown().unwrap())
            .await
            .unwrap();
        assert!(report.retirement.is_none());
        drop(report);
        drop(client);
        assert!(
            ae_storage::pool::Stored::load(s.configured().chain().database().view().unwrap())
                .unwrap()
                .members(10)
                .unwrap()
                .contains(&tx.hash())
        );
        w.shutdown();
        assert!(within(wt).await.unwrap().joined);
        s.stop_and_join().await;
    }
    println!(
        "Configured generator: explicit signature exhaustion stops candidate execution atomically; normal limits pack the same real spend"
    );
}

use ae_node::pow as root;
use ae_sync::chain::pow::group;
fn root_config(lab: &Lab) -> root::Config {
    root::Config {
        workers: lab.worker_config(),
        connections: network_config(),
        poll: Duration::from_millis(5),
        startup_timeout: Duration::from_secs(10),
        ..Default::default()
    }
}
fn launch_root(lab: &Lab, prepared: Prepared) -> root::Running {
    root::launch(
        prepared,
        root_config(lab),
        Arc::new(group::SystemClocks::default()),
        || Ok(Default::default()),
    )
    .unwrap()
}
async fn root_ready(h: &root::Handle, starts: u64) {
    within(async {
        loop {
            let s = h.status();
            assert!(s.running && s.failure.is_none(), "{s:?}");
            if s.ready && s.production_starts == starts {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
}
fn first_key(s: &Prepared) -> (Vec<u8>, u64, Hash256) {
    let block = fixtures::blocks(s.configured().chain().settings().profile()).remove(0);
    let ae_chain::block::Block::Key(k) = &block else {
        unreachable!()
    };
    (
        block.encode().unwrap(),
        k.time + 60_000,
        block.hash().unwrap(),
    )
}
fn assert_root_joined(exit: &root::Exit) {
    assert!(exit.status.joined, "{:?}", exit.status);
    assert!(!exit.status.running);
    assert_eq!(exit.status.phase, root::Phase::Stopped);
    if let Some(w) = &exit.status.workers {
        assert!(w.joined && !w.running, "{w:?}");
    }
    if let Some(c) = &exit.status.connections {
        assert!(c.joined && !c.running, "{c:?}");
    }
    if let Some(pending) = &exit.status.pending_key {
        assert!(
            pending.joined && !pending.running && !pending.accepting,
            "{pending:?}"
        );
    }
    if let Some(p) = &exit.production {
        assert!(p.as_ref().unwrap().is_settled());
    }
    if let Some(p) = &exit.commands {
        assert!(p.as_ref().unwrap().is_settled());
    }
}

#[tokio::test]
async fn configured_cuckoo_root_joins_all_modes_and_retains_accepted_mainnet_receipt() {
    for mode in [
        json!({}),
        json!({"offline_mode":true}),
        json!({"maintenance_mode":true}),
        json!({"dev_mode":true}),
    ] {
        let lab = Lab::new();
        let normal = mode == json!({});
        let s = if normal {
            lab.public()
        } else {
            lab.services("pow_cuckoo", mode, Default::default())
        };
        let block = normal.then(|| first_key(&s));
        let db = s.configured().chain().database().clone();
        let running = launch_root(&lab, s);
        let h = running.handle();
        root_ready(&h, 1).await;
        assert_eq!(h.connections().is_some(), normal);
        h.set_enabled(true).unwrap();
        within(async {
            while !h
                .production()
                .unwrap()
                .current()
                .unwrap()
                .producer
                .status()
                .enabled
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        h.set_enabled(false).unwrap();
        let importer = h.importer().unwrap();
        let accepted = block
            .as_ref()
            .map(|(bytes, time, _)| importer.import_synced_block(bytes.clone(), *time).unwrap());
        let exit = within(running.shutdown()).await.unwrap();
        assert_root_joined(&exit);
        assert!(exit.status.failure.is_none(), "{:?}", exit.status);
        assert_eq!(h.set_enabled(true), Err(root::Error::Closed));
        assert!(h.take_exit().is_none());
        if let Some(ticket) = accepted {
            ticket.wait().await.unwrap();
            let (bytes, time, hash) = block.unwrap();
            assert_eq!(db.view().unwrap().status().tip, Some(hash));
            assert!(matches!(
                importer.import_synced_block(bytes, time),
                Err(ae_sync::chain::Error::Closed)
            ));
            drop(importer);
            drop(db);
            drop(h);
            drop(exit);
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
        "Configured Cuckoo root: four application modes, real first-mainnet-key admission, unread receipt survives joined shutdown and canonical tip reopens"
    );
}

#[tokio::test]
async fn configured_cuckoo_root_rebinds_managed_workers_without_replacing_importer() {
    let lab = Lab::new();
    let s = lab.public();
    let (bytes, time, _) = first_key(&s);
    let running = launch_root(&lab, s);
    let h = running.handle();
    root_ready(&h, 1).await;
    let importer = h.importer().unwrap();
    importer
        .import_synced_block(bytes.clone(), time)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let w = h.workers().unwrap();
    let c = h.connections().unwrap();
    let first_connection = c.current().unwrap();
    let p = h.production().unwrap();
    p.current().unwrap().generator.request_shutdown();
    within(async {
        loop {
            while h.try_output().is_some() {}
            assert!(h.status().running, "{:?}", h.status());
            if p.current().is_some_and(|g| g.id.generator == 2) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert_eq!(h.status().production_starts, 1);
    let mut retired = 0;
    for (child, starts) in [(workers::Child::Keys, 2), (workers::Child::Admission, 3)] {
        let old = h.production().unwrap();
        within(w.restart(child, 1).unwrap().wait()).await.unwrap();
        within(async {
            loop {
                while let Some(output) = h.try_output() {
                    if let root::Output::Retired(r) = output {
                        assert!(!r.status.running);
                        assert!(!r.settled.is_empty());
                        retired += 1;
                    }
                }
                let status = h.status();
                assert!(status.running && status.failure.is_none(), "{status:?}");
                if status.ready && status.production_starts == starts {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(!old.status().running);
        assert!(Arc::ptr_eq(&importer, &h.importer().unwrap()));
        importer
            .import_synced_block(bytes.clone(), time)
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    assert_eq!(retired, 2);
    within(w.restart(workers::Child::Resilience, 1).unwrap().wait())
        .await
        .unwrap();
    importer
        .import_synced_block(bytes.clone(), time)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(h.status().production_starts, 3);
    assert_eq!(c.current().unwrap().number, first_connection.number);
    assert!(first_connection.network.status().running);
    first_connection.cache.as_ref().unwrap().shutdown();
    let mut c2 = c.clone();
    connection_ready(&mut c2, 2).await;
    assert!(first_connection.network.status().joined);
    importer
        .import_synced_block(bytes, time)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let exit = within(running.shutdown()).await.unwrap();
    assert_root_joined(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
    println!(
        "Configured Cuckoo root: real pair, key, admission, resilience and connection-cache replacements retain one importer and settled retirement receipts"
    );
}

#[tokio::test]
async fn configured_cuckoo_root_full_output_queue_preserves_rebinding_and_shutdown() {
    for resume in [false, true] {
        let lab = Lab::new();
        let mut cfg = root_config(&lab);
        cfg.outputs = 1;
        cfg.startup_timeout = Duration::from_secs(2);
        let running = root::launch(
            lab.public(),
            cfg,
            Arc::new(group::SystemClocks::default()),
            || Ok(Default::default()),
        )
        .unwrap();
        let h = running.handle();
        root_ready(&h, 1).await;
        // The first Ready observation fills the only root output slot.
        tokio::time::sleep(Duration::from_millis(30)).await;
        within(
            h.workers()
                .unwrap()
                .restart(workers::Child::Keys, 1)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        within(async {
            while h.status().phase != root::Phase::Rebinding {
                assert!(h.status().running, "{:?}", h.status());
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        h.set_enabled(true).unwrap();
        tokio::time::sleep(Duration::from_millis(2100)).await;
        assert!(
            h.status().running && h.status().failure.is_none(),
            "{:?}",
            h.status()
        );
        assert_eq!(h.status().production_starts, 1);
        if resume {
            assert!(matches!(h.try_output(), Some(root::Output::Production(_))));
            root_ready(&h, 2).await;
            within(async {
                while !h
                    .production()
                    .unwrap()
                    .current()
                    .unwrap()
                    .producer
                    .status()
                    .enabled
                {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
        }
        let exit = within(running.shutdown()).await.unwrap();
        assert_root_joined(&exit);
        assert!(exit.status.failure.is_none(), "{:?}", exit.status);
        assert_eq!(exit.retained.len(), 1);
        assert_eq!(matches!(exit.retained[0], root::Output::Retired(_)), resume);
        assert!(
            !exit
                .production
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .settled
                .is_empty()
        );
    }
    println!(
        "Configured Cuckoo root: full output queue backpressures replacement beyond startup timeout, retains desired production state on resume, and preserves joined shutdown receipts"
    );
}

#[tokio::test]
async fn configured_cuckoo_root_partial_startup_failures_join_started_children() {
    for stage in 0..3 {
        let lab = Lab::new();
        let mut cfg = root_config(&lab);
        match stage {
            0 => cfg.workers.poll = Duration::ZERO,
            1 => cfg.connections.health_interval = Duration::ZERO,
            _ => cfg.production.poll = Duration::ZERO,
        }
        let running = root::launch(
            lab.public(),
            cfg,
            Arc::new(group::SystemClocks::default()),
            || Ok(Default::default()),
        )
        .unwrap();
        let exit = within(running.wait()).await.unwrap();
        assert_root_joined(&exit);
        let owner = ["Workers", "Connections", "Production"][stage];
        assert_eq!(
            exit.status.failure,
            Some(root::Error::Start(format!("{owner}(Configuration)")))
        );
        assert_eq!(exit.status.production_starts, 0);
        assert_eq!(exit.prepared.is_some(), stage == 0);
        assert_eq!(exit.services.is_some(), stage > 0);
        assert_eq!(exit.status.connections.is_some(), stage == 2);
    }
    println!(
        "Configured Cuckoo root: worker, connection and production startup failures retain inputs and join every previously started owner"
    );
}

#[tokio::test]
async fn configured_cuckoo_root_drop_and_early_shutdown_keep_single_terminal_receipt() {
    for started in [false, true] {
        let lab = Lab::new();
        let running = launch_root(&lab, lab.public());
        let h = running.handle();
        if started {
            root_ready(&h, 1).await;
        }
        drop(running);
        within(async {
            while h.status().running {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        let exit = h.take_exit().unwrap();
        assert_root_joined(&exit);
        assert!(exit.status.failure.is_none(), "{:?}", exit.status);
        assert_eq!(exit.prepared.is_some(), !started);
        assert!(h.take_exit().is_none());
    }
    println!(
        "Configured Cuckoo root: dropping either prepared or running ownership joins children and retains one retrievable terminal receipt"
    );
}

#[tokio::test]
async fn configured_cuckoo_root_permanent_child_exit_is_terminal_without_root_restart() {
    let original: Value =
        serde_json::from_str(include_str!("fixtures/erlang_root_supervision.json")).unwrap();
    assert_eq!(original["root"]["intensity"], 0);
    assert_eq!(original["root"]["strategy"], "one_for_one");
    for child in [
        root::Child::Workers,
        root::Child::Connections,
        root::Child::Production,
        root::Child::PendingKey,
    ] {
        let lab = Lab::new();
        let running = launch_root(&lab, lab.public());
        let h = running.handle();
        root_ready(&h, 1).await;
        match child {
            root::Child::Workers => h.workers().unwrap().shutdown(),
            root::Child::Connections => h.connections().unwrap().shutdown(),
            root::Child::Production => h.production().unwrap().request_shutdown(),
            root::Child::PendingKey => h.pending_key().unwrap().request_shutdown(),
        }
        let exit = within(running.wait()).await.unwrap();
        assert_root_joined(&exit);
        assert_eq!(exit.status.failure, Some(root::Error::ChildStopped(child)));
        assert_eq!(exit.status.production_starts, 1);
    }
    println!(
        "Configured Cuckoo root: permanent worker, connection or production owner exit is terminal under the original zero-root-restart policy"
    );
}

#[tokio::test]
async fn configured_importer_activation_rejects_unready_or_foreign_worker_owner() {
    let lab = Lab::new();
    let prepared = lab.public();
    let (w, service) = prepared
        .workers(lab.worker_config(), Default::default())
        .unwrap();
    let failed = prepared.activate(&w).err().unwrap();
    assert!(matches!(failed.error, pow::Error::Configuration));
    let task = tokio::spawn(service.run());
    within(async {
        while !w.status().ready {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let other = Lab::new();
    let failed_other = other.public().activate(&w).err().unwrap();
    assert!(matches!(failed_other.error, pow::Error::Binding));
    drop(failed_other);
    let services = failed.prepared.activate(&w).unwrap();
    w.shutdown();
    assert!(within(task).await.unwrap().joined);
    services.stop_and_join().await;
}

#[path = "configured_services/family.rs"]
mod family;
#[path = "configured_services/on_demand.rs"]
mod on_demand_root;
#[path = "configured_services/stratum.rs"]
mod stratum_root;
