use super::*;
use ae_node::{
    bootstrap::configured::{self, hyperchain as startup},
    configuration::execution::Resources,
    hyperchain as root,
};
use ae_sync::{
    chain::hyperchain::conductor::{self, runtime as producer},
    keys,
};
use ae_vm_fate::maps::order::Otp26;
use std::{
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::{Duration, Instant},
};
#[path = "hyperchain/parent.rs"]
mod parent;
use parent::Parent;

fn key(n: u64) -> [u8; 32] {
    let mut k = [0; 32];
    k[24..].copy_from_slice(&n.to_be_bytes());
    k
}
fn signer(n: u64) -> ae_crypto::Ed25519SecretKey {
    ae_crypto::Ed25519SecretKey::from_seed(&key(n))
}
struct Hc {
    temp: Temp,
    config: Configuration,
    initial: Trees,
    profile: ae_node::configuration::execution::Specification,
}
impl Hc {
    fn new(protocol: u64, system: Value, parent: &Parent) -> Self {
        let data = unpack(include_str!(
            "../../../ae-hyperchain/tests/fixtures/erlang_hc_genesis.json"
        ));
        let row = data["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["protocol"] == protocol)
            .unwrap();
        let temp = Temp::new();
        let specs: Vec<_> = row["specs"]
            .as_array()
            .unwrap()
            .iter()
            .cloned()
            .map(contract_numbers)
            .collect();
        let calls: Vec<_> = row["calls"]
            .as_array()
            .unwrap()
            .iter()
            .cloned()
            .map(contract_numbers)
            .collect();
        let contracts = temp.write(
            "contracts.json",
            serde_json::to_vec(&json!({"contracts":specs,"calls":calls})).unwrap(),
        );
        let validators: Vec<_> = row["validators"].as_array().unwrap().iter().enumerate().map(|(i,v)| json!({
            "owner":public(&v["owner"]),"caller":public(&v["caller"]),
            "sign_key":api::encode(ApiType::AccountPubkey,&signer(12+i as u64).public_key().0),
            "stake":num(&v["stake"]),"restake":v["restake"]
        })).collect();
        let stakers: Vec<_> = [12,13].into_iter().map(|n| {
            let s = signer(n);
            let mut private = key(n).to_vec();private.extend(s.public_key().0);
            json!({"hyper_chain_account":{"pub":api::encode(ApiType::AccountPubkey,&s.public_key().0),"priv":hex::encode(private)}})
        }).collect();
        let cfg = json!({"fork_management":{"network_id":"ae_configured_hc"},"system":system,
        "chain":{"hard_forks":{protocol.to_string():{"height":0,"contracts_file":contracts}},
        "consensus":{"0":{"type":"hyperchain","config":{
            "contract_owner":api::encode(ApiType::AccountPubkey,&key(1)),
            "election_contract":specs[1]["pubkey"],"rewards_contract":specs[1]["pubkey"],"staking_contract":specs[0]["pubkey"],
            "initial_validators":validators,"stakers":stakers,"child_epoch_length":3,"pinning_reward_value":7,
            "child_block_time":4,"child_block_production_time":1,"fixed_coinbase":37,"fee_distribution":[30,50,20],
            "parent_chain":{"start_height":1,"finality":2,"parent_epoch_length":5,
                "polling":{"fetch_interval":20,"nodes":[parent.url]}}
        }}}}});
        let config = Configuration::from_value(cfg).unwrap();
        let mut initial = empty();
        for a in row["initial"]["accounts"].as_array().unwrap() {
            let key = AccountPublicKey(native_bytes(&a["key"]).try_into().unwrap());
            initial
                .accounts
                .enter(&Account::deserialize(key, &native_bytes(&a["encoded"])).unwrap())
                .unwrap();
        }
        initial
            .accounts
            .enter(&Account::new(
                signer(41).public_key(),
                1_000_000_000_000_000_000u64.into(),
            ))
            .unwrap();
        Self {
            temp,
            config,
            initial,
            profile: startup_profile::specification(&data["profile"]),
        }
    }
    fn open(&self, resources: Resources, path: &str) -> configured::Open {
        configured::Prepared::load(
            &self.config.resolve(&self.temp.context()).unwrap(),
            None,
            &self.temp.0,
            &self.initial,
            &self.profile,
            resources,
            &6_000_000.into(),
            Default::default(),
            Default::default(),
        )
        .unwrap()
        .open(path, Default::default())
        .unwrap()
    }
    fn prepared(&self, resources: Resources, path: &str) -> startup::Prepared {
        startup::Prepared::new(
            self.open(resources, path),
            startup::Limits {
                micro_cycle_ms: 1,
                ..Default::default()
            },
        )
        .unwrap()
    }
    fn inputs(&self, p: startup::Prepared) -> root::ConfiguredInputs {
        let (gossip, cache) = ae_storage::pool::worker::GossipCache::managed(200, 10000).unwrap();
        let mut runtime = p.runtime();
        runtime.gossip = gossip;
        runtime.pool_worker.origins_interval = None;
        root::ConfiguredInputs {
            prepared: p,
            runtime,
            services: Default::default(),
            cache,
            identity: root::Identity::Managed,
        }
    }
    fn root_config(&self) -> root::Config {
        let mut c = root::Config::default();
        c.workers.keys = Some(keys::Config::new(
            self.temp.0.join("keys"),
            b"isolated fixture".to_vec(),
        ));
        c.workers.gc = None;
        c.workers.poll = Duration::from_millis(2);
        c.connections.network.listen = Some(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::LOCALHOST,
            0,
        ));
        c.connections.network.startup.include_default_peers = false;
        c.connections.network.registry.policy.maximum_outbound = 0;
        c.connections.network.registry.policy.maximum_probes = 0;
        c.connections.health_interval = Duration::from_millis(2);
        c.poll = Duration::from_millis(2);
        c.producer.poll = Duration::from_millis(1);
        c
    }
    fn launch(&self, inputs: root::ConfiguredInputs, clock: Clock) -> root::Running<Otp26> {
        match root::launch_configured(inputs, self.root_config(), clock, || Ok(Default::default()))
        {
            Ok(r) => r,
            Err(e) => panic!("{:?}", e.error),
        }
    }
}
#[derive(Clone)]
struct Clock(Arc<Mutex<(u64, Instant)>>);
impl Clock {
    fn new() -> Self {
        Self(Arc::new(Mutex::new((1000, Instant::now()))))
    }
    fn tick(&self, h: &root::Handle<Otp26>) {
        if let Some(d) = h.status().producer.and_then(|p| p.deadline) {
            let mut t = self.0.lock().unwrap();
            t.0 += d.saturating_duration_since(t.1).as_millis() as u64;
            t.1 = d;
        }
    }
}
impl producer::Clock for Clock {
    fn now(&self) -> Result<(u64, Instant), conductor::Error> {
        Ok(*self.0.lock().unwrap())
    }
}
async fn ready(h: &mut root::Handle<Otp26>) {
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
fn clean(e: &root::Exit<Otp26>) {
    assert!(e.status.joined && !e.status.running, "{:?}", e.status);
    assert!(e.configured.is_some());
    if let Some(w) = &e.status.workers {
        assert!(w.joined);
    }
    if let Some(c) = &e.status.connections {
        assert!(c.joined);
    }
}
async fn produce(h: &root::Handle<Otp26>, clock: &Clock, count: usize) -> Vec<conductor::Output> {
    h.start_production().unwrap();
    let mut outputs = vec![];
    let mut keys = 0;
    let end = Instant::now() + Duration::from_secs(30);
    while keys < count {
        assert!(
            Instant::now() < end && h.status().failure.is_none(),
            "{:?}",
            h.status()
        );
        clock.tick(h);
        while let Some(o) = h.try_output() {
            if let conductor::Output::Attempt(a) = &o {
                keys += usize::from(matches!(
                    a.outcome,
                    ae_sync::chain::hyperchain::eoe::owner::production::Outcome::Produced { .. }
                ));
            }
            outputs.push(o);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    h.stop_production().unwrap();
    outputs
}
fn transaction(p: &startup::Prepared) -> Arc<ae_tx::SignedTransaction<ae_tx::Transaction>> {
    use ae_tx::CanonicalTransaction;
    let payer = signer(41);
    let mut tx = ae_tx::Spend::new(
        payer.public_key(),
        ae_types::Id::new(ae_types::IdTag::Account, key(42)),
    )
    .unwrap();
    tx.set_nonce(1.into());
    tx.set_amount(7.into());
    tx.set_fee(100_000_000_000_000u64.into());
    let mut bytes = p
        .configured()
        .chain()
        .settings()
        .profile()
        .signing_prefix(false);
    bytes.extend(tx.encode());
    Arc::new(ae_tx::SignedTransaction::new(
        ae_tx::Transaction::new(ae_tx::TransactionLeaf::Spend(tx)),
        vec![payer.sign(&bytes)],
    ))
}

#[test]
fn configured_hyperchain_preserves_genesis_binding_rules_and_explicit_execution() {
    let parent = Parent::start();
    for protocol in [6, 7, 8] {
        let lab = Hc::new(protocol, json!({"dev_mode":true}), &parent);
        let mut resources = Resources::default();
        resources.signatures.max_encoded_bytes = 0;
        let p = lab.prepared(resources, "chain");
        let e = p.execution();
        assert_eq!(e.rules.fixed_coinbase, 37.into());
        assert_eq!(e.rules.fee_distribution, [30.into(), 50.into(), 20.into()]);
        assert_eq!(e.rules.configured_gas_limit, 6_000_000.into());
        assert_eq!(
            e.identity.execution_profile,
            p.configured().execution().fingerprint()
        );
        let reader = e
            .reader(
                p.configured().chain().database().view().unwrap(),
                e.profile.clone(),
            )
            .unwrap();
        let view = reader.view(&ae_hyperchain::chain::RunEnv::Top).unwrap();
        reader.with_execution(&view, |cfg| {
            assert_eq!(cfg.signature_limits.max_encoded_bytes, 0)
        });
        assert!(
            reader
                .epoch_info(&ae_hyperchain::chain::RunEnv::Top)
                .is_ok()
        );
        let foreign = execution_fixture::execution(p.configured().chain().settings().profile());
        let mut bad = e.clone();
        bad.owned = Some(Arc::new(foreign));
        assert!(matches!(
            bad.validate_execution(),
            Err(ae_chain::repository::Error::Identity)
        ));
        assert!(
            bad.reader(
                p.configured().chain().database().view().unwrap(),
                bad.profile.clone()
            )
            .is_err()
        );
    }
    println!(
        "Configured Hyperchain: protocols 6-8 retain actual genesis VM binding, reward/fee rules and explicit execution resources; foreign consensus execution is rejected"
    );
}

#[tokio::test]
async fn configured_hyperchain_root_produces_real_transfer_and_reopens_in_all_modes() {
    let parent = Parent::start();
    for mode in [
        json!({}),
        json!({"dev_mode":true}),
        json!({"offline_mode":true}),
        json!({"maintenance_mode":true}),
    ] {
        let network = mode == json!({});
        let lab = Hc::new(8, mode, &parent);
        let p = lab.prepared(Default::default(), "chain");
        let tx = transaction(&p);
        let hash = tx.hash();
        let db = p.configured().chain().database().clone();
        let clock = Clock::new();
        let running = lab.launch(lab.inputs(p), clock.clone());
        let mut h = running.handle();
        ready(&mut h).await;
        assert_eq!(h.connections().is_some(), network);
        let pool = h
            .workers()
            .unwrap()
            .bindings()
            .admission
            .current()
            .unwrap()
            .client;
        pool.push(tx, ae_mempool::pool::Event::Created, false)
            .unwrap()
            .await
            .unwrap();
        let outputs = produce(&h, &clock, 3).await;
        assert!(!outputs.is_empty());
        assert!(db.view().unwrap().location(hash).unwrap().is_some());
        let e = running.shutdown().await.unwrap();
        clean(&e);
        assert!(e.status.failure.is_none(), "{:?}", e.status);
        let status = db.view().unwrap().status();
        let identity = db.view().unwrap().identity().clone();
        assert!(status.revision > 3);
        drop(outputs);
        drop(e);
        drop(h);
        drop(pool);
        drop(db);
        let reopened = lab.prepared(Default::default(), "chain");
        assert_eq!(
            reopened
                .configured()
                .chain()
                .database()
                .view()
                .unwrap()
                .status(),
            status
        );
        assert_eq!(reopened.configured().chain().identity(), &identity);
    }
    assert!(!parent.requests.lock().unwrap().is_empty());
    println!(
        "Configured Hyperchain root: actual configured contracts, localhost parent queries, funded transfer, normal/development/offline/maintenance production, joined shutdown and exact durable reopen"
    );
}

#[tokio::test]
async fn configured_hyperchain_start_failure_and_early_drop_retain_opened_chain() {
    let parent = Parent::start();
    for fail in [false, true] {
        let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
        let p = lab.prepared(Default::default(), "chain");
        let identity = p.configured().chain().identity().clone();
        let mut inputs = lab.inputs(p);
        if fail {
            inputs.runtime.events_per_step = 0;
        }
        let running = lab.launch(inputs, Clock::new());
        let mut h = running.handle();
        if !fail {
            drop(running);
        } else {
            let e = running.wait().await.unwrap();
            clean(&e);
            assert!(matches!(e.status.failure, Some(root::Error::Start(_))));
            assert_eq!(e.configured.as_ref().unwrap().chain().identity(), &identity);
            continue;
        }
        tokio::time::timeout(Duration::from_secs(20), async {
            while h.status().running {
                h.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        let e = h.take_exit().unwrap();
        clean(&e);
        assert!(e.status.failure.is_none(), "{:?}", e.status);
        assert_eq!(e.configured.as_ref().unwrap().chain().identity(), &identity);
        assert!(h.take_exit().is_none());
    }
    println!(
        "Configured Hyperchain root: invalid conductor startup and early root drop retain the exact opened chain without claiming unfinished children joined"
    );
}

#[tokio::test]
async fn configured_hyperchain_signature_limit_survives_peer_key_replacement() {
    let parent = Parent::start();
    for restricted in [true, false] {
        let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
        let mut resources = Resources::default();
        if restricted {
            resources.signatures.max_encoded_bytes = 0;
        }
        let p = lab.prepared(resources, "chain");
        let tx = transaction(&p);
        let db = p.configured().chain().database().clone();
        let clock = Clock::new();
        let running = lab.launch(lab.inputs(p), clock.clone());
        let mut h = running.handle();
        ready(&mut h).await;
        let importer = h.importer().unwrap();
        let workers = h.workers().unwrap();
        let pool = workers.bindings().admission.current().unwrap().client;
        drop(produce(&h, &clock, 3).await);
        let old_keys = workers.bindings().keys.unwrap().current().unwrap();
        let peer = old_keys.info().keys.peer;
        assert_eq!(
            workers
                .restart(ae_sync::runtime::workers::Child::Keys, 1)
                .unwrap()
                .wait()
                .await
                .unwrap(),
            2
        );
        assert!(!old_keys.info().running);
        assert_eq!(
            workers
                .bindings()
                .keys
                .unwrap()
                .current()
                .unwrap()
                .info()
                .keys
                .peer,
            peer
        );
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                assert!(h.status().failure.is_none(), "{:?}", h.status());
                let p = h.status().producer.unwrap();
                if !p.producing && !p.stopping {
                    break;
                }
                clock.tick(&h);
                while h.try_output().is_some() {}
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&importer, &h.importer().unwrap()));
        assert!(pool.same_worker(&workers.bindings().admission.current().unwrap().client));
        let before = db.view().unwrap().status();
        pool.push(tx.clone(), ae_mempool::pool::Event::Created, false)
            .unwrap()
            .await
            .unwrap();
        let e = if restricted {
            h.start_production().unwrap();
            tokio::time::timeout(Duration::from_secs(20), async {
                while h.status().running {
                    clock.tick(&h);
                    while h.try_output().is_some() {}
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            let e = running.wait().await.unwrap();
            let failure = e
                .producer
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .status
                .failure
                .as_ref()
                .unwrap();
            assert!(
                matches!(failure, conductor::Error::Production(_)),
                "{failure:?}"
            );
            assert!(
                format!("{failure:?}").contains("Signature(ResourceLimit)"),
                "{failure:?}"
            );
            assert_eq!(db.view().unwrap().status(), before);
            assert!(
                ae_storage::pool::Stored::load(db.view().unwrap())
                    .unwrap()
                    .members(10)
                    .unwrap()
                    .contains(&tx.hash())
            );
            e
        } else {
            drop(produce(&h, &clock, 3).await);
            let e = running.shutdown().await.unwrap();
            assert!(e.status.failure.is_none(), "{:?}", e.status);
            e
        };
        clean(&e);
        assert_eq!(e.status.producer.as_ref().unwrap().session_generation, 1);
        assert_eq!(
            db.view().unwrap().location(tx.hash()).unwrap().is_some(),
            !restricted
        );
        assert!(matches!(
            importer.import_synced_block(vec![], 1000),
            Err(ae_sync::chain::Error::Closed)
        ));
    }
    assert!(parent.requests.lock().unwrap().contains(&6));
    println!(
        "Configured Hyperchain resources: signature exhaustion after peer-key replacement stops production, preserves selected state and retains the transaction; normal limits commit through the same importer and pool"
    );
}

#[tokio::test]
async fn configured_hyperchain_ledger_enforces_signature_limit_and_can_resume_same_database() {
    use ae_chain::block::Block;
    let parent = Parent::start();
    let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
    let p = lab.prepared(Default::default(), "source");
    let tx = transaction(&p);
    let db = p.configured().chain().database().clone();
    let clock = Clock::new();
    let running = lab.launch(lab.inputs(p), clock.clone());
    let mut h = running.handle();
    ready(&mut h).await;
    h.workers()
        .unwrap()
        .bindings()
        .admission
        .current()
        .unwrap()
        .client
        .push(tx.clone(), ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    let outputs = produce(&h, &clock, 3).await;
    let e = running.shutdown().await.unwrap();
    clean(&e);
    assert!(e.status.failure.is_none());
    let view = db.view().unwrap();
    let mut next = view.status().tip.unwrap();
    let mut blocks = vec![];
    while next != view.identity().genesis {
        let b = view.require_block(next).unwrap();
        next = Hash256(b.header().common().previous_hash);
        blocks.push(b);
    }
    blocks.reverse();
    assert!(blocks.iter().any(
        |b| matches!(b, Block::Micro(m) if m.transactions.iter().any(|t| t.hash() == tx.hash()))
    ));
    drop(outputs);
    drop(e);
    drop(h);
    drop(view);
    drop(db);
    let mut first = 0;
    let mut rejected = false;
    for restricted in [true, false] {
        let mut resources = Resources::default();
        if restricted {
            resources.signatures.max_encoded_bytes = 0;
        }
        let p = lab.prepared(resources, "replay");
        let db = p.configured().chain().database().clone();
        let running = lab.launch(lab.inputs(p), Clock::new());
        let mut h = running.handle();
        ready(&mut h).await;
        let importer = h.importer().unwrap();
        for (index, block) in blocks.iter().enumerate().skip(first) {
            let before = db.view().unwrap().status();
            let result = importer
                .import_synced_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait()
                .await;
            if restricted && matches!(block, Block::Micro(_)) {
                let error = result.unwrap_err();
                assert!(
                    matches!(
                        error.error,
                        ae_sync::chain::Error::Validation {
                            operational: true,
                            ..
                        }
                    ),
                    "{error:?}"
                );
                assert!(format!("{error:?}").contains("Signature"), "{error:?}");
                assert_eq!(db.view().unwrap().status(), before);
                assert!(db.view().unwrap().location(tx.hash()).unwrap().is_none());
                assert!(!importer.recovery_required());
                first = index;
                rejected = true;
                break;
            }
            result.unwrap();
        }
        let e = running.shutdown().await.unwrap();
        clean(&e);
        assert!(e.status.failure.is_none(), "{:?}", e.status);
        if !restricted {
            assert!(db.view().unwrap().location(tx.hash()).unwrap().is_some());
        }
        drop(e);
        drop(h);
        drop(importer);
        drop(db);
    }
    assert!(rejected);
    println!(
        "Configured Hyperchain ledger: live importer rejects a genuine signed microblock operationally at the configured signature limit without changing selected state, then accepts it after reopening the same database with normal resources"
    );
}

#[tokio::test]
async fn configured_hyperchain_partial_startup_failures_join_children_and_retain_database() {
    let parent = Parent::start();
    for case in 0..4 {
        let lab = Hc::new(8, json!({}), &parent);
        let p = lab.prepared(Default::default(), "chain");
        let identity = p.configured().chain().identity().clone();
        let mut cfg = lab.root_config();
        match case {
            0 => cfg.workers.poll = Duration::ZERO,
            1 => cfg.workers.keys = None,
            2 => cfg.connections.health_interval = Duration::ZERO,
            3 => cfg.producer.poll = Duration::ZERO,
            _ => unreachable!(),
        }
        let running = match root::launch_configured(lab.inputs(p), cfg, Clock::new(), || {
            Ok(Default::default())
        }) {
            Ok(r) => r,
            Err(e) => panic!("{:?}", e.error),
        };
        let e = tokio::time::timeout(Duration::from_secs(20), running.wait())
            .await
            .unwrap()
            .unwrap();
        clean(&e);
        assert!(e.producer.as_ref().unwrap().is_ok());
        assert!(
            matches!(
                (case, &e.status.failure),
                (0, Some(root::Error::Workers(_)))
                    | (1, Some(root::Error::Identity))
                    | (2, Some(root::Error::Connections(_)))
                    | (3, Some(root::Error::ProducerSpawn(_)))
            ),
            "{case}: {:?}",
            e.status
        );
        assert_eq!(e.configured.as_ref().unwrap().chain().identity(), &identity);
        drop(e);
        assert_eq!(
            lab.prepared(Default::default(), "chain")
                .configured()
                .chain()
                .identity(),
            &identity
        );
    }
    println!(
        "Configured Hyperchain root: worker, identity, connection and producer startup failures join initialized children, retain the exact configuration binding and release the database for reopen"
    );
}

#[test]
fn configured_hyperchain_without_async_runtime_returns_original_inputs() {
    let parent = Parent::start();
    let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
    let p = lab.prepared(Default::default(), "chain");
    let identity = p.configured().chain().identity().clone();
    let failure =
        match root::launch_configured(lab.inputs(p), lab.root_config(), Clock::new(), || {
            Ok(Default::default())
        }) {
            Ok(_) => panic!("requires runtime"),
            Err(e) => e,
        };
    assert_eq!(failure.error, root::Error::NoRuntime);
    assert_eq!(
        failure.inputs.prepared.configured().chain().identity(),
        &identity
    );
    drop(failure);
    assert_eq!(
        lab.prepared(Default::default(), "chain")
            .configured()
            .chain()
            .identity(),
        &identity
    );
    println!(
        "Configured Hyperchain root: absent async runtime returns the original unopened-services recipe and database ownership"
    );
}

#[test]
fn configured_hyperchain_conductor_keeps_resources_through_session_and_pool_replacement() {
    let parent = Parent::start();
    for restricted in [true, false] {
        let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
        let mut resources = Resources::default();
        if restricted {
            resources.signatures.max_encoded_bytes = 0;
        }
        let p = lab.prepared(resources, "chain");
        let tx = transaction(&p);
        let mut runtime = p.runtime();
        runtime.pool_worker.origins_interval = None;
        let active = p.activate(runtime, Default::default()).unwrap();
        let mut c = active.conductor;
        let db = active.open.chain().database().clone();
        let importer = c.importer();
        let old_pool = c.pool();
        let mut wall = 1000u64;
        let mut clock = Instant::now();
        let tick = |c: &mut conductor::Conductor<Otp26>, wall: &mut u64, clock: &mut Instant| {
            let result = c.step(*wall, *clock);
            if let Some(deadline) = c.status().deadline {
                *wall += deadline.saturating_duration_since(*clock).as_millis() as u64;
                *clock = deadline;
            }
            std::thread::sleep(Duration::from_millis(1));
            result
        };
        let produced = |o: &conductor::Output| matches!(o, conductor::Output::Attempt(a) if matches!(a.outcome, ae_sync::chain::hyperchain::eoe::owner::production::Outcome::Produced { .. }));
        c.start_production().unwrap();
        let end = Instant::now() + Duration::from_secs(30);
        let mut keys = 0;
        while keys < 3 {
            assert!(Instant::now() < end, "{:?}", c.status());
            tick(&mut c, &mut wall, &mut clock).unwrap();
            if let Some(o) = c.take_output() {
                keys += usize::from(produced(&o));
            }
        }
        c.restart_production_session(1).unwrap();
        let end = Instant::now() + Duration::from_secs(20);
        while c.status().session_generation != 2 || c.status().session_replacing {
            assert!(Instant::now() < end, "{:?}", c.status());
            tick(&mut c, &mut wall, &mut clock).unwrap();
            c.take_output();
        }
        assert!(!c.status().producing);
        c.request_pool_restart(1).unwrap();
        let end = Instant::now() + Duration::from_secs(20);
        while c.status().pool.generation != 2 || c.status().pool.restarting {
            assert!(Instant::now() < end, "{:?}", c.status());
            tick(&mut c, &mut wall, &mut clock).unwrap();
            c.take_output();
        }
        assert!(old_pool.info().joined);
        assert!(Arc::ptr_eq(&importer, &c.importer()));
        let before = db.view().unwrap().status();
        c.pool()
            .push(tx.clone(), ae_mempool::pool::Event::Created, false)
            .unwrap()
            .wait_timeout(Duration::from_secs(5))
            .unwrap();
        c.start_production().unwrap();
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(Instant::now() < end, "{:?}", c.status());
            let result = tick(&mut c, &mut wall, &mut clock);
            c.take_output();
            if restricted {
                if let Err(error) = result {
                    assert!(
                        format!("{error:?}").contains("Signature(ResourceLimit)"),
                        "{error:?}"
                    );
                    break;
                }
            } else {
                result.unwrap();
                if db.view().unwrap().location(tx.hash()).unwrap().is_some() {
                    break;
                }
            }
        }
        let report = c.shutdown();
        assert!(report.pool.as_ref().unwrap().joined);
        assert!(
            report.parent.as_ref().is_none_or(Result::is_ok)
                && report.connector_joined
                && report.pins_joined
        );
        assert!(report.owner.errors.is_empty());
        assert_eq!(report.status.session_generation, 2);
        if restricted {
            assert_eq!(db.view().unwrap().status(), before);
            assert!(
                ae_storage::pool::Stored::load(db.view().unwrap())
                    .unwrap()
                    .members(10)
                    .unwrap()
                    .contains(&tx.hash())
            );
        } else {
            assert!(report.status.failure.is_none());
        }
        assert_eq!(
            db.view().unwrap().location(tx.hash()).unwrap().is_some(),
            !restricted
        );
    }
    println!(
        "Configured Hyperchain conductor: logical session and ordinary-pool replacement retain one importer and explicit signature resources; genuine transfer executes with normal limits and fails atomically with exhausted resources"
    );
}

#[tokio::test]
async fn configured_hyperchain_shutdown_joins_pending_parent_entropy_acquisition() {
    let parent = Parent::start();
    parent.deny(6);
    let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
    let p = lab.prepared(Default::default(), "chain");
    let db = p.configured().chain().database().clone();
    let clock = Clock::new();
    let running = lab.launch(lab.inputs(p), clock.clone());
    let mut h = running.handle();
    ready(&mut h).await;
    h.start_production().unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !parent.requests.lock().unwrap().contains(&6) {
            assert!(h.status().failure.is_none(), "{:?}", h.status());
            clock.tick(&h);
            while h.try_output().is_some() {}
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let before = db.view().unwrap().status();
    let e = tokio::time::timeout(Duration::from_secs(10), running.shutdown())
        .await
        .unwrap()
        .unwrap();
    clean(&e);
    assert!(e.status.failure.is_none(), "{:?}", e.status);
    assert_eq!(db.view().unwrap().status(), before);
    drop(e);
    drop(h);
    drop(db);
    assert_eq!(
        lab.prepared(Default::default(), "chain")
            .configured()
            .chain()
            .database()
            .view()
            .unwrap()
            .status(),
        before
    );
    println!(
        "Configured Hyperchain entropy: later-epoch key preparation requests missing parent data through a joined worker; shutdown cancels pending acquisition without changing selected state and the exact database reopens"
    );
}

#[tokio::test]
async fn configured_supervision_retires_parent_acquisition_and_keeps_execution_limits() {
    use producer::supervision::Notice;
    use std::sync::atomic::Ordering;
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
    for restricted in [false, true] {
        let parent = Parent::start();
        parent.deny(6);
        let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
        let mut resources = Resources::default();
        if restricted {
            resources.signatures.max_encoded_bytes = 0;
        }
        let p = lab.prepared(resources, "chain");
        let tx = transaction(&p);
        let db = p.configured().chain().database().clone();
        let clock = Clock::new();
        let clocks = clock.clone();
        let first = Arc::new(AtomicBool::new(false));
        let fail = first.clone();
        let running = root::launch_configured_supervised(
            lab.inputs(p),
            lab.root_config(),
            move |generation| {
                Ok(Failing(
                    clocks.clone(),
                    if generation == 1 {
                        fail.clone()
                    } else {
                        Arc::new(AtomicBool::new(false))
                    },
                ))
            },
            || Ok(Default::default()),
        )
        .unwrap_or_else(|e| panic!("{:?}", e.error));
        let mut h = running.handle();
        ready(&mut h).await;
        let importer = h.importer().unwrap();
        let workers = h.workers().unwrap();
        let pool = workers.bindings().admission.current().unwrap().client;
        h.start_production().unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !parent.requests.lock().unwrap().contains(&6) {
                assert!(h.status().failure.is_none(), "{:?}", h.status());
                clock.tick(&h);
                while h.try_output().is_some() {}
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let before = db.view().unwrap().status();
        first.store(true, Ordering::Release);
        let mut notices = vec![];
        let mut outputs = vec![];
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(h.status().failure.is_none(), "{:?}", h.status());
                while let Some(o) = h.try_output() {
                    outputs.push(o);
                }
                if let Some(n) = h.try_production_notice() {
                    let done = matches!(n, Notice::Restarted { generation: 2 });
                    notices.push(n);
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
        assert!(matches!(
            notices.as_slice(),
            [Notice::Retired(_), Notice::Restarted { generation: 2 }]
        ));
        assert!(
            outputs
                .iter()
                .any(|o| matches!(o, conductor::Output::Stopped(_)))
        );
        assert_eq!(db.view().unwrap().status(), before);
        assert!(Arc::ptr_eq(&importer, &h.importer().unwrap()));
        assert!(pool.same_worker(&workers.bindings().admission.current().unwrap().client));
        assert!(!h.status().producer.unwrap().producing);
        parent.deny(-1);
        pool.push(tx.clone(), ae_mempool::pool::Event::Created, false)
            .unwrap()
            .await
            .unwrap();
        let exit = if restricted {
            h.start_production().unwrap();
            tokio::time::timeout(Duration::from_secs(20), running.wait())
                .await
                .unwrap()
                .unwrap()
        } else {
            drop(produce(&h, &clock, 2).await);
            running.shutdown().await.unwrap()
        };
        clean(&exit);
        assert_eq!(exit.production.as_ref().unwrap().status.attempts, 1);
        assert_eq!(
            db.view().unwrap().location(tx.hash()).unwrap().is_some(),
            !restricted
        );
        if restricted {
            // Closing the failed producer also closes borrowed admission.
            // The root may observe that worker departure first; the joined
            // producer receipt retains the authoritative execution failure.
            let failure = exit
                .producer
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .status
                .failure
                .as_ref()
                .unwrap();
            assert!(exit.status.failure.is_some());
            assert!(
                format!("{failure:?}").contains("Signature(ResourceLimit)"),
                "{failure:?}",
            );
            assert_eq!(db.view().unwrap().status(), before);
            assert!(
                ae_storage::pool::Stored::load(db.view().unwrap())
                    .unwrap()
                    .members(10)
                    .unwrap()
                    .contains(&tx.hash())
            );
        } else {
            assert!(exit.status.failure.is_none());
        }
        let status = db.view().unwrap().status();
        drop((outputs, notices, exit, h, importer, workers, pool, db));
        assert_eq!(
            lab.prepared(Default::default(), "chain")
                .configured()
                .chain()
                .database()
                .view()
                .unwrap()
                .status(),
            status
        );
    }
    println!(
        "Supervised Hyperchain root: pending parent entropy retires before fresh production; one importer, pool, configured execution limits and exact durable reopen survive replacement"
    );
}

#[tokio::test]
async fn configured_supervision_initial_factory_failure_retains_opened_chain() {
    let parent = Parent::start();
    let lab = Hc::new(8, json!({"dev_mode":true}), &parent);
    let p = lab.prepared(Default::default(), "chain");
    let identity = p.configured().chain().identity().clone();
    let running = root::launch_configured_supervised(
        lab.inputs(p),
        lab.root_config(),
        |_| Err::<Clock, _>("injected initial factory error".into()),
        || Ok(Default::default()),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error));
    let exit = running.wait().await.unwrap();
    clean(&exit);
    assert_eq!(
        exit.status.failure,
        Some(root::Error::ProducerSpawn(
            "injected initial factory error".into()
        ))
    );
    assert_eq!(
        exit.configured.as_ref().unwrap().chain().identity(),
        &identity
    );
    assert!(exit.production.is_none());
    drop(exit);
    assert_eq!(
        lab.prepared(Default::default(), "chain")
            .configured()
            .chain()
            .identity(),
        &identity
    );
}
