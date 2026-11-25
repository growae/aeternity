use ae_node::{
    configuration::{
        Configuration,
        resolution::{Context, Directories},
        runtime::{Core, Error},
    },
    runtime::{self, Settings},
};
use ae_storage::pool::worker::{CacheConfigurationError, GossipCache, LifecycleError};
use ae_types::{Hash256, Unsigned};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};

fn context() -> Context {
    Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap()
}
fn core(value: Value) -> Core {
    Configuration::from_value(value)
        .unwrap()
        .resolve(&context())
        .map(|r| Core::resolve(&r, Path::new("/startup")).unwrap())
        .unwrap()
}
#[test]
fn original_core_consumer_defaults_precedence_merges_and_tree_expansion() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_core_runtime_config.json")).unwrap();
    let mut comparisons = 0;
    let mut boundaries = 0;
    let mut schema_boundaries = 0;
    for c in fixture["cases"].as_array().unwrap() {
        let name = c["name"].as_str().unwrap();
        let mut ctx = context();
        for (key, value) in c["application"].as_object().unwrap() {
            ctx.set("aecore".into(), key.clone(), value.clone())
                .unwrap();
        }
        // The reference captures consumer lookup after cache_config, not
        // file validation. The real schema forbids zero mining gas settings.
        if name == "zero-policies" {
            assert!(matches!(
                Configuration::from_value(c["user"].clone()),
                Err(ae_node::configuration::Error::Schema { .. })
            ));
            schema_boundaries += 1;
            continue;
        }
        let user = Configuration::from_value(c["user"].clone()).unwrap();
        let resolved = user.resolve(&ctx).unwrap();
        let got = Core::resolve(&resolved, Path::new("/startup"));
        // These are native lookup values outside the existing typed owners'
        // domain. They are rejected, not coerced or called equivalent startup.
        match name {
            "negative-runtime-values" => {
                assert!(matches!(got, Err(Error::Range("tx_ttl"))));
                boundaries += 1;
                continue;
            }
            "failure-negative-values" => {
                assert!(matches!(got, Err(Error::Range("tx_failures"))));
                boundaries += 1;
                continue;
            }
            _ => (),
        }
        let got = got.unwrap_or_else(|e| panic!("{name}: {e:?}"));
        let want = &c["result"];
        let p = got.pool();
        let q = &want["policy"];
        assert_eq!(p.tx_ttl, q["tx_ttl"].as_u64().unwrap(), "{name}");
        assert_eq!(
            p.invalid_tx_ttl,
            q["invalid_tx_ttl"].as_u64().unwrap(),
            "{name}"
        );
        assert_eq!(
            p.future_nonce_ttl,
            q["future_nonce_tx_ttl"].as_u64().unwrap(),
            "{name}"
        );
        assert_eq!(
            p.allow_reentry,
            q["allow_reentry"].as_bool().unwrap(),
            "{name}"
        );
        for (actual, key) in [
            (&p.nonce_baseline, "nonce_baseline"),
            (&p.nonce_offset, "nonce_offset"),
            (&p.minimum_miner_gas_price, "minimum_miner_gas_price"),
            (&p.maximum_auth_gas, "maximum_auth_fun_gas"),
        ] {
            assert_eq!(
                actual,
                &Unsigned::from_decimal(q[key].as_number().unwrap().as_str()).unwrap(),
                "{name}/{key}"
            );
        }
        let failures = got.failures();
        let mut expected = want["failures"].as_object().unwrap().clone();
        assert_eq!(
            json!(failures.enabled),
            expected.remove("enabled").unwrap(),
            "{name}"
        );
        assert_eq!(
            json!(failures.common),
            expected.remove("common").unwrap(),
            "{name}"
        );
        assert_eq!(
            json!(failures.transactions),
            Value::Object(expected),
            "{name}"
        );
        assert_eq!(
            got.cache_size() as u64,
            want["cache_size"].as_u64().unwrap(),
            "{name}"
        );
        let gc = got.garbage_collection();
        let q = &want["gc"];
        assert_eq!(gc.enabled, q["enabled"].as_bool().unwrap());
        assert_eq!(gc.history, q["history"].as_u64().unwrap());
        assert_eq!(gc.minimum_height, q["minimum_height"].as_u64().unwrap());
        assert_eq!(gc.during_sync, q["during_sync"].as_bool().unwrap());
        for (i, tree) in [
            "accounts",
            "calls",
            "channels",
            "contracts",
            "ns",
            "oracles",
            "ns_cache",
            "oracles_cache",
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                gc.trees.contains(i),
                q["trees"].as_array().unwrap().contains(&json!(tree)),
                "{name}/{tree}"
            );
        }
        comparisons += 1;
    }
    assert_eq!((comparisons, boundaries, schema_boundaries), (42, 2, 1));
    println!(
        "Core configuration: 42 native policy/merge/GC comparisons, two typed-owner boundaries and one schema rejection"
    );
}
#[test]
fn resilience_schema_rules_and_key_paths_use_explicit_startup_context() {
    let mut ctx = context();
    ctx.set(
        "aecore".into(),
        "sync_allowed_height_from_top".into(),
        json!(77),
    )
    .unwrap();
    ctx.set(
        "aecore".into(),
        "resist_forks_from_start".into(),
        json!(true),
    )
    .unwrap();
    ctx.set(
        "aecore".into(),
        "keys_dir".into(),
        json!("$DATA_DIR/peer-keys"),
    )
    .unwrap();
    ctx.set(
        "aecore".into(),
        "peer_password".into(),
        json!("private-test-value"),
    )
    .unwrap();
    let user = Configuration::empty();
    let resolved = user.resolve(&ctx).unwrap();
    let got = Core::resolve(&resolved, Path::new("/original/startup")).unwrap();
    assert_eq!(got.resilience().depth, 20);
    assert!(!got.resilience().from_start);
    assert_eq!(
        got.keys().directory,
        Path::new("/original/startup/data/peer-keys")
    );
    assert!(!format!("{got:?}").contains("private-test-value"));
    assert!(matches!(
        Core::resolve(&resolved, Path::new("relative")),
        Err(Error::WorkingDirectory)
    ));
    for depth in [-10, 0, 37] {
        let c = core(
            json!({"sync":{"sync_allowed_height_from_top":depth,"resist_forks_from_start":true},"keys":{"dir":"/fixed/keys"}}),
        );
        assert_eq!(c.resilience().depth, depth.max(0) as u64);
        assert!(c.resilience().from_start);
        assert_eq!(c.keys().directory, Path::new("/fixed/keys"));
    }
}
#[test]
fn applying_core_is_atomic_and_keeps_resource_and_consensus_settings() {
    let invalid = Configuration::from_value(json!({"mempool":{"cache_size":0}})).unwrap();
    assert!(matches!(
        Core::resolve(&invalid.resolve(&context()).unwrap(), Path::new("/startup")),
        Err(Error::Cache(ae_mempool::gossip::Error::InvalidCapacity))
    ));
    let core = core(
        json!({"mempool":{"tx_ttl":73,"cache_size":7},"chain":{"garbage_collection":{"history":55}}}),
    );
    let mut cfg = ae_node::pow::Config {
        cache_limit: 3,
        ..Default::default()
    };
    cfg.workers.pool.limits.entries = 123;
    cfg.workers.pool.events = 19;
    cfg.workers.pool.consensus = ae_protocol::ConsensusType::Pos;
    cfg.workers.keys = Some(ae_sync::keys::Config::new("original-keys".into(), vec![]));
    cfg.workers.keys.as_mut().unwrap().requests = 17;
    cfg.workers.gc.as_mut().unwrap().limits.rows = 37;
    let mut s = Settings::Cuckoo(Box::new(runtime::Cuckoo {
        config: cfg,
        clocks: Arc::new(ae_sync::chain::pow::group::SystemClocks::default()),
    }));
    assert_eq!(
        s.apply_core(&core),
        Err(Error::Cache(ae_mempool::gossip::Error::ResourceLimit))
    );
    let Settings::Cuckoo(v) = &mut s else {
        unreachable!()
    };
    assert_eq!(v.config.workers.pool.policy.tx_ttl, 6720);
    assert_eq!(
        v.config.workers.keys.as_ref().unwrap().directory,
        Path::new("original-keys")
    );
    v.config.cache_limit = 10;
    s.apply_core(&core).unwrap();
    let Settings::Cuckoo(v) = s else {
        unreachable!()
    };
    assert_eq!(v.config.cache_entries, 7);
    assert_eq!(v.config.workers.pool.policy.tx_ttl, 73);
    assert_eq!(v.config.workers.pool.limits.entries, 123);
    assert_eq!(v.config.workers.pool.events, 19);
    assert_eq!(
        v.config.workers.pool.consensus,
        ae_protocol::ConsensusType::Pos
    );
    assert_eq!(v.config.workers.keys.as_ref().unwrap().requests, 17);
    assert_eq!(v.config.workers.gc.unwrap().limits.rows, 37);
    assert_eq!(v.config.workers.resilience.unwrap().initial.depth, 20);
    assert_eq!(v.config.connections.sync.resilience.depth, 20);
}
#[test]
fn idle_cache_configuration_keeps_binding_hard_limit_and_incarnation_rules() {
    let (cache, mut control) = GossipCache::managed(3, 5).unwrap();
    let retained = cache.clone();
    control.configure_idle(2).unwrap();
    assert!(control.uses_cache(&retained));
    assert_eq!(
        control.configure_idle(6),
        Err(CacheConfigurationError::Capacity(
            ae_mempool::gossip::Error::ResourceLimit
        ))
    );
    let owner = control.start().unwrap();
    let a = Hash256([1; 32]);
    let b = Hash256([2; 32]);
    let c = Hash256([3; 32]);
    assert!(!cache.check(a));
    assert!(retained.check(a));
    assert_eq!(
        control.configure_idle(4),
        Err(CacheConfigurationError::Lifecycle(
            LifecycleError::AlreadyRunning
        ))
    );
    assert!(cache.check(a));
    assert!(!cache.check(b));
    assert!(!cache.check(c));
    assert!(!cache.check(a));
    drop(owner);
    control.configure_idle(1).unwrap();
    let next = control.start().unwrap();
    assert!(!retained.check(a));
    assert!(!retained.check(b));
    assert!(!retained.check(a));
    drop(next);
    control.shutdown();
    assert_eq!(
        control.configure_idle(1),
        Err(CacheConfigurationError::Lifecycle(LifecycleError::Closed))
    );
}
