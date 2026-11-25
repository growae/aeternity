use ae_node::configuration::{
    Configuration,
    discovery::{Discovery, Error},
    resolution::{Context, Directories},
};
use serde_json::{Value, json};

fn context() -> Context {
    Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap()
}
fn resolve(user: Value, application: Value) -> Result<Discovery, Error> {
    let cfg = Configuration::from_value(user).unwrap();
    let mut ctx = context();
    for (key, value) in application.as_object().unwrap() {
        ctx.set("aecore".into(), key.clone(), value.clone())
            .unwrap();
    }
    Discovery::resolve(&cfg.resolve(&ctx).unwrap())
}
#[test]
fn original_effective_constructor_geometry_and_gossip_settings_match_schema_classifications() {
    let native: Value = serde_json::from_str(include_str!(
        "fixtures/erlang_discovery_runtime_config.json"
    ))
    .unwrap();
    let schema: Value = serde_json::from_str(include_str!(
        "fixtures/erlang_discovery_runtime_schema.json"
    ))
    .unwrap();
    let mut matched = 0;
    let mut rejected = 0;
    for (case, validation) in native["cases"]
        .as_array()
        .unwrap()
        .iter()
        .zip(schema["cases"].as_array().unwrap())
    {
        assert_eq!(case["name"], validation["name"]);
        let input = Configuration::from_value(case["user"].clone());
        if validation["schema_result"]["status"] == "error" {
            assert!(matches!(
                input,
                Err(ae_node::configuration::Error::Schema { .. })
            ));
            rejected += 1;
            continue;
        }
        assert!(input.is_ok());
        let result = resolve(case["user"].clone(), case["application"].clone()).unwrap();
        let pool = result.pool();
        assert_eq!(
            json!({
                "select_verif_prob":pool.verified_probability,"max_update_lapse":pool.maximum_update_lapse,
                "standby_times":pool.standby_times,"max_rejections":pool.maximum_rejections,
                "verif_group_shard":pool.verified_group_shard,"unver_group_shard":pool.unverified_group_shard,
                "unver_source_shard":pool.unverified_source_shard/pool.unverified_group_shard
            }),
            case["result"]["effective_pool"],
            "{}",
            case["name"]
        );
        assert_eq!(
            json!({"max_refs":1,"skew":pool.eviction_skew,"bucket_count":pool.verified_buckets,"bucket_size":pool.verified_size}),
            case["result"]["verified_pool"]
        );
        assert_eq!(
            json!({"max_refs":pool.maximum_references,"skew":pool.eviction_skew,"bucket_count":pool.unverified_buckets,"bucket_size":pool.unverified_size}),
            case["result"]["unverified_pool"]
        );
        assert_eq!(json!(result.maximum_gossip()), case["result"]["max_gossip"]);
        assert_eq!(json!(result.gossip_depth()), case["result"]["gossip_depth"]);
        matched += 1;
    }
    assert_eq!((matched, rejected), (37, 3));
}

#[test]
fn discovery_application_preserves_resources_and_fails_atomically_on_incompatible_capacity() {
    let discovery=resolve(json!({"sync":{"max_gossip":2,"gossip_allowed_height_from_top":0,"peer_pool":{"max_rejections":3}}}),json!({"peer_pool":{"verif_bsize":4}})).unwrap();
    let mut config = ae_sync::runtime::connection::Config::default();
    config.network.registry.policy.pool.verified_buckets = 3;
    config.network.registry.policy.pool.maximum_host_bytes = 73;
    config.gossip.policy.enabled = false;
    config.gossip.policy.snapshot_limit = 71;
    config.propagation.peer_limit = 67;
    config.importer.jobs = 3;
    discovery.apply(&mut config).unwrap();
    assert_eq!(config.network.registry.policy.pool.verified_size, 4);
    assert_eq!(config.network.registry.policy.pool.verified_buckets, 3);
    assert_eq!(config.network.registry.policy.pool.maximum_host_bytes, 73);
    assert_eq!(config.network.registry.policy.pool.maximum_rejections, 3);
    assert!(!config.gossip.policy.enabled);
    assert_eq!(config.gossip.policy.snapshot_limit, 71);
    assert_eq!(config.gossip.policy.max_peers, 2);
    assert_eq!(config.propagation.received_peers, 2);
    assert_eq!(config.propagation.peer_limit, 67);
    assert_eq!(config.importer.ledger.policy.gossip_depth, 0);
    assert_eq!(config.importer.jobs, 3);
    let before = config.network.registry.policy.pool.clone();
    config.propagation.peer_limit = 1;
    assert_eq!(
        discovery.apply(&mut config),
        Err(Error::Range("max_gossip capacity"))
    );
    assert_eq!(config.network.registry.policy.pool, before);
    config.propagation.peer_limit = 67;
    config.network.registry.policy.pool.maximum_host_bytes = 0;
    let before = config.network.registry.policy.pool.clone();
    assert_eq!(discovery.apply(&mut config), Err(Error::Range("peer_pool")));
    assert_eq!(config.network.registry.policy.pool, before);
}

#[test]
fn typed_boundaries_and_native_raw_constructor_values_remain_explicit() {
    for name in ["seed", "secret", "disable_strong_random"] {
        assert_eq!(
            resolve(json!({}), json!({"peer_pool":{name:true}})).unwrap_err(),
            Error::Unsupported(name)
        );
    }
    for application in [
        json!({"peer_pool":{"verif_bcount":0}}),
        json!({"peer_pool":{"unver_source_shard":3}}),
        json!({"peer_pool":{"select_verif_prob":-0.1}}),
        json!({"peer_pool":{"standby_times":[]}}),
    ] {
        assert!(matches!(
            resolve(json!({}), application),
            Err(Error::Range(_))
        ));
    }
    assert!(matches!(
        resolve(json!({"sync":{"max_gossip":65_537}}), json!({})),
        Err(Error::Range("max_gossip"))
    ));
    let mut ctx = context();
    ctx.set("aecore".into(), "other".into(), json!(3)).unwrap();
    ctx.set(
        "aecore".into(),
        "peer_pool".into(),
        json!({"max_rejections":"$env(other)"}),
    )
    .unwrap();
    let config =
        Configuration::from_value(json!({"sync":{"peer_pool":{"max_rejections":7}}})).unwrap();
    assert!(matches!(
        Discovery::resolve(&config.resolve(&ctx).unwrap()),
        Err(Error::Range("max_rejections"))
    ));
    ctx.remove("aecore", "peer_pool");
    ctx.set(
        "aecore".into(),
        "peer_unblock_interval".into(),
        json!("$env(other)"),
    )
    .unwrap();
    assert!(matches!(
        ae_node::configuration::network::Network::resolve(&config.resolve(&ctx).unwrap()),
        Err(ae_node::configuration::network::Error::Range(
            "peer_unblock_interval"
        ))
    ));
}

#[test]
fn resolved_pool_drives_selection_standby_removal_and_stale_bucket_eviction() {
    use ae_p2p::discovery::{Effect, Peer, Pool, Selection, Target};
    use ae_types::PeerPublicKey;
    let settings = resolve(
        json!({"sync":{"peer_pool":{"max_rejections":7,"standby_times":[10,20],"max_update_lapse":60000}}}),
        json!({"peer_pool":{"max_rejections":2,"select_verif_prob":1.5}}),
    ).unwrap();
    let peer = |n: u8, trusted| Peer {
        id: PeerPublicKey([n; 32]),
        address: std::net::Ipv4Addr::new(127, 0, 0, n),
        source: std::net::Ipv4Addr::LOCALHOST,
        host: format!("127.0.0.{n}").into_bytes(),
        port: 3015,
        trusted,
    };
    let a = peer(1, true);
    let b = peer(2, false);
    let mut pool = Pool::with_seed(settings.pool().clone(), b"test secret", [3; 32]).unwrap();
    pool.update(0, a.clone()).unwrap();
    pool.update(0, b.clone()).unwrap();
    assert_eq!(
        pool.random_select(0, Target::Both, |_| true).unwrap(),
        Selection::Selected(a.clone())
    );
    // The verified peer remains selected, so native preference falls back.
    assert_eq!(
        pool.random_select(0, Target::Both, |_| true).unwrap(),
        Selection::Selected(b.clone())
    );
    pool.reject(0, b.id).unwrap();
    assert_eq!(
        pool.random_select(9, Target::Both, |_| true).unwrap(),
        Selection::Wait(1)
    );
    assert_eq!(
        pool.random_select(10, Target::Both, |_| true).unwrap(),
        Selection::Selected(b.clone())
    );
    pool.reject(10, b.id).unwrap();
    assert_eq!(
        pool.random_select(29, Target::Both, |_| true).unwrap(),
        Selection::Wait(1)
    );
    assert_eq!(
        pool.random_select(30, Target::Both, |_| true).unwrap(),
        Selection::Selected(b.clone())
    );
    let removal = pool.reject(30, b.id).unwrap();
    assert!(removal.effects.contains(&Effect::Delete(b.clone())));
    assert!(pool.find(b.id).is_none());
    pool.release(30, a.id).unwrap();
    assert_eq!(
        pool.random_select(30, Target::Both, |_| true).unwrap(),
        Selection::Selected(a)
    );
    let settings = resolve(
        json!({"sync":{"peer_pool":{"max_update_lapse":60000}}}),
        json!({"peer_pool":{"unver_bcount":1,"unver_bsize":2,"unver_max_refs":1}}),
    )
    .unwrap();
    let mut pool = Pool::with_seed(settings.pool().clone(), b"test secret", [4; 32]).unwrap();
    pool.update(0, b.clone()).unwrap();
    let c = peer(3, false);
    pool.update(0, c.clone()).unwrap();
    let d = peer(4, false);
    let replaced = pool.update(60001, d.clone()).unwrap();
    assert!(replaced.effects.contains(&Effect::Delete(b)));
    assert!(replaced.effects.contains(&Effect::Delete(c)));
    assert_eq!(pool.all(Target::Both).collect::<Vec<_>>(), vec![&d]);
}
