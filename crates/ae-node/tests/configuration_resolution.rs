use ae_node::configuration::{
    Configuration,
    resolution::{Context, Definition, Directories, Error, Strategy, Variable},
};
use ae_sync::keys;
use serde_json::{Value, json};
use std::{fs, path::PathBuf};

fn context() -> Context {
    Context::new(Directories {
        home: "/release".into(),
        data: "/data".into(),
        log: "/log".into(),
    })
    .unwrap()
}

#[test]
fn captured_resolution_retains_its_context_and_literal_expansion_across_replacement() {
    use std::sync::Arc;
    let configuration =
        Configuration::from_value(json!({"sync": {"whitelist_file": "$ROOT"}})).unwrap();
    let mut original = context();
    original
        .set_global_variables(vec![
            Definition::new("ROOT".into(), Variable::Literal(json!("$HOME/original"))).unwrap(),
        ])
        .unwrap();
    original
        .set(
            "aehttp".into(),
            "external".into(),
            json!([["gas_limit", 123]]),
        )
        .unwrap();
    let original = Arc::new(original);
    let weak = Arc::downgrade(&original);
    let snapshot = configuration.capture(original.clone()).unwrap();
    drop(original);
    let mut replacement = context();
    replacement
        .set_global_variables(vec![
            Definition::new("ROOT".into(), Variable::Literal(json!("/replacement"))).unwrap(),
        ])
        .unwrap();
    replacement
        .set(
            "aehttp".into(),
            "external".into(),
            json!([["gas_limit", 456]]),
        )
        .unwrap();
    let newer = configuration.capture(Arc::new(replacement)).unwrap();
    drop(configuration);
    for _ in 0..2 {
        let resolved = snapshot.resolved();
        assert_eq!(
            resolved.get(&["sync", "whitelist_file"]).unwrap(),
            Some(&json!("$HOME/original"))
        );
        assert_eq!(
            ae_node::configuration::http::external_dry_run_gas_limit(&resolved).unwrap(),
            json!(123)
        );
    }
    assert_eq!(
        newer.resolved().get(&["sync", "whitelist_file"]).unwrap(),
        Some(&json!("/replacement"))
    );
    assert_eq!(
        ae_node::configuration::http::external_dry_run_gas_limit(&newer.resolved()).unwrap(),
        json!(456)
    );
    assert!(weak.upgrade().is_some());
    let retained = snapshot.clone();
    drop(snapshot);
    assert!(weak.upgrade().is_some());
    drop(retained);
    assert!(weak.upgrade().is_none());
}
fn definitions(value: &Value) -> Vec<Definition> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            Definition::new(
                v["name"].as_str().unwrap().into(),
                if v["kind"] == "literal" {
                    Variable::Literal(v["value"].clone())
                } else {
                    Variable::Expand(v["value"].clone())
                },
            )
            .unwrap()
        })
        .collect()
}
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "ae-resolution-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
#[test]
fn original_expansion_custom_scopes_and_key_worker_inputs() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_node_resolution.json")).unwrap();
    let mut count = 0;
    for c in fixture["cases"].as_array().unwrap() {
        let mut context = context();
        for key in fixture["known"].as_array().unwrap() {
            context.intern(key.as_str().unwrap().into()).unwrap();
        }
        for (app, values) in c["applications"].as_object().unwrap() {
            for (key, value) in values.as_object().unwrap() {
                context
                    .set(app.clone(), key.clone(), value.clone())
                    .unwrap();
            }
        }
        context
            .set_global_variables(definitions(&c["global_variables"]))
            .unwrap();
        context
            .set_private_variables("aeutils".into(), definitions(&c["private_variables"]))
            .unwrap();
        let user = Configuration::from_value(c["user"].clone()).unwrap();
        let resolved = user.resolve(&context);
        let got = match c["kind"].as_str().unwrap() {
            "expand" => context.expand("aeutils", &c["value"]),
            "mode" => resolved
                .and_then(|v| v.mode())
                .map(|v| json!(v.development)),
            "keys" => {
                let mut cfg = resolved.unwrap().keys().unwrap();
                assert_eq!(
                    cfg.directory,
                    PathBuf::from(c["result"]["directory"].as_str().unwrap()),
                    "{}",
                    c["name"]
                );
                // The password is intentionally not exposed by Config. Verify
                // it by reopening actual encrypted peer keys with the native result.
                let d = Directory::new();
                cfg.directory = d.0.join("keys");
                let worker = keys::Worker::spawn(cfg.clone()).unwrap();
                let id = worker
                    .client()
                    .peer_identity()
                    .unwrap()
                    .blocking_wait()
                    .unwrap();
                let peer = id.public_key();
                worker.shutdown().unwrap();
                let reopened = keys::Worker::spawn(keys::Config::new(
                    cfg.directory,
                    c["result"]["password"]
                        .as_str()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                ))
                .unwrap();
                assert_eq!(reopened.info().keys.peer, peer);
                reopened.shutdown().unwrap();
                count += 1;
                continue;
            }
            _ => panic!("unknown fixture"),
        };
        assert_eq!(
            got.is_ok(),
            c["ok"].as_bool().unwrap(),
            "{}: {got:?}",
            c["name"]
        );
        if let Ok(got) = got {
            assert_eq!(got, c["result"], "{}", c["name"]);
        }
        count += 1;
    }
    assert_eq!(count, 113);
    println!(
        "Node resolution: {count} native expansion/service comparisons; fifteen encrypted key-worker reopens"
    );
}
#[test]
fn original_lookup_order_and_dynamic_expansion_without_mutating_raw_config() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_node_resolution_lookup.json")).unwrap();
    let fallback = json!("fallback");
    for c in fixture.as_array().unwrap() {
        let raw = if c["user"] == true {
            json!({"keys":{"dir":c["input"]}})
        } else {
            json!({})
        };
        let user = Configuration::from_value(raw.clone()).unwrap();
        let mut context = context();
        context
            .set("aeutils".into(), "other".into(), json!("$HOME/other"))
            .unwrap();
        if c["application"] == true {
            context
                .set(
                    "aeutils".into(),
                    "config_key".into(),
                    json!("application-value"),
                )
                .unwrap();
        }
        for (phase, suffix) in [("before", "one"), ("after", "two")] {
            for (key, prefix) in [
                ("home", "release"),
                ("log_dir", "log"),
                ("data_dir", "data"),
            ] {
                context
                    .set(
                        "setup".into(),
                        key.into(),
                        json!(format!("/{prefix}-{suffix}")),
                    )
                    .unwrap();
            }
            let r = user.resolve(&context).unwrap();
            let expected = &c[phase];
            let path = &["keys", "dir"];
            assert_eq!(
                r.get(path).unwrap(),
                expected["user_map"].get("value"),
                "{phase}: {}",
                c["input"]
            );
            assert_eq!(
                r.user_or_application(path, "aeutils", "config_key", &fallback)
                    .unwrap(),
                expected["user_first"]["value"]
            );
            assert_eq!(
                r.find(
                    path,
                    &[
                        Strategy::Application {
                            app: "aeutils",
                            key: "config_key"
                        },
                        Strategy::User,
                        Strategy::Value(&fallback)
                    ]
                )
                .unwrap()
                .unwrap(),
                expected["application_first"]["value"]
            );
            assert_eq!(
                r.config_value(path, "aeutils", "config_key", &fallback)
                    .unwrap(),
                expected["config_value"]["value"]
            );
            assert_eq!(user.user_map(), &expected["stored_map"]);
            assert_eq!(user.user_map(), &raw);
        }
    }
    println!("Node resolution: forty native lookup scenarios before and after application changes");
}

#[test]
fn native_short_float_and_byte_regex_edges() {
    let mut f: Value =
        serde_json::from_str(include_str!("fixtures/erlang_node_resolution_edges.json")).unwrap();
    let exhaustive: Value =
        serde_json::from_str(include_str!("fixtures/erlang_node_resolution_bytes.json")).unwrap();
    f["cases"]
        .as_array_mut()
        .unwrap()
        .extend(exhaustive["cases"].as_array().unwrap().iter().cloned());
    assert_eq!(f["cases"].as_array().unwrap().len(), 3690);
    let mut failures = Vec::new();
    for c in f["cases"].as_array().unwrap() {
        let mut ctx = context();
        ctx.intern("other".into()).unwrap();
        for (app, values) in c["applications"].as_object().unwrap() {
            for (key, value) in values.as_object().unwrap() {
                ctx.set(app.clone(), key.clone(), value.clone()).unwrap();
            }
        }
        let defs = c["global_variables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| {
                Definition::new(
                    d["name"].as_str().unwrap().into(),
                    Variable::Literal(d["value"].clone()),
                )
            })
            .collect::<Result<Vec<_>, _>>();
        let got = defs
            .and_then(|defs| ctx.set_global_variables(defs))
            .and_then(|()| ctx.expand("aeutils", &c["value"]));
        if got.is_ok() != c["ok"].as_bool().unwrap()
            || got.as_ref().is_ok_and(|v| *v != c["result"])
        {
            failures.push(format!("{}: {got:?} expected {}", c["name"], c["result"]));
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches: {:?}",
        failures.len(),
        &failures[..failures.len().min(30)]
    );
    println!("Node resolution: 1,402 native float interpolations and 2,288 byte-regex cases");
}
#[test]
fn bounded_resolution_cycles_invalid_paths_and_transactional_context_updates() {
    use ae_node::configuration::MAX_BYTES;
    let mut ctx = context();
    ctx.set(
        "aeutils".into(),
        "secret".into(),
        json!("synthetic-password-never-in-diagnostics"),
    )
    .unwrap();
    assert!(!format!("{ctx:?}").contains("synthetic-password"));
    ctx.set("aeutils".into(), "other".into(), json!("$env(other)"))
        .unwrap();
    assert_eq!(ctx.application("aeutils", "other"), Err(Error::Cycle));
    ctx.remove("aeutils", "other");
    assert_eq!(
        ctx.expand("aeutils", &json!("$env(other)")),
        Ok(json!("$env(other)"))
    );
    assert_eq!(
        ctx.expand("aeutils", &json!("$env(uninterned-name)")),
        Ok(json!("uninterned-name)"))
    );
    assert!(matches!(
        ctx.set(
            "aeutils".into(),
            "secret".into(),
            json!("x".repeat(MAX_BYTES))
        ),
        Err(Error::Limit)
    ));
    assert_eq!(
        ctx.application("aeutils", "secret").unwrap(),
        Some(json!("synthetic-password-never-in-diagnostics"))
    );
    assert!(matches!(
        ctx.set_global_variables(
            (0..1025)
                .map(|_| Definition::new("X".into(), Variable::Literal(json!("v"))).unwrap())
                .collect()
        ),
        Err(Error::Limit)
    ));
    assert_eq!(ctx.expand("aeutils", &json!("$X")), Ok(json!("$X")));
    ctx.set_global_variables(vec![
        Definition::new("X".into(), Variable::Literal(json!("x".repeat(100_000)))).unwrap(),
    ])
    .unwrap();
    assert_eq!(
        ctx.expand("aeutils", &json!("$X".repeat(200))),
        Err(Error::Limit)
    );
    ctx.set_library("aeutils".into(), "/library".into(), "/library/priv".into())
        .unwrap();
    assert_eq!(
        ctx.expand("aeutils", &json!("$LIB_DIR/$PRIV_DIR")),
        Ok(json!("/library//library/priv"))
    );
    assert_eq!(
        ctx.expand("unavailable", &json!("$LIB_DIR")),
        Err(Error::Directory)
    );
    // Non-UTF8 output remains representable in native byte-list values.
    ctx.set_global_variables(vec![
        Definition::new("X".into(), Variable::Literal(json!([255]))).unwrap(),
    ])
    .unwrap();
    assert_eq!(ctx.expand("aeutils", &json!([36, 88])), Ok(json!([255])));
    assert_eq!(ctx.expand("aeutils", &json!("$X")), Err(Error::Encoding));
    let user = Configuration::from_value(json!({"keys":{"dir":"\u{0000}"}})).unwrap();
    assert!(matches!(
        user.resolve(&ctx).unwrap().keys(),
        Err(Error::Directory)
    ));
    let mut full = Context::new(Directories {
        home: "x".repeat(MAX_BYTES - 200),
        data: String::new(),
        log: String::new(),
    })
    .unwrap();
    assert_eq!(full.intern("y".repeat(256)), Err(Error::Limit));
    assert_eq!(
        full.expand("aeutils", &json!("$env(other)")),
        Ok(json!("other)"))
    );
}
#[test]
fn network_and_mode_precedence_preserve_explicit_false() {
    let mut ctx = context();
    let empty = Configuration::empty();
    assert_eq!(
        empty.resolve(&ctx).unwrap().network_id(None).unwrap(),
        b"ae_mainnet"
    );
    ctx.set("aecore".into(), "network_id".into(), json!("ae_dev"))
        .unwrap();
    let r = empty.resolve(&ctx).unwrap();
    assert!(!r.mode().unwrap().development);
    assert_eq!(r.network_id(None).unwrap(), b"ae_dev");
    assert_eq!(r.network_id(Some(b"cli")).unwrap(), b"cli");
    for explicit in [None, Some(false), Some(true)] {
        let mut value = json!({"fork_management":{"network_id":"ae_dev"},"system":{"offline_mode":true,"maintenance_mode":true}});
        if let Some(v) = explicit {
            value["system"]["dev_mode"] = json!(v);
        }
        let cfg = Configuration::from_value(value).unwrap();
        let r = cfg.resolve(&ctx).unwrap();
        assert_eq!(r.mode().unwrap().development, explicit.unwrap_or(true));
        assert!(r.mode().unwrap().maintenance && r.mode().unwrap().offline);
        assert_eq!(r.network_id(None).unwrap(), b"ae_dev");
    }
    let user = Configuration::from_value(json!({"system":{"dev_mode":true}})).unwrap();
    ctx.remove("aecore", "network_id");
    assert_eq!(
        user.resolve(&ctx).unwrap().network_id(None).unwrap(),
        b"ae_dev"
    );
    ctx.set("aecore".into(), "network_id".into(), json!(7))
        .unwrap();
    assert_eq!(
        user.resolve(&ctx).unwrap().network_id(None),
        Err(Error::Field("network_id"))
    );
    assert_eq!(
        user.resolve(&ctx)
            .unwrap()
            .network_id(Some(&vec![0; 65537])),
        Err(Error::Limit)
    );
}
#[test]
fn resolved_peers_feed_the_durable_registry_without_network_io() {
    use ae_chain::repository::Identity;
    use ae_protocol::NetworkProfile;
    use ae_storage::Database;
    use ae_types::{Hash256, PeerPublicKey};
    use std::sync::Arc;
    let address = |byte| {
        String::from_utf8(
            ae_p2p::address::encode(
                PeerPublicKey([byte; 32]),
                b"localhost",
                &3015.into(),
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap()
    };
    let trusted = address(1);
    let blocked = address(2);
    let mut ctx = context();
    ctx.set("aeutils".into(), "peer".into(), json!(trusted))
        .unwrap();
    ctx.set("aecore".into(), "peers".into(), json!([address(3)]))
        .unwrap();
    ctx.set(
        "aecore".into(),
        "include_default_peers".into(),
        json!(false),
    )
    .unwrap();
    ctx.set("aecore".into(), "blocked_peers".into(), json!([blocked]))
        .unwrap();
    let empty = Configuration::empty();
    let defaults = empty.resolve(&ctx).unwrap().peers().unwrap();
    assert!(defaults.peers.is_empty());
    assert!(defaults.include_default_peers);
    let config =
        Configuration::from_value(json!({"peers":[trusted,blocked],"include_default_peers":false}))
            .unwrap();
    let peers = config.resolve(&ctx).unwrap().peers().unwrap();
    assert_eq!(peers.peers[0], trusted.as_bytes());
    assert_eq!(peers.blocked_peers[0], blocked.as_bytes());
    let temp = Directory::new();
    let profile = NetworkProfile::custom(
        b"resolution-test".to_vec(),
        NetworkProfile::mainnet().activations().to_vec(),
    )
    .unwrap();
    let identity = Identity::new(
        &profile,
        ae_state::genesis::header(ae_state::genesis::Network::Mainnet)
            .unwrap()
            .hash()
            .unwrap(),
        Hash256::ZERO,
    );
    let db = Arc::new(Database::open(temp.0.join("db"), identity).unwrap());
    let mut owner =
        ae_sync::registry::Service::start(db, PeerPublicKey([9; 32]), Default::default()).unwrap();
    let status = owner
        .configure_startup(0, peers, Default::default())
        .unwrap();
    assert!(status.ready);
    assert_eq!(status.queued, 1);
    assert_eq!(status.protected_trusted, 1);
    assert_eq!(owner.policy().blocked().count(), 1);
    owner.advance_startup(1, 8).unwrap();
    let mut resolves = 0;
    while let Some(delivery) = owner.next_effect().unwrap().cloned() {
        if let ae_p2p::registry::Effect::Resolve { host, .. } = delivery.effect {
            assert_eq!(host, b"localhost");
            resolves += 1;
        }
        assert!(owner.acknowledge(delivery.id));
    }
    assert_eq!(resolves, 1);
}
#[test]
fn resolved_mining_and_hyperchain_use_existing_configuration_owners() {
    use ae_sync::chain::{hyperchain, pow};
    use ae_types::AccountPublicKey;
    let mut ctx = context();
    ctx.set(
        "aeutils".into(),
        "executable".into(),
        json!("fixture-miner"),
    )
    .unwrap();
    let config=Configuration::from_value(json!({"mining":{"cuckoo":{"miners":[{"executable":"$env(executable)","extra_args":"$HOME","addressed_instances":[2,4],"repeats":3}]}}})).unwrap();
    let resolved = config.resolve(&ctx).unwrap();
    let mining = resolved
        .mining(&pow::configuration::System::default())
        .unwrap();
    assert_eq!(mining.miners[0].executable, "fixture-miner");
    assert_eq!(mining.miners[0].extra_args, "/release");
    let prepared = mining
        .prepare(
            &pow::configuration::Directories {
                cuckoo: Some("/miners".into()),
                prebuilt: None,
            },
            Default::default(),
            pow::producer::mining::Config::new(AccountPublicKey([1; 32])),
        )
        .unwrap();
    assert_eq!(prepared.instances.len(), 2);
    assert_eq!(prepared.instances[0].device, Some(4.into()));
    assert_eq!(prepared.config.instances[0].repeats, 3);
    let fixture: Value = serde_json::from_str(include_str!(
        "../../ae-sync/tests/fixtures/hc_configuration.json"
    ))
    .unwrap();
    let row = fixture["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "ae")
        .unwrap();
    let mut child = row["config"].clone();
    child["parent_chain"]["consensus"] = json!({"network_id":"$env(parent_network)"});
    ctx.set(
        "aeutils".into(),
        "parent_network".into(),
        json!("parent-fixture"),
    )
    .unwrap();
    let config = Configuration::from_value(
        json!({"chain":{"consensus":{"0":{"type":"hyperchain","config":child}}}}),
    )
    .unwrap();
    let resolved = config.resolve(&ctx).unwrap();
    let hc = resolved.hyperchain("0").unwrap();
    assert_eq!(hc.network, b"parent-fixture");
    let expected = hyperchain::configuration::Configuration::parse(
        &serde_json::to_vec(&row["config"]).unwrap(),
    )
    .unwrap();
    let keys: Vec<_> = hc.public_keys().collect();
    assert_eq!(keys, expected.public_keys().collect::<Vec<_>>());
    for key in keys {
        assert_eq!(
            hc.keys().sign(key, b"resolution").unwrap(),
            expected.keys().sign(key, b"resolution").unwrap()
        );
    }
    assert!(matches!(
        resolved.hyperchain("1"),
        Err(Error::Field("consensus"))
    ));
}
