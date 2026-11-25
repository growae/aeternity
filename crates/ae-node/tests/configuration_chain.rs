use ae_codec::headers::Header;
use ae_node::configuration::{
    Configuration,
    chain::Settings,
    consensus::{self, Kind, Schedule},
    protocols::{self, Protocols},
    resolution::{Context, Definition, Directories, Variable},
};
use ae_protocol::ConsensusType;
use ae_types::Hash256;
use num_bigint::BigInt;
use serde_json::{Value, json};

fn context() -> Context {
    Context::new(Directories {
        home: "/release".into(),
        data: "/data".into(),
        log: "/log".into(),
    })
    .unwrap()
}
fn int(value: &Value) -> BigInt {
    value.to_string().parse().unwrap()
}
fn kind(value: &Value) -> Kind {
    match value.as_str().unwrap() {
        "pow_cuckoo" => Kind::PowCuckoo,
        "on_demand" => Kind::OnDemand,
        "hyperchain" => Kind::Hyperchain,
        _ => panic!("fixture consensus"),
    }
}
#[test]
fn original_consensus_selection_and_startup_checks() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_node_consensus.json")).unwrap();
    let mut checked = 0;
    for c in fixture["cases"].as_array().unwrap() {
        let name = &c["name"];
        let schedule = Schedule::select(
            c["network"].as_str().unwrap().as_bytes(),
            c.get("application").or(c.get("user")),
        );
        assert_eq!(
            schedule.is_ok(),
            c["result"]["ok"] == true,
            "{name}: {schedule:?}"
        );
        let Ok(schedule) = schedule else {
            assert_eq!(c["check"]["ok"], false);
            continue;
        };
        let actual:Vec<_>=schedule.entries().iter().map(|(h,s)|json!({"height":serde_json::from_str::<Value>(&h.to_string()).unwrap(),"module":s.kind().module(),"config":s.config()})).collect();
        assert_eq!(json!(actual), c["result"]["value"]["schedule"], "{name}");
        assert_eq!(
            if schedule.family() == ConsensusType::Pow {
                "pow"
            } else {
                "pos"
            },
            c["result"]["value"]["type"].as_str().unwrap(),
            "{name}"
        );
        for query in c["result"]["value"]["selections"].as_array().unwrap() {
            let got = schedule.at(&int(&query["height"]));
            assert_eq!(got.is_ok(), query["result"]["ok"] == true, "{name}");
            if let Ok(spec) = got {
                assert_eq!(
                    json!({"module":spec.kind().module(),"config":spec.config()}),
                    query["result"]["value"],
                    "{name}"
                );
            }
        }
        let got = schedule.check();
        assert_eq!(got.is_ok(), c["check"]["ok"] == true, "{name}: {got:?}");
        if let Ok(got) = got {
            assert_eq!(got.at(&BigInt::from(-1)), got.genesis());
            checked += 1;
        }
    }
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 380);
    assert_eq!(checked, 244);
    println!("Node chain: 380 native consensus scenarios; 244 successful startup checks");
}
// Native application settings allow values outside the execution core's
// supported protocol/height widths. Keep these cases explicit and counted.
fn boundary(name: &str) -> Option<protocols::Error> {
    for network in ["ae_dev", "private"] {
        for app in 0..3 {
            for (index, error) in [
                (10, protocols::Error::UnsupportedProtocol),
                (11, protocols::Error::Limit),
                (14, protocols::Error::Height),
            ] {
                if name == format!("{network}-{index}-{app}") {
                    return Some(error);
                }
            }
        }
    }
    match name {
        "hyperchain-10" => Some(protocols::Error::UnsupportedProtocol),
        "hyperchain-11" => Some(protocols::Error::Limit),
        "hyperchain-14" => Some(protocols::Error::Height),
        "fork-private-10" | "fork-private-13" | "fork-private-14" => Some(protocols::Error::Fork),
        _ => None,
    }
}
#[test]
fn original_protocol_tables_fork_checks_and_genesis_bytes() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_node_protocols.json")).unwrap();
    let mut built = 0;
    let mut boundaries = 0;
    let mut genesis = 0;
    for c in fixture["cases"].as_array().unwrap() {
        let name = c["name"].as_str().unwrap();
        let network = c["network"].as_str().unwrap().as_bytes();
        let selected = Protocols::select(network, c.get("user").or(c.get("application")));
        assert_eq!(
            selected.is_ok(),
            c["protocols"]["ok"] == true,
            "{name}: {selected:?}"
        );
        let Ok(selected) = selected else {
            assert_eq!(c["check"]["ok"], false);
            continue;
        };
        let table: Vec<_> = selected
            .entries()
            .iter()
            .map(|(v, h)| json!([serde_json::from_str::<Value>(&v.to_string()).unwrap(), h]))
            .collect();
        assert_eq!(json!(table), c["protocols"]["value"], "{name}");
        let profile = selected.build(kind(&c["consensus"]), c.get("fork"));
        if let Some(error) = boundary(name) {
            assert_eq!(c["check"]["ok"], true);
            assert_eq!(profile.unwrap_err(), error, "{name}");
            boundaries += 1;
            continue;
        }
        assert_eq!(
            profile.is_ok(),
            c["check"]["ok"] == true,
            "{name}: {profile:?}"
        );
        let Ok(profile) = profile else { continue };
        built += 1;
        let fork=profile.community_fork().map(|f|json!({"signalling_start_height":f.start,"signalling_end_height":f.end,"signalling_block_count":f.threshold,"info_field":f.info,"version":f.version})).unwrap_or(Value::Null);
        assert_eq!(fork, c["installed_fork"], "{name}");
        for query in c["queries"].as_array().unwrap() {
            if let Some(h) = query["height"].as_u64() {
                assert_eq!(
                    profile.scheduled_protocol_at(h) as u32,
                    query["result"]["value"].as_u64().unwrap() as u32,
                    "{name}"
                );
            }
        }
        // Raw native genesis serializes wide/negative timestamps modulo 2^64
        // and rejects a raw float. File validation and Rust startup bound it.
        if c.get("genesis_time").is_some_and(|v| v.as_u64().is_none()) {
            continue;
        }
        let mut ctx = context();
        let hard_forks: serde_json::Map<String, Value> = selected
            .entries()
            .iter()
            .map(|(v, h)| (v.to_string(), h.clone()))
            .collect();
        ctx.set(
            "aecore".into(),
            "hard_forks".into(),
            Value::Object(hard_forks),
        )
        .unwrap();
        let cfg=Configuration::from_value(json!({"chain":{"consensus":{"0":{"type":c["consensus"],"config":{"genesis_start_time":c.get("genesis_time").cloned().unwrap_or(json!(0))}}}}})).unwrap();
        let settings = Settings::resolve(&cfg.resolve(&ctx).unwrap(), Some(network)).unwrap();
        let header = Header::Key(settings.genesis_header(Hash256([0; 32])));
        assert_eq!(
            hex::encode_upper(header.encode().unwrap()),
            c["genesis"]["value"].as_str().unwrap(),
            "{name}"
        );
        genesis += 1;
    }
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 664);
    assert_eq!((built, boundaries, genesis), (523, 24, 520));
    println!(
        "Node chain: 664 native protocol scenarios; 523 profiles; 24 explicit raw-input boundaries; 520 native genesis byte comparisons"
    );
}
#[test]
fn resolved_precedence_fixed_networks_and_fresh_fork_state() {
    let user=Configuration::from_value(json!({"fork_management":{"network_id":"private"},"chain":{"consensus":{"0":{"type":"pow_cuckoo"}},"hard_forks":{"1":0,"6":3}}})).unwrap();
    let mut ctx = context();
    ctx.set(
        "aecore".into(),
        "consensus".into(),
        json!({"0":{"type":"hyperchain","config":{"genesis_start_time":999}}}),
    )
    .unwrap();
    ctx.set("aecore".into(), "hard_forks".into(), json!({"7":0}))
        .unwrap();
    let s = Settings::resolve(&user.resolve(&ctx).unwrap(), None).unwrap();
    assert_eq!(s.consensus().family(), ConsensusType::Pos);
    assert_eq!(s.profile().activations()[1].height, 3);
    assert_eq!(s.genesis_header(Hash256([0; 32])).time, 0); // user-only HC getter
    // Invalid application expansion is not evaluated for fixed network rules.
    ctx.set_global_variables(vec![
        Definition::new("LOOP".into(), Variable::Expand(json!("$LOOP"))).unwrap(),
    ])
    .unwrap();
    ctx.set("aecore".into(), "consensus".into(), json!("$LOOP"))
        .unwrap();
    ctx.set("aecore".into(), "hard_forks".into(), json!("$LOOP"))
        .unwrap();
    for network in [b"ae_mainnet".as_slice(), b"ae_uat"] {
        let s = Settings::resolve(&user.resolve(&ctx).unwrap(), Some(network)).unwrap();
        assert_eq!(s.consensus().genesis().kind(), Kind::PowCuckoo);
        assert_eq!(s.profile().activations().len(), 6);
    }
    assert!(Settings::resolve(&user.resolve(&ctx).unwrap(), None).is_err());
    ctx.remove("aecore", "consensus");
    ctx.remove("aecore", "hard_forks");
    let fork = json!({"enabled":true,"signalling_start_height":5,"signalling_end_height":10,"signalling_block_count":4,"info_field":17,"version":7});
    let enabled =
        Configuration::from_value(json!({"fork_management":{"network_id":"private","fork":fork}}))
            .unwrap();
    let old = Settings::resolve(&enabled.resolve(&ctx).unwrap(), None).unwrap();
    assert!(old.profile().community_fork().is_some());
    let fresh = Settings::resolve(&user.resolve(&ctx).unwrap(), None).unwrap();
    assert!(fresh.profile().community_fork().is_none());
    assert!(old.profile().community_fork().is_some());
    let development = Settings::resolve(
        &Configuration::empty().resolve(&ctx).unwrap(),
        Some(b"ae_dev"),
    )
    .unwrap();
    assert_eq!(development.consensus().genesis().kind(), Kind::OnDemand);
    assert_eq!(development.genesis_header(Hash256([0; 32])).target, 1338);
}
#[test]
fn finite_boundaries_redaction_and_hyperchain_adapter() {
    assert_eq!(
        Schedule::select(b"private", Some(&json!({"x":{"type":"pow_cuckoo"}}))),
        Err(consensus::Error::Height)
    );
    let huge: serde_json::Map<String, Value> = (0..1025)
        .map(|n| (n.to_string(), json!({"type":"pow_cuckoo"})))
        .collect();
    assert_eq!(
        Schedule::select(b"private", Some(&Value::Object(huge))),
        Err(consensus::Error::Limit)
    );
    let raw = json!({"0":{"type":"hyperchain","config":{"password":"do-not-log","election_contract":"ct_1111111111111111111111111111111237x6p1","parent_chain":{}}}});
    let schedule = Schedule::select(b"private", Some(&raw))
        .unwrap()
        .check()
        .unwrap();
    assert!(!format!("{schedule:?}").contains("do-not-log"));
    assert_eq!(schedule.genesis().hyperchain().unwrap().election[31], 9);
    assert!(matches!(
        Schedule::select(b"private", None)
            .unwrap()
            .check()
            .unwrap()
            .genesis()
            .hyperchain(),
        Err(consensus::Error::Kind)
    ));
    let ctx = context();
    let cfg=Configuration::from_value(json!({"chain":{"hard_forks":{"7":0},"consensus":{"0":{"type":"hyperchain","config":{"genesis_start_time":18446744073709551616u128}}}}})).unwrap();
    assert!(matches!(
        Settings::resolve(&cfg.resolve(&ctx).unwrap(), Some(b"private")),
        Err(ae_node::configuration::chain::Error::GenesisTime)
    ));
    assert_eq!(
        Protocols::select(&vec![0; 65537], None),
        Err(protocols::Error::Limit)
    );
}
