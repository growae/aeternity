use ae_node::configuration::{
    Configuration,
    network::{Network, Optional},
    resolution::{Context, Directories},
};
use ae_sync::{
    analytics,
    runtime::{connection, workers},
};
use serde_json::{Value, json};

fn context(application: &Value) -> Context {
    let mut ctx = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    for (key, value) in application.as_object().unwrap() {
        ctx.set("aecore".into(), key.clone(), value.clone())
            .unwrap();
    }
    ctx
}
fn apply(
    user: Value,
    ctx: &Context,
    workers: &mut workers::Config,
    connection: &mut connection::Config,
) -> Optional {
    let cfg = Configuration::from_value(user).unwrap();
    let resolved = cfg.resolve(ctx).unwrap();
    Network::resolve(&resolved).unwrap().apply(connection);
    let optional = Optional::resolve(&resolved).unwrap();
    optional.apply(workers, connection);
    optional
}
fn caps(config: &connection::Config) -> Value {
    json!([
        config.network.registry.policy.maximum_inbound,
        config.network.registry.policy.maximum_outbound
    ])
}
fn expected(value: &Value) -> Value {
    json!([value["inbound"], value["outbound"]])
}

#[test]
fn original_flags_initialization_and_disabled_child_restart_match_actual_retained_workers() {
    let native: Value =
        serde_json::from_str(include_str!("fixtures/erlang_optional_network_config.json")).unwrap();
    let schema: Value =
        serde_json::from_str(include_str!("fixtures/erlang_optional_network_schema.json")).unwrap();
    assert_eq!(schema["cases"].as_array().unwrap().len(), 18);
    for case in schema["cases"].as_array().unwrap() {
        assert_eq!(
            case["schema_result"],
            json!({"status":"ok","unchanged":true})
        );
        Configuration::from_value(case["user"].clone()).unwrap();
    }
    assert_eq!(native["cases"].as_array().unwrap().len(), 16);
    let mut restarts = 0;
    for case in native["cases"].as_array().unwrap() {
        let ctx = context(&case["application"]);
        let owner = analytics::Owner::default();
        let mut workers = workers::Config::default();
        let mut connection = connection::Config::default();
        let optional = apply(case["user"].clone(), &ctx, &mut workers, &mut connection);
        let result = &case["result"];
        assert_eq!(
            caps(&connection),
            expected(&result["before"]),
            "{}",
            case["name"]
        );
        assert_eq!(json!(optional.analytics), result["before"]["analytics"]);
        assert_eq!(json!(optional.upnp), result["before"]["upnp"]);
        assert_eq!(
            json!(usize::from(workers.upnp.is_some())),
            result["before"]["upnp_children"]
        );
        // Only the analytics worker starts here. UPnP sockets are exercised by
        // the separate family tests using explicit localhost emulators.
        let worker = owner.spawn(workers.analytics.clone().unwrap()).unwrap();
        let client = worker.client();
        client.configure_network(
            &mut connection.network.registry.policy,
            connection.analytics_limits,
        );
        assert_eq!(
            caps(&connection),
            expected(&result["after_init"]),
            "{}",
            case["name"]
        );
        assert_eq!(
            connection.network.registry.policy.analytics,
            optional.analytics
        );
        worker.shutdown().unwrap();
        assert!(client.info().joined);
        if let Some(user) = case.get("restart_user") {
            let optional = apply(user.clone(), &ctx, &mut workers, &mut connection);
            let worker = owner.spawn(workers.analytics.clone().unwrap()).unwrap();
            worker.client().configure_network(
                &mut connection.network.registry.policy,
                connection.analytics_limits,
            );
            assert_eq!(caps(&connection), expected(&result["after_restart"]));
            assert_eq!(
                connection.network.registry.policy.analytics,
                optional.analytics
            );
            worker.shutdown().unwrap();
            restarts += 1;
        }
    }
    assert_eq!(restarts, 2);
}

#[test]
fn optional_flags_preserve_resources_and_user_limit_provenance_in_both_application_orders() {
    let ctx = context(&json!({"peer_analytics":false,"upnp_enabled":false}));
    let cfg=Configuration::from_value(json!({"sync":{"peer_analytics":true,"upnp_enabled":true,"max_inbound":7,"max_outbound":0}})).unwrap();
    let resolved = cfg.resolve(&ctx).unwrap();
    let optional = Optional::resolve(&resolved).unwrap();
    let network = Network::resolve(&resolved).unwrap();
    for reverse in [false, true] {
        let mut workers = workers::Config::default();
        let mut analytics = analytics::Config::default();
        analytics.limits.requests = 13;
        workers.analytics = Some(analytics);
        let mapping = ae_sync::upnp::Config {
            system_gateways: false,
            requests: 11,
            ..Default::default()
        };
        let ports = mapping.ports.clone();
        workers.upnp = Some(mapping);
        let mut connection = connection::Config::default();
        connection.network.registry.policy.maximum_connections = 3;
        if reverse {
            optional.apply(&mut workers, &mut connection);
            network.apply(&mut connection);
        } else {
            network.apply(&mut connection);
            optional.apply(&mut workers, &mut connection);
        }
        assert_eq!(
            connection.analytics_limits,
            analytics::NetworkLimits {
                inbound: Some(7),
                outbound: Some(0)
            }
        );
        assert_eq!(workers.analytics.unwrap().limits.requests, 13);
        let mapping = workers.upnp.unwrap();
        assert!(!mapping.system_gateways);
        assert_eq!(mapping.requests, 11);
        assert_eq!(mapping.ports.get(), ports.get());
        assert_eq!(connection.network.registry.policy.maximum_connections, 3);
    }
    let cfg = Configuration::empty();
    let mut workers = workers::Config {
        upnp: Some(Default::default()),
        ..Default::default()
    };
    Optional::resolve(&cfg.resolve(&ctx).unwrap())
        .unwrap()
        .apply(&mut workers, &mut Default::default());
    assert!(!workers.analytics.unwrap().enabled);
    assert!(workers.upnp.is_none());
}
