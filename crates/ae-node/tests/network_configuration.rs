use ae_node::configuration::{
    Configuration,
    network::{Error, Network},
    resolution::{Context, Directories},
};
use serde_json::{Value, json};
use std::time::Duration;
fn context() -> Context {
    Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap()
}
fn network(value: Value) -> Result<Network, Error> {
    let config = Configuration::from_value(value).unwrap();
    let ctx = context();
    Network::resolve(&config.resolve(&ctx).unwrap())
}
#[test]
fn native_network_lookup_precedence_matches_all_schema_valid_cases() {
    let data: Value =
        serde_json::from_str(include_str!("fixtures/erlang_network_runtime_config.json")).unwrap();
    for case in data["cases"].as_array().unwrap() {
        let mut ctx = context();
        for (key, value) in case["application"].as_object().unwrap() {
            ctx.set("aecore".into(), key.clone(), value.clone())
                .unwrap();
        }
        let config = Configuration::from_value(case["user"].clone()).unwrap();
        let resolved = config.resolve(&ctx).unwrap();
        let got = Network::resolve(&resolved).unwrap();
        let listener = got.listener();
        let transport = got.transport();
        let peers = got.peers();
        let actual = json!({
            "listener":{"sync_port":got.listen().port(),"ext_sync_port":got.advertised_port(),"sync_listen_address":got.listen().ip().octets(),"acceptors":listener.acceptors,"max_inbound_hard":listener.maximum_connections},
            "transport":{"connect_timeout":transport.connect_timeout.as_millis(),"first_ping_timeout":transport.first_ping_timeout.as_millis(),"noise_hs_timeout":transport.noise_handshake_timeout.as_millis(),"close_timeout":transport.close_timeout.as_millis(),"gossiped_peers_count":got.ping().share,"max_gossiped_peers_count":32,"ping_connected_peer_percentage":got.ping().connected_percent,"is_node_info_sharing_enabled":got.share_node_info()},
            "peers":{"ping_interval":peers.ping_interval,"unblock_interval":peers.unblock_interval,"max_inbound":peers.maximum_inbound,"max_outbound":peers.maximum_outbound,"max_tcp_probes":peers.maximum_probes,"single_outbound_per_group":peers.single_outbound_per_group,"resolve_max_retries":peers.resolver_retries,"resolve_backoff_times":peers.resolver_backoff,"log_peer_connection_count_interval":got.peer_count_interval().as_millis()},
            "minimum_ping_version":got.minimum_ping(),"pool_sync_interval":got.pool_sync_ms()
        });
        assert_eq!(actual, case["result"], "{}", case["name"]);
    }
    assert_eq!(data["cases"].as_array().unwrap().len(), 68);
}
#[test]
fn network_application_preserves_resources_bindings_and_unrelated_policy() {
    let mut config = ae_sync::runtime::connection::Config::default();
    config.network.transport.connections = 67;
    config.network.registry.policy.maximum_connections = 66;
    config.network.registry.policy.maximum_pending_inbound = 7;
    config.network.registry.policy.admission_timeout = 123;
    config.network.startup.defaults = Some(vec![b"explicit-seed-override".to_vec()]);
    config.network.startup.addresses.host_bytes = 73;
    config.peer.ping.sync_start = -41;
    config.observer.interval = Duration::from_secs(71);
    config.sync.resilience.depth = 31;
    let policy=network(json!({"sync":{"port":3100,"external_port":3200,"max_inbound_hard":2,"acceptors":3,"provide_node_info":false,"minimum_ping_version":2,"log_peer_connection_count_interval":23},"include_default_peers":false})).unwrap();
    policy.apply(&mut config);
    assert_eq!(config.network.listen.unwrap().port(), 3100);
    assert_eq!(config.peer.port, 3200);
    assert_eq!(config.network.listener.maximum_connections, 2);
    assert_eq!(config.network.listener.acceptors, 3);
    assert_eq!(
        config.network.transport.runtime.handshake_timeout,
        Duration::from_secs(5)
    );
    assert_eq!(config.network.transport.runtime.minimum_ping, 2);
    assert!(config.peer.node_info.is_none());
    assert_eq!(
        config.observer.peer_count_interval,
        Some(Duration::from_millis(23))
    );
    assert_eq!(config.network.transport.connections, 67);
    assert_eq!(config.network.registry.policy.maximum_connections, 66);
    assert_eq!(config.network.registry.policy.maximum_pending_inbound, 7);
    assert_eq!(config.network.registry.policy.admission_timeout, 123);
    assert_eq!(
        config.network.startup.defaults,
        Some(vec![b"explicit-seed-override".to_vec()])
    );
    assert_eq!(config.network.startup.addresses.host_bytes, 73);
    assert!(!config.network.startup.include_default_peers);
    assert_eq!(config.peer.ping.sync_start, -41);
    assert_eq!(config.observer.interval, Duration::from_secs(71));
    assert_eq!(config.sync.resilience.depth, 31);
    assert_eq!(config.network.pool.session.resync_ms, 1_800_000);
    network(json!({})).unwrap().apply(&mut config);
    assert!(config.peer.node_info.is_some());
}
#[test]
fn network_domain_errors_are_explicit_and_zero_listener_threshold_is_retained() {
    for (field, value) in [
        ("port", json!(-1)),
        ("external_port", json!(65536)),
        ("connect_timeout", json!(0)),
        ("minimum_ping_version", json!(3)),
        ("acceptors", json!(65537)),
    ] {
        assert!(
            matches!(network(json!({"sync":{field:value}})), Err(Error::Range(_))),
            "{field}"
        );
    }
    assert!(matches!(
        network(json!({"sync":{"listen_address":"::1"}})),
        Err(Error::UnsupportedAddress)
    ));
    assert!(matches!(
        network(json!({"sync":{"listen_address":"invalid"}})),
        Err(Error::Address)
    ));
    let policy=network(json!({"sync":{"port":0,"max_inbound_hard":0,"acceptors":0,"max_inbound":0,"max_outbound":0,"log_peer_connection_count_interval":0}})).unwrap();
    assert_eq!(policy.listener().maximum_connections, 0);
    assert_eq!(policy.listener().acceptors, 0);
    assert_eq!(policy.advertised_port(), 0);
    assert_eq!(policy.peer_count_interval(), Duration::ZERO);
}
