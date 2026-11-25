use ae_node::{
    configuration::{
        Configuration,
        resolution::{Context, Directories},
    },
    runtime,
};
use ae_p2p::{
    message::{Details, Kind, Message, Value},
    noise, ping, runtime as net,
};
use ae_sync::{
    observe,
    runtime::{connection, workers},
};
use std::time::Duration;
#[path = "optional_services.rs"]
mod optional_services;
pub use optional_services::Services;

pub fn configure_policy_only(
    prepared: &runtime::Prepared,
    settings: &mut runtime::Settings,
    enabled: bool,
) {
    let cfg=Configuration::from_value(serde_json::json!({"include_default_peers":false,
        "sync":{"listen_address":"127.0.0.1","port":0,"peer_analytics":enabled,"upnp_enabled":false}})).unwrap();
    let mut ctx = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    for (key, value) in [
        ("sync_max_inbound", 17),
        ("sync_max_outbound", 19),
        ("sync_max_tcp_probes", 0),
    ] {
        ctx.set("aecore".into(), key.into(), serde_json::json!(value))
            .unwrap();
    }
    let upnp = match settings {
        runtime::Settings::Cuckoo(s) => &mut s.config.workers.upnp,
        runtime::Settings::OnDemand(s) => &mut s.config.workers.upnp,
        runtime::Settings::Hyperchain(s) => &mut s.config.workers.upnp,
    };
    // An explicitly disabled mapping service must never construct this invalid
    // runtime configuration or attempt any system gateway discovery.
    *upnp = Some(ae_sync::upnp::Config {
        requests: 0,
        system_gateways: false,
        ..Default::default()
    });
    let resolved = cfg.resolve(&ctx).unwrap();
    prepared.configure_network(&resolved, settings).unwrap();
    prepared
        .configure_optional_network(&resolved, settings)
        .unwrap();
}

pub async fn verify_policy_only(
    handle: &runtime::Handle,
    enabled: bool,
) -> ae_sync::analytics::Client {
    let workers = if let Some(pow) = handle.pow() {
        pow.workers().unwrap()
    } else {
        (*handle.hyperchain().unwrap().workers().unwrap()).clone()
    };
    let bindings = workers.bindings();
    assert!(bindings.upnp.is_none() && workers.status().upnp.is_none());
    let analytics = bindings.analytics.unwrap().current().unwrap();
    assert_eq!(analytics.enabled(), enabled);
    assert!(analytics.info().running);
    let mut connections = handle.connections().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(mut generation) = connections.current() {
                while !generation.network.status().ready {
                    generation.network.changed().await.unwrap();
                }
                let metrics = generation.network.metrics().unwrap().wait().await.unwrap();
                assert_eq!(
                    (metrics.maximum_inbound, metrics.maximum_outbound),
                    if enabled {
                        (1_000_000, 1_000_000)
                    } else {
                        (17, 19)
                    }
                );
                assert_eq!(metrics.registry.counts.inbound, 0);
                break;
            }
            connections.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    analytics
}

pub fn configure(
    prepared: &mut runtime::Prepared,
    settings: &mut runtime::Settings,
    services: &Services,
) {
    let configuration = Configuration::from_value(serde_json::json!({
        "include_default_peers": false,
        "sync": {"listen_address":"127.0.0.1", "port":0, "external_port":3456,
            "acceptors":3, "max_inbound_hard":2, "max_inbound":7, "max_outbound":0,
            "minimum_ping_version":2, "gossiped_peers_count":4, "ping":{"peer_percentage":67}, "ping_interval":100,
            "log_peer_connection_count_interval":20,"peer_analytics":true,"upnp_enabled":true}
    }))
    .unwrap();
    let mut context = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    context
        .set(
            "aecore".into(),
            "sync_max_tcp_probes".into(),
            serde_json::json!(0),
        )
        .unwrap();
    prepared
        .configure_network(&configuration.resolve(&context).unwrap(), settings)
        .unwrap();
    prepared
        .configure_optional_network(&configuration.resolve(&context).unwrap(), settings)
        .unwrap();
    let upnp = match settings {
        runtime::Settings::Cuckoo(s) => &mut s.config.workers.upnp,
        runtime::Settings::OnDemand(s) => &mut s.config.workers.upnp,
        runtime::Settings::Hyperchain(s) => &mut s.config.workers.upnp,
    };
    services.configure(upnp.as_mut().unwrap());
    let discovery = Configuration::from_value(serde_json::json!({
        "sync":{"max_gossip":1,"gossip_allowed_height_from_top":0,
            "peer_pool":{"max_rejections":2,"max_update_lapse":60000,"standby_times":[10,20]}}
    }))
    .unwrap();
    context
        .set(
            "aecore".into(),
            "peer_pool".into(),
            serde_json::json!({"max_rejections":3}),
        )
        .unwrap();
    prepared
        .configure_discovery(&discovery.resolve(&context).unwrap(), settings)
        .unwrap();
    let config = match settings {
        runtime::Settings::Cuckoo(s) => &mut s.config.connections,
        runtime::Settings::OnDemand(s) => &mut s.config.connections,
        runtime::Settings::Hyperchain(s) => &mut s.config.connections,
    };
    assert_eq!(config.peer.port, 3456);
    assert_eq!(config.network.transport.runtime.minimum_ping, 2);
    assert_eq!(config.peer.ping.connected_percent, 67);
    assert_eq!(config.peer.ping.share, 4);
    assert_eq!(config.network.pool.session.resync_ms, 1_800_000);
    config.observer.interval = Duration::from_secs(30);
}

pub async fn verify(
    mut owner: connection::Handle,
    db: &ae_storage::Database,
    local: ae_types::PeerPublicKey,
    workers: &workers::Handle,
    services: &Services,
) -> connection::Generation {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut retired: Option<connection::Generation> = None;
        for number in 1..=2 {
            let mut generation = loop {
                if let Some(generation) = owner.current()
                    && generation.number >= number
                {
                    break generation;
                }
                owner.changed().await.unwrap();
            };
            if let Some(previous) = retired.take() {
                assert!(previous.network.status().joined && previous.observer.status().joined);
                assert!(previous.network.status().listener.unwrap().closed);
                assert_eq!(previous.network.status().listener.unwrap().active, 0);
            }
            while !generation.network.status().ready {
                generation.network.changed().await.unwrap();
            }
            let status = generation.network.status();
            let listener = status.listener.unwrap();
            assert_eq!((listener.acceptors, listener.maximum_connections), (3, 2));
            assert!(status.listening.unwrap().ip().is_loopback());
            assert_eq!(generation.gossip.status().policy.max_peers, 1);
            services
                .mapped(workers, status.listening.unwrap().port(), if number == 1 { 1 } else { 3 })
                .await;
            let binding = db.view().unwrap().identity().clone();
            let network = noise::Network::new(1, binding.genesis, &binding.network).unwrap();
            let socket = tokio::net::TcpStream::connect(status.listening.unwrap())
                .await
                .unwrap();
            let (client, mut events, driver) = net::establish(
                socket,
                &noise::Identity::from_secret([98; 32]),
                &network,
                noise::Role::Initiator(local),
                Default::default(),
            )
            .await
            .unwrap();
            let driver = tokio::spawn(driver.run());
            let request = ping::encode(
                &ae_sync::peer::snapshot::local(&db.view().unwrap(), false).unwrap(),
                vec![],
                &[],
                ping::Encoding {
                    version: 2,
                    minimum_version: 2,
                    port: 4321,
                    share: 0.into(),
                    limits: Default::default(),
                },
            )
            .unwrap();
            let net::Reply::Response(response) =
                client.request(&request, None).unwrap().await.unwrap()
            else {
                panic!()
            };
            let Details::Response {
                message: Some(Ok(body)),
                ..
            } = response.message().details()
            else {
                panic!("{:?}", response.message())
            };
            assert_eq!(body.fields()["port"], Value::Integer(3456.into()));
            // Native ping_obj_rsp echoes the requested share, while local_ping_obj
            // uses gossiped_peers_count for the separately scheduled outgoing ping.
            assert_eq!(body.fields()["share"], Value::Integer(0.into()));
            loop {
                if let Some(net::Event::Message(ping)) = events.recv().await {
                    if ping.message().kind() == Kind::GetNodeInfo {
                        client.send(&node_info()).unwrap().await.unwrap();
                        continue;
                    }
                    if ping.message().kind() != Kind::Ping {
                        continue;
                    }
                    assert_eq!(ping.message().fields()["share"], Value::Integer(4.into()));
                    assert_eq!(ping.message().fields()["port"], Value::Integer(3456.into()));
                    client
                        .send(&Message::response_ok(&request, Default::default()).unwrap())
                        .unwrap()
                        .await
                        .unwrap();
                    break;
                }
            }
            let responder = client.clone();
            let reply = Message::response_ok(&request, Default::default()).unwrap();
            let drain = tokio::spawn(async move {
                while let Some(event) = events.recv().await {
                    if let net::Event::Message(message) = event {
                        let reply = match message.message().kind() {
                            Kind::Ping => reply.clone(),
                            Kind::GetNodeInfo => node_info(),
                            _ => continue,
                        };
                        let Ok(sent) = responder.send(&reply) else {
                            break;
                        };
                        if sent.await.is_err() {
                            break;
                        }
                    }
                }
            });
            let analytics = workers.bindings().analytics.unwrap().current().unwrap();
            loop {
                let snapshot = analytics.snapshot().unwrap().await.unwrap();
                if snapshot.pending == 0
                    && snapshot.stats.values().any(|stat| matches!(&stat.info,
                        ae_sync::analytics::PeerInfo::Resolved(info) if info.network_id == b"configured-test"))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            if number == 1 {
                let before = workers.status();
                let mapping = workers.bindings().upnp.unwrap().current().unwrap();
                workers
                    .restart(workers::Child::Analytics, before.analytics_generation)
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
                workers
                    .restart(workers::Child::Upnp, before.upnp_generation)
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
                assert!(analytics.info().joined && mapping.info().joined);
                assert!(
                    workers
                        .bindings()
                        .analytics
                        .unwrap()
                        .current()
                        .unwrap()
                        .enabled()
                );
                services
                    .mapped(workers, status.listening.unwrap().port(), 2)
                    .await;
                assert_eq!(owner.status().generation, number);
                assert!(!client.is_closed());
            }
            let metrics = generation.network.metrics().unwrap().wait().await.unwrap();
            assert_eq!((metrics.maximum_inbound, metrics.maximum_outbound), (7, 0));
            assert_eq!(metrics.registry.counts.inbound, 1);
            let mut records = generation.observer.subscribe(None).unwrap();
            loop {
                if let Some(observe::Delivery::Record(record)) = records.recv().await {
                    let value: serde_json::Value =
                        serde_json::from_slice(record.json_line()).unwrap();
                    if record.category == "peers" && value["data"]["inbound"] == 1 {
                        assert_eq!(value["data"]["maximum_inbound"], 7);
                        assert_eq!(value["data"]["maximum_outbound"], 0);
                        break;
                    }
                }
            }
            client.shutdown();
            driver.await.unwrap();
            drain.await.unwrap();
            while generation.network.status().listener.unwrap().active != 0 {
                generation.network.changed().await.unwrap();
            }
            if number == 2 {
                return generation;
            }
            generation.peer.shutdown();
            retired = Some(generation);
        }
        unreachable!()
    })
    .await
    .unwrap()
}

fn node_info() -> Message {
    ae_sync::peer::info::response(
        Some(&ae_sync::peer::info::Metadata::current()),
        b"configured-test",
        &1u64.into(),
        &2u64.into(),
        Default::default(),
    )
    .unwrap()
    .unwrap()
}
