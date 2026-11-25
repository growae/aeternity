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
use ae_sync::{observe, runtime::connection};
use std::time::Duration;

pub fn configure(prepared: &mut runtime::Prepared, settings: &mut runtime::Settings) {
    let configuration = Configuration::from_value(serde_json::json!({
        "include_default_peers": false,
        "sync": {"listen_address":"127.0.0.1", "port":0, "external_port":3456,
            "acceptors":3, "max_inbound_hard":2, "max_inbound":7, "max_outbound":0,
            "minimum_ping_version":2, "gossiped_peers_count":4, "ping":{"peer_percentage":67}, "ping_interval":100,
            "log_peer_connection_count_interval":20}
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
) -> connection::Generation {
    tokio::time::timeout(Duration::from_secs(10), async {
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
                if let Some(net::Event::Message(ping)) = events.recv().await
                    && ping.message().kind() == Kind::Ping
                {
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
