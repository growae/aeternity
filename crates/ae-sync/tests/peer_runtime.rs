mod common;
#[path = "peer_runtime/connection_owner.rs"]
mod connection_owner;
#[path = "peer_runtime/worker_owner.rs"]
mod worker_owner;
use ae_p2p::{
    message::{self, Details, Kind, Message, Value},
    noise, ping,
    registry::{Command, Info},
    runtime as net,
};
use ae_protocol::{ConsensusType, NetworkProfile};
use ae_sync::{
    gossip, network,
    peer::{self, snapshot},
};
use ae_types::{Hash256, PeerPublicKey};
use common::{Node, within};
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

struct App {
    network: network::Handle,
    gossip: gossip::Handle,
    application: peer::Handle,
    events: peer::Events,
    network_task: JoinHandle<network::Exit>,
    gossip_task: JoinHandle<gossip::Status>,
    application_task: JoinHandle<peer::Status>,
    identity: Arc<noise::Identity>,
}
fn config() -> network::Config {
    let mut c = network::Config {
        listen: Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
        ..Default::default()
    };
    c.startup.include_default_peers = false;
    c.registry.policy.maximum_outbound = 0;
    c.registry.policy.maximum_probes = 0;
    c
}
fn identities() -> (Arc<noise::Identity>, Arc<noise::Identity>) {
    let a = Arc::new(noise::Identity::from_secret([1; 32]));
    let b = Arc::new(noise::Identity::from_secret([2; 32]));
    if a.public_key() < b.public_key() {
        (a, b)
    } else {
        (b, a)
    }
}
async fn start(node: &Node, c: network::Config, p: peer::Config) -> App {
    let (identity, _) = identities();
    let (mut network, scheduled, pool, service) = network::create(
        node.db.clone(),
        node.client(),
        identity.clone(),
        NetworkProfile::mainnet(),
        c,
    )
    .await
    .unwrap();
    let network_task = tokio::spawn(service.run());
    let (gossip, incoming, service) = gossip::create(
        node.db.clone(),
        node.client(),
        network.peers(),
        pool,
        NetworkProfile::mainnet(),
        ConsensusType::Pow,
        Default::default(),
    )
    .unwrap();
    let gossip_task = tokio::spawn(service.run());
    let (application, events, service) =
        peer::create(node.db.clone(), network.clone(), scheduled, incoming, p).unwrap();
    let application_task = tokio::spawn(service.run());
    within(async {
        while !network.status().ready {
            assert!(network.status().failure.is_none());
            network.changed().await.unwrap();
        }
    })
    .await;
    App {
        network,
        gossip,
        application,
        events,
        network_task,
        gossip_task,
        application_task,
        identity,
    }
}
async fn stop(app: App) {
    app.application.shutdown();
    let status = within(app.application_task).await.unwrap();
    assert!(status.joined);
    assert!(!status.running);
    assert_eq!(status.connections, 0);
    app.gossip.shutdown();
    app.network.shutdown();
    assert!(!within(app.gossip_task).await.unwrap().running);
    assert!(within(app.network_task).await.unwrap().status.joined);
}
async fn connect(node: &Node, app: &App) -> (net::Client, net::Events, JoinHandle<net::Exit>) {
    let (_, remote) = identities();
    let socket = TcpStream::connect(app.network.status().listening.unwrap())
        .await
        .unwrap();
    establish(
        node,
        socket,
        &remote,
        noise::Role::Initiator(app.identity.public_key()),
    )
    .await
}
async fn establish(
    node: &Node,
    socket: TcpStream,
    id: &noise::Identity,
    role: noise::Role,
) -> (net::Client, net::Events, JoinHandle<net::Exit>) {
    let binding = node.db.view().unwrap().identity().clone();
    let network = noise::Network::new(1, binding.genesis, &binding.network).unwrap();
    let (client, events, driver) = within(net::establish(
        socket,
        id,
        &network,
        role,
        Default::default(),
    ))
    .await
    .unwrap();
    (client, events, tokio::spawn(driver.run()))
}
fn ping(node: &Node, version: u16, temporary: bool) -> Message {
    let local = snapshot::local(&node.db.view().unwrap(), temporary).unwrap();
    ping::encode(
        &local,
        vec![],
        &[],
        ping::Encoding {
            version,
            minimum_version: 1,
            port: 4321,
            share: 0.into(),
            limits: Default::default(),
        },
    )
    .unwrap()
}
fn alter(message: &Message, updates: &[(&str, Value)]) -> Message {
    let mut fields = message.fields().clone();
    for (key, value) in updates {
        fields.insert((*key).into(), value.clone());
    }
    Message::new(
        message.kind(),
        message.version(),
        fields,
        Default::default(),
    )
    .unwrap()
}
fn message(kind: Kind, version: u16, fields: &[(&str, Value)]) -> Message {
    Message::new(
        kind,
        version,
        fields
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect(),
        Default::default(),
    )
    .unwrap()
}
async fn response(client: &net::Client, request: &Message) -> net::Received {
    let net::Reply::Response(received) = within(client.request(request, None).unwrap())
        .await
        .unwrap()
    else {
        panic!()
    };
    received
}
fn body(received: &net::Received) -> &Message {
    let Details::Response {
        message: Some(Ok(body)),
        ..
    } = received.message().details()
    else {
        panic!("{:?}", received.message())
    };
    body
}
async fn incoming(events: &mut net::Events) -> net::Received {
    within(async {
        loop {
            if let net::Event::Message(m) = events.recv().await.unwrap() {
                return m;
            }
        }
    })
    .await
}
async fn observed(events: &mut peer::Events) -> peer::Event {
    within(async {
        loop {
            let event = events.recv().await.unwrap();
            if !matches!(event.kind(), peer::EventKind::Network(_)) {
                return event;
            }
        }
    })
    .await
}
fn cap(n: u64) -> message::ChainPoi {
    message::ChainPoi {
        height: n.into(),
        root_hash: vec![1; 32],
        genesis: vec![2; 32],
        top: vec![3; 32],
        poi: vec![4; 10],
    }
}

#[tokio::test]
async fn application_rejects_invalid_binding_or_metadata_and_stop_before_run_joins_readers() {
    let node = Node::new();
    let other = Node::new();
    for (wrong, oversized, deferred) in [
        (true, false, true),
        (false, true, true),
        (false, false, false),
        (false, false, true),
    ] {
        let (identity, _) = identities();
        let mut network_config = config();
        network_config.transport.runtime.defer_pings = deferred;
        let (network, scheduled, pool, network_service) = network::create(
            node.db.clone(),
            node.client(),
            identity,
            NetworkProfile::mainnet(),
            network_config,
        )
        .await
        .unwrap();
        let (gossip, incoming, gossip_service) = gossip::create(
            node.db.clone(),
            node.client(),
            network.peers(),
            pool,
            NetworkProfile::mainnet(),
            ConsensusType::Pow,
            Default::default(),
        )
        .unwrap();
        let mut application = peer::Config::default();
        if oversized {
            application.node_info.as_mut().unwrap().version = vec![0; 1025];
        }
        let result = peer::create(
            if wrong {
                other.db.clone()
            } else {
                node.db.clone()
            },
            network.clone(),
            scheduled,
            incoming,
            application,
        );
        if wrong || !deferred {
            assert!(matches!(result, Err(peer::Error::Configuration)));
        } else if oversized {
            assert!(matches!(
                result,
                Err(peer::Error::Message(message::Error::ResourceLimit))
            ));
        } else {
            let (handle, _events, service) = result.unwrap();
            handle.shutdown();
            let status = service.run().await;
            assert!(status.joined);
            assert!(!status.running);
        }
        gossip.shutdown();
        network.shutdown();
        assert!(!gossip_service.run().await.running);
        assert!(network_service.run().await.status.joined);
    }
}

#[tokio::test]
async fn initial_v2_skips_processing_then_errors_register_last_capability_and_valid_ping_learns_peers()
 {
    let node = Node::new();
    let mut app = start(&node, config(), Default::default()).await;
    let (client, _events, remote_task) = connect(&node, &app).await;
    let wrong = alter(
        &ping(&node, 2, true),
        &[
            ("genesis_hash", Value::Binary(vec![9; 32])),
            (
                "capabilities",
                Value::Binary(
                    message::encode_capabilities(&[cap(1), cap(2)], Default::default()).unwrap(),
                ),
            ),
        ],
    );
    let first = response(&client, &wrong).await;
    assert_eq!(body(&first).kind(), Kind::Ping);
    assert_eq!(
        body(&first).fields()["genesis_hash"],
        ping(&node, 2, false).fields()["genesis_hash"]
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(25), app.events.recv())
            .await
            .is_err()
    );
    // Native ping errors have an outer v1 envelope and no inner ping object.
    // They cannot complete a v2 request slot; the callback effects still occur.
    within(client.send(&wrong).unwrap()).await.unwrap();
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Status {
            temporary: true,
            ..
        }
    ));
    assert!(
        matches!(observed(&mut app.events).await.kind(), peer::EventKind::Capabilities { chain_poi,.. } if *chain_poi == cap(2))
    );
    let refused = response(
        &client,
        &alter(
            &ping(&node, 1, true),
            &[("genesis_hash", Value::Binary(vec![9; 32]))],
        ),
    )
    .await;
    assert_eq!(
        refused.message().fields()["reason"],
        Value::Binary(b"wrong_genesis_hash".to_vec())
    );
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Status { .. }
    ));
    let learned = Info {
        id: PeerPublicKey([8; 32]),
        host: b"127.0.0.3".to_vec(),
        port: 9999,
    };
    let wire = message::Peer {
        host: learned.host.clone(),
        port: u64::from(learned.port).into(),
        pubkey: learned.id.0.to_vec(),
    }
    .encode(Default::default())
    .unwrap();
    let valid = alter(
        &ping(&node, 2, true),
        &[
            ("share", Value::Integer(1.into())),
            ("peers", Value::List(vec![Value::Binary(wire)])),
        ],
    );
    assert_eq!(body(&response(&client, &valid).await).kind(), Kind::Ping);
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Status { .. }
    ));
    within(async {
        loop {
            if let Some(saved) = node.db.peer_view().unwrap().get(learned.id, 1024).unwrap() {
                assert_eq!(saved.source, std::net::IpAddr::V4(Ipv4Addr::LOCALHOST));
                assert!(!saved.trusted);
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    stop(app).await;
    within(remote_task).await.unwrap();
}

#[tokio::test]
async fn malformed_capabilities_close_after_status_and_sync_intent_without_success_response() {
    let node = Node::new();
    let mut app = start(&node, config(), Default::default()).await;
    let (client, _events, remote_task) = connect(&node, &app).await;
    response(&client, &ping(&node, 2, true)).await;
    let bad = alter(
        &ping(&node, 2, false),
        &[
            ("best_hash", Value::Binary(vec![])),
            (
                "difficulty",
                Value::Integer(
                    ae_types::Unsigned::from_decimal("79228162514264337593543950336").unwrap(),
                ),
            ),
            ("capabilities", Value::Binary(vec![0xff])),
        ],
    );
    assert!(within(client.request(&bad, None).unwrap()).await.is_err());
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Status { .. }
    ));
    assert!(
        matches!(observed(&mut app.events).await.kind(),peer::EventKind::Chain {intent:peer::ChainIntent::Start {best,..},..} if best.is_empty())
    );
    within(async {
        while app.application.status().counts.failed_connections == 0 {
            app.application.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(
        app.application.status().last_error,
        Some(peer::Error::MalformedCapabilities)
    );
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    stop(app).await;
    within(remote_task).await.unwrap();
}

#[tokio::test]
async fn temporary_connection_replies_with_sync_disabled_then_closes_and_service_joins() {
    let node = Node::new();
    let mut c = config();
    c.registry.policy.maximum_inbound = 0;
    let app = start(&node, c, Default::default()).await;
    let (client, _events, task) = connect(&node, &app).await;
    let received = response(&client, &ping(&node, 1, true)).await;
    assert_eq!(
        body(&received).fields()["sync_allowed"],
        Value::Boolean(false)
    );
    within(task).await.unwrap();
    stop(app).await;
}

#[tokio::test]
async fn scheduled_ping_completes_before_v2_probe_and_simultaneous_ping_is_refused() {
    let node = Node::new();
    let mut c = config();
    c.registry.policy.maximum_outbound = 1;
    c.registry.policy.ping_interval = 60_000;
    let mut app = start(&node, c, Default::default()).await;
    let (_, remote) = identities();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = ae_p2p::discovery::Peer {
        id: remote.public_key(),
        address: Ipv4Addr::LOCALHOST,
        source: Ipv4Addr::LOCALHOST,
        host: b"127.0.0.1".to_vec(),
        port: listener.local_addr().unwrap().port(),
        trusted: false,
    };
    app.network
        .command(Command::Add(peer.clone()))
        .unwrap()
        .wait()
        .await
        .unwrap();
    app.network
        .command(Command::Connect(Some(peer.id)))
        .unwrap()
        .wait()
        .await
        .unwrap();
    let (socket, _) = within(listener.accept()).await.unwrap();
    let (client, mut events, remote_task) =
        establish(&node, socket, &remote, noise::Role::Responder).await;
    let first = incoming(&mut events).await;
    assert_eq!(first.message().version(), 1);
    let collision = response(&client, &ping(&node, 1, true)).await;
    assert_eq!(
        collision.message().fields()["reason"],
        Value::Binary(b"already_pinging".to_vec())
    );
    within(
        client
            .send(&Message::response_ok(&ping(&node, 1, true), Default::default()).unwrap())
            .unwrap(),
    )
    .await
    .unwrap();
    let probe = incoming(&mut events).await;
    assert_eq!(probe.message().version(), 2);
    within(async {
        while app.network.status().counts.pings_completed == 0 {
            app.network.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(app.network.status().counts.pings_completed, 1);
    within(async {
        while app.application.status().counts.probes == 0 {
            app.application.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(app.application.status().counts.probes, 1);
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Status { .. }
    ));
    let wrong = alter(
        &ping(&node, 2, true),
        &[("genesis_hash", Value::Binary(vec![9; 32]))],
    );
    within(
        client
            .send(&Message::response_ok(&wrong, Default::default()).unwrap())
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), app.events.recv())
            .await
            .is_err()
    );
    stop(app).await;
    within(remote_task).await.unwrap();
}

#[tokio::test]
async fn headers_and_transaction_expansion_are_served_from_actual_storage() {
    let node = Node::new();
    let mut app = start(&node, config(), Default::default()).await;
    let (client, _events, task) = connect(&node, &app).await;
    response(&client, &ping(&node, 2, true)).await;
    let view = node.db.view().unwrap();
    let genesis = view.identity().genesis;
    let by_hash = message(
        Kind::GetHeaderByHash,
        1,
        &[("hash", Value::Binary(genesis.0.to_vec()))],
    );
    let result = response(&client, &by_hash).await;
    assert_eq!(
        body(&result).fields()["hdr"],
        Value::Binary(
            view.require_block(genesis)
                .unwrap()
                .header()
                .encode()
                .unwrap()
        )
    );
    let offchain = message(
        Kind::GetHeaderByHeight,
        2,
        &[
            ("height", Value::Integer(999.into())),
            ("top_hash", Value::Binary(vec![9; 32])),
        ],
    );
    assert_eq!(
        response(&client, &offchain).await.message().fields()["reason"],
        Value::Binary(b"not_on_chain".to_vec())
    );
    let missing = alter(
        &offchain,
        &[("top_hash", Value::Binary(genesis.0.to_vec()))],
    );
    assert_eq!(
        response(&client, &missing).await.message().fields()["reason"],
        Value::Binary(b"header_not_found".to_vec())
    );
    let tx = common::transaction();
    let transactions = message(
        Kind::Transactions,
        1,
        &[("txs", Value::List(vec![Value::Binary(tx.encode())]))],
    );
    within(client.send(&transactions).unwrap()).await.unwrap();
    within(async {
        while node
            .db
            .pool_view()
            .unwrap()
            .transaction(tx.hash())
            .unwrap()
            .is_none()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let expand = message(
        Kind::GetBlockTransactions,
        1,
        &[
            ("hash", Value::Binary(vec![123; 32])),
            (
                "tx_hashes",
                Value::List(vec![
                    Value::Binary(tx.hash().0.to_vec()),
                    Value::Binary(tx.hash().0.to_vec()),
                ]),
            ),
        ],
    );
    let expanded = response(&client, &expand).await;
    assert_eq!(
        body(&expanded).fields()["hash"],
        Value::Binary(vec![123; 32])
    );
    assert_eq!(
        body(&expanded).fields()["txs"],
        Value::List(vec![Value::Binary(tx.encode()), Value::Binary(tx.encode())])
    );
    let missing = alter(
        &expand,
        &[(
            "tx_hashes",
            Value::List(vec![Value::Binary(Hash256([9; 32]).0.to_vec())]),
        )],
    );
    let expected = snapshot::serve(&node.db.pool_view().unwrap(), &missing, Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(
        expected.fields()["reason"],
        Value::Binary(b"tx_missing".to_vec())
    );
    let mut pending = client.request(&missing, None).unwrap();
    within(async {
        while app.application.status().counts.served < 5 {
            app.application.changed().await.unwrap();
        }
    })
    .await;
    // Native error envelopes omit the expansion hash, so the runtime cannot
    // attribute this response to any expansion request. The slot stays pending.
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut pending)
            .await
            .is_err()
    );
    client.shutdown();
    assert!(within(pending).await.is_err());
    drop(view);
    stop(app).await;
    within(task).await.unwrap();
}

#[tokio::test]
async fn successors_and_generations_are_served_over_the_actual_encrypted_application() {
    let node = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (client, _events, task) = connect(&node, &app).await;
    response(&client, &ping(&node, 2, true)).await;
    let view = node.db.view().unwrap();
    let genesis = view.identity().genesis;
    for forward in [true, false] {
        let generation = message(
            Kind::GetGeneration,
            1,
            &[
                ("hash", Value::Binary(genesis.0.to_vec())),
                ("forward", Value::Boolean(forward)),
            ],
        );
        let result = response(&client, &generation).await;
        assert_eq!(body(&result).fields()["forward"], Value::Boolean(forward));
        assert_eq!(body(&result).fields()["micro_blocks"], Value::List(vec![]));
        assert_eq!(
            body(&result).fields()["key_block"],
            Value::Binary(view.require_block(genesis).unwrap().encode().unwrap())
        );
        let missing = alter(&generation, &[("hash", Value::Binary(vec![]))]);
        assert_eq!(
            response(&client, &missing).await.message().fields()["reason"],
            Value::Binary(b"block_not_found".to_vec())
        );
    }
    for version in [1, 2] {
        let mut fields = vec![
            (
                if version == 1 { "hash" } else { "from_hash" },
                Value::Binary(genesis.0.to_vec()),
            ),
            (
                "n",
                Value::Integer(
                    ae_types::Unsigned::from_field_bytes(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                        .unwrap(),
                ),
            ),
        ];
        if version == 2 {
            fields.push(("target_hash", Value::Binary(genesis.0.to_vec())));
        }
        let successors = message(Kind::GetSuccessors, version, &fields);
        let result = response(&client, &successors).await;
        assert_eq!(body(&result).fields()["header_hashes"], Value::List(vec![]));
        if version == 2 {
            let missing = alter(
                &successors,
                &[
                    ("from_hash", Value::Binary(vec![])),
                    ("target_hash", Value::Binary(vec![])),
                ],
            );
            assert_eq!(
                response(&client, &missing).await.message().fields()["reason"],
                Value::Binary(b"not_on_chain".to_vec())
            );
        }
    }
    client.shutdown();
    within(task).await.unwrap();
    stop(app).await;
}

#[tokio::test]
async fn node_info_reports_rust_metadata_and_live_discovery_counts() {
    let node = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (client, _events, task) = connect(&node, &app).await;
    response(&client, &ping(&node, 2, true)).await;
    let request = message(Kind::GetNodeInfo, 1, &[]);
    let result = response(&client, &request).await;
    let fields = body(&result).fields();
    let metadata = peer::info::Metadata::current();
    for (name, value) in [
        ("version", metadata.version),
        ("revision", metadata.revision),
        ("vendor", metadata.vendor),
        ("os", metadata.os),
        (
            "network_id",
            node.db.view().unwrap().identity().network.clone(),
        ),
    ] {
        assert_eq!(fields[name], Value::Binary(value));
    }
    assert_eq!(fields["verified_peers"], Value::Integer(1.into()));
    assert_eq!(fields["unverified_peers"], Value::Integer(0.into()));
    let mut discovered = ae_p2p::discovery::Peer {
        id: PeerPublicKey([91; 32]),
        address: Ipv4Addr::new(127, 0, 0, 7),
        source: Ipv4Addr::LOCALHOST,
        host: b"127.0.0.7".to_vec(),
        port: 2020,
        trusted: false,
    };
    within(
        app.network
            .command(Command::Add(discovered.clone()))
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    let result = response(&client, &request).await;
    assert_eq!(
        body(&result).fields()["verified_peers"],
        Value::Integer(1.into())
    );
    assert_eq!(
        body(&result).fields()["unverified_peers"],
        Value::Integer(1.into())
    );
    within(
        app.network
            .command(Command::Delete(discovered.id))
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    discovered.trusted = true;
    within(
        app.network
            .command(Command::Add(discovered))
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    let result = response(&client, &request).await;
    assert_eq!(
        body(&result).fields()["verified_peers"],
        Value::Integer(2.into())
    );
    assert_eq!(
        body(&result).fields()["unverified_peers"],
        Value::Integer(0.into())
    );
    assert_eq!(app.network.status().policy.unwrap().counts.inbound, 1);
    assert_eq!(app.network.status().policy.unwrap().counts.outbound, 0);
    client.shutdown();
    within(task).await.unwrap();
    stop(app).await;
}

#[tokio::test]
async fn disabled_node_info_serves_other_requests_until_native_timeout_closes_connection() {
    let node = Node::new();
    let mut app = start(
        &node,
        config(),
        peer::Config {
            node_info: None,
            ..Default::default()
        },
    )
    .await;
    let (client, _events, task) = connect(&node, &app).await;
    response(&client, &ping(&node, 2, true)).await;
    let request = message(Kind::GetNodeInfo, 1, &[]);
    let pending = client
        .request(&request, Some(Duration::from_millis(100)))
        .unwrap();
    let header = message(
        Kind::GetHeaderByHash,
        1,
        &[(
            "hash",
            Value::Binary(node.db.view().unwrap().identity().genesis.0.to_vec()),
        )],
    );
    assert_eq!(body(&response(&client, &header).await).kind(), Kind::Header);
    assert!(!client.is_closed());
    assert!(matches!(
        within(pending).await,
        Err(net::Error::RequestFailed(
            ae_p2p::requests::Failure::Timeout
        ))
    ));
    within(task).await.unwrap();
    assert!(client.is_closed());
    within(async {
        while app.application.status().counts.served < 1 {
            app.application.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(app.application.status().counts.served, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), app.events.recv())
            .await
            .is_err()
    );
    stop(app).await;
}

#[tokio::test]
async fn encrypted_generation_download_commits_actual_ledger_and_rejects_foreign_owner() {
    use ae_sync::chain::{Config, Direction, Error, Importer, Outcome};
    let node = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let _ = response(&remote, &ping(&node, 1, false)).await;
    let client = within(async {
        loop {
            let clients = app.network.peers().peers().unwrap().wait().await.unwrap();
            if let Some(client) = clients
                .into_iter()
                .find(|c| c.peer() == identities().1.public_key())
            {
                break client;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let all = common::ledger_fixtures::blocks(&NetworkProfile::mainnet());
    let key = &all[0];
    let hash = key.hash().unwrap();
    let rules = common::ledger_fixtures::rules(node.db.view().unwrap().identity().genesis);
    let importer = Importer::new(
        node.db.clone(),
        common::ledger_fixtures::execution(&NetworkProfile::mainnet()),
        rules.clone(),
        Config::default(),
    )
    .unwrap();
    let other = Node::new();
    let foreign = Importer::new(
        other.db.clone(),
        common::ledger_fixtures::execution(&NetworkProfile::mainnet()),
        rules,
        Config::default(),
    )
    .unwrap();
    assert_eq!(
        foreign
            .fetch(
                &app.network,
                client.clone(),
                hash,
                Direction::Backward,
                u64::MAX - 600_000
            )
            .await
            .unwrap_err()
            .error,
        Error::Configuration
    );
    foreign.stop();
    foreign.join().await;
    let fetch = importer.fetch(
        &app.network,
        client.clone(),
        hash,
        Direction::Backward,
        u64::MAX - 600_000,
    );
    let serve = async {
        loop {
            let request = incoming(&mut events).await;
            if request.message().kind() == Kind::GetGeneration {
                assert_eq!(
                    request.message().fields()["hash"],
                    Value::Binary(hash.0.to_vec())
                );
                let body = common::import::message(key, &[], Direction::Backward);
                remote
                    .send(&Message::response_ok(&body, Default::default()).unwrap())
                    .unwrap()
                    .await
                    .unwrap();
                break;
            }
        }
    };
    let (report, ()) = within(async { tokio::join!(fetch, serve) }).await;
    let report = report.unwrap();
    assert_eq!(report.accepted.len(), 1);
    assert!(matches!(
        report.accepted[0].outcome,
        Outcome::Committed { .. }
    ));
    assert_eq!(node.db.view().unwrap().require_block(hash).unwrap(), *key);
    remote.shutdown();
    within(driver).await.unwrap();
    within(client.monitor().wait()).await;
    assert_eq!(
        importer
            .fetch(
                &app.network,
                client,
                hash,
                Direction::Backward,
                u64::MAX - 600_000
            )
            .await
            .unwrap_err()
            .error,
        Error::Stale
    );
    importer.stop();
    importer.join().await;
    stop(app).await;
}

async fn admitted_client(app: &App, peer: PeerPublicKey) -> net::Client {
    within(async {
        loop {
            let clients = app.network.peers().peers().unwrap().wait().await.unwrap();
            if let Some(client) = clients.into_iter().find(|c| c.peer() == peer) {
                return client;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
}
async fn generation_request(events: &mut net::Events) -> net::Received {
    loop {
        let request = incoming(events).await;
        if request.message().kind() == Kind::GetGeneration {
            return request;
        }
    }
}
fn download_importer(node: &Node, config: ae_sync::chain::Config) -> ae_sync::chain::Importer {
    ae_sync::chain::Importer::new(
        node.db.clone(),
        common::ledger_fixtures::execution(&NetworkProfile::mainnet()),
        common::ledger_fixtures::rules(node.db.view().unwrap().identity().genesis),
        config,
    )
    .unwrap()
}
#[tokio::test]
async fn generation_downloads_reserve_shared_slots_and_bytes_before_any_wire_request() {
    use ae_sync::chain::{Config, Direction, Error, Outcome};
    let limits = message::Limits::default();
    let weight = limits.body_bytes + limits.work_bytes;
    // Exhaust slots with bytes remaining, then bytes with a slot remaining.
    for (jobs, bytes) in [(1, weight * 2), (2, weight)] {
        let node = Node::new();
        let app = start(&node, config(), Default::default()).await;
        let (remote, mut events, driver) = connect(&node, &app).await;
        let _ = response(&remote, &ping(&node, 1, false)).await;
        let client = admitted_client(&app, remote_peer()).await;
        let second_id = noise::Identity::from_secret([9; 32]);
        let socket = TcpStream::connect(app.network.status().listening.unwrap())
            .await
            .unwrap();
        let (second, mut second_events, second_driver) = establish(
            &node,
            socket,
            &second_id,
            noise::Role::Initiator(app.identity.public_key()),
        )
        .await;
        let _ = response(&second, &ping(&node, 1, false)).await;
        let second_client = admitted_client(&app, second_id.public_key()).await;
        let importer = download_importer(
            &node,
            Config {
                jobs,
                bytes,
                ..Default::default()
            },
        );
        let all = common::ledger_fixtures::blocks(&NetworkProfile::mainnet());
        let key = &all[0];
        let hash = key.hash().unwrap();
        let mut fetch = Box::pin(importer.fetch(
            &app.network,
            client.clone(),
            hash,
            Direction::Backward,
            u64::MAX - 600_000,
        ));
        tokio::select! {
            result = &mut fetch => panic!("response not supplied: {result:?}"),
            _ = generation_request(&mut events) => (),
        }
        assert_eq!(importer.available(), (jobs - 1, bytes - weight));
        let error = within(importer.fetch(
            &app.network,
            second_client.clone(),
            hash,
            Direction::Backward,
            u64::MAX - 600_000,
        ))
        .await
        .unwrap_err();
        assert_eq!(error.error, Error::Busy);
        assert!(error.progress.unwrap().accepted.is_empty());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                generation_request(&mut second_events)
            )
            .await
            .is_err()
        );
        assert!(!second_client.is_closed());
        let packet = common::import::packet(key, &[], Direction::Backward);
        assert!(matches!(
            importer.import(
                packet.clone(),
                hash,
                Direction::Backward,
                u64::MAX - 600_000
            ),
            Err(Error::Busy)
        ));
        let body = common::import::message(key, &[], Direction::Backward);
        remote
            .send(&Message::response_ok(&body, Default::default()).unwrap())
            .unwrap()
            .await
            .unwrap();
        assert!(matches!(
            within(fetch).await.unwrap().accepted[0].outcome,
            Outcome::Committed { .. }
        ));
        assert_eq!(importer.available(), (jobs, bytes));
        // An unread offline result is part of the very same budget.
        let unread = importer
            .import(packet, hash, Direction::Backward, u64::MAX - 600_000)
            .unwrap();
        assert_eq!(
            within(importer.fetch(
                &app.network,
                second_client,
                hash,
                Direction::Backward,
                u64::MAX - 600_000
            ))
            .await
            .unwrap_err()
            .error,
            Error::Busy
        );
        assert_eq!(
            unread.wait().await.unwrap().accepted[0].outcome,
            Outcome::Present
        );
        assert_eq!(importer.available(), (jobs, bytes));
        importer.stop();
        importer.join().await;
        remote.shutdown();
        second.shutdown();
        within(driver).await.unwrap();
        within(second_driver).await.unwrap();
        stop(app).await;
    }
}
fn remote_peer() -> PeerPublicKey {
    identities().1.public_key()
}

#[tokio::test]
async fn pending_generation_downloads_release_capacity_on_shutdown_cancellation_and_timeout() {
    use ae_sync::chain::{Config, Direction, Error};
    for mode in ["shutdown", "cancel", "timeout"] {
        let node = Node::new();
        let app = start(&node, config(), Default::default()).await;
        let (remote, mut events, driver) = connect(&node, &app).await;
        let _ = response(&remote, &ping(&node, 1, false)).await;
        let client = admitted_client(&app, remote_peer()).await;
        let limits = Config {
            jobs: 1,
            timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let available = (limits.jobs, limits.bytes);
        let importer = download_importer(&node, limits);
        let before = node.db.view().unwrap().status();
        let hash = common::ledger_fixtures::blocks(&NetworkProfile::mainnet())[0]
            .hash()
            .unwrap();
        let mut fetch = Box::pin(importer.fetch(
            &app.network,
            client.clone(),
            hash,
            Direction::Backward,
            u64::MAX - 600_000,
        ));
        tokio::select! {
            result = &mut fetch => panic!("response not supplied: {result:?}"),
            _ = generation_request(&mut events) => (),
        }
        assert_eq!(importer.available().0, 0);
        if mode == "cancel" {
            drop(fetch);
        } else {
            if mode == "shutdown" {
                importer.stop();
            }
            let failure = within(fetch).await.unwrap_err();
            assert_eq!(
                failure.error,
                if mode == "shutdown" {
                    Error::Closed
                } else {
                    Error::Timeout
                }
            );
            if mode == "shutdown" {
                assert!(failure.progress.unwrap().accepted.is_empty());
            }
        }
        within(client.monitor().wait()).await;
        within(driver).await.unwrap();
        assert_eq!(importer.available(), available);
        assert_eq!(node.db.view().unwrap().status(), before);
        importer.stop();
        importer.join().await;
        stop(app).await;
    }
}

#[tokio::test]
async fn generation_download_limits_refusals_and_validation_failures_return_capacity() {
    use ae_sync::chain::{Config, Direction, Error};
    let node = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let _ = response(&remote, &ping(&node, 1, false)).await;
    let client = admitted_client(&app, remote_peer()).await;
    let all = common::ledger_fixtures::blocks(&NetworkProfile::mainnet());
    let key = &all[0];
    let hash = key.hash().unwrap();
    for field in 0..4 {
        let mut limits = Config::default();
        match field {
            0 => limits.messages.body_bytes -= 1,
            1 => limits.messages.work_bytes -= 1,
            2 => limits.messages.items -= 1,
            _ => limits.messages.depth -= 1,
        }
        let available = (limits.jobs, limits.bytes);
        let importer = download_importer(&node, limits);
        assert_eq!(
            importer
                .fetch(
                    &app.network,
                    client.clone(),
                    hash,
                    Direction::Backward,
                    u64::MAX - 600_000
                )
                .await
                .unwrap_err()
                .error,
            Error::Configuration
        );
        assert_eq!(importer.available(), available);
        importer.stop();
        importer.join().await;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), generation_request(&mut events))
            .await
            .is_err()
    );
    assert!(!client.is_closed());
    let limits = Config {
        jobs: 1,
        ..Default::default()
    };
    let available = (limits.jobs, limits.bytes);
    let importer = download_importer(&node, limits);
    for case in ["refused", "invalid", "valid"] {
        let fetch = importer.fetch(
            &app.network,
            client.clone(),
            hash,
            Direction::Backward,
            u64::MAX - 600_000,
        );
        let serve = async {
            let _ = generation_request(&mut events).await;
            let response = match case {
                "refused" => Message::response_error(
                    Kind::Generation,
                    b"not_found".to_vec(),
                    Default::default(),
                )
                .unwrap(),
                "invalid" => {
                    let body = common::import::message(key, &[], Direction::Forward);
                    Message::response_ok(&body, Default::default()).unwrap()
                }
                _ => Message::response_ok(
                    &common::import::message(key, &[], Direction::Backward),
                    Default::default(),
                )
                .unwrap(),
            };
            remote.send(&response).unwrap().await.unwrap();
        };
        let (result, ()) = within(async { tokio::join!(fetch, serve) }).await;
        match case {
            "refused" => assert_eq!(
                result.unwrap_err().error,
                Error::Refused(b"not_found".to_vec())
            ),
            "invalid" => assert!(matches!(result.unwrap_err().error, Error::Generation(_))),
            _ => assert_eq!(result.unwrap().accepted.len(), 1),
        }
        assert_eq!(importer.available(), available);
    }
    assert_eq!(node.db.view().unwrap().require_block(hash).unwrap(), *key);
    importer.stop();
    importer.join().await;
    remote.shutdown();
    within(driver).await.unwrap();
    stop(app).await;
}

#[tokio::test]
async fn request_queued_before_first_ping_can_reply_while_negotiation_is_deferred() {
    let node = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (remote, _events, task) = connect(&node, &app).await;
    let info = remote
        .request(&message(Kind::GetNodeInfo, 1, &[]), None)
        .unwrap();
    let pool = remote
        .request(&message(Kind::PoolInit, 1, &[]), None)
        .unwrap();
    let first_ping = remote.request(&ping(&node, 2, true), None).unwrap();
    let (info, pool, first_ping) = within(async { tokio::join!(info, pool, first_ping) }).await;
    let net::Reply::Response(info) = info.unwrap() else {
        panic!()
    };
    let net::Reply::Response(first_ping) = first_ping.unwrap() else {
        panic!()
    };
    let net::Reply::Response(pool) = pool.unwrap() else {
        panic!()
    };
    assert_eq!(body(&info).kind(), Kind::NodeInfo);
    assert_eq!(body(&pool).kind(), Kind::PoolInit);
    assert_eq!(body(&first_ping).kind(), Kind::Ping);
    assert!(!remote.is_closed());
    assert_eq!(remote.selected_ping_version(), Some(2));
    stop(app).await;
    within(task).await.unwrap();
}

#[tokio::test]
async fn incompatible_ping_negotiation_preserves_application_effects_and_response_before_close() {
    let node = Node::new();
    let mut app = start(&node, config(), Default::default()).await;
    let (remote, _events, task) = connect(&node, &app).await;
    let versions = |values| {
        Value::List(vec![Value::Record(std::collections::BTreeMap::from([
            ("protocol".into(), Value::Binary(b"ping".to_vec())),
            ("vsns".into(), Value::List(values)),
        ]))])
    };
    let initial = alter(
        &ping(&node, 2, true),
        &[("versions", versions(vec![Value::Integer(1.into())]))],
    );
    assert_eq!(body(&response(&remote, &initial).await).version(), 2);
    // Node-info completion establishes that initial negotiation has finished.
    assert_eq!(
        body(&response(&remote, &message(Kind::GetNodeInfo, 1, &[])).await).kind(),
        Kind::NodeInfo
    );
    let advertised = cap(12);
    let incompatible = alter(
        &ping(&node, 2, true),
        &[
            ("versions", versions(vec![])),
            (
                "capabilities",
                Value::Binary(
                    message::encode_capabilities(
                        std::slice::from_ref(&advertised),
                        Default::default(),
                    )
                    .unwrap(),
                ),
            ),
        ],
    );
    let answered = response(&remote, &incompatible).await;
    assert_eq!(body(&answered).kind(), Kind::Ping);
    assert_eq!(body(&answered).version(), 2);
    let status = observed(&mut app.events).await;
    let peer::EventKind::Status { client, .. } = status.kind() else {
        panic!()
    };
    assert_eq!(client.selected_ping_version(), Some(1));
    assert!(
        matches!(observed(&mut app.events).await.kind(), peer::EventKind::Capabilities { client: source, chain_poi } if source.generation() == client.generation() && *chain_poi == advertised)
    );
    assert_eq!(
        within(client.monitor().wait()).await.reason,
        net::Error::Request(ae_p2p::requests::Error::NoCommonPingVersion)
    );
    within(task).await.unwrap();
    assert!(app.application.status().running);
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    stop(app).await;
}

#[tokio::test]
async fn scheduled_ping_caller_completes_before_incompatible_initial_v2_retires_connection() {
    let node = Node::new();
    let mut c = config();
    c.transport.runtime.minimum_ping = 2;
    c.registry.policy.maximum_outbound = 1;
    c.registry.policy.ping_interval = 60_000;
    let mut app = start(&node, c, Default::default()).await;
    let (_, remote) = identities();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let info = ae_p2p::discovery::Peer {
        id: remote.public_key(),
        address: Ipv4Addr::LOCALHOST,
        source: Ipv4Addr::LOCALHOST,
        host: b"127.0.0.1".to_vec(),
        port: listener.local_addr().unwrap().port(),
        trusted: false,
    };
    app.network
        .command(Command::Add(info.clone()))
        .unwrap()
        .wait()
        .await
        .unwrap();
    app.network
        .command(Command::Connect(Some(info.id)))
        .unwrap()
        .wait()
        .await
        .unwrap();
    let (socket, _) = within(listener.accept()).await.unwrap();
    let (client, mut events, remote_task) =
        establish(&node, socket, &remote, noise::Role::Responder).await;
    assert_eq!(incoming(&mut events).await.message().version(), 2);
    let incompatible = alter(
        &ping(&node, 2, true),
        &[(
            "versions",
            Value::List(vec![Value::Record(std::collections::BTreeMap::from([
                ("protocol".into(), Value::Binary(b"ping".to_vec())),
                ("vsns".into(), Value::List(vec![])),
            ]))]),
        )],
    );
    within(
        client
            .send(&Message::response_ok(&incompatible, Default::default()).unwrap())
            .unwrap(),
    )
    .await
    .unwrap();
    within(remote_task).await.unwrap();
    within(async {
        while app.network.status().counts.pings_completed == 0 {
            app.network.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(app.network.status().counts.pings_completed, 1);
    assert_eq!(app.network.status().counts.pings_failed, 0);
    assert!(app.application.status().running);
    stop(app).await;
}

#[tokio::test]
async fn v2_minimum_admits_and_serves_legacy_ping_then_advertises_only_v2() {
    let node = Node::new();
    let mut c = config();
    c.transport.runtime.minimum_ping = 2;
    let mut app = start(&node, c, Default::default()).await;
    let (remote, _events, task) = connect(&node, &app).await;
    let legacy = response(&remote, &ping(&node, 1, true)).await;
    assert_eq!(body(&legacy).version(), 1);
    let status = observed(&mut app.events).await;
    let peer::EventKind::Status { client, .. } = status.kind() else {
        panic!()
    };
    assert_eq!(client.minimum_ping_version(), 2);
    assert_eq!(client.ping_version(), 2);
    assert_eq!(client.selected_ping_version(), None);
    assert!(!client.is_closed());
    let info = response(&remote, &message(Kind::GetNodeInfo, 1, &[])).await;
    assert_eq!(body(&info).kind(), Kind::NodeInfo);
    let upgraded = response(&remote, &ping(&node, 2, true)).await;
    assert_eq!(body(&upgraded).version(), 2);
    assert_eq!(
        body(&upgraded).fields()["versions"],
        Value::List(vec![Value::Record(std::collections::BTreeMap::from([
            ("protocol".into(), Value::Binary(b"ping".to_vec())),
            ("vsns".into(), Value::List(vec![Value::Integer(2.into())])),
        ]))])
    );
    assert_eq!(client.selected_ping_version(), Some(2));
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    stop(app).await;
    within(task).await.unwrap();
}

#[tokio::test]
async fn matching_top_schedules_real_pool_sync_and_emits_generation_intent_in_order() {
    let node = Node::new();
    let mut app = start(&node, config(), Default::default()).await;
    let (client, mut events, task) = connect(&node, &app).await;
    assert_eq!(
        body(&response(&client, &ping(&node, 1, false)).await).kind(),
        Kind::Ping
    );
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Status { .. }
    ));
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Chain {
            intent: peer::ChainIntent::Generation(_),
            ..
        }
    ));
    assert!(matches!(
        observed(&mut app.events).await.kind(),
        peer::EventKind::Chain {
            intent: peer::ChainIntent::Done,
            ..
        }
    ));
    assert_eq!(incoming(&mut events).await.message().kind(), Kind::PoolInit);
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    stop(app).await;
    within(task).await.unwrap();
}

#[tokio::test]
async fn output_capacity_and_abrupt_application_drop_close_owned_connections() {
    for abort in [false, true] {
        let node = Node::new();
        let p = peer::Config {
            events: 1,
            ..Default::default()
        };
        let app = start(&node, config(), p).await;
        let (client, _events, task) = connect(&node, &app).await;
        response(&client, &ping(&node, 2, true)).await;
        if abort {
            app.application_task.abort();
        } else {
            response(&client, &ping(&node, 2, true)).await;
            assert!(
                within(client.request(&ping(&node, 2, true), None).unwrap())
                    .await
                    .is_err()
            );
        }
        within(task).await.unwrap();
        if abort {
            assert!(
                within(app.application_task)
                    .await
                    .unwrap_err()
                    .is_cancelled()
            );
            app.gossip.shutdown();
            app.network.shutdown();
            within(app.gossip_task).await.unwrap();
            within(app.network_task).await.unwrap();
        } else {
            stop(app).await;
        }
    }
}

#[path = "peer_runtime/chain_sync.rs"]
mod chain_sync;
#[path = "peer_runtime/shared_sync.rs"]
mod shared_sync;

#[path = "peer_runtime/staged_download.rs"]
mod staged_download;

#[path = "peer_runtime/block_gossip.rs"]
mod block_gossip;

#[path = "peer_runtime/lifecycle.rs"]
mod lifecycle;

#[path = "peer_runtime/capabilities.rs"]
mod capabilities;

#[path = "peer_runtime/closing.rs"]
mod closing;
#[path = "peer_runtime/preping.rs"]
mod preping;

#[path = "peer_runtime/maintenance.rs"]
mod maintenance;
#[path = "peer_runtime/observer.rs"]
mod observer;

#[path = "peer_runtime/conductor.rs"]
mod conductor;

#[path = "peer_runtime/cache_owner.rs"]
mod cache_owner;

#[path = "peer_runtime/key_owner.rs"]
mod key_owner;

#[path = "peer_runtime/routing_owner.rs"]
mod routing_owner;

#[path = "peer_runtime/analytics.rs"]
mod analytics;
