mod common;
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
async fn application_rejects_a_different_database_and_stop_before_run_joins_readers() {
    let node = Node::new();
    let other = Node::new();
    for wrong in [true, false] {
        let (identity, _) = identities();
        let (network, scheduled, pool, network_service) = network::create(
            node.db.clone(),
            node.client(),
            identity,
            NetworkProfile::mainnet(),
            config(),
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
        let result = peer::create(
            if wrong {
                other.db.clone()
            } else {
                node.db.clone()
            },
            network.clone(),
            scheduled,
            incoming,
            Default::default(),
        );
        if wrong {
            assert!(matches!(result, Err(peer::Error::Configuration)));
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
