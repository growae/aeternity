use super::*;
use ae_sync::peer::capabilities::CHAIN_POI;

// A raw encrypted peer deliberately remains alive after receiving Close.
// The normal runtime correctly terminates then, hiding the grace callbacks.
struct Raw {
    socket: TcpStream,
    crypto: noise::Transport,
}
async fn write(socket: &TcpStream, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        socket.writable().await.unwrap();
        match socket.try_write(bytes) {
            Ok(0) => panic!("closed writer"),
            Ok(n) => bytes = &bytes[n..],
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
            Err(e) => panic!("{e}"),
        }
    }
}
async fn read(socket: &TcpStream, mut bytes: &mut [u8]) -> bool {
    while !bytes.is_empty() {
        socket.readable().await.unwrap();
        match socket.try_read(bytes) {
            Ok(0) => return false,
            Ok(n) => bytes = &mut bytes[n..],
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
            Err(e) => panic!("{e}"),
        }
    }
    true
}
async fn record(socket: &TcpStream) -> Option<Vec<u8>> {
    let mut prefix = [0; 2];
    if !read(socket, &mut prefix).await {
        return None;
    }
    let mut bytes = vec![0; usize::from(u16::from_be_bytes(prefix))];
    assert!(read(socket, &mut bytes).await);
    Some(bytes)
}
impl Raw {
    async fn connect(node: &Node, app: &App) -> Self {
        let socket = TcpStream::connect(app.network.status().listening.unwrap())
            .await
            .unwrap();
        Self::establish(
            node,
            socket,
            noise::Role::Initiator(app.identity.public_key()),
        )
        .await
    }
    async fn establish(node: &Node, socket: TcpStream, role: noise::Role) -> Self {
        let binding = node.db.view().unwrap().identity().clone();
        let network = noise::Network::new(1, binding.genesis, &binding.network).unwrap();
        let mut handshake = noise::Handshake::new(&identities().1, &network, role).unwrap();
        within(async {
            while !handshake.is_finished() {
                if handshake.is_my_turn() {
                    write(
                        &socket,
                        &noise::record::encode(&handshake.write().unwrap()).unwrap(),
                    )
                    .await;
                } else {
                    handshake.read(&record(&socket).await.unwrap()).unwrap();
                }
            }
        })
        .await;
        Self {
            socket,
            crypto: handshake.finish().unwrap(),
        }
    }
    async fn send(&mut self, messages: &[Message]) {
        let mut bytes = vec![];
        for message in messages {
            for packet in ae_p2p::framing::fragment(
                &message.encode_packet(Default::default()).unwrap(),
                Default::default(),
            )
            .unwrap()
            {
                bytes.extend(noise::record::encode(&self.crypto.seal(&packet).unwrap()).unwrap());
            }
        }
        within(write(&self.socket, &bytes)).await;
    }
    async fn receive(&mut self) -> Option<Message> {
        let mut assembler = ae_p2p::framing::Assembler::new(Default::default());
        loop {
            let ciphertext = record(&self.socket).await?;
            if let Some(packet) = assembler
                .push(&self.crypto.open(&ciphertext).unwrap())
                .unwrap()
            {
                return Some(Message::decode_packet(&packet, Default::default()).unwrap());
            }
        }
    }
    async fn next(&mut self) -> Message {
        within(self.receive()).await.expect("encrypted message")
    }
    async fn quiet(&mut self) {
        assert!(
            tokio::time::timeout(Duration::from_millis(25), self.receive())
                .await
                .is_err()
        );
    }
}
fn response_body(response: &Message) -> &Message {
    let Details::Response {
        message: Some(Ok(body)),
        ..
    } = response.details()
    else {
        panic!("{response:?}");
    };
    body
}
async fn local(app: &mut App) -> net::Client {
    within(async {
        loop {
            if let peer::EventKind::Status { client, .. } = app.events.recv().await.unwrap().kind()
            {
                return client.clone();
            }
        }
    })
    .await
}
async fn closing(app: &App, client: &net::Client) {
    let info = within(app.network.connection(client.clone()).unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    assert!(info.admitted && info.closing);
    assert!(!client.is_closed());
    assert!(
        within(app.network.peers().peers().unwrap().wait())
            .await
            .unwrap()
            .iter()
            .all(|peer| peer.generation() != client.generation())
    );
}
async fn served(app: &mut App, minimum: u64) {
    within(async {
        while app.application.status().counts.served < minimum {
            app.application.changed().await.unwrap();
        }
    })
    .await;
}
async fn acknowledge(raw: &mut Raw, client: &net::Client) {
    raw.send(&[message(Kind::Close, 1, &[])]).await;
    assert_eq!(
        within(client.monitor().wait()).await.reason,
        net::Error::LocalClose
    );
    assert!(within(raw.receive()).await.is_none());
}

#[tokio::test]
async fn temporary_ping_retires_registry_before_close_and_serves_coalesced_grace_callbacks() {
    let node = Node::new();
    let mut c = config();
    c.registry.policy.maximum_inbound = 0;
    let mut app = start(&node, c, Default::default()).await;
    let mut raw = Raw::connect(&node, &app).await;
    let tx = common::transaction();
    raw.send(&[
        ping(&node, 1, true),
        message(Kind::GetNodeInfo, 1, &[]),
        message(
            Kind::GetHeaderByHeight,
            1,
            &[("height", Value::Integer(0.into()))],
        ),
        message(Kind::PoolInit, 1, &[]),
        message(
            Kind::Transactions,
            1,
            &[("txs", Value::List(vec![Value::Binary(tx.encode())]))],
        ),
    ])
    .await;
    let client = local(&mut app).await;
    let reply = raw.next().await;
    assert_eq!(
        response_body(&reply).fields()["sync_allowed"],
        Value::Boolean(false)
    );
    assert_eq!(raw.next().await.kind(), Kind::Close);
    closing(&app, &client).await;
    assert!(
        matches!(within(app.network.command(Command::SampleConnected { maximum: 10, direction: None }).unwrap().wait()).await.unwrap(), ae_p2p::registry::Outcome::Peers(peers) if peers.is_empty())
    );
    served(&mut app, 2).await;
    within(async {
        while app.gossip.status().counts.ordinary_admitted == 0 {
            app.gossip.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(
        node.db.pool_view().unwrap().members(10).unwrap(),
        vec![tx.hash()]
    );
    // Another ping is processed while closing, with no response or second Close.
    raw.send(&[ping(&node, 2, true), message(Kind::GetNodeInfo, 1, &[])])
        .await;
    served(&mut app, 3).await;
    raw.quiet().await;
    assert!(app.network.status().failure.is_none());
    acknowledge(&mut raw, &client).await;
    stop(app).await;
}

#[tokio::test]
async fn temporary_rejection_closes_after_response_but_negotiation_fault_remains_abrupt() {
    for incompatible in [false, true] {
        let node = Node::new();
        let mut c = config();
        c.registry.policy.maximum_inbound = 0;
        let mut app = start(&node, c, Default::default()).await;
        let mut raw = Raw::connect(&node, &app).await;
        let request = if incompatible {
            alter(
                &ping(&node, 2, true),
                &[(
                    "versions",
                    Value::List(vec![Value::Record(std::collections::BTreeMap::from([
                        ("protocol".into(), Value::Binary(b"ping".to_vec())),
                        ("vsns".into(), Value::List(vec![])),
                    ]))]),
                )],
            )
        } else {
            alter(
                &ping(&node, 1, true),
                &[("genesis_hash", Value::Binary(vec![9; 32]))],
            )
        };
        raw.send(&[request]).await;
        let response = raw.next().await;
        assert_eq!(response.kind(), Kind::Response);
        if incompatible {
            assert_eq!(response_body(&response).kind(), Kind::Ping);
        } else {
            assert!(matches!(
                response.details(),
                Details::Response { message: None, .. }
            ));
            assert_eq!(raw.next().await.kind(), Kind::Close);
            raw.send(&[message(Kind::Close, 1, &[])]).await;
        }
        assert!(within(raw.receive()).await.is_none());
        within(async {
            while app.network.status().counts.closed == 0 {
                app.network.changed().await.unwrap();
            }
        })
        .await;
        assert_eq!(app.network.status().counts.accepted, 1);
        stop(app).await;
    }
}

#[tokio::test]
async fn policy_and_explicit_disconnect_keep_exact_metadata_and_callbacks_until_terminal() {
    for mode in 0..3 {
        let node = Node::new();
        let mut app = start(&node, config(), Default::default()).await;
        let mut raw = Raw::connect(&node, &app).await;
        raw.send(&[ping(&node, 1, true)]).await;
        assert_eq!(response_body(&raw.next().await).kind(), Kind::Ping);
        let client = local(&mut app).await;
        let ticket = match mode {
            0 => app.network.command(Command::Delete(client.peer())).unwrap(),
            1 => app
                .network
                .command(Command::Block(Info {
                    id: client.peer(),
                    host: b"127.0.0.1".to_vec(),
                    port: 4321,
                }))
                .unwrap(),
            _ => app.network.disconnect(client.clone()).unwrap(),
        };
        within(ticket.wait()).await.unwrap();
        assert_eq!(raw.next().await.kind(), Kind::Close);
        closing(&app, &client).await;
        raw.send(&[
            message(Kind::GetNodeInfo, 1, &[]),
            ping(&node, 2, true),
            message(
                Kind::GetHeaderByHeight,
                1,
                &[("height", Value::Integer(0.into()))],
            ),
        ])
        .await;
        served(&mut app, 2).await;
        raw.quiet().await;
        acknowledge(&mut raw, &client).await;
        stop(app).await;
    }
}

fn capabilities(node: &Node, n: u64, port: u16) -> Message {
    alter(
        &ping(node, 2, true),
        &[
            ("port", Value::Integer(u64::from(port).into())),
            (
                "capabilities",
                Value::Binary(message::encode_capabilities(&[cap(n)], Default::default()).unwrap()),
            ),
        ],
    )
}

#[tokio::test]
async fn crossing_scheduled_ping_response_finishes_after_policy_cancels_its_timer_owner() {
    let node = Node::new();
    let mut c = config();
    c.registry.policy.maximum_outbound = 1;
    let mut app = start(&node, c, Default::default()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = ae_p2p::discovery::Peer {
        id: identities().1.public_key(),
        address: Ipv4Addr::LOCALHOST,
        source: Ipv4Addr::LOCALHOST,
        host: b"127.0.0.1".to_vec(),
        port: listener.local_addr().unwrap().port(),
        trusted: false,
    };
    within(
        app.network
            .command(Command::Add(peer.clone()))
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    within(
        app.network
            .command(Command::Connect(Some(peer.id)))
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    let (socket, _) = within(listener.accept()).await.unwrap();
    let mut raw = Raw::establish(&node, socket, noise::Role::Responder).await;
    let scheduled = raw.next().await;
    assert_eq!((scheduled.kind(), scheduled.version()), (Kind::Ping, 1));
    let client = within(app.network.peers().peers().unwrap().wait())
        .await
        .unwrap()
        .pop()
        .unwrap();
    within(
        app.network
            .command(Command::Delete(peer.id))
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    assert_eq!(raw.next().await.kind(), Kind::Close);
    closing(&app, &client).await;
    raw.send(&[
        Message::response_ok(&ping(&node, 1, true), Default::default()).unwrap(),
        message(Kind::GetNodeInfo, 1, &[]),
    ])
    .await;
    served(&mut app, 1).await;
    assert_eq!(app.application.status().counts.probes, 1);
    raw.send(&[
        Message::response_ok(&ping(&node, 2, true), Default::default()).unwrap(),
        message(Kind::GetNodeInfo, 1, &[]),
    ])
    .await;
    served(&mut app, 2).await;
    assert_eq!(client.selected_ping_version(), Some(2));
    assert_eq!(app.network.status().counts.pings_completed, 0);
    raw.quiet().await;
    acknowledge(&mut raw, &client).await;
    stop(app).await;
}

#[tokio::test]
async fn same_key_sync_replacement_waits_for_crossing_reply_without_cancelling_old_transport() {
    let node = Node::new();
    let source = Node::new();
    chain_sync::seed(&source, usize::MAX).await;
    let app = start(&node, config(), Default::default()).await;
    let network = app.network.clone();
    let mut old = Raw::connect(&node, &app).await;
    let mut new = Raw::connect(&node, &app).await;
    let (mut sync, _events, finish) = chain_sync::synchronize(&node, app, Default::default());
    old.send(&[ping(&source, 1, false)]).await;
    assert_eq!(response_body(&old.next().await).kind(), Kind::Ping);
    assert_eq!(old.next().await.kind(), Kind::GetHeaderByHash);
    let old_client = within(network.peers().peers().unwrap().wait())
        .await
        .unwrap()
        .pop()
        .unwrap();
    let announce = alter(
        &ping(&source, 1, false),
        &[("port", Value::Integer(4322.into()))],
    );
    new.send(&[announce.clone(), announce]).await;
    assert_eq!(response_body(&new.next().await).kind(), Kind::Ping);
    assert_eq!(response_body(&new.next().await).kind(), Kind::Ping);
    assert_eq!(old.next().await.kind(), Kind::Close);
    within(async {
        while sync.status().queued != 1 || sync.status().counts.coalesced == 0 {
            sync.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(sync.status().active.len(), 1);
    assert_eq!(&sync.status().active[0].generation, old_client.generation());
    assert!(old_client.monitor().exit().is_none());
    old.send(&[Message::response_error(
        Kind::Header,
        b"block_not_found".to_vec(),
        Default::default(),
    )
    .unwrap()])
        .await;
    within(async {
        while sync.status().counts.finished == 0 {
            tokio::select! {
                _ = sync.changed() => (),
                request = new.receive() => {
                    let request = request.unwrap();
                    if let Some(reply) = snapshot::serve(&source.db.pool_view().unwrap(), &request, Default::default()).unwrap() {
                        new.send(&[reply]).await;
                    }
                }
            }
        }
    }).await;
    let counts = sync.status().counts;
    assert_eq!(
        (
            counts.replaced,
            counts.cancelled,
            counts.failed,
            counts.finished
        ),
        (1, 0, 1, 1)
    );
    assert_eq!(
        node.db.view().unwrap().status().tip,
        source.db.view().unwrap().status().tip
    );
    assert!(old_client.monitor().exit().is_none());
    old.quiet().await;
    acknowledge(&mut old, &old_client).await;
    sync.shutdown();
    assert!(within(finish).await.joined);
}
#[tokio::test]
async fn old_closing_callbacks_and_cleanup_do_not_replace_new_generation_capabilities() {
    let node = Node::new();
    let mut app = start(&node, config(), Default::default()).await;
    let mut old = Raw::connect(&node, &app).await;
    old.send(&[ping(&node, 1, true)]).await;
    old.next().await;
    let old_client = local(&mut app).await;
    old.send(&[capabilities(&node, 7, 4321)]).await;
    old.next().await;
    old.send(&[capabilities(&node, 7, 4321)]).await;
    old.next().await;
    assert_eq!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap()[0]
            .value,
        ae_sync::peer::capabilities::Value::ChainPoi(vec![cap(7)])
    );
    let mut new = Raw::connect(&node, &app).await;
    new.send(&[capabilities(&node, 8, 4322), capabilities(&node, 8, 4322)])
        .await;
    new.next().await;
    new.next().await;
    assert_eq!(old.next().await.kind(), Kind::Close);
    closing(&app, &old_client).await;
    let fresh = app
        .application
        .peers_with_capability(CHAIN_POI)
        .await
        .unwrap();
    assert_eq!(fresh.len(), 1);
    assert_eq!(
        fresh[0].value,
        ae_sync::peer::capabilities::Value::ChainPoi(vec![cap(8)])
    );
    assert_ne!(&fresh[0].generation, old_client.generation());
    old.send(&[
        capabilities(&node, 9, 4321),
        message(Kind::GetNodeInfo, 1, &[]),
    ])
    .await;
    served(&mut app, 1).await;
    old.quiet().await;
    assert_eq!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap(),
        fresh
    );
    acknowledge(&mut old, &old_client).await;
    new.send(&[message(Kind::GetNodeInfo, 1, &[])]).await;
    assert_eq!(response_body(&new.next().await).kind(), Kind::NodeInfo);
    assert_eq!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap(),
        fresh
    );
    stop(app).await;
}
