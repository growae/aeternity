mod common;
use ae_p2p::{
    discovery::{Class, Peer},
    message::{Kind, Message, Value},
    noise,
    registry::{Command, Effect, Info, Outcome},
    runtime as net,
};
use ae_protocol::{ConsensusType, NetworkProfile};
use ae_sync::{gossip, network, pool};
use ae_types::PeerPublicKey;
use common::{Node, within};
use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

fn identities() -> (Arc<noise::Identity>, Arc<noise::Identity>) {
    let a = Arc::new(noise::Identity::from_secret([1; 32]));
    let b = Arc::new(noise::Identity::from_secret([2; 32]));
    if a.public_key() < b.public_key() {
        (a, b)
    } else {
        (b, a)
    }
}
fn context(node: &Node) -> noise::Network {
    let binding = node.db.view().unwrap().identity().clone();
    noise::Network::new(1, binding.genesis, &binding.network).unwrap()
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
fn ping(node: &Node, port: u16) -> Message {
    let genesis = node.db.view().unwrap().identity().genesis;
    Message::new(
        Kind::Ping,
        2,
        BTreeMap::from([
            ("port".into(), Value::Integer(u64::from(port).into())),
            ("share".into(), Value::Integer(0.into())),
            ("genesis_hash".into(), Value::Binary(genesis.0.to_vec())),
            ("difficulty".into(), Value::Integer(1.into())),
            ("best_hash".into(), Value::Binary(genesis.0.to_vec())),
            ("sync_allowed".into(), Value::Boolean(true)),
            ("peers".into(), Value::List(vec![])),
            ("versions".into(), Value::List(vec![])),
            ("height".into(), Value::Integer(0.into())),
            ("capabilities".into(), Value::Binary(vec![0xc0])),
        ]),
        Default::default(),
    )
    .unwrap()
}
async fn status(
    h: &mut network::Handle,
    test: impl Fn(&network::Status) -> bool,
) -> network::Status {
    within(async {
        loop {
            let s = h.status();
            assert!(s.failure.is_none(), "network failure: {:?}", s.failure);
            if test(&s) {
                return s;
            }
            assert!(s.running, "network stopped before condition");
            h.changed().await.unwrap();
        }
    })
    .await
}
async fn start(
    node: &Node,
    local: Arc<noise::Identity>,
    c: network::Config,
) -> (
    network::Handle,
    network::Events,
    pool::Events,
    JoinHandle<network::Exit>,
) {
    let (mut h, events, incoming, service) = network::create(
        node.db.clone(),
        node.client(),
        local,
        NetworkProfile::mainnet(),
        c,
    )
    .await
    .unwrap();
    let task = tokio::spawn(service.run());
    status(&mut h, |s| s.ready).await;
    (h, events, incoming, task)
}
async fn stop(h: &network::Handle, task: JoinHandle<network::Exit>) -> network::Exit {
    h.shutdown();
    let exit = within(task).await.unwrap();
    assert!(exit.status.joined);
    assert!(!exit.status.running);
    assert!(!exit.status.ready);
    assert!(exit.status.listening.is_none());
    assert!(exit.status.failure.is_none(), "{:?}", exit.status.failure);
    assert_eq!(
        (
            exit.status.sockets,
            exit.status.resolutions,
            exit.status.pings
        ),
        (0, 0, 0)
    );
    assert!(!exit.pool.as_ref().unwrap().running);
    assert_eq!(exit.pool.as_ref().unwrap().peers, 0);
    assert!(exit.registry.as_ref().unwrap().policy().is_closed());
    exit
}
async fn eof(socket: &TcpStream) {
    within(async {
        loop {
            socket.readable().await.unwrap();
            match socket.try_read(&mut [0; 1]) {
                Ok(0) => break,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                result => panic!("unexpected protocol bytes or socket error: {result:?}"),
            }
        }
    })
    .await;
}
async fn command(h: &network::Handle, c: Command) -> Outcome {
    within(h.command(c).unwrap().wait()).await.unwrap()
}
async fn incoming(
    node: &Node,
    h: &network::Handle,
    local: PeerPublicKey,
    remote: &noise::Identity,
) -> (net::Client, net::Events, JoinHandle<net::Exit>) {
    let socket = TcpStream::connect(h.status().listening.unwrap())
        .await
        .unwrap();
    let (client, events, driver) = within(net::establish(
        socket,
        remote,
        &context(node),
        noise::Role::Initiator(local),
        Default::default(),
    ))
    .await
    .unwrap();
    (client, events, tokio::spawn(driver.run()))
}
async fn message(events: &mut net::Events) -> net::Received {
    within(async {
        loop {
            if let net::Event::Message(message) = events.recv().await.unwrap() {
                return message;
            }
        }
    })
    .await
}
async fn forwarded(events: &mut pool::Events) -> (net::Client, net::Received) {
    within(async {
        loop {
            let event = events.recv().await.unwrap();
            if let net::Event::Message(message) = event.event {
                return (event.client, message);
            }
        }
    })
    .await
}
async fn next_ping(events: &mut network::Events) -> network::Ping {
    within(async {
        loop {
            if let network::Event::Ping(ping) = events.recv().await.unwrap() {
                return *ping;
            }
        }
    })
    .await
}
fn peer(id: PeerPublicKey, port: u16) -> Peer {
    Peer {
        id,
        address: Ipv4Addr::new(127, 0, 0, 2),
        source: Ipv4Addr::new(10, 20, 30, 40),
        host: b"127.0.0.1".to_vec(),
        port,
        trusted: false,
    }
}
async fn outgoing(
    node: &Node,
    h: &network::Handle,
    remote: &noise::Identity,
) -> (Peer, net::Client, net::Events, JoinHandle<net::Exit>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = peer(remote.public_key(), listener.local_addr().unwrap().port());
    command(h, Command::Add(peer.clone())).await;
    let outcome = command(h, Command::Connect(Some(peer.id))).await;
    assert!(matches!(
        outcome,
        Outcome::Selected(_) | Outcome::Unavailable
    ));
    let (socket, _) = within(listener.accept()).await.unwrap();
    let (client, events, driver) = within(net::establish(
        socket,
        remote,
        &context(node),
        noise::Role::Responder,
        Default::default(),
    ))
    .await
    .unwrap();
    (peer, client, events, tokio::spawn(driver.run()))
}

#[tokio::test]
async fn database_binding_stop_before_run_and_failed_bind_release_owned_workers() {
    let node = Node::new();
    let (local, _) = identities();
    assert!(matches!(
        network::create(
            node.db.clone(),
            node.client(),
            local.clone(),
            NetworkProfile::uat(),
            config()
        )
        .await,
        Err(network::Error::Configuration)
    ));
    let (h, _events, _incoming, service) = network::create(
        node.db.clone(),
        node.client(),
        local.clone(),
        NetworkProfile::mainnet(),
        config(),
    )
    .await
    .unwrap();
    h.shutdown();
    let exit = service.run().await;
    assert!(exit.status.joined);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status.failure);
    assert!(exit.registry.unwrap().policy().is_closed());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = config();
    c.listen = Some(match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => panic!(),
    });
    let (_h, _events, _incoming, service) = network::create(
        node.db.clone(),
        node.client(),
        local,
        NetworkProfile::mainnet(),
        c,
    )
    .await
    .unwrap();
    let exit = within(service.run()).await;
    assert!(matches!(
        exit.status.failure,
        Some(network::Error::Io(std::io::ErrorKind::AddrInUse))
    ));
    assert!(exit.status.joined);
    assert!(exit.registry.unwrap().policy().is_closed());
}

#[tokio::test]
async fn listener_reserves_before_noise_limits_silent_sockets_and_expires_admission() {
    let node = Node::new();
    let (local, _) = identities();
    let mut c = config();
    c.registry.policy.maximum_pending_inbound = 1;
    c.registry.policy.admission_timeout = 500;
    let (mut h, _events, _incoming, task) = start(&node, local, c).await;
    let first = TcpStream::connect(h.status().listening.unwrap())
        .await
        .unwrap();
    status(&mut h, |s| {
        s.policy.as_ref().unwrap().counts.pending_inbound == 1
    })
    .await;
    let second = TcpStream::connect(h.status().listening.unwrap())
        .await
        .unwrap();
    eof(&second).await;
    eof(&first).await;
    let s = status(&mut h, |s| {
        s.policy.as_ref().unwrap().counts.pending_inbound == 0 && s.sockets == 0
    })
    .await;
    assert_eq!(s.policy.unwrap().counts.inbound, 0);
    assert_eq!(s.counts.rejected, 1);
    stop(&h, task).await;
}

#[tokio::test]
async fn admitted_ping_is_responded_and_old_generation_cannot_affect_replacement() {
    let node = Node::new();
    let (local, remote) = identities();
    let (mut h, _events, mut forwarded_events, task) = start(&node, local.clone(), config()).await;
    let (first, _events, first_task) = incoming(&node, &h, local.public_key(), &remote).await;
    let request = first.request(&ping(&node, 4321), None).unwrap();
    let (first_local, received) = forwarded(&mut forwarded_events).await;
    let facts = within(h.connection(first_local.clone()).unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(facts.peer, remote.public_key());
    assert_eq!(facts.address, Ipv4Addr::LOCALHOST);
    assert_eq!(facts.kind, ae_p2p::registry::Kind::Permanent);
    assert_eq!(h.network_id(), node.db.view().unwrap().identity().network);
    assert!(
        within(h.pool_counts(first_local.clone()).unwrap().wait())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(received.message(), &ping(&node, 4321));
    within(
        first_local
            .finish_ping(
                &received,
                Some(&Message::response_ok(received.message(), Default::default()).unwrap()),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(within(request).await, Ok(net::Reply::Response(_))));
    assert_eq!(
        h.ping_received(first_local.clone())
            .unwrap()
            .wait()
            .await
            .unwrap(),
        Outcome::Done
    );
    let (replacement, _replacement_events, replacement_task) =
        incoming(&node, &h, local.public_key(), &remote).await;
    within(replacement.send(&ping(&node, 4322)).unwrap())
        .await
        .unwrap();
    let (replacement_local, replacement_ping) = forwarded(&mut forwarded_events).await;
    within(
        replacement_local
            .finish_ping(
                &replacement_ping,
                Some(
                    &Message::response_ok(replacement_ping.message(), Default::default()).unwrap(),
                ),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    status(&mut h, |s| s.counts.accepted == 2).await;
    assert_ne!(first_local.generation(), replacement_local.generation());
    assert_eq!(
        within(h.pool_counts(first_local.clone()).unwrap().wait())
            .await
            .unwrap(),
        None
    );
    assert!(
        within(h.pool_counts(replacement_local.clone()).unwrap().wait())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        within(h.connection(first_local.clone()).unwrap().wait())
            .await
            .unwrap(),
        None
    );
    let replacement_facts = within(h.connection(replacement_local.clone()).unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(facts.token, replacement_facts.token);
    for ticket in [
        h.ping_received(first_local.clone()).unwrap(),
        h.synchronize(first_local.clone()).unwrap(),
        h.learn(
            first_local.clone(),
            vec![Info {
                id: PeerPublicKey([7; 32]),
                host: b"127.0.0.1".to_vec(),
                port: 9876,
            }],
        )
        .unwrap(),
        h.suspect(first_local.clone()).unwrap(),
        h.disconnect(first_local).unwrap(),
    ] {
        assert_eq!(within(ticket.wait()).await.unwrap(), Outcome::Stale);
    }
    within(first_task).await.unwrap();
    assert!(!replacement_local.is_closed());
    assert_eq!(
        command(
            &h,
            Command::SampleConnected {
                maximum: 10,
                direction: None
            }
        )
        .await,
        Outcome::Peers(vec![Info {
            id: remote.public_key(),
            host: b"127.0.0.1".to_vec(),
            port: 4322
        }])
    );
    assert_eq!(
        within(h.suspect(replacement_local).unwrap().wait())
            .await
            .unwrap(),
        Outcome::Done
    );
    within(replacement_task).await.unwrap();
    let exit = stop(&h, task).await;
    assert_eq!(exit.registry.unwrap().saved_peers().count(), 1);
}

#[tokio::test]
async fn inbound_temporary_is_closed_after_reply_and_blocked_identity_is_not_forwarded() {
    let node = Node::new();
    let (local, remote) = identities();
    let mut c = config();
    c.registry.policy.maximum_inbound = 0;
    let (mut h, mut events, mut forwarded_events, task) = start(&node, local.clone(), c).await;
    let (client, _remote_events, remote_task) =
        incoming(&node, &h, local.public_key(), &remote).await;
    let request = client.request(&ping(&node, 3015), None).unwrap();
    let (local_client, received) = forwarded(&mut forwarded_events).await;
    let admitted_generation = local_client.generation().clone();
    assert_eq!(
        within(h.connection(local_client.clone()).unwrap().wait())
            .await
            .unwrap()
            .unwrap()
            .kind,
        ae_p2p::registry::Kind::Temporary
    );
    status(&mut h, |s| s.policy.as_ref().unwrap().counts.temporary == 1).await;
    within(
        local_client
            .finish_ping(
                &received,
                Some(&Message::response_ok(received.message(), Default::default()).unwrap()),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(within(request).await, Ok(net::Reply::Response(_))));
    assert_eq!(
        h.ping_received(local_client).unwrap().wait().await.unwrap(),
        Outcome::Done
    );
    within(remote_task).await.unwrap();
    status(&mut h, |s| s.sockets == 0).await;
    command(
        &h,
        Command::Block(Info {
            id: remote.public_key(),
            host: b"127.0.0.1".to_vec(),
            port: 3015,
        }),
    )
    .await;
    assert!(
        matches!(within(events.recv()).await, Some(network::Event::Notice(Effect::Blocked(id))) if id == remote.public_key())
    );
    let (client, _remote_events, remote_task) =
        incoming(&node, &h, local.public_key(), &remote).await;
    within(client.send(&ping(&node, 3015)).unwrap())
        .await
        .unwrap();
    within(remote_task).await.unwrap();
    status(&mut h, |s| s.counts.rejected == 1 && s.sockets == 0).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), async {
            while let Some(event) = forwarded_events.recv().await {
                match event.event {
                    net::Event::Ping { generation, .. } => {
                        assert_eq!(generation, admitted_generation)
                    }
                    net::Event::Message(_) => panic!("blocked peer message was forwarded"),
                }
            }
            panic!("network owner unexpectedly stopped");
        })
        .await
        .is_err()
    );
    stop(&h, task).await;
}

#[tokio::test]
async fn dial_resolves_stored_host_and_completed_ping_error_reschedules_without_closing() {
    let node = Node::new();
    let (local, remote) = identities();
    let mut c = config();
    c.registry.policy.maximum_outbound = 1;
    c.registry.policy.ping_interval = 30;
    let (mut h, mut events, _incoming, task) = start(&node, local, c).await;
    let (peer, remote_client, mut remote_events, remote_task) = outgoing(&node, &h, &remote).await;
    let first = next_ping(&mut events).await;
    let facts = within(h.connection(first.client.clone()).unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(facts.address, Ipv4Addr::LOCALHOST);
    assert_ne!(facts.address, peer.address);
    let learned = Info {
        id: PeerPublicKey([7; 32]),
        host: b"127.0.0.3".to_vec(),
        port: 9876,
    };
    assert_eq!(
        within(
            h.learn(first.client.clone(), vec![learned.clone()])
                .unwrap()
                .wait()
        )
        .await
        .unwrap(),
        Outcome::Done
    );
    within(async {
        loop {
            if node
                .db
                .peer_view()
                .unwrap()
                .get(learned.id, 1024)
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let generation = first.client.generation().clone();
    // Error envelopes have no inner ping version and use the v1 request slot.
    let v1 = Message::new(
        Kind::Ping,
        1,
        ping(&node, 3015).fields().clone(),
        Default::default(),
    )
    .unwrap();
    let request = first.client.request(&v1, None).unwrap();
    assert_eq!(
        message(&mut remote_events).await.message().kind(),
        Kind::Ping
    );
    within(
        remote_client
            .send(
                &Message::response_error(Kind::Ping, b"test refusal".to_vec(), Default::default())
                    .unwrap(),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    let net::Reply::Response(refused) = within(request).await.unwrap() else {
        panic!()
    };
    assert_eq!(refused.message().fields()["result"], Value::Boolean(false));
    let first_client = first.client.clone();
    assert!(within(first.complete_and_wait(false)).await);
    within(first_client.finish_ping(&refused, None).unwrap())
        .await
        .unwrap();
    let second = next_ping(&mut events).await;
    assert_eq!(second.client.generation(), &generation);
    assert!(!second.client.is_closed());
    let request = second.client.request(&ping(&node, 3015), None).unwrap();
    let received = message(&mut remote_events).await;
    within(
        remote_client
            .send(&Message::response_ok(received.message(), Default::default()).unwrap())
            .unwrap(),
    )
    .await
    .unwrap();
    let net::Reply::Response(accepted) = within(request).await.unwrap() else {
        panic!()
    };
    let second_client = second.client.clone();
    assert!(within(second.complete_and_wait(true)).await);
    within(second_client.finish_ping(&accepted, None).unwrap())
        .await
        .unwrap();
    status(&mut h, |s| {
        s.counts.pings_completed == 1 && s.counts.pings_failed == 1
    })
    .await;
    let exit = stop(&h, task).await;
    let registry = exit.registry.unwrap();
    let discovered = registry.policy().pool().find(learned.id).unwrap();
    assert_eq!(discovered.source, Ipv4Addr::LOCALHOST);
    assert!(!discovered.trusted);
    assert_eq!(registry.policy().pool().find(peer.id), Some(&peer));
    let saved = registry
        .saved_peers()
        .find(|row| row.id == peer.id)
        .unwrap();
    assert_eq!(saved.host, peer.host);
    within(remote_task).await.unwrap();
}

#[tokio::test]
async fn probes_use_fresh_host_send_zero_protocol_bytes_and_report_alive_then_dead() {
    let node = Node::new();
    let (local, remote) = identities();
    let mut c = config();
    c.registry.policy.maximum_probes = 1;
    let (mut h, mut events, _incoming, task) = start(&node, local, c).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = peer(remote.public_key(), listener.local_addr().unwrap().port());
    command(&h, Command::Add(peer.clone())).await;
    assert!(matches!(
        command(&h, Command::Probe(Class::Unverified)).await,
        Outcome::Selected(_)
    ));
    let (socket, _) = within(listener.accept()).await.unwrap();
    eof(&socket).await;
    assert!(
        matches!(within(events.recv()).await, Some(network::Event::Notice(Effect::ProbeStatus { peer: id, alive: true })) if id == peer.id)
    );
    drop(listener);
    assert!(matches!(
        command(&h, Command::Probe(Class::Verified)).await,
        Outcome::Selected(_)
    ));
    assert!(
        matches!(within(events.recv()).await, Some(network::Event::Notice(Effect::ProbeStatus { peer: id, alive: false })) if id == peer.id)
    );
    let s = status(&mut h, |s| s.counts.probe_dead == 1 && s.sockets == 0).await;
    assert_eq!(s.counts.probe_alive, 1);
    assert_eq!(s.counts.dialed, 0);
    assert_eq!(h.peers().status().peers, 0);
    let exit = stop(&h, task).await;
    assert_eq!(
        exit.registry.unwrap().policy().pool().find(peer.id),
        Some(&peer)
    );
}

#[tokio::test]
async fn full_application_queue_retains_ping_but_delete_prunes_it_and_closes_socket() {
    let node = Node::new();
    let (local, remote) = identities();
    let mut c = config();
    c.registry.policy.maximum_outbound = 1;
    c.commands = 1;
    c.events = 1;
    let (mut h, _events, _incoming, task) = start(&node, local, c).await;
    let blocked = Info {
        id: PeerPublicKey([9; 32]),
        host: b"127.0.0.1".to_vec(),
        port: 1,
    };
    let ticket = h.command(Command::Block(blocked.clone())).unwrap();
    status(&mut h, |s| s.policy.as_ref().unwrap().counts.blocked == 1).await;
    assert!(matches!(
        h.command(Command::IsBlocked(blocked.id)),
        Err(network::Error::Busy)
    ));
    ticket.wait().await.unwrap();
    let (peer, _client, _remote_events, remote_task) = outgoing(&node, &h, &remote).await;
    status(&mut h, |s| {
        s.counts.dialed == 1 && s.policy.as_ref().unwrap().effects == 1
    })
    .await;
    assert_eq!(h.status().pings, 0);
    command(&h, Command::Delete(peer.id)).await;
    within(remote_task).await.unwrap();
    status(&mut h, |s| {
        s.sockets == 0 && s.policy.as_ref().unwrap().effects == 0
    })
    .await;
    stop(&h, task).await;
}

#[tokio::test]
async fn abandoned_or_expired_ping_closes_exact_driver_and_late_reply_is_rejected() {
    for expired in [false, true] {
        let node = Node::new();
        let (local, remote) = identities();
        let mut c = config();
        c.registry.policy.maximum_outbound = 1;
        c.ping_timeout = Duration::from_millis(80);
        let (mut h, mut events, _incoming, task) = start(&node, local, c).await;
        let (_peer, _client, _remote_events, remote_task) = outgoing(&node, &h, &remote).await;
        let ping = next_ping(&mut events).await;
        let monitor = ping.client.monitor();
        let held = if expired {
            Some(ping)
        } else {
            drop(ping);
            None
        };
        within(monitor.wait()).await;
        if let Some(ping) = held {
            assert!(!within(ping.complete_and_wait(true)).await);
        }
        status(&mut h, |s| s.counts.pings_failed == 1).await;
        stop(&h, task).await;
        within(remote_task).await.unwrap();
    }
}

#[tokio::test]
async fn admitted_socket_composes_with_real_gossip_and_transaction_storage() {
    let node = Node::new();
    let (local, remote) = identities();
    let (h, _events, incoming_events, task) = start(&node, local.clone(), config()).await;
    let (mut gossip_handle, mut application, service) = gossip::create(
        node.db.clone(),
        node.client(),
        h.peers(),
        incoming_events,
        NetworkProfile::mainnet(),
        ConsensusType::Pow,
        Default::default(),
    )
    .unwrap();
    let gossip_task = tokio::spawn(service.run());
    let (client, mut remote_events, remote_task) =
        incoming(&node, &h, local.public_key(), &remote).await;
    within(client.send(&ping(&node, 3015)).unwrap())
        .await
        .unwrap();
    within(async {
        loop {
            let received = application.recv().await.unwrap();
            if let net::Event::Message(ping) = received.event {
                received
                    .client
                    .finish_ping(
                        &ping,
                        Some(&Message::response_ok(ping.message(), Default::default()).unwrap()),
                    )
                    .unwrap()
                    .await
                    .unwrap();
                break;
            }
        }
    })
    .await;
    let good = common::transaction();
    let message = Message::new(
        Kind::Transactions,
        1,
        BTreeMap::from([(
            "txs".into(),
            Value::List(vec![
                Value::Binary(vec![0xff]),
                Value::Binary(good.encode()),
            ]),
        )]),
        Default::default(),
    )
    .unwrap();
    within(client.send(&message).unwrap()).await.unwrap();
    within(async {
        while gossip_handle.status().counts.ordinary_admitted != 1 {
            gossip_handle.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(
        node.db.pool_view().unwrap().members(10).unwrap(),
        vec![good.hash()]
    );
    assert_eq!(gossip_handle.status().counts.malformed, 1);
    let echoed = self::message(&mut remote_events).await;
    assert_eq!(echoed.message().kind(), Kind::Transactions);
    assert_eq!(
        echoed.message().fields()["txs"],
        Value::List(vec![Value::Binary(good.encode())])
    );
    gossip_handle.shutdown();
    let gossip_status = within(gossip_task).await.unwrap();
    assert!(!gossip_status.running);
    assert_eq!(gossip_status.operations, 0);
    // Closing the application's pool consumer is also a supervised stop signal.
    let exit = within(task).await.unwrap();
    assert!(exit.status.joined);
    assert_eq!(
        exit.status.failure,
        Some(network::Error::Pool(pool::Error::Closed))
    );
    within(remote_task).await.unwrap();
}

#[tokio::test]
async fn restart_feeds_saved_peer_through_owned_startup_resolution_and_automatic_dial() {
    let node = Node::new();
    let (local, remote) = identities();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let saved = peer(remote.public_key(), listener.local_addr().unwrap().port());
    let (h, _events, _incoming, task) = start(&node, local.clone(), config()).await;
    command(&h, Command::Add(saved.clone())).await;
    drop(stop(&h, task).await);
    let mut c = config();
    c.startup_limits.maximum_step = 1;
    c.registry.policy.maximum_outbound = 1;
    let (mut h, mut events, _incoming, task) = start(&node, local, c).await;
    // No external Resolve/Connect command: the owner drives saved startup,
    // resolution completion and the registry's connect timer itself.
    let (socket, _) = within(listener.accept()).await.unwrap();
    let (_client, _events, driver) = within(net::establish(
        socket,
        &remote,
        &context(&node),
        noise::Role::Responder,
        Default::default(),
    ))
    .await
    .unwrap();
    let remote_task = tokio::spawn(driver.run());
    let ping = next_ping(&mut events).await;
    assert_eq!(ping.client.peer(), saved.id);
    let s = status(&mut h, |s| s.counts.dialed == 1 && s.resolutions == 0).await;
    assert!(s.startup.unwrap().ready);
    let exit = stop(&h, task).await;
    assert!(!within(ping.complete_and_wait(true)).await);
    let registry = exit.registry.unwrap();
    let restored = registry.policy().pool().find(saved.id).unwrap();
    assert_eq!(restored.address, Ipv4Addr::LOCALHOST);
    assert_eq!(restored.source, saved.source);
    assert_eq!(restored.host, saved.host);
    let record = registry.saved_peers().next().unwrap();
    assert_eq!(record.source, std::net::IpAddr::V4(saved.source));
    assert_eq!(record.address, std::net::IpAddr::V4(Ipv4Addr::LOCALHOST));
    within(remote_task).await.unwrap();
}

#[tokio::test]
async fn saturated_socket_capacity_keeps_timers_live_and_never_dials_an_expired_token() {
    let node = Node::new();
    let (local, remote) = identities();
    let mut c = config();
    c.transport.connections = 1;
    c.registry.policy.maximum_outbound = 1;
    c.registry.policy.attempt_timeout = 1000;
    let (mut h, _events, mut incoming_events, task) = start(&node, local.clone(), c).await;
    let (client, _remote_events, remote_task) =
        incoming(&node, &h, local.public_key(), &remote).await;
    within(client.send(&ping(&node, 3015)).unwrap())
        .await
        .unwrap();
    let (local_client, ping) = forwarded(&mut incoming_events).await;
    within(
        local_client
            .finish_ping(
                &ping,
                Some(&Message::response_ok(ping.message(), Default::default()).unwrap()),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    status(&mut h, |s| s.counts.accepted == 1).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let other = noise::Identity::from_secret([3; 32]);
    let waiting = peer(other.public_key(), listener.local_addr().unwrap().port());
    command(&h, Command::Add(waiting.clone())).await;
    assert!(matches!(
        command(&h, Command::Connect(Some(waiting.id))).await,
        Outcome::Selected(_) | Outcome::Unavailable
    ));
    status(&mut h, |s| s.resolutions == 1 && s.sockets == 1).await;
    status(&mut h, |s| {
        s.resolutions == 0 && s.policy.as_ref().unwrap().counts.outbound == 0
    })
    .await;
    command(&h, Command::Delete(waiting.id)).await;
    assert_eq!(
        h.disconnect(local_client).unwrap().wait().await.unwrap(),
        Outcome::Done
    );
    within(remote_task).await.unwrap();
    status(&mut h, |s| s.sockets == 0).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
    assert_eq!(h.status().counts.dialed, 0);
    stop(&h, task).await;
}

#[tokio::test]
async fn dropping_an_unstarted_owner_rejects_queued_commands_and_does_not_claim_joined_run() {
    let node = Node::new();
    let (local, _) = identities();
    let (h, _events, _incoming, service) = network::create(
        node.db.clone(),
        node.client(),
        local,
        NetworkProfile::mainnet(),
        config(),
    )
    .await
    .unwrap();
    assert!(matches!(
        h.command(Command::Shutdown),
        Err(network::Error::Unsupported)
    ));
    let ticket = h
        .command(Command::IsBlocked(PeerPublicKey([9; 32])))
        .unwrap();
    drop(service);
    assert_eq!(ticket.wait().await, Err(network::Error::Closed));
    assert!(!h.status().running);
    assert!(!h.status().joined);
    assert!(h.status().listening.is_none());
    assert_eq!(h.status().failure, Some(network::Error::Closed));
    assert!(matches!(
        h.command(Command::IsBlocked(PeerPublicKey([9; 32]))),
        Err(network::Error::Closed)
    ));
}
