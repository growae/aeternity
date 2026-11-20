use ae_p2p::{
    message::{self, Kind, Message, Value},
    noise,
    registry::{Command, Effect, Failure, Outcome, Token},
    runtime as net,
};
use ae_sync::{pool, registry, transport};
use ae_types::{Hash256, PeerPublicKey};
use std::{collections::BTreeMap, net::Ipv4Addr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
mod common;
use common::{Node, within};

fn identities() -> (Arc<noise::Identity>, Arc<noise::Identity>) {
    let a = Arc::new(noise::Identity::from_secret([1; 32]));
    let b = Arc::new(noise::Identity::from_secret([2; 32]));
    if a.public_key() < b.public_key() {
        (a, b)
    } else {
        (b, a)
    }
}
fn network() -> Arc<noise::Network> {
    Arc::new(noise::Network::new(1, Hash256::ZERO, b"ae_dev").unwrap())
}
fn ping() -> Message {
    Message::new(
        Kind::Ping,
        2,
        BTreeMap::from([
            ("port".into(), Value::Integer(3015.into())),
            ("share".into(), Value::Integer(0.into())),
            ("genesis_hash".into(), Value::Binary(vec![0; 32])),
            ("difficulty".into(), Value::Integer(1.into())),
            ("best_hash".into(), Value::Binary(vec![1; 32])),
            ("sync_allowed".into(), Value::Boolean(true)),
            ("peers".into(), Value::List(vec![])),
            ("versions".into(), Value::List(vec![])),
            ("height".into(), Value::Integer(1.into())),
            ("capabilities".into(), Value::Binary(vec![0xc0])),
        ]),
        Default::default(),
    )
    .unwrap()
}
async fn reserve(client: &registry::worker::Client, now: u64) -> Token {
    let Outcome::Reserved(token) = client
        .apply(
            now,
            Command::ReserveInbound {
                address: Ipv4Addr::LOCALHOST,
            },
        )
        .unwrap()
        .await
        .unwrap()
    else {
        panic!()
    };
    token
}
async fn handshake(
    sockets: &mut transport::Sockets,
    token: Token,
    local: PeerPublicKey,
    remote: &noise::Identity,
) -> (net::Client, net::Events, JoinHandle<net::Exit>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote_socket = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (accepted, _) = listener.accept().await.unwrap();
    sockets.inbound(token, accepted).unwrap();
    let network = network();
    let (client, events, driver) = within(async {
        tokio::select! {
            _ = sockets.next() => panic!("admission before first ping"),
            established = net::establish(remote_socket, remote, &network, noise::Role::Initiator(local), Default::default()) => established.unwrap(),
        }
    }).await;
    (client, events, tokio::spawn(driver.run()))
}
async fn ready(
    sockets: &mut transport::Sockets,
    client: &net::Client,
    before_ping: bool,
) -> Box<transport::Ready> {
    let (event, ()) = within(async {
        tokio::join!(sockets.next(), async {
            if before_ping {
                let message = Message::new(
                    Kind::GetNodeInfo,
                    1,
                    BTreeMap::new(),
                    message::Limits::default(),
                )
                .unwrap();
                assert!(matches!(
                    client.send(&message).unwrap().await,
                    Ok(net::Reply::Sent)
                ));
            }
            assert!(matches!(
                client.send(&ping()).unwrap().await,
                Ok(net::Reply::Sent)
            ));
        })
    })
    .await;
    let Some(transport::Event::Ready(ready)) = event else {
        panic!()
    };
    ready
}

#[tokio::test]
async fn durable_admission_replays_first_ping_and_stale_close_cannot_retire_replacement() {
    let node = Node::new();
    let (local, remote) = identities();
    let worker = registry::worker::Worker::start(
        registry::Service::start(node.db.clone(), local.public_key(), Default::default()).unwrap(),
        Default::default(),
    )
    .unwrap();
    let policy = worker.client();
    let mut sockets =
        transport::Sockets::new(local.clone(), network(), Default::default()).unwrap();
    let (pool, mut events, pool_task) = node.service(Default::default());
    let first = reserve(&policy, 0).await;
    let (remote_client, _remote_events, remote_task) =
        handshake(&mut sockets, first, local.public_key(), &remote).await;
    assert_eq!(
        policy
            .inspect()
            .unwrap()
            .await
            .unwrap()
            .counts
            .pending_inbound,
        1
    );
    assert_eq!(policy.inspect().unwrap().await.unwrap().counts.inbound, 0);
    let first_ready = ready(&mut sockets, &remote_client, true).await;
    let first_client = first_ready.connection.client().clone();
    assert_eq!(
        policy
            .apply(
                1,
                Command::Accept {
                    token: first,
                    peer: first_ready.peer.clone()
                }
            )
            .unwrap()
            .await
            .unwrap(),
        Outcome::Accepted(ae_p2p::registry::Kind::Permanent)
    );
    pool.attach(first_ready.connection, false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    // Same generation and original order, including the negotiation event
    // preceding v2 Ping. The pool gets the actual credit-bearing Received.
    for expected in [Some(Kind::GetNodeInfo), None, Some(Kind::Ping)] {
        let event = within(events.recv()).await.unwrap();
        assert_eq!(event.peer, remote.public_key());
        assert_eq!(event.client.generation(), first_client.generation());
        match (event.event, expected) {
            (net::Event::Message(received), Some(kind)) => {
                assert_eq!(&received.generation, first_client.generation());
                assert_eq!(received.message().kind(), kind);
                if kind == Kind::Ping {
                    assert_eq!(received.message(), &ping());
                }
            }
            (
                net::Event::Ping {
                    generation,
                    action: ae_p2p::requests::PingAction::Selected(2),
                },
                None,
            ) => assert_eq!(&generation, first_client.generation()),
            _ => panic!("lost or reordered admission event"),
        }
    }
    let second = reserve(&policy, 2).await;
    let (replacement, _replacement_events, replacement_task) =
        handshake(&mut sockets, second, local.public_key(), &remote).await;
    let second_ready = ready(&mut sockets, &replacement, false).await;
    let second_client = second_ready.connection.client().clone();
    assert_ne!(first_client.generation(), second_client.generation());
    assert_eq!(
        policy
            .apply(
                3,
                Command::Accept {
                    token: second,
                    peer: second_ready.peer.clone()
                }
            )
            .unwrap()
            .await
            .unwrap(),
        Outcome::Accepted(ae_p2p::registry::Kind::Permanent)
    );
    let close = policy.next_effect().unwrap().await.unwrap().unwrap();
    assert_eq!(close.effect, Effect::Close(first));
    assert!(sockets.cancel(first));
    assert!(policy.acknowledge(close.id).unwrap().await.unwrap());
    assert!(policy.next_effect().unwrap().await.unwrap().is_none());
    pool.attach(second_ready.connection, false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let Some(transport::Event::Closed { token, exit, .. }) = within(sockets.next()).await else {
        panic!()
    };
    assert_eq!(token, first);
    assert_eq!(&exit.generation, first_client.generation());
    assert_eq!(
        policy
            .apply(
                4,
                Command::Failed {
                    peer: remote.public_key(),
                    token: first,
                    reason: Failure::Down
                }
            )
            .unwrap()
            .await
            .unwrap(),
        Outcome::Stale
    );
    assert_eq!(policy.inspect().unwrap().await.unwrap().counts.inbound, 1);
    assert!(second_client.monitor().exit().is_none());
    assert!(!second_client.is_closed());
    within(remote_task).await.unwrap();
    sockets.stop();
    let Some(transport::Event::Closed { token, exit, .. }) = within(sockets.next()).await else {
        panic!()
    };
    assert_eq!(token, second);
    assert_eq!(&exit.generation, second_client.generation());
    assert_eq!(sockets.occupied(), 0);
    assert!(sockets.next().await.is_none());
    assert_eq!(
        policy
            .apply(
                5,
                Command::Failed {
                    peer: remote.public_key(),
                    token: second,
                    reason: Failure::Closed
                }
            )
            .unwrap()
            .await
            .unwrap(),
        Outcome::Done
    );
    within(replacement_task).await.unwrap();
    pool.shutdown();
    assert_eq!(within(pool_task).await.unwrap().peers, 0);
    let service = worker.shutdown().unwrap();
    assert_eq!(service.policy().counts().pending_inbound, 0);
    assert_eq!(service.policy().counts().inbound, 0);
    let saved: Vec<_> = service.saved_peers().collect();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].id, remote.public_key());
}

#[tokio::test]
async fn expired_registry_reservation_rejects_late_first_ping_and_pool_refusal_releases_transport()
{
    let node = Node::new();
    let (local, remote) = identities();
    let mut config = registry::Config::default();
    config.policy.admission_timeout = 1;
    let worker = registry::worker::Worker::start(
        registry::Service::start(node.db.clone(), local.public_key(), config).unwrap(),
        Default::default(),
    )
    .unwrap();
    let policy = worker.client();
    let mut sockets =
        transport::Sockets::new(local.clone(), network(), Default::default()).unwrap();
    let token = reserve(&policy, 0).await;
    let (client, _events, task) = handshake(&mut sockets, token, local.public_key(), &remote).await;
    let timer = policy.next_timer().unwrap().await.unwrap().unwrap();
    assert_eq!(timer.deadline, 1);
    assert_eq!(
        policy
            .apply(1, Command::Timeout(timer.token))
            .unwrap()
            .await
            .unwrap(),
        Outcome::Done
    );
    // A late authenticated ping is transport-ready, but no longer policy-owned.
    let ready = ready(&mut sockets, &client, false).await;
    assert_eq!(
        policy
            .apply(
                2,
                Command::Accept {
                    token,
                    peer: ready.peer.clone()
                }
            )
            .unwrap()
            .await
            .unwrap(),
        Outcome::Stale
    );
    let monitor = ready.connection.client().monitor();
    let (handle, _forwarded, service) =
        pool::create(node.db.clone(), node.client(), Default::default()).unwrap();
    handle.shutdown();
    assert!(matches!(
        handle.attach(ready.connection, false),
        Err(pool::Error::Closed)
    ));
    assert_eq!(monitor.wait().await.reason, net::Error::Cancelled);
    assert!(
        matches!(within(sockets.next()).await, Some(transport::Event::Closed { token: actual, .. }) if actual == token)
    );
    within(task).await.unwrap();
    assert_eq!(service.run().await.peers, 0);
    sockets.stop();
    assert!(sockets.next().await.is_none());
    let service = worker.shutdown().unwrap();
    assert_eq!(service.policy().counts().pending_inbound, 0);
    assert_eq!(service.saved_peers().count(), 0);
    // The retained timeout effect is harmless after transport completion.
    assert_eq!(
        service.next_effect().unwrap().unwrap().effect,
        Effect::Close(token)
    );
    assert!(!sockets.cancel(token));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), monitor.wait())
            .await
            .is_ok()
    );
}
