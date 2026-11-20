mod common;
use ae_mempool::{
    hc_votes,
    pool::{Event, Signed},
};
use ae_p2p::{
    message::{Kind, Message, Value},
    runtime as net,
};
use ae_protocol::{ConsensusType, NetworkProfile};
use ae_storage::pool::worker;
use ae_sync::{gossip, pool};
use common::*;
use std::{collections::BTreeMap, time::Duration};
use tokio::task::JoinHandle;

struct Running {
    handle: gossip::Handle,
    _events: gossip::Events,
    peers: pool::Handle,
    task: JoinHandle<gossip::Status>,
    pool_task: JoinHandle<pool::Status>,
}
fn prepare(
    node: &Node,
    config: gossip::Config,
    consensus: ConsensusType,
) -> (
    gossip::Handle,
    gossip::Events,
    gossip::Service,
    pool::Handle,
    JoinHandle<pool::Status>,
) {
    let (peers, events, pool_task) = node.service(Default::default());
    let (handle, events, service) = gossip::create(
        node.db.clone(),
        node.client(),
        peers.clone(),
        events,
        NetworkProfile::mainnet(),
        consensus,
        config,
    )
    .unwrap();
    (handle, events, service, peers, pool_task)
}
fn start(node: &Node, config: gossip::Config, consensus: ConsensusType) -> Running {
    let (handle, events, service, peers, pool_task) = prepare(node, config, consensus);
    Running {
        handle,
        _events: events,
        peers,
        task: tokio::spawn(service.run()),
        pool_task,
    }
}
async fn status(
    handle: &mut gossip::Handle,
    predicate: impl Fn(&gossip::Status) -> bool,
) -> gossip::Status {
    within(async {
        loop {
            let s = handle.status();
            if predicate(&s) {
                return s;
            }
            handle.changed().await.unwrap();
        }
    })
    .await
}
async fn stop(r: Running) {
    r.handle.shutdown();
    let status = within(r.task).await.unwrap();
    assert!(!status.running);
    assert_eq!(status.operations, 0);
    r.peers.shutdown();
    assert!(!within(r.pool_task).await.unwrap().running);
}
fn txs(values: Vec<Vec<u8>>) -> Message {
    Message::new(
        Kind::Transactions,
        1,
        BTreeMap::from([(
            "txs".into(),
            Value::List(values.into_iter().map(Value::Binary).collect()),
        )]),
        Default::default(),
    )
    .unwrap()
}
async fn received(events: &mut net::Events) -> Vec<Vec<u8>> {
    let net::Event::Message(message) = within(events.recv()).await.unwrap() else {
        panic!()
    };
    assert_eq!(message.message().kind(), Kind::Transactions);
    let Value::List(values) = &message.message().fields()["txs"] else {
        panic!()
    };
    values
        .iter()
        .map(|v| {
            let Value::Binary(v) = v else { panic!() };
            v.clone()
        })
        .collect()
}
async fn send(client: &net::Client, values: Vec<Vec<u8>>) {
    within(client.send(&txs(values)).unwrap()).await.unwrap();
}
fn vote() -> Signed {
    let data: serde_json::Value = serde_json::from_str(include_str!(
        "../../ae-mempool/tests/fixtures/erlang_hc_vote.json"
    ))
    .unwrap();
    let row = data["signatures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["label"] == "normal" && r["mode"] == "pos" && r["protocol"] == 1)
        .unwrap();
    Signed::decode(&hex::decode(row["signed"].as_str().unwrap()).unwrap()).unwrap()
}

#[tokio::test]
async fn malformed_entries_are_isolated_and_real_admission_gossips_once() {
    let node = Node::new();
    let good = transaction();
    let bad = invalid_signature(&good);
    let mut service = start(&node, Default::default(), ConsensusType::Pow);
    let (local, (remote, mut events, driver)) = pair(30, 31).await;
    let driver = tokio::spawn(driver.run());
    service
        .peers
        .attach(connection(local), false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    send(&remote, vec![vec![0xff], good.encode(), bad.encode()]).await;
    let current = status(&mut service.handle, |s| s.counts.decoded == 2).await;
    assert_eq!(
        (
            current.counts.malformed,
            current.counts.ordinary_admitted,
            current.counts.rejected
        ),
        (1, 1, 1)
    );
    assert_eq!(received(&mut events).await, vec![good.encode()]);
    assert_eq!(
        node.db.pool_view().unwrap().members(10).unwrap(),
        vec![good.hash()]
    );
    send(&remote, vec![good.encode()]).await;
    status(&mut service.handle, |s| s.counts.decoded == 3).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), events.recv())
            .await
            .is_err()
    );
    assert_eq!(service.handle.status().counts.ordinary_admitted, 1);
    stop(service).await;
    within(driver).await.unwrap();
}

#[tokio::test]
async fn three_composed_services_relay_created_transactions_and_converge() {
    let a = Node::new();
    let b = Node::new();
    let c = Node::new();
    let ar = start(&a, Default::default(), ConsensusType::Pow);
    let br = start(&b, Default::default(), ConsensusType::Pow);
    let mut cr = start(&c, Default::default(), ConsensusType::Pow);
    let (ab, ba) = pair(32, 33).await;
    let (bc, cb) = pair(33, 34).await;
    for (handle, established) in [
        (&ar.peers, ab),
        (&br.peers, ba),
        (&br.peers, bc),
        (&cr.peers, cb),
    ] {
        handle
            .attach(connection(established), false)
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    let tx = transaction();
    a.client()
        .push(tx.clone(), Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    status(&mut cr.handle, |s| s.counts.ordinary_admitted == 1).await;
    for node in [&a, &b, &c] {
        assert_eq!(
            node.db.pool_view().unwrap().members(10).unwrap(),
            vec![tx.hash()]
        );
    }
    stop(cr).await;
    stop(br).await;
    stop(ar).await;
}

#[tokio::test]
async fn gossip_toggle_and_random_target_cap_apply_to_committed_events() {
    let node = Node::new();
    let mut config = gossip::Config::default();
    config.policy.enabled = false;
    config.policy.max_peers = 1;
    let mut service = start(&node, config, ConsensusType::Pow);
    let (a, (remote_a, mut events_a, driver_a)) = pair(35, 36).await;
    let (b, (remote_b, mut events_b, driver_b)) = pair(35, 37).await;
    let driver_a = tokio::spawn(driver_a.run());
    let driver_b = tokio::spawn(driver_b.run());
    for connection_value in [a, b] {
        service
            .peers
            .attach(connection(connection_value), false)
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    let tx = transaction();
    node.client().force_push(tx.clone()).unwrap().await.unwrap();
    status(&mut service.handle, |s| s.counts.suppressed == 1).await;
    service
        .handle
        .set_policy(gossip::Policy {
            enabled: true,
            ..config.policy
        })
        .unwrap();
    node.client().force_push(tx.clone()).unwrap().await.unwrap();
    status(&mut service.handle, |s| s.counts.sent == 1).await;
    let selected = tokio::select! { _=received(&mut events_a)=>0,_=received(&mut events_b)=>1 };
    let other = if selected == 0 {
        &mut events_b
    } else {
        &mut events_a
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(50), other.recv())
            .await
            .is_err()
    );
    assert_eq!(service.handle.status().counts.fanout_events, 1);
    assert!(!remote_a.is_closed() && !remote_b.is_closed());
    stop(service).await;
    within(driver_a).await.unwrap();
    within(driver_b).await.unwrap();
}

#[tokio::test]
async fn lag_recovery_never_advances_past_an_incomplete_snapshot_and_can_resume() {
    let node = Node::with_settings(worker::Settings {
        event_count: 1,
        origins_interval: None,
        ..Default::default()
    });
    let mut config = gossip::Config::default();
    config.policy.snapshot_limit = 1;
    let (mut handle, events, service, peers, pool_task) =
        prepare(&node, config, ConsensusType::Pow);
    let good = transaction();
    let other = invalid_signature(&good);
    node.client()
        .force_push(good.clone())
        .unwrap()
        .await
        .unwrap();
    node.client()
        .force_push(other.clone())
        .unwrap()
        .await
        .unwrap();
    let (local, (_remote, mut remote_events, driver)) = pair(38, 39).await;
    let driver = tokio::spawn(driver.run());
    peers
        .attach(connection(local), false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let task = tokio::spawn(service.run());
    let s = status(&mut handle, |s| matches!(s.feed, gossip::Feed::Paused(_))).await;
    assert_eq!(
        s.feed,
        gossip::Feed::Paused(gossip::Error::SnapshotIncomplete {
            visible: 1,
            total: 2
        })
    );
    assert_eq!(s.counts.sent, 0);
    assert_eq!(s.counts.recovered, 0);
    handle
        .set_policy(gossip::Policy {
            snapshot_limit: 10,
            ..config.policy
        })
        .unwrap();
    let mut values = received(&mut remote_events).await;
    values.extend(received(&mut remote_events).await);
    values.sort();
    let mut expected = vec![good.encode(), other.encode()];
    expected.sort();
    assert_eq!(values, expected);
    status(&mut handle, |s| {
        s.counts.sent == 2 && s.counts.recovered == 1
    })
    .await;
    assert_eq!(handle.status().counts.gaps, 1);
    stop(Running {
        handle,
        _events: events,
        peers,
        task,
        pool_task,
    })
    .await;
    within(driver).await.unwrap();
}

#[tokio::test]
async fn hyperchain_votes_use_the_separate_owner_shared_cache_and_epoch_lifecycle() {
    for consensus in [ConsensusType::Pow, ConsensusType::Pos] {
        let node = Node::new();
        let mut service = start(&node, Default::default(), consensus);
        let (local, (remote, mut events, driver)) = pair(40, 41).await;
        let driver = tokio::spawn(driver.run());
        service
            .peers
            .attach(connection(local), false)
            .unwrap()
            .wait()
            .await
            .unwrap();
        let vote = vote();
        send(&remote, vec![vote.encode(), vec![0xff]]).await;
        status(&mut service.handle, |s| s.counts.decoded == 1).await;
        assert_eq!(node.client().info().submitted, 0);
        assert!(node.db.pool_view().unwrap().members(10).unwrap().is_empty());
        if consensus == ConsensusType::Pos {
            assert_eq!(service.handle.status().counts.votes_admitted, 1);
            assert_eq!(received(&mut events).await, vec![vote.encode()]);
            assert_eq!(
                service
                    .handle
                    .peek_votes(7.into(), Some(2.into()), 10)
                    .unwrap()
                    .wait()
                    .await
                    .unwrap()
                    .len(),
                1
            );
            service
                .handle
                .advance_epoch(8.into())
                .unwrap()
                .wait()
                .await
                .unwrap();
            assert_eq!(
                service
                    .handle
                    .peek_votes(7.into(), None, 10)
                    .unwrap()
                    .wait()
                    .await
                    .unwrap()
                    .len(),
                1
            );
            service
                .handle
                .advance_epoch(9.into())
                .unwrap()
                .wait()
                .await
                .unwrap();
            assert!(
                service
                    .handle
                    .peek_votes(7.into(), None, 10)
                    .unwrap()
                    .wait()
                    .await
                    .unwrap()
                    .is_empty()
            );
            // Clearing the short-lived vote pool does not clear the shared cache.
            assert_eq!(
                service
                    .handle
                    .push_vote(vote.encode(), hc_votes::Event::Created)
                    .unwrap()
                    .wait()
                    .await
                    .unwrap(),
                hc_votes::PushOutcome::Cached
            );
            node.client().gossip_cache().reset().unwrap();
            drop(
                service
                    .handle
                    .push_vote(vote.encode(), hc_votes::Event::Created)
                    .unwrap(),
            );
            assert_eq!(received(&mut events).await, vec![vote.encode()]);
            status(&mut service.handle, |s| s.counts.votes_admitted == 2).await;
        } else {
            assert_eq!(service.handle.status().counts.rejected, 1);
            assert!(
                service
                    .handle
                    .peek_votes(7.into(), None, 10)
                    .unwrap()
                    .wait()
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        stop(service).await;
        within(driver).await.unwrap();
    }
}

#[tokio::test]
async fn abrupt_gossip_shutdown_joins_waiters_and_closes_its_owned_forwarding_chain() {
    let node = Node::new();
    let service = start(&node, Default::default(), ConsensusType::Pow);
    let (local, (remote, _events, driver)) = pair(42, 43).await;
    let driver = tokio::spawn(driver.run());
    service
        .peers
        .attach(connection(local), false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    tokio::task::yield_now().await;
    service.task.abort();
    assert!(within(service.task).await.unwrap_err().is_cancelled());
    assert!(!within(service.pool_task).await.unwrap().running);
    within(driver).await.unwrap();
    assert!(remote.is_closed());
    assert!(matches!(
        service.handle.push_vote(vec![], hc_votes::Event::Created),
        Err(gossip::Error::Closed)
    ));
}

#[tokio::test]
async fn unstarted_owner_bounds_control_bytes_and_releases_accepted_vote_observers() {
    let node = Node::new();
    let config = gossip::Config {
        commands: 1,
        command_bytes: 512,
        ..Default::default()
    };
    let (handle, _events, service, peers, pool_task) = prepare(&node, config, ConsensusType::Pos);
    let accepted = handle
        .push_vote(vote().encode(), hc_votes::Event::Created)
        .unwrap();
    assert!(matches!(
        handle.advance_epoch(1.into()),
        Err(gossip::Error::Busy)
    ));
    drop(service);
    assert!(matches!(accepted.wait().await, Err(gossip::Error::Closed)));
    assert!(matches!(handle.retry_feed(), Err(gossip::Error::Closed)));
    peers.shutdown();
    within(pool_task).await.unwrap();
    assert_eq!(node.client().info().submitted, 0);
}

#[test]
#[ignore = "requires the isolated pinned Erlang gossip handler harness"]
fn native_gossip_runtime_worker() {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async{
        let node=Node::new();let mut running=start(&node,Default::default(),ConsensusType::Pos);
        let role=std::env::var("AE_NOISE_ROLE").unwrap();
        let initiator=ae_p2p::noise::Identity::from_secret([1;32]);let responder=ae_p2p::noise::Identity::from_secret([2;32]);
        let network=ae_p2p::noise::Network::new(1,ae_types::Hash256([0;32]),b"ae_dev").unwrap();
        let(socket,identity,role)=if role=="initiator"{
            (within(tokio::net::TcpStream::connect(std::env::var("AE_NOISE_ADDRESS").unwrap())).await.unwrap(),&initiator,ae_p2p::noise::Role::Initiator(responder.public_key()))
        }else{
            assert_eq!(role,"responder");let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            std::fs::write(std::env::var("AE_NOISE_READY").unwrap(),listener.local_addr().unwrap().port().to_string()).unwrap();
            let(socket,_)=within(listener.accept()).await.unwrap();(socket,&responder,ae_p2p::noise::Role::Responder)
        };
        let established=within(net::establish(socket,identity,&network,role,net::Config::default())).await.unwrap();
        let client=established.0.clone();running.peers.attach(connection(established),false).unwrap().wait().await.unwrap();
        let s=status(&mut running.handle,|s|s.counts.decoded==2&&s.counts.sent==2).await;
        assert_eq!((s.counts.malformed,s.counts.ordinary_admitted,s.counts.votes_admitted),(1,1,1));
        assert_eq!(node.db.pool_view().unwrap().members(10).unwrap(),vec![transaction().hash()]);
        assert_eq!(running.handle.peek_votes(7.into(),None,10).unwrap().wait().await.unwrap().len(),1);
        send(&client,vec![vec![0xff],transaction().encode(),vote().encode(),vec![0xff]]).await;
        let forwarded=within(running._events.recv()).await.unwrap();
        let net::Event::Message(marker)=forwarded.event else{panic!()};
        assert_eq!(marker.message().kind(),Kind::GetNodeInfo);assert_eq!(&marker.generation,client.generation());
        let close=Message::new(Kind::Close,1,Default::default(),Default::default()).unwrap();
        within(client.send(&close).unwrap()).await.unwrap();stop(running).await;
        println!("Native gossip bytes, malformed-entry isolation, actual durable and vote admission, original native type routing, and joined shutdown passed");
    });
}
