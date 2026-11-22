mod common;
use ae_mempool::hc_votes::{Event, PushOutcome};
use ae_p2p::{
    message::{Kind, Message, Value},
    runtime as net,
};
use ae_protocol::{ConsensusType, NetworkProfile};
use ae_storage::pool::worker::{CacheController, CacheOwner, GossipCache};
use ae_sync::{gossip, pool, runtime::workers, votes};
use common::{Node, within};
use tokio::task::JoinHandle;

struct Running {
    gossip: gossip::Handle,
    _events: gossip::Events,
    peers: pool::Handle,
    task: JoinHandle<gossip::Status>,
    peer_task: JoinHandle<pool::Status>,
    _cache: CacheOwner,
}
fn start(node: &Node, bindings: &workers::Bindings, controller: &mut CacheController) -> Running {
    let cache = controller.start().unwrap();
    let (peers, incoming, service) = pool::create(
        node.db.clone(),
        bindings.admission.current().unwrap().client,
        Default::default(),
    )
    .unwrap();
    let peer_task = tokio::spawn(service.run());
    let (gossip, events, service) = gossip::create_with_votes(
        node.db.clone(),
        bindings.admission.clone(),
        peers.clone(),
        incoming,
        NetworkProfile::mainnet(),
        ConsensusType::Pos,
        Default::default(),
        bindings.votes.clone(),
    )
    .unwrap();
    Running {
        gossip,
        _events: events,
        peers,
        task: tokio::spawn(service.run()),
        peer_task,
        _cache: cache,
    }
}
async fn stop(r: Running) {
    r.gossip.shutdown();
    assert!(!within(r.task).await.unwrap().running);
    r.peers.shutdown();
    assert!(!within(r.peer_task).await.unwrap().running);
}
fn vote() -> Vec<u8> {
    let f: serde_json::Value = serde_json::from_str(include_str!(
        "../../ae-mempool/tests/fixtures/erlang_hc_vote.json"
    ))
    .unwrap();
    hex::decode(
        f["signatures"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["label"] == "normal" && r["mode"] == "pos" && r["protocol"] == 1)
            .unwrap()["signed"]
            .as_str()
            .unwrap(),
    )
    .unwrap()
}
async fn connect(r: &Running) -> (net::Client, net::Events, JoinHandle<net::Exit>) {
    let (local, (remote, events, driver)) = common::pair(93, 94).await;
    let task = tokio::spawn(driver.run());
    within(
        r.peers
            .attach(common::connection(local), false)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    (remote, events, task)
}
async fn transaction(events: &mut net::Events) -> Vec<u8> {
    let net::Event::Message(received) = within(events.recv()).await.unwrap() else {
        panic!()
    };
    assert_eq!(received.message().kind(), Kind::Transactions);
    let Value::List(values) = &received.message().fields()["txs"] else {
        panic!()
    };
    let [Value::Binary(bytes)] = values.as_slice() else {
        panic!()
    };
    bytes.clone()
}
async fn send(peer: &net::Client, bytes: Vec<u8>) {
    let message = Message::new(
        Kind::Transactions,
        1,
        [("txs".into(), Value::List(vec![Value::Binary(bytes)]))].into(),
        Default::default(),
    )
    .unwrap();
    within(peer.send(&message).unwrap()).await.unwrap();
}
async fn count(gossip: &gossip::Handle) -> usize {
    within(gossip.peek_votes(7.into(), None, 10).unwrap().wait())
        .await
        .unwrap()
        .len()
}

#[tokio::test]
async fn encrypted_gossip_replacement_keeps_votes_and_vote_worker_replacement_keeps_connections() {
    let mut node = Node::new();
    node.stop_worker();
    let (cache, mut controller) = GossipCache::managed(100, 1000).unwrap();
    let mut config = workers::Config::default();
    config.pool.consensus = ConsensusType::Pos;
    config.admission.origins_interval = None;
    let (mut workers, service) = workers::create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        config,
        cache.clone(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    within(async {
        while !workers.status().ready {
            assert!(workers.status().running);
            workers.changed().await.unwrap();
        }
    })
    .await;
    let bindings = workers.bindings();
    let old = bindings.votes.as_ref().unwrap().current().unwrap();
    let first = start(&node, &bindings, &mut controller);
    let (remote, mut events, driver) = connect(&first).await;
    let bytes = vote();
    send(&remote, bytes.clone()).await;
    assert_eq!(transaction(&mut events).await, bytes);
    assert_eq!(count(&first.gossip).await, 1);
    stop(first).await;
    within(driver).await.unwrap();
    assert!(remote.is_closed());
    assert!(old.info().running);
    let second = start(&node, &bindings, &mut controller);
    let (remote, mut events, driver) = connect(&second).await;
    assert_eq!(count(&second.gossip).await, 1);
    assert_eq!(workers.status().votes_generation, 1);
    assert!(matches!(
        within(
            second
                .gossip
                .push_vote(bytes.clone(), Event::Created)
                .unwrap()
                .wait()
        )
        .await
        .unwrap(),
        PushOutcome::Accepted {
            inserted: false,
            ..
        }
    ));
    assert_eq!(transaction(&mut events).await, bytes);
    assert_eq!(
        workers
            .restart(workers::Child::Votes, 1)
            .unwrap()
            .wait()
            .await,
        Ok(2)
    );
    assert!(old.info().joined);
    assert!(matches!(
        old.push(bytes.clone(), Event::Created),
        Err(votes::Error::Worker(votes::Fault::Stopped))
    ));
    assert_eq!(count(&second.gossip).await, 0);
    assert_eq!(
        within(
            second
                .gossip
                .push_vote(bytes.clone(), Event::Created)
                .unwrap()
                .wait()
        )
        .await
        .unwrap(),
        PushOutcome::Cached
    );
    cache.reset().unwrap();
    send(&remote, bytes.clone()).await;
    assert_eq!(transaction(&mut events).await, bytes);
    assert_eq!(count(&second.gossip).await, 1);
    assert_eq!(
        workers
            .restart(workers::Child::Admission, 1)
            .unwrap()
            .wait()
            .await,
        Ok(2)
    );
    assert_eq!(count(&second.gossip).await, 1);
    for (epoch, expected) in [(8, 1), (8, 1), (7, 1), (9, 0)] {
        within(second.gossip.advance_epoch(epoch.into()).unwrap().wait())
            .await
            .unwrap();
        assert_eq!(count(&second.gossip).await, expected);
    }
    assert!(!remote.is_closed());
    assert!(node.db.pool_view().unwrap().members(10).unwrap().is_empty());
    stop(second).await;
    within(driver).await.unwrap();
    assert!(
        bindings
            .votes
            .as_ref()
            .unwrap()
            .current()
            .unwrap()
            .info()
            .running
    );
    workers.shutdown();
    assert!(within(task).await.unwrap().joined);
    assert!(bindings.votes.unwrap().is_closed());
}
