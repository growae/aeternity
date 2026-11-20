use ae_mempool::{
    pool::{Event, Location},
    sync::Snapshot,
};
use ae_p2p::{message, noise, pool as wire, runtime as net};
use ae_sync::pool::{self, Config, Handle, Local};
use ae_types::{Hash256, PeerPublicKey};
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
mod common;
use common::*;

async fn until(handle: &mut Handle, predicate: impl Fn(&pool::Status) -> bool) -> pool::Status {
    within(async {
        loop {
            let status = handle.status();
            if predicate(&status) {
                return status;
            }
            handle.changed().await.unwrap();
        }
    })
    .await
}
async fn request(
    client: &net::Client,
    request: wire::Request,
) -> Result<wire::Reply, wire::ResponseError> {
    let message = request
        .clone()
        .into_message(message::Limits::default())
        .unwrap();
    let net::Reply::Response(response) = within(client.request(&message, None).unwrap())
        .await
        .unwrap()
    else {
        panic!()
    };
    request.response(response.message())
}
async fn next(events: &mut net::Events) -> wire::Request {
    let net::Event::Message(message) = within(events.recv()).await.unwrap() else {
        panic!()
    };
    wire::Request::from_message(message.message()).unwrap()
}
async fn answer(client: &net::Client, reply: wire::Reply) {
    assert!(matches!(
        within(
            client
                .send(&reply.into_response(message::Limits::default()).unwrap())
                .unwrap()
        )
        .await,
        Ok(net::Reply::Sent)
    ));
}
async fn stop(handle: Handle, task: JoinHandle<pool::Status>) {
    handle.shutdown();
    let status = within(task).await.unwrap();
    assert!(!status.running);
    assert_eq!(status.peers, 0);
    assert_eq!(status.operations, 0);
    assert_eq!(status.incoming, 0);
}

#[tokio::test]
async fn two_encrypted_services_synchronize_durable_membership_and_count_rejected_fetches() {
    let source = Node::new();
    let destination = Node::new();
    let good = transaction();
    let bad = invalid_signature(&good);
    source
        .client()
        .push(good.clone(), Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    source
        .client()
        .force_push(bad.clone())
        .unwrap()
        .await
        .unwrap();
    assert_eq!(source.db.pool_view().unwrap().members(10).unwrap().len(), 2);
    let (a, _ae, at) = source.service(Config::default());
    let (mut b, _be, bt) = destination.service(Config::default());
    let (ac, bc) = pair(1, 2).await;
    within(a.attach(connection(ac), false).unwrap().wait())
        .await
        .unwrap();
    within(b.attach(connection(bc), true).unwrap().wait())
        .await
        .unwrap();
    let status = until(&mut b, |s| s.counts.completed == 1).await;
    assert_eq!(status.local, Local::Cooldown);
    assert_eq!(
        (
            status.counts.fetched,
            status.counts.admitted,
            status.counts.rejected
        ),
        (2, 1, 1)
    );
    let stored = destination.db.pool_view().unwrap();
    assert_eq!(stored.location(good.hash()).unwrap(), Location::Pool);
    assert_eq!(stored.location(bad.hash()).unwrap(), Location::Unknown);
    drop(stored);
    stop(b, bt).await;
    stop(a, at).await;
    assert_eq!(
        destination.db.pool_view().unwrap().members(10).unwrap(),
        vec![good.hash()]
    );
}

#[tokio::test]
async fn incoming_reads_require_session_and_use_fresh_views_after_snapshot_creation() {
    let node = Node::new();
    let good = transaction();
    let later = invalid_signature(&good);
    node.client()
        .push(good.clone(), Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    let (handle, _events, task) = node.service(Config::default());
    let (local, (remote, _remote_events, driver)) = pair(3, 4).await;
    let driver = tokio::spawn(driver.run());
    handle
        .attach(connection(local), false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let hashes = vec![good.hash().0.to_vec()];
    assert_eq!(
        request(&remote, wire::Request::Get(hashes.clone())).await,
        Err(wire::ResponseError::Peer(b"no_active_sync".to_vec()))
    );
    assert_eq!(
        request(&remote, wire::Request::Init).await,
        Ok(wire::Reply::Init)
    );
    assert_eq!(
        request(&remote, wire::Request::Init).await,
        Err(wire::ResponseError::Peer(
            b"already_syncing_with_peer".to_vec()
        ))
    );
    node.client()
        .force_push(later.clone())
        .unwrap()
        .await
        .unwrap();
    let values = vec![
        later.hash().0.to_vec(),
        vec![0; 32],
        good.hash().0.to_vec(),
        later.hash().0.to_vec(),
    ];
    assert_eq!(
        request(&remote, wire::Request::Get(values)).await,
        Ok(wire::Reply::Transactions(vec![
            later.encode(),
            good.encode(),
            later.encode()
        ]))
    );
    assert_eq!(
        request(&remote, wire::Request::Finish(false)).await,
        Ok(wire::Reply::Finished(false))
    );
    assert_eq!(
        request(&remote, wire::Request::Get(hashes)).await,
        Err(wire::ResponseError::Peer(b"no_active_sync".to_vec()))
    );
    stop(handle, task).await;
    within(driver).await.unwrap();
}

#[tokio::test]
async fn replacing_connection_preserves_authorized_session_and_rejects_stale_owners() {
    let node = Node::new();
    let good = transaction();
    node.client()
        .force_push(good.clone())
        .unwrap()
        .await
        .unwrap();
    let (mut handle, _events, task) = node.service(Config::default());
    let (old, (old_remote, _old_events, old_driver)) = pair(5, 6).await;
    let old_monitor = old.0.monitor();
    let old_driver = tokio::spawn(old_driver.run());
    let old_id = handle
        .attach(connection(old), false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        request(&old_remote, wire::Request::Init).await,
        Ok(wire::Reply::Init)
    );
    let (new, (new_remote, _new_events, new_driver)) = pair(5, 6).await;
    let new_monitor = new.0.monitor();
    assert_eq!(old_monitor.peer(), new_monitor.peer());
    assert_ne!(old_monitor.generation(), new_monitor.generation());
    let new_driver = tokio::spawn(new_driver.run());
    let new_id = handle
        .attach(connection(new), false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_ne!(old_id, new_id);
    until(&mut handle, |s| {
        s.peers == 1 && s.counts.connections_closed == 1
    })
    .await;
    within(old_driver).await.unwrap();
    assert_eq!(
        within(old_monitor.wait()).await.reason,
        net::Error::Shutdown
    );
    assert!(new_monitor.exit().is_none());
    assert!(old_remote.is_closed());
    old_remote.shutdown();
    assert_eq!(
        request(
            &new_remote,
            wire::Request::Get(vec![good.hash().0.to_vec()])
        )
        .await,
        Ok(wire::Reply::Transactions(vec![good.encode()]))
    );
    assert_eq!(handle.status().incoming, 1);
    stop(handle, task).await;
    within(new_driver).await.unwrap();
}

#[tokio::test]
async fn whole_fetched_list_decodes_before_any_durable_submission() {
    let node = Node::new();
    let good = transaction();
    let tree = Snapshot::build([good.hash(), Hash256([99; 32])], Default::default()).unwrap();
    let (mut handle, _events, task) = node.service(Config::default());
    let (local, (remote, mut events, driver)) = pair(7, 8).await;
    let driver = tokio::spawn(driver.run());
    handle
        .attach(connection(local), true)
        .unwrap()
        .wait()
        .await
        .unwrap();
    loop {
        match next(&mut events).await {
            wire::Request::Init => answer(&remote, wire::Reply::Init).await,
            wire::Request::Unfold(unfolds) => {
                answer(
                    &remote,
                    wire::Reply::Unfold(tree.respond_wire(&unfolds, Default::default()).unwrap()),
                )
                .await
            }
            wire::Request::Get(_) => {
                answer(
                    &remote,
                    wire::Reply::Transactions(vec![good.encode(), vec![0xff]]),
                )
                .await;
                break;
            }
            other => panic!("unexpected request {other:?}"),
        }
    }
    until(&mut handle, |s| s.counts.connections_closed == 1).await;
    assert!(node.db.pool_view().unwrap().members(10).unwrap().is_empty());
    assert_eq!(node.client().info().submitted, 0);
    stop(handle, task).await;
    within(driver).await.unwrap();
}

#[tokio::test]
async fn finish_uses_original_requested_flag_despite_false_or_error_reply() {
    for error in [false, true] {
        let node = Node::new();
        let tree = Snapshot::build([], Default::default()).unwrap();
        let (mut handle, _events, task) = node.service(Config::default());
        let (local, (remote, mut events, driver)) = pair(9, 10).await;
        let driver = tokio::spawn(driver.run());
        handle
            .attach(connection(local), true)
            .unwrap()
            .wait()
            .await
            .unwrap();
        loop {
            match next(&mut events).await {
                wire::Request::Init => answer(&remote, wire::Reply::Init).await,
                wire::Request::Unfold(unfolds) => {
                    answer(
                        &remote,
                        wire::Reply::Unfold(
                            tree.respond_wire(&unfolds, Default::default()).unwrap(),
                        ),
                    )
                    .await
                }
                wire::Request::Finish(done) => {
                    assert!(done);
                    if error {
                        let response = message::Message::response_error(
                            message::Kind::PoolFinish,
                            b"rejected".to_vec(),
                            message::Limits::default(),
                        )
                        .unwrap();
                        within(remote.send(&response).unwrap()).await.unwrap();
                    } else {
                        answer(&remote, wire::Reply::Finished(false)).await;
                    }
                    break;
                }
                wire::Request::Get(hashes) => {
                    assert!(hashes.is_empty());
                    answer(&remote, wire::Reply::Transactions(vec![])).await;
                }
            }
        }
        until(&mut handle, |s| s.counts.completed == 1).await;
        assert_eq!(handle.status().local, Local::Cooldown);
        stop(handle, task).await;
        within(driver).await.unwrap();
    }
}

#[tokio::test]
async fn action_deadline_cancels_delayed_work_and_exits_without_admission() {
    let source = Node::new();
    let destination = Node::new();
    let good = transaction();
    source.client().force_push(good).unwrap().await.unwrap();
    let (a, _ae, at) = source.service(Config::default());
    let config = Config {
        action_timeout: Duration::from_millis(20),
        ..Config::default()
    };
    let (mut b, _be, bt) = destination.service(config);
    let (ac, bc) = pair(11, 12).await;
    a.attach(connection(ac), false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    b.attach(connection(bc), true)
        .unwrap()
        .wait()
        .await
        .unwrap();
    until(&mut b, |s| {
        s.counts.failures > 0 && matches!(s.local, Local::Idle)
    })
    .await;
    assert!(
        destination
            .db
            .pool_view()
            .unwrap()
            .members(10)
            .unwrap()
            .is_empty()
    );
    stop(b, bt).await;
    stop(a, at).await;
}

#[tokio::test]
async fn peer_and_command_limits_include_queued_connections_and_shutdown_releases_them() {
    let node = Node::new();
    let (handle, _events, service) = pool::create(
        node.db.clone(),
        node.client(),
        Config {
            peers: 1,
            commands: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let (first, (first_remote, _first_events, first_driver)) = pair(13, 14).await;
    let first_driver = tokio::spawn(first_driver.run());
    let queued = handle.attach(connection(first), false).unwrap();
    assert!(matches!(
        handle.synchronize(PeerPublicKey([0; 32])),
        Err(pool::Error::Busy)
    ));
    let (second, (_second_remote, _second_events, second_driver)) = pair(13, 15).await;
    let second_driver = tokio::spawn(second_driver.run());
    assert!(matches!(
        handle.attach(connection(second), false),
        Err(pool::Error::Busy)
    ));
    within(second_driver).await.unwrap();
    drop(service);
    assert!(matches!(queued.wait().await, Err(pool::Error::Closed)));
    within(first_driver).await.unwrap();
    assert!(first_remote.is_closed());
    assert!(matches!(
        handle.synchronize(PeerPublicKey([0; 32])),
        Err(pool::Error::Closed)
    ));
}

#[tokio::test]
async fn remote_tree_retry_preserves_request_and_cooldown_requires_explicit_restart() {
    let node = Node::new();
    let tree = Snapshot::build([], Default::default()).unwrap();
    let mut config = Config::default();
    config.session.resync_ms = 25;
    let (mut handle, _events, task) = node.service(config);
    let (local, (remote, mut events, driver)) = pair(16, 17).await;
    let peer = local.0.peer();
    let driver = tokio::spawn(driver.run());
    handle
        .attach(connection(local), true)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(next(&mut events).await, wire::Request::Init);
    answer(&remote, wire::Reply::Init).await;
    let wire::Request::Unfold(first) = next(&mut events).await else {
        panic!()
    };
    let started = tokio::time::Instant::now();
    let error = message::Message::response_error(
        message::Kind::PoolUnfold,
        b"tree_not_ready".to_vec(),
        Default::default(),
    )
    .unwrap();
    within(remote.send(&error).unwrap()).await.unwrap();
    assert_eq!(
        next(&mut events).await,
        wire::Request::Unfold(first.clone())
    );
    assert!(started.elapsed() >= Duration::from_millis(1050));
    answer(
        &remote,
        wire::Reply::Unfold(tree.respond_wire(&first, Default::default()).unwrap()),
    )
    .await;
    assert_eq!(next(&mut events).await, wire::Request::Get(vec![]));
    answer(&remote, wire::Reply::Transactions(vec![])).await;
    assert_eq!(next(&mut events).await, wire::Request::Finish(true));
    answer(&remote, wire::Reply::Finished(true)).await;
    until(&mut handle, |s| {
        s.counts.completed == 1 && s.local == Local::Idle
    })
    .await;
    assert_eq!(handle.status().operations, 0);
    assert!(handle.synchronize(peer).unwrap().wait().await.unwrap());
    assert_eq!(next(&mut events).await, wire::Request::Init);
    // Abort while an outbound request is waiting: the service owns and drops
    // every connection/action future, then joins its fixed blocking workers.
    task.abort();
    assert!(within(task).await.unwrap_err().is_cancelled());
    within(driver).await.unwrap();
    assert!(remote.is_closed());
    assert!(matches!(handle.synchronize(peer), Err(pool::Error::Closed)));
}

#[tokio::test]
async fn incoming_limit_precedes_duplicate_check_and_finish_releases_capacity() {
    let node = Node::new();
    let mut config = Config::default();
    config.session.incoming = 1;
    let (handle, _events, task) = node.service(config);
    let (a, (remote_a, _events_a, driver_a)) = pair(18, 19).await;
    let (b, (remote_b, _events_b, driver_b)) = pair(18, 20).await;
    let driver_a = tokio::spawn(driver_a.run());
    let driver_b = tokio::spawn(driver_b.run());
    handle
        .attach(connection(a), false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    handle
        .attach(connection(b), false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        request(&remote_a, wire::Request::Init).await,
        Ok(wire::Reply::Init)
    );
    for remote in [&remote_a, &remote_b] {
        assert_eq!(
            request(remote, wire::Request::Init).await,
            Err(wire::ResponseError::Peer(
                b"too_many_simultaneous_sync_attempts".to_vec()
            ))
        );
    }
    assert_eq!(
        request(&remote_a, wire::Request::Finish(true)).await,
        Ok(wire::Reply::Finished(true))
    );
    assert_eq!(
        request(&remote_b, wire::Request::Init).await,
        Ok(wire::Reply::Init)
    );
    stop(handle, task).await;
    within(driver_a).await.unwrap();
    within(driver_b).await.unwrap();
}

#[test]
#[ignore = "requires the isolated pinned Erlang pool synchronization harness"]
fn native_pool_runtime_worker() {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let node = Node::new();
        let role = std::env::var("AE_NOISE_ROLE").unwrap();
        let initiator = noise::Identity::from_secret([1;32]);
        let responder = noise::Identity::from_secret([2;32]);
        let network = noise::Network::new(1,Hash256([0;32]),b"ae_dev").unwrap();
        let (socket, identity, role) = if role == "initiator" {
            (within(TcpStream::connect(std::env::var("AE_NOISE_ADDRESS").unwrap())).await.unwrap(), &initiator, noise::Role::Initiator(responder.public_key()))
        } else {
            assert_eq!(role,"responder");
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            std::fs::write(std::env::var("AE_NOISE_READY").unwrap(),listener.local_addr().unwrap().port().to_string()).unwrap();
            let (socket,_) = within(listener.accept()).await.unwrap();
            (socket,&responder,noise::Role::Responder)
        };
        let established = within(net::establish(socket,identity,&network,role,net::Config::default())).await.unwrap();
        let close = established.0.clone();
        let (mut handle,_events,task) = node.service(Config::default());
        handle.attach(connection(established),true).unwrap().wait().await.unwrap();
        let status = until(&mut handle,|s|s.counts.completed==1).await;
        assert_eq!(status.local,Local::Cooldown);
        assert_eq!((status.counts.fetched,status.counts.admitted,status.counts.rejected),(1,1,0));
        assert_eq!(node.db.pool_view().unwrap().members(10).unwrap(),vec![transaction().hash()]);
        let message = message::Message::new(message::Kind::Close,1,Default::default(),Default::default()).unwrap();
        within(close.send(&message).unwrap()).await.unwrap();
        stop(handle,task).await;
        println!("Native pool tree exchange, signed transaction decode, durable admission and joined shutdown passed");
    });
}

#[tokio::test]
async fn non_pool_forwarding_retains_identity_and_overflow_closes_only_that_peer() {
    let node = Node::new();
    let config = Config {
        forwarded: 1,
        ..Config::default()
    };
    let (mut handle, mut events, task) = node.service(config);
    let (local, (remote, _remote_events, driver)) = pair(21, 22).await;
    let peer = local.0.peer();
    let generation = local.0.generation().clone();
    let driver = tokio::spawn(driver.run());
    handle
        .attach(connection(local), false)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let message = message::Message::new(
        message::Kind::Transactions,
        1,
        std::collections::BTreeMap::from([(
            "txs".into(),
            message::Value::List(vec![message::Value::Binary(transaction().encode())]),
        )]),
        Default::default(),
    )
    .unwrap();
    within(remote.send(&message).unwrap()).await.unwrap();
    let first = within(events.recv()).await.unwrap();
    assert_eq!(first.peer, peer);
    let net::Event::Message(first) = first.event else {
        panic!()
    };
    assert_eq!(first.generation, generation);
    assert_eq!(first.message(), &message);
    drop(first);
    within(remote.send(&message).unwrap()).await.unwrap();
    within(remote.send(&message).unwrap()).await.unwrap();
    until(&mut handle, |s| s.counts.connections_closed == 1).await;
    assert!(matches!(handle.status().local, Local::Idle));
    stop(handle, task).await;
    within(driver).await.unwrap();
}
