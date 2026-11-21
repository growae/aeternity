use super::*;
use ae_chain::block::Block;
use ae_sync::chain::{self, sync};

pub(super) async fn seed(node: &Node, count: usize) -> Vec<Block> {
    let all = common::ledger_fixtures::blocks(&NetworkProfile::mainnet());
    let importer = download_importer(node, Default::default());
    let mut last = &all[0];
    for block in all.iter().take(count) {
        let (key, micros, direction) = if matches!(block, Block::Key(_)) {
            last = block;
            (block, &[][..], chain::Direction::Backward)
        } else {
            (last, std::slice::from_ref(block), chain::Direction::Forward)
        };
        importer
            .import(
                common::import::packet(key, micros, direction),
                key.hash().unwrap(),
                direction,
                u64::MAX - 600_000,
            )
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    importer.stop();
    importer.join().await;
    all
}
fn synchronize(
    node: &Node,
    app: App,
    config: sync::Config,
) -> (
    sync::Handle,
    sync::Events,
    impl std::future::Future<Output = sync::Status>,
) {
    synchronize_with_importer(node, app, config, Default::default())
}
pub(super) fn synchronize_with_importer(
    node: &Node,
    app: App,
    config: sync::Config,
    importer: chain::Config,
) -> (
    sync::Handle,
    sync::Events,
    impl std::future::Future<Output = sync::Status>,
) {
    let (handle, events, service) = sync::create(
        download_importer(node, importer),
        app.network.clone(),
        app.application.clone(),
        app.events,
        config,
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let finish = async move {
        let status = within(task).await.unwrap();
        app.application.shutdown();
        assert!(within(app.application_task).await.unwrap().joined);
        app.gossip.shutdown();
        app.network.shutdown();
        assert!(!within(app.gossip_task).await.unwrap().running);
        assert!(within(app.network_task).await.unwrap().status.joined);
        status
    };
    (handle, events, finish)
}
async fn serve_until_finished(
    handle: &mut sync::Handle,
    remote: &net::Client,
    events: &mut net::Events,
    source: &Node,
    exhausted: bool,
    micro_header: bool,
) -> Vec<Kind> {
    let mut requests = vec![];
    within(async {
        loop {
            if handle.status().counts.finished+handle.status().counts.failed>0 { break; }
            tokio::select! {
                _=handle.changed()=>(),
                event=events.recv()=>{
                    let net::Event::Message(received)=event.unwrap() else {continue};
                    let request=received.message();
                    let reply=if exhausted && request.kind()==Kind::GetSuccessors {
                        let body=message(Kind::HeaderHashes,1,&[("header_hashes",Value::List(vec![]))]);
                        Some(Message::response_ok(&body,Default::default()).unwrap())
                    } else if micro_header && request.kind()==Kind::GetHeaderByHeight {
                        let header=common::ledger_fixtures::blocks(&NetworkProfile::mainnet())[1].header().encode().unwrap();
                        let body=message(Kind::Header,1,&[("hdr",Value::Binary(header))]);
                        Some(Message::response_ok(&body,Default::default()).unwrap())
                    } else { snapshot::serve(&source.db.pool_view().unwrap(),request,Default::default()).unwrap() };
                    if let Some(reply)=reply {
                        requests.push(request.kind());
                        remote.send(&reply).unwrap().await.unwrap();
                    }
                }
            }
        }
    }).await;
    requests
}
#[tokio::test]
async fn actual_sync_consumes_start_intents_agrees_batches_imports_and_forwards_events() {
    for prefix in [0, 1, 2, 3] {
        let node = Node::new();
        let source = Node::new();
        let all = seed(&source, usize::MAX).await;
        seed(&node, prefix).await;
        let app = start(&node, config(), Default::default()).await;
        let (remote, mut events, driver) = connect(&node, &app).await;
        let (mut sync, mut forwarded, finish) = synchronize(
            &node,
            app,
            sync::Config {
                headers: 1,
                ..Default::default()
            },
        );
        let _ = response(&remote, &ping(&source, 1, false)).await;
        let requests =
            serve_until_finished(&mut sync, &remote, &mut events, &source, false, prefix == 2)
                .await;
        let status = sync.status();
        if prefix == 2 {
            // The native agreement accepts this stored micro hash. Its next
            // successor request is refused by the original serving semantics,
            // rather than being rejected locally as a non-key ancestor.
            let completion = status.last.unwrap();
            assert_eq!(
                completion.result,
                Err(sync::Error::Refused(b"block_not_found".to_vec()))
            );
            assert_eq!(
                completion.progress.agreed.unwrap().hash,
                all[1].hash().unwrap()
            );
            assert_eq!(completion.progress.committed, 0);
            assert_eq!(
                node.db.view().unwrap().status().tip,
                Some(all[1].hash().unwrap())
            );
            assert!(requests.contains(&Kind::GetSuccessors));
            sync.shutdown();
            assert!(within(finish).await.joined);
            within(driver).await.unwrap();
            continue;
        }
        assert_eq!(status.counts.failed, 0, "{:?}", status.last);
        assert_eq!(status.counts.finished, 1);
        let report = status.last.unwrap();
        assert_eq!(
            report.result,
            Ok(sync::Finish::Exhausted {
                target_selected: true
            })
        );
        assert_eq!(report.progress.committed, (all.len() - prefix) as u64);
        assert!(requests.contains(&Kind::GetHeaderByHash));
        assert!(requests.contains(&Kind::GetSuccessors));
        if prefix > 0 {
            assert!(requests.contains(&Kind::GetHeaderByHeight));
        }
        assert_eq!(
            node.db.view().unwrap().status().tip,
            source.db.view().unwrap().status().tip
        );
        for block in &all {
            assert_eq!(
                node.db
                    .view()
                    .unwrap()
                    .require_record(block.hash().unwrap())
                    .unwrap()
                    .roots
                    .state
                    .hash()
                    .0,
                block.header().common().state_hash
            );
        }
        assert!(matches!(
            within(forwarded.recv()).await.unwrap().kind(),
            peer::EventKind::Status { .. }
        ));
        assert!(matches!(
            within(forwarded.recv()).await.unwrap().kind(),
            peer::EventKind::Chain {
                intent: peer::ChainIntent::Start { .. },
                ..
            }
        ));
        sync.shutdown();
        let status = within(finish).await;
        assert!(status.joined);
        assert!(!status.running);
        within(driver).await.unwrap();
    }
}
#[tokio::test]
async fn empty_successors_cannot_claim_target_selected_or_enable_resilience() {
    let node = Node::new();
    let source = Node::new();
    seed(&source, usize::MAX).await;
    let before = node.db.view().unwrap().status();
    let app = start(&node, config(), Default::default()).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let (mut sync, _forwarded, finish) = synchronize(&node, app, Default::default());
    let _ = response(&remote, &ping(&source, 1, false)).await;
    serve_until_finished(&mut sync, &remote, &mut events, &source, true, false).await;
    let completion = sync.status().last.unwrap();
    assert_eq!(
        completion.result,
        Ok(sync::Finish::Exhausted {
            target_selected: false
        })
    );
    assert_eq!(completion.progress.committed, 0);
    assert_eq!(node.db.view().unwrap().status(), before);
    assert!(!node.db.view().unwrap().resilience().state.synchronized());
    sync.shutdown();
    assert!(within(finish).await.joined);
    within(driver).await.unwrap();
}
#[tokio::test]
async fn sync_shutdown_cancels_pending_header_requests_and_joins_without_importing() {
    let node = Node::new();
    let source = Node::new();
    seed(&source, 1).await;
    let before = node.db.view().unwrap().status();
    let app = start(&node, config(), Default::default()).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let (sync, _forwarded, finish) = synchronize(&node, app, Default::default());
    let _ = response(&remote, &ping(&source, 1, false)).await;
    within(async {
        loop {
            if incoming(&mut events).await.message().kind() == Kind::GetHeaderByHash {
                break;
            }
        }
    })
    .await;
    sync.shutdown();
    let status = within(finish).await;
    assert!(status.joined);
    assert!(status.active.is_empty());
    within(driver).await.unwrap();
    assert_eq!(node.db.view().unwrap().status(), before);
}
#[tokio::test]
async fn forwarded_event_backpressure_stops_sync_without_discarding_status_silently() {
    let node = Node::new();
    let source = Node::new();
    seed(&source, 1).await;
    let app = start(&node, config(), Default::default()).await;
    let (remote, _events, driver) = connect(&node, &app).await;
    let (_sync, mut forwarded, finish) = synchronize(
        &node,
        app,
        sync::Config {
            forwarded: 1,
            ..Default::default()
        },
    );
    let _ = remote
        .request(&ping(&source, 1, false), None)
        .unwrap()
        .await;
    let status = within(finish).await;
    assert!(status.joined);
    assert_eq!(status.last_error, Some(sync::Error::Busy));
    assert_eq!(status.counts.started, 0);
    assert!(matches!(
        forwarded.recv().await.unwrap().kind(),
        peer::EventKind::Status { .. }
    ));
    within(driver).await.unwrap();
}

#[tokio::test]
async fn multiple_peers_share_import_capacity_queue_fairly_and_coalesce_duplicate_targets() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    for concurrent in [1, 2] {
        let node = Node::new();
        let source = Node::new();
        seed(&source, usize::MAX).await;
        let app = start(&node, config(), Default::default()).await;
        let (first, first_events, first_driver) = connect(&node, &app).await;
        let id = noise::Identity::from_secret([9; 32]);
        let socket = TcpStream::connect(app.network.status().listening.unwrap())
            .await
            .unwrap();
        let (second, second_events, second_driver) = establish(
            &node,
            socket,
            &id,
            noise::Role::Initiator(app.identity.public_key()),
        )
        .await;
        let (mut sync, _forwarded, finish) = synchronize(
            &node,
            app,
            sync::Config {
                concurrent,
                ..Default::default()
            },
        );
        let held = Arc::new(AtomicBool::new(false));
        let headers = Arc::new(AtomicUsize::new(0));
        let (entered, mut observed) = tokio::sync::mpsc::channel(1);
        let (release, wait) = tokio::sync::watch::channel(false);
        let mut servers = vec![];
        for (remote, mut events) in [
            (first.clone(), first_events),
            (second.clone(), second_events),
        ] {
            let db = source.db.clone();
            let held = held.clone();
            let entered = entered.clone();
            let mut wait = wait.clone();
            let headers = headers.clone();
            servers.push(tokio::spawn(async move {
                while let Some(event) = events.recv().await {
                    let net::Event::Message(received) = event else {
                        continue;
                    };
                    if received.message().kind() == Kind::GetHeaderByHash {
                        headers.fetch_add(1, Ordering::SeqCst);
                    }
                    if received.message().kind() == Kind::GetGeneration
                        && !held.swap(true, Ordering::SeqCst)
                    {
                        entered.send(()).await.unwrap();
                        wait.wait_for(|open| *open).await.unwrap();
                    }
                    let reply = snapshot::serve(
                        &db.pool_view().unwrap(),
                        received.message(),
                        Default::default(),
                    )
                    .unwrap();
                    if let Some(reply) = reply {
                        remote.send(&reply).unwrap().await.unwrap();
                    }
                }
            }));
        }
        let _ = response(&first, &ping(&source, 1, false)).await;
        let _ = response(&second, &ping(&source, 1, false)).await;
        within(observed.recv()).await.unwrap();
        within(async {
            loop {
                let status = sync.status();
                assert!(status.active.len() <= concurrent);
                if concurrent == 1 && status.queued == 1 {
                    assert_eq!(headers.load(Ordering::SeqCst), 1);
                    break;
                }
                if concurrent == 2 && status.active.iter().any(|a| a.progress.retries > 0) {
                    break;
                }
                sync.changed().await.unwrap();
            }
        })
        .await;
        let _ = response(&first, &ping(&source, 1, false)).await;
        let _ = response(&second, &ping(&source, 1, false)).await;
        within(async {
            while sync.status().counts.coalesced < 2 {
                sync.changed().await.unwrap();
            }
        })
        .await;
        release.send_replace(true);
        within(async {
            while sync.status().counts.finished + sync.status().counts.failed < 2 {
                sync.changed().await.unwrap();
            }
        })
        .await;
        assert_eq!(sync.status().counts.failed, 0, "{:?}", sync.status().last);
        assert_eq!(sync.status().counts.started, 2);
        assert_eq!(
            node.db.view().unwrap().status().tip,
            source.db.view().unwrap().status().tip
        );
        sync.shutdown();
        assert!(within(finish).await.joined);
        within(first_driver).await.unwrap();
        within(second_driver).await.unwrap();
        for server in servers {
            within(server).await.unwrap();
        }
    }
}

#[tokio::test]
async fn matching_top_done_intent_waits_for_forward_generation_import() {
    let node = Node::new();
    let source = Node::new();
    let all = seed(&source, 2).await;
    seed(&node, 1).await;
    let app = start(&node, config(), Default::default()).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let (mut sync, _forwarded, finish) = synchronize(&node, app, Default::default());
    let _ = response(&remote, &ping(&source, 1, false)).await;
    let request = within(generation_request(&mut events)).await;
    assert_eq!(sync.status().counts.finished, 0);
    assert_eq!(
        node.db.view().unwrap().status().tip,
        Some(all[0].hash().unwrap())
    );
    within(async {
        while sync.status().counts.done_announcements == 0 {
            sync.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(sync.status().counts.finished, 0);
    let reply = snapshot::serve(
        &source.db.pool_view().unwrap(),
        request.message(),
        Default::default(),
    )
    .unwrap()
    .unwrap();
    remote.send(&reply).unwrap().await.unwrap();
    within(async {
        while sync.status().counts.finished + sync.status().counts.failed == 0 {
            sync.changed().await.unwrap();
        }
    })
    .await;
    let completion = sync.status().last.unwrap();
    assert_eq!(
        completion.result,
        Ok(sync::Finish::Forward {
            requested: all[0].hash().unwrap()
        })
    );
    assert_eq!(completion.progress.committed, 1);
    assert_eq!(
        node.db.view().unwrap().status().tip,
        Some(all[1].hash().unwrap())
    );
    sync.shutdown();
    assert!(within(finish).await.joined);
    within(driver).await.unwrap();
}

#[tokio::test]
async fn header_timeout_and_negative_successors_leave_committed_storage_unchanged() {
    for timeout in [true, false] {
        let node = Node::new();
        let source = Node::new();
        seed(&source, 1).await;
        let before = node.db.view().unwrap().status();
        let app = start(&node, config(), Default::default()).await;
        let (remote, mut events, driver) = connect(&node, &app).await;
        let (mut sync, _forwarded, finish) = synchronize(
            &node,
            app,
            sync::Config {
                timeout: Duration::from_millis(500),
                ..Default::default()
            },
        );
        let _ = response(&remote, &ping(&source, 1, false)).await;
        loop {
            let request = incoming(&mut events).await;
            if request.message().kind() == Kind::GetHeaderByHash {
                if timeout {
                    break;
                }
                let reply = snapshot::serve(
                    &source.db.pool_view().unwrap(),
                    request.message(),
                    Default::default(),
                )
                .unwrap()
                .unwrap();
                remote.send(&reply).unwrap().await.unwrap();
            } else if request.message().kind() == Kind::GetSuccessors {
                remote
                    .send(
                        &Message::response_error(
                            Kind::HeaderHashes,
                            b"not_on_chain".to_vec(),
                            Default::default(),
                        )
                        .unwrap(),
                    )
                    .unwrap()
                    .await
                    .unwrap();
                break;
            }
        }
        within(async {
            while sync.status().counts.failed == 0 {
                sync.changed().await.unwrap();
            }
        })
        .await;
        let result = sync.status().last.unwrap().result;
        assert_eq!(
            result,
            Err(if timeout {
                sync::Error::Timeout
            } else {
                sync::Error::Refused(b"not_on_chain".to_vec())
            })
        );
        assert_eq!(node.db.view().unwrap().status(), before);
        sync.shutdown();
        assert!(within(finish).await.joined);
        within(driver).await.unwrap();
    }
}

#[tokio::test]
async fn aborting_sync_owner_closes_pending_generation_and_publishes_stopped_state() {
    let node = Node::new();
    let source = Node::new();
    seed(&source, 1).await;
    let before = node.db.view().unwrap().status();
    let app = start(&node, config(), Default::default()).await;
    let (remote, mut events, driver) = connect(&node, &app).await;
    let (sync, _forwarded, service) = sync::create(
        download_importer(&node, Default::default()),
        app.network.clone(),
        app.application.clone(),
        app.events,
        Default::default(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let _ = response(&remote, &ping(&source, 1, false)).await;
    within(async {
        loop {
            if incoming(&mut events).await.message().kind() == Kind::GetHeaderByHash {
                break;
            }
        }
    })
    .await;
    task.abort();
    assert!(within(task).await.unwrap_err().is_cancelled());
    assert!(!sync.status().running);
    assert!(!sync.status().joined);
    assert!(sync.status().active.is_empty());
    within(driver).await.unwrap();
    assert_eq!(node.db.view().unwrap().status(), before);
    app.application.shutdown();
    assert!(within(app.application_task).await.unwrap().joined);
    app.gossip.shutdown();
    app.network.shutdown();
    assert!(!within(app.gossip_task).await.unwrap().running);
    assert!(within(app.network_task).await.unwrap().status.joined);
}

#[tokio::test]
async fn failed_connection_can_be_replaced_by_a_new_generation_and_finish_sync() {
    let node = Node::new();
    let source = Node::new();
    seed(&source, usize::MAX).await;
    let app = start(&node, config(), Default::default()).await;
    let address = app.network.status().listening.unwrap();
    let identity = app.identity.public_key();
    let (old, mut old_events, old_driver) = connect(&node, &app).await;
    let (mut sync, _forwarded, finish) = synchronize(&node, app, Default::default());
    let _ = response(&old, &ping(&source, 1, false)).await;
    within(async {
        loop {
            if incoming(&mut old_events).await.message().kind() == Kind::GetHeaderByHash {
                break;
            }
        }
    })
    .await;
    old.shutdown();
    within(old_driver).await.unwrap();
    within(async {
        while sync.status().counts.failed == 0 {
            sync.changed().await.unwrap();
        }
    })
    .await;
    let failed = sync.status().last.unwrap();
    assert!(failed.result.is_err());
    let socket = TcpStream::connect(address).await.unwrap();
    let (remote, mut events, driver) = establish(
        &node,
        socket,
        &identities().1,
        noise::Role::Initiator(identity),
    )
    .await;
    let _ = response(&remote, &ping(&source, 1, false)).await;
    within(async {loop {
        if sync.status().counts.finished>0 {break;}
        tokio::select! {
            _=sync.changed()=>(),
            event=events.recv()=>{
                let net::Event::Message(received)=event.unwrap() else {continue};
                let reply=snapshot::serve(&source.db.pool_view().unwrap(),received.message(),Default::default()).unwrap();
                if let Some(reply)=reply {remote.send(&reply).unwrap().await.unwrap();}
            }
        }
    }}).await;
    let completion = sync.status().last.unwrap();
    assert_ne!(completion.generation, failed.generation);
    assert_eq!(
        completion.result,
        Ok(sync::Finish::Exhausted {
            target_selected: true
        })
    );
    assert_eq!(sync.status().counts.failed, 1);
    assert_eq!(
        node.db.view().unwrap().status().tip,
        source.db.view().unwrap().status().tip
    );
    sync.shutdown();
    assert!(within(finish).await.joined);
    within(driver).await.unwrap();
}
