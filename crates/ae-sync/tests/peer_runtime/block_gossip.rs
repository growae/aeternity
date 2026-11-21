use super::*;
use ae_chain::block::{Block, Light};
use ae_sync::chain::{
    self,
    sync::{self, blocks},
};
const WEIGHT: usize = 80 << 20;
struct Running {
    handle: sync::Handle,
    _forwarded: sync::Events,
    remote: net::Client,
    events: net::Events,
    driver: JoinHandle<net::Exit>,
    finish: JoinHandle<sync::Status>,
    network: network::Handle,
    identity: Arc<noise::Identity>,
    bytes: usize,
}
async fn run(node: &Node, settings: sync::Config, import: chain::Config) -> Running {
    run_mode(node, settings, import, true).await
}
async fn run_mode(
    node: &Node,
    settings: sync::Config,
    import: chain::Config,
    admitted: bool,
) -> Running {
    let app = start(node, config(), Default::default()).await;
    let (remote, events, driver) = connect(node, &app).await;
    if admitted {
        response(&remote, &ping(node, 2, true)).await;
        admitted_client(&app, remote_peer()).await;
    }
    let network = app.network.clone();
    let identity = app.identity.clone();
    let bytes = settings.block_bytes;
    let (handle, forwarded, service) = sync::create(
        download_importer(node, import),
        app.network.clone(),
        app.application.clone(),
        app.events,
        settings,
    )
    .unwrap();
    let finish = tokio::spawn(async move {
        let status = service.run().await;
        app.application.shutdown();
        assert!(within(app.application_task).await.unwrap().joined);
        app.gossip.shutdown();
        app.network.shutdown();
        assert!(!within(app.gossip_task).await.unwrap().running);
        assert!(within(app.network_task).await.unwrap().status.joined);
        status
    });
    Running {
        handle,
        _forwarded: forwarded,
        remote,
        events,
        driver,
        finish,
        network,
        identity,
        bytes,
    }
}
impl Running {
    async fn wait(&mut self, predicate: impl Fn(&blocks::Status) -> bool) -> blocks::Status {
        within(async {
            loop {
                let status = self.handle.status();
                assert!(status.running, "{status:?}");
                if predicate(&status.blocks) {
                    return status.blocks;
                }
                self.handle.changed().await.unwrap();
            }
        })
        .await
    }
    async fn stop(self) -> sync::Status {
        self.handle.shutdown();
        let status = within(self.finish).await.unwrap();
        assert!(status.joined);
        assert_eq!(status.blocks.active, 0);
        assert_eq!(status.blocks.available_bytes, self.bytes);
        let c = &status.blocks.counts;
        assert_eq!(c.received, c.started + c.refused);
        assert_eq!(c.started, c.accepted + c.ignored + c.failed + c.cancelled);
        within(self.driver).await.unwrap();
        status
    }
}
fn blocks() -> Vec<Block> {
    common::ledger_fixtures::blocks(&NetworkProfile::mainnet())
}
fn gossip(block: &Block, light: bool) -> Message {
    match block {
        Block::Key(_) => message(
            Kind::KeyBlock,
            1,
            &[("key_block", Value::Binary(block.encode().unwrap()))],
        ),
        Block::Micro(m) => message(
            Kind::MicroBlock,
            1,
            &[
                (
                    "micro_block",
                    Value::Binary(if light {
                        Light::from_micro(m).encode().unwrap()
                    } else {
                        block.encode().unwrap()
                    }),
                ),
                ("light", Value::Boolean(light)),
            ],
        ),
    }
}
async fn send(r: &Running, block: &Block, light: bool) {
    r.remote.send(&gossip(block, light)).unwrap().await.unwrap();
}
async fn expansion(events: &mut net::Events) -> net::Received {
    within(async {
        loop {
            let m = incoming(events).await;
            if m.message().kind() == Kind::GetBlockTransactions {
                return m;
            }
        }
    })
    .await
}
fn reply(block: &Block, transactions: Vec<Vec<u8>>) -> Message {
    Message::response_ok(
        &message(
            Kind::BlockTransactions,
            1,
            &[
                ("hash", Value::Binary(block.hash().unwrap().0.to_vec())),
                (
                    "txs",
                    Value::List(transactions.into_iter().map(Value::Binary).collect()),
                ),
            ],
        ),
        Default::default(),
    )
    .unwrap()
}
fn txs(block: &Block) -> Vec<Vec<u8>> {
    let Block::Micro(m) = block else { panic!() };
    m.transactions.iter().map(|t| t.encode()).collect()
}
fn assert_tip(node: &Node, block: &Block) {
    let view = node.db.view().unwrap();
    let hash = block.hash().unwrap();
    assert_eq!(view.status().tip, Some(hash));
    assert_eq!(view.require_block(hash).unwrap(), *block);
    assert_eq!(
        view.require_record(hash).unwrap().roots.state.hash().0,
        block.header().common().state_hash
    );
}
#[tokio::test]
async fn preping_full_and_light_block_gossip_validate_and_commit_before_admission() {
    for light in [false, true] {
        let node = Node::new();
        let all = blocks();
        let mut r = run_mode(&node, Default::default(), Default::default(), false).await;
        send(&r, &all[0], false).await;
        r.wait(|s| s.counts.accepted == 1).await;
        assert_tip(&node, &all[0]);
        send(&r, &all[1], light).await;
        if light {
            let request = expansion(&mut r.events).await;
            assert_eq!(
                request.message().fields()["hash"],
                Value::Binary(all[1].hash().unwrap().0.to_vec())
            );
            r.remote
                .send(&reply(&all[1], txs(&all[1])))
                .unwrap()
                .await
                .unwrap();
        }
        r.wait(|s| s.counts.accepted == 2).await;
        assert_tip(&node, &all[1]);
        assert_eq!(r.network.status().counts.accepted, 0);
        assert!(
            within(r.network.peers().peers().unwrap().wait())
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(node.db.view().unwrap().status().revision, 3);
        let generation = response(
            &r.remote,
            &message(
                Kind::GetGeneration,
                1,
                &[
                    ("hash", Value::Binary(all[0].hash().unwrap().0.to_vec())),
                    ("forward", Value::Boolean(true)),
                ],
            ),
        )
        .await;
        assert_eq!(
            body(&generation).fields()["micro_blocks"],
            Value::List(vec![Value::Binary(all[1].encode().unwrap())])
        );
        assert_eq!(r.stop().await.blocks.counts.committed, 2);
    }
}
#[tokio::test]
async fn encrypted_full_blocks_are_consumed_validated_and_committed_without_disconnecting_bad_gossip()
 {
    let node = Node::new();
    let all = blocks();
    let mut r = run(&node, Default::default(), Default::default()).await;
    let bad = message(Kind::KeyBlock, 1, &[("key_block", Value::Binary(vec![0]))]);
    r.remote.send(&bad).unwrap().await.unwrap();
    r.wait(|s| s.counts.ignored == 1).await;
    assert!(!r.remote.is_closed());
    send(&r, &all[0], false).await;
    r.wait(|s| s.counts.accepted == 1).await;
    assert_tip(&node, &all[0]);
    send(&r, &all[1], false).await;
    r.wait(|s| s.counts.accepted == 2).await;
    assert_tip(&node, &all[1]);
    let Block::Micro(mut bad) = all[1].clone() else {
        panic!()
    };
    bad.transactions.clear();
    send(&r, &Block::Micro(bad), false).await;
    let status = r.wait(|s| s.counts.ignored == 2).await;
    assert!(matches!(
        status.last.unwrap().result,
        Ok(blocks::Outcome::Ignored(blocks::Ignored::Invalid(_)))
    ));
    assert!(!r.remote.is_closed());
    assert_eq!(node.db.view().unwrap().status().revision, 3);
    assert_eq!(r.stop().await.blocks.counts.committed, 2);
}
#[tokio::test]
async fn encrypted_light_blocks_use_local_pool_or_expand_and_decode_every_reply_transaction() {
    for mode in [
        "missing",
        "local",
        "extra",
        "bad_tail",
        "wrong_hash",
        "short",
        "local_limit",
        "missing_limit",
    ] {
        let node = Node::new();
        let all = chain_sync::seed(&node, 1).await;
        let local = matches!(mode, "local" | "local_limit");
        let limited = matches!(mode, "local_limit" | "missing_limit");
        if local {
            node.client()
                .push(
                    common::transaction(),
                    ae_mempool::pool::Event::Created,
                    false,
                )
                .unwrap()
                .wait()
                .unwrap();
        }
        let mut config = sync::Config::default();
        if limited {
            config.messages.body_bytes = all[1].encode().unwrap().len() - 1;
            gossip(&all[1], true)
                .encode_packet(config.messages)
                .unwrap();
            reply(&all[1], txs(&all[1]))
                .encode_packet(config.messages)
                .unwrap();
        }
        let mut r = run(&node, config, Default::default()).await;
        send(&r, &all[1], true).await;
        if !local {
            let request = expansion(&mut r.events).await;
            assert_eq!(
                request.message().fields()["hash"],
                Value::Binary(all[1].hash().unwrap().0.to_vec())
            );
            let Block::Micro(m) = &all[1] else { panic!() };
            assert_eq!(
                request.message().fields()["tx_hashes"],
                Value::List(
                    m.transactions
                        .iter()
                        .map(|t| Value::Binary(t.hash().0.to_vec()))
                        .collect()
                )
            );
            let mut returned = txs(&all[1]);
            match mode {
                "extra" => returned.extend(txs(&all[1])),
                "bad_tail" => returned.push(vec![0]),
                "wrong_hash" => {
                    returned = vec![common::invalid_signature(&m.transactions[0]).encode()]
                }
                "short" => returned.clear(),
                _ => (),
            }
            r.remote
                .send(&reply(&all[1], returned))
                .unwrap()
                .await
                .unwrap();
        }
        let status = r
            .wait(|s| s.counts.accepted + s.counts.ignored + s.counts.failed == 1)
            .await;
        if limited {
            assert_eq!(status.last.unwrap().result, Err(sync::Error::ResourceLimit));
            assert_eq!(status.counts.failed, 1);
            assert_tip(&node, &all[0]);
        } else if matches!(mode, "missing" | "local" | "extra") {
            assert_eq!(status.counts.accepted, 1, "{mode}: {status:?}");
            assert_tip(&node, &all[1]);
            send(&r, &all[1], true).await;
            let status = r.wait(|s| s.counts.ignored == 1).await;
            assert!(matches!(
                status.last.unwrap().result,
                Ok(blocks::Outcome::Ignored(blocks::Ignored::Known))
            ));
        } else {
            assert_eq!(status.counts.ignored, 1, "{mode}: {status:?}");
            assert_tip(&node, &all[0]);
        }
        assert!(!r.remote.is_closed());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), expansion(&mut r.events))
                .await
                .is_err()
        );
        r.stop().await;
    }
}
fn two() -> (sync::Config, chain::Config) {
    (
        sync::Config {
            block_bytes: 6 * WEIGHT,
            read_bytes: 2 * WEIGHT,
            ..Default::default()
        },
        chain::Config {
            bytes: 2 * WEIGHT,
            ..Default::default()
        },
    )
}
#[tokio::test]
async fn duplicate_expansions_coalesce_but_full_micro_blocks_can_win_the_race() {
    for full in [false, true] {
        let node = Node::new();
        let all = chain_sync::seed(&node, 1).await;
        let (config, import) = two();
        let mut r = run(&node, config, import).await;
        send(&r, &all[1], true).await;
        expansion(&mut r.events).await;
        send(&r, &all[1], !full).await;
        let status = r.wait(|s| s.counts.accepted + s.counts.ignored == 1).await;
        if full {
            assert_eq!(status.counts.accepted, 1);
            assert_tip(&node, &all[1]);
        } else {
            assert!(matches!(
                status.last.unwrap().result,
                Ok(blocks::Outcome::Ignored(blocks::Ignored::Duplicate))
            ));
            assert_tip(&node, &all[0]);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), expansion(&mut r.events))
                .await
                .is_err()
        );
        r.remote
            .send(&reply(&all[1], txs(&all[1])))
            .unwrap()
            .await
            .unwrap();
        let status = r
            .wait(|s| s.active == 0 && s.counts.accepted == if full { 2 } else { 1 })
            .await;
        assert_eq!(status.counts.committed, 1);
        assert_eq!(status.counts.present, u64::from(full));
        assert_tip(&node, &all[1]);
        assert_eq!(node.db.view().unwrap().status().revision, 3);
        r.stop().await;
    }
}
#[tokio::test]
async fn block_capacity_and_pending_expansion_shutdown_refund_credits_and_join() {
    let node = Node::new();
    let all = chain_sync::seed(&node, 1).await;
    let mut r = run(
        &node,
        sync::Config {
            block_jobs: 1,
            ..Default::default()
        },
        Default::default(),
    )
    .await;
    send(&r, &all[1], true).await;
    expansion(&mut r.events).await;
    send(&r, &all[1], true).await;
    let status = r.wait(|s| s.counts.refused == 1).await;
    assert_eq!(status.active, 1);
    assert!(!r.remote.is_closed());
    assert_tip(&node, &all[0]);
    let status = r.stop().await;
    assert_eq!(status.blocks.counts.cancelled, 1);
    assert_tip(&node, &all[0]);
}
#[tokio::test]
async fn expansion_timeout_closes_only_its_connection_and_replacement_can_import() {
    let node = Node::new();
    let all = chain_sync::seed(&node, 1).await;
    let mut r = run(
        &node,
        sync::Config {
            timeout: Duration::from_millis(250),
            ..Default::default()
        },
        Default::default(),
    )
    .await;
    send(&r, &all[1], true).await;
    expansion(&mut r.events).await;
    // A refusal has no hash and cannot complete a native expansion slot.
    r.remote
        .send(
            &Message::response_error(
                Kind::BlockTransactions,
                b"tx_missing".to_vec(),
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap()
        .await
        .unwrap();
    let status = r.wait(|s| s.counts.failed == 1).await;
    let completion = status.last.unwrap();
    assert!(matches!(
        completion.result,
        Err(sync::Error::Import(chain::Failure {
            progress: None,
            error: chain::Error::Timeout,
        }))
    ));
    let old = completion.generation;
    within(r.driver).await.unwrap();
    assert_tip(&node, &all[0]);
    let socket = TcpStream::connect(r.network.status().listening.unwrap())
        .await
        .unwrap();
    let (remote, events, driver) = establish(
        &node,
        socket,
        &identities().1,
        noise::Role::Initiator(r.identity.public_key()),
    )
    .await;
    r.remote = remote;
    r.events = events;
    r.driver = driver;
    response(&r.remote, &ping(&node, 2, true)).await;
    send(&r, &all[1], true).await;
    expansion(&mut r.events).await;
    r.remote
        .send(&reply(&all[1], txs(&all[1])))
        .unwrap()
        .await
        .unwrap();
    let status = r.wait(|s| s.counts.accepted == 1).await;
    assert_ne!(status.last.unwrap().generation, old);
    assert_tip(&node, &all[1]);
    r.stop().await;
}

#[tokio::test]
async fn live_sync_posting_sets_the_connection_height_gate_and_completion_clears_it() {
    let node = Node::new();
    let source = Node::new();
    let all = chain_sync::seed(&source, 3).await;
    let mut r = run(
        &node,
        sync::Config {
            headers: 1,
            read_bytes: 2 * WEIGHT,
            ..Default::default()
        },
        Default::default(),
    )
    .await;
    response(&r.remote, &ping(&source, 2, false)).await;
    let pending = within(async {
        loop {
            let received = incoming(&mut r.events).await;
            let request = received.message();
            if request.kind() == Kind::GetSuccessors
                && request.fields()["from_hash"] == Value::Binary(all[0].hash().unwrap().0.to_vec())
            {
                break received;
            }
            if let Some(response) =
                snapshot::serve(&source.db.pool_view().unwrap(), request, Default::default())
                    .unwrap()
            {
                r.remote.send(&response).unwrap().await.unwrap();
            }
        }
    })
    .await;
    assert_tip(&node, &all[0]);
    let Block::Key(mut high) = all[2].clone() else {
        panic!()
    };
    high.common.height = 3;
    let high = Block::Key(high);
    send(&r, &high, false).await;
    let status = r.wait(|s| s.counts.ignored == 1).await;
    assert!(matches!(
        status.last.unwrap().result,
        Ok(blocks::Outcome::Ignored(blocks::Ignored::Height))
    ));
    // Height two passes the gate; its missing micro parent still prevents admission.
    send(&r, &all[2], false).await;
    let status = r.wait(|s| s.counts.ignored == 2).await;
    assert!(matches!(
        status.last.unwrap().result,
        Ok(blocks::Outcome::Ignored(blocks::Ignored::Invalid(_)))
    ));
    assert_tip(&node, &all[0]);
    assert_eq!(pending.message().kind(), Kind::GetSuccessors);
    let empty = Message::response_ok(
        &message(
            Kind::HeaderHashes,
            1,
            &[("header_hashes", Value::List(vec![]))],
        ),
        Default::default(),
    )
    .unwrap();
    r.remote.send(&empty).unwrap().await.unwrap();
    within(async {
        loop {
            if r.handle.status().counts.finished == 1 {
                break;
            }
            tokio::select! {
                _ = r.handle.changed() => (),
                event = r.events.recv() => if let Some(net::Event::Message(m)) = event
                    && let Some(reply) = snapshot::serve(
                        &source.db.pool_view().unwrap(), m.message(), Default::default(),
                    ).unwrap() {
                    r.remote.send(&reply).unwrap().await.unwrap();
                },
            }
        }
    })
    .await;
    assert_tip(&node, &all[1]);
    send(&r, &high, false).await;
    let status = r.wait(|s| s.counts.ignored == 3).await;
    assert!(matches!(
        status.last.unwrap().result,
        Ok(blocks::Outcome::Ignored(blocks::Ignored::Invalid(_)))
    ));
    assert_eq!(status.counts.committed, 0);
    r.stop().await;
}

#[test]
#[ignore = "requires the isolated pinned Erlang light-block runtime harness"]
fn native_light_block_worker() {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let node=Node::new();let all=blocks();let app=start(&node,config(),Default::default()).await;
        let network=app.network.clone();
        let (mut handle, _forwarded,service)=sync::create(download_importer(&node,Default::default()),network.clone(),app.application.clone(),app.events,Default::default()).unwrap();
        let task=tokio::spawn(service.run());
        let genesis=node.db.view().unwrap().identity().genesis;
        std::fs::write(std::env::var("AE_LIGHT_READY").unwrap(),format!("{}\n{}\n",network.status().listening.unwrap().port(),hex::encode(genesis.0))).unwrap();
        within(async{while handle.status().blocks.counts.accepted<1{assert!(handle.status().running,"{:?}",handle.status());handle.changed().await.unwrap();}}).await;
        assert_tip(&node,&all[0]);
        let client=within(async{loop{let peers=network.peers().peers().unwrap().wait().await.unwrap();if let Some(client)=peers.into_iter().next(){break client;}tokio::task::yield_now().await;}}).await;
        let local=std::env::var("AE_LIGHT_MODE").unwrap()=="local";
        if local {node.client().push(common::transaction(),ae_mempool::pool::Event::Created,false).unwrap().wait().unwrap();}
        let marker=message(Kind::GetNodeInfo,1,&[]);
        client.send(&marker).unwrap().await.unwrap();
        within(async{while handle.status().blocks.counts.accepted<2{assert!(handle.status().running,"{:?}",handle.status());handle.changed().await.unwrap();}}).await;
        assert_tip(&node,&all[1]);client.send(&marker).unwrap().await.unwrap();
        within(async{while handle.status().blocks.counts.ignored<1{assert!(handle.status().running,"{:?}",handle.status());handle.changed().await.unwrap();}}).await;
        client.send(&marker).unwrap().await.unwrap();
        within(async{while handle.status().blocks.counts.ignored<2{assert!(handle.status().running,"{:?}",handle.status());handle.changed().await.unwrap();}}).await;
        assert_eq!(node.db.view().unwrap().status().revision,3);
        let counts=handle.status().blocks.counts;
        assert_eq!((counts.accepted,counts.committed,counts.ignored,counts.failed,counts.refused),(2,2,2,0,0));
        client.send(&message(Kind::Close,1,&[])).unwrap().await.unwrap();
        handle.shutdown();let status=within(task).await.unwrap();assert!(status.joined);assert_eq!(status.blocks.available_bytes,256<<20);
        app.application.shutdown();assert!(within(app.application_task).await.unwrap().joined);
        app.gossip.shutdown();app.network.shutdown();assert!(!within(app.gossip_task).await.unwrap().running);assert!(within(app.network_task).await.unwrap().status.joined);
        println!("Original encrypted key/light gossip, {} transaction reconstruction, real ledger roots, known-light suppression, malformed full-body rejection and joined shutdown passed",if local{"local"}else{"expanded"});
    });
}
