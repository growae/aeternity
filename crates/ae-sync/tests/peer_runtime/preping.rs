use super::*;
use ae_sync::peer::capabilities::CHAIN_POI;

fn header(node: &Node) -> Message {
    message(
        Kind::GetHeaderByHash,
        1,
        &[(
            "hash",
            Value::Binary(node.db.view().unwrap().identity().genesis.0.to_vec()),
        )],
    )
}

#[tokio::test]
async fn authenticated_requests_finish_before_any_first_ping_and_remain_outside_peer_selection() {
    let node = Node::new();
    let mut app = start(&node, config(), Default::default()).await;
    let (remote, mut events, task) = connect(&node, &app).await;
    let tx = common::transaction();
    remote
        .send(&message(
            Kind::Transactions,
            1,
            &[("txs", Value::List(vec![Value::Binary(tx.encode())]))],
        ))
        .unwrap()
        .await
        .unwrap();
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
    // Provisional recipients are excluded from ordinary transaction fanout.
    assert!(
        tokio::time::timeout(Duration::from_millis(30), events.recv())
            .await
            .is_err()
    );
    assert_eq!(
        body(&response(&remote, &message(Kind::GetNodeInfo, 1, &[])).await).kind(),
        Kind::NodeInfo
    );
    let view = node.db.view().unwrap();
    let genesis = view.identity().genesis;
    let encoded = view
        .require_block(genesis)
        .unwrap()
        .header()
        .encode()
        .unwrap();
    assert_eq!(
        body(&response(&remote, &header(&node)).await).fields()["hdr"],
        Value::Binary(encoded.clone())
    );
    for version in [1, 2] {
        let mut fields = vec![("height", Value::Integer(0.into()))];
        if version == 2 {
            fields.push(("top_hash", Value::Binary(genesis.0.to_vec())));
        }
        assert_eq!(
            body(&response(&remote, &message(Kind::GetHeaderByHeight, version, &fields)).await)
                .fields()["hdr"],
            Value::Binary(encoded.clone())
        );
        let mut fields = vec![
            (
                if version == 1 { "hash" } else { "from_hash" },
                Value::Binary(genesis.0.to_vec()),
            ),
            ("n", Value::Integer(8.into())),
        ];
        if version == 2 {
            fields.push(("target_hash", Value::Binary(genesis.0.to_vec())));
        }
        assert_eq!(
            body(&response(&remote, &message(Kind::GetSuccessors, version, &fields)).await)
                .fields()["header_hashes"],
            Value::List(vec![])
        );
    }
    for forward in [false, true] {
        let result = response(
            &remote,
            &message(
                Kind::GetGeneration,
                1,
                &[
                    ("hash", Value::Binary(genesis.0.to_vec())),
                    ("forward", Value::Boolean(forward)),
                ],
            ),
        )
        .await;
        assert_eq!(
            body(&result).fields()["key_block"],
            Value::Binary(view.require_block(genesis).unwrap().encode().unwrap())
        );
        assert_eq!(body(&result).fields()["micro_blocks"], Value::List(vec![]));
    }
    let hashes = Value::List(vec![Value::Binary(tx.hash().0.to_vec())]);
    let expanded = response(
        &remote,
        &message(
            Kind::GetBlockTransactions,
            1,
            &[
                ("hash", Value::Binary(vec![123; 32])),
                ("tx_hashes", hashes.clone()),
            ],
        ),
    )
    .await;
    assert_eq!(
        body(&expanded).fields()["txs"],
        Value::List(vec![Value::Binary(tx.encode())])
    );
    assert_eq!(
        body(&response(&remote, &message(Kind::PoolInit, 1, &[])).await).kind(),
        Kind::PoolInit
    );
    within(async {
        loop {
            let reply = response(
                &remote,
                &message(Kind::PoolUnfold, 1, &[("unfolds", Value::List(vec![]))]),
            )
            .await;
            if matches!(
                reply.message().details(),
                Details::Response {
                    message: Some(Ok(_)),
                    ..
                }
            ) {
                assert_eq!(body(&reply).fields()["unfolds"], Value::List(vec![]));
                break;
            }
            assert_eq!(
                reply.message().fields()["reason"],
                Value::Binary(b"tree_not_ready".to_vec())
            );
            tokio::task::yield_now().await;
        }
    })
    .await;
    let fetched = response(
        &remote,
        &message(Kind::PoolGet, 1, &[("tx_hashes", hashes)]),
    )
    .await;
    assert_eq!(
        body(&fetched).fields()["txs"],
        Value::List(vec![Value::Binary(tx.encode())])
    );
    assert_eq!(
        body(
            &response(
                &remote,
                &message(Kind::PoolFinish, 1, &[("done", Value::Boolean(true))])
            )
            .await
        )
        .kind(),
        Kind::PoolFinish
    );
    drop(view);
    assert_eq!(app.network.status().counts.accepted, 0);
    assert!(
        within(app.network.peers().peers().unwrap().wait())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        body(&response(&remote, &ping(&node, 2, true)).await).kind(),
        Kind::Ping
    );
    assert_eq!(app.network.status().counts.accepted, 1);
    assert_eq!(
        within(app.network.peers().peers().unwrap().wait())
            .await
            .unwrap()
            .len(),
        1
    );
    stop(app).await;
    within(task).await.unwrap();
}

#[tokio::test]
async fn queued_request_precedes_blocked_first_ping_response_and_retirement() {
    let node = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (_, identity) = identities();
    within(
        app.network
            .command(Command::Block(Info {
                id: identity.public_key(),
                host: b"127.0.0.1".to_vec(),
                port: 4321,
            }))
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    let (remote, _events, task) = connect(&node, &app).await;
    let earlier = remote
        .request(&message(Kind::GetNodeInfo, 1, &[]), None)
        .unwrap();
    let rejected = remote.request(&ping(&node, 1, true), None).unwrap();
    let later = remote.request(&header(&node), None).unwrap();
    let net::Reply::Response(earlier) = within(earlier).await.unwrap() else {
        panic!()
    };
    assert_eq!(body(&earlier).kind(), Kind::NodeInfo);
    let net::Reply::Response(rejected) = within(rejected).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        rejected.message().fields()["reason"],
        Value::Binary(b"blocked".to_vec())
    );
    assert!(within(later).await.is_err());
    within(task).await.unwrap();
    assert_eq!(app.network.status().counts.accepted, 0);
    assert!(app.application.status().running);
    stop(app).await;
}

#[tokio::test]
async fn provisional_same_key_does_not_replace_admitted_peer_or_its_capabilities() {
    let node = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (old, _events, old_task) = connect(&node, &app).await;
    let announced = alter(
        &ping(&node, 2, true),
        &[(
            "capabilities",
            Value::Binary(message::encode_capabilities(&[cap(7)], Default::default()).unwrap()),
        )],
    );
    response(&old, &announced).await;
    response(&old, &announced).await;
    let before = app
        .application
        .peers_with_capability(CHAIN_POI)
        .await
        .unwrap();
    assert_eq!(before.len(), 1);
    let (provisional, _events, provisional_task) = connect(&node, &app).await;
    assert_eq!(
        body(&response(&provisional, &message(Kind::GetNodeInfo, 1, &[])).await).kind(),
        Kind::NodeInfo
    );
    let peers = within(app.network.peers().peers().unwrap().wait())
        .await
        .unwrap();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].generation(), &before[0].generation);
    assert_eq!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap(),
        before
    );
    assert!(!old.is_closed());
    provisional.shutdown();
    within(provisional_task).await.unwrap();
    assert_eq!(
        body(&response(&old, &message(Kind::GetNodeInfo, 1, &[])).await).kind(),
        Kind::NodeInfo
    );
    assert_eq!(
        app.application
            .peers_with_capability(CHAIN_POI)
            .await
            .unwrap(),
        before
    );
    let (replacement, _events, replacement_task) = connect(&node, &app).await;
    response(&replacement, &message(Kind::GetNodeInfo, 1, &[])).await;
    response(
        &replacement,
        &alter(&announced, &[("port", Value::Integer(4322.into()))]),
    )
    .await;
    within(old_task).await.unwrap();
    response(&replacement, &announced).await;
    let after = app
        .application
        .peers_with_capability(CHAIN_POI)
        .await
        .unwrap();
    assert_eq!(after.len(), 1);
    assert_ne!(after[0].generation, before[0].generation);
    assert_eq!(after[0].value, before[0].value);
    stop(app).await;
    within(replacement_task).await.unwrap();
}

#[tokio::test]
async fn serving_pre_ping_requests_does_not_extend_the_first_ping_deadline() {
    let node = Node::new();
    let mut c = config();
    c.transport.first_ping_timeout = Duration::from_millis(300);
    let app = start(&node, c, Default::default()).await;
    let (remote, _events, task) = connect(&node, &app).await;
    response(&remote, &message(Kind::GetNodeInfo, 1, &[])).await;
    assert_eq!(app.network.status().counts.accepted, 0);
    within(task).await.unwrap();
    assert!(app.application.status().running);
    stop(app).await;
}
