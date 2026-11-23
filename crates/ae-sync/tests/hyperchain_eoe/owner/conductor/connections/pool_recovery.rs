use super::*;
use ae_p2p::{
    message::{Kind, Message, Value},
    ping, runtime as net,
};
use ae_sync::peer::snapshot;

#[tokio::test]
async fn encrypted_connection_keeps_its_identity_and_gossips_through_admission_replacement() {
    let node = Node::new(1);
    let (mut c, controller) = prepared(&node);
    let identity = Arc::new(noise::Identity::from_secret([92; 32]));
    let (mut handle, service) =
        connections::create_hyperchain(&c, identity.clone(), controller, settings(), || {
            Ok(Default::default())
        })
        .unwrap();
    let task = tokio::spawn(service.run());
    let generation = ready(&mut handle, 1).await;
    let socket = tokio::net::TcpStream::connect(generation.network.status().listening.unwrap())
        .await
        .unwrap();
    let binding = node.db.view().unwrap().identity().clone();
    let network = noise::Network::new(1, binding.genesis, &binding.network).unwrap();
    let remote = noise::Identity::from_secret([93; 32]);
    let (peer, _events, driver) = within(net::establish(
        socket,
        &remote,
        &network,
        noise::Role::Initiator(identity.public_key()),
        Default::default(),
    ))
    .await
    .unwrap();
    let driver = tokio::spawn(driver.run());
    let ping = ping::encode(
        &snapshot::local(&node.db.view().unwrap(), false).unwrap(),
        vec![],
        &[],
        ping::Encoding {
            version: 1,
            minimum_version: 1,
            port: 4321,
            share: 0.into(),
            limits: Default::default(),
        },
    )
    .unwrap();
    assert!(matches!(
        within(peer.request(&ping, None).unwrap()).await.unwrap(),
        net::Reply::Response(_)
    ));
    let txs = super::super::pool_supervision::transactions(1);
    let message = |tx: &ae_mempool::pool::Signed| {
        Message::new(
            Kind::Transactions,
            1,
            [("txs".into(), Value::List(vec![Value::Binary(tx.encode())]))].into(),
            Default::default(),
        )
        .unwrap()
    };
    within(peer.send(&message(&txs[0])).unwrap()).await.unwrap();
    within(async {
        while node
            .db
            .pool_view()
            .unwrap()
            .location(txs[0].hash())
            .unwrap()
            != ae_mempool::pool::Location::Pool
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    // Observe the completed first operation, not only its durable write, so
    // the following in-flight operation can only be the second packet.
    within(async {
        while generation.gossip.status().counts.ordinary_admitted != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let old = c.pool();
    c.request_pool_restart(1).unwrap();
    within(peer.send(&message(&txs[1])).unwrap()).await.unwrap();
    // Force delivery during the withdrawn binding interval. A TCP send only
    // acknowledges transport; it does not establish which pool sees admission.
    within(async {
        while generation.gossip.status().operations == 0 {
            assert_eq!(generation.gossip.status().counts.rejected, 0);
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(c.pool_binding().current().is_none());
    super::super::pool_supervision::replaced(&mut c, 2);
    within(async {
        while node
            .db
            .pool_view()
            .unwrap()
            .location(txs[1].hash())
            .unwrap()
            != ae_mempool::pool::Location::Pool
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(old.info().joined && !peer.is_closed());
    assert_eq!(handle.status().generation, generation.number);
    assert!(matches!(
        within(peer.request(&ping, None).unwrap()).await.unwrap(),
        net::Reply::Response(_)
    ));
    handle.shutdown();
    assert!(within(task).await.unwrap().joined);
    within(driver).await.unwrap();
    assert!(c.pool().info().accepting);
    assert!(shutdown(c).pool.unwrap().joined);
}
