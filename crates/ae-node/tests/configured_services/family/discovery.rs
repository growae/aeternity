use super::*;
use ae_chain::block::{Block, Light};
use ae_p2p::{
    message::{Kind as WireKind, Message, Value as WireValue},
    noise, ping, runtime as net,
};
use ae_sync::chain::sync::{self, blocks};
use std::collections::BTreeMap;

fn configure(p: &mut node::Prepared, s: &mut node::Settings, limit: usize) {
    let cfg = Configuration::from_value(json!({"sync":{
        "max_gossip":limit,"gossip_allowed_height_from_top":0
    }}))
    .unwrap();
    let ctx = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    p.configure_discovery(&cfg.resolve(&ctx).unwrap(), s)
        .unwrap();
}

#[test]
fn configured_discovery_rejects_wrong_family_without_mutation() {
    let lab = Lab::new();
    let mut p = prepare(lab.services("on_demand", json!({}), Default::default()));
    let mut s = cuckoo(&lab);
    let cfg = Configuration::empty();
    let ctx = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    assert_eq!(
        p.configure_discovery(&cfg.resolve(&ctx).unwrap(), &mut s),
        Err(ae_node::configuration::discovery::Error::Family)
    );
    let node::Settings::Cuckoo(s) = s else {
        unreachable!()
    };
    assert_eq!(s.config.connections.gossip.policy.max_peers, 16);
    assert_eq!(s.config.connections.importer.ledger.policy.gossip_depth, 5);
}

#[tokio::test]
async fn configured_discovery_controls_real_fanout_and_both_stale_block_paths() {
    for limit in [0, 1, 2] {
        let lab = Lab::new();
        let mut p = prepare(lab.public());
        let db = p.configured().chain().database().clone();
        let tx = transfer(p.configured());
        let mut s = cuckoo(&lab);
        configure(&mut p, &mut s, limit);
        let running = p.launch(s, || Ok(Default::default())).unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        let bindings = h.pow().unwrap().workers().unwrap().bindings();
        let local = bindings
            .keys
            .as_ref()
            .unwrap()
            .current()
            .unwrap()
            .peer_identity()
            .unwrap()
            .await
            .unwrap()
            .public_key();
        let mut generation = connection_ready(&mut h.connections().unwrap(), 1).await;
        while !generation.network.status().ready {
            generation.network.changed().await.unwrap();
        }
        let binding = db.view().unwrap().identity().clone();
        let network = noise::Network::new(1, binding.genesis, &binding.network).unwrap();
        let request = ping::encode(
            &ae_sync::peer::snapshot::local(&db.view().unwrap(), false).unwrap(),
            vec![],
            &[],
            ping::Encoding {
                version: 2,
                minimum_version: 1,
                port: 4321,
                share: 0.into(),
                limits: Default::default(),
            },
        )
        .unwrap();
        let mut peers = vec![];
        let mut events = vec![];
        let mut drivers = vec![];
        for secret in [131, 132] {
            let socket =
                tokio::net::TcpStream::connect(generation.network.status().listening.unwrap())
                    .await
                    .unwrap();
            let (peer, incoming, driver) = net::establish(
                socket,
                &noise::Identity::from_secret([secret; 32]),
                &network,
                noise::Role::Initiator(local),
                Default::default(),
            )
            .await
            .unwrap();
            drivers.push(tokio::spawn(driver.run()));
            assert!(matches!(
                within(peer.request(&request, None).unwrap()).await.unwrap(),
                net::Reply::Response(_)
            ));
            peers.push(peer);
            events.push(incoming);
        }
        within(async {
            while generation
                .network
                .metrics()
                .unwrap()
                .wait()
                .await
                .unwrap()
                .registry
                .counts
                .inbound
                != 2
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let importer = h.importer().unwrap();
        let blocks = fixtures::blocks(&ae_protocol::NetworkProfile::mainnet());
        within(
            importer
                .import_block(blocks[0].encode().unwrap(), u64::MAX - 600_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        within(async {
            while generation.propagation.status().records < 1 {
                generation.propagation.changed().await.unwrap();
            }
        })
        .await;
        assert_eq!(generation.propagation.status().sent, limit as u64);
        assert_eq!(generation.propagation.status().failed, 0);
        // Forced publication exercises the actual committed pool event and its
        // encrypted fanout; ordinary transaction validity is tested separately.
        within(
            bindings
                .admission
                .current()
                .unwrap()
                .client
                .force_push(tx.clone())
                .unwrap(),
        )
        .await
        .unwrap();
        within(async {
            while generation.gossip.status().counts.fanout_events < 1 {
                generation.gossip.changed().await.unwrap();
            }
        })
        .await;
        assert_eq!(generation.gossip.status().counts.sent, limit as u64);
        let (mut key_messages, mut tx_messages) = (0, 0);
        for (peer, incoming) in peers.iter().zip(&mut events) {
            // A response on the same encrypted stream is a barrier after both
            // services have completed their send tickets.
            within(peer.request(&request, None).unwrap()).await.unwrap();
            while let Ok(event) = incoming.try_recv() {
                let net::Event::Message(message) = event else {
                    continue;
                };
                let message = message.message();
                match message.kind() {
                    WireKind::KeyBlock => {
                        key_messages += 1;
                        assert_eq!(
                            message.fields()["key_block"],
                            WireValue::Binary(blocks[0].encode().unwrap())
                        );
                    }
                    WireKind::Transactions => {
                        tx_messages += 1;
                        assert_eq!(
                            message.fields()["txs"],
                            WireValue::List(vec![WireValue::Binary(tx.encode())])
                        );
                    }
                    _ => {}
                }
            }
        }
        assert_eq!((key_messages, tx_messages), (limit, limit));
        for block in &blocks[1..3] {
            within(
                importer
                    .import_block(block.encode().unwrap(), u64::MAX - 600_000)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
        }
        let Block::Micro(mut stale) = blocks[1].clone() else {
            panic!()
        };
        stale.header.time += 1;
        let revision = db.view().unwrap().status().revision;
        let failure = within(
            importer
                .import_block(
                    Block::Micro(stale.clone()).encode().unwrap(),
                    u64::MAX - 600_000,
                )
                .unwrap()
                .wait(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            failure.error,
            ae_sync::chain::Error::Policy { height: 1, top: 2 }
        ));
        let message = Message::new(
            WireKind::MicroBlock,
            1,
            BTreeMap::from([
                (
                    "micro_block".into(),
                    WireValue::Binary(Light::from_micro(&stale).encode().unwrap()),
                ),
                ("light".into(), WireValue::Boolean(true)),
            ]),
            Default::default(),
        )
        .unwrap();
        within(peers[0].send(&message).unwrap()).await.unwrap();
        within(async {
            while generation.sync.status().blocks.counts.ignored < 1 {
                generation.sync.changed().await.unwrap();
            }
        })
        .await;
        assert_eq!(
            generation.sync.status().blocks.last.unwrap().result,
            Ok(blocks::Outcome::Ignored(blocks::Ignored::Invalid(
                Box::new(sync::Error::Gossip(ae_chain::gossip::Error::TooOld))
            )))
        );
        assert_eq!(db.view().unwrap().status().revision, revision);
        let exit = running.shutdown().await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        assert_eq!(generation.network.status().listener.unwrap().active, 0);
        for driver in drivers {
            within(driver).await.unwrap();
        }
    }
}

#[tokio::test]
async fn configured_discovery_rejects_a_mismatched_retained_importer_and_joins_started_workers() {
    for kind in ["pow_cuckoo", "on_demand"] {
        let lab = Lab::new();
        let mut p = prepare(lab.services(kind, json!({}), Default::default()));
        let mut s = if kind == "on_demand" {
            commands(&lab)
        } else {
            cuckoo(&lab)
        };
        configure(&mut p, &mut s, 1);
        match &mut s {
            node::Settings::Cuckoo(s) => {
                s.config.connections.importer.ledger.policy.gossip_depth = 1
            }
            node::Settings::OnDemand(s) => {
                s.config.connections.importer.ledger.policy.gossip_depth = 1
            }
            _ => unreachable!(),
        }
        let exit = within(p.launch(s, || Ok(Default::default())).unwrap().wait())
            .await
            .unwrap();
        assert!(exit.status().joined && exit.status().failure.is_some());
        let node::Error::Pow(failure) = exit.status().failure.unwrap() else {
            panic!("expected PoW connection failure")
        };
        let root::Error::Connections(connection::Error::Startup(failure)) = *failure else {
            panic!("expected importer startup failure")
        };
        assert_eq!(failure.child, connection::Child::Importer);
        assert_eq!(failure.detail, "Configuration");
        assert!(exit.configured().is_some());
    }
}
