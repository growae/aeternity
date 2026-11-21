use super::*;
use crate::{
    runtime::workers,
    test_support::{Node, connection, pair, transaction, within},
};
use ae_protocol::NetworkProfile;

async fn answer(remote: &net::Client, reply: wire::Reply) {
    within(
        remote
            .send(&reply.into_response(Default::default()).unwrap())
            .unwrap(),
    )
    .await
    .unwrap();
}
#[tokio::test]
async fn fetched_action_keeps_old_admission_client_and_later_session_uses_replacement() {
    let mut node = Node::new();
    node.stop_worker();
    let config = workers::Config {
        admission: worker::Settings {
            origins_interval: None,
            ..Default::default()
        },
        pool: ae_storage::pool::Config {
            policy: ae_mempool::pool::Config {
                minimum_miner_gas_price: 1.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let (mut workers, service) = workers::create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        config,
        Default::default(),
    )
    .unwrap();
    let worker_task = tokio::spawn(service.run());
    within(async {
        while !workers.status().ready {
            workers.changed().await.unwrap();
        }
    })
    .await;
    let config = Config {
        session: session::Limits {
            resync_ms: 20,
            ..Default::default()
        },
        ..Default::default()
    };
    let (mut handle, _incoming, service) =
        create_bound(node.db.clone(), workers.bindings().admission, config).unwrap();
    let task = tokio::spawn(service.run());
    let (local, (remote, mut events, driver)) = pair(191, 192).await;
    let remote_key = local.0.peer();
    let driver = tokio::spawn(driver.run());
    handle
        .attach(connection(local), true)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let tx = transaction();
    let tree = protocol::Snapshot::build([tx.hash()], Default::default()).unwrap();
    for round in 0..2 {
        loop {
            let net::Event::Message(message) = within(events.recv()).await.unwrap() else {
                continue;
            };
            match wire::Request::from_message(message.message()).unwrap() {
                wire::Request::Init => answer(&remote, wire::Reply::Init).await,
                wire::Request::Unfold(values) => {
                    answer(
                        &remote,
                        wire::Reply::Unfold(
                            tree.respond_wire(&values, Default::default()).unwrap(),
                        ),
                    )
                    .await
                }
                wire::Request::Get(hashes) => {
                    assert_eq!(hashes, vec![tx.hash().0.to_vec()]);
                    if round == 0 {
                        workers
                            .restart(workers::Child::Admission, 1)
                            .unwrap()
                            .wait()
                            .await
                            .unwrap();
                        assert!(node.db.pool_view().unwrap().members(10).unwrap().is_empty());
                    }
                    answer(&remote, wire::Reply::Transactions(vec![tx.encode()])).await;
                }
                wire::Request::Finish(done) => {
                    answer(&remote, wire::Reply::Finished(done)).await;
                    break;
                }
            }
        }
        within(async {
            while handle.status().counts.completed < round + 1 {
                handle.changed().await.unwrap();
            }
        })
        .await;
        if round == 0 {
            assert_eq!(handle.status().counts.rejected, 1);
            assert_eq!(handle.status().counts.admitted, 0);
            assert_eq!(
                workers
                    .bindings()
                    .admission
                    .current()
                    .unwrap()
                    .client
                    .info()
                    .submitted,
                0
            );
            assert!(node.db.pool_view().unwrap().members(10).unwrap().is_empty());
            // Native resync expiry only clears the cooldown. A new peer sync
            // trigger starts the next exchange on this same live connection.
            within(async {
                while handle.status().local != Local::Idle {
                    handle.changed().await.unwrap();
                }
            })
            .await;
            assert!(
                handle
                    .synchronize(remote_key)
                    .unwrap()
                    .wait()
                    .await
                    .unwrap()
            );
        }
    }
    assert_eq!(handle.status().counts.fetched, 2);
    assert_eq!(handle.status().counts.admitted, 1);
    assert_eq!(handle.status().counts.rejected, 1);
    assert_eq!(
        node.db.pool_view().unwrap().members(10).unwrap(),
        vec![tx.hash()]
    );
    assert!(!remote.is_closed());
    handle.shutdown();
    within(task).await.unwrap();
    within(driver).await.unwrap();
    workers.shutdown();
    assert!(within(worker_task).await.unwrap().joined);
}
