use super::*;
use ae_sync::{
    routing,
    runtime::{connection as owner, workers},
};
use connection_owner::{closed, ready, remote, settings};
#[tokio::test]
async fn routing_replacement_retains_cache_and_encrypted_connections_until_parent_shutdown() {
    let mut node = Node::new();
    node.stop_worker();
    let mut config = worker_owner::worker_config();
    config.routing = Some(routing::Setup {
        config: routing::Config {
            rules: vec![routing::Rule {
                name: b"ae.**".to_vec(),
                actions: b"none".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        },
        ..Default::default()
    });
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
            assert!(workers.status().running);
            workers.changed().await.unwrap();
        }
    })
    .await;
    let bindings = workers.bindings();
    let old = bindings.routing.as_ref().unwrap().current().unwrap();
    let mut config = settings();
    config.observer.interval = Duration::from_millis(20);
    let (mut handle, service) = owner::create_with_workers(
        worker_owner::inputs(&node, &bindings),
        config,
        || Ok(Default::default()),
        bindings.clone(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let mut generation = ready(&mut handle, 1).await;
    let (peer, _events, driver) = remote(&node, &generation).await;
    response(&peer, &ping(&node, 1, false)).await;
    within(async {
        while generation.observer.status().samples < 2 {
            generation.observer.changed().await.unwrap();
        }
    })
    .await;
    let key = ae_sync::observe::metrics::Metric {
        name: "ae.epoch.aecore.chain.height.value",
        value: ae_sync::observe::metrics::Number::Integer(0),
    }
    .routing()
    .1;
    assert_eq!(
        old.destinations(key.clone()).unwrap().await,
        Ok(Some(vec![]))
    );
    old.add(key.clone(), routing::Destination::Send)
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        workers
            .restart(workers::Child::Routing, 1)
            .unwrap()
            .wait()
            .await,
        Ok(2)
    );
    let next = bindings.routing.as_ref().unwrap().current().unwrap();
    assert_eq!(
        next.destinations(key.clone()).unwrap().await,
        Ok(Some(vec!["send".into()]))
    );
    assert_eq!(
        old.destinations(key.clone()).unwrap().await,
        Ok(Some(vec!["send".into()]))
    );
    assert!(matches!(
        old.add(key.clone(), routing::Destination::Log),
        Err(routing::Error::Stopped)
    ));
    let before = generation.observer.status().samples;
    within(async {
        while generation.observer.status().samples <= before {
            generation.observer.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(generation.observer.status().routing_errors, 0);
    response(&peer, &ping(&node, 1, false)).await;
    assert_eq!(handle.status().starts, 1);
    assert_eq!(handle.status().restart_attempts, 0);
    assert_eq!(
        (
            workers.status().routing_generation,
            workers.status().admission_generation,
            workers.status().gc_generation
        ),
        (2, 1, 1)
    );
    workers.shutdown();
    assert!(within(worker_task).await.unwrap().joined);
    let status = within(task).await.unwrap();
    assert_eq!(status.error, Some(owner::Error::RoutingClosed));
    assert_eq!(status.restart_attempts, 0);
    assert!(status.joined);
    closed(&generation);
    within(driver).await.unwrap();
    assert!(matches!(
        next.destinations(key),
        Err(routing::Error::Closed)
    ));
}
