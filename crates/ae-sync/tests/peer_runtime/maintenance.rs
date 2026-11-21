use super::*;
use ae_chain::repository::retention;
use ae_storage::gc::worker::Worker;
use ae_sync::{chain::sync, events, maintenance};

fn worker(node: &Node) -> Worker {
    Worker::spawn(
        node.db.clone(),
        retention::Config {
            enabled: true,
            history: 50,
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
}
fn binding(
    node: &Node,
    sync: &sync::Handle,
    worker: &Worker,
) -> (maintenance::Handle, JoinHandle<maintenance::Status>) {
    let (handle, service) = maintenance::create(
        &node.db,
        sync.clone(),
        worker.client(),
        maintenance::Config {
            poll_interval: Duration::from_millis(10),
        },
    )
    .unwrap();
    (handle, tokio::spawn(service.run()))
}
async fn ready(
    handle: &mut maintenance::Handle,
    predicate: impl Fn(&maintenance::Status) -> bool,
) -> maintenance::Status {
    within(async {
        loop {
            let status = handle.status();
            assert!(status.running, "{status:?}");
            if predicate(&status) {
                return status;
            }
            handle.changed().await.unwrap();
        }
    })
    .await
}
async fn completed(handle: &mut sync::Handle, count: u64) {
    within(async {
        loop {
            let status = handle.status();
            assert!(status.running, "{status:?}");
            if status.lifecycle.peer_completions >= count {
                return;
            }
            handle.changed().await.unwrap();
        }
    })
    .await;
}
fn drain(mut incoming: sync::Events) -> JoinHandle<()> {
    tokio::spawn(async move { while incoming.recv().await.is_some() {} })
}

#[tokio::test]
async fn idle_start_never_enables_gc_but_late_binding_recovers_completion_after_history_overflow() {
    let node = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (remote, _incoming, driver) = connect(&node, &app).await;
    let (mut sync, incoming, finish) = chain_sync::synchronize(
        &node,
        app,
        sync::Config {
            events: events::Config {
                records: 1,
                bytes: 192,
                event_bytes: 128,
                subscribers: 1,
            },
            ..Default::default()
        },
    );
    let drain = drain(incoming);
    let feed = sync.events();
    let cursor = feed.cursor();
    let worker = worker(&node);
    let (mut idle, task) = binding(&node, &sync, &worker);
    ready(&mut idle, |s| s.gc.is_some()).await;
    assert!(!worker.info().synced);
    assert!(sync.status().lifecycle.gc_ready.is_none());
    assert_eq!(idle.status().notifications, 0);
    idle.shutdown();
    assert!(within(task).await.unwrap().joined);
    for count in 1..=8 {
        response(&remote, &ping(&node, 1, false)).await;
        completed(&mut sync, count).await;
    }
    assert!(!worker.info().synced);
    assert_eq!(feed.info().evicted, 7);
    assert_eq!(sync.status().lifecycle.event_cursor, Some(feed.cursor()));
    let mut sub = feed.subscribe(Some(cursor)).unwrap();
    assert!(matches!(
        sub.recv().await,
        Some(events::Delivery::Gap { missed: 7 })
    ));
    let (mut handle, task) = binding(&node, &sync, &worker);
    let status = ready(&mut handle, |s| s.notifications == 1).await;
    assert_eq!(status.cause, sync.status().lifecycle.gc_ready);
    assert!(worker.info().synced);
    drop(sub);
    let mut sub = feed
        .subscribe(sync.status().lifecycle.event_cursor)
        .unwrap();
    response(&remote, &ping(&node, 1, false)).await;
    completed(&mut sync, 9).await;
    assert!(matches!(sub.recv().await, Some(events::Delivery::Event(_))));
    assert_eq!(handle.status().notifications, 1);
    handle.shutdown();
    assert!(within(task).await.unwrap().joined);
    sync.shutdown();
    assert!(within(finish).await.joined);
    within(drain).await.unwrap();
    within(driver).await.unwrap();
    while sub.recv().await.is_some() {}
    assert!(feed.info().closed);
    assert!(worker.info().running);
    worker.shutdown().unwrap();
}

#[tokio::test]
async fn required_gc_consumer_reports_source_exit_and_rejects_foreign_bindings() {
    let node = Node::new();
    let foreign = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (sync, incoming, finish) = chain_sync::synchronize(&node, app, Default::default());
    let drain = drain(incoming);
    let worker = worker(&node);
    let other = Worker::spawn(foreign.db.clone(), Default::default(), Default::default()).unwrap();
    assert!(matches!(
        maintenance::create(&node.db, sync.clone(), other.client(), Default::default()),
        Err(maintenance::Error::Configuration)
    ));
    assert!(matches!(
        maintenance::create(
            &foreign.db,
            sync.clone(),
            other.client(),
            Default::default()
        ),
        Err(maintenance::Error::Configuration)
    ));
    assert!(matches!(
        maintenance::create(
            &node.db,
            sync.clone(),
            worker.client(),
            maintenance::Config {
                poll_interval: Duration::ZERO
            }
        ),
        Err(maintenance::Error::Configuration)
    ));
    let (abandoned, service) =
        maintenance::create(&node.db, sync.clone(), worker.client(), Default::default()).unwrap();
    drop(service);
    assert_eq!(
        abandoned.status().last_error,
        Some(maintenance::Error::Closed)
    );
    assert!(!abandoned.status().joined);
    assert!(sync.status().running && worker.info().running);
    let (mut handle, task) = binding(&node, &sync, &worker);
    ready(&mut handle, |s| s.gc.is_some()).await;
    sync.shutdown();
    assert!(within(finish).await.joined);
    within(drain).await.unwrap();
    let status = within(task).await.unwrap();
    assert!(status.joined && !status.running);
    assert_eq!(status.last_error, Some(maintenance::Error::SyncClosed));
    assert_eq!(status.notifications, 0);
    assert!(worker.info().running && !worker.info().synced);
    worker.shutdown().unwrap();
    other.shutdown().unwrap();
}

#[tokio::test]
async fn gc_restart_does_not_redirect_stale_consumer_and_new_binding_reconciles_readiness() {
    let node = Node::new();
    let app = start(&node, config(), Default::default()).await;
    let (remote, _incoming, driver) = connect(&node, &app).await;
    let (mut sync, incoming, finish) = chain_sync::synchronize(&node, app, Default::default());
    let drain = drain(incoming);
    let original = worker(&node);
    let old_client = original.client();
    let (mut handle, task) = binding(&node, &sync, &original);
    response(&remote, &ping(&node, 1, false)).await;
    completed(&mut sync, 1).await;
    ready(&mut handle, |s| s.notifications == 1).await;
    original.shutdown().unwrap();
    let replacement = worker(&node);
    let status = within(task).await.unwrap();
    assert_eq!(status.last_error, Some(maintenance::Error::GcClosed));
    assert!(status.joined);
    assert!(!replacement.info().synced);
    assert!(!old_client.on_sync_done(false));
    assert!(matches!(
        maintenance::create(&node.db, sync.clone(), old_client, Default::default()),
        Err(maintenance::Error::Configuration)
    ));
    let (mut fresh, task) = binding(&node, &sync, &replacement);
    ready(&mut fresh, |s| s.notifications == 1).await;
    assert!(replacement.info().synced);
    fresh.shutdown();
    assert!(within(task).await.unwrap().joined);
    sync.shutdown();
    assert!(within(finish).await.joined);
    within(drain).await.unwrap();
    within(driver).await.unwrap();
    replacement.shutdown().unwrap();
}
