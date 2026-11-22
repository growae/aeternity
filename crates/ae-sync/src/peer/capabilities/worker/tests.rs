use super::*;
use std::sync::mpsc;

fn gate(c: &Client, panic: bool) -> (Ticket<()>, mpsc::Sender<()>) {
    let (entered, ready) = mpsc::sync_channel(1);
    let (release, wait) = mpsc::channel();
    let ticket = c
        .inner
        .submit(128, move |_| {
            entered.send(()).unwrap();
            wait.recv().unwrap();
            assert!(!panic, "injected worker failure");
            Ok(())
        })
        .unwrap();
    ready
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    (ticket, release)
}
#[test]
fn bounded_mailbox_drains_accepted_requests_after_callers_drop_and_rejects_new_work() {
    let worker = Worker::spawn(Config {
        queue: QueueLimits {
            requests: 3,
            bytes: 1024,
        },
        ..Default::default()
    })
    .unwrap();
    let client = worker.client();
    let (first, release) = gate(&client, false);
    drop(client.set("future", &Value::Opaque(vec![1, 2])).unwrap());
    let state = client.local().unwrap();
    assert!(matches!(
        client.get("future"),
        Err(Error::Worker(Fault::Busy))
    ));
    assert_eq!(client.info().pending, 3);
    worker.request_shutdown();
    assert!(matches!(client.local(), Err(Error::Worker(Fault::Stopped))));
    release.send(()).unwrap();
    assert_eq!(first.blocking_wait(), Ok(()));
    assert_eq!(
        state.blocking_wait().unwrap()["future"],
        Value::Opaque(vec![1, 2])
    );
    worker.shutdown().unwrap();
    let info = client.info();
    assert!(info.joined && !info.running && !info.accepting);
    assert_eq!(
        (info.pending, info.bytes, info.submitted, info.completed),
        (0, 0, 3, 3)
    );
}
#[test]
fn byte_limit_includes_running_request_and_registry_errors_are_nonfatal() {
    let worker = Worker::spawn(Config {
        queue: QueueLimits {
            requests: 8,
            bytes: 300,
        },
        ..Default::default()
    })
    .unwrap();
    let client = worker.client();
    let (first, release) = gate(&client, false);
    assert!(matches!(
        client.set("x", &Value::Opaque(vec![1])),
        Err(Error::Worker(Fault::Busy))
    ));
    release.send(()).unwrap();
    first.blocking_wait().unwrap();
    client
        .set("x", &Value::Opaque(vec![1]))
        .unwrap()
        .blocking_wait()
        .unwrap();
    assert_eq!(
        client.wire().unwrap().blocking_wait(),
        Err(Error::Registry(RegistryError::UnsupportedKey("x".into())))
    );
    assert!(client.info().running);
    assert!(matches!(
        client.set("x", &Value::Opaque(vec![0; 1 << 20])),
        Err(Error::Registry(RegistryError::Capacity))
    ));
    client.delete("x").unwrap().blocking_wait().unwrap();
    assert!(client.wire().unwrap().blocking_wait().unwrap().is_empty());
    worker.shutdown().unwrap();
}
#[test]
fn panic_disconnects_accepted_calls_and_fresh_worker_does_not_replay_them() {
    let worker = Worker::spawn(Default::default()).unwrap();
    let old = worker.client();
    old.set("old", &Value::Opaque(vec![9]))
        .unwrap()
        .blocking_wait()
        .unwrap();
    let (first, release) = gate(&old, true);
    let lost = old.set("queued", &Value::Opaque(vec![3])).unwrap();
    release.send(()).unwrap();
    assert_eq!(
        first.blocking_wait(),
        Err(Error::Worker(Fault::Disconnected))
    );
    assert_eq!(
        lost.blocking_wait(),
        Err(Error::Worker(Fault::Disconnected))
    );
    assert_eq!(worker.shutdown(), Err(Error::Worker(Fault::Panicked)));
    assert!(old.info().joined);
    assert_eq!(old.info().pending, 0);
    let next = Worker::spawn(Default::default()).unwrap();
    assert!(
        next.client()
            .local()
            .unwrap()
            .blocking_wait()
            .unwrap()
            .is_empty()
    );
    assert!(matches!(old.local(), Err(Error::Worker(Fault::Stopped))));
    next.shutdown().unwrap();
}
#[tokio::test]
async fn new_call_waits_for_binding_replacement_but_accepted_call_keeps_old_owner() {
    let node = crate::test_support::Node::new();
    let (owner, binding) = bindings::Owner::new(&node.db);
    let first = Worker::spawn(Default::default()).unwrap();
    let old = first.client();
    owner.install(old.clone(), 1);
    let (blocked, release) = gate(&old, false);
    let accepted = old.set("old", &Value::Opaque(vec![1])).unwrap();
    let recorded = old.local().unwrap();
    owner.withdraw();
    first.request_shutdown();
    let pending =
        tokio::spawn(
            async move { call(&binding, |c| c.set("new", &Value::Opaque(vec![2]))).await },
        );
    tokio::task::yield_now().await;
    assert!(!pending.is_finished());
    release.send(()).unwrap();
    blocked.await.unwrap();
    accepted.await.unwrap();
    assert!(recorded.await.unwrap().contains_key("old"));
    first.shutdown().unwrap();
    let next = Worker::spawn(Default::default()).unwrap();
    let client = next.client();
    owner.install(client.clone(), 2);
    pending.await.unwrap().unwrap();
    let state = client.local().unwrap().await.unwrap();
    assert_eq!(
        state,
        BTreeMap::from([("new".into(), Value::Opaque(vec![2]))])
    );
    owner.close();
    next.shutdown().unwrap();
}
