use super::*;
use crate::test_support::{Node, within};
#[tokio::test]
async fn maintenance_rebinds_new_operations_and_preserves_durable_membership() {
    let mut node = Node::new();
    node.stop_worker();
    let (mut handle, service) = crate::runtime::workers::create(
        node.db.clone(),
        ae_protocol::NetworkProfile::mainnet(),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    within(async {
        while !handle.status().ready {
            assert!(handle.status().running);
            handle.changed().await.unwrap();
        }
    })
    .await;
    let bindings = handle.bindings();
    let gc = bindings.pool_gc.as_ref().unwrap().current().unwrap();
    assert_eq!(
        bindings
            .admission
            .current()
            .unwrap()
            .client
            .origins_interval(),
        None
    );
    gc.adjust_ttl(1).unwrap().await.unwrap();
    handle
        .restart(crate::runtime::workers::Child::Admission, 1)
        .unwrap()
        .wait()
        .await
        .unwrap();
    gc.adjust_ttl(-1).unwrap().await.unwrap();
    assert_eq!(gc.info().last_admission_generation, Some(2));
    assert_eq!(handle.status().pool_gc_generation, 1);
    handle
        .restart(crate::runtime::workers::Child::PoolMaintenance, 1)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(gc.adjust_ttl(0), Err(Error::Stopped)));
    assert!(gc.info().joined);
    assert_eq!(handle.status().admission_generation, 2);
    let new = bindings.pool_gc.as_ref().unwrap().current().unwrap();
    let accepted = new.collect_origins().unwrap();
    handle.shutdown();
    accepted.await.unwrap();
    assert!(within(task).await.unwrap().joined);
    assert!(new.info().joined);
}

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../ae-mempool/tests/fixtures/erlang_pool_gc.json"
    ))
    .unwrap()
}
struct Gate(
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
);
impl Work for Gate {
    fn wait(self: Box<Self>) -> Completed {
        self.0.send(()).unwrap();
        self.1.recv_timeout(Duration::from_secs(10)).unwrap();
        Completed {
            error: None,
            reply: Box::new(|| {}),
        }
    }
}
fn gate(client: &Client) -> std::sync::mpsc::SyncSender<()> {
    let (entered, ready) = std::sync::mpsc::sync_channel(1);
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    {
        let mut state = client.shared.lock();
        state.queue.push_back(Queued {
            bytes: 128,
            work: Box::new(Gate(entered, wait)),
        });
        state.info.pending += 1;
        state.info.pending_bytes += 128;
        client.shared.changed.notify_all();
    }
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    release
}
fn settle(client: &Client, predicate: impl Fn(&Info) -> bool) {
    let start = Instant::now();
    while !predicate(&client.info()) {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "{:?}",
            client.info()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn exact_accepted_operations_survive_rebinding_with_queue_limits_and_joined_shutdown() {
    let mut node = Node::new();
    let tx = crate::test_support::transaction();
    node.client()
        .force_push(tx.clone())
        .unwrap()
        .wait()
        .unwrap();
    let (owner, binding) = bindings::Owner::new(&node.db);
    owner.install(bindings::Pool::new(node.client()), 1);
    let worker = Worker::start(
        &node.db,
        binding.clone(),
        Config {
            requests: 2,
            request_bytes: 256,
        },
        Duration::from_secs(3600),
        INTERVAL,
    )
    .unwrap();
    let client = worker.client();
    client
        .shorten_ttl(tx.hash(), -7)
        .unwrap()
        .blocking_wait()
        .unwrap();
    let release = gate(&client);
    let old = client.ttl(tx.hash()).unwrap();
    assert!(matches!(client.adjust_ttl(0), Err(Error::Busy)));
    owner.withdraw();
    release.send(()).unwrap();
    assert_eq!(old.blocking_wait().unwrap(), -7);
    assert!(matches!(
        client.adjust_ttl(0),
        Err(Error::AdmissionUnavailable)
    ));
    node.stop_worker();
    let next = admission::Worker::spawn(
        node.db.clone(),
        ae_protocol::NetworkProfile::mainnet(),
        Default::default(),
        admission::Settings {
            origins_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    owner.install(bindings::Pool::new(next.client()), 2);
    assert!(client.ttl(tx.hash()).unwrap().blocking_wait().unwrap() >= 0);
    assert_eq!(client.info().last_admission_generation, Some(2));
    let release = gate(&client);
    let accepted = client.adjust_ttl(1).unwrap();
    worker.request_shutdown();
    assert!(matches!(client.ttl(tx.hash()), Err(Error::Stopped)));
    drop(accepted);
    release.send(()).unwrap();
    worker.shutdown().unwrap();
    assert!(client.info().joined);
    assert_eq!(client.info().pending, 0);
    assert_eq!(
        node.db.pool_view().unwrap().members(10).unwrap(),
        vec![tx.hash()]
    );
    next.shutdown().unwrap();
}
#[test]
fn native_scheduler_boundaries_and_completion_relative_fairness() {
    for row in fixture()["schedules"].as_array().unwrap() {
        assert_eq!(
            initial_delay(row["random"].as_u64().unwrap() as u32)
                .unwrap()
                .as_millis() as u64,
            row["initial"][0]["timer"].as_u64().unwrap()
        );
        let events = row["after_callback"].as_array().unwrap();
        assert_eq!(events.last().unwrap()["timer"], INTERVAL.as_millis() as u64);
        assert_eq!(events[events.len() - 2]["metric"], "origin_gced");
    }
    assert_eq!(initial_delay(0), Err(Error::Configuration));
    assert_eq!(initial_delay(10_001), Err(Error::Configuration));
    let node = Node::new();
    let (_, binding) = bindings::Owner::<bindings::Pool>::new(&node.db);
    assert!(matches!(
        Worker::spawn(&node.db, binding, Default::default()),
        Err(Error::BindingClosed)
    ));
    let binding = bindings::Admission::fixed(&node.db, node.client());
    let worker = Worker::start(
        &node.db,
        binding,
        Default::default(),
        Duration::from_millis(20),
        Duration::from_millis(80),
    )
    .unwrap();
    let client = worker.client();
    let release = gate(&client);
    std::thread::sleep(Duration::from_millis(30));
    let accepted = client.adjust_ttl(0).unwrap();
    release.send(()).unwrap();
    accepted.blocking_wait().unwrap();
    settle(&client, |i| i.background_runs == 1);
    let release = gate(&client);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(client.info().background_runs, 1);
    release.send(()).unwrap();
    settle(&client, |i| i.background_runs == 2);
    // Completing a late pass installs one interval; it never catches up missed ticks.
    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(client.info().background_runs, 2);
    worker.shutdown().unwrap();
}
#[test]
fn stopped_admission_closed_binding_and_double_timer_configuration_are_distinct() {
    let mut node = Node::new();
    let (owner, binding) = bindings::Owner::new(&node.db);
    owner.install(bindings::Pool::new(node.client()), 1);
    let worker = Worker::spawn(&node.db, binding, Default::default()).unwrap();
    let client = worker.client();
    node.stop_worker();
    assert!(matches!(
        client.adjust_ttl(0),
        Err(Error::Admission(admission::Error::Stopped))
    ));
    owner.close();
    settle(&client, |i| !i.running);
    assert_eq!(worker.shutdown(), Err(Error::BindingClosed));
    let node = Node::with_settings(Default::default());
    let binding = bindings::Admission::fixed(&node.db, node.client());
    assert!(matches!(
        Worker::spawn(&node.db, binding, Default::default()),
        Err(Error::Configuration)
    ));
}
