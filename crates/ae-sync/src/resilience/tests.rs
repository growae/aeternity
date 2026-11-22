use super::*;
use crate::test_support::Node;
use std::{sync::mpsc, time::Duration};

fn snapshot(client: &Client) -> Snapshot {
    client.snapshot().unwrap().blocking_wait().unwrap()
}
fn gate(client: &Client, panic: bool) -> (Ticket<()>, mpsc::Sender<()>) {
    let (entered, ready) = mpsc::sync_channel(1);
    let (release, wait) = mpsc::channel();
    let ticket = client
        .dispatch
        .client
        .submit(128, move |_| {
            entered.send(()).unwrap();
            wait.recv().unwrap();
            assert!(!panic, "injected resilience child failure");
            Ok(())
        })
        .unwrap();
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    (ticket, release)
}

#[test]
fn child_restart_retains_disabled_activation_and_parent_drop_resets_it() {
    let node = Node::new();
    let owner = Owner::new(
        node.db.clone(),
        Config {
            initial: policy::Config {
                depth: 3,
                from_start: true,
            },
            ..Default::default()
        },
    )
    .unwrap();
    let source = owner.source();
    let worker = owner.start().unwrap();
    let old = worker.client();
    assert!(matches!(
        owner.start(),
        Err(Error::Storage(repository::Error::PolicyOwned))
    ));
    let initial = snapshot(&old);
    assert_eq!(initial.startup_reads, 2);
    assert_eq!(initial.policy.state.recorded_active(), Some(3));
    assert!(!initial.policy.state.synchronized());
    assert_eq!(
        source
            .configure(policy::Config {
                depth: 0,
                from_start: true
            })
            .unwrap()
            .delivery,
        Delivery::Accepted
    );
    assert_eq!(snapshot(&old).policy.state.recorded_active(), Some(3));
    assert_eq!(source.notify_sync_done(), Delivery::Accepted);
    let disabled = snapshot(&old);
    native_snapshot(&disabled, "recorded_disabled");
    assert_eq!(disabled.policy.state.recorded_active(), Some(0));
    assert!(disabled.policy.state.synchronized());
    assert!(!disabled.sync_subscribed);
    worker.shutdown().unwrap();
    assert_eq!(source.notify_sync_done(), Delivery::Unavailable);
    assert_eq!(
        source
            .configure(policy::Config {
                depth: 9,
                from_start: true
            })
            .unwrap()
            .delivery,
        Delivery::Unavailable
    );
    assert_eq!(source.snapshot().unwrap(), disabled.policy);
    let worker = owner.start().unwrap();
    let client = worker.client();
    let restarted = snapshot(&client);
    native_snapshot(&restarted, "restart_keeps_disabled");
    assert_eq!(restarted.policy.state.configured_depth(), Some(9));
    assert_eq!(restarted.policy.state.recorded_active(), Some(0));
    assert!(restarted.sync_subscribed);
    assert_eq!(restarted.sync_callbacks, 0);
    assert!(matches!(old.snapshot(), Err(Error::Worker(Fault::Stopped))));
    assert_eq!(source.notify_sync_done(), Delivery::Accepted);
    assert_eq!(snapshot(&client).policy.state.active_depth(), Some(9));
    worker.shutdown().unwrap();
    drop(owner);
    assert!(source.is_closed());
    assert_eq!(
        source.snapshot(),
        Err(Error::Storage(repository::Error::PolicyClosed))
    );
    assert_eq!(
        node.db.view().unwrap().resilience().state,
        Default::default()
    );
    let owner = Owner::new(node.db.clone(), Default::default()).unwrap();
    let worker = owner.start().unwrap();
    let fresh = snapshot(&worker.client());
    assert!(!fresh.policy.state.synchronized());
    assert_eq!(fresh.policy.state.recorded_active(), None);
    worker.shutdown().unwrap();
}

#[test]
fn reserved_controls_remain_bounded_and_share_fifo_with_queries() {
    let node = Node::new();
    let owner = Owner::new(
        node.db.clone(),
        Config {
            queue: QueueLimits {
                requests: 3,
                bytes: 384,
            },
            ..Default::default()
        },
    )
    .unwrap();
    let source = owner.source();
    let worker = owner.start().unwrap();
    let client = worker.client();
    let (blocked, release) = gate(&client, false);
    let before = client.snapshot().unwrap();
    assert_eq!(source.notify_sync_done(), Delivery::Accepted);
    assert_eq!(source.notify_sync_done(), Delivery::Coalesced);
    assert_eq!(
        source
            .configure(policy::Config {
                depth: 7,
                from_start: false
            })
            .unwrap()
            .delivery,
        Delivery::Accepted
    );
    assert_eq!(
        source
            .configure(policy::Config {
                depth: 11,
                from_start: false
            })
            .unwrap()
            .delivery,
        Delivery::Coalesced
    );
    let after = client.snapshot().unwrap();
    assert!(matches!(client.snapshot(), Err(Error::Worker(Fault::Busy))));
    assert_eq!(client.info().pending, 5);
    assert_eq!(client.info().control_pending, 2);
    worker.request_shutdown();
    assert_eq!(source.notify_sync_done(), Delivery::Unavailable);
    release.send(()).unwrap();
    drop(blocked); // Accepted work still drains after the caller drops its ticket.
    let before = before.blocking_wait().unwrap();
    assert!(!before.policy.state.synchronized());
    assert_eq!(before.configuration.value.depth, 11);
    let after = after.blocking_wait().unwrap();
    assert_eq!(after.policy.state.active_depth(), Some(11));
    assert_eq!(
        (after.sync_callbacks, after.configuration_callbacks),
        (1, 1)
    );
    worker.shutdown().unwrap();
    let info = client.info();
    assert!(info.joined && !info.running && !info.accepting);
    assert_eq!((info.pending, info.control_pending, info.bytes), (0, 0, 0));
    assert_eq!((info.submitted, info.completed), (5, 5));
}

#[test]
fn child_panic_loses_queued_signals_without_replaying_old_completions() {
    let node = Node::new();
    let owner = Owner::new(node.db.clone(), Default::default()).unwrap();
    let source = owner.source();
    let worker = owner.start().unwrap();
    let old = worker.client();
    let (blocked, release) = gate(&old, true);
    assert_eq!(source.notify_sync_done(), Delivery::Accepted);
    let lost = old.snapshot().unwrap();
    release.send(()).unwrap();
    assert_eq!(
        blocked.blocking_wait(),
        Err(Error::Worker(Fault::Disconnected))
    );
    assert_eq!(
        lost.blocking_wait(),
        Err(Error::Worker(Fault::Disconnected))
    );
    assert_eq!(worker.shutdown(), Err(Error::Worker(Fault::Panicked)));
    assert!(!node.db.policy_recovery_required());
    assert!(!source.snapshot().unwrap().state.synchronized());
    assert_eq!(source.notify_sync_done(), Delivery::Unavailable);
    let worker = owner.start().unwrap();
    let state = snapshot(&worker.client());
    assert!(state.sync_subscribed);
    assert_eq!(state.sync_callbacks, 0);
    assert!(!state.policy.state.synchronized());
    assert_eq!(source.notify_sync_done(), Delivery::Accepted);
    assert!(snapshot(&worker.client()).policy.state.synchronized());
    worker.shutdown().unwrap();
}

#[test]
fn parent_close_rejects_accepted_work_and_old_clients_do_not_retain_ownership() {
    let node = Node::new();
    let owner = Owner::new(node.db.clone(), Default::default()).unwrap();
    let source = owner.source();
    let worker = owner.start().unwrap();
    let client = worker.client();
    let (blocked, release) = gate(&client, false);
    let pending = client.snapshot().unwrap();
    drop(owner);
    assert!(source.is_closed());
    assert!(!node.db.policy_ready());
    assert!(matches!(
        Owner::new(node.db.clone(), Default::default()),
        Err(Error::Storage(repository::Error::PolicyOwned))
    ));
    release.send(()).unwrap();
    blocked.blocking_wait().unwrap();
    assert_eq!(pending.blocking_wait(), Err(Error::Closed));
    assert_eq!(worker.shutdown(), Err(Error::Closed));
    let next = Owner::new(node.db.clone(), Default::default()).unwrap();
    assert!(source.is_closed());
    let worker = next.start().unwrap();
    assert!(node.db.policy_ready());
    assert!(matches!(
        client.snapshot(),
        Err(Error::Worker(Fault::Stopped))
    ));
    worker.shutdown().unwrap();
}

#[test]
fn subscription_precedes_both_startup_reads_and_deferred_controls_keep_arrival_order() {
    let node = Node::new();
    let owner = Owner::new(
        node.db.clone(),
        Config {
            initial: policy::Config {
                depth: 3,
                from_start: false,
            },
            ..Default::default()
        },
    )
    .unwrap();
    let source = owner.source();
    let (entered, ready) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    *source.0.startup_gate.lock().unwrap() = Some((entered, resume));
    assert_eq!(source.notify_sync_done(), Delivery::Unavailable);
    let starting = source.clone();
    let start = std::thread::spawn(move || Worker::spawn(starting, Default::default()));
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(source.snapshot().unwrap().state.configured_depth(), Some(3));
    assert_eq!(source.snapshot().unwrap().state.recorded_active(), None);
    assert_eq!(
        source
            .configure(policy::Config {
                depth: 7,
                from_start: false
            })
            .unwrap()
            .delivery,
        Delivery::Accepted
    );
    assert_eq!(source.notify_sync_done(), Delivery::Accepted);
    assert_eq!(
        source
            .configure(policy::Config {
                depth: 9,
                from_start: false
            })
            .unwrap()
            .delivery,
        Delivery::Coalesced
    );
    assert_eq!(source.notify_sync_done(), Delivery::Coalesced);
    release.send(()).unwrap();
    let worker = start.join().unwrap().unwrap();
    let state = snapshot(&worker.client());
    assert_eq!(state.startup_reads, 2);
    assert_eq!(state.policy.state.active_depth(), Some(9));
    assert_eq!(
        (state.configuration_callbacks, state.sync_callbacks),
        (1, 1)
    );
    assert!(!state.sync_subscribed);
    worker.shutdown().unwrap();
}

#[test]
fn startup_rereads_external_depth_before_skipping_an_unsynchronized_config_notice() {
    let node = Node::new();
    let owner = Owner::new(
        node.db.clone(),
        Config {
            initial: policy::Config {
                depth: 3,
                from_start: false,
            },
            ..Default::default()
        },
    )
    .unwrap();
    let source = owner.source();
    let (entered, ready) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    *source.0.startup_gate.lock().unwrap() = Some((entered, resume));
    let starting = source.clone();
    let start = std::thread::spawn(move || Worker::spawn(starting, Default::default()));
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(source.snapshot().unwrap().state.configured_depth(), Some(3));
    source
        .configure(policy::Config {
            depth: 9,
            from_start: false,
        })
        .unwrap();
    release.send(()).unwrap();
    let worker = start.join().unwrap().unwrap();
    let state = snapshot(&worker.client());
    // The queued config callback is skipped before sync. Only the second init
    // read can publish the new cached value in this case.
    assert_eq!(state.policy.state.configured_depth(), Some(9));
    assert_eq!(state.policy.state.recorded_active(), None);
    assert_eq!(
        (state.configuration_callbacks, state.sync_callbacks),
        (1, 0)
    );
    assert!(!state.policy.state.synchronized());
    worker.shutdown().unwrap();
}

fn native_snapshot(actual: &Snapshot, name: &str) {
    let fixture: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_resilience_worker.json"
    )))
    .unwrap();
    let expected = &fixture["lifecycle"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == name)
        .unwrap()["state"];
    assert_eq!(
        actual.policy.state.configured_depth().unwrap_or(0),
        expected["cached"].as_u64().unwrap()
    );
    assert_eq!(
        actual.policy.state.recorded_active(),
        expected["active"].as_u64()
    );
    assert_eq!(
        actual.policy.state.synchronized(),
        expected["synced"].as_bool().unwrap()
    );
    assert_eq!(
        actual.sync_subscribed,
        expected["sync_subscribed"].as_bool().unwrap()
    );
}
