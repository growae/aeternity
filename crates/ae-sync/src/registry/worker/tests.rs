use super::*;
use crate::registry::tests::{Temp, config, db, peer, service};
use std::time::{Duration, Instant};
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn discovery_counts_are_serialized_current_and_keep_unread_reply_credits() {
    let temp = Temp::new();
    let worker = Worker::start(
        service(db(&temp), config()),
        Settings {
            requests: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let client = worker.client();
    let rt = runtime();
    assert_eq!(
        rt.block_on(client.pool_counts().unwrap()).unwrap(),
        PoolCounts::default()
    );
    rt.block_on(client.apply(1, Command::Add(peer(1, true))).unwrap())
        .unwrap();
    rt.block_on(client.apply(2, Command::Add(peer(2, false))).unwrap())
        .unwrap();
    let counts = client.pool_counts().unwrap();
    assert!(matches!(client.inspect(), Err(Error::Saturated)));
    assert_eq!(
        rt.block_on(counts).unwrap(),
        PoolCounts {
            verified: 1,
            unverified: 1
        }
    );
    rt.block_on(
        client
            .apply(3, Command::Connect(Some(peer(2, false).id)))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        rt.block_on(client.pool_counts().unwrap()).unwrap(),
        PoolCounts {
            verified: 1,
            unverified: 1
        }
    );
    assert_eq!(
        rt.block_on(client.inspect().unwrap())
            .unwrap()
            .counts
            .outbound,
        2
    );
    rt.block_on(client.apply(4, Command::Delete(peer(2, false).id)).unwrap())
        .unwrap();
    assert_eq!(
        rt.block_on(client.pool_counts().unwrap()).unwrap(),
        PoolCounts {
            verified: 1,
            unverified: 0
        }
    );
    worker.shutdown().unwrap();
    assert!(matches!(client.pool_counts(), Err(Error::Stopped)));
}

#[test]
fn dropped_observations_preserve_mutations_and_effects_and_unread_replies_hold_quotas() {
    let temp = Temp::new();
    let database = db(&temp);
    let worker = Worker::start(
        service(database.clone(), config()),
        Settings {
            requests: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let client = worker.client();
    let ticket = client.apply(1, Command::Add(peer(1, true))).unwrap();
    assert!(matches!(client.inspect(), Err(Error::Saturated)));
    drop(ticket);
    wait(|| client.outstanding().0 == 0);
    let rt = runtime();
    let info = rt.block_on(client.inspect().unwrap()).unwrap();
    assert_eq!(info.policy_revision, 1);
    assert_eq!(info.effects, 1);
    let first = rt.block_on(client.next_effect().unwrap()).unwrap().unwrap();
    let same = rt.block_on(client.next_effect().unwrap()).unwrap().unwrap();
    assert_eq!(same, first);
    assert!(rt.block_on(client.acknowledge(first.id).unwrap()).unwrap());
    assert!(!rt.block_on(client.acknowledge(first.id).unwrap()).unwrap());
    let ticket = client.apply(2, Command::Add(peer(2, false))).unwrap();
    // Receiving a later command is unnecessary to prove publication: inspect
    // the independent actual database while its reply is still unread.
    wait(|| {
        database
            .peer_view()
            .unwrap()
            .get(peer(2, false).id, 1024)
            .unwrap()
            .is_some()
    });
    assert_eq!(client.outstanding().0, 1);
    assert!(matches!(client.inspect(), Err(Error::Saturated)));
    rt.block_on(ticket).unwrap();
    assert_eq!(client.outstanding(), (0, 0));
    assert!(matches!(
        client.apply(
            3,
            Command::Sample {
                maximum: 1025,
                percent_connected: None,
                exclude: vec![]
            }
        ),
        Err(Error::RequestTooLarge)
    ));
    let mut huge = peer(3, false);
    huge.host = vec![0; 5 << 20];
    assert!(matches!(
        client.apply(3, Command::Add(huge)),
        Err(Error::RequestTooLarge)
    ));
    let service = worker.shutdown().unwrap();
    assert_eq!(service.saved_peers().count(), 1);
    assert!(matches!(client.inspect(), Err(Error::Stopped)));
}

#[test]
fn stop_joins_the_running_operation_and_rejects_waiting_work() {
    let temp = Temp::new();
    let worker = Worker::start(service(db(&temp), config()), Settings::default()).unwrap();
    let client = worker.client();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let running = client
        .submit(256, move |s| {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            s.apply(1, Command::Add(peer(1, false)))
        })
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let waiting = client.apply(2, Command::Add(peer(2, false))).unwrap();
    let stop = thread::spawn(move || worker.shutdown());
    wait(|| client.shared.lock().stopped);
    assert!(!stop.is_finished());
    release_tx.send(()).unwrap();
    let service = stop.join().unwrap().unwrap();
    let rt = runtime();
    rt.block_on(running).unwrap();
    assert!(matches!(rt.block_on(waiting), Err(Error::Stopped)));
    assert_eq!(
        service.saved_peers().map(|p| p.id).collect::<Vec<_>>(),
        vec![peer(1, false).id]
    );
    assert_eq!(client.outstanding(), (0, 0));
}

#[test]
fn worker_panics_stop_admission_without_reusing_a_possibly_published_owner() {
    let temp = Temp::new();
    let worker = Worker::start(service(db(&temp), config()), Settings::default()).unwrap();
    let client = worker.client();
    let ticket: Ticket<()> = client
        .submit(256, |_| panic!("injected owner panic"))
        .unwrap();
    assert!(matches!(runtime().block_on(ticket), Err(Error::Panicked)));
    wait(|| client.shared.lock().stopped);
    let mut service = worker.shutdown().unwrap();
    assert!(service.recovery_required());
    assert!(matches!(
        service.recover(),
        Err(super::super::Error::Inconsistent)
    ));
    assert!(matches!(client.inspect(), Err(Error::Stopped)));
    assert_eq!(client.outstanding(), (0, 0));
}

#[test]
fn an_unobserved_inbound_reservation_remains_bounded_and_can_expire_through_the_worker() {
    let temp = Temp::new();
    let worker = Worker::start(service(db(&temp), config()), Settings::default()).unwrap();
    let client = worker.client();
    let rt = runtime();
    drop(
        client
            .apply(
                1,
                Command::ReserveInbound {
                    address: "127.0.0.1".parse().unwrap(),
                },
            )
            .unwrap(),
    );
    wait(|| client.outstanding().0 == 0);
    assert_eq!(
        rt.block_on(client.inspect().unwrap())
            .unwrap()
            .counts
            .pending_inbound,
        1
    );
    let timer = rt.block_on(client.next_timer().unwrap()).unwrap().unwrap();
    assert_eq!(
        rt.block_on(
            client
                .apply(timer.deadline - 1, Command::Timeout(timer.token))
                .unwrap()
        )
        .unwrap(),
        Outcome::Ignored
    );
    assert_eq!(
        rt.block_on(client.next_timer().unwrap()).unwrap(),
        Some(timer)
    );
    rt.block_on(
        client
            .apply(timer.deadline, Command::Timeout(timer.token))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        rt.block_on(client.inspect().unwrap())
            .unwrap()
            .counts
            .pending_inbound,
        0
    );
    assert!(rt.block_on(client.next_timer().unwrap()).unwrap().is_none());
    let delivery = rt.block_on(client.next_effect().unwrap()).unwrap().unwrap();
    assert!(matches!(
        delivery.effect,
        ae_p2p::registry::Effect::Close(_)
    ));
    assert!(
        rt.block_on(client.acknowledge(delivery.id).unwrap())
            .unwrap()
    );
    assert_eq!(
        rt.block_on(
            client
                .apply(timer.deadline, Command::Timeout(timer.token))
                .unwrap()
        )
        .unwrap(),
        Outcome::Stale
    );
    assert!(
        rt.block_on(client.next_effect().unwrap())
            .unwrap()
            .is_none()
    );
    worker.shutdown().unwrap();
}

#[tokio::test]
async fn borrowed_async_join_keeps_executor_live_and_cancelled_wait_retains_owner() {
    let temp = Temp::new();
    let mut worker = Worker::start(service(db(&temp), config()), Settings::default()).unwrap();
    let client = worker.client();
    let (started, observing) = oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let running = client
        .submit(256, move |s| {
            started.send(()).unwrap();
            gate.recv_timeout(Duration::from_secs(10)).unwrap();
            s.apply(1, Command::Add(peer(1, false)))
        })
        .unwrap();
    observing.await.unwrap();
    let queued = client.apply(2, Command::Add(peer(2, false))).unwrap();
    worker.stop();
    assert!(matches!(client.inspect(), Err(Error::Stopped)));
    // A current-thread timer must run while the actual store worker is gated.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), worker.join())
            .await
            .is_err()
    );
    assert!(worker.join.is_some());
    assert_eq!(client.outstanding().0, 2);
    release.send(()).unwrap();
    let service = tokio::time::timeout(Duration::from_secs(10), worker.join())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        service.saved_peers().map(|p| p.id).collect::<Vec<_>>(),
        vec![peer(1, false).id]
    );
    assert!(matches!(worker.join().await, Err(Error::Stopped)));
    running.await.unwrap();
    assert!(matches!(queued.await, Err(Error::Stopped)));
    assert_eq!(client.outstanding(), (0, 0));
}
