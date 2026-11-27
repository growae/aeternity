use super::*;
use std::sync::{atomic::AtomicUsize, mpsc};
use tokio::sync::oneshot;

async fn within<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("bounded test")
}
fn config() -> Config {
    Config {
        threads: 1,
        requests: 1,
        bytes: 8,
    }
}
struct Guard(Arc<AtomicUsize>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

#[tokio::test]
async fn completed_response_keeps_capacity_through_reader_and_output_ownership() {
    let (client, owner) = create(config()).unwrap();
    assert!(matches!(
        client.reserve(8, Timeout::After(Duration::MAX)),
        Err(Error::Configuration)
    ));
    assert_eq!(client.available(), (1, 8));
    let drops = Arc::new(AtomicUsize::new(0));
    let reader = Guard(drops.clone());
    let ticket = client
        .submit(8, Timeout::Unbounded, move |control| {
            control.check().unwrap();
            (reader, 19)
        })
        .unwrap();
    let retained = within(ticket.wait_retained()).await.unwrap();
    assert_eq!(retained.value.1, 19);
    assert_eq!(client.available(), (0, 0));
    assert!(matches!(
        client.reserve(1, Timeout::Unbounded),
        Err(Error::Busy)
    ));
    owner.shutdown().await;
    assert_eq!(drops.load(Ordering::Acquire), 0);
    assert_eq!(client.available(), (0, 0));
    drop(retained.value);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    drop(retained.credits);
    assert_eq!(client.available(), (1, 8));
    assert!(matches!(
        client.reserve(1, Timeout::Unbounded),
        Err(Error::Stopped)
    ));
}

#[tokio::test]
async fn delayed_polling_preserves_timely_results_and_rejects_expired_queued_work() {
    let (client, owner) = create(Config {
        requests: 2,
        ..config()
    })
    .unwrap();
    let timely = client
        .submit(4, Timeout::After(Duration::from_millis(100)), |_| 19)
        .unwrap();
    within(client.pool.drain()).await;
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert_eq!(within(timely.wait()).await, Ok(19));
    let (release, wait) = mpsc::channel();
    let (started, entered) = oneshot::channel();
    let first = client
        .submit(4, Timeout::Unbounded, move |_| {
            started.send(()).unwrap();
            wait.recv().unwrap();
        })
        .unwrap();
    within(entered).await.unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let flag = called.clone();
    let expired = client
        .submit(4, Timeout::After(Duration::from_millis(5)), move |_| {
            flag.store(true, Ordering::Release)
        })
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    release.send(()).unwrap();
    within(client.pool.drain()).await;
    assert_eq!(within(first.wait()).await, Ok(()));
    assert_eq!(within(expired.wait()).await, Err(Error::Timeout));
    assert!(!called.load(Ordering::Acquire));
    owner.shutdown().await;
    assert_eq!(client.available(), (2, 8));
}
#[tokio::test]
async fn timeout_and_caller_drop_hold_capacity_until_nonpreemptible_work_exits() {
    for caller_drop in [false, true] {
        let (client, owner) = create(config()).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let reader = Guard(drops.clone());
        let (release, wait) = mpsc::channel();
        let (started, entered) = oneshot::channel();
        let (checked, observed) = oneshot::channel();
        let ticket = client
            .submit(
                8,
                if caller_drop {
                    Timeout::Unbounded
                } else {
                    Timeout::After(Duration::from_millis(100))
                },
                move |control| {
                    let _reader = reader;
                    started.send(()).unwrap();
                    wait.recv().unwrap();
                    checked.send(control.check()).unwrap();
                    19
                },
            )
            .unwrap();
        within(entered).await.unwrap();
        if caller_drop {
            drop(ticket);
        } else {
            assert_eq!(within(ticket.wait()).await, Err(Error::Timeout));
        }
        assert_eq!(client.available(), (0, 0));
        assert_eq!(drops.load(Ordering::Acquire), 0);
        assert!(matches!(
            client.reserve(1, Timeout::Unbounded),
            Err(Error::Busy)
        ));
        release.send(()).unwrap();
        assert_eq!(
            within(observed).await.unwrap(),
            Err(if caller_drop {
                Error::Cancelled
            } else {
                Error::Timeout
            })
        );
        owner.shutdown().await;
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert_eq!(client.available(), (1, 8));
    }
}
#[tokio::test]
async fn expired_or_cancelled_queued_work_never_enters_the_operation() {
    let (client, owner) = create(Config {
        requests: 2,
        ..config()
    })
    .unwrap();
    let (release, wait) = mpsc::channel();
    let (started, entered) = oneshot::channel();
    let first = client
        .submit(4, Timeout::Unbounded, move |_| {
            started.send(()).unwrap();
            wait.recv().unwrap();
        })
        .unwrap();
    within(entered).await.unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let flag = called.clone();
    let queued = client
        .submit(4, Timeout::After(Duration::ZERO), move |_| {
            flag.store(true, Ordering::Release);
        })
        .unwrap();
    assert_eq!(within(queued.wait()).await, Err(Error::Timeout));
    assert_eq!(client.available(), (0, 0));
    release.send(()).unwrap();
    within(first.wait()).await.unwrap();
    owner.shutdown().await;
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(client.available(), (2, 8));
}
#[tokio::test]
async fn shutdown_cancels_running_work_and_discards_queued_readers() {
    let (client, owner) = create(Config {
        requests: 2,
        ..config()
    })
    .unwrap();
    let (started, entered) = oneshot::channel();
    let (release, wait) = mpsc::channel();
    let first = client
        .submit(4, Timeout::Unbounded, move |control| {
            started.send(()).unwrap();
            wait.recv().unwrap();
            control.check()
        })
        .unwrap();
    within(entered).await.unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let reader = Guard(drops.clone());
    let queued = client
        .submit(4, Timeout::Unbounded, move |_| {
            let _reader = reader;
            panic!("queued work after stop")
        })
        .unwrap();
    owner.stop();
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert_eq!(within(queued.wait()).await, Err(Error::Stopped));
    assert_eq!(client.available(), (1, 4));
    release.send(()).unwrap();
    assert_eq!(within(first.wait()).await, Err(Error::Stopped));
    within(owner.shutdown()).await;
    assert_eq!(client.available(), (2, 8));
}
#[tokio::test]
async fn panic_is_contained_and_does_not_poison_the_next_request() {
    let (client, owner) = create(config()).unwrap();
    let failed = client
        .submit(8, Timeout::Unbounded, |_| -> () {
            panic!("fixture failure")
        })
        .unwrap();
    assert_eq!(within(failed.wait()).await, Err(Error::Panicked));
    let next = client
        .submit(8, Timeout::Unbounded, |_| Err::<(), _>("native error"))
        .unwrap();
    assert_eq!(within(next.wait()).await, Ok(Err("native error")));
    owner.shutdown().await;
    assert_eq!(client.available(), (1, 8));
}
#[tokio::test]
async fn dropping_the_owner_joins_even_with_live_clients_and_cancelled_waiters() {
    let (client, owner) = create(config()).unwrap();
    let (release, wait) = mpsc::channel();
    let (started, entered) = oneshot::channel();
    let ticket = client
        .submit(8, Timeout::Unbounded, move |control| {
            started.send(()).unwrap();
            wait.recv().unwrap();
            assert_eq!(control.check(), Err(Error::Stopped));
        })
        .unwrap();
    within(entered).await.unwrap();
    drop(ticket);
    let (dropping, observed) = oneshot::channel();
    let (stopped, done) = oneshot::channel();
    let thread = std::thread::spawn(move || {
        owner.stop();
        dropping.send(()).unwrap();
        drop(owner);
        stopped.send(()).unwrap();
    });
    within(observed).await.unwrap();
    let mut done = Box::pin(done);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut done)
            .await
            .is_err()
    );
    assert_eq!(client.available(), (0, 0));
    release.send(()).unwrap();
    within(done).await.unwrap();
    thread.join().unwrap();
    assert_eq!(client.available(), (1, 8));
}
