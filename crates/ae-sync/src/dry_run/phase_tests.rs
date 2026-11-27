use super::*;
use std::sync::{atomic::AtomicUsize, mpsc};
use tokio::sync::oneshot;
struct Guard(Arc<AtomicUsize>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
fn pool() -> (Client, Worker) {
    create(Config {
        threads: 1,
        requests: 1,
        bytes: 8,
    })
    .unwrap()
}
async fn within<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("bounded phase test")
}

#[tokio::test]
async fn native_clock_excludes_preparation_and_rendering_but_retains_their_ownership() {
    let (client, owner) = pool();
    let drops = Arc::new(AtomicUsize::new(0));
    let reader = Guard(drops.clone());
    let (before, entered) = oneshot::channel();
    let (release_before, wait_before) = mpsc::channel();
    let (done, completed) = oneshot::channel();
    let (release_after, wait_after) = mpsc::channel();
    let ticket = client
        .reserve(8, Timeout::After(Duration::from_secs(4)))
        .unwrap()
        .submit_phased(move |control| {
            before.send(()).unwrap();
            wait_before.recv().unwrap();
            control
                .start_native(Timeout::After(Duration::from_millis(100)))
                .unwrap();
            control.finish_native().unwrap();
            done.send(()).unwrap();
            wait_after.recv().unwrap();
            control.check().unwrap();
            reader
        })
        .unwrap();
    within(entered).await.unwrap();
    let mut waiting = tokio::spawn(ticket.wait_retained());
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut waiting)
            .await
            .is_err()
    );
    release_before.send(()).unwrap();
    within(completed).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut waiting)
            .await
            .is_err()
    );
    assert_eq!(client.available(), (0, 0));
    assert_eq!(drops.load(Ordering::Acquire), 0);
    release_after.send(()).unwrap();
    let held = within(waiting).await.unwrap().unwrap();
    owner.shutdown().await;
    assert_eq!(drops.load(Ordering::Acquire), 0);
    assert_eq!(client.available(), (0, 0));
    drop(held);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert_eq!(client.available(), (1, 8));
}

#[tokio::test]
async fn native_timeout_latches_while_blocked_work_keeps_its_source_and_credits() {
    let (client, owner) = pool();
    let drops = Arc::new(AtomicUsize::new(0));
    let reader = Guard(drops.clone());
    let (started, entered) = oneshot::channel();
    let (release, wait) = mpsc::channel();
    let (checked, seen) = oneshot::channel();
    let ticket = client
        .reserve(8, Timeout::Unbounded)
        .unwrap()
        .submit_phased(move |control| {
            let _reader = reader;
            control
                .start_native(Timeout::After(Duration::from_millis(50)))
                .unwrap();
            started.send(()).unwrap();
            wait.recv().unwrap();
            checked
                .send((
                    control.check(),
                    control.finish_native(),
                    control.start_native(Timeout::Unbounded),
                ))
                .unwrap();
        })
        .unwrap();
    within(entered).await.unwrap();
    assert!(matches!(
        within(ticket.wait()).await,
        Err(Error::NativeTimeout)
    ));
    assert_eq!(client.available(), (0, 0));
    assert_eq!(drops.load(Ordering::Acquire), 0);
    release.send(()).unwrap();
    assert_eq!(
        within(seen).await.unwrap(),
        (
            Err(Error::NativeTimeout),
            Err(Error::NativeTimeout),
            Err(Error::NativeTimeout)
        )
    );
    owner.shutdown().await;
    assert_eq!(client.available(), (1, 8));
    assert_eq!(drops.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn service_deadline_still_applies_after_native_completion() {
    let (client, owner) = pool();
    let (started, entered) = oneshot::channel();
    let (release, wait) = mpsc::channel();
    let ticket = client
        .reserve(8, Timeout::After(Duration::from_millis(150)))
        .unwrap()
        .submit_phased(move |control| {
            control.start_native(Timeout::Unbounded).unwrap();
            control.finish_native().unwrap();
            started.send(()).unwrap();
            wait.recv().unwrap();
            control.check()
        })
        .unwrap();
    within(entered).await.unwrap();
    assert_eq!(within(ticket.wait()).await, Err(Error::Timeout));
    assert_eq!(client.available(), (0, 0));
    release.send(()).unwrap();
    owner.shutdown().await;
}

#[tokio::test]
async fn native_phase_cannot_hide_caller_drop_or_owner_shutdown() {
    for stopping in [false, true] {
        let (client, owner) = pool();
        let (started, entered) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        let (checked, seen) = oneshot::channel();
        let ticket = client
            .reserve(8, Timeout::Unbounded)
            .unwrap()
            .submit_phased(move |control| {
                control.start_native(Timeout::Unbounded).unwrap();
                started.send(()).unwrap();
                wait.recv().unwrap();
                checked.send(control.check()).unwrap();
            })
            .unwrap();
        within(entered).await.unwrap();
        let mut ticket = Some(ticket);
        if stopping {
            owner.stop()
        } else {
            drop(ticket.take())
        }
        assert_eq!(client.available(), (0, 0));
        release.send(()).unwrap();
        assert_eq!(
            within(seen).await.unwrap(),
            Err(if stopping {
                Error::Stopped
            } else {
                Error::Cancelled
            })
        );
        if let Some(ticket) = ticket {
            assert_eq!(within(ticket.wait()).await, Err(Error::Stopped))
        }
        owner.shutdown().await;
        assert_eq!(client.available(), (1, 8));
    }
}

#[test]
fn native_phase_is_single_use_and_preserves_zero_disabled_and_overflow_distinctions() {
    let clock = phase::Clock::new();
    assert_eq!(clock.finish(), Err(Error::Configuration));
    assert_eq!(
        clock.start(Timeout::After(Duration::MAX)),
        Err(Error::Configuration)
    );
    clock.start(Timeout::After(Duration::ZERO)).unwrap();
    assert_eq!(clock.check(), Err(Error::NativeTimeout));
    assert_eq!(clock.finish(), Err(Error::NativeTimeout));
    assert_eq!(clock.start(Timeout::Unbounded), Err(Error::Configuration));
    let clock = phase::Clock::new();
    clock.start(Timeout::Unbounded).unwrap();
    assert_eq!(clock.check(), Ok(()));
    assert_eq!(clock.start(Timeout::Unbounded), Err(Error::Configuration));
    clock.finish().unwrap();
    assert_eq!(clock.finish(), Err(Error::Configuration));
    assert_eq!(clock.check(), Ok(()));
}
