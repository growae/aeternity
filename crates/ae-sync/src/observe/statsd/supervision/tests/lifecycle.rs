use super::*;
use crate::observe::{sink, statsd};
use std::sync::atomic::AtomicUsize;

struct Gate {
    first: bool,
    entered: Option<oneshot::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}
impl statsd::Backend for Gate {
    fn hostname(&mut self) -> io::Result<String> {
        Ok("localhost".into())
    }
    fn resolve(&mut self, _: &str) -> io::Result<IpAddr> {
        Ok("127.0.0.1".parse().unwrap())
    }
    fn open(&mut self, _: SocketAddr) -> io::Result<UdpSocket> {
        if std::mem::replace(&mut self.first, false) {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        let _ = self.entered.take().unwrap().send(());
        self.release.recv().unwrap();
        UdpSocket::bind("127.0.0.1:0")
    }
}
struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(s) = self.0.take() {
            let _ = s.send(());
        }
    }
}
#[tokio::test]
async fn replacement_joins_blocked_work_and_never_replays_its_accepted_mailbox() {
    let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (tx, entered) = oneshot::channel();
    let (release, rx) = std::sync::mpsc::channel();
    let mut gate = Some(Gate {
        first: true,
        entered: Some(tx),
        release: rx,
    });
    let created = Arc::new(AtomicUsize::new(0));
    let count = created.clone();
    let mut config = settings(receiver.local_addr().unwrap().port());
    config.records = 2;
    config.bytes = 8192;
    config.target.reconnect_interval = Duration::ZERO;
    let mut owner = Owner::configured(
        config,
        Box::new(|| 0),
        Box::new(move |s| {
            count.fetch_add(1, Ordering::SeqCst);
            let backend: Box<dyn statsd::Backend> = match gate.take() {
                Some(gate) => Box::new(gate),
                None => Box::new(Resolve {
                    fail: Arc::new(AtomicBool::new(false)),
                }),
            };
            sink::Worker::metrics_with_backend(&s.sink(), backend).map(Worker)
        }),
    )
    .unwrap();
    let release = Release(Some(release));
    owner.activate();
    tokio::time::timeout(Duration::from_secs(3), entered)
        .await
        .unwrap()
        .unwrap();
    advance(&mut owner, |s| s.phase == Phase::Running).await;
    let stable = owner.client();
    assert!(stable.submit(vec![b"old:1|g".to_vec()]));
    assert!(stable.submit(vec![b"old:2|g".to_vec()]));
    assert!(!stable.submit(vec![b"full:3|g".to_vec()]));
    assert_eq!(stable.info().outstanding, 2);
    let mut handle = owner.handle();
    let old = handle.current().unwrap();
    let ticket = handle.restart(old.number).unwrap();
    let job = tokio::spawn(async move {
        owner.poll().await;
        owner
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while handle.status().phase != Phase::Retiring {
            handle.changed().await;
        }
    })
    .await
    .unwrap();
    assert!(!job.is_finished());
    assert!(!old.client.info().joined);
    assert!(!stable.info().ready);
    assert_eq!(created.load(Ordering::SeqCst), 1);
    assert!(!stable.submit(vec![b"retiring:4|g".to_vec()]));
    drop(release);
    let mut owner = tokio::time::timeout(Duration::from_secs(3), job)
        .await
        .unwrap()
        .unwrap();
    advance(&mut owner, |s| s.phase == Phase::Running).await;
    assert_eq!(ticket.wait().await, Ok(2));
    assert!(old.client.info().joined);
    assert_eq!(old.client.info().outstanding, 0);
    assert_eq!(old.client.info().retained_bytes, 0);
    assert_eq!(stable.info().dropped, 4);
    let mut bytes = [0; 64];
    assert_eq!(
        receiver.try_recv(&mut bytes).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    packet(&mut owner, &receiver).await;
    assert_eq!(stable.info().written, 1);
    owner.finish().await;
}
#[tokio::test]
async fn returned_and_unwound_constructors_join_partial_workers_and_keep_native_start_distinction()
{
    let mode = Arc::new(AtomicUsize::new(0));
    let partial = Arc::new(Mutex::new(vec![]));
    let retained = partial.clone();
    let control = mode.clone();
    let mut owner = Owner::configured(
        settings(0),
        Box::new(|| 0),
        Box::new(move |s| {
            if control.load(Ordering::SeqCst) == 0 {
                return Err(WorkerError::Io(io::ErrorKind::PermissionDenied));
            }
            let worker = Worker::prepare(s)?;
            if control.load(Ordering::SeqCst) == 1 {
                retained.lock().unwrap().push(worker.client());
                panic!("controlled partial metrics constructor unwind");
            }
            Ok(worker)
        }),
    )
    .unwrap();
    owner.activate();
    assert_eq!(owner.handle().status().phase, Phase::Empty);
    assert_eq!(owner.handle().status().child_restart_attempts, 0);
    mode.store(1, Ordering::SeqCst);
    let ticket = owner.handle().start(1).unwrap();
    owner.poll().await;
    assert!(matches!(ticket.wait().await, Err(Error::Worker(_))));
    assert_eq!(owner.handle().status().child_restart_attempts, 0);
    {
        let retired = partial.lock().unwrap();
        assert_eq!(retired.len(), 1);
        assert!(retired[0].info().joined);
        assert_eq!(retired[0].info().open_attempts, 0);
    }
    mode.store(2, Ordering::SeqCst);
    let ticket = owner.handle().start(1).unwrap();
    advance(&mut owner, |s| s.phase == Phase::Running).await;
    assert_eq!(ticket.wait().await, Ok(3));
    mode.store(0, Ordering::SeqCst);
    let ticket = owner.handle().restart(3).unwrap();
    owner.poll().await;
    assert_eq!(ticket.wait().await, Err(Error::ChildLimit));
    assert_eq!(owner.handle().status().generation, 6);
    assert_eq!(owner.handle().status().child_restart_attempts, 4);
    owner.finish().await;
}
#[tokio::test]
async fn shutdown_and_drop_close_pending_controls_and_do_not_spin_or_activate_a_paused_child() {
    let mut owner = Owner::prepare(settings(8125)).unwrap();
    let client = owner.client();
    let old = owner.handle().current().unwrap();
    assert!(client.submit(vec![b"paused:1|g".to_vec()]));
    let ticket = owner.handle().restart(1).unwrap();
    owner.finish().await;
    assert_eq!(ticket.wait().await, Err(Error::Closed));
    assert!(old.client.info().joined);
    assert_eq!(client.info().open_attempts, 0);
    assert_eq!(client.info().dropped, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), owner.changed())
            .await
            .is_err()
    );
    owner.finish().await;
    assert_eq!(client.info().dropped, 1);
    let owner = Owner::prepare(settings(8125)).unwrap();
    let client = owner.client();
    let ticket = owner.handle().restart(1).unwrap();
    drop(owner);
    assert_eq!(ticket.wait().await, Err(Error::Closed));
    assert!(client.info().joined && !client.info().running);
    assert!(!client.submit(vec![b"closed:2|g".to_vec()]));
}

#[tokio::test]
async fn cancelled_retirement_and_shutdown_waits_keep_the_actual_worker_owned_until_joined() {
    for shutting_down in [false, true] {
        let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (entered, waiting) = oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let (unblock, requested) = std::sync::mpsc::channel();
        // Also release on a failed regression so an old synchronous Drop join
        // cannot hang the test process forever.
        let releaser = std::thread::spawn(move || {
            let _ = requested.recv_timeout(Duration::from_secs(3));
            let _ = release.send(());
        });
        let mut gate = Some(Gate {
            first: true,
            entered: Some(entered),
            release: blocked,
        });
        let mut config = settings(receiver.local_addr().unwrap().port());
        config.target.reconnect_interval = Duration::ZERO;
        let mut owner = Owner::configured(
            config,
            Box::new(|| 0),
            Box::new(move |s| {
                let backend: Box<dyn statsd::Backend> = match gate.take() {
                    Some(gate) => Box::new(gate),
                    None => Box::new(Resolve {
                        fail: Arc::new(AtomicBool::new(false)),
                    }),
                };
                sink::Worker::metrics_with_backend(&s.sink(), backend).map(Worker)
            }),
        )
        .unwrap();
        owner.activate();
        tokio::time::timeout(Duration::from_secs(3), waiting)
            .await
            .unwrap()
            .unwrap();
        advance(&mut owner, |s| s.phase == Phase::Running).await;
        let old = owner.handle().current().unwrap();
        let ticket = owner.handle().restart(old.number).unwrap();
        let timed_out = if shutting_down {
            tokio::time::timeout(Duration::from_millis(20), owner.finish())
                .await
                .is_err()
        } else {
            tokio::time::timeout(Duration::from_millis(20), owner.poll())
                .await
                .is_err()
        };
        assert!(timed_out);
        assert_eq!(owner.handle().status().phase, Phase::Retiring);
        assert!(
            !old.client.info().joined,
            "cancelled wait synchronously joined blocked work"
        );
        assert!(!owner.client().info().ready);
        let _ = unblock.send(());
        releaser.join().unwrap();
        if shutting_down {
            owner.poll().await;
            assert_eq!(ticket.wait().await, Err(Error::Closed));
            assert_eq!(owner.handle().status().phase, Phase::Stopped);
            assert_eq!(owner.handle().status().generation, old.number);
        } else {
            advance(&mut owner, |s| s.phase == Phase::Running).await;
            assert_eq!(ticket.wait().await, Ok(old.number + 1));
            assert_eq!(owner.handle().status().child_restart_attempts, 1);
            packet(&mut owner, &receiver).await;
            owner.finish().await;
        }
        assert!(old.client.info().joined);
        assert!(owner.client().info().joined);
    }
}
