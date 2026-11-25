use super::*;
use std::{
    net::IpAddr,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};
struct Log {
    bytes: Arc<Mutex<Vec<u8>>>,
    flushes: Arc<AtomicUsize>,
}
impl io::Write for Log {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn graceful_log_shutdown_drains_records_even_before_the_first_worker_iteration() {
    let bytes = Arc::new(Mutex::new(vec![]));
    let flushes = Arc::new(AtomicUsize::new(0));
    let config = Config::default();
    let record = feed::Ring::new().push("test", json!(42), &config).unwrap();
    let mut worker = Worker::prepared(
        Destination::Log(Box::new(Log {
            bytes: bytes.clone(),
            flushes: flushes.clone(),
        })),
        &config,
        "ae-log-drain-test",
        false,
    )
    .unwrap();
    assert!(worker.client().submit(Batch::Log(record.clone())));
    {
        let mut state = worker.shared.state.lock().unwrap();
        state.started = true;
        state.stop = true;
        worker.shared.wake.notify_one();
    }
    worker.finish().await;
    assert_eq!(*bytes.lock().unwrap(), record.json_line());
    assert_eq!(flushes.load(Ordering::SeqCst), 1);
    assert_eq!(worker.info().written, 1);
    assert_eq!(worker.info().dropped, 0);
    assert!(worker.info().joined);
}
struct Network {
    failures: usize,
    opens: Arc<Mutex<Vec<Instant>>>,
}
impl statsd::Backend for Network {
    fn hostname(&mut self) -> io::Result<String> {
        Ok("localhost".into())
    }
    fn resolve(&mut self, _: &str) -> io::Result<IpAddr> {
        Ok("127.0.0.1".parse().unwrap())
    }
    fn open(&mut self, _: SocketAddr) -> io::Result<std::net::UdpSocket> {
        self.opens.lock().unwrap().push(Instant::now());
        if self.failures > 0 {
            self.failures -= 1;
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        std::net::UdpSocket::bind("127.0.0.1:0")
    }
}
fn worker(backend: impl statsd::Backend + 'static, target: SocketAddr, delay: Duration) -> Worker {
    Worker::prepared(
        Destination::Udp(statsd::Udp::with_backend(
            statsd::Target::Configured(statsd::Config {
                host: statsd::Host::Name("127.0.0.1".into()),
                port: target.port(),
                reconnect_interval: delay,
            }),
            Box::new(backend),
        )),
        &Config {
            sink_records: 2,
            sink_bytes: 8192,
            record_bytes: 1024,
            ..Default::default()
        },
        "ae-metrics-test",
        true,
    )
    .unwrap()
}
#[tokio::test]
async fn open_failures_retry_on_their_own_timer_and_shutdown_cancels_pending_reconnect() {
    let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for delay in [Duration::ZERO, Duration::from_millis(40)] {
        let opens = Arc::new(Mutex::new(vec![]));
        let mut worker = worker(
            Network {
                failures: 1,
                opens: opens.clone(),
            },
            receiver.local_addr().unwrap(),
            delay,
        );
        let mut client = worker.client();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !client.info().connected {
                client.changed().await;
            }
        })
        .await
        .unwrap();
        let attempts = opens.lock().unwrap().clone();
        assert_eq!(attempts.len(), 2);
        assert!(attempts[1].duration_since(attempts[0]) >= delay);
        assert!(client.submit(Batch::Metrics(vec![b"native-reconnect:1|g".to_vec()])));
        let mut bytes = [0; 64];
        let (n, _) = tokio::time::timeout(Duration::from_secs(3), receiver.recv_from(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&bytes[..n], b"native-reconnect:1|g");
        worker.finish().await;
        assert!(client.info().joined);
        assert_eq!(client.info().open_attempts, 2);
    }
    let opens = Arc::new(Mutex::new(vec![]));
    let mut worker = worker(
        Network {
            failures: usize::MAX,
            opens: opens.clone(),
        },
        receiver.local_addr().unwrap(),
        Duration::from_secs(60),
    );
    let mut client = worker.client();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !client.info().ready {
            client.changed().await;
        }
    })
    .await
    .unwrap();
    assert!(!client.info().connected);
    assert_eq!(client.info().open_attempts, 1);
    tokio::time::timeout(Duration::from_secs(1), worker.finish())
        .await
        .unwrap();
    assert_eq!(opens.lock().unwrap().len(), 1);
    assert!(client.info().joined);
}
struct ResolveGate {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
    opens: Arc<AtomicUsize>,
}
impl statsd::Backend for ResolveGate {
    fn hostname(&mut self) -> io::Result<String> {
        Ok("localhost".into())
    }
    fn resolve(&mut self, _: &str) -> io::Result<IpAddr> {
        let _ = self.entered.take().unwrap().send(());
        self.release.recv().unwrap();
        Ok("127.0.0.1".parse().unwrap())
    }
    fn open(&mut self, _: SocketAddr) -> io::Result<std::net::UdpSocket> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        std::net::UdpSocket::bind("127.0.0.1:0")
    }
}
struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
#[tokio::test]
async fn unresolved_jobs_hold_their_limits_and_shutdown_joins_the_actual_lookup() {
    let (tx, entered) = tokio::sync::oneshot::channel();
    let (release, rx) = std::sync::mpsc::channel();
    let opens = Arc::new(AtomicUsize::new(0));
    let mut worker = worker(
        ResolveGate {
            entered: Some(tx),
            release: rx,
            opens: opens.clone(),
        },
        "127.0.0.1:8125".parse().unwrap(),
        Duration::ZERO,
    );
    let release = Release(Some(release));
    let client = worker.client();
    tokio::time::timeout(Duration::from_secs(3), entered)
        .await
        .unwrap()
        .unwrap();
    assert!(client.submit(Batch::Metrics(vec![vec![1; 1024]])));
    assert!(client.submit(Batch::Metrics(vec![vec![2; 1024]])));
    assert!(!client.submit(Batch::Metrics(vec![vec![3; 1024]])));
    assert_eq!(client.info().outstanding, 2);
    assert!(!client.info().ready);
    client.stop();
    let joined = tokio::spawn(async move {
        worker.finish().await;
    });
    tokio::task::yield_now().await;
    assert!(!joined.is_finished());
    assert!(!client.info().joined);
    drop(release);
    tokio::time::timeout(Duration::from_secs(3), joined)
        .await
        .unwrap()
        .unwrap();
    assert!(client.info().joined);
    assert_eq!(client.info().outstanding, 0);
    assert_eq!(client.info().retained_bytes, 0);
    assert_eq!(opens.load(Ordering::SeqCst), 0);
}
