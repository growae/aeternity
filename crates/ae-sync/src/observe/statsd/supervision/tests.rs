use super::*;
use std::{
    io,
    net::{IpAddr, SocketAddr, UdpSocket},
    sync::atomic::AtomicU64,
    time::Duration,
};

async fn advance(owner: &mut Owner, predicate: impl Fn(&Status) -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            owner.poll().await;
            if predicate(&owner.handle().status()) {
                break;
            }
            owner.changed().await;
        }
    })
    .await
    .unwrap();
}
fn settings(port: u16) -> Settings {
    Settings {
        target: super::super::Config {
            host: super::super::Host::Name("127.0.0.1".into()),
            port,
            ..Default::default()
        },
        ..Default::default()
    }
}
async fn packet(owner: &mut Owner, receiver: &tokio::net::UdpSocket) {
    let client = owner.client();
    assert!(client.submit(vec![b"ae.metric:42|g".to_vec()]));
    let mut bytes = [0; 64];
    let (n, _) = tokio::time::timeout(Duration::from_secs(2), receiver.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..n], b"ae.metric:42|g");
}
#[tokio::test]
async fn real_worker_panic_and_administrative_exit_replace_only_the_logger() {
    let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut owner = Owner::prepare(settings(receiver.local_addr().unwrap().port())).unwrap();
    let stable = owner.client();
    owner.activate();
    advance(&mut owner, |s| s.phase == Phase::Running).await;
    packet(&mut owner, &receiver).await;
    assert!(stable.submit(vec![vec![0; 65536], b"ae.metric:42|g".to_vec()]));
    let mut bytes = [0; 64];
    let (n, _) = receiver.recv_from(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..n], b"ae.metric:42|g");
    owner.poll().await;
    assert_eq!(owner.handle().status().generation, 1);
    assert_eq!(owner.handle().status().child_restart_attempts, 0);
    let first = owner.handle().current().unwrap();
    owner.worker.as_ref().unwrap().0.client().panic_for_test();
    advance(&mut owner, |s| {
        s.generation == 2 && s.phase == Phase::Running
    })
    .await;
    assert!(first.client.info().joined && first.client.info().panicked);
    assert!(!first.client.submit(vec![b"stale".to_vec()]));
    assert!(stable.same_owner(&owner.client()));
    packet(&mut owner, &receiver).await;
    let second = owner.handle().current().unwrap();
    let ticket = owner.handle().restart(2).unwrap();
    advance(&mut owner, |s| {
        s.generation == 3 && s.phase == Phase::Running
    })
    .await;
    assert_eq!(ticket.wait().await, Ok(3));
    assert!(second.client.info().joined);
    packet(&mut owner, &receiver).await;
    assert_eq!(stable.info().open_attempts, 3);
    assert_eq!(owner.handle().status().replacements, 2);
    let third = owner.handle().current().unwrap();
    third.client.request_shutdown();
    advance(&mut owner, |s| {
        s.generation == 4 && s.phase == Phase::Running
    })
    .await;
    assert!(third.client.info().joined);
    packet(&mut owner, &receiver).await;
    assert_eq!(stable.info().open_attempts, 4);
    assert_eq!(owner.handle().status().replacements, 3);
    owner.finish().await;
    assert!(stable.info().joined && !stable.info().running);
}
#[tokio::test]
async fn nested_native_restart_limits_recreate_an_empty_dynamic_supervisor() {
    let native: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_supervision.json"
    ))
    .unwrap();
    let mut owner =
        Owner::configured(settings(0), Box::new(|| 0), Box::new(Worker::prepare)).unwrap();
    owner.activate();
    advance(&mut owner, |s| s.phase == Phase::Running).await;
    for row in native["cycles"].as_array().unwrap() {
        for _ in 0..row["successful_restarts"].as_u64().unwrap() {
            let generation = owner.handle().status().generation;
            let old = owner.handle().current().unwrap();
            let ticket = owner.handle().restart(generation).unwrap();
            advance(&mut owner, |s| {
                s.generation > generation && s.phase == Phase::Running
            })
            .await;
            assert_eq!(ticket.wait().await, Ok(generation + 1));
            assert!(old.client.info().joined);
        }
        let status = owner.handle().status();
        let ticket = owner.handle().restart(status.generation).unwrap();
        advance(&mut owner, |s| {
            matches!(s.phase, Phase::Empty | Phase::Exhausted)
        })
        .await;
        assert!(owner.handle().current().is_none());
        assert!(owner.client().info().joined);
        assert!(!owner.client().submit(vec![b"no logger".to_vec()]));
        if row["parent_survived"].as_bool().unwrap() {
            assert_eq!(ticket.wait().await, Err(Error::ChildLimit));
            assert_eq!(
                owner.handle().status().supervisor_generation,
                status.supervisor_generation + 1
            );
            assert_eq!(row["logger_children_after_parent_restart"], 0);
            let ticket = owner
                .handle()
                .start(status.supervisor_generation + 1)
                .unwrap();
            advance(&mut owner, |s| s.phase == Phase::Running).await;
            assert!(ticket.wait().await.is_ok());
        } else {
            assert_eq!(ticket.wait().await, Err(Error::ParentLimit));
            let ticket = owner.handle().start(status.supervisor_generation).unwrap();
            owner.poll().await;
            assert_eq!(ticket.wait().await, Err(Error::ParentLimit));
        }
    }
    let status = owner.handle().status();
    assert_eq!(status.child_restart_attempts, 24);
    assert_eq!(status.parent_restart_attempts, 6);
    assert_eq!(status.replacements, 18);
    owner.finish().await;
}

struct Resolve {
    fail: Arc<AtomicBool>,
}
impl super::super::Backend for Resolve {
    fn hostname(&mut self) -> io::Result<String> {
        Ok("localhost".into())
    }
    fn resolve(&mut self, _: &str) -> io::Result<IpAddr> {
        if self.fail.load(Ordering::Acquire) {
            Err(io::ErrorKind::AddrNotAvailable.into())
        } else {
            Ok("127.0.0.1".parse().unwrap())
        }
    }
    fn open(&mut self, _: SocketAddr) -> io::Result<UdpSocket> {
        UdpSocket::bind("127.0.0.1:0")
    }
}
fn controlled(fail: Arc<AtomicBool>, clock: Arc<AtomicU64>) -> Owner {
    Owner::configured(
        settings(0),
        Box::new(move || clock.load(Ordering::Acquire)),
        Box::new(move |s| {
            crate::observe::sink::Worker::metrics_with_backend(
                &s.sink(),
                Box::new(Resolve { fail: fail.clone() }),
            )
            .map(Worker)
        }),
    )
    .unwrap()
}
#[tokio::test]
async fn initial_rejection_is_not_retried_but_failed_reinitialization_consumes_the_budget() {
    let native: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_logger.json"
    ))
    .unwrap();
    assert_eq!(native["cases"][0]["child_retained"], false);
    assert_eq!(native["cases"][8]["logger_supervisor_shutdown"], true);
    let fail = Arc::new(AtomicBool::new(true));
    let mut owner = controlled(fail.clone(), Arc::new(AtomicU64::new(0)));
    owner.activate();
    advance(&mut owner, |s| s.phase == Phase::Empty).await;
    let status = owner.handle().status();
    assert_eq!(status.generation, 1);
    assert_eq!(status.child_restart_attempts, 0);
    assert!(owner.client().info().joined);
    owner.poll().await;
    assert_eq!(owner.handle().status().generation, 1);
    fail.store(false, Ordering::Release);
    let ticket = owner.handle().start(status.supervisor_generation).unwrap();
    advance(&mut owner, |s| s.phase == Phase::Running).await;
    assert_eq!(ticket.wait().await, Ok(2));
    fail.store(true, Ordering::Release);
    let ticket = owner.handle().restart(2).unwrap();
    advance(&mut owner, |s| s.phase == Phase::Empty).await;
    assert_eq!(ticket.wait().await, Err(Error::ChildLimit));
    let status = owner.handle().status();
    assert_eq!(status.generation, 5);
    assert_eq!(status.child_restart_attempts, 4);
    assert_eq!(status.parent_restart_attempts, 1);
    assert_eq!(status.supervisor_generation, 2);
    owner.finish().await;
}
#[tokio::test]
async fn commands_are_bounded_fenced_and_restart_windows_include_the_tenth_second() {
    let native: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_windows.json"
    ))
    .unwrap();
    fn compare<const I: usize>(value: &serde_json::Value) {
        assert_eq!(value["intensity"], I);
        assert_eq!(value["period"], 10);
        for trace in value["traces"].as_array().unwrap() {
            let mut window = Window::<I, 10>::default();
            for row in trace.as_array().unwrap() {
                assert_eq!(
                    window.record(row["now"].as_u64().unwrap()),
                    row["allowed"].as_bool().unwrap()
                );
            }
        }
    }
    compare::<3>(&native["cases"][0]);
    compare::<5>(&native["cases"][1]);

    let clock = Arc::new(AtomicU64::new(0));
    let mut owner = controlled(Arc::new(AtomicBool::new(false)), clock.clone());
    owner.activate();
    advance(&mut owner, |s| s.phase == Phase::Running).await;
    for time in [0, 0, 10] {
        clock.store(time, Ordering::Release);
        let generation = owner.handle().status().generation;
        let ticket = owner.handle().restart(generation).unwrap();
        assert!(matches!(
            owner.handle().restart(generation),
            Err(Error::Busy)
        ));
        advance(&mut owner, |s| {
            s.generation > generation && s.phase == Phase::Running
        })
        .await;
        assert!(ticket.wait().await.is_ok());
        let stale = owner.handle().restart(generation).unwrap();
        owner.poll().await;
        assert_eq!(stale.wait().await, Err(Error::Stale));
    }
    let ticket = owner
        .handle()
        .restart(owner.handle().status().generation)
        .unwrap();
    owner.poll().await;
    assert_eq!(ticket.wait().await, Err(Error::ChildLimit));
    let stale = owner.handle().start(1).unwrap();
    owner.poll().await;
    assert_eq!(stale.wait().await, Err(Error::Stale));
    owner.finish().await;
    assert!(matches!(owner.handle().start(2), Err(Error::Closed)));
    let clock = Arc::new(AtomicU64::new(0));
    let mut owner = controlled(Arc::new(AtomicBool::new(false)), clock.clone());
    owner.activate();
    advance(&mut owner, |s| s.phase == Phase::Running).await;
    for time in [0, 0, 10, 11] {
        clock.store(time, Ordering::Release);
        let generation = owner.handle().status().generation;
        let ticket = owner.handle().restart(generation).unwrap();
        advance(&mut owner, |s| {
            s.phase == Phase::Running && s.generation > generation
        })
        .await;
        assert!(ticket.wait().await.is_ok());
    }
    assert_eq!(owner.handle().status().parent_restart_attempts, 0);
    owner.finish().await;
}

mod lifecycle;
