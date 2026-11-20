use super::*;
use ae_p2p::registry::{Command, Effect, Info, Registry};
use ae_types::PeerPublicKey;
use std::{
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};
use tokio::sync::oneshot;

fn token() -> Token {
    let mut registry = Registry::new(PeerPublicKey([128; 32]), Default::default()).unwrap();
    let prepared = registry
        .prepare(
            0,
            Command::Resolve {
                peer: Info {
                    id: PeerPublicKey([1; 32]),
                    host: b"localhost".to_vec(),
                    port: 3015,
                },
                source: None,
                trusted: false,
            },
        )
        .unwrap();
    registry
        .commit(prepared)
        .unwrap()
        .effects
        .into_iter()
        .find_map(|e| match e {
            Effect::Resolve { token, .. } => Some(token),
            _ => None,
        })
        .unwrap()
}
async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("bounded resolver test")
}
fn settings() -> Settings {
    Settings {
        threads: 1,
        jobs: 2,
        bytes: 264,
        maximum_host_bytes: 32,
    }
}
fn gate() -> (
    Arc<Lookup>,
    oneshot::Receiver<()>,
    mpsc::Sender<()>,
    Arc<AtomicUsize>,
) {
    let (started, observed) = oneshot::channel();
    let started = Mutex::new(Some(started));
    let (release, wait) = mpsc::channel();
    let wait = Mutex::new(wait);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let lookup = Arc::new(move |host: &str| {
        count.fetch_add(1, Ordering::SeqCst);
        if host == "slow" {
            started.lock().unwrap().take().unwrap().send(()).unwrap();
            wait.lock().unwrap().recv().unwrap();
        }
        Ok(Ipv4Addr::LOCALHOST)
    });
    (lookup, observed, release, calls)
}

#[tokio::test]
async fn system_resolver_matches_original_numeric_startup_results_and_resolves_localhost() {
    let resolver = Resolver::start(Default::default()).unwrap();
    let data: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_peer_startup.json"
    )))
    .unwrap();
    let mut total = 0;
    for scenario in data["scenarios"].as_array().unwrap() {
        for peer in scenario["adds"].as_array().unwrap() {
            let token = token();
            let pending = resolver
                .submit(token, hex::decode(peer["host"].as_str().unwrap()).unwrap())
                .unwrap();
            assert_eq!(pending.token(), token);
            let expected = Ipv4Addr::from(std::array::from_fn::<_, 4, _>(|i| {
                u8::try_from(peer["address"][i].as_u64().unwrap()).unwrap()
            }));
            assert_eq!(
                within(pending).await,
                Completion {
                    token,
                    result: Ok(expected)
                }
            );
            total += 1;
        }
    }
    assert_eq!(total, 26);
    assert!(
        within(resolver.submit(token(), b"localhost".to_vec()).unwrap())
            .await
            .result
            .unwrap()
            .is_loopback()
    );
    resolver.shutdown().await;
    eprintln!("System resolver: 26 original numeric startup results and localhost lookup");
}

#[tokio::test]
async fn cancelled_and_unread_resolutions_retain_actual_job_and_byte_capacity() {
    let (lookup, started, release, calls) = gate();
    let resolver = Resolver::with_lookup(settings(), lookup).unwrap();
    let first = resolver.submit(token(), b"slow".to_vec()).unwrap();
    within(started).await.unwrap();
    let queued = resolver.submit(token(), b"skip".to_vec()).unwrap();
    drop(queued);
    drop(first);
    assert_eq!(
        resolver.capacity(),
        Capacity {
            available_jobs: 0,
            available_bytes: 0,
            stopped: false
        }
    );
    assert!(matches!(
        resolver.submit(token(), b"next".to_vec()),
        Err(Error::Busy)
    ));
    release.send(()).unwrap();
    within(async {
        while resolver.capacity().available_jobs != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let unread = resolver.submit(token(), b"fast".to_vec()).unwrap();
    within(async {
        while calls.load(Ordering::SeqCst) != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert_eq!(
        resolver.capacity(),
        Capacity {
            available_jobs: 1,
            available_bytes: 132,
            stopped: false
        }
    );
    assert!(matches!(
        resolver.submit(token(), b"extra".to_vec()),
        Err(Error::Busy)
    ));
    assert_eq!(within(unread).await.result, Ok(Ipv4Addr::LOCALHOST));
    assert_eq!(resolver.capacity().available_bytes, 264);
    resolver.shutdown().await;
}

#[tokio::test]
async fn stopping_cancels_queued_lookups_and_join_waits_for_the_running_os_boundary() {
    let (lookup, started, release, calls) = gate();
    let resolver = Resolver::with_lookup(settings(), lookup).unwrap();
    let running = resolver.submit(token(), b"slow".to_vec()).unwrap();
    within(started).await.unwrap();
    let queued = resolver.submit(token(), b"skip".to_vec()).unwrap();
    resolver.stop();
    assert_eq!(within(queued).await.result, Err(Error::Stopped));
    assert!(matches!(
        resolver.submit(token(), b"next".to_vec()),
        Err(Error::Stopped)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(1), resolver.join())
            .await
            .is_err()
    );
    assert_eq!(
        resolver.capacity(),
        Capacity {
            available_jobs: 1,
            available_bytes: 132,
            stopped: true
        }
    );
    release.send(()).unwrap();
    within(resolver.join()).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(within(running).await.result, Ok(Ipv4Addr::LOCALHOST));
    assert_eq!(resolver.capacity().available_jobs, 2);
}

#[tokio::test]
async fn abrupt_resolver_drop_joins_workers_and_lookup_panics_are_contained() {
    let resolver = Resolver::with_lookup(
        settings(),
        Arc::new(|host| {
            if host == "oops" {
                panic!("injected name resolver panic");
            }
            Err(Error::Lookup(io::ErrorKind::NotFound))
        }),
    )
    .unwrap();
    assert_eq!(
        within(resolver.submit(token(), b"oops".to_vec()).unwrap())
            .await
            .result,
        Err(Error::Panicked)
    );
    assert_eq!(
        within(resolver.submit(token(), b"next".to_vec()).unwrap())
            .await
            .result,
        Err(Error::Lookup(io::ErrorKind::NotFound))
    );
    resolver.shutdown().await;
    let (lookup, started, release, calls) = gate();
    let done = Arc::new(AtomicBool::new(false));
    let completed = done.clone();
    let resolver = Resolver::with_lookup(
        settings(),
        Arc::new(move |host| {
            let result = lookup(host);
            completed.store(true, Ordering::SeqCst);
            result
        }),
    )
    .unwrap();
    let pending = resolver.submit(token(), b"slow".to_vec()).unwrap();
    within(started).await.unwrap();
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        release.send(()).unwrap();
    });
    drop(resolver);
    assert!(done.load(Ordering::SeqCst));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(within(pending).await.result, Ok(Ipv4Addr::LOCALHOST));
    releaser.join().unwrap();
}

#[tokio::test]
async fn resolver_validation_precedes_admission_and_preserves_system_hostname_spelling() {
    for invalid in [
        Settings {
            threads: 0,
            ..settings()
        },
        Settings {
            threads: 33,
            ..settings()
        },
        Settings {
            jobs: 65537,
            ..settings()
        },
        Settings {
            bytes: 0,
            ..settings()
        },
        Settings {
            maximum_host_bytes: 65537,
            ..settings()
        },
    ] {
        assert!(matches!(
            Resolver::start(invalid),
            Err(Error::Configuration)
        ));
    }
    let seen = Arc::new(Mutex::new(vec![]));
    let captured = seen.clone();
    let resolver = Resolver::with_lookup(
        settings(),
        Arc::new(move |host| {
            captured.lock().unwrap().push(host.to_string());
            Err(Error::NoIpv4Address)
        }),
    )
    .unwrap();
    for invalid in [
        b"".as_slice(),
        b"bad\0name",
        b"white space",
        b"bad\xffhost",
        b"::1",
        b"[127.0.0.1]",
    ] {
        assert!(matches!(
            resolver.submit(token(), invalid.to_vec()),
            Err(Error::InvalidHost)
        ));
    }
    assert!(matches!(
        resolver.submit(token(), vec![b'x'; 33]),
        Err(Error::ResourceLimit)
    ));
    assert!(seen.lock().unwrap().is_empty());
    let hosts = [
        "MiXeD%20host",
        "127.1",
        "0177.0.0.1",
        "0x7f000001",
        "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    ];
    for host in hosts {
        assert_eq!(
            within(resolver.submit(token(), host.as_bytes().to_vec()).unwrap())
                .await
                .result,
            Err(Error::NoIpv4Address)
        );
    }
    assert_eq!(*seen.lock().unwrap(), hosts);
    assert_eq!(resolver.capacity().available_jobs, 2);
    resolver.shutdown().await;
}
