use super::*;
use ae_types::Hash256;
use pool::Sink;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
static TEMP: AtomicU64 = AtomicU64::new(1);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "ae-stratum-ingress-{}-{}",
            std::process::id(),
            TEMP.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn identity() -> ae_chain::repository::Identity {
    ae_chain::repository::Identity::new(
        &ae_protocol::NetworkProfile::uat(),
        ae_state::genesis::header(ae_state::genesis::Network::Uat)
            .unwrap()
            .hash()
            .unwrap(),
        Hash256::ZERO,
    )
}
fn open(t: &Temp) -> Arc<Database> {
    Arc::new(Database::open(&t.0, identity()).unwrap())
}
fn effects() -> Vec<share::Effect> {
    vec![share::Effect::SubmitShare {
        account: "ak_worker".into(),
        target: 17.into(),
        hash: Hash256([9; 32]),
    }]
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn until(mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn queue_credit_covers_executing_and_unread_requests_and_graceful_shutdown_drains_once() {
    let t = Temp::new();
    let db = open(&t);
    let (entered, seen) = mpsc::sync_channel(1);
    let (release, wait) = mpsc::sync_channel(1);
    let owner = Accounting::start_with(
        db.clone(),
        Default::default(),
        Default::default(),
        Limits {
            records: 2,
            bytes: 1500,
        },
        move |id| {
            if id.sequence == 1 {
                entered.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        },
    )
    .unwrap();
    let client = owner.client();
    let mut oversized = effects();
    let share::Effect::SubmitShare { account, .. } = &mut oversized[0] else {
        panic!()
    };
    *account = format!("ak_{}", "a".repeat(1000));
    assert!(matches!(client.submit(&oversized), Err(pool::Error::Limit)));
    assert_eq!(client.retained(), (0, 0));
    let first = client.submit(&effects()).unwrap();
    seen.recv_timeout(Duration::from_secs(3)).unwrap();
    let second = client.submit(&effects()).unwrap();
    assert!(matches!(client.submit(&effects()), Err(pool::Error::Busy)));
    assert_eq!(client.retained().0, 2);
    release.send(()).unwrap();
    until(|| {
        db.stratum(Default::default())
            .unwrap()
            .ingress(Default::default())
            .unwrap()
            .sequence()
            == 2
    });
    assert_eq!(client.retained().0, 2);
    assert!(matches!(client.submit(&effects()), Err(pool::Error::Busy)));
    assert_eq!(runtime().block_on(first.completion).unwrap(), first.id);
    assert_eq!(client.retained().0, 1);
    let third = client.submit(&effects()).unwrap();
    assert_eq!(third.id.sequence, 3);
    drop(third);
    let report = owner.shutdown();
    assert!(report.failure.is_none());
    assert!(report.pending.is_empty());
    assert_eq!(report.last.as_ref().unwrap().id.sequence, 3);
    assert_eq!(runtime().block_on(second.completion).unwrap(), second.id);
    assert_eq!(client.retained(), (0, 0));
    assert!(client.is_closed());
    assert!(matches!(
        client.submit(&effects()),
        Err(pool::Error::Closed)
    ));
    assert_eq!(
        db.stratum(Default::default()).unwrap().info().counts,
        [1, 3, 0, 0, 0, 0]
    );
    drop(report);
    let next = Accounting::start(
        db.clone(),
        Default::default(),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(next.client().epoch(), 2);
    let input = next.client().submit(&effects()).unwrap();
    assert_eq!(input.id.sequence, 1);
    assert_eq!(runtime().block_on(input.completion).unwrap(), input.id);
    next.shutdown();
}
#[test]
fn panic_retains_exact_unknown_and_unexecuted_inputs_and_old_clients_cannot_follow_replacement() {
    let t = Temp::new();
    let db = open(&t);
    let (entered, seen) = mpsc::sync_channel(1);
    let (release, wait) = mpsc::sync_channel(1);
    let owner = Accounting::start_with(
        db.clone(),
        Default::default(),
        Default::default(),
        Limits {
            records: 2,
            bytes: 8192,
        },
        move |_| {
            entered.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(10)).unwrap();
            panic!("injected ingress worker panic");
        },
    )
    .unwrap();
    let client = owner.client();
    let first = client.submit(&effects()).unwrap();
    seen.recv_timeout(Duration::from_secs(3)).unwrap();
    let second = client.submit(&effects()).unwrap();
    release.send(()).unwrap();
    assert_eq!(
        runtime().block_on(first.completion),
        Err(pool::Error::Unknown)
    );
    assert_eq!(
        runtime().block_on(second.completion),
        Err(pool::Error::Closed)
    );
    drop(owner);
    let report = client.take_dropped_report().unwrap();
    assert!(matches!(report.failure, Some(Error::Panicked)));
    assert_eq!(report.pending.len(), 2);
    assert!(report.pending[0].unknown);
    assert!(!report.pending[1].unknown);
    assert_eq!(report.pending[0].id, first.id);
    assert_eq!(report.pending[1].id, second.id);
    assert_eq!(
        report.pending[0].input,
        Input::from_effects(&effects()).unwrap()
    );
    assert!(client.take_dropped_report().is_none());
    assert_eq!(client.retained().0, 2);
    assert_eq!(
        db.stratum(Default::default()).unwrap().info().counts,
        [0; 6]
    );
    drop(report);
    assert_eq!(client.retained(), (0, 0));
    let replacement = Accounting::start(
        db,
        Default::default(),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(replacement.client().epoch(), 2);
    assert!(matches!(
        client.submit(&effects()),
        Err(pool::Error::Closed)
    ));
    replacement.shutdown();
}
#[test]
fn durable_rejection_stops_prefix_and_preserves_queued_requests_without_reallocating() {
    let t = Temp::new();
    let db = open(&t);
    let (entered, seen) = mpsc::sync_channel(1);
    let (release, wait) = mpsc::sync_channel(1);
    let owner = Accounting::start_with(
        db.clone(),
        storage::Limits {
            records: 1,
            ..Default::default()
        },
        Default::default(),
        Limits {
            records: 2,
            bytes: 8192,
        },
        move |id| {
            if id.sequence == 1 {
                entered.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        },
    )
    .unwrap();
    let client = owner.client();
    assert!(matches!(client.submit(&[]), Err(pool::Error::Invalid)));
    let first = client.submit(&effects()).unwrap();
    assert_eq!(first.id.sequence, 1);
    seen.recv_timeout(Duration::from_secs(3)).unwrap();
    let second = client.submit(&effects()).unwrap();
    release.send(()).unwrap();
    assert_eq!(
        runtime().block_on(first.completion),
        Err(pool::Error::Rejected)
    );
    assert_eq!(
        runtime().block_on(second.completion),
        Err(pool::Error::Closed)
    );
    let report = owner.shutdown();
    assert_eq!(report.pending.len(), 2);
    assert!(report.pending.iter().all(|p| !p.unknown));
    assert!(report.last.is_none());
    assert_eq!(
        db.stratum(Default::default()).unwrap().info().next_key,
        Some(u64::MAX)
    );
    assert_eq!(
        db.stratum(Default::default())
            .unwrap()
            .ingress(Default::default())
            .unwrap()
            .sequence(),
        0
    );
}
mod tcp;
