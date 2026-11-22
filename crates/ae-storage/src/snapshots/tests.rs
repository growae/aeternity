use super::*;
use ae_protocol::NetworkProfile;
use std::{path::PathBuf, sync::Barrier, time::Duration};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        Self(std::env::temp_dir().join(format!(
            "ae-owned-snapshot-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn database(path: &Temp) -> Arc<Database> {
    Arc::new(
        Database::open(
            &path.0,
            repo::Identity::new(&NetworkProfile::mainnet(), Hash256::ZERO, Hash256::ZERO),
        )
        .unwrap(),
    )
}

#[test]
fn real_snapshot_survives_deletion_corruption_and_external_database_drop() {
    let path = Temp::new();
    let db = database(&path);
    let owner = Owner::start(db.clone(), Default::default()).unwrap();
    let bytes = b"immutable trie node";
    let hash = blake2b_256(bytes);
    let key = repo::key(repo::NODES, hash.0);
    db.db.put(&key, bytes).unwrap();
    let old = owner.capture(|v| Ok(v.status())).unwrap();
    assert_eq!(old.value, old.metadata().status);
    db.db.delete(&key).unwrap();
    let deleted = owner.capture(|_| Ok(())).unwrap();
    db.db.put(&key, b"corrupt replacement").unwrap();
    let corrupt = owner.capture(|_| Ok(())).unwrap();
    assert_eq!(deleted.reader().node(&hash), Ok(None));
    assert_eq!(
        corrupt.reader().node(&hash),
        Err(TrieError::HashMismatch(hash))
    );
    let reader = old.reader().clone();
    let weak = Arc::downgrade(&db);
    drop((old, deleted, corrupt, owner, db));
    assert!(weak.upgrade().is_some());
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let reader = reader.clone();
            thread::spawn(move || {
                for _ in 0..20 {
                    assert_eq!(reader.node(&hash).unwrap().unwrap(), bytes);
                }
            })
        })
        .collect();
    for r in readers {
        r.join().unwrap();
    }
    drop(reader); // Joined, including snapshot destruction and database close.
    assert!(weak.upgrade().is_none());
    let reopened = database(&path);
    assert!(matches!(
        reopened.view().unwrap().source().node(&hash),
        Err(TrieError::HashMismatch(_))
    ));
}

#[test]
fn live_readers_retain_capacity_and_exclusive_owner_until_joined_drop() {
    let path = Temp::new();
    let db = database(&path);
    let owner = Owner::start(
        db.clone(),
        Config {
            maximum_snapshots: 1,
            maximum_node_bytes: 3,
            ..Default::default()
        },
    )
    .unwrap();
    let hash = blake2b_256(b"four");
    db.db.put(repo::key(repo::NODES, hash.0), b"four").unwrap();
    let capture = owner.capture(|_| Ok(())).unwrap();
    assert_eq!(capture.reader().node(&hash), Err(TrieError::ResourceLimit));
    assert_eq!(capture.reader().node(&Hash256::ZERO), Ok(None));
    assert!(matches!(
        owner.capture(|_| Ok(())),
        Err(Error::ResourceLimit)
    ));
    let reader = capture.reader().clone();
    drop(capture);
    assert_eq!(owner.live_snapshots(), 1);
    drop(owner);
    assert!(matches!(
        Owner::start(db.clone(), Default::default()),
        Err(Error::AlreadyRunning)
    ));
    drop(reader);
    let owner = Owner::start(db, Default::default()).unwrap();
    let capture = owner.capture(|_| Ok(())).unwrap();
    assert_eq!(capture.reader().node(&hash).unwrap().unwrap(), b"four");
    drop(capture);
    assert_eq!(owner.live_snapshots(), 0);
}

#[test]
fn initialization_failures_release_capacity_and_failed_feed_requires_reopen() {
    let path = Temp::new();
    let db = database(&path);
    for config in [
        Config {
            maximum_snapshots: 0,
            ..Default::default()
        },
        Config {
            command_capacity: 0,
            ..Default::default()
        },
        Config {
            maximum_node_bytes: 0,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            Owner::start(db.clone(), config),
            Err(Error::Config)
        ));
    }
    let owner = Owner::start(db.clone(), Default::default()).unwrap();
    assert!(matches!(
        owner.capture::<(), _>(|_| Err(repo::Error::ResourceLimit)),
        Err(Error::Repository(repo::Error::ResourceLimit))
    ));
    assert_eq!(owner.live_snapshots(), 0);
    assert!(matches!(
        owner.capture::<(), _>(|_| panic!("injected inspection panic")),
        Err(Error::Worker(_))
    ));
    assert_eq!(owner.live_snapshots(), 0);
    let old = owner.capture(|_| Ok(())).unwrap();
    let cursor = old.metadata().cursor.clone();
    db.events.fail(events::Failure::UncertainWrite);
    assert!(matches!(
        owner.capture(|_| Ok(())),
        Err(Error::Repository(repo::Error::Backend(_)))
    ));
    assert_eq!(owner.live_snapshots(), 1);
    // A known older immutable view stays readable. It does not become a fresh
    // capture or silently attach to the replacement database/event source.
    assert_eq!(old.reader().node(&Hash256::ZERO), Ok(None));
    drop((old, owner, db));
    let db = database(&path);
    assert!(matches!(
        db.events().after(&cursor, 1),
        Err(events::Error::WrongSource)
    ));
    let owner = Owner::start(db, Default::default()).unwrap();
    assert_ne!(owner.capture(|_| Ok(())).unwrap().metadata().cursor, cursor);
}

#[test]
fn capture_releases_writer_lock_and_inspection_and_nodes_share_one_revision() {
    let path = Temp::new();
    let db = database(&path);
    let owner = Owner::start(db.clone(), Default::default()).unwrap();
    let hash = blake2b_256(b"before");
    db.db
        .put(repo::key(repo::NODES, hash.0), b"before")
        .unwrap();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let other = owner.clone();
    let (a, b) = (entered.clone(), release.clone());
    let capture = thread::spawn(move || {
        other
            .capture(move |v| {
                a.wait();
                b.wait();
                assert_eq!(v.source().node(&hash).unwrap().unwrap(), b"before");
                Ok(v.status())
            })
            .unwrap()
    });
    entered.wait();
    let _writer = db
        .writer
        .try_lock()
        .expect("inspection must not retain writer lock");
    db.db.delete(repo::key(repo::NODES, hash.0)).unwrap();
    release.wait();
    let old = capture.join().unwrap();
    assert_eq!(old.metadata().status, old.value);
    assert_eq!(old.metadata().cursor.revision(), old.value.revision);
    assert_eq!(old.reader().node(&hash).unwrap().unwrap(), b"before");
}

#[test]
fn saturated_node_queue_is_retryable_and_abandoned_receipts_do_not_stop_worker() {
    let path = Temp::new();
    let db = database(&path);
    let owner = Owner::start(
        db,
        Config {
            command_capacity: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let capture = owner.capture(|_| Ok(())).unwrap();
    let reader = capture.reader();
    let sender = reader.0.sender.as_ref().unwrap();
    // Hold the worker in its reply send, then fill the one queued slot.
    let (reply, receive) = mpsc::sync_channel(0);
    sender
        .send(Request {
            hash: Hash256::ZERO,
            reply,
        })
        .unwrap();
    let (reply, abandoned) = mpsc::sync_channel(1);
    sender
        .send(Request {
            hash: Hash256::ZERO,
            reply,
        })
        .unwrap();
    assert_eq!(reader.node(&Hash256::ZERO), Err(TrieError::ResourceLimit));
    drop(abandoned);
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(5)).unwrap(),
        Ok(None)
    );
    // Queue ordering makes this blocking test request a drain barrier.
    let (reply, receive) = mpsc::sync_channel(1);
    sender
        .send(Request {
            hash: Hash256::ZERO,
            reply,
        })
        .unwrap();
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(5)).unwrap(),
        Ok(None)
    );
    assert_eq!(reader.node(&Hash256::ZERO), Ok(None));
    drop(capture);
    assert_eq!(owner.live_snapshots(), 0);
}
