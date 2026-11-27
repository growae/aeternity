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
fn shared_factory_preserves_limits_and_reader_owned_generation() {
    let path = Temp::new();
    let db = database(&path);
    assert!(matches!(Owner::current(&db), Err(Error::NotRunning)));
    let owner = Owner::start(
        db.clone(),
        Config {
            maximum_snapshots: 1,
            maximum_node_bytes: 3,
            ..Default::default()
        },
    )
    .unwrap();
    let shared = Owner::current(&db).unwrap();
    let hash = blake2b_256(b"four");
    db.db.put(repo::key(repo::NODES, hash.0), b"four").unwrap();
    let capture = shared.capture(|_| Ok(())).unwrap();
    assert_eq!(owner.live_snapshots(), 1);
    assert!(matches!(
        owner.capture(|_| Ok(())),
        Err(Error::ResourceLimit)
    ));
    drop((owner, shared));
    let retained = Owner::current(&db).unwrap();
    assert_eq!(capture.reader().node(&hash), Err(TrieError::ResourceLimit));
    assert!(matches!(
        Owner::start(db.clone(), Default::default()),
        Err(Error::AlreadyRunning)
    ));
    drop(capture);
    assert_eq!(retained.live_snapshots(), 0);
    drop(retained);
    assert!(matches!(Owner::current(&db), Err(Error::NotRunning)));
    let next = Owner::start(db.clone(), Default::default()).unwrap();
    assert_eq!(
        next.capture(|_| Ok(()))
            .unwrap()
            .reader()
            .node(&hash)
            .unwrap()
            .unwrap(),
        b"four"
    );
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
        Config {
            maximum_read_bytes: 0,
            ..Default::default()
        },
        Config {
            maximum_scan_entries: 0,
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
        .send(Request::Node {
            hash: Hash256::ZERO,
            reply,
        })
        .unwrap();
    let (reply, abandoned) = mpsc::sync_channel(1);
    sender
        .send(Request::Node {
            hash: Hash256::ZERO,
            reply,
        })
        .unwrap();
    assert_eq!(reader.node(&Hash256::ZERO), Err(TrieError::ResourceLimit));
    assert_eq!(
        repo::Read::get(reader, b"queued"),
        Err(repo::Error::ResourceLimit)
    );
    drop(abandoned);
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(5)).unwrap(),
        Ok(None)
    );
    // Queue ordering makes this blocking test request a drain barrier.
    let (reply, receive) = mpsc::sync_channel(1);
    sender
        .send(Request::Node {
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

#[test]
fn owned_repository_reads_and_scans_keep_the_same_revision_and_joined_lease() {
    use repo::Read;
    let path = Temp::new();
    let db = database(&path);
    for (k, v) in [(b"x1", b"one"), (b"x2", b"two"), (b"y1", b"end")] {
        db.db.put(k, v).unwrap();
    }
    let owner = Owner::start(db.clone(), Default::default()).unwrap();
    let old = owner.view().unwrap();
    let reader = old.value.source().clone();
    assert_eq!(reader.source_id(), old.metadata().source_id);
    assert_eq!(reader.resilience(), old.metadata().resilience);
    assert_eq!(old.value.status(), old.metadata().status);
    db.db.delete(b"x1").unwrap();
    db.db.put(b"x2", b"changed").unwrap();
    assert_eq!(reader.get(b"x1").unwrap(), Some(b"one".to_vec()));
    let expected = vec![
        (b"x1".to_vec(), b"one".to_vec()),
        (b"x2".to_vec(), b"two".to_vec()),
    ];
    assert_eq!(reader.prefix(b"x", 2).unwrap(), expected);
    assert_eq!(reader.range(b"x1", b"y", 2).unwrap(), expected);
    assert!(reader.range(b"y", b"x", 0).unwrap().is_empty());
    let mut seen = vec![];
    reader
        .visit_range(b"x", b"y", 2, &mut |k, v| {
            // The callback can issue another read; it runs outside the worker.
            assert_eq!(reader.get(k).unwrap().as_deref(), Some(v));
            seen.push((k.to_vec(), v.to_vec()));
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, expected);
    assert_eq!(
        reader.visit_range(b"x", b"y", 2, &mut |_, _| Err(repo::Error::Conflict)),
        Err(repo::Error::Conflict)
    );
    let weak = Arc::downgrade(&db);
    drop((db, owner, old));
    assert!(weak.upgrade().is_some());
    assert_eq!(reader.get(b"x2").unwrap(), Some(b"two".to_vec()));
    drop(reader);
    assert!(weak.upgrade().is_none());
}

#[test]
fn repository_payload_and_scan_limits_reject_without_truncated_success() {
    use repo::Read;
    let path = Temp::new();
    let db = database(&path);
    db.db.put(b"a1", b"123").unwrap();
    db.db.put(b"a2", b"456").unwrap();
    db.db.put(b"big", b"123456789").unwrap();
    let owner = Owner::start(
        db.clone(),
        Config {
            maximum_read_bytes: 8,
            maximum_scan_entries: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let captured = owner.capture(|_| Ok(())).unwrap();
    let r = captured.reader();
    assert_eq!(r.get(b"big"), Err(repo::Error::ResourceLimit));
    assert_eq!(r.get(b"123456789"), Err(repo::Error::ResourceLimit));
    assert_eq!(r.get(b"a1"), Ok(Some(b"123".to_vec())));
    assert_eq!(r.prefix(b"a", 100), Err(repo::Error::ResourceLimit));
    assert_eq!(r.prefix(b"a1", 0), Err(repo::Error::ResourceLimit));
    assert_eq!(r.range(b"a1", b"a2", 1).unwrap().len(), 1);
    assert_eq!(
        r.range(b"12345", b"67890", 10),
        Err(repo::Error::ResourceLimit)
    );
    assert!(r.prefix(b"absent", 0).unwrap().is_empty());
    drop((captured, owner));
    let owner = Owner::start(
        db,
        Config {
            maximum_read_bytes: 8,
            maximum_scan_entries: 10,
            ..Default::default()
        },
    )
    .unwrap();
    let captured = owner.capture(|_| Ok(())).unwrap();
    assert_eq!(
        captured.reader().prefix(b"a", 100),
        Err(repo::Error::ResourceLimit)
    );
    let mut visits = 0;
    assert_eq!(
        captured.reader().visit_range(b"a", b"b", 10, &mut |_, _| {
            visits += 1;
            Ok(())
        }),
        Err(repo::Error::ResourceLimit)
    );
    assert_eq!(visits, 0);
}
