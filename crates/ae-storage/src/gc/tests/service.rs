use super::*;
use crate::gc::worker::{Info, Worker};
use std::time::{Duration, Instant};

fn until(worker: &Worker, predicate: impl Fn(&Info) -> bool) -> Info {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut info = worker.info();
    while !predicate(&info) {
        assert!(Instant::now() < deadline, "worker did not settle: {info:?}");
        info = worker.wait_for_change(info.version, Duration::from_millis(100));
    }
    info
}
fn config() -> Config {
    Config {
        enabled: true,
        history: 50,
        ..Default::default()
    }
}
// An already-admitted selected generation, optionally with a micro top, is
// installed atomically. Consensus validity is covered by real replay elsewhere.
fn advance(db: &Database, height: u64, micro: bool) -> Status {
    let guard = db.writer.lock().unwrap();
    let view = db.view_locked(&guard).unwrap();
    let previous = view.status().tip.unwrap();
    let record = view.require_record(previous).unwrap();
    let Header::Key(mut header) = genesis::header(Network::Uat).unwrap() else {
        panic!()
    };
    header.common.height = height;
    header.common.previous_hash = previous.0;
    header.common.previous_key_hash = previous.0;
    header.common.state_hash = record.roots.state.hash().0;
    let block = Block::Key(header.clone());
    let key = block.hash().unwrap();
    let mut batch = WriteBatch::default();
    batch.put(repo::key(repo::BLOCKS, key.0), block.encode().unwrap());
    batch.put(repo::key(repo::RECORDS, key.0), record.encode());
    batch.put(repo::key(repo::CANONICAL, key.0), []);
    batch.put(repo::key(repo::KEYS, height.to_be_bytes()), key.0);
    let top = if micro {
        header.common.previous_hash = key.0;
        header.common.previous_key_hash = key.0;
        let block = Block::Micro(ae_chain::block::MicroBlock {
            header: ae_codec::headers::MicroHeader {
                common: header.common,
                transactions_hash: [0; 32],
                time: 1,
                proof_of_fraud: None,
                signature: [0; 64],
            },
            transactions: vec![],
            proof: None,
        });
        let hash = block.hash().unwrap();
        batch.put(repo::key(repo::BLOCKS, hash.0), block.encode().unwrap());
        batch.put(repo::key(repo::RECORDS, hash.0), record.encode());
        batch.put(repo::key(repo::CANONICAL, hash.0), []);
        hash
    } else {
        key
    };
    let status = Status {
        revision: view.status().revision + 1,
        tip: Some(top),
    };
    batch.put(repo::STATUS, status.encode());
    batch.put(
        repo::key(repo::EVENTS, status.revision.to_be_bytes()),
        Change {
            revision: status.revision,
            inserted: Some(top),
            removed: vec![],
            discarded: vec![],
            old_tip: Some(previous),
            new_tip: top,
            ancestor: Some(previous),
            detached: vec![],
            attached: if micro { vec![key, top] } else { vec![key] },
            requeue: vec![],
            confirm: vec![],
        }
        .encode(),
    );
    db.db.write_opt(batch, &write_options()).unwrap();
    db.wake_gc();
    status
}

#[test]
fn worker_latches_sync_and_collects_coalesced_key_micro_generations() {
    let temp = Temp::new();
    let db = Arc::new(setup(&temp.0).0);
    let worker = Worker::spawn(db.clone(), config(), Limits::default()).unwrap();
    let client = worker.client();
    until(&worker, |i| i.version > 0);
    let status = advance(&db, 53, false);
    until(&worker, |i| i.observed_revision == status.revision);
    assert!(client.on_sync_done(true));
    assert!(!worker.info().synced);
    assert!(client.on_sync_done(false));
    until(&worker, |i| i.synced);
    assert_eq!(db.gc_state().unwrap().epoch, 0);
    let status = advance(&db, 54, true);
    let info = until(&worker, |i| {
        i.observed_revision == status.revision && i.state.phase == Phase::Complete
    });
    assert_eq!(info.state.height, 54);
    assert!(info.pages > 0 && info.state.removed > 0);
    assert!(client.on_sync_done(true));
    assert!(worker.info().synced);
    assert!(worker.set_enabled(false));
    let status = advance(&db, 104, true);
    until(&worker, |i| i.observed_revision == status.revision);
    assert_eq!(db.gc_state().unwrap().height, 54);
    assert!(!worker.set_enabled(true));
    until(&worker, |i| {
        i.state.height == 104 && i.state.phase == Phase::Complete
    });
    for height in [154, 204, 254] {
        assert!(worker.set_enabled(false));
        let status = advance(&db, height, true);
        until(&worker, |i| i.observed_revision == status.revision);
        assert_eq!(db.gc_state().unwrap().height, height - 50);
        assert!(!worker.set_enabled(true));
        until(&worker, |i| {
            i.state.height == height && i.state.phase == Phase::Complete
        });
    }
    assert!(!worker.shutdown().unwrap().running);
    assert!(!client.on_sync_done(false));
    assert_eq!(Arc::strong_count(&db), 1);
    db.view().unwrap().verify_tip().unwrap();
}

#[test]
fn gc_client_is_weak_and_never_redirects_to_a_replacement_worker() {
    let temp = Temp::new();
    let db = Arc::new(setup(&temp.0).0);
    let foreign_temp = Temp::new();
    let foreign = Arc::new(setup(&foreign_temp.0).0);
    let worker = Worker::spawn(db.clone(), config(), Limits::default()).unwrap();
    let client = worker.client();
    assert!(client.uses_database(&db));
    assert!(!client.uses_database(&foreign));
    assert!(client.on_sync_done(false));
    assert!(client.info().unwrap().synced);
    drop(worker);
    assert_eq!(Arc::strong_count(&db), 1);
    assert!(client.info().is_none());
    let worker = Worker::spawn(db.clone(), config(), Limits::default()).unwrap();
    assert!(!client.on_sync_done(false));
    assert!(!worker.info().synced);
    assert!(worker.client().on_sync_done(true));
    assert!(!worker.info().synced);
    assert!(worker.client().on_sync_done(false));
    assert!(worker.info().synced);
    worker.shutdown().unwrap();
}

#[test]
fn first_start_uses_current_height_without_claiming_pruned_history() {
    for enabled in [false, true] {
        let temp = Temp::new();
        let db = Arc::new(setup(&temp.0).0);
        advance(&db, 500, true);
        let worker = Worker::spawn(
            db.clone(),
            Config {
                enabled,
                during_sync: true,
                ..config()
            },
            Limits::default(),
        )
        .unwrap();
        // With no control messages, the first version change follows the
        // startup scheduling attempt. A missing baseline would collect at 500.
        until(&worker, |i| i.version > 0);
        assert_eq!(db.gc_state().unwrap(), State::default());
        let status = advance(&db, 549, false);
        until(&worker, |i| i.observed_revision == status.revision);
        assert_eq!(db.gc_state().unwrap().epoch, 0);
        let status = advance(&db, 550, true);
        if !enabled {
            until(&worker, |i| i.observed_revision == status.revision);
            assert_eq!(db.gc_state().unwrap().epoch, 0);
            assert!(!worker.set_enabled(true));
        }
        until(&worker, |i| {
            i.state.height == 550 && i.state.phase == Phase::Complete
        });
        worker.shutdown().unwrap();
    }
}

#[test]
fn failed_work_retries_once_then_requires_explicit_reenable() {
    let temp = Temp::new();
    let db = Arc::new(setup(&temp.0).0);
    db.begin_gc(Selection::default()).unwrap();
    let pending = db
        .view()
        .unwrap()
        .source()
        .prefix(&[repo::GC_PENDING], 100)
        .unwrap();
    let key = repo::key(repo::NODES, &pending[0].0[1..]);
    let raw = db.db.get(&key).unwrap().unwrap();
    db.db.delete(&key).unwrap();
    let worker = Worker::spawn(
        db.clone(),
        config(),
        Limits {
            rows: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let failed = until(&worker, |i| i.faulted);
    assert_eq!(failed.failures, 2);
    assert!(!failed.enabled && failed.state.active());
    assert!(failed.last_error.unwrap().contains("MissingNode"));
    assert!(!worker.set_enabled(false));
    db.db.put(&key, raw).unwrap();
    db.wake_gc();
    let info = worker.wait_for_change(worker.info().version, Duration::from_millis(10));
    assert!(info.faulted && info.failures == 2);
    assert!(!worker.set_enabled(true));
    let complete = until(&worker, |i| i.state.phase == Phase::Complete);
    assert!(!complete.faulted && complete.failures == 0 && complete.last_error.is_none());
    worker.shutdown().unwrap();
}

#[test]
fn disabled_worker_drains_manual_work_and_shutdown_preserves_durable_resume() {
    let temp = Temp::new();
    let db = Arc::new(setup(&temp.0).0);
    let worker = Worker::spawn(
        db.clone(),
        Config::default(),
        Limits {
            rows: 1,
            ..Default::default()
        },
    )
    .unwrap();
    until(&worker, |i| i.version > 0);
    db.begin_gc(Selection::default()).unwrap();
    worker.shutdown().unwrap();
    assert_eq!(Arc::strong_count(&db), 1);
    let state = db.gc_state().unwrap();
    assert_eq!(state.height, 4);
    assert_ne!(state.phase, Phase::Cancelled);
    drop(db);
    let db = Arc::new(Database::open(&temp.0, identity()).unwrap());
    let worker = Worker::spawn(db.clone(), Config::default(), Limits::default()).unwrap();
    let info = until(&worker, |i| i.state.phase == Phase::Complete);
    assert!(!info.enabled);
    db.begin_gc(Selection::new(&[Tree::Names]).unwrap())
        .unwrap();
    until(&worker, |i| {
        i.state.epoch == 2 && i.state.phase == Phase::Complete
    });
    drop(worker);
    assert_eq!(Arc::strong_count(&db), 1);
    let worker = Worker::spawn(db, Config::default(), Limits::default()).unwrap();
    worker.shutdown().unwrap();
}

#[test]
fn worker_configuration_and_exclusive_ownership_fail_without_mutation() {
    let temp = Temp::new();
    let db = Arc::new(setup(&temp.0).0);
    let before = all(&db);
    assert!(
        Worker::spawn(
            db.clone(),
            Config {
                history: 49,
                ..config()
            },
            Limits::default()
        )
        .is_err()
    );
    assert!(
        Worker::spawn(
            db.clone(),
            config(),
            Limits {
                rows: 0,
                ..Default::default()
            }
        )
        .is_err()
    );
    let worker = Worker::spawn(db.clone(), Config::default(), Limits::default()).unwrap();
    assert!(matches!(
        Worker::spawn(db.clone(), config(), Limits::default()),
        Err(Error::GcActive)
    ));
    worker.shutdown().unwrap();
    assert_eq!(all(&db), before);
}
