use super::*;
use crate::tests::{Temp, identity, plan};
use std::sync::atomic::Ordering;

#[test]
fn unacknowledged_writes_close_success_stream_until_reopen() {
    for mode in [1, 2] {
        let dir = Temp::new();
        let db = crate::Database::open(&dir.0, identity()).unwrap();
        let reader = db.events();
        let cursor = reader.cursor();
        db.event_write_failure.store(mode, Ordering::Relaxed);
        assert!(db.commit(plan(&db)).is_err());
        assert_eq!(reader.info().records, 0);
        assert_eq!(reader.info().status.revision, 0);
        assert_eq!(
            reader.after(&cursor, 1).unwrap_err(),
            Error::Failed(Failure::UncertainWrite)
        );
        assert!(db.event_snapshot().is_err());
        assert!(db.gc_event_snapshot().is_err());
        assert_eq!(
            db.gc_events().info().failure,
            Some(crate::gc::events::Failure::UncertainWrite)
        );
        let revision = u64::from(mode == 2);
        assert_eq!(db.view().unwrap().status().revision, revision);
        drop(db);
        let reopened = crate::Database::open(&dir.0, identity()).unwrap();
        assert_eq!(reopened.events().cursor().revision(), revision);
        assert_eq!(reopened.events().info().records, 0);
        assert_eq!(
            reopened.events().after(&cursor, 1).unwrap_err(),
            Error::WrongSource
        );
        assert!(!reopened.events().info().closed);
    }
}

#[test]
fn rejected_commit_and_competing_writers_never_publish_false_success() {
    let dir = Temp::new();
    let db = crate::Database::open(&dir.0, identity()).unwrap();
    let reader = db.events();
    let cursor = reader.cursor();
    assert!(
        db.commit_checked(plan(&db), || Err(crate::Error::Conflict))
            .is_err()
    );
    assert!(reader.after(&cursor, 1).unwrap().records.is_empty());
    assert!(!reader.info().closed);
    let a = plan(&db);
    let b = plan(&db);
    std::thread::scope(|s| {
        let first = s.spawn(|| db.commit(a));
        let second = s.spawn(|| db.commit(b));
        let results = [first.join().unwrap(), second.join().unwrap()];
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert!(
            results
                .iter()
                .any(|r| matches!(r, Err(crate::Error::Conflict)))
        );
    });
    let observed = reader.after(&cursor, 2).unwrap();
    assert_eq!(observed.records.len(), 1);
    assert_eq!(observed.cursor.revision(), 1);
    assert!(observed.records[0].effects().next().is_none());
    let mut ahead = observed.cursor.clone();
    ahead.revision += 1;
    assert_eq!(reader.after(&ahead, 1).unwrap_err(), Error::Ahead);
    assert_eq!(reader.after(&cursor, 0).unwrap_err(), Error::Limit);
    assert_eq!(
        reader.wait(&cursor, 1, Duration::MAX).unwrap_err(),
        Error::Limit
    );
}

#[test]
fn waiters_wake_after_commit_or_database_close_without_owning_database() {
    let dir = Temp::new();
    let db = crate::Database::open(&dir.0, identity()).unwrap();
    let reader = db.events();
    let cursor = reader.cursor();
    std::thread::scope(|s| {
        let waiting = s.spawn(|| reader.wait(&cursor, 1, Duration::from_secs(30)).unwrap());
        db.commit(plan(&db)).unwrap();
        let batch = waiting.join().unwrap();
        assert_eq!(batch.cursor.revision(), 1);
        assert!(!batch.closed);
    });
    let cursor = reader.cursor();
    std::thread::scope(|s| {
        let waiting = s.spawn(|| reader.wait(&cursor, 1, Duration::from_secs(30)).unwrap());
        drop(db);
        let batch = waiting.join().unwrap();
        assert!(batch.closed && batch.records.is_empty());
    });
    crate::Database::open(&dir.0, identity()).unwrap();
}

#[test]
fn persisted_revision_binding_preserves_gap_and_foreign_cursor_checks() {
    let t = Temp::new();
    let db = crate::Database::open(&t.0, identity()).unwrap();
    let old = db.events();
    let before = old.cursor();
    db.commit(plan(&db)).unwrap();
    assert_eq!(old.at_revision(0).unwrap(), before);
    assert_eq!(old.at_revision(2), Err(Error::Ahead));
    drop(db);
    let db = crate::Database::open(&t.0, identity()).unwrap();
    let new = db.events();
    let restored = new.at_revision(0).unwrap();
    assert_ne!(restored, before);
    assert!(matches!(new.after(&restored, 1), Err(Error::Gap { .. })));
    assert_eq!(new.after(&before, 1).unwrap_err(), Error::WrongSource);
    assert_eq!(new.at_revision(1).unwrap(), new.cursor());
}
