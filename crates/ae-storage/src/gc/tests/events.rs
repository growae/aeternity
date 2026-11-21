use super::*;
use crate::gc::events::{self, Kind};
use std::time::Duration;
fn finish(db: &Database) {
    while db.gc_state().unwrap().active() {
        db.gc_step(Limits {
            rows: 1,
            ..Default::default()
        })
        .unwrap();
    }
}
#[test]
fn durable_gc_switch_scan_and_deletion_milestones_have_distinct_events() {
    let temp = Temp::new();
    let (db, _) = setup(&temp.0);
    let reader = db.gc_events();
    let (pinned, cursor) = db.gc_event_snapshot().unwrap();
    assert_eq!(reader.info().state, State::default());
    assert!(db.begin_gc(Selection::default()).unwrap());
    let first = reader.after(&cursor, 10).unwrap();
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.records[0].kind, Kind::Switch);
    assert_eq!(first.records[0].state, db.gc_state().unwrap());
    assert_eq!(first.records[0].chain, db.view().unwrap().status());
    assert_eq!(pinned.retention().unwrap(), State::default());
    assert!(db.begin_gc(Selection::default()).is_err());
    loop {
        let state = db
            .gc_step(Limits {
                rows: 1,
                ..Default::default()
            })
            .unwrap();
        let progress = reader.after(&first.cursor, 10).unwrap();
        if !progress.records.is_empty() {
            assert_eq!(progress.records.len(), 1);
            assert_eq!(progress.records[0].kind, Kind::ScansComplete);
            assert_eq!(state.phase, Phase::Sweep);
            assert_eq!(state.pending, 0);
            assert_eq!(state.removed, 0);
            break;
        }
    }
    finish(&db);
    let history = reader.after(&cursor, 10).unwrap();
    assert_eq!(
        history.records.iter().map(|r| r.kind).collect::<Vec<_>>(),
        vec![Kind::Switch, Kind::ScansComplete, Kind::CollectionComplete]
    );
    assert!(history.records[2].state.removed > 0);
    assert_eq!(history.cursor.sequence(), 3);
    db.gc_step(Limits::default()).unwrap();
    db.cancel_gc().unwrap();
    assert!(
        reader
            .after(&history.cursor, 10)
            .unwrap()
            .records
            .is_empty()
    );
    drop(pinned);
    drop(db);
    assert!(reader.after(&cursor, 10).unwrap().closed);
    let reopened = Database::open(&temp.0, identity()).unwrap();
    assert_eq!(reopened.gc_events().info().state.phase, Phase::Complete);
    assert_eq!(reopened.gc_events().info().records, 0);
    assert_eq!(
        reopened.gc_events().after(&cursor, 1).unwrap_err(),
        events::Error::WrongSource
    );
}
#[test]
fn gc_cancel_restart_and_small_histories_report_gaps_without_losing_durable_progress() {
    for bytes in [1, 1 << 20] {
        let temp = Temp::new();
        drop(setup(&temp.0));
        let db = Database::open_with_notifications(
            &temp.0,
            identity(),
            Default::default(),
            Default::default(),
            Notifications {
                gc: events::Config { records: 1, bytes },
                ..Default::default()
            },
        )
        .unwrap();
        let reader = db.gc_events();
        let cursor = reader.cursor();
        db.begin_gc(Selection::default()).unwrap();
        db.cancel_gc().unwrap();
        db.begin_gc(Selection::default()).unwrap();
        finish(&db);
        let events::Error::Gap { missed, resume } = reader.after(&cursor, 10).unwrap_err() else {
            panic!()
        };
        assert_eq!(missed, if bytes == 1 { 5 } else { 4 });
        let batch = reader.after(&resume, 10).unwrap();
        assert_eq!(batch.records.len(), usize::from(bytes != 1));
        assert_eq!(batch.cursor.sequence(), 5);
        assert_eq!(reader.info().state.epoch, 2);
        assert_eq!(reader.info().state.phase, Phase::Complete);
        assert!(reader.info().bytes <= bytes);
        assert_eq!(db.gc_event_snapshot().unwrap().1, batch.cursor);
    }
}
#[test]
fn gc_write_and_acknowledgment_failures_close_the_stream_without_false_success() {
    for operation in ["begin", "step", "cancel"] {
        for failure in [1, 2] {
            let temp = Temp::new();
            let (db, _) = setup(&temp.0);
            if operation != "begin" {
                db.begin_gc(Selection::default()).unwrap();
            }
            let reader = db.gc_events();
            let cursor = reader.cursor();
            let before = db.gc_state().unwrap();
            db.gc_write_failure.store(failure, Ordering::Relaxed);
            let result = match operation {
                "begin" => db.begin_gc(Selection::default()).map(|_| ()),
                "step" => db
                    .gc_step(Limits {
                        rows: 1,
                        ..Default::default()
                    })
                    .map(|_| ()),
                "cancel" => db.cancel_gc().map(|_| ()),
                _ => unreachable!(),
            };
            assert!(result.is_err());
            assert_eq!(reader.cursor(), cursor);
            assert_eq!(
                reader.after(&cursor, 1).unwrap_err(),
                events::Error::Failed(events::Failure::UncertainWrite)
            );
            assert!(db.gc_event_snapshot().is_err());
            let after = db.gc_state().unwrap();
            assert_eq!(after == before, failure == 1);
            drop(db);
            let reopened = Database::open(&temp.0, identity()).unwrap();
            assert_eq!(reopened.gc_events().info().state, after);
            assert!(!reopened.gc_events().info().closed);
            assert_eq!(reopened.gc_events().info().sequence, 0);
        }
    }
}
#[test]
fn rejected_gc_work_and_wait_cancellation_preserve_notification_cursors() {
    let temp = Temp::new();
    let (db, _) = setup(&temp.0);
    let reader = db.gc_events();
    let cursor = reader.cursor();
    assert!(
        db.begin_gc_checked(Selection::default(), None, || Err(Error::Conflict))
            .is_err()
    );
    assert_eq!(reader.cursor(), cursor);
    assert!(!reader.info().closed);
    assert!(
        reader
            .wait(&cursor, 1, Duration::ZERO)
            .unwrap()
            .records
            .is_empty()
    );
    std::thread::scope(|scope| {
        let wait = scope.spawn(|| reader.wait(&cursor, 1, Duration::from_secs(30)).unwrap());
        db.begin_gc(Selection::default()).unwrap();
        assert_eq!(wait.join().unwrap().records[0].kind, Kind::Switch);
    });
    let cursor = reader.cursor();
    assert!(
        db.gc_step_checked(Limits::default(), || Err(Error::Conflict))
            .is_err()
    );
    assert_eq!(reader.cursor(), cursor);
    assert_eq!(reader.info().state, db.gc_state().unwrap());
    std::thread::scope(|scope| {
        let wait = scope.spawn(|| reader.wait(&cursor, 1, Duration::from_secs(30)).unwrap());
        drop(db);
        assert!(wait.join().unwrap().closed);
    });
}

#[test]
fn native_gc_callback_publications_match_acknowledged_storage_boundaries() {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_gc_events.json"
    ))
    .unwrap();
    assert_eq!(corpus["cases"].as_array().unwrap().len(), 22);
    for row in corpus["cases"].as_array().unwrap() {
        let count = row["count"].as_u64().unwrap_or(3) as usize;
        let mut trees = vec![Tree::Accounts, Tree::Contracts, Tree::Channels];
        trees.truncate(count);
        if row["reverse"] == true {
            trees.reverse();
        }
        let selection = Selection::new(&trees).unwrap();
        let scan = row["kind"] == "scan";
        let steps = if scan {
            let baseline = Temp::new();
            let (db, _) = setup(&baseline.0);
            service::advance(&db, 50, false);
            db.begin_gc(selection).unwrap();
            let mut steps = 0;
            loop {
                steps += 1;
                let state = db
                    .gc_step(Limits {
                        rows: 1,
                        ..Default::default()
                    })
                    .unwrap();
                if state.phase == Phase::Sweep && state.pending == 0 {
                    break steps;
                }
            }
        } else {
            0
        };
        let temp = Temp::new();
        let (db, _) = setup(&temp.0);
        service::advance(&db, 50, false);
        if scan {
            db.begin_gc(selection).unwrap();
            for _ in 1..steps {
                db.gc_step(Limits {
                    rows: 1,
                    ..Default::default()
                })
                .unwrap();
            }
        }
        let reader = db.gc_events();
        let cursor = reader.cursor();
        let fault = row["fault"].as_str().unwrap();
        db.gc_write_failure.store(
            match fault {
                "write_failure" => 1,
                "ack_failure" => 2,
                _ => 0,
            },
            Ordering::Relaxed,
        );
        let result = if scan {
            db.gc_step(Limits {
                rows: 1,
                ..Default::default()
            })
            .map(|_| ())
        } else {
            db.begin_gc_checked(selection, None, || {
                if fault == "transaction_failure" {
                    Err(Error::Conflict)
                } else {
                    Ok(())
                }
            })
            .map(|_| ())
        };
        assert_eq!(result.is_ok(), row["result"] == "ok", "{row}");
        let actual = if result.is_ok() {
            reader
                .after(&cursor, 10)
                .unwrap()
                .records
                .iter()
                .map(|record| match record.kind {
                    Kind::Switch => {
                        serde_json::json!({"event":"gc_switch","height":record.state.height})
                    }
                    Kind::ScansComplete => serde_json::json!({"event":"scans_complete"}),
                    _ => panic!(),
                })
                .collect::<Vec<_>>()
        } else {
            assert_eq!(reader.cursor(), cursor);
            vec![]
        };
        assert_eq!(serde_json::json!(actual), row["events"], "{row}");
        let durable = db.gc_state().unwrap();
        assert_eq!(
            if scan {
                durable.phase == Phase::Sweep && durable.pending == 0
            } else {
                durable.epoch == 1
            },
            matches!(fault, "none" | "ack_failure")
        );
    }
}
