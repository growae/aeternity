use super::*;
use crate::tests::{Temp, identity, plan};
use ae_chain::resilience::{Config, Event};

#[test]
fn parent_policy_lifetime_and_exclusive_child_preserve_then_reset_activation() {
    let dir = Temp::new();
    let db = Arc::new(Database::open(&dir.0, identity()).unwrap());
    let owner = Owner::new(db.clone()).unwrap();
    let store = owner.store();
    assert!(!db.policy_ready());
    assert!(matches!(db.commit(plan(&db)), Err(Error::PolicyClosed)));
    assert!(matches!(Owner::new(db.clone()), Err(Error::PolicyOwned)));
    assert_eq!(
        db.update_resilience(Event::Start, Default::default()),
        Err(Error::PolicyOwned)
    );
    assert_eq!(db.clear_finalized_height(), Err(Error::PolicyOwned));
    let lease = store.acquire().unwrap();
    assert!(matches!(store.acquire(), Err(Error::PolicyOwned)));
    let initial = Config {
        depth: 3,
        from_start: true,
    };
    lease.update(Event::Start, initial).unwrap();
    assert!(db.policy_ready());
    assert_eq!(lease.snapshot().unwrap().state.recorded_active(), Some(3));
    drop(lease);
    assert!(db.policy_ready());
    let lease = store.acquire().unwrap();
    let state = lease
        .update(
            Event::Start,
            Config {
                depth: 7,
                ..initial
            },
        )
        .unwrap()
        .state;
    assert_eq!(state.configured_depth(), Some(7));
    assert_eq!(state.active_depth(), Some(3));
    lease
        .update(
            Event::Synced,
            Config {
                depth: 0,
                ..initial
            },
        )
        .unwrap();
    drop(lease);
    let lease = store.acquire().unwrap();
    let state = lease
        .update(
            Event::Start,
            Config {
                depth: 9,
                ..initial
            },
        )
        .unwrap()
        .state;
    assert_eq!(state.recorded_active(), Some(0));
    assert!(state.synchronized());
    let before = db.view().unwrap();
    drop(owner);
    assert!(store.is_closed());
    assert!(!db.policy_ready());
    assert_eq!(db.view().unwrap().resilience().state, Default::default());
    assert_eq!(before.resilience().state, state);
    assert_eq!(
        lease.update(Event::Synced, initial),
        Err(Error::PolicyClosed)
    );
    assert!(matches!(Owner::new(db.clone()), Err(Error::PolicyOwned)));
    drop(lease);
    assert!(matches!(store.acquire(), Err(Error::PolicyClosed)));
    assert_eq!(
        db.update_resilience(Event::Start, initial),
        Err(Error::PolicyClosed)
    );
    let fresh = Owner::new(db.clone()).unwrap();
    let lease = fresh.store().acquire().unwrap();
    assert!(!db.policy_ready());
    lease.update(Event::Start, initial).unwrap();
    db.commit(plan(&db)).unwrap();
    drop(lease);
    drop(fresh);
    assert!(!db.policy_ready());
}

#[test]
fn actual_policy_writes_follow_selected_height_and_parent_drop_keeps_durable_finalization() {
    let dir = Temp::new();
    let db = Arc::new(Database::open(&dir.0, identity()).unwrap());
    db.commit(plan(&db)).unwrap();
    // Reuse the storage fixture's assumed-admitted key/micro selections. This
    // exercises real RocksDB writes, not PoW validity for synthesized headers.
    crate::gc::tests::service::advance(&db, 20, true);
    let owner = Owner::new(db.clone()).unwrap();
    let lease = owner.store().acquire().unwrap();
    let cfg = Config {
        depth: 3,
        from_start: false,
    };
    lease.update(Event::Start, cfg).unwrap();
    lease
        .update(Event::ConfigChanged, Config { depth: 6, ..cfg })
        .unwrap();
    assert_eq!(lease.snapshot().unwrap().state.configured_depth(), Some(3));
    assert_eq!(db.view().unwrap().finalized_height().unwrap(), None);
    lease.update(Event::Synced, cfg).unwrap();
    assert_eq!(db.view().unwrap().finalized_height().unwrap(), Some(16));
    lease
        .update(Event::ConfigChanged, Config { depth: 6, ..cfg })
        .unwrap();
    assert_eq!(db.view().unwrap().finalized_height().unwrap(), Some(13));
    crate::gc::tests::service::advance(&db, 4, false);
    lease.update(Event::ConfigChanged, cfg).unwrap();
    assert_eq!(db.view().unwrap().finalized_height().unwrap(), Some(13));
    drop(lease);
    drop(owner);
    assert_eq!(db.view().unwrap().finalized_height().unwrap(), Some(13));
    assert_eq!(db.view().unwrap().resilience().state, Default::default());
    let owner = Owner::new(db.clone()).unwrap();
    let lease = owner.store().acquire().unwrap();
    lease.update(Event::Start, cfg).unwrap();
    lease.clear_finalized_height().unwrap();
    assert_eq!(db.view().unwrap().finalized_height().unwrap(), None);
}

#[test]
fn uncertain_policy_write_and_clear_block_commits_but_keep_durable_reads_and_reopen() {
    for clear in [false, true] {
        for failure in [1, 2, 3] {
            let dir = Temp::new();
            let db = Arc::new(Database::open(&dir.0, identity()).unwrap());
            let stale = plan(&db);
            db.commit(plan(&db)).unwrap();
            crate::gc::tests::service::advance(&db, 20, true);
            let owner = Owner::new(db.clone()).unwrap();
            let lease = owner.store().acquire().unwrap();
            let cfg = Config {
                depth: 3,
                from_start: true,
            };
            lease.update(Event::Start, cfg).unwrap();
            lease.update(Event::Synced, cfg).unwrap();
            let before = db.view().unwrap();
            let cached = before.resilience();
            db.policy_write_failure.store(failure, Ordering::Relaxed);
            let failed = if clear {
                lease.clear_finalized_height()
            } else {
                lease
                    .update(Event::ConfigChanged, Config { depth: 4, ..cfg })
                    .map(|_| ())
            };
            assert!(matches!(failed, Err(Error::Backend(_))));
            assert!(db.policy_recovery_required());
            assert!(db.events().info().closed && db.gc_events().info().closed);
            assert_eq!(lease.snapshot(), Err(Error::RecoveryRequired));
            assert!(matches!(db.commit(stale), Err(Error::RecoveryRequired)));
            assert_eq!(db.cancel_gc(), Err(Error::RecoveryRequired));
            let durable = if failure == 1 {
                Some(16)
            } else if clear {
                None
            } else {
                Some(15)
            };
            let after = db.view().unwrap();
            assert_eq!(after.finalized_height().unwrap(), durable);
            assert_eq!(after.resilience().state, cached.state);
            assert!(after.resilience().generation > cached.generation);
            assert_eq!(before.finalized_height().unwrap(), Some(16));
            assert!(db.peer_view().is_ok() && db.pool_view().is_ok());
            drop(after);
            drop(before);
            drop(lease);
            drop(owner);
            assert!(matches!(
                Owner::new(db.clone()),
                Err(Error::RecoveryRequired)
            ));
            drop(db);
            let reopened = Database::open(&dir.0, identity()).unwrap();
            assert!(!reopened.policy_recovery_required());
            assert!(reopened.policy_ready());
            assert_eq!(
                reopened.view().unwrap().finalized_height().unwrap(),
                durable
            );
        }
    }
}

#[test]
fn a_commit_waiting_on_the_policy_writer_observes_quarantine_before_acquiring_it() {
    use std::{sync::mpsc, time::Duration};
    let dir = Temp::new();
    let db = Arc::new(Database::open(&dir.0, identity()).unwrap());
    let old = plan(&db);
    db.commit(plan(&db)).unwrap();
    crate::gc::tests::service::advance(&db, 20, false);
    let owner = Owner::new(db.clone()).unwrap();
    let lease = owner.store().acquire().unwrap();
    let config = Config {
        depth: 3,
        from_start: true,
    };
    lease.update(Event::Start, config).unwrap();
    lease.update(Event::Synced, config).unwrap();
    db.policy_write_failure.store(2, Ordering::Relaxed);
    let (entered, ready) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    let policy = std::thread::spawn(move || {
        lease.db.update_resilience_owned(
            Event::ConfigChanged,
            Config { depth: 4, ..config },
            Some(&lease.inner),
            || {
                entered.send(()).unwrap();
                resume.recv().unwrap();
                Ok(())
            },
        )
    });
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    let (commit_send, committed) = mpsc::sync_channel(1);
    let pending = db.clone();
    let commit = std::thread::spawn(move || {
        commit_send.send(pending.commit(old)).unwrap();
    });
    assert!(matches!(
        committed.recv_timeout(Duration::from_millis(25)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release.send(()).unwrap();
    assert!(matches!(policy.join().unwrap(), Err(Error::Backend(_))));
    assert!(matches!(
        committed.recv_timeout(Duration::from_secs(5)).unwrap(),
        Err(Error::RecoveryRequired)
    ));
    commit.join().unwrap();
    assert_eq!(db.view().unwrap().finalized_height().unwrap(), Some(15));
}

fn worker_fixture() -> serde_json::Value {
    serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../ae-sync/tests/fixtures/erlang_resilience_worker.json"
    )))
    .unwrap()
}
fn native_state(db: &Database, name: &str) {
    let fixture = worker_fixture();
    let row = fixture["lifecycle"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == name)
        .unwrap();
    let expected = &row["state"];
    let view = db.view().unwrap();
    let state = view.resilience().state;
    assert_eq!(
        state.configured_depth().unwrap_or(0),
        expected["cached"].as_u64().unwrap(),
        "{name}"
    );
    assert_eq!(
        state.recorded_active(),
        expected["active"].as_u64(),
        "{name}"
    );
    assert_eq!(
        state.synchronized(),
        expected["synced"].as_bool().unwrap(),
        "{name}"
    );
    assert_eq!(
        view.finalized_height().unwrap(),
        expected["finalized"].as_u64(),
        "{name}"
    );
}

#[test]
fn original_worker_lifecycle_matches_real_selected_height_policy_writes() {
    let dir = Temp::new();
    let db = Arc::new(Database::open(&dir.0, identity()).unwrap());
    db.commit(plan(&db)).unwrap();
    crate::gc::tests::service::advance(&db, 20, false);
    let owner = Owner::new(db.clone()).unwrap();
    let store = owner.store();
    let lease = store.acquire().unwrap();
    let config = |depth, from_start| Config { depth, from_start };
    lease.update(Event::Start, config(3, false)).unwrap();
    native_state(&db, "start");
    lease
        .update(Event::ConfigChanged, config(7, false))
        .unwrap();
    native_state(&db, "before_sync_ignores_payload");
    lease.update(Event::Synced, config(7, false)).unwrap();
    native_state(&db, "sync_uses_external_config");
    crate::gc::tests::service::advance(&db, 20, true);
    lease
        .update(Event::ConfigChanged, config(3, false))
        .unwrap();
    native_state(&db, "micro_top");
    lease
        .update(Event::ConfigChanged, config(6, false))
        .unwrap();
    native_state(&db, "positive_decrease");
    crate::gc::tests::service::advance(&db, 4, false);
    lease
        .update(Event::ConfigChanged, config(3, false))
        .unwrap();
    native_state(&db, "nonpositive_keeps_height");
    lease.update(Event::ConfigChanged, config(0, true)).unwrap();
    native_state(&db, "recorded_disabled");
    drop(lease);
    native_state(&db, "child_absent_parent_retained");
    let lease = store.acquire().unwrap();
    lease.update(Event::Start, config(9, true)).unwrap();
    native_state(&db, "restart_keeps_disabled");
    // The typed interface has no unrelated-config event to dispatch.
    native_state(&db, "unrelated_config_ignored");
    lease
        .update(Event::ConfigChanged, config(11, false))
        .unwrap();
    native_state(&db, "retained_synced_accepts_config");
    lease.update(Event::Synced, config(11, false)).unwrap();
    native_state(&db, "new_completion_after_restart");
    drop(lease);
    drop(owner);
    assert!(store.is_closed());
    assert_eq!(
        db.view().unwrap().finalized_height().unwrap(),
        worker_fixture()["lifecycle"]["parent_close"]["finalized"].as_u64()
    );
    crate::gc::tests::service::advance(&db, 20, true);
    let owner = Owner::new(db.clone()).unwrap();
    let lease = owner.store().acquire().unwrap();
    lease.update(Event::Start, config(5, false)).unwrap();
    native_state(&db, "new_parent_clears_cache");
    lease.update(Event::Synced, config(5, false)).unwrap();
    native_state(&db, "new_parent_sync");
}

#[test]
fn native_partial_cache_on_write_failure_is_an_explicit_quarantined_boundary() {
    let fixture = worker_fixture();
    for (index, failure) in [1, 2].into_iter().enumerate() {
        let native = &fixture["failures"][index];
        let dir = Temp::new();
        let db = Arc::new(Database::open(&dir.0, identity()).unwrap());
        db.commit(plan(&db)).unwrap();
        crate::gc::tests::service::advance(&db, 20, true);
        let owner = Owner::new(db.clone()).unwrap();
        let store = owner.store();
        let lease = store.acquire().unwrap();
        lease
            .update(
                Event::Start,
                Config {
                    depth: 3,
                    from_start: true,
                },
            )
            .unwrap();
        let before = db.view().unwrap().resilience().state;
        assert_eq!(
            before.configured_depth(),
            native["before"]["cached"].as_u64()
        );
        assert_eq!(
            before.synchronized(),
            native["before"]["synced"].as_bool().unwrap()
        );
        db.policy_write_failure.store(failure, Ordering::Relaxed);
        assert!(matches!(
            lease.update(
                Event::Synced,
                Config {
                    depth: 4,
                    from_start: false
                }
            ),
            Err(Error::Backend(_))
        ));
        let after = db.view().unwrap();
        assert_eq!(
            after.finalized_height().unwrap(),
            native["after_failure"]["finalized"].as_u64()
        );
        assert_eq!(after.resilience().state, before);
        // Native publishes synced/cache before its DB callback. Rust keeps the
        // last complete state and latches recovery instead of restarting into
        // a partly published policy after an uncertain durable outcome.
        assert_eq!(native["after_failure"]["cached"], 4);
        assert_eq!(native["after_failure"]["synced"], true);
        assert_ne!(
            after.resilience().state.synchronized(),
            native["after_failure"]["synced"].as_bool().unwrap()
        );
        assert_eq!(store.snapshot(), Err(Error::RecoveryRequired));
        drop(lease);
        assert!(matches!(store.acquire(), Err(Error::RecoveryRequired)));
    }
}

#[test]
fn empty_cache_parent_replacement_invalidates_plans_and_old_import_scopes() {
    let dir = Temp::new();
    let db = Arc::new(Database::open(&dir.0, identity()).unwrap());
    let standalone = db.policy_scope().unwrap();
    let owner = Owner::new(db.clone()).unwrap();
    assert!(standalone.is_closed());
    let lease = owner.store().acquire().unwrap();
    lease.update(Event::Start, Default::default()).unwrap();
    let scope = db.policy_scope().unwrap();
    let old = plan(&db);
    drop(lease);
    assert!(!scope.is_closed());
    let lease = owner.store().acquire().unwrap();
    lease.update(Event::Start, Default::default()).unwrap();
    assert!(!scope.is_closed());
    drop(lease);
    drop(owner);
    assert!(scope.is_closed());
    let owner = Owner::new(db.clone()).unwrap();
    let lease = owner.store().acquire().unwrap();
    lease.update(Event::Start, Default::default()).unwrap();
    assert!(db.policy_ready());
    assert!(matches!(db.commit(old), Err(Error::Conflict)));
    assert!(matches!(
        db.commit_in_policy_scope(plan(&db), &scope),
        Err(Error::PolicyClosed)
    ));
    assert!(matches!(
        db.commit_in_policy_scope(plan(&db), &standalone),
        Err(Error::PolicyClosed)
    ));
    db.commit_in_policy_scope(plan(&db), &db.policy_scope().unwrap())
        .unwrap();
}
