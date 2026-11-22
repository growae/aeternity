use super::*;

fn pending() -> Arc<Signed> {
    // A native signed transaction with TTL 5, force-admitted to isolate pool
    // lifecycle from its custom network signature and account balance.
    let data: J = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_durable_pool.json"
    ))
    .unwrap();
    Arc::new(
        ae_tx::storage::decode(
            &hex::decode(data["rows"][0]["txs"][1].as_str().unwrap()).unwrap(),
            Default::default(),
        )
        .unwrap(),
    )
}

#[test]
fn real_key_events_expire_membership_and_sync_target_defers_collection() {
    for syncing in [false, true] {
        let temp = Temp::new();
        let db = database(&temp);
        let t = pending();
        let h = t.hash();
        let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
        service.force_push(t.clone()).unwrap();
        assert_eq!(service.inspect(h).unwrap().expires, 5);
        if syncing {
            service.sync_target(100).unwrap();
        }
        for block in blocks() {
            db.commit(prepare(&db, &block)).unwrap();
            let height = block.header().common().height;
            let before = db.pool_view().unwrap().location(h).unwrap();
            service.refresh().unwrap();
            let after = db.pool_view().unwrap().location(h).unwrap();
            if syncing || height < 5 {
                assert_eq!(after, Location::Pool);
            } else {
                assert_eq!(after, Location::Deleted);
                if height == 5 {
                    assert_eq!(before, Location::Pool);
                }
            }
            assert_eq!(
                db.pool_view().unwrap().transaction(h).unwrap().as_ref(),
                Some(t.as_ref())
            );
        }
        if syncing {
            assert!(service.collect(false, usize::MAX).unwrap().is_empty());
            drop(service);
            let mut service =
                Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
            assert_eq!(service.refresh().unwrap().gc_height, 6);
            assert_eq!(service.collect(false, 1).unwrap(), vec![h]);
            assert_eq!(
                db.pool_view().unwrap().location(h).unwrap(),
                Location::Deleted
            );
        }
    }
}

#[test]
fn candidate_rejects_concurrent_chain_commit_without_publishing_visited_state() {
    let temp = Temp::new();
    let db = database(&temp);
    let t = tx();
    let h = t.hash();
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    service.push(t.clone(), Event::Created, false).unwrap();
    let before = service.inspect(h).unwrap();
    let mut update = Some(prepare(&db, &blocks()[0]));
    let result = service.candidate(
        1000000,
        &BTreeSet::new(),
        ae_mempool::pool::SelectOptions {
            ceiling: Some(100),
            ..Default::default()
        },
        || {
            if let Some(update) = update.take() {
                db.commit(update).unwrap();
            }
            0
        },
    );
    assert!(update.is_none());
    assert!(matches!(result, Err(Failure::Repository(Error::Conflict))));
    assert_eq!(service.inspect(h).unwrap(), before);
    let candidate = service
        .candidate(1000000, &BTreeSet::new(), Default::default(), || 0)
        .unwrap();
    assert_eq!(Some(candidate.block), db.view().unwrap().status().tip);
    assert_eq!(candidate.transactions, vec![t]);
    assert!(service.inspect(h).unwrap().visited);
}

#[test]
fn delayed_rollback_rebuilds_without_requeueing_deleted_canonical_transactions() {
    let temp = Temp::new();
    let db = database(&temp);
    let accepted = tx();
    let local = pending();
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    service
        .push(accepted.clone(), Event::Created, false)
        .unwrap();
    service.force_push(local.clone()).unwrap();
    for block in blocks() {
        db.commit(prepare(&db, &block)).unwrap();
    }
    db.rollback(repo::rollback::Target::Height(0), Default::default())
        .unwrap();
    assert_eq!(
        db.pool_view().unwrap().location(accepted.hash()).unwrap(),
        Location::Unknown
    );
    assert_eq!(service.peek(100, None, None).unwrap(), vec![local.clone()]);
    assert_eq!(service.inspect(local.hash()).unwrap().expires, 5);
    assert_eq!(
        service.refresh().unwrap().chain,
        db.view().unwrap().status()
    );
    drop(service);
    drop(db);
    let db = database(&temp);
    let mut service = Service::start(db, NetworkProfile::mainnet(), config()).unwrap();
    assert_eq!(service.peek(100, None, None).unwrap(), vec![local]);
}

#[test]
fn resource_limits_leave_durable_membership_and_owner_recoverable() {
    let temp = Temp::new();
    let db = database(&temp);
    assert!(matches!(
        Service::start(db.clone(), NetworkProfile::uat(), config()),
        Err(Failure::Repository(Error::Identity))
    ));
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    let t = tx();
    service.push(t.clone(), Event::Created, false).unwrap();
    drop(service);
    for mode in 0..3 {
        let mut cfg = config();
        match mode {
            0 => cfg.limits.entries = 0,
            1 => cfg.limits.retained_record_bytes = 1,
            _ => cfg.limits.record_bytes = 1,
        }
        let err = Service::start(db.clone(), NetworkProfile::mainnet(), cfg)
            .err()
            .unwrap();
        assert!(err.is_operational(), "{err:?}");
        assert_eq!(db.pool_view().unwrap().members(1).unwrap(), vec![t.hash()]);
    }
    let mut cfg = config();
    cfg.events = 0;
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), cfg).unwrap();
    db.commit(prepare(&db, &blocks()[0])).unwrap();
    assert_eq!(
        service.refresh(),
        Err(Failure::Repository(Error::ResourceLimit))
    );
    service.recover(config()).unwrap();
    assert_eq!(service.peek(1, None, None).unwrap(), vec![t.clone()]);
    let mut cfg = config();
    cfg.commit_bytes = 0;
    service.recover(cfg).unwrap();
    assert_eq!(
        service.delete(t.hash()),
        Err(Failure::Repository(Error::ResourceLimit))
    );
    assert_eq!(service.peek(1, None, None).unwrap(), vec![t]);
}

#[test]
fn conflicting_metering_metadata_never_replaces_existing_pool_or_chain_records() {
    let temp = Temp::new();
    let db = database(&temp);
    let original = tx();
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    service
        .push(original.clone(), Event::Created, false)
        .unwrap();
    let bytes = ae_tx::storage::encode(&original, Default::default()).unwrap();
    let Value::List(mut fields) = Value::decode(&bytes[ae_tx::storage::MAGIC.len()..]).unwrap()
    else {
        panic!()
    };
    fields[1] = Value::Bytes(vec![0]);
    let mut altered = ae_tx::storage::MAGIC.to_vec();
    altered.extend(Value::List(fields).encode());
    let altered = Arc::new(ae_tx::storage::decode(&altered, Default::default()).unwrap());
    assert_eq!(altered.hash(), original.hash());
    assert_eq!(altered.encode(), original.encode());
    assert_ne!(altered, original);
    for canonical in [false, true] {
        if canonical {
            for b in blocks().iter().take(2) {
                db.commit(prepare(&db, b)).unwrap();
            }
        }
        let before = service.refresh().unwrap();
        assert_eq!(
            service.force_push(altered.clone()),
            Err(Failure::Repository(Error::TransactionMetadataConflict))
        );
        assert_eq!(service.refresh().unwrap(), before);
        assert_eq!(
            db.pool_view()
                .unwrap()
                .transaction(original.hash())
                .unwrap()
                .as_ref(),
            Some(original.as_ref())
        );
    }
}

#[test]
fn malformed_members_and_records_are_reported_without_dropping_membership() {
    for mode in 0..6 {
        let temp = Temp::new();
        let db = database(&temp);
        let t = tx();
        let h = t.hash();
        let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
        service.push(t, Event::Created, false).unwrap();
        drop(service);
        match mode {
            0 => db.db.put(repo::key(MEMBERS, h.0), [2]).unwrap(),
            1 => db.db.delete(repo::key(RECORDS, h.0)).unwrap(),
            2 => db.db.put(repo::key(RECORDS, h.0), [0]).unwrap(),
            3 => db
                .db
                .put(
                    repo::key(RECORDS, h.0),
                    ae_tx::storage::encode(&pending(), Default::default()).unwrap(),
                )
                .unwrap(),
            4 => db.db.put([MEMBERS, 1], MEMBER).unwrap(),
            _ => db
                .db
                .put(repo::key(repo::LOCATIONS, h.0), identity().genesis.0)
                .unwrap(),
        }
        assert!(
            Service::start(db.clone(), NetworkProfile::mainnet(), config()).is_err(),
            "mode {mode}"
        );
        assert!(db.db.get(repo::key(MEMBERS, h.0)).unwrap().is_some());
    }
    for bytes in [
        None,
        Some(vec![1]),
        Some(encode_revision(0)),
        Some(encode_revision(u64::MAX)),
    ] {
        let temp = Temp::new();
        let db = database(&temp);
        let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
        service.force_push(tx()).unwrap();
        drop(service);
        match &bytes {
            Some(bytes) => db.db.put(REVISION, bytes).unwrap(),
            None => db.db.delete(REVISION).unwrap(),
        }
        if bytes == Some(encode_revision(u64::MAX)) {
            let mut service =
                Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
            assert_eq!(
                service.delete(tx().hash()),
                Err(Failure::Repository(Error::ResourceLimit))
            );
        } else {
            drop(db);
            assert!(matches!(
                Database::open(&temp.0, identity()),
                Err(Error::Corrupt(_))
            ));
        }
    }
}

#[test]
fn explicit_gc_signed_ttl_metrics_and_failed_commit_preserve_durable_records() {
    let temp = Temp::new();
    let db = database(&temp);
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    let t = pending();
    service.force_push(t.clone()).unwrap();
    assert_eq!(service.ttl(t.hash()).unwrap(), 5);
    service.adjust_ttl(10).unwrap();
    assert_eq!(service.ttl(t.hash()).unwrap(), -5);
    service.sync_target(100).unwrap();
    assert!(service.collect_at(0, true, 100).unwrap().is_empty());
    let mut limited = config();
    limited.commit_bytes = 0;
    service.recover(limited).unwrap();
    service.adjust_ttl(10).unwrap();
    service.sync_target(100).unwrap();
    assert!(matches!(
        service.collect_at(0, false, 100),
        Err(Failure::Repository(Error::ResourceLimit))
    ));
    assert_eq!(service.ttl(t.hash()).unwrap(), -5);
    assert_eq!(service.inspection().maintenance, Maintenance::default());
    assert_eq!(
        db.pool_view().unwrap().location(t.hash()).unwrap(),
        Location::Pool
    );
    service.recover(config()).unwrap();
    service.adjust_ttl(10).unwrap();
    assert_eq!(service.collect_at(0, false, 100).unwrap(), vec![t.hash()]);
    assert_eq!(service.inspection().maintenance.gced, 1);
    assert_eq!(
        db.pool_view().unwrap().location(t.hash()).unwrap(),
        Location::Deleted
    );
    assert_eq!(
        db.pool_view()
            .unwrap()
            .transaction(t.hash())
            .unwrap()
            .as_ref(),
        Some(t.as_ref())
    );
    drop(service);
    let mut restored = Service::start(db, NetworkProfile::mainnet(), config()).unwrap();
    assert!(restored.peek(10, None, None).unwrap().is_empty());
}

#[test]
fn explicit_candidate_binds_database_and_returns_below_minimum_before_parent_lookup() {
    let temp = Temp::new();
    let db = database(&temp);
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    let view = db.view().unwrap();
    let absent = Hash256([99; 32]);
    for gas in [0, 14999] {
        let got = service
            .candidate_at(
                &view,
                absent,
                gas,
                &BTreeSet::new(),
                Default::default(),
                || panic!("low gas must not read clock"),
            )
            .unwrap();
        assert_eq!(got.block, absent);
        assert!(got.transactions.is_empty());
        assert_eq!(got.gas_left, gas);
        assert!(!got.expired);
    }
    assert!(
        service
            .candidate_at(
                &view,
                absent,
                15000,
                &BTreeSet::new(),
                Default::default(),
                || 0
            )
            .is_err()
    );
    let other = Temp::new();
    let foreign = database(&other);
    let other_view = foreign.view().unwrap();
    assert!(matches!(
        service.candidate_at(
            &other_view,
            absent,
            0,
            &BTreeSet::new(),
            Default::default(),
            || 0
        ),
        Err(Failure::Repository(Error::Identity))
    ));
}
#[test]
fn failure_batches_preserve_duplicate_order_and_roll_back_every_count_on_failed_commit() {
    let temp = Temp::new();
    let db = database(&temp);
    let t = tx();
    let h = t.hash();
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    service.force_push(t.clone()).unwrap();
    drop(service);
    let mut limited = config();
    limited.commit_bytes = 0;
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), limited).unwrap();
    let report = |reason: &str| FailedTransaction {
        hash: h,
        reason: reason.into(),
        tuple: false,
    };
    let before = service.inspect(h).unwrap();
    let revision = db.pool_view().unwrap().revision();
    assert_eq!(
        service.record_failures(&[report("unrecognized"), report("crash")]),
        Err(Failure::Repository(Error::ResourceLimit))
    );
    assert_eq!(service.inspect(h).unwrap(), before);
    assert_eq!(db.pool_view().unwrap().revision(), revision);
    assert_eq!(db.pool_view().unwrap().location(h).unwrap(), Location::Pool);
    service.recover(config()).unwrap();
    let mut batch = vec![report("unrecognized"); 6];
    batch.push(FailedTransaction {
        hash: Hash256([99; 32]),
        reason: "crash".into(),
        tuple: false,
    });
    assert_eq!(
        service.record_failures(&batch).unwrap(),
        vec![false, false, false, false, true, false, false]
    );
    assert_eq!(
        db.pool_view().unwrap().location(h).unwrap(),
        Location::Deleted
    );
    assert_eq!(
        db.pool_view().unwrap().transaction(h).unwrap().as_ref(),
        Some(t.as_ref())
    );
    drop(service);
    drop(db);
    let db = database(&temp);
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    assert!(service.peek(100, None, None).unwrap().is_empty());
    assert_eq!(
        db.pool_view().unwrap().location(h).unwrap(),
        Location::Deleted
    );
}
