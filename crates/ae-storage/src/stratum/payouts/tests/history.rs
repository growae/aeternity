use super::super::history as journal;
use super::*;
fn rows(db: &Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.db
        .iterator(rocksdb::IteratorMode::Start)
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}
fn history(db: &Database) -> journal::Page {
    db.stratum(Default::default())
        .unwrap()
        .payout_history(0, 1024, journal::MAX_BYTES as usize)
        .unwrap()
}
fn sweep(db: &Database) {
    for _ in 0..10000 {
        if !db.gc_state().unwrap().active() {
            return;
        }
        db.gc_step(crate::gc::Limits {
            rows: 1,
            ..Default::default()
        })
        .unwrap();
    }
    panic!("bounded history fixture GC");
}
fn hkey(index: u64) -> Vec<u8> {
    [journal::PREFIX, &index.to_be_bytes()].concat()
}
fn strip(db: &Database) {
    let keys: Vec<_> = rows(db)
        .into_iter()
        .map(|(k, _)| k)
        .filter(|k| k == journal::HEAD || k.starts_with(journal::PREFIX))
        .collect();
    for k in keys {
        db.db.delete(k).unwrap();
    }
}
fn old_marker(db: &Database) {
    let Value::List(mut fields) = Value::decode(&db.identity.encode()).unwrap() else {
        panic!()
    };
    fields[0] = number(20);
    db.db
        .put(repo::IDENTITY, Value::List(fields).encode())
        .unwrap();
}
#[test]
fn exact_preparation_signature_and_every_dispatch_attempt_survive_pagination_and_restart() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup(&db);
    let r = prepared(&mut w, e);
    let snapshot = db.stratum(Default::default()).unwrap();
    let s = signed(&mut w, e, &r);
    let a = w
        .begin_payout_submission(e, &s, Default::default())
        .unwrap();
    let b = w
        .unaccepted_payout_submission(e, &a, Default::default())
        .unwrap();
    let c = w
        .begin_payout_submission(e, &b, Default::default())
        .unwrap();
    let d = w
        .reject_payout_submission(e, &c, "local pool rejection".into(), Default::default())
        .unwrap();
    let all = history(&db);
    assert_eq!(all.entries.len(), 7);
    assert!(!all.summary.as_ref().unwrap().partial);
    for (entry, r) in all.entries.iter().skip(1).zip([r, s, a, b, c, d.clone()]) {
        assert_eq!(entry.information.pending.as_ref(), Some(&r));
    }
    assert_eq!(
        snapshot
            .payout_history(0, 1024, 1 << 20)
            .unwrap()
            .entries
            .len(),
        2
    );
    let stable = db.stratum(Default::default()).unwrap();
    let mut cursor = 0;
    let mut entries = vec![];
    loop {
        let page = stable.payout_history(cursor, 2, 1 << 20).unwrap();
        assert_eq!(page.summary, all.summary);
        entries.extend(page.entries);
        match page.next {
            Some(n) => cursor = n,
            None => break,
        }
    }
    assert_eq!(entries, all.entries);
    let first_bytes = entries[0].encode().unwrap().len() + hkey(1).len();
    let page = stable.payout_history(0, 100, first_bytes).unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.next, Some(1));
    assert!(stable.payout_history(0, 1, first_bytes - 1).is_err());
    assert!(stable.payout_history(1, 1, first_bytes).is_err());
    for (after, limit, bytes) in [
        (8, 1, 1 << 20),
        (0, 0, 1 << 20),
        (0, 1025, 1 << 20),
        (0, 1, 0),
    ] {
        assert!(stable.payout_history(after, limit, bytes).is_err());
    }
    set_top(&db, 1, Protocol::Roma, Some(GROSS + 1), 0);
    w.refresh_payout_context(e, Default::default()).unwrap();
    assert_eq!(history(&db), all);
    drop(stable);
    drop(snapshot);
    drop(w);
    drop(db);
    let db = database(&t);
    let (mut w, e) = {
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let e = w.begin_payouts(config(), Default::default()).unwrap().epoch;
        (w, e)
    };
    assert_eq!(history(&db), all);
    assert_eq!(
        w.prepare_payout(e, 0.into(), Default::default()).unwrap(),
        Selection::Existing(Box::new(d))
    );
    assert_eq!(history(&db), all);
    db.stratum(Default::default()).unwrap().verify().unwrap();
}
#[test]
fn completed_requests_and_proofs_survive_active_release_pruning_and_later_completion() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup(&db);
    reward(&mut w, true);
    commit(&mut w, vec![Command::Payment(pay((100, 1)))]);
    let mut completed = vec![];
    for (n, height) in [(0, 100), (1, 101)] {
        if n == 1 {
            set_top(&db, 281, Protocol::Roma, Some(GROSS + 1), 1);
            w.refresh_payout_context(e, Default::default()).unwrap();
        }
        let r = prepared(&mut w, e);
        let r = signed(&mut w, e, &r);
        let block = include_call(&db, &r, height, |_| {});
        set_top(&db, height + 180, Protocol::Roma, Some(GROSS + 1), n + 1);
        if n == 1 {
            db.begin_gc(Default::default()).unwrap();
            sweep(&db);
        }
        let Confirmation::Confirmed(c) = w.confirm_payout(e, &r, Default::default()).unwrap()
        else {
            panic!()
        };
        let all = history(&db);
        let last = all.entries.last().unwrap();
        assert_eq!(last.kind, journal::Kind::Completed);
        assert_eq!(last.completed_request, Some(r.clone()));
        assert!(last.receipt.is_some());
        assert_eq!(c.block, block);
        assert!(db.db.get(super::super::retention::KEY).unwrap().is_none());
        assert_eq!(
            w.confirm_payout(e, &r, Default::default()).unwrap(),
            Confirmation::Confirmed(c.clone())
        );
        assert_eq!(history(&db), all);
        completed.push((r, c));
    }
    let expected = history(&db);
    assert_eq!(
        expected
            .entries
            .iter()
            .filter(|e| e.kind == journal::Kind::Completed)
            .count(),
        2
    );
    assert_eq!(stored(&db).last.as_ref(), Some(&completed[1].1));
    db.begin_gc(Default::default()).unwrap();
    sweep(&db);
    db.db
        .delete(repo::key(repo::CANONICAL, completed[0].1.block.0))
        .unwrap();
    db.db
        .delete(repo::key(repo::LOCATIONS, completed[0].1.hash.0))
        .unwrap();
    assert_eq!(history(&db), expected);
    drop(w);
    drop(db);
    let db = database(&t);
    assert_eq!(history(&db), expected);
    db.stratum(Default::default()).unwrap().verify().unwrap();
    println!(
        "Payout history: two complete requests retain proofs after pruning and later completion"
    );
}
#[test]
fn legacy_upgrade_records_one_partial_baseline_without_inventing_prior_attempts() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup(&db);
    let r = prepared(&mut w, e);
    let r = signed(&mut w, e, &r);
    let saved = stored(&db);
    drop(w);
    strip(&db);
    old_marker(&db);
    let before = rows(&db);
    drop(db);
    let db = database(&t);
    assert_eq!(stored(&db), saved);
    let page = history(&db);
    assert_eq!(page.entries.len(), 1);
    assert!(page.summary.as_ref().unwrap().partial);
    assert_eq!(page.entries[0].kind, journal::Kind::Baseline);
    assert_eq!(page.entries[0].information.pending, Some(r.clone()));
    for (k, v) in before {
        if k != repo::IDENTITY {
            assert_eq!(db.db.get(k).unwrap(), Some(v));
        }
    }
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let e = w.begin_payouts(config(), Default::default()).unwrap().epoch;
    assert_eq!(history(&db), page);
    w.begin_payout_submission(e, &r, Default::default())
        .unwrap();
    assert_eq!(history(&db).entries.len(), 2);
    assert!(history(&db).summary.unwrap().partial);
    drop(w);
    drop(db);
    let db = database(&t);
    db.stratum(Default::default()).unwrap().verify().unwrap();
}
#[test]
fn missing_reordered_foreign_or_corrupt_history_blocks_open_without_schema_rewrite() {
    for mode in 0..10 {
        let t = Temp::new();
        let db = database(&t);
        let (mut w, e) = setup(&db);
        prepared(&mut w, e);
        drop(w);
        match mode {
            0 => db.db.delete(journal::HEAD).unwrap(),
            1 => db.db.delete(hkey(1)).unwrap(),
            2 => db.db.put(hkey(1), b"invalid").unwrap(),
            3 => {
                let raw = db.db.get(hkey(1)).unwrap().unwrap();
                db.db.put(hkey(3), raw).unwrap();
            }
            4..=6 | 9 => {
                let raw = db.db.get(journal::HEAD).unwrap().unwrap();
                let Value::List(mut fields) = Value::decode(&raw).unwrap() else {
                    panic!()
                };
                match mode {
                    4 => fields[0] = number(2),
                    5 => fields[1] = Value::Bytes(b"wrong identity".to_vec()),
                    9 => fields[5] = number(1),
                    _ => fields[3] = number(1),
                };
                db.db
                    .put(journal::HEAD, Value::List(fields).encode())
                    .unwrap();
            }
            7 | 8 => {
                let mut page = history(&db);
                let last = page.entries.last_mut().unwrap();
                let old_size = last.encode().unwrap().len();
                if mode == 7 {
                    last.previous = Hash256(key(77));
                } else {
                    last.information.context.chain.revision = u64::MAX;
                }
                let raw = last.encode().unwrap();
                db.db.put(hkey(last.index), &raw).unwrap();
                let Value::List(mut fields) =
                    Value::decode(&db.db.get(journal::HEAD).unwrap().unwrap()).unwrap()
                else {
                    panic!()
                };
                fields[4] = Value::Bytes(ae_crypto::blake2b_256(&raw).0.to_vec());
                fields[3] = number(integer(&fields[3]).unwrap() + (raw.len() - old_size) as u64);
                db.db
                    .put(journal::HEAD, Value::List(fields).encode())
                    .unwrap();
            }
            _ => unreachable!(),
        }
        old_marker(&db);
        let before = rows(&db);
        drop(db);
        assert!(Database::open(&t.0, identity()).is_err(), "{mode}");
        let raw = rocksdb::DB::open_default(&t.0).unwrap();
        let after: Vec<_> = raw
            .iterator(rocksdb::IteratorMode::Start)
            .map(|x| {
                let (k, v) = x.unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect();
        assert_eq!(after, before);
    }
}
#[test]
fn history_append_is_atomic_across_failed_or_lost_preparation_acknowledgments() {
    for applied in [false, true] {
        let t = Temp::new();
        let db = database(&t);
        let (mut w, e) = setup(&db);
        let before = rows(&db);
        let result = w.prepare_payout_with(e, 0.into(), Default::default(), |stage, db, b| {
            assert_eq!(stage, Stage::Prepare);
            if applied {
                write(db, b)?;
            }
            Err(Error::Backend("history injected".into()))
        });
        assert!(result.is_err());
        assert!(w.recovery_required());
        if !applied {
            assert_eq!(rows(&db), before);
        } else {
            assert_eq!(history(&db).entries.len(), 2);
        }
        assert!(matches!(
            (applied, w.recover().unwrap()),
            (true, super::super::super::Recovery::Applied(_))
                | (false, super::super::super::Recovery::NotApplied(_))
        ));
        let _ = w.prepare_payout(e, 0.into(), Default::default()).unwrap();
        assert_eq!(history(&db).entries.len(), 2);
        drop(w);
        drop(db);
        let db = database(&t);
        assert_eq!(history(&db).entries.len(), 2);
        db.stratum(Default::default()).unwrap().verify().unwrap();
    }
}
#[test]
fn history_quota_and_receipt_corruption_fail_before_accounting_mutation() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup(&db);
    let head = db.db.get(journal::HEAD).unwrap().unwrap();
    let Value::List(mut fields) = Value::decode(&head).unwrap() else {
        panic!()
    };
    fields[3] = number(journal::MAX_BYTES);
    db.db
        .put(journal::HEAD, Value::List(fields).encode())
        .unwrap();
    let before = rows(&db);
    assert!(matches!(
        w.prepare_payout(e, 0.into(), Default::default()),
        Err(Failure::Storage(Error::ResourceLimit))
    ));
    assert_eq!(rows(&db), before);
    assert!(!w.recovery_required());
    db.db.put(journal::HEAD, head).unwrap();
    reward(&mut w, true);
    let r = prepared(&mut w, e);
    let r = signed(&mut w, e, &r);
    include(&db, &r, 100);
    set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
    assert!(matches!(
        w.confirm_payout(e, &r, Default::default()).unwrap(),
        Confirmation::Confirmed(_)
    ));
    let mut page = history(&db);
    let last = page.entries.last_mut().unwrap();
    let mut large = last.clone();
    large.receipt = Some(vec![0; super::super::retention::MAX_BYTES + 1]);
    assert!(matches!(
        large.encode(),
        Err(Failure::Storage(Error::ResourceLimit))
    ));
    let mut invalid = last.clone();
    invalid.index = 0;
    assert!(invalid.encode().is_err());
    let proof = last.receipt.as_mut().unwrap();
    *proof.last_mut().unwrap() ^= 1;
    let raw = last.encode().unwrap();
    db.db.put(hkey(last.index), &raw).unwrap();
    let Value::List(mut fields) =
        Value::decode(&db.db.get(journal::HEAD).unwrap().unwrap()).unwrap()
    else {
        panic!()
    };
    fields[4] = Value::Bytes(ae_crypto::blake2b_256(&raw).0.to_vec());
    db.db
        .put(journal::HEAD, Value::List(fields).encode())
        .unwrap();
    assert!(db.stratum(Default::default()).unwrap().verify().is_err());
    drop(w);
    drop(db);
    assert!(Database::open(&t.0, identity()).is_err());
}

mod recovery;
