use super::super::retention as archive;
use super::*;
use ae_state::calls::ReturnType;

fn rows(db: &Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.db
        .iterator(rocksdb::IteratorMode::Start)
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}
fn pending(db: &Arc<Database>) -> (Writer, u64, Record) {
    let (mut w, epoch) = setup(db);
    reward(&mut w, true);
    let r = prepared(&mut w, epoch);
    let r = signed(&mut w, epoch, &r);
    (w, epoch, r)
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
    panic!("bounded fixture GC did not complete");
}
fn archived(db: &Database) -> Vec<u8> {
    db.db.get(archive::KEY).unwrap().unwrap()
}

#[test]
fn receipts_survive_real_sweep_and_reopen_without_retaining_the_inclusion_tree() {
    for case in 0..5 {
        let temp = Temp::new();
        let db = database(&temp);
        let (w, _, r) = pending(&db);
        let block = include_call(&db, &r, 100, |call| {
            if case == 4 {
                *call = None;
                return;
            }
            let call = call.as_mut().unwrap();
            match case {
                1 => {
                    call.return_type = ReturnType::Error;
                    call.return_value = b"out_of_gas".to_vec();
                }
                2 => {
                    call.return_type = ReturnType::Revert;
                    call.return_value = b"failed".to_vec();
                }
                3 => call.return_value[31] ^= 1,
                _ => {}
            }
        });
        let root = db
            .view()
            .unwrap()
            .require_record(block)
            .unwrap()
            .roots
            .state
            .calls;
        set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
        let state = stored(&db);
        assert!(db.begin_gc(Default::default()).unwrap());
        let proof = archived(&db);
        assert_eq!(stored(&db), state);
        assert!(matches!(
            db.view().unwrap().trees(block, Default::default()),
            Err(Error::Pruned { .. })
        ));
        sweep(&db);
        if let Some(root) = root {
            assert!(db.db.get(repo::key(repo::NODES, root.0)).unwrap().is_none());
        }
        drop(w);
        drop(db);
        let db = database(&temp);
        assert_eq!(archived(&db), proof);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let info = w.begin_payouts(config(), Default::default()).unwrap();
        assert_eq!(info.pending, Some(r.clone()));
        let before = rows(&db);
        let result = w
            .confirm_payout(info.epoch, &r, Default::default())
            .unwrap();
        match (case, result) {
            (0, Confirmation::Confirmed(c)) => {
                assert_eq!(c.block, block);
                assert!(stored(&db).pending.is_none());
                assert!(db.db.get(archive::KEY).unwrap().is_none());
                assert_eq!(
                    w.confirm_payout(info.epoch, &r, Default::default())
                        .unwrap(),
                    Confirmation::Confirmed(c)
                );
            }
            (4, Confirmation::Unavailable(u)) => {
                assert_eq!(u.reason, UnavailableReason::MissingCall);
                assert_eq!(rows(&db), before);
            }
            (n, Confirmation::Failed(f)) => {
                assert_eq!(f.inclusion.block, block);
                assert_eq!(
                    f.reason,
                    match n {
                        1 => FailureReason::ContractError,
                        2 => FailureReason::ContractRevert,
                        3 => FailureReason::UnexpectedReturn,
                        _ => panic!(),
                    }
                );
                assert_eq!(rows(&db), before);
            }
            other => panic!("{other:?}"),
        }
    }
    println!("Retained payout receipts: five outcomes survive physical pruning and reopen");
}

#[test]
fn retained_success_requires_current_depth_location_and_exact_inclusion_after_reorg() {
    let temp = Temp::new();
    let db = database(&temp);
    let (mut w, e, r) = pending(&db);
    let first = include(&db, &r, 100);
    set_top(&db, 101, Protocol::Roma, Some(GROSS + 1), 0);
    db.begin_gc(Default::default()).unwrap();
    sweep(&db);
    assert_eq!(
        w.confirm_payout(e, &r, Default::default()).unwrap(),
        Confirmation::Pending
    );
    set_top(&db, 279, Protocol::Roma, Some(GROSS + 1), 0);
    assert_eq!(
        w.confirm_payout(e, &r, Default::default()).unwrap(),
        Confirmation::Pending
    );
    db.db
        .delete(repo::key(repo::LOCATIONS, r.hash().unwrap().unwrap().0))
        .unwrap();
    set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
    assert_eq!(
        w.confirm_payout(e, &r, Default::default()).unwrap(),
        Confirmation::Pending
    );
    db.db.delete(repo::key(repo::CANONICAL, first.0)).unwrap();
    let second = include_call(&db, &r, 101, |c| {
        c.as_mut().unwrap().return_type = ReturnType::Revert;
    });
    set_top(&db, 281, Protocol::Roma, Some(GROSS + 1), 0);
    db.begin_gc(Default::default()).unwrap();
    sweep(&db);
    let Confirmation::Failed(f) = w.confirm_payout(e, &r, Default::default()).unwrap() else {
        panic!()
    };
    assert_eq!(f.inclusion.block, second);
    assert_eq!(f.reason, FailureReason::ContractRevert);
    // A changed dispatch observation does not invalidate the same signed
    // transaction's evidence. It does not authorize using the old success.
    let r = w
        .begin_payout_submission(e, &r, Default::default())
        .unwrap();
    let r = w
        .reject_payout_submission(e, &r, "local rejection".into(), Default::default())
        .unwrap();
    assert!(matches!(
        w.confirm_payout(e, &r, Default::default()).unwrap(),
        Confirmation::Failed(_)
    ));
    db.db.delete(repo::key(repo::CANONICAL, second.0)).unwrap();
    db.db.put(repo::key(repo::CANONICAL, first.0), []).unwrap();
    db.db
        .put(
            repo::key(repo::LOCATIONS, r.hash().unwrap().unwrap().0),
            first.0,
        )
        .unwrap();
    let Confirmation::Confirmed(c) = w.confirm_payout(e, &r, Default::default()).unwrap() else {
        panic!()
    };
    assert_eq!(c.block, first);
    assert!(db.db.get(archive::KEY).unwrap().is_none());
}

#[test]
fn capture_failure_and_uncertain_gc_write_preserve_atomic_receipt_floor_pairs() {
    for failure in [1, 2] {
        let temp = Temp::new();
        let db = database(&temp);
        let (w, _, r) = pending(&db);
        include(&db, &r, 100);
        set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
        let before = rows(&db);
        db.gc_write_failure
            .store(failure, std::sync::atomic::Ordering::Relaxed);
        assert!(db.begin_gc(Default::default()).is_err());
        if failure == 1 {
            assert_eq!(rows(&db), before);
        } else {
            assert_eq!(db.gc_state().unwrap().height, 280);
            assert!(!archived(&db).is_empty());
        }
        drop(w);
        drop(db);
        let db = database(&temp);
        if failure == 1 {
            db.begin_gc(Default::default()).unwrap();
        }
        sweep(&db);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let info = w.begin_payouts(config(), Default::default()).unwrap();
        assert!(matches!(
            w.confirm_payout(info.epoch, &r, Default::default())
                .unwrap(),
            Confirmation::Confirmed(_)
        ));
    }
    let temp = Temp::new();
    let db = database(&temp);
    let (_w, _, r) = pending(&db);
    let block = include(&db, &r, 100);
    set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
    let root = db
        .view()
        .unwrap()
        .require_record(block)
        .unwrap()
        .roots
        .state
        .calls
        .unwrap();
    let original = db.db.get(repo::key(repo::NODES, root.0)).unwrap().unwrap();
    db.db.delete(repo::key(repo::NODES, root.0)).unwrap();
    let before = rows(&db);
    assert!(matches!(
        db.begin_gc(Default::default()),
        Err(Error::Trie(ae_trie::Error::MissingNode(_)))
    ));
    assert_eq!(rows(&db), before);
    db.db
        .put(repo::key(repo::NODES, root.0), vec![0; (16 << 20) + 1])
        .unwrap();
    let before = rows(&db);
    assert!(matches!(
        db.begin_gc(Default::default()),
        Err(Error::Trie(ae_trie::Error::ResourceLimit))
    ));
    assert_eq!(rows(&db), before);
    db.db.put(repo::key(repo::NODES, root.0), original).unwrap();
    db.begin_gc(Default::default()).unwrap();
    assert!(!archived(&db).is_empty());
}

#[test]
fn bounded_archive_exhaustion_stops_gc_without_discarding_prior_inclusions() {
    let temp = Temp::new();
    let db = database(&temp);
    let (_w, _, r) = pending(&db);
    for n in 0..17 {
        let height = 100 + n;
        set_top(&db, height, Protocol::Roma, Some(GROSS + 1), 0);
        include_call(&db, &r, height, |c| {
            c.as_mut().unwrap().return_type = ReturnType::Revert
        });
        set_top(&db, height + 1, Protocol::Roma, Some(GROSS + 1), 0);
        let before = rows(&db);
        if n == 16 {
            assert!(matches!(
                db.begin_gc(Default::default()),
                Err(Error::ResourceLimit)
            ));
            assert_eq!(rows(&db), before);
        } else {
            db.begin_gc(Default::default()).unwrap();
            sweep(&db);
        }
        assert_eq!(stored(&db).pending, Some(r.clone()));
    }
}

#[test]
fn legacy_pruned_receipts_remain_unknown_and_schema_upgrade_preserves_pending_bytes() {
    let temp = Temp::new();
    let db = database(&temp);
    let (w, _, r) = pending(&db);
    include(&db, &r, 100);
    set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
    let mut gc = repo::retention::State::default()
        .start(101, Default::default())
        .unwrap();
    gc.phase = repo::retention::Phase::Complete;
    db.db.put(repo::GC_STATE, gc.encode()).unwrap();
    let Value::List(mut id) = Value::decode(&identity().encode()).unwrap() else {
        panic!()
    };
    id[0] = number(19);
    db.db.put(repo::IDENTITY, Value::List(id).encode()).unwrap();
    let before = rows(&db);
    drop(w);
    drop(db);
    let db = database(&temp);
    assert_eq!(stored(&db).pending, Some(r.clone()));
    let mut after = rows(&db);
    after.retain(|(k, _)| k != repo::IDENTITY);
    assert_eq!(
        after,
        before
            .into_iter()
            .filter(|(k, _)| k != repo::IDENTITY)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        db.db.get(repo::IDENTITY).unwrap().unwrap(),
        identity().encode()
    );
    db.begin_gc(Default::default()).unwrap();
    sweep(&db);
    assert!(db.db.get(archive::KEY).unwrap().is_none());
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let info = w.begin_payouts(config(), Default::default()).unwrap();
    assert!(matches!(
        w.confirm_payout(info.epoch, &r, Default::default())
            .unwrap(),
        Confirmation::Unavailable(ReceiptUnavailable {
            reason: UnavailableReason::Pruned { .. },
            ..
        })
    ));
    assert_eq!(stored(&db).pending, Some(r));
}

#[test]
fn corrupt_or_foreign_proofs_never_authorize_cleanup_or_schema_upgrade() {
    for case in 0..10 {
        let temp = Temp::new();
        let db = database(&temp);
        let (mut w, e, r) = pending(&db);
        include(&db, &r, 100);
        set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
        db.begin_gc(Default::default()).unwrap();
        sweep(&db);
        let good = archived(&db);
        let Value::List(mut fields) = Value::decode(&good).unwrap() else {
            panic!()
        };
        match case {
            0 => fields[2] = number(r.sequence + 1),
            1 => {
                let Value::Bytes(b) = &mut fields[1] else {
                    panic!()
                };
                b[3] ^= 1;
            }
            2 => {
                let Value::Bytes(b) = &mut fields[4] else {
                    panic!()
                };
                *b.last_mut().unwrap() ^= 1;
            }
            7 => fields[0] = number(2),
            9 => {
                let Value::List(entries) = &mut fields[5] else {
                    panic!()
                };
                entries.push(entries[0].clone());
            }
            _ => {
                let Value::List(entries) = &mut fields[5] else {
                    panic!()
                };
                let Value::List(proof) = &mut entries[0] else {
                    panic!()
                };
                match case {
                    3 => {
                        let Value::List(roots) = &mut proof[1] else {
                            panic!()
                        };
                        let Value::Bytes(b) = &mut roots[1] else {
                            panic!()
                        };
                        b[0] ^= 1;
                    }
                    4 => {
                        let Value::List(nodes) = &mut proof[2] else {
                            panic!()
                        };
                        let Value::List(node) = &mut nodes[0] else {
                            panic!()
                        };
                        let Value::Bytes(b) = &mut node[1] else {
                            panic!()
                        };
                        b[0] ^= 1;
                    }
                    5 => proof[2] = Value::List(vec![]),
                    6 => {
                        let Value::List(nodes) = &mut proof[2] else {
                            panic!()
                        };
                        let raw = vec![0x80];
                        nodes.push(Value::List(vec![
                            Value::Bytes(ae_crypto::blake2b_256(&raw).0.to_vec()),
                            Value::Bytes(raw),
                        ]));
                    }
                    8 => {
                        let Value::Bytes(raw) = &proof[0] else {
                            panic!()
                        };
                        let Header::Micro(mut h) =
                            Header::decode(raw, ae_codec::headers::HeaderMode::ProofOfWork)
                                .unwrap()
                        else {
                            panic!()
                        };
                        h.time += 1;
                        proof[0] = Value::Bytes(Header::Micro(h).encode().unwrap());
                    }
                    _ => panic!(),
                }
            }
        }
        let bad = Value::List(fields).encode();
        db.db.put(archive::KEY, &bad).unwrap();
        let before = rows(&db);
        let result = w.confirm_payout(e, &r, Default::default());
        if case == 8 {
            assert!(matches!(result, Ok(Confirmation::Unavailable(_))));
        } else {
            assert!(result.is_err(), "case {case}: {result:?}");
        }
        assert_eq!(rows(&db), before);
        if case == 0 {
            let Value::List(mut id) = Value::decode(&identity().encode()).unwrap() else {
                panic!()
            };
            id[0] = number(19);
            let old = Value::List(id).encode();
            db.db.put(repo::IDENTITY, &old).unwrap();
            drop(w);
            drop(db);
            assert!(Database::open(&temp.0, identity()).is_err());
            let raw = rocksdb::DB::open_default(&temp.0).unwrap();
            assert_eq!(raw.get(repo::IDENTITY).unwrap(), Some(old));
            assert_eq!(raw.get(archive::KEY).unwrap(), Some(bad));
        } else {
            db.db.put(archive::KEY, good).unwrap();
            assert!(matches!(
                w.confirm_payout(e, &r, Default::default()).unwrap(),
                Confirmation::Confirmed(_)
            ));
        }
    }
}

#[test]
fn receipt_release_is_checked_and_atomic_with_accounting_even_after_lost_acknowledgment() {
    for applied in [false, true] {
        let temp = Temp::new();
        let db = database(&temp);
        let (mut w, e, r) = pending(&db);
        include_call(&db, &r, 100, |c| {
            c.as_mut().unwrap().log.push(ae_state::calls::LogEntry {
                address: vec![0; 32],
                topics: vec![],
                data: vec![7; 32000],
            })
        });
        set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
        let s = db.stratum(Default::default()).unwrap();
        let stale = archive::release(&s).unwrap();
        let version = s.version();
        drop(s);
        db.begin_gc(Default::default()).unwrap();
        sweep(&db);
        let before = rows(&db);
        assert!(matches!(
            w.commit_extra(version, vec![], vec![stale], write),
            Err(Failure::Storage(Error::Conflict))
        ));
        assert_eq!(rows(&db), before);
        assert!(matches!(
            w.confirm_payout(
                e,
                &r,
                Limits {
                    bytes: 8000,
                    ..Default::default()
                }
            ),
            Err(Failure::Storage(Error::ResourceLimit))
        ));
        assert_eq!(rows(&db), before);
        assert!(
            w.confirm_payout_with(e, &r, Default::default(), |stage, db, batch| {
                assert_eq!(stage, Stage::Confirm);
                if applied {
                    write(db, batch)?;
                }
                Err(Error::Backend("receipt cleanup acknowledgment".into()))
            })
            .is_err()
        );
        if applied {
            assert!(db.db.get(archive::KEY).unwrap().is_none());
            assert!(stored(&db).pending.is_none());
            assert!(matches!(w.recover().unwrap(), Recovery::Applied(_)));
        } else {
            assert_eq!(rows(&db), before);
            assert!(matches!(w.recover().unwrap(), Recovery::NotApplied(_)));
        }
        assert!(matches!(
            w.confirm_payout(e, &r, Default::default()).unwrap(),
            Confirmation::Confirmed(_)
        ));
        assert!(db.db.get(archive::KEY).unwrap().is_none());
    }
}

mod insertion;
mod recovery;
