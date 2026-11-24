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
    let (mut w, e) = setup(db);
    reward(&mut w, true);
    let r = prepared(&mut w, e);
    let r = signed(&mut w, e, &r);
    (w, e, r)
}
#[test]
fn failed_reverted_and_wrong_total_receipts_preserve_every_row_and_survive_reopen() {
    for case in 0..5 {
        let temp = Temp::new();
        let db = database(&temp);
        let (mut w, e, r) = pending(&db);
        let block = include_call(&db, &r, 100, |c| {
            let c = c.as_mut().unwrap();
            match case {
                0 => {
                    c.return_type = ReturnType::Error;
                    c.return_value = b"out_of_gas".to_vec();
                }
                1 => {
                    c.return_type = ReturnType::Revert;
                    c.return_value = b"no payout".to_vec();
                }
                2 => c.return_value[31] ^= 1,
                3 => c.return_value.clear(),
                _ => c.return_value.push(0),
            }
        });
        set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
        let before = rows(&db);
        let Confirmation::Failed(failed) = w
            .confirm_payout_with(e, &r, Default::default(), |_, _, _| {
                panic!("failed payout must not write")
            })
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(failed.inclusion.block, block);
        assert_eq!(failed.inclusion.hash, r.hash().unwrap().unwrap());
        assert_eq!(failed.inclusion.payment, r.payment.id);
        assert_eq!(
            failed.reason,
            match case {
                0 => FailureReason::ContractError,
                1 => FailureReason::ContractRevert,
                _ => FailureReason::UnexpectedReturn,
            }
        );
        assert_eq!(rows(&db), before);
        assert_eq!(stored(&db).pending, Some(r.clone()));
        assert_eq!(
            w.confirm_payout(e, &r, Default::default()).unwrap(),
            Confirmation::Failed(failed.clone())
        );
        assert_eq!(rows(&db), before);
        drop(w);
        drop(db);
        let db = database(&temp);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let i = w.begin_payouts(config(), Default::default()).unwrap();
        assert_eq!(i.pending, Some(r.clone()));
        assert_eq!(
            w.confirm_payout(i.epoch, &r, Default::default()).unwrap(),
            Confirmation::Failed(failed)
        );
        assert_eq!(
            db.stratum(Default::default()).unwrap().info().counts,
            [1, 1, 2, 1, 1, 0]
        );
    }
}
#[test]
fn absent_and_pruned_call_state_stays_explicitly_pending_until_evidence_is_available() {
    for pruned in [false, true] {
        let temp = Temp::new();
        let db = database(&temp);
        let (mut w, e, r) = pending(&db);
        let block = include_call(&db, &r, 100, |c| {
            if !pruned {
                *c = None;
            }
        });
        set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
        if pruned {
            let gc = repo::retention::State::default()
                .start(101, Default::default())
                .unwrap();
            db.db.put(repo::GC_STATE, gc.encode()).unwrap();
        }
        let before = rows(&db);
        let Confirmation::Unavailable(u) = w.confirm_payout(e, &r, Default::default()).unwrap()
        else {
            panic!()
        };
        assert_eq!(u.inclusion.block, block);
        assert_eq!(
            u.reason,
            if pruned {
                UnavailableReason::Pruned { floor: 101 }
            } else {
                UnavailableReason::MissingCall
            }
        );
        assert_eq!(rows(&db), before);
        // The pruning case models unavailable retention metadata without
        // physically deleting nodes; restoring an archive permits retry.
        if pruned {
            db.db.delete(repo::GC_STATE).unwrap();
        } else {
            db.db.delete(repo::key(repo::CANONICAL, block.0)).unwrap();
            let replacement = include(&db, &r, 100);
            assert_ne!(replacement, block);
        }
        assert!(matches!(
            w.confirm_payout(e, &r, Default::default()).unwrap(),
            Confirmation::Confirmed(_)
        ));
        assert!(stored(&db).pending.is_none());
    }
}
#[test]
fn receipt_identity_height_price_and_gas_binding_rejects_corrupt_successes() {
    for case in 0..7 {
        let temp = Temp::new();
        let db = database(&temp);
        let (mut w, e, r) = pending(&db);
        include_call(&db, &r, 100, |c| {
            let c = c.as_mut().unwrap();
            match case {
                0 => c.caller.value = key(88),
                1 => c.nonce = c.nonce.add(&1.into()),
                2 => c.height = 101.into(),
                3 => c.gas_price = c.gas_price.add(&1.into()),
                4 => c.gas_used = Unsigned::from(u64::MAX),
                5 => c.call_target = Some(config().contract.0.to_vec()),
                _ => {
                    c.call_target = Some(config().contract.0.to_vec());
                    c.contract = key(77);
                }
            }
        });
        set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
        let before = rows(&db);
        assert!(matches!(
            w.confirm_payout(e, &r, Default::default()),
            Err(Failure::Storage(Error::Corrupt(
                "Stratum payout call binding"
            )))
        ));
        assert_eq!(rows(&db), before);
        assert_eq!(stored(&db).pending, Some(r));
    }
}
#[test]
fn receipt_resource_and_missing_node_failures_are_retryable_without_cleanup() {
    let temp = Temp::new();
    let db = database(&temp);
    let (mut w, e, r) = pending(&db);
    let block = include_call(&db, &r, 100, |c| {
        c.as_mut().unwrap().log.push(ae_state::calls::LogEntry {
            address: vec![0; 32],
            topics: vec![],
            data: vec![7; 32000],
        })
    });
    set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
    let limits = Limits {
        bytes: 8000,
        ..Default::default()
    };
    stored(&db).encode(limits, core::Limits::default()).unwrap();
    let before = rows(&db);
    assert!(matches!(
        w.confirm_payout(e, &r, limits),
        Err(Failure::Storage(Error::ResourceLimit))
    ));
    assert_eq!(rows(&db), before);
    let node = db
        .view()
        .unwrap()
        .require_record(block)
        .unwrap()
        .roots
        .state
        .calls
        .unwrap();
    let k = repo::key(repo::NODES, node.0);
    let raw = db.db.get(&k).unwrap().unwrap();
    db.db.delete(&k).unwrap();
    let absent = rows(&db);
    assert!(w.confirm_payout(e, &r, Default::default()).is_err());
    assert_eq!(rows(&db), absent);
    db.db.put(&k, &raw).unwrap();
    assert!(matches!(
        w.confirm_payout(e, &r, Default::default()).unwrap(),
        Confirmation::Confirmed(_)
    ));
}
#[test]
fn canonical_reorg_rechecks_failed_receipts_before_allowing_later_success() {
    let temp = Temp::new();
    let db = database(&temp);
    let (mut w, e, r) = pending(&db);
    let failed = include_call(&db, &r, 100, |c| {
        c.as_mut().unwrap().return_type = ReturnType::Error
    });
    set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
    assert!(matches!(
        w.confirm_payout(e, &r, Default::default()).unwrap(),
        Confirmation::Failed(_)
    ));
    db.db.delete(repo::key(repo::CANONICAL, failed.0)).unwrap();
    db.db
        .delete(repo::key(repo::LOCATIONS, r.hash().unwrap().unwrap().0))
        .unwrap();
    assert_eq!(
        w.confirm_payout(e, &r, Default::default()).unwrap(),
        Confirmation::Pending
    );
    let succeeded = include(&db, &r, 101);
    assert_ne!(failed, succeeded);
    assert_eq!(
        w.confirm_payout(e, &r, Default::default()).unwrap(),
        Confirmation::Pending
    );
    set_top(&db, 281, Protocol::Roma, Some(GROSS + 1), 0);
    let Confirmation::Confirmed(done) = w.confirm_payout(e, &r, Default::default()).unwrap() else {
        panic!()
    };
    assert_eq!(done.block, succeeded);
}
#[test]
fn legacy_sent_rows_require_the_configured_direct_contract_identity() {
    let signed_record = {
        let temp = Temp::new();
        let db = database(&temp);
        let (mut w, e, r) = pending(&db);
        let _ = (&mut w, e);
        r
    };
    for wrong in [false, true] {
        let temp = Temp::new();
        let db = database(&temp);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        commit(
            &mut w,
            vec![Command::Payment(signed_record.expected_payment().unwrap())],
        );
        reward(&mut w, true);
        let mut cfg = config();
        if wrong {
            cfg.contract = ContractPublicKey(key(66));
        }
        let i = w.begin_payouts(cfg, Default::default()).unwrap();
        let legacy = i.pending.unwrap();
        assert!(legacy.request.is_none());
        include(&db, &signed_record, 100);
        set_top(&db, 280, Protocol::Roma, Some(GROSS + 1), 0);
        let before = rows(&db);
        if wrong {
            assert!(matches!(
                w.confirm_payout(i.epoch, &legacy, Default::default()),
                Err(Failure::Storage(Error::Corrupt(
                    "Stratum payout call identity"
                )))
            ));
            assert_eq!(rows(&db), before);
        } else {
            assert!(matches!(
                w.confirm_payout(i.epoch, &legacy, Default::default())
                    .unwrap(),
                Confirmation::Confirmed(_)
            ));
        }
    }
}
