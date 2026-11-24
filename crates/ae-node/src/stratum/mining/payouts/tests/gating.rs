use super::*;

fn pending_preflight(a: &mut Worker, w: &mut storage::Writer) -> Record {
    a.request_check();
    until(|| {
        tick(a, w);
        matches!(a.pending, Some(Pending::Preflight { .. }))
    });
    stored(&a.db).pending.unwrap()
}
fn completion(a: &mut Worker) -> Arc<task::Completion> {
    a.pending.as_mut().unwrap().join();
    let Some(Pending::Preflight {
        result: Some(result),
        ..
    }) = &a.pending
    else {
        panic!("expected owned preflight");
    };
    result.clone()
}
fn rollback(db: &Database) {
    db.rollback(
        ae_chain::repository::rollback::Target::Height(0),
        Default::default(),
    )
    .unwrap();
}

#[test]
fn invalid_contracts_and_insufficient_maximum_funds_hold_exact_unsigned_and_signed_requests() {
    for case in 0..6 {
        for signed in [false, true] {
            let temp = Temp::new();
            let mut input = trees();
            match case {
                0 => input.contracts.delete(config().contract.0).unwrap(),
                5 => input
                    .accounts
                    .enter(&Account::new(
                        config().caller,
                        (GROSS + 1_000_000_000).into(),
                    ))
                    .unwrap(),
                _ => {
                    let mut contract = input
                        .contracts
                        .record(config().contract.0)
                        .unwrap()
                        .unwrap();
                    let ae_state::contracts::Code::Inline(raw) = &contract.code else {
                        panic!()
                    };
                    let mut code = ae_codec::sophia::SophiaCode::decode(raw).unwrap();
                    code.byte_code = match case {
                        // Valid ABI metadata with deliberately substituted VM code.
                        2 => vec![0x5b, 0x60, 0, 0x56], // exhaust gas
                        3 => vec![0x60, 0, 0x60, 0, 0x52, 0x60, 0, 0x60, 0, 0xfd],
                        4 => vec![0x34, 0x60, 0, 0xf3], // return amount without paying
                        _ => vec![],
                    };
                    contract.code = ae_state::contracts::Code::Inline(if case == 1 {
                        vec![0xff]
                    } else {
                        code.encode().unwrap()
                    });
                    input
                        .contracts
                        .enter(&contract, &Default::default())
                        .unwrap();
                }
            }
            let db = database_with_trees(&temp, &input);
            seed(&db, payment());
            let p = pool(&db, 1);
            let s = signer();
            let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
            let mut a = actor(&db, &mut w, &s, &p);
            let durable::Selection::Prepared(before) = w
                .prepare_payout_with_generation(
                    a.epoch,
                    1.into(),
                    a.inputs.generation,
                    Default::default(),
                )
                .unwrap()
            else {
                panic!()
            };
            let mut before = *before;
            if signed {
                let signature =
                    secret().sign(before.request.as_ref().unwrap().prepared.signing_bytes());
                before = w
                    .sign_payout(
                        a.epoch,
                        &before,
                        signature,
                        Date::from_unix_millis(1_700_000_000_000).unwrap(),
                        Default::default(),
                    )
                    .unwrap();
            }
            // A dispatch bug would now surface as a stopped-signer error.
            s.request_shutdown();
            until(|| s.is_finished());
            let chain = db.view().unwrap().status();
            pending_preflight(&mut a, &mut w);
            let first = completion(&mut a);
            let Err(task::Error::Evaluation(error)) = &first.result else {
                panic!("case {case}: {:?}", first.result)
            };
            match case {
                2 => assert!(matches!(
                    error.as_ref(),
                    task::EvaluationFailure::Receipt(preflight::ReceiptFailure::Return(
                        ae_state::calls::ReturnType::Error
                    ))
                )),
                3 => assert!(matches!(
                    error.as_ref(),
                    task::EvaluationFailure::Receipt(preflight::ReceiptFailure::Return(
                        ae_state::calls::ReturnType::Revert
                    ))
                )),
                4 => assert!(matches!(
                    error.as_ref(),
                    task::EvaluationFailure::Effects { .. }
                )),
                _ => assert!(matches!(
                    error.as_ref(),
                    task::EvaluationFailure::Execution(_)
                )),
            }
            tick(&mut a, &mut w);
            assert!(a.pending.is_none() && a.authority.is_none());
            assert_eq!(stored(&db).pending, Some(before.clone()));
            assert_eq!(p.client().info().submitted, 0);
            for _ in 0..3 {
                tick(&mut a, &mut w);
            }
            assert!(
                a.pending.is_none(),
                "failed checks wait for a retry trigger"
            );
            pending_preflight(&mut a, &mut w);
            let retry = completion(&mut a);
            assert_ne!(retry.id, first.id);
            assert_eq!(retry.prepared, first.prepared);
            assert!(retry.result.is_err());
            a.finish(&mut w).unwrap();
            assert_eq!(a.inputs.snapshots.live_snapshots(), 0);
            assert_eq!(stored(&db).pending, Some(before));
            assert_eq!(db.view().unwrap().status(), chain);
            assert_eq!(p.client().info().submitted, 0);
            p.shutdown().unwrap();
            s.retire();
        }
    }
    println!(
        "Live payout preflight: twelve unsigned/signed contract and funding failures hold exact bytes"
    );
}

#[test]
fn snapshot_pressure_holds_the_request_then_retries_with_the_same_owner() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    let held: Vec<_> = (0..8).map(|_| a.inputs.snapshots.view().unwrap()).collect();
    let before = pending_preflight(&mut a, &mut w);
    let result = completion(&mut a);
    assert!(matches!(
        result.result,
        Err(task::Error::Source(
            ae_sync::chain::pow::stratum::execution::Error::Snapshot(
                ae_storage::snapshots::Error::ResourceLimit
            )
        ))
    ));
    tick(&mut a, &mut w);
    assert_eq!(stored(&db).pending, Some(before.clone()));
    assert_eq!(p.client().info().submitted, 1); // price only
    drop(held);
    assert_eq!(a.inputs.snapshots.live_snapshots(), 0);
    a.request_check();
    let after = accepted(&mut a, &mut w);
    assert_eq!(after.request, before.request);
    assert_eq!(p.client().info().submitted, 2);
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}

#[test]
fn joined_preflight_survives_read_failure_and_report_without_reexecution() {
    for report in [false, true] {
        let temp = Temp::new();
        let db = database(&temp);
        seed(&db, payment());
        let p = pool(&db, 1);
        let s = signer();
        let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
        let mut a = actor(&db, &mut w, &s, &p);
        let before = pending_preflight(&mut a, &mut w);
        let first = completion(&mut a);
        assert!(first.result.is_ok(), "{:?}", first.result);
        let limits = a.inputs.limits;
        a.inputs.limits.bytes = 1;
        assert!(a.tick(&mut w, true, Instant::now()).is_err());
        assert!(Arc::ptr_eq(&first, &completion(&mut a)));
        assert_eq!(stored(&db).pending, Some(before.clone()));
        if report {
            let Some(Completion::Preflight { record, result }) = a.report().unsettled else {
                panic!()
            };
            assert_eq!(*record, before);
            assert!(Arc::ptr_eq(&result, &first));
        } else {
            a.inputs.limits = limits;
            let after = accepted(&mut a, &mut w);
            assert_eq!(after.request, before.request);
            let Some(Notice::Preflight(result)) = a.shared.status().last else {
                panic!()
            };
            assert!(Arc::ptr_eq(&result, &first));
            a.finish(&mut w).unwrap();
        }
        assert_eq!(a.inputs.snapshots.live_snapshots(), 0);
        assert_eq!(p.client().info().submitted, if report { 1 } else { 2 });
        p.shutdown().unwrap();
        s.retire();
    }
}

#[test]
fn chain_change_during_accepted_signature_rechecks_the_exact_signed_transaction() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    pending_sign(&mut a, &mut w);
    let before = stored(&db).pending.unwrap();
    let first = a.authority.as_ref().unwrap().checked.clone();
    rollback(&db);
    assert!(!a.preflight.is_current(&first).unwrap());
    a.pending.as_mut().unwrap().join();
    tick(&mut a, &mut w);
    assert!(matches!(a.pending, Some(Pending::Preflight { .. })));
    let signed = stored(&db).pending.unwrap();
    assert_eq!(signed.request, before.request);
    assert_eq!(
        signed.signed.as_ref().unwrap().transaction,
        before
            .request
            .as_ref()
            .unwrap()
            .prepared
            .attach_signature(
                secret().sign(before.request.as_ref().unwrap().prepared.signing_bytes())
            )
            .unwrap()
            .encode()
    );
    assert_eq!(p.client().info().submitted, 1);
    let second = completion(&mut a);
    let checked = second.result.as_ref().unwrap();
    assert_eq!(checked.evaluation(), first.evaluation());
    assert_ne!(
        checked.metadata().status.revision,
        first.metadata().status.revision
    );
    let after = accepted(&mut a, &mut w);
    assert_eq!(
        after.signed.as_ref().unwrap().transaction,
        signed.signed.as_ref().unwrap().transaction
    );
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}

#[test]
fn stale_completed_check_never_reaches_signing_before_a_fresh_check() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    let before = pending_preflight(&mut a, &mut w);
    let first = completion(&mut a);
    rollback(&db);
    tick(&mut a, &mut w);
    assert_eq!(stored(&db).pending, Some(before));
    assert!(matches!(a.pending, Some(Pending::Preflight { .. })));
    let fresh = completion(&mut a);
    assert_ne!(fresh.id, first.id);
    assert_eq!(fresh.prepared, first.prepared);
    assert!(a.authority.is_none());
    accepted(&mut a, &mut w);
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}

#[test]
fn preflight_shutdown_joins_readers_and_never_dispatches_late_work() {
    for report in [false, true] {
        let temp = Temp::new();
        let db = database(&temp);
        seed(&db, payment());
        let p = pool(&db, 1);
        let s = signer();
        let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
        let mut a = actor(&db, &mut w, &s, &p);
        let before = pending_preflight(&mut a, &mut w);
        let builder = a.preflight.clone();
        let result = if report {
            let Some(Completion::Preflight { record, result }) = a.report().unsettled else {
                panic!()
            };
            assert_eq!(*record, before);
            result
        } else {
            a.finish(&mut w).unwrap();
            let Some(Notice::Preflight(result)) = a.shared.status().last else {
                panic!()
            };
            result
        };
        assert!(result.result.is_ok() || matches!(result.result, Err(task::Error::Cancelled)));
        assert!(!builder.busy());
        assert_eq!(a.inputs.snapshots.live_snapshots(), 0);
        assert_eq!(stored(&db).pending, Some(before));
        for _ in 0..3 {
            a.request_check();
            tick(&mut a, &mut w);
        }
        assert_eq!(p.client().info().submitted, 1);
        p.shutdown().unwrap();
        s.retire();
    }
}

#[test]
fn post_intent_stale_error_and_close_keep_nonacceptance_through_failed_restore() {
    for case in 0..3 {
        let temp = Temp::new();
        let db = database(&temp);
        seed(&db, payment());
        let p = pool(&db, 1);
        let s = signer();
        let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
        let mut a = actor(&db, &mut w, &s, &p);
        pending_sign(&mut a, &mut w);
        a.pending.as_mut().unwrap().join();
        a.complete(&mut w, true).unwrap();
        let ready = stored(&db).pending.unwrap();
        let sending = w
            .begin_payout_submission(a.epoch, &ready, Default::default())
            .unwrap();
        let tx = Signed::decode(&sending.signed.as_ref().unwrap().transaction).unwrap();
        match case {
            0 => rollback(&db),
            1 => a.inputs.limits.payout.transaction_bytes = 1,
            _ => a.shared.stop(),
        }
        a.inputs.limits.bytes = 1; // fail restoration after nonacceptance is retained
        assert!(a.submit(&mut w, sending.clone(), tx, true).is_err());
        assert_eq!(stored(&db).pending, Some(sending.clone()));
        assert_eq!(p.client().info().submitted, 1);
        let Some(Pending::Unaccepted { record, reason }) = &a.pending else {
            panic!()
        };
        assert_eq!(record.as_ref(), &sending);
        assert!(matches!(
            (case, reason),
            (0, Withdrawal::Stale) | (1, Withdrawal::Preflight(_)) | (2, Withdrawal::Stopped)
        ));
        a.inputs.limits = Default::default();
        let result = a.complete(&mut w, true);
        assert_eq!(result.is_err(), case == 1);
        let restored = stored(&db).pending.unwrap();
        assert!(matches!(
            restored.signed.as_ref().unwrap().dispatch,
            Dispatch::Ready(1)
        ));
        assert_eq!(restored.request, ready.request);
        assert_eq!(
            restored.signed.as_ref().unwrap().transaction,
            ready.signed.as_ref().unwrap().transaction
        );
        if case == 0 {
            let after = accepted(&mut a, &mut w);
            assert_eq!(after.request, ready.request);
            assert_eq!(p.client().info().submitted, 2);
            a.finish(&mut w).unwrap();
        } else {
            let report = a.report();
            assert_eq!(report.unsettled.is_some(), case == 1);
            assert_eq!(p.client().info().submitted, 1);
        }
        p.shutdown().unwrap();
        s.retire();
    }
}

#[test]
fn startup_rejects_foreign_snapshot_or_importer_before_creating_a_payout_epoch() {
    let temp = Temp::new();
    let elsewhere = Temp::new();
    let db = database(&temp);
    let other = database(&elsewhere);
    let own_execution = execution_owner(&db, &network());
    let other_execution = execution_owner(&other, &network());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    for case in 0..3 {
        let mut i = Inputs::new(
            config(),
            s.client(),
            p.client(),
            snapshots(if case == 0 { &other } else { &db }),
        );
        if case == 2 {
            i.preflight.history = 0;
        }
        let source = if case == 1 {
            &other_execution
        } else {
            &own_execution
        };
        assert!(
            Worker::start(
                &mut w,
                db.clone(),
                Default::default(),
                i,
                Arc::new(Wall),
                &source.stratum_admitter()
            )
            .is_err()
        );
        assert!(
            db.stratum(Default::default())
                .unwrap()
                .payouts(Default::default())
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(p.client().info().submitted, 0);
    p.shutdown().unwrap();
    s.retire();
}

#[test]
fn restarted_unsigned_and_signed_requests_keep_their_generation_and_shared_snapshot_owner() {
    use ae_stratum::payout::Generation;
    for generation in [Generation::Native, Generation::Bounded(Default::default())] {
        for signed in [false, true] {
            let temp = Temp::new();
            let db = database(&temp);
            seed(&db, payment());
            let p = pool(&db, 1);
            let s = signer();
            let execution = execution_owner(&db, &network());
            let snapshots = snapshots(&db);
            let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
            let mut i = Inputs::new(config(), s.client(), p.client(), snapshots.clone());
            i.generation = generation;
            let mut a = Worker::start(
                &mut w,
                db.clone(),
                Default::default(),
                i,
                Arc::new(Wall),
                &execution.stratum_admitter(),
            )
            .unwrap();
            if signed {
                pending_sign(&mut a, &mut w);
            } else {
                pending_preflight(&mut a, &mut w);
            }
            a.finish(&mut w).unwrap();
            let before = stored(&db).pending.unwrap();
            let original = before.request.as_ref().unwrap().prepared.clone();
            assert_eq!(before.signed.is_some(), signed);
            drop(a);
            drop(w);
            assert_eq!(snapshots.live_snapshots(), 0);
            assert!(matches!(
                ae_storage::snapshots::Owner::start(db.clone(), Default::default()),
                Err(ae_storage::snapshots::Error::AlreadyRunning)
            ));
            let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
            let mut i = Inputs::new(config(), s.client(), p.client(), snapshots.clone());
            i.generation = if generation == Generation::Native {
                Generation::Bounded(Default::default())
            } else {
                Generation::Native
            };
            let mut a = Worker::start(
                &mut w,
                db.clone(),
                Default::default(),
                i,
                Arc::new(Wall),
                &execution.stratum_admitter(),
            )
            .unwrap();
            let after = accepted(&mut a, &mut w);
            assert_eq!(after.request, before.request);
            assert_eq!(
                after.request.as_ref().unwrap().prepared.generation(),
                generation
            );
            assert_eq!(
                after.signed.as_ref().unwrap().transaction,
                original
                    .attach_signature(secret().sign(original.signing_bytes()))
                    .unwrap()
                    .encode()
            );
            assert_eq!(p.client().info().submitted, 2);
            a.finish(&mut w).unwrap();
            assert_eq!(snapshots.live_snapshots(), 0);
            p.shutdown().unwrap();
            s.retire();
        }
    }
}

#[test]
fn live_preflight_preserves_importer_vm_and_block_gas_limits_without_signing() {
    for zero_work in [false, true] {
        let temp = Temp::new();
        let db = database(&temp);
        seed(&db, payment());
        let p = pool(&db, 1);
        let s = signer();
        let execution = fixture::execution(&network()).with_config(|c| {
            let mut limits = c.aevm_limits;
            if zero_work {
                limits.max_work = 0;
            }
            ae_execution::transactions::owned::Owned::capture(&ae_execution::transactions::Config {
                aevm_limits: limits,
                ..*c
            })
            .unwrap()
        });
        let mut rules = fixture::rules(db.view().unwrap().identity().genesis);
        if !zero_work {
            rules.admission.configured_gas_limit = 1.into();
        }
        let execution = ExecutionOwner(
            chain::Importer::new(db.clone(), execution, rules, Default::default()).unwrap(),
        );
        let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
        let mut a = Worker::start(
            &mut w,
            db.clone(),
            Default::default(),
            inputs(&db, &s, &p),
            Arc::new(Wall),
            &execution.stratum_admitter(),
        )
        .unwrap();
        s.request_shutdown();
        until(|| s.is_finished());
        let before = pending_preflight(&mut a, &mut w);
        let result = completion(&mut a);
        let Err(task::Error::Evaluation(failure)) = &result.result else {
            panic!("{:?}", result.result)
        };
        if zero_work {
            assert!(matches!(
                failure.as_ref(),
                task::EvaluationFailure::Execution(_)
            ));
        } else {
            assert!(matches!(
                failure.as_ref(),
                task::EvaluationFailure::BlockGas
            ));
        }
        tick(&mut a, &mut w);
        assert!(a.pending.is_none() && a.authority.is_none());
        assert_eq!(stored(&db).pending, Some(before));
        assert_eq!(p.client().info().submitted, 1);
        a.finish(&mut w).unwrap();
        assert_eq!(a.inputs.snapshots.live_snapshots(), 0);
        p.shutdown().unwrap();
        s.retire();
    }
}
