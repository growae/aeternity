use super::*;
use crate::stratum::mining::payouts::preflight::task;

#[test]
fn independent_resource_axes_preserve_original_payout_results_or_all_input_roots() {
    let mut successful = 0;
    let mut stopped = 0;
    for p in [3, 6, 8] {
        for case in [
            "prepared_one",
            "prepared_two",
            "prepared_thirty_three",
            "prepared_two_hundred",
        ] {
            let row = gas::gas_find(p, case);
            let signed =
                SignedTransaction::<ContractTransaction>::decode(&bytes(&row["signed"])).unwrap();
            for axis in ["default", "lower_work", "bytes", "depth", "words"] {
                let mut limits = Limits::default();
                if axis != "default" {
                    limits.max_work = 1_000_000;
                }
                match axis {
                    "bytes" => limits.max_bytes = corpus_limits().max_bytes,
                    "depth" => limits.max_depth = corpus_limits().max_depth,
                    "words" => limits.max_memory_words *= 2,
                    _ => {}
                }
                let mut trees = from_snapshot(&row["before"]);
                let before = ae_chain::repository::Roots::of(&trees);
                let result = aevm::execute_signed_call(
                    &signed,
                    &mut trees,
                    &NetworkProfile::mainnet(),
                    &context(row),
                    &vm_config(b"ae_mainnet"),
                    limits,
                );
                if case == "prepared_two_hundred" && axis != "default" {
                    assert!(
                        matches!(
                            result,
                            Err(ae_vm_aevm::outer_call::Fault::Runtime(
                                ae_vm_aevm::runtime::Fault::Stop(
                                    ae_vm_aevm::execution::Stop::ResourceLimit
                                ),
                            ))
                        ),
                        "{p}/{case}/{axis}: {result:?}"
                    );
                    assert_eq!(ae_chain::repository::Roots::of(&trees), before);
                    stopped += 1;
                } else {
                    assert_state(&trees, &result.unwrap(), row);
                    successful += 1;
                }
            }
        }
    }
    assert_eq!((successful, stopped), (48, 12));
    println!("Payout resource axes: 48 native-result successes and 12 atomic lower-work stops");
}

#[test]
fn lower_importer_work_holds_maximum_batch_until_explicit_restart_with_default_limits() {
    for version in [3, 6, 8] {
        let temp = Temp::new();
        let row = gas::gas_find(version, "prepared_two_hundred");
        let (db, profile, _) = tasks::setup(&temp, row);
        let env = environment(row);
        seed(&db, prepared_payment(row));
        let mut config = ae_storage::pool::Config::default();
        config.policy.minimum_miner_gas_price = 1_000_000.into();
        let p = txpool::Worker::spawn(
            db.clone(),
            profile.clone(),
            config,
            txpool::Settings {
                origins_interval: None,
                ..Default::default()
            },
        )
        .unwrap();
        let s = signer();
        let snapshots = snapshots(&db);
        let config = durable::Config {
            caller: env.caller,
            contract: env.contract,
            delay: 2,
        };
        let low = ExecutionOwner(tasks::importer(
            &db,
            &profile,
            Limits {
                max_work: 1_000_000,
                ..Default::default()
            },
            6_000_000,
        ));
        let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
        let mut a = Worker::start(
            &mut w,
            db.clone(),
            Default::default(),
            Inputs::new(config.clone(), s.client(), p.client(), snapshots.clone()),
            Arc::new(Wall),
            &low.stratum_admitter(),
        )
        .unwrap();
        a.request_check();
        until(|| {
            tick(&mut a, &mut w);
            matches!(a.pending, Some(Pending::Preflight { .. }))
        });
        let before = stored(&db).pending.unwrap();
        a.pending.as_mut().unwrap().join();
        let Some(Pending::Preflight {
            result: Some(result),
            ..
        }) = &a.pending
        else {
            panic!()
        };
        let Err(task::Error::Evaluation(error)) = &result.result else {
            panic!("{:?}", result.result)
        };
        assert!(matches!(
            error.as_ref(),
            task::EvaluationFailure::Execution(task::ExecutionFailure::Runtime(
                ae_vm_aevm::runtime::Fault::Stop(ae_vm_aevm::execution::Stop::ResourceLimit,)
            ),)
        ));
        tick(&mut a, &mut w);
        assert!(a.pending.is_none());
        assert_eq!(stored(&db).pending, Some(before.clone()));
        assert!(before.signed.is_none());
        assert_eq!(p.client().info().submitted, 1);
        a.finish(&mut w).unwrap();
        drop(a);
        drop(w);
        drop(low);
        assert_eq!(snapshots.live_snapshots(), 0);
        // Change the configured execution limits through explicit owner restart.
        // The original request and its bounded generation remain immutable.
        let execution = execution_owner(&db, &profile);
        let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
        let mut inputs = Inputs::new(config, s.client(), p.client(), snapshots.clone());
        inputs.generation = payout::Generation::Native;
        let mut a = Worker::start(
            &mut w,
            db.clone(),
            Default::default(),
            inputs,
            Arc::new(Wall),
            &execution.stratum_admitter(),
        )
        .unwrap();
        let after = accepted(&mut a, &mut w);
        assert_eq!(after.request, before.request);
        let prepared = &before.request.as_ref().unwrap().prepared;
        assert_eq!(
            prepared.generation(),
            payout::Generation::Bounded(Default::default())
        );
        assert_eq!(
            prepared.call().transaction().encode(),
            bytes(&row["transaction"])
        );
        assert_eq!(
            after.signed.as_ref().unwrap().transaction,
            prepared
                .attach_signature(secret().sign(prepared.signing_bytes()))
                .unwrap()
                .encode()
        );
        assert_eq!(p.client().info().submitted, 2);
        a.finish(&mut w).unwrap();
        assert_eq!(snapshots.live_snapshots(), 0);
        p.shutdown().unwrap();
        s.retire();
    }
    println!(
        "Payout resource restart: three immutable 200-recipient requests pass with default owner limits"
    );
}
