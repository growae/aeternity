use super::*;
pub(super) static GAS_DATA: LazyLock<J> = LazyLock::new(|| {
    let p: J = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_stratum_payout_gas.json"
    )))
    .unwrap();
    expand(&p["root"], p["values"].as_array().unwrap())
});
pub(super) fn gas_find(p: u32, name: &str) -> &'static J {
    GAS_DATA["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == p && r["case_name"] == name)
        .unwrap()
}
#[test]
fn bounded_payout_generation_and_signed_execution_match_78_original_vm_cases() {
    assert_eq!(GAS_DATA["contract"], DATA["contract"]);
    assert_eq!(GAS_DATA["wrapped"], DATA["wrapped"]);
    let mut calls = 0;
    let mut prepared = 0;
    let mut over = 0;
    for row in GAS_DATA["runs"].as_array().unwrap() {
        let policy = payout::GasPolicy {
            max_transfers: 1000,
            block_gas_limit: 50_000_000,
            ..Default::default()
        };
        let env = environment(row);
        let expected = bytes(&row["transaction"]);
        let tx = ContractTransaction::decode(&expected).unwrap();
        let public = policy.call(&transfers(row), &env, b"ae_mainnet", Default::default());
        if tx.gas_limit() > 6_000_000.into() {
            assert!(matches!(
                public,
                Err(payout::Error::BlockGas {
                    maximum: 6_000_000,
                    ..
                })
            ));
            over += 1;
        } else {
            assert_eq!(public.unwrap().transaction().encode(), expected);
        }
        // Six 226-recipient calls exceed the public block gate. The complete
        // native execution comparison here is standalone VM evaluation.
        let call = policy
            .call(&transfers(row), &env, b"ae_private", Default::default())
            .unwrap();
        assert_eq!(call.transaction().encode(), expected);
        if !row["prepared"].is_null() {
            let plan = payout::prepare_with_generation(
                &prepared_payment(row),
                &env,
                Some(&unsigned("1000000000000000000000000000")),
                b"ae_mainnet",
                payout::Generation::Bounded(Default::default()),
                Default::default(),
            )
            .unwrap();
            assert_eq!(
                plan.net(),
                &number(row["prepared"]["net"].as_str().unwrap())
            );
            assert_eq!(plan.call().transaction().encode(), expected);
            assert_eq!(
                plan.attach_signature(secret().sign(plan.signing_bytes()))
                    .unwrap()
                    .encode(),
                bytes(&row["signed"])
            );
            prepared += 1;
        }
        let signed =
            SignedTransaction::<ContractTransaction>::decode(&bytes(&row["signed"])).unwrap();
        let mut t = from_snapshot(&row["before"]);
        let receipt = aevm::execute_signed_call(
            &signed,
            &mut t,
            &NetworkProfile::mainnet(),
            &context(row),
            &vm_config(b"ae_mainnet"),
            corpus_limits(),
        )
        .unwrap();
        assert_eq!(receipt.call.return_type, ReturnType::Ok);
        assert!(payout::confirms_total(
            tx.amount(),
            &receipt.call.return_value
        ));
        assert!(receipt.call.gas_used <= *tx.gas());
        assert_state(&t, &receipt, row);
        calls += 1;
    }
    assert_eq!((calls, prepared, over), (78, 24, 6));
}

#[test]
fn bounded_live_payouts_sign_submit_and_execute_original_code_with_configured_budgets() {
    let mut defaults = 0;
    let mut retries = 0;
    for p in [3, 6, 8] {
        for case in [
            "prepared_one",
            "prepared_two",
            "prepared_thirty_three",
            "prepared_two_hundred",
        ] {
            let row = gas_find(p, case);
            let input = from_snapshot(&row["before"]);
            let env = environment(row);
            let temp = Temp::new();
            let profile = NetworkProfile::custom(
                b"ae_payout_contract_local".to_vec(),
                vec![Activation {
                    height: 0,
                    protocol: protocol(row),
                }],
            )
            .unwrap();
            let Header::Key(mut h) = genesis::header(Network::Uat).unwrap() else {
                panic!()
            };
            h.common.version = p;
            h.common.state_hash = input.roots().hash().0;
            if p >= 2 {
                h.info = Some([0; 4]);
            }
            let id = Identity::new(
                &profile,
                Header::Key(h.clone()).hash().unwrap(),
                Hash256::ZERO,
            );
            let db = Arc::new(Database::open(&temp.0, id).unwrap());
            db.commit(
                ledger::bootstrap(
                    &db.view().unwrap(),
                    &profile,
                    &h,
                    &input,
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap();
            seed(&db, prepared_payment(row));
            let s = signer();
            let mut config = ae_storage::pool::Config::default();
            config.policy.minimum_miner_gas_price = 1_000_000.into();
            let pool = txpool::Worker::spawn(
                db.clone(),
                profile.clone(),
                config,
                txpool::Settings {
                    origins_interval: None,
                    ..Default::default()
                },
            )
            .unwrap();
            let execution = execution_owner(&db, &profile);
            let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
            let i = Inputs::new(
                durable::Config {
                    caller: env.caller,
                    contract: env.contract,
                    delay: 2,
                },
                s.client(),
                pool.client(),
                snapshots(&db),
            );
            let mut a = Worker::start(
                &mut w,
                db.clone(),
                Default::default(),
                i,
                Arc::new(Wall),
                &execution.stratum_admitter(),
            )
            .unwrap();
            a.request_check();
            let record = accepted(&mut a, &mut w);
            let signed = SignedTransaction::<ContractTransaction>::decode(
                &record.signed.as_ref().unwrap().transaction,
            )
            .unwrap();
            assert_eq!(signed.transaction().encode(), bytes(&row["transaction"]));
            assert!(signed.transaction().gas_limit() <= 6_000_000.into());
            assert_eq!(
                record.request.as_ref().unwrap().prepared.generation(),
                payout::Generation::Bounded(Default::default())
            );
            let mut t = input;
            let before = t.roots();
            let first = aevm::execute_signed_call(
                &signed,
                &mut t,
                &profile,
                &context(row),
                &vm_config(profile.network_id()),
                Limits::default(),
            );
            let r = match first {
                Ok(r) => {
                    defaults += 1;
                    r
                }
                Err(ae_vm_aevm::outer_call::Fault::Runtime(ae_vm_aevm::runtime::Fault::Stop(
                    ae_vm_aevm::execution::Stop::ResourceLimit,
                ))) => {
                    assert_eq!(t.roots(), before);
                    retries += 1;
                    println!(
                        "Bounded payout resource retry protocol {p} case {case}: default stop preserved all roots"
                    );
                    aevm::execute_signed_call(
                        &signed,
                        &mut t,
                        &profile,
                        &context(row),
                        &vm_config(profile.network_id()),
                        corpus_limits(),
                    )
                    .unwrap()
                }
                Err(e) => {
                    panic!("unexpected payout execution error protocol {p} case {case}: {e:?}")
                }
            };
            assert_eq!(r.call.return_type, ReturnType::Ok);
            assert_state(&t, &r, row);
            a.finish(&mut w).unwrap();
            assert!(a.report().unsettled.is_none());
            drop(a);
            drop(w);
            assert!(pool.shutdown().is_ok());
            s.retire();
        }
    }
    assert_eq!(defaults + retries, 12);
    assert!(defaults >= 6 && retries > 0);
    println!(
        "Bounded payout live execution: {defaults} default successes, {retries} explicit budget retries"
    );
}

#[test]
fn split_batches_execute_original_contract_code_and_pay_each_exact_net_entitlement() {
    let mut count = 0;
    for version in [3, 6, 8] {
        let row = gas_find(version, "prepared_two_hundred");
        let mut input = from_snapshot(&row["before"]);
        let mut payment = prepared_payment(row);
        payment.relative.insert(
            ae_codec::api::encode(ae_codec::api::ApiType::AccountPubkey, &[255; 32]).into_bytes(),
            Number::Float((1.0 / 201.0f64).to_bits()),
        );
        for value in payment.relative.values_mut() {
            *value = Number::Float((1.0 / 201.0f64).to_bits());
        }
        let mut env = environment(row);
        let plan = payout::prepare_batches(
            &payment,
            &env,
            Some(
                input
                    .accounts
                    .lookup(env.caller)
                    .unwrap()
                    .unwrap()
                    .balance(),
            ),
            b"ae_mainnet",
            payout::Generation::Bounded(Default::default()),
            payout::Batching {
                input: Default::default(),
                payout: Default::default(),
                maximum_index: 0,
                maximum_batches: 4096,
            },
        )
        .unwrap();
        assert_eq!(plan.payments.len(), 2);
        let mut all = ae_stratum::rewards::Amounts::new();
        for payment in plan.payments {
            let caller_before = input.accounts.lookup(env.caller).unwrap().unwrap();
            env.account_nonce = caller_before.nonce().clone();
            let prepared = payout::prepare_with_generation(
                &payment,
                &env,
                Some(caller_before.balance()),
                b"ae_mainnet",
                payout::Generation::Bounded(Default::default()),
                Default::default(),
            )
            .unwrap();
            let mut expected = Vec::new();
            for (address, amount) in prepared.absolute() {
                assert!(all.insert(address.clone(), amount.clone()).is_none());
                let (_, key) =
                    ae_codec::api::decode(std::str::from_utf8(address).unwrap()).unwrap();
                let key = AccountPublicKey(key.try_into().unwrap());
                let before = input
                    .accounts
                    .lookup(key)
                    .unwrap()
                    .map_or(Unsigned::from(0), |a| a.balance().clone());
                expected.push((key, before, amount.clone()));
            }
            let signed = prepared
                .attach_signature(secret().sign(prepared.signing_bytes()))
                .unwrap();
            let receipt = aevm::execute_signed_call(
                &signed,
                &mut input,
                &NetworkProfile::mainnet(),
                &context(row),
                &vm_config(b"ae_mainnet"),
                corpus_limits(),
            )
            .unwrap();
            assert_eq!(receipt.call.return_type, ReturnType::Ok);
            assert!(payout::confirms_total(
                signed.transaction().amount(),
                &receipt.call.return_value
            ));
            assert!(receipt.call.gas_used <= *signed.transaction().gas());
            for (key, before, amount) in expected {
                let after = input.accounts.lookup(key).unwrap().unwrap();
                assert_eq!(
                    BigInt::from_bytes_be(num_bigint::Sign::Plus, after.balance().as_bytes())
                        - BigInt::from_bytes_be(num_bigint::Sign::Plus, before.as_bytes()),
                    amount
                );
            }
            assert_eq!(
                input.accounts.lookup(env.caller).unwrap().unwrap().nonce(),
                &env.account_nonce.add(&1.into())
            );
            count += 1;
        }
        assert_eq!(all.len(), 201);
    }
    assert_eq!(count, 6);
    println!("Split payout execution: six original-code calls preserve all 201 beneficiaries");
}
