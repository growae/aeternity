use super::*;
static CREATE: LazyLock<J> = LazyLock::new(|| {
    support::fixture(include_str!(
        "../../fixtures/erlang_aevm_create_preparation.json"
    ))
});
static VECTORS: LazyLock<J> = LazyLock::new(|| {
    support::fixture(include_str!(
        "../../fixtures/erlang_aevm_create_vectors.json"
    ))
});
fn create_context(row: &J) -> Context {
    let mut context = transaction_context(row);
    if row["case_name"] == "payer" || row["case_name"] == "payer_failure" {
        context.payer = Some(AccountPublicKey(Word::from(3).0));
    }
    if matches!(row["case_name"].as_str(), Some("ga_return" | "ga_revert")) {
        let owner = AccountPublicKey(Word::from(9).0);
        context.ga_authenticated.insert(owner);
        context.ga_nonces.insert(owner, Word::from(42).0.to_vec());
    }
    context
}
fn check_creation(row: &J) {
    let mut s = state(row);
    let before = snapshot(&s);
    let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
    let r = aevm::process_create(
        &tx,
        &mut s.trees,
        &create_context(row),
        &expanded_config(),
        Limits::default(),
    );
    let actual = match r {
        Ok(r) => {
            assert_eq!(r.gas, tx.used_gas(&r.call.gas_used));
            s.events = Arc::new(receipt_events(r));
            tuple(vec![atom("ok"), snapshot(&s)])
        }
        Err(outer_call::Fault::Runtime(Fault::Host(aevm::HostError::Transaction(e)))) => {
            let reason = match e {
                contracts::Error::Rejected(s) => s,
                contracts::Error::Primitive(ae_execution::Error::InsufficientFunds) => {
                    "insufficient_funds"
                }
                e => panic!("{e:?}"),
            };
            assert_eq!(snapshot(&s), before);
            tuple(vec![atom("error"), atom(reason)])
        }
        Err(e) => panic!("{e:?}"),
    };
    assert_eq!(
        support::logical(actual),
        support::logical(row["result"]["value"].clone()),
        "protocol={} vm={} case={} dry={}",
        row["protocol"],
        row["vm"],
        row["case_name"],
        row["dry_run"]
    );
}
#[test]
fn historical_aevm_creation_matches_original_initializer_store_and_failure_accounting() {
    let rows = CREATE["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 256);
    for row in rows {
        check_creation(row);
    }
}
#[test]
fn aevm_creation_composes_nested_calls_maps_typed_state_and_historical_metadata() {
    let rows = VECTORS["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 624);
    for row in rows {
        check_creation(row);
    }
}

#[test]
fn created_aevm_contracts_reload_scalar_and_map_state_and_strip_only_historical_init() {
    let mut count = 0;
    for row in VECTORS["runs"].as_array().unwrap() {
        for next in row["followups"].as_array().unwrap() {
            count += 1;
            let mut s = state(next);
            let tx = ContractTransaction::decode(&bytes(&next["transaction"])).unwrap();
            let r = aevm::process_call(
                &tx,
                &mut s.trees,
                &transaction_context(next),
                &expanded_config(),
                Limits::default(),
            )
            .unwrap();
            if next["case_name"] == "probe" {
                assert_eq!(r.call.return_type, ae_state::calls::ReturnType::Ok);
            }
            if next["case_name"] == "init" && next["vm"] == 6 {
                assert_eq!(r.call.return_value, b"unknown_function");
            }
            s.events = Arc::new(receipt_events(r));
            assert_eq!(
                support::logical(tuple(vec![atom("ok"), snapshot(&s)])),
                support::logical(next["result"]["value"].clone()),
                "{}/{} {} {}",
                row["protocol"],
                row["vm"],
                row["case_name"],
                next["case_name"]
            );
        }
    }
    assert_eq!(count, 40);
}

#[test]
fn aevm_creation_limits_and_backend_failures_preserve_all_roots_and_retry() {
    use crate::aevm_spend::invocations::failures::{Control, wrapped};
    use ae_vm_aevm::execution::Stop;
    use std::sync::atomic::Ordering::Relaxed;
    let cfg = expanded_config();
    for case in [
        "map_put",
        "nested_store",
        "parent_revert",
        "wrong_state_after_child",
    ] {
        let row = VECTORS["runs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| {
                r["protocol"] == 4
                    && r["case_name"] == case
                    && r["gas"] == 100000
                    && r["dry_run"] == true
            })
            .unwrap();
        let input = state(row);
        let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let context = create_context(row);
        let a = Arc::new(Control::default());
        let c = Arc::new(Control::default());
        let base = wrapped(&input.trees, &a, &c);
        let before = base.roots();
        let run =
            |t: &mut Trees<_, _>, limits| aevm::process_create(&tx, t, &context, &cfg, limits);
        let check = |t: Trees<_, _>, r| {
            let s = State {
                trees: t,
                onchain: None,
                account: input.account,
                vm_version: input.vm_version,
                network_id: input.network_id.clone(),
                context: input.context.clone(),
                event_context: EventContext::None,
                events: Arc::new(receipt_events(r)),
            };
            assert_eq!(
                support::logical(snapshot(&s)),
                support::logical(row["result"]["value"][1][1].clone()),
                "{case}"
            );
        };
        let mut t = base.clone();
        let r = run(&mut t, Limits::default()).unwrap_or_else(|_| panic!("baseline"));
        check(t, r);
        for (control, count) in [(&a, a.writes.load(Relaxed)), (&c, c.writes.load(Relaxed))] {
            assert!(count > 0);
            for allowed in 0..=count {
                control.remaining.store(allowed, Relaxed);
                let mut t = base.clone();
                match run(&mut t, Limits::default()) {
                    Err(outer_call::Fault::Runtime(Fault::Host(_))) => {
                        assert!(allowed < count);
                        assert_eq!(t.roots(), before);
                    }
                    Ok(r) => {
                        assert_eq!(allowed, count);
                        check(t, r);
                    }
                    Err(_) => panic!("backend failure changed to VM outcome"),
                }
                control.remaining.store(usize::MAX, Relaxed);
                let mut retry = base.clone();
                let r = run(&mut retry, Limits::default()).unwrap_or_else(|_| panic!("retry"));
                check(retry, r);
            }
            control.read_error.store(true, Relaxed);
            let mut t = base.clone();
            assert!(matches!(
                run(&mut t, Limits::default()),
                Err(outer_call::Fault::Runtime(Fault::Host(_)))
            ));
            assert_eq!(t.roots(), before);
            control.read_error.store(false, Relaxed);
        }
        for dimension in 0..4 {
            let mut stopped = false;
            let mut reached = false;
            for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
                let mut limits = Limits::default();
                match dimension {
                    0 => limits.max_work = n,
                    1 => limits.max_bytes = n,
                    2 => limits.max_depth = n,
                    _ => limits.max_memory_words = n,
                }
                let mut t = base.clone();
                match run(&mut t, limits) {
                    Err(outer_call::Fault::Runtime(Fault::Stop(Stop::ResourceLimit))) => {
                        stopped = true;
                        assert_eq!(t.roots(), before);
                    }
                    Ok(r) => {
                        reached = true;
                        check(t, r);
                    }
                    Err(_) => panic!("unexpected budget failure"),
                }
                if reached {
                    break;
                }
            }
            assert!(stopped && reached, "{case}: {dimension}");
        }
        assert_eq!(base.roots(), before);
    }
}

#[test]
fn signed_aevm_creation_checks_network_payer_and_dry_run_signatures() {
    use ae_crypto::Ed25519SecretKey;
    use ae_tx::SignedTransaction;
    let secret = Ed25519SecretKey::from_seed(&[75; 32]);
    let owner = secret.public_key();
    let network = ae_protocol::NetworkProfile::mainnet();
    for row in CREATE["runs"].as_array().unwrap().iter().filter(|r| {
        r["protocol"].as_u64().unwrap() <= 4 && r["case_name"] == "return" && r["dry_run"] == false
    }) {
        let old = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let mut tx =
            ContractTransaction::new(Id::new(IdTag::Account, owner.0), old.operation().clone())
                .unwrap();
        tx.set_nonce(old.nonce().clone());
        tx.set_fee(old.fee().clone());
        tx.set_gas(old.gas().clone());
        tx.set_gas_price(old.gas_price().clone());
        let mut base = trees(&row["before"]);
        let mut account = Account::new(owner, 1_000_000_000_000_000u64.into());
        account.set_nonce(5.into());
        base.accounts.enter(&account).unwrap();
        for sponsored in [false, true] {
            let mut context = create_context(row);
            if sponsored {
                context.payer = Some(AccountPublicKey(Word::from(9).0));
            }
            for hashed in [false, true] {
                let mut payload = network.signing_prefix(sponsored);
                payload.extend(if hashed {
                    ae_crypto::blake2b_256(&tx.encode()).0.to_vec()
                } else {
                    tx.encode()
                });
                let signed = SignedTransaction::new(tx.clone(), vec![secret.sign(&payload)]);
                let cfg = expanded_config();
                for dry in [false, true] {
                    context.dry_run = dry;
                    for bad in [
                        SignedTransaction::new(tx.clone(), vec![]),
                        SignedTransaction::new(tx.clone(), vec![secret.sign(b"wrong domain")]),
                    ] {
                        let mut t = base.clone();
                        assert!(matches!(
                            aevm::execute_signed_create(
                                &bad,
                                &mut t,
                                &network,
                                &context,
                                &cfg,
                                Limits::default()
                            ),
                            Err(outer_call::Fault::Runtime(Fault::Host(
                                aevm::HostError::Transaction(contracts::Error::Primitive(
                                    ae_execution::Error::Signature(_)
                                ))
                            )))
                        ));
                        assert_eq!(t.roots(), base.roots());
                    }
                }
                let mut t = base.clone();
                assert!(matches!(
                    aevm::execute_signed_create(
                        &signed,
                        &mut t,
                        &ae_protocol::NetworkProfile::uat(),
                        &context,
                        &cfg,
                        Limits::default()
                    ),
                    Err(outer_call::Fault::Runtime(Fault::Host(
                        aevm::HostError::Transaction(contracts::Error::Primitive(
                            ae_execution::Error::Signature(_)
                        ))
                    )))
                ));
                assert_eq!(t.roots(), base.roots());
                let r = aevm::execute_signed_create(
                    &signed,
                    &mut t,
                    &network,
                    &context,
                    &cfg,
                    Limits::default(),
                );
                if hashed && context.protocol < Protocol::Lima {
                    assert!(matches!(
                        r,
                        Err(outer_call::Fault::Runtime(Fault::Host(
                            aevm::HostError::Transaction(contracts::Error::Primitive(
                                ae_execution::Error::Signature(_)
                            ))
                        )))
                    ));
                    assert_eq!(t.roots(), base.roots());
                    continue;
                }
                let r = r.unwrap();
                assert_eq!(r.transaction_hash, Some(signed.hash()));
                assert_eq!(r.call.return_type, ae_state::calls::ReturnType::Ok);
                let mut expected = base.clone();
                let unsigned =
                    aevm::process_create(&tx, &mut expected, &context, &cfg, Limits::default())
                        .unwrap();
                assert_eq!(t.roots(), expected.roots());
                assert_eq!(r.call, unsigned.call);
                assert_eq!(r.events, unsigned.events);
            }
        }
    }
}
