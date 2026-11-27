use super::*;
use crate::aevm_spend::invocations::failures::{Control, wrapped};
use ae_vm_aevm::execution::Stop;
use std::sync::atomic::Ordering::Relaxed;
fn row(section: &str, case: &str) -> &'static J {
    VECTORS[section]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["protocol"] == 4
                && r["case_name"] == case
                && if section == "runs" {
                    r["mode"] == "offchain" && r["gas"] == 100000
                } else {
                    r["dry_run"] == false
                }
        })
        .unwrap()
}
#[test]
fn interrupted_aevm_nested_calls_and_rollbacks_never_publish_partial_roots() {
    use std::sync::atomic::AtomicUsize;
    for case in [
        "nested_store",
        "nested_spend",
        "parent_revert",
        "parent_error",
    ] {
        let row = row("transactions", case);
        let initial = state(row);
        let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let context = transaction_context(row);
        let before = initial.trees.roots();
        let run = |limit| {
            let count = Arc::new(AtomicUsize::new(0));
            let c = count.clone();
            let mut cfg = expanded_config();
            cfg.control = Some(ae_types::work::Control::new(move || {
                c.fetch_add(1, Relaxed) >= limit
            }));
            let mut t = initial.trees.clone();
            let result = aevm::process_call(&tx, &mut t, &context, &cfg, Limits::default());
            (result, t, count.load(Relaxed))
        };
        let (result, expected, total) = run(usize::MAX);
        let expected_receipt = result.unwrap();
        assert!(total > 32);
        for limit in [0, 1, 2, 3, 8, 32, total / 2, total - 1] {
            let (result, t, _) = run(limit);
            assert!(
                matches!(
                    result,
                    Err(outer_call::Fault::Runtime(Fault::Stop(Stop::Interrupted)))
                ),
                "{case} checkpoint {limit}: {result:?}"
            );
            assert_eq!(t.roots(), before);
        }
        let (result, retry, _) = run(usize::MAX);
        assert_eq!(retry.roots(), expected.roots());
        assert_eq!(
            result.unwrap().call.serialize().unwrap(),
            expected_receipt.call.serialize().unwrap()
        );
    }
}
#[test]
fn real_aevm_recursive_resource_stops_preserve_inputs_and_allow_retry() {
    let cfg = expanded_config();
    for case in ["two_calls", "nested_spend", "parent_revert"] {
        let row = row("runs", case);
        let s = expanded_state(row);
        let before = snapshot(&s);
        let inner = s.onchain.as_ref().unwrap().roots();
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
                match invoke(row, &s, &cfg, limits) {
                    Err(outer_call::Fault::Runtime(Fault::Stop(Stop::ResourceLimit))) => {
                        stopped = true
                    }
                    Ok(out) => {
                        check_callback(row, Ok(out));
                        reached = true;
                    }
                    Err(_) => panic!("unexpected operational failure"),
                }
                assert_eq!(snapshot(&s), before);
                assert_eq!(s.onchain.as_ref().unwrap().roots(), inner);
                if reached {
                    break;
                }
            }
            assert!(stopped && reached, "{case} dimension {dimension}");
            check_callback(row, invoke(row, &s, &cfg, Limits::default()));
        }
    }
}
#[test]
fn real_aevm_transaction_budget_and_backend_failures_publish_no_partial_roots() {
    let cfg = expanded_config();
    for case in [
        "nested_store",
        "nested_spend",
        "parent_revert",
        "parent_error",
    ] {
        let row = row("transactions", case);
        let input = state(row);
        let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let context = transaction_context(row);
        let a = Arc::new(Control::default());
        let c = Arc::new(Control::default());
        let base = wrapped(&input.trees, &a, &c);
        let before = base.roots();
        let run = |t: &mut Trees<_, _>, limits| aevm::process_call(&tx, t, &context, &cfg, limits);
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
        let receipt = run(&mut t, Limits::default()).unwrap_or_else(|_| panic!("baseline"));
        check(t, receipt);
        let counts = [a.writes.load(Relaxed), c.writes.load(Relaxed)];
        assert!(counts[0] > 1 && counts[1] > 0);
        for (control, count) in [(&a, counts[0]), (&c, counts[1])] {
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
                    Err(_) => panic!("backend failure was converted into VM failure"),
                }
                control.remaining.store(usize::MAX, Relaxed);
                assert_eq!(base.roots(), before);
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
            assert!(stopped && reached);
        }
    }
}
#[test]
fn signed_aevm_calls_validate_network_and_payer_domains_before_execution() {
    use ae_crypto::Ed25519SecretKey;
    use ae_tx::SignedTransaction;
    let secret = Ed25519SecretKey::from_seed(&[74; 32]);
    let caller = secret.public_key();
    let network = ae_protocol::NetworkProfile::mainnet();
    for row in VECTORS["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["case_name"] == "nested_store" && r["dry_run"] == false)
    {
        let mut base = trees(&row["before"]);
        let mut a = Account::new(caller, 1_000_000_000_000_000u64.into());
        a.set_nonce(5.into());
        base.accounts.enter(&a).unwrap();
        let old = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let mut tx =
            ContractTransaction::new(Id::new(IdTag::Account, caller.0), old.operation().clone())
                .unwrap();
        tx.set_nonce(old.nonce().clone());
        tx.set_fee(old.fee().clone());
        tx.set_gas(old.gas().clone());
        tx.set_gas_price(old.gas_price().clone());
        for sponsored in [false, true] {
            let mut context = transaction_context(row);
            if sponsored {
                context.payer = Some(AccountPublicKey(Word::from(9).0));
            }
            let cfg = expanded_config();
            for hashed in [false, true] {
                let mut payload = network.signing_prefix(sponsored);
                payload.extend(if hashed {
                    ae_crypto::blake2b_256(&tx.encode()).0.to_vec()
                } else {
                    tx.encode()
                });
                let signed = SignedTransaction::new(tx.clone(), vec![secret.sign(&payload)]);
                for bad in [
                    SignedTransaction::new(tx.clone(), vec![]),
                    SignedTransaction::new(tx.clone(), vec![secret.sign(b"wrong domain")]),
                ] {
                    for dry in [false, true] {
                        let mut c = context.clone();
                        c.dry_run = dry;
                        let mut t = base.clone();
                        let r = aevm::execute_signed_call(
                            &bad,
                            &mut t,
                            &network,
                            &c,
                            &cfg,
                            Limits::default(),
                        );
                        assert!(matches!(
                            r,
                            Err(outer_call::Fault::Runtime(Fault::Host(
                                aevm::HostError::Transaction(contracts::Error::Primitive(
                                    ae_execution::Error::Signature(
                                        ae_tx::Error::SignatureVerificationFailed
                                    )
                                ))
                            )))
                        ));
                        assert_eq!(t.roots(), base.roots());
                    }
                }
                let mut t = base.clone();
                assert!(matches!(
                    aevm::execute_signed_call(
                        &signed,
                        &mut t,
                        &ae_protocol::NetworkProfile::uat(),
                        &context,
                        &cfg,
                        Limits::default()
                    ),
                    Err(outer_call::Fault::Runtime(Fault::Host(
                        aevm::HostError::Transaction(contracts::Error::Primitive(
                            ae_execution::Error::Signature(
                                ae_tx::Error::SignatureVerificationFailed
                            )
                        ))
                    )))
                ));
                assert_eq!(t.roots(), base.roots());
                let r = aevm::execute_signed_call(
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
                                ae_execution::Error::Signature(
                                    ae_tx::Error::SignatureVerificationFailed
                                )
                            ))
                        )))
                    ));
                    assert_eq!(t.roots(), base.roots());
                    continue;
                }
                let r = r.unwrap();
                assert_eq!(r.transaction_hash, Some(signed.hash()));
                assert_eq!(r.call.return_type, ae_state::calls::ReturnType::Ok);
                assert_eq!(r.call.caller, Id::new(IdTag::Account, caller.0));
                let mut expected = base.clone();
                let unsigned =
                    aevm::process_call(&tx, &mut expected, &context, &cfg, Limits::default())
                        .unwrap();
                assert_eq!(t.roots(), expected.roots());
                assert_eq!(r.events, unsigned.events);
                assert_eq!(r.call, unsigned.call);
            }
        }
    }
}
