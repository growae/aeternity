use super::*;
use ae_execution::{
    Context as TxContext,
    contracts::{self, fate::Config},
};
use ae_tx::{CanonicalTransaction, ContractTransaction};
use ae_types::AccountPublicKey;
use contract_call_tests::{chain, check, failure, trees, vm_context};

static CREATES: LazyLock<J> = LazyLock::new(|| {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_contract_create.json")).unwrap();
    let pool = rows(&packed["values"]);
    name_tests::expand(&packed["root"], pool, pool.len())
});
pub(super) fn tx_context(row: &J) -> TxContext {
    let mut context = TxContext::transaction(
        50,
        ae_protocol::Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
    );
    context.dry_run = row["dry_run"].as_bool().unwrap();
    context.origin = match row["context"].as_str() {
        Some("aetx_ga") => ae_execution::Origin::Generalized,
        Some("aetx_contract") => ae_execution::Origin::Contract,
        _ => ae_execution::Origin::Transaction,
    };
    if row["payer"].is_string() {
        context.payer = Some(AccountPublicKey(remote_tests::key(&row["payer"])));
    }
    if row["authenticated"] == true {
        let owner = AccountPublicKey(remote_tests::small(1));
        context.ga_authenticated.insert(owner);
    }
    if row["ga_nonce"] == true {
        let owner = AccountPublicKey(remote_tests::small(1));
        context
            .ga_nonces
            .insert(owner, remote_tests::small(42).to_vec());
    }
    context
}
#[test]
fn contract_creations_match_original_transaction_processing() {
    assert_eq!(rows(&CREATES["cases"]).len(), 909);
    let order = otp_order::profile(&CREATES["profile"]);
    let overrides = Overrides::default();
    for row in rows(&CREATES["cases"]) {
        let host = chain(row);
        let mut t = trees(row, &host);
        let context = tx_context(row);
        let vm = vm_context(row);
        let config = Config {
            diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
            context: &vm,
            order: &order,
            overrides: &overrides,
            environment: &host.environment,
            history: host.history.clone(),
            auth_tx: None,
            metering_floor: None,
        };
        let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let checked =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || match contracts::process_create(
                    &tx,
                    &mut t,
                    &context,
                    &config,
                    Limits::default(),
                    10000,
                ) {
                    Ok(receipt) => {
                        check(row, &t, &receipt);
                        for followup in rows(&row["result"]["followups"]) {
                            assert_eq!(publication_tests::snapshot(&t), followup["initial"]);
                            let mut next = t.clone();
                            let mut context = context.clone();
                            context.dry_run = true;
                            let tx = ContractTransaction::decode(&bytes(&followup["transaction"]))
                                .unwrap();
                            let receipt = contracts::process_call(
                                &tx,
                                &mut next,
                                &context,
                                &config,
                                Limits::default(),
                                10000,
                            )
                            .unwrap();
                            check(followup, &next, &receipt);
                        }
                    }
                    Err(e) => {
                        assert_eq!(failure(&e), row["result"]);
                        assert_eq!(publication_tests::snapshot(&t), row["initial"]);
                    }
                },
            ));
        assert!(
            checked.is_ok(),
            "protocol {} VM {} case {} mode {}",
            row["protocol"],
            row["vm"],
            row["case_id"],
            row["mode"]
        );
    }
}

#[test]
fn creation_backend_and_work_failures_publish_no_partial_nonce_deposit_or_contract() {
    use oracle_write_tests::{Faults, fault_host};
    let order = otp_order::profile(&CREATES["profile"]);
    let overrides = Overrides::default();
    for mode in ["return", "store_map", "revert", "exit", "nested_create"] {
        let row = rows(&CREATES["cases"])
            .iter()
            .find(|r| {
                r["protocol"] == 8
                    && r["gas"] == 100000
                    && r["mode"] == mode
                    && r["result"]["kind"] == "ok"
            })
            .unwrap();
        let faults = Faults::default();
        let host = fault_host(chain(row), &faults);
        let base = trees(row, &host);
        let before = format!("{base:?}");
        let vm = vm_context(row);
        let context = tx_context(row);
        let config = Config {
            diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
            context: &vm,
            order: &order,
            overrides: &overrides,
            environment: &host.environment,
            history: host.history.clone(),
            auth_tx: None,
            metering_floor: None,
        };
        let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
        for (limits, steps) in [
            (
                Limits {
                    max_nodes: 0,
                    ..Default::default()
                },
                10000,
            ),
            (
                Limits {
                    max_bytes: 0,
                    ..Default::default()
                },
                10000,
            ),
            (
                Limits {
                    max_depth: 0,
                    ..Default::default()
                },
                10000,
            ),
            (Limits::default(), 0),
        ] {
            let mut t = base.clone();
            assert!(
                contracts::process_create(&tx, &mut t, &context, &config, limits, steps).is_err()
            );
            assert_eq!(format!("{t:?}"), before);
            let receipt =
                contracts::process_create(&tx, &mut t, &context, &config, Limits::default(), 10000)
                    .unwrap();
            check(row, &t, &receipt);
        }
        for reads in [false, true] {
            let counter = if reads {
                &faults.reads_left
            } else {
                &faults.writes_left
            };
            let mut failures = 0;
            for allowance in 0..4096 {
                let mut t = base.clone();
                counter.set(Some(allowance));
                let out = contracts::process_create(
                    &tx,
                    &mut t,
                    &context,
                    &config,
                    Limits::default(),
                    10000,
                );
                counter.set(None);
                match out {
                    Ok(receipt) => {
                        check(row, &t, &receipt);
                        break;
                    }
                    Err(e) => {
                        assert!(
                            format!("{e:?}").contains("Backend"),
                            "{mode} {reads} {allowance}: {e:?}"
                        );
                        assert_eq!(format!("{t:?}"), before);
                        failures += 1;
                        let receipt = contracts::process_create(
                            &tx,
                            &mut t,
                            &context,
                            &config,
                            Limits::default(),
                            10000,
                        )
                        .unwrap();
                        check(row, &t, &receipt);
                    }
                }
                assert!(allowance < 4095, "never reached successful budget");
            }
            assert!(failures > 0);
        }
    }
}

#[test]
fn signed_creation_enforces_network_and_sponsor_domains_and_uses_owner_address() {
    use ae_crypto::Ed25519SecretKey;
    use ae_tx::SignedTransaction;
    use ae_types::{Id, IdTag};
    let secret = Ed25519SecretKey::from_seed(&[39; 32]);
    let owner = secret.public_key();
    let network = ae_protocol::NetworkProfile::mainnet();
    let order = otp_order::profile(&CREATES["profile"]);
    let overrides = Overrides::default();
    for row in rows(&CREATES["cases"])
        .iter()
        .filter(|r| r["case_id"] == 0 && r["gas"] == 100000 && r["result"]["kind"] == "ok")
    {
        let host = chain(row);
        let mut base = trees(row, &host);
        let mut a = ae_state::accounts::Account::new(owner, 1_000_000_000_000_000u64.into());
        a.set_nonce(7.into());
        base.accounts.enter(&a).unwrap();
        let vm = vm_context(row);
        let config = Config {
            diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
            context: &vm,
            order: &order,
            overrides: &overrides,
            environment: &host.environment,
            history: host.history.clone(),
            auth_tx: None,
            metering_floor: None,
        };
        let old = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let mut tx =
            ContractTransaction::new(Id::new(IdTag::Account, owner.0), old.operation().clone())
                .unwrap();
        tx.set_nonce(old.nonce().clone());
        tx.set_fee(old.fee().clone());
        tx.set_gas(old.gas().clone());
        tx.set_gas_price(old.gas_price().clone());
        for sponsored in [false, true] {
            for hashed in [false, true] {
                let mut context = tx_context(row);
                if sponsored {
                    context.payer = Some(AccountPublicKey(remote_tests::small(3)));
                }
                let mut payload = network.signing_prefix(sponsored);
                payload.extend(if hashed {
                    ae_crypto::blake2b_256(&tx.encode()).0.to_vec()
                } else {
                    tx.encode()
                });
                let signed = SignedTransaction::new(tx.clone(), vec![secret.sign(&payload)]);
                let mut t = base.clone();
                assert!(matches!(
                    contracts::execute_signed_create(
                        &signed,
                        &mut t,
                        &ae_protocol::NetworkProfile::uat(),
                        &context,
                        &config,
                        Limits::default(),
                        10000
                    ),
                    Err(contracts::Error::Primitive(ae_execution::Error::Signature(
                        ae_tx::Error::SignatureVerificationFailed
                    )))
                ));
                assert_eq!(format!("{t:?}"), format!("{base:?}"));
                let mut missing_context = context.clone();
                missing_context.dry_run = true;
                assert!(matches!(
                    contracts::execute_signed_create(
                        &SignedTransaction::new(tx.clone(), vec![]),
                        &mut t,
                        &network,
                        &missing_context,
                        &config,
                        Limits::default(),
                        10000
                    ),
                    Err(contracts::Error::Primitive(ae_execution::Error::Signature(
                        ae_tx::Error::SignatureVerificationFailed
                    )))
                ));
                assert_eq!(format!("{t:?}"), format!("{base:?}"));
                let receipt = contracts::execute_signed_create(
                    &signed,
                    &mut t,
                    &network,
                    &context,
                    &config,
                    Limits::default(),
                    10000,
                )
                .unwrap();
                assert_eq!(receipt.transaction_hash, Some(signed.hash()));
                assert_eq!(receipt.call.return_type, ae_state::calls::ReturnType::Ok);
                assert!(receipt.call.return_value.is_empty());
                let key = ae_state::contracts::contract_id(
                    owner,
                    ae_state::contracts::Nonce::Number(tx.nonce()),
                )
                .0;
                assert_eq!(receipt.call.contract, key);
                assert!(t.contracts.is_contract(key).unwrap());
                assert_eq!(receipt.call.caller.value, owner.0);
                assert_eq!(
                    t.accounts.lookup(owner).unwrap().unwrap().nonce(),
                    &8.into()
                );
            }
        }
    }
    for row in rows(&CREATES["cases"])
        .iter()
        .filter(|r| r["case_id"] == 17 && r["gas"] == 100000 && r["result"]["kind"] == "ok")
    {
        let host = chain(row);
        let mut t = trees(row, &host);
        let vm = vm_context(row);
        let context = tx_context(row);
        let config = Config {
            diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
            context: &vm,
            order: &order,
            overrides: &overrides,
            environment: &host.environment,
            history: host.history.clone(),
            auth_tx: None,
            metering_floor: None,
        };
        let signed = SignedTransaction::new(
            ContractTransaction::decode(&bytes(&row["transaction"])).unwrap(),
            vec![],
        );
        let receipt = contracts::execute_signed_create(
            &signed,
            &mut t,
            &network,
            &context,
            &config,
            Limits::default(),
            10000,
        )
        .unwrap();
        check(row, &t, &receipt);
        assert_eq!(receipt.transaction_hash, Some(signed.hash()));
    }
}
