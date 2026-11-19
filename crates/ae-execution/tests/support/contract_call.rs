use super::*;
use ae_execution::{
    Context as TxContext,
    contracts::{self, Event, fate::Config},
};
use ae_state::Trees;
use ae_tx::{CanonicalTransaction, ContractTransaction};
use ae_types::AccountPublicKey;
use serde_json::json;

static CALLS: LazyLock<J> = LazyLock::new(|| {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_contract_call.json")).unwrap();
    let pool = rows(&packed["values"]);
    name_tests::expand(&packed["root"], pool, pool.len())
});
pub(super) fn chain(row: &J) -> ae_execution::fate_chain::remote::State {
    let mut row = row.clone();
    row["pending"] = json!([]);
    publication_tests::chain(&row)
}
pub(super) fn trees<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    host: &ae_execution::fate_chain::remote::State<A, C>,
) -> Trees<A, C> {
    let (calls, channels) =
        publication_tests::other_trees(row, host.contracts.tree().store().clone());
    Trees {
        accounts: host.accounts.clone(),
        contracts: host.contracts.clone(),
        names: host.names.clone(),
        oracles: host.oracles.clone(),
        calls,
        channels,
    }
}
fn tx_context(row: &J) -> TxContext {
    let mut context = TxContext::transaction(
        50,
        ae_protocol::Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
    );
    context.dry_run = row["dry_run"].as_bool().unwrap();
    context.origin = match row["context"].as_str() {
        Some("aetx_contract") => ae_execution::Origin::Contract,
        Some("aetx_ga") => ae_execution::Origin::Generalized,
        _ => ae_execution::Origin::Transaction,
    };
    if row["payer"].is_string() {
        context.payer = Some(AccountPublicKey(remote_tests::key(&row["payer"])));
    }
    if row["authenticated"] == true {
        context
            .ga_authenticated
            .insert(AccountPublicKey(remote_tests::small(4)));
    }
    if row["ga_nonce"] == true {
        context.ga_nonces.insert(
            AccountPublicKey(remote_tests::small(4)),
            remote_tests::small(42).to_vec(),
        );
    }
    context
}
pub(super) fn vm_context(row: &J) -> Context {
    let mut row = row.clone();
    row["auth"] = json!(false);
    row["onchain"] = json!(true);
    context(&row)
}
pub(super) fn check<A: ae_trie::NodeReader, C: ae_trie::NodeReader>(
    row: &J,
    t: &Trees<A, C>,
    receipt: &contracts::Receipt,
) {
    let expected = &row["result"];
    assert_eq!(expected["kind"], "ok");
    assert_eq!(publication_tests::snapshot(t), expected["trees"]);
    let call = &expected["calls"][0];
    assert_eq!(
        hex::encode(receipt.call.serialize().unwrap()),
        call["encoded"]
    );
    assert_eq!(hex::encode(receipt.call.id), call["id"]);
    if expected["used_gas"].is_string() {
        assert_eq!(
            BigInt::from_bytes_be(num_bigint::Sign::Plus, receipt.gas.as_bytes()),
            number(&expected["used_gas"])
        );
    }
    let events: Vec<_> = receipt
        .events
        .iter()
        .map(|e| match e {
            Event::Delta {
                account,
                amount,
                debit,
                info,
            } => {
                let mut n = BigInt::from_bytes_be(num_bigint::Sign::Plus, amount.as_bytes());
                if *debit {
                    n = -n;
                }
                json!({"account":hex::encode(account.0),"amount":n.to_string(),"info":info})
            }
            Event::Internal(e) => {
                let j = remote_tests::event_json(e);
                if j["kind"] == "delta" {
                    json!({"account":j["key"],"amount":j["amount"],"info":j["info"]})
                } else {
                    j
                }
            }
        })
        .collect();
    assert_eq!(json!(events), expected["events"]);
    if !expected["caller_balance"].is_null() {
        let account = t
            .accounts
            .lookup(AccountPublicKey(remote_tests::small(1)))
            .unwrap()
            .unwrap();
        assert_eq!(
            BigInt::from_bytes_be(num_bigint::Sign::Plus, account.balance().as_bytes()).to_string(),
            expected["caller_balance"]
        );
        assert_eq!(
            account.nonce(),
            &expected["caller_nonce"].as_u64().unwrap().into()
        );
    }
}
pub(super) fn failure(e: &contracts::Error) -> J {
    use ae_execution::Error as P;
    use ae_state::names::Error as N;
    let reason = match e {
        contracts::Error::Rejected(reason) => reason,
        contracts::Error::Primitive(P::TooLowFee) => "too_low_fee",
        contracts::Error::Primitive(P::TtlExpired) => "ttl_expired",
        contracts::Error::Primitive(P::InvalidAtProtocol) => "invalid_at_protocol",
        contracts::Error::Primitive(P::AccountNotFound) => "account_not_found",
        contracts::Error::Primitive(P::NotBasicAccount) => "not_a_basic_account",
        contracts::Error::Primitive(P::NotGeneralizedAccount) => "not_a_generalized_account",
        contracts::Error::Primitive(P::GaNonceMustBeZero) => "nonce_in_ga_tx_should_be_zero",
        contracts::Error::Primitive(P::BadGaEnvironment) => "bad_ga_tx_env",
        contracts::Error::Primitive(P::NonceAlreadyUsed) => "tx_nonce_already_used_for_account",
        contracts::Error::Primitive(P::NonceTooHigh) => "tx_nonce_too_high_for_account",
        contracts::Error::Primitive(P::InsufficientFunds) => "insufficient_funds",
        contracts::Error::Primitive(P::Name(N::NameNotFound)) => "name_does_not_exist",
        contracts::Error::Primitive(P::Name(N::NameRevoked)) => "name_revoked",
        contracts::Error::Primitive(P::Name(N::PointerNotFound)) => "pointer_id_not_found",
        contracts::Error::Primitive(P::ReferenceException { class, reason }) => {
            return json!({"kind":"exception","class":class,"reason":reason});
        }
        contracts::Error::Exception(Exception::Native(reason)) => {
            return json!({"kind":"exception","class":"error","reason":reason});
        }
        e => panic!("unexpected {e:?}"),
    };
    json!({"kind":"error","reason":reason})
}
fn transaction(row: &J) -> ContractTransaction {
    let mut tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
    if let Some(stack) = row["stack"].as_array() {
        tx.set_call_context(
            tx.call_origin(),
            stack.iter().map(|n| n.as_u64().unwrap().into()).collect(),
        )
        .unwrap();
    }
    tx
}
#[test]
fn contract_calls_match_original_transaction_processing() {
    assert_eq!(rows(&CALLS["cases"]).len(), 1387);
    let order = otp_order::profile(&CALLS["profile"]);
    let overrides = Overrides::default();
    for row in rows(&CALLS["cases"]) {
        let host = chain(row);
        let mut t = trees(row, &host);
        assert_eq!(publication_tests::snapshot(&t), row["initial"]);
        let context = tx_context(row);
        let vm = vm_context(row);
        let config = Config {
            context: &vm,
            order: &order,
            overrides: &overrides,
            environment: &host.environment,
            history: host.history.clone(),
            auth_tx: None,
            metering_floor: None,
        };
        let tx = transaction(row);
        let label = format!(
            "protocol {} VM {} case {} mode {} gas {} amount {} dry {}",
            row["protocol"],
            row["vm"],
            row["case_id"],
            row["mode"],
            row["gas"],
            row["amount"],
            row["dry_run"]
        );
        let checked =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || match contracts::process_call(
                    &tx,
                    &mut t,
                    &context,
                    &config,
                    Limits::default(),
                    10000,
                ) {
                    Ok(receipt) => check(row, &t, &receipt),
                    Err(e) => {
                        assert_eq!(failure(&e), row["result"]);
                        assert_eq!(publication_tests::snapshot(&t), row["initial"]);
                    }
                },
            ));
        assert!(checked.is_ok(), "{label}");
    }
}

#[test]
fn contract_call_operational_failures_preserve_input_and_allow_retry() {
    use oracle_write_tests::{Faults, fault_host};
    let order = otp_order::profile(&CALLS["profile"]);
    let overrides = Overrides::default();
    for mode in ["return", "store", "revert", "exit"] {
        let row = rows(&CALLS["cases"])
            .iter()
            .find(|r| {
                r["protocol"] == 8
                    && r["mode"] == mode
                    && r["gas"] == 100000
                    && r["amount"] == 0
                    && r["dry_run"] == false
            })
            .unwrap();
        let faults = Faults::default();
        let host = fault_host(chain(row), &faults);
        let base = trees(row, &host);
        let original = format!("{base:?}");
        let context = tx_context(row);
        let vm = vm_context(row);
        let config = Config {
            context: &vm,
            order: &order,
            overrides: &overrides,
            environment: &host.environment,
            history: host.history.clone(),
            auth_tx: None,
            metering_floor: None,
        };
        let tx = transaction(row);
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
                contracts::process_call(&tx, &mut t, &context, &config, limits, steps).is_err()
            );
            assert_eq!(format!("{t:?}"), original);
            let receipt =
                contracts::process_call(&tx, &mut t, &context, &config, Limits::default(), 10000)
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
                let out = contracts::process_call(
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
                        assert_eq!(format!("{t:?}"), original);
                        failures += 1;
                        let receipt = contracts::process_call(
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
                assert!(allowance < 4095, "no successful budget for {mode}");
            }
            assert!(failures > 0);
        }
    }
}

#[test]
fn signed_contract_calls_enforce_domains_before_state_changes() {
    use ae_crypto::Ed25519SecretKey;
    use ae_tx::SignedTransaction;
    use ae_types::{Id, IdTag};
    let secret = Ed25519SecretKey::from_seed(&[73; 32]);
    let caller = secret.public_key();
    let network = ae_protocol::NetworkProfile::mainnet();
    let order = otp_order::profile(&CALLS["profile"]);
    let overrides = Overrides::default();
    for row in rows(&CALLS["cases"]).iter().filter(|r| {
        r["mode"] == "return" && r["gas"] == 100000 && r["amount"] == 0 && r["dry_run"] == false
    }) {
        let host = chain(row);
        let mut base = trees(row, &host);
        let mut account = ae_state::accounts::Account::new(caller, 1_000_000_000_000_000u64.into());
        account.set_nonce(7.into());
        base.accounts.enter(&account).unwrap();
        let vm = vm_context(row);
        let config = Config {
            context: &vm,
            order: &order,
            overrides: &overrides,
            environment: &host.environment,
            history: host.history.clone(),
            auth_tx: None,
            metering_floor: None,
        };
        let old = transaction(row);
        let mut tx =
            ContractTransaction::new(Id::new(IdTag::Account, caller.0), old.operation().clone())
                .unwrap();
        tx.set_nonce(old.nonce().clone());
        tx.set_fee(old.fee().clone());
        tx.set_gas(old.gas().clone());
        tx.set_gas_price(old.gas_price().clone());
        for sponsored in [false, true] {
            let mut context = tx_context(row);
            if sponsored {
                context.payer = Some(AccountPublicKey(remote_tests::small(1)));
            }
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
                        let mut context = context.clone();
                        context.dry_run = dry;
                        let mut t = base.clone();
                        assert!(matches!(
                            contracts::execute_signed_call(
                                &bad,
                                &mut t,
                                &network,
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
                    }
                }
                let mut t = base.clone();
                assert!(matches!(
                    contracts::execute_signed_call(
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
                let receipt = contracts::execute_signed_call(
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
                assert_eq!(receipt.call.caller, Id::new(IdTag::Account, caller.0));
                assert_eq!(
                    receipt.call.return_value,
                    bytes(&row["result"]["calls"][0]["return_value"])
                );
                assert_eq!(
                    receipt.call.gas_used.as_bytes(),
                    number(&row["result"]["calls"][0]["gas_used"])
                        .to_bytes_be()
                        .1
                );
                let a = t.accounts.lookup(caller).unwrap().unwrap();
                assert_eq!(a.nonce(), &8.into());
                let expected = if sponsored {
                    1_000_000_000_000_000u64.into()
                } else {
                    BigInt::from(1_000_000_000_000_000u64)
                        - BigInt::from(1_000_000_000_000u64)
                        - number(&row["result"]["calls"][0]["gas_used"]) * 1_000_000u64
                };
                assert_eq!(
                    BigInt::from_bytes_be(num_bigint::Sign::Plus, a.balance().as_bytes()),
                    expected
                );
            }
        }
    }
    // Already-authenticated GA identities require no leftover signatures.
    for row in rows(&CALLS["cases"]).iter().filter(|r| {
        r["ga_nonce"] == true && r["authenticated"] == true && r["result"]["kind"] == "ok"
    }) {
        let host = chain(row);
        let mut t = trees(row, &host);
        let vm = vm_context(row);
        let context = tx_context(row);
        let config = Config {
            context: &vm,
            order: &order,
            overrides: &overrides,
            environment: &host.environment,
            history: host.history.clone(),
            auth_tx: None,
            metering_floor: None,
        };
        let signed = SignedTransaction::new(transaction(row), vec![]);
        let receipt = contracts::execute_signed_call(
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
