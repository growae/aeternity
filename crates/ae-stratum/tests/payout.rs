use ae_codec::api::{self, ApiType};
use ae_protocol::Protocol;
use ae_stratum::{
    payout::{self, Environment, Limits},
    rewards::Amounts,
};
use ae_tx::CanonicalTransaction;
use ae_types::{AccountPublicKey, ContractPublicKey};
use num_bigint::{BigInt, Sign};
fn decimal(n: &ae_types::Unsigned) -> String {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes()).to_string()
}
fn key(n: u64) -> [u8; 32] {
    let mut k = [0; 32];
    k[24..].copy_from_slice(&n.to_be_bytes());
    k
}
fn transfers(count: u64) -> Amounts {
    (1..=count)
        .map(|n| {
            (
                api::encode(ApiType::AccountPubkey, &key(n)).into_bytes(),
                BigInt::from(n) * BigInt::from(1_000_000_000_000_000_000u64) + n,
            )
        })
        .collect()
}
fn environment() -> Environment {
    Environment {
        caller: AccountPublicKey(key(17)),
        contract: ContractPublicKey(key(29)),
        account_nonce: 0.into(),
        minimum_miner_gas_price: 0.into(),
        height: 100000,
        protocol: Protocol::Roma,
    }
}
#[test]
fn original_payout_arguments_preserve_all_112_transaction_encodings_and_gas_estimates() {
    let rows: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/erlang_stratum_payout_args.json")).unwrap();
    for row in rows.as_array().unwrap() {
        let input = &row["input"];
        let mut env = environment();
        env.account_nonce = input["nonce"].as_u64().unwrap().into();
        env.minimum_miner_gas_price = input["pool_minimum"].as_u64().unwrap().into();
        env.protocol = Protocol::try_from(input["protocol"].as_u64().unwrap() as u32).unwrap();
        env.height = input["height"].as_u64().unwrap();
        let call = payout::call(
            &transfers(input["count"].as_u64().unwrap()),
            &env,
            Limits::default(),
        )
        .unwrap();
        assert_eq!(
            hex::encode(call.transaction().encode()),
            row["wire"].as_str().unwrap(),
            "{}",
            input["name"]
        );
        assert_eq!(decimal(call.run_gas()), row["run_gas"].as_str().unwrap());
        assert_eq!(decimal(call.run_fee()), row["run_fee"].as_str().unwrap());
        assert_eq!(
            decimal(&call.transaction().fee_gas()),
            row["final_fee_gas"].as_str().unwrap()
        );
        assert_ne!(call.transaction().gas(), &call.transaction().fee_gas());
    }
}
#[test]
fn malformed_accounts_and_operational_limits_do_not_return_partially_prepared_calls() {
    let original = transfers(33);
    let env = environment();
    let limits = Limits::default();
    for limits in [
        Limits {
            transfers: ae_stratum::rewards::Limits {
                entries: 32,
                ..limits.transfers
            },
            ..limits
        },
        Limits {
            transaction_bytes: 10,
            ..limits
        },
        Limits {
            heap: ae_vm_aevm::heap::Limits {
                max_nodes: 2,
                ..limits.heap
            },
            ..limits
        },
    ] {
        assert!(payout::call(&original, &env, limits).is_err());
    }
    for address in [
        vec![255],
        b"ak_invalid".to_vec(),
        api::encode(ApiType::ContractPubkey, &key(1)).into_bytes(),
    ] {
        let map = [(address, 1.into())].into();
        assert_eq!(
            payout::call(&map, &env, limits),
            Err(payout::Error::Address)
        );
    }
    assert_eq!(original, transfers(33));
    assert!(payout::call(&original, &env, limits).is_ok());
}
fn send_rows() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/erlang_stratum_payout_send.json")).unwrap()
}
fn payment(row: &serde_json::Value) -> ae_stratum::accounting::Payment {
    let mut payment: ae_stratum::accounting::Payment = ae_stratum::rewards::Payment {
        id: (row["input"]["height"].as_u64().unwrap(), 0),
        total: row["total"].as_str().unwrap().parse().unwrap(),
        relative: row["relative"]["map"]
            .as_array()
            .unwrap()
            .iter()
            .map(|pair| {
                (
                    hex::decode(pair[0]["binary"].as_str().unwrap()).unwrap(),
                    ae_stratum::proof::Number::Float(
                        u64::from_str_radix(pair[1]["float"].as_str().unwrap(), 16).unwrap(),
                    ),
                )
            })
            .collect(),
    }
    .into();
    if row["input"]["mode"] == "already_sent" {
        payment.tx_hash = Some(ae_types::Hash256(key(99)));
    }
    payment
}
fn send_env(row: &serde_json::Value) -> Environment {
    let mut env = environment();
    env.caller = AccountPublicKey(
        hex::decode(row["caller"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap(),
    );
    env.account_nonce = row["input"]["nonce"].as_u64().unwrap().into();
    env.minimum_miner_gas_price = row["input"]["pool_minimum"].as_u64().unwrap().into();
    env.protocol = Protocol::try_from(row["input"]["protocol"].as_u64().unwrap() as u32).unwrap();
    env.height = row["input"]["height"].as_u64().unwrap();
    env
}
fn amounts(row: &serde_json::Value) -> Amounts {
    row["map"]
        .as_array()
        .unwrap()
        .iter()
        .map(|pair| {
            (
                hex::decode(pair[0]["binary"].as_str().unwrap()).unwrap(),
                pair[1]["integer"].as_str().unwrap().parse().unwrap(),
            )
        })
        .collect()
}
fn int(n: impl ToString) -> serde_json::Value {
    serde_json::json!({"integer":n.to_string()})
}
fn atom(n: &str) -> serde_json::Value {
    serde_json::json!({"atom":n})
}
fn tuple(n: Vec<serde_json::Value>) -> serde_json::Value {
    serde_json::json!({"tuple":n})
}
#[test]
fn original_complete_send_calculations_preserve_two_pass_allocations_signatures_and_rejections() {
    let rows = send_rows();
    let secret = ae_crypto::Ed25519SecretKey::from_seed(&key(7));
    let mut success = 0;
    let mut rejected = 0;
    for row in rows.as_array().unwrap() {
        let payment = payment(row);
        let env = send_env(row);
        assert_eq!(secret.public_key(), env.caller);
        let balance = row["balance"]
            .as_str()
            .map(|s| ae_types::Unsigned::from_decimal(s).unwrap());
        let result = payout::prepare(
            &payment,
            &env,
            balance.as_ref(),
            row["input"]["network"].as_str().unwrap().as_bytes(),
            Limits::default(),
        );
        let expected = &row["result"];
        if expected["signed"].is_string() {
            let plan = result.unwrap();
            success += 1;
            assert_eq!(plan.payment_id(), payment.id);
            assert_eq!(plan.gross(), &payment.total);
            assert_eq!(plan.net().to_string(), expected["net"].as_str().unwrap());
            assert_eq!(plan.absolute(), &amounts(&expected["absolute"]));
            assert_eq!(
                hex::encode(plan.call().transaction().encode()),
                expected["unsigned"].as_str().unwrap(),
                "{}",
                row["input"]["name"]
            );
            assert_eq!(
                decimal(plan.call().run_gas()),
                expected["run_gas"].as_str().unwrap()
            );
            assert_eq!(
                decimal(plan.call().run_fee()),
                expected["run_fee"].as_str().unwrap()
            );
            let signature = secret.sign(plan.signing_bytes());
            assert_eq!(
                hex::encode(signature.0),
                expected["signature"].as_str().unwrap()
            );
            let signed = plan.attach_signature(signature).unwrap();
            assert_eq!(
                hex::encode(signed.encode()),
                expected["signed"].as_str().unwrap()
            );
            assert_eq!(
                hex::encode(signed.hash().0),
                expected["hash"].as_str().unwrap()
            );
            if row["input"]["mode"] == "one_net" && row["input"]["count"].as_u64().unwrap() > 1 {
                assert_eq!(plan.absolute().len(), 1);
                assert!(plan.estimate().run_gas() > plan.call().run_gas());
            }
        } else {
            let error = result.unwrap_err();
            rejected += 1;
            let reason = match error {
                payout::Error::NonpositiveNet {
                    gross,
                    fee,
                    run_fee,
                    net,
                } => tuple(vec![
                    atom("negative_net_amount"),
                    int(net),
                    tuple(vec![int(gross), int(decimal(&fee)), int(decimal(&run_fee))]),
                ]),
                payout::Error::InsufficientBalance { gross, balance } => tuple(vec![
                    atom("caller_insufficient_funds"),
                    int(decimal(&balance)),
                    int(gross),
                ]),
                payout::Error::AccountUnavailable => tuple(vec![atom("badmatch"), atom("none")]),
                payout::Error::AlreadySent => atom("function_clause"),
                payout::Error::Rewards(ae_stratum::rewards::Error::BadMatch) => tuple(vec![
                    atom("badmatch"),
                    tuple(vec![
                        serde_json::json!({"map":[]}),
                        int(&payment.total),
                        int(0),
                    ]),
                ]),
                payout::Error::Address => {
                    if row["input"]["mode"] == "bad_address" {
                        atom("badarith")
                    } else {
                        atom("function_clause")
                    }
                }
                other => panic!("unexpected error {other:?}: {}", row["input"]["name"]),
            };
            assert_eq!(
                tuple(vec![atom("error"), reason]),
                expected["error"],
                "{}",
                row["input"]["name"]
            );
        }
    }
    assert_eq!((success, rejected), (48, 45));
}
#[test]
fn signatures_require_the_exact_plain_network_prefixed_bytes_and_original_caller() {
    let rows = send_rows();
    let row = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["input"]["protocol"] == 6 && r["input"]["mode"] == "large")
        .unwrap();
    let payment = payment(row);
    let env = send_env(row);
    let balance = ae_types::Unsigned::from_decimal(row["balance"].as_str().unwrap()).unwrap();
    let secret = ae_crypto::Ed25519SecretKey::from_seed(&key(7));
    let native = payout::prepare(
        &payment,
        &env,
        Some(&balance),
        row["input"]["network"].as_str().unwrap().as_bytes(),
        Limits::default(),
    )
    .unwrap();
    let other = payout::prepare(
        &payment,
        &env,
        Some(&balance),
        b"ae_other_network",
        Limits::default(),
    )
    .unwrap();
    let signature = secret.sign(native.signing_bytes());
    assert!(native.attach_signature(signature).is_ok());
    assert_eq!(
        other.attach_signature(signature),
        Err(payout::Error::Signature)
    );
    assert_eq!(
        native.attach_signature(secret.sign(native.signing_hash().as_ref())),
        Err(payout::Error::Signature)
    );
    assert_eq!(
        native.attach_signature(
            ae_crypto::Ed25519SecretKey::from_seed(&key(8)).sign(native.signing_bytes())
        ),
        Err(payout::Error::Signature)
    );
    let mut altered = native.signing_bytes().to_vec();
    altered[0] ^= 1;
    assert_eq!(
        native.attach_signature(secret.sign(&altered)),
        Err(payout::Error::Signature)
    );
}
#[test]
fn prepared_envelopes_restore_exact_signing_requests_without_new_chain_or_account_inputs() {
    let rows = send_rows();
    let secret = ae_crypto::Ed25519SecretKey::from_seed(&key(7));
    for row in rows
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["result"]["signed"].is_string())
    {
        let mut payment = payment(row);
        let mut environment = send_env(row);
        let balance = ae_types::Unsigned::from_decimal(row["balance"].as_str().unwrap()).unwrap();
        let prepared = payout::prepare(
            &payment,
            &environment,
            Some(&balance),
            row["input"]["network"].as_str().unwrap().as_bytes(),
            Limits::default(),
        )
        .unwrap();
        let saved = prepared.encode(Limits::default()).unwrap();
        // A future chain observation must not change this already prepared call.
        payment.total += 1;
        payment.relative.clear();
        environment.account_nonce = 999999.into();
        environment.protocol = Protocol::Salus;
        let restored = payout::Prepared::decode(&saved, Limits::default()).unwrap();
        assert_eq!(restored, prepared);
        assert_eq!(restored.encode(Limits::default()).unwrap(), saved);
        let signed = restored
            .attach_signature(secret.sign(restored.signing_bytes()))
            .unwrap();
        assert_eq!(
            hex::encode(signed.encode()),
            row["result"]["signed"].as_str().unwrap()
        );
        assert_ne!(&restored.request().payment.total, &payment.total);
        assert_ne!(
            restored.request().environment.account_nonce,
            environment.account_nonce
        );
    }
}
#[test]
fn prepared_envelopes_reject_mismatched_payloads_corruption_versions_and_resource_exhaustion() {
    use ae_trie::Value;
    let rows = send_rows();
    let row = &rows[0];
    let payment = payment(row);
    let env = send_env(row);
    let balance = ae_types::Unsigned::from_decimal(row["balance"].as_str().unwrap()).unwrap();
    let prepared = payout::prepare(
        &payment,
        &env,
        Some(&balance),
        b"ae_mainnet",
        Limits::default(),
    )
    .unwrap();
    let saved = prepared.encode(Limits::default()).unwrap();
    let Value::List(fields) = Value::decode(&saved).unwrap() else {
        panic!()
    };
    for index in [0, 1, 2, 5, 6, 7, 8, 9, 10, 11] {
        let mut changed = fields.clone();
        let Value::Bytes(bytes) = &mut changed[index] else {
            panic!()
        };
        bytes[0] ^= 1;
        assert!(
            payout::Prepared::decode(&Value::List(changed).encode(), Limits::default()).is_err(),
            "field {index}"
        );
    }
    for index in [0, 1, 2, 3, 5] {
        let mut changed = fields.clone();
        let Value::List(env) = &mut changed[3] else {
            panic!()
        };
        let Value::Bytes(bytes) = &mut env[index] else {
            panic!()
        };
        bytes[0] ^= if index == 3 { 2 } else { 1 };
        // Protocol 1 -> 0 is invalid; nonce, price and identifiers bind the call.
        assert!(
            payout::Prepared::decode(&Value::List(changed).encode(), Limits::default()).is_err(),
            "environment {index}"
        );
    }
    let limits = Limits {
        record_bytes: saved.len() - 1,
        ..Default::default()
    };
    assert_eq!(
        payout::Prepared::decode(&saved, limits),
        Err(payout::Error::Limit)
    );
    assert_eq!(prepared.encode(limits), Err(payout::Error::Limit));
    let mut extended = saved.clone();
    extended.push(0);
    assert!(payout::Prepared::decode(&extended, Limits::default()).is_err());
    assert_eq!(
        payout::Prepared::decode(&saved, Limits::default())
            .unwrap()
            .signing_bytes(),
        prepared.signing_bytes()
    );
}
