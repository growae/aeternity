use super::*;
use ae_stratum::payout::{GasPolicy, Generation, Prepared};
use ae_types::Unsigned;
fn big(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
fn pay(count: u64, gross: BigInt) -> ae_stratum::accounting::Payment {
    ae_stratum::rewards::Payment {
        id: (100, 0),
        total: gross,
        relative: transfers(count)
            .keys()
            .map(|k| {
                (
                    k.clone(),
                    ae_stratum::proof::Number::Float((1.0 / count as f64).to_bits()),
                )
            })
            .collect(),
    }
    .into()
}
#[test]
fn bounded_payout_gas_covers_declared_execution_and_solves_exact_final_fee() {
    for protocol in 1u32..=8 {
        for n in [0, 1, 2, 32, 33, 199, 200] {
            let mut env = environment();
            env.protocol = Protocol::try_from(protocol).unwrap();
            for price in [0, 1, 127, 128, 255, 256, 1_000_000, u64::MAX] {
                env.minimum_miner_gas_price = price.into();
                let c = GasPolicy::default()
                    .call(&transfers(n), &env, b"ae_mainnet", Limits::default())
                    .unwrap();
                let tx = c.transaction();
                assert_eq!(tx.gas(), &Unsigned::from(250 + 22000 * n));
                assert_eq!(c.run_gas(), tx.gas());
                assert_eq!(big(c.run_fee()), big(tx.gas()) * big(tx.gas_price()));
                assert_eq!(big(tx.fee()), big(tx.gas_price()) * big(&tx.fee_gas()));
                assert!(tx.fee() >= &tx.minimum_fee(env.protocol, false));
                assert!(tx.gas_limit() <= 6_000_000.into());
                assert_eq!(tx.encode().len(), tx.wire_size());
            }
        }
    }
}
#[test]
fn public_network_caps_include_overhead_and_cannot_be_overridden_by_local_policy() {
    let mut p = GasPolicy {
        max_transfers: 1000,
        block_gas_limit: 50_000_000,
        ..Default::default()
    };
    let mut env = environment();
    env.protocol = Protocol::Ceres;
    for network in [b"ae_mainnet".as_slice(), b"ae_uat".as_slice()] {
        assert!(matches!(
            p.call(&transfers(226), &env, network, Limits::default()),
            Err(payout::Error::BlockGas {
                maximum: 6_000_000,
                ..
            })
        ));
        assert!(
            p.call(&transfers(225), &env, network, Limits::default())
                .is_ok()
        );
    }
    let c = p
        .call(&transfers(226), &env, b"ae_private", Limits::default())
        .unwrap();
    let exact = c.transaction().gas_limit().to_u64().unwrap();
    p.block_gas_limit = exact;
    assert!(
        p.call(&transfers(226), &env, b"ae_private", Limits::default())
            .is_ok()
    );
    p.block_gas_limit -= 1;
    assert!(matches!(
        p.call(&transfers(226), &env, b"ae_private", Limits::default()),
        Err(payout::Error::BlockGas { .. })
    ));
    assert!(matches!(
        GasPolicy::default().call(&transfers(201), &env, b"ae_mainnet", Limits::default()),
        Err(payout::Error::TransferLimit {
            actual: 201,
            maximum: 200
        })
    ));
}
#[test]
fn bounded_preparation_reserves_maximum_cost_and_preserves_the_captured_policy() {
    let mut env = environment();
    env.protocol = Protocol::Ceres;
    let generation = Generation::Bounded(GasPolicy::default());
    for count in [1, 2, 33, 200] {
        let payment = pay(count, BigInt::from(1_000_000_000_000_000_000u64));
        let balance = Unsigned::from(2_000_000_000_000_000_000u64);
        let plan = payout::prepare_with_generation(
            &payment,
            &env,
            Some(&balance),
            b"ae_mainnet",
            generation,
            Limits::default(),
        )
        .unwrap();
        let tx = plan.call().transaction();
        let total: BigInt = plan.absolute().values().cloned().sum();
        assert_eq!(total, *plan.net());
        assert_eq!(total, big(tx.amount()));
        assert!(total + big(tx.fee()) + big(plan.call().run_fee()) <= payment.total);
        assert_eq!(plan.generation(), generation);
        let raw = plan.encode(Limits::default()).unwrap();
        assert_eq!(Prepared::decode(&raw, Limits::default()).unwrap(), plan);
        let native = payout::prepare(
            &payment,
            &env,
            Some(&balance),
            b"ae_mainnet",
            Limits::default(),
        )
        .unwrap();
        assert_ne!(native.signing_bytes(), plan.signing_bytes());
        assert_eq!(native.generation(), Generation::Native);
        assert_eq!(
            Prepared::decode(
                &native.encode(Limits::default()).unwrap(),
                Limits::default()
            )
            .unwrap(),
            native
        );
    }
}
#[test]
fn bounded_envelopes_reject_changed_gas_inputs_and_invalid_or_exhausted_policy() {
    use ae_trie::Value;
    let payment = pay(33, BigInt::from(1_000_000_000_000_000_000u64));
    let env = environment();
    let p = payout::prepare_with_generation(
        &payment,
        &env,
        Some(&Unsigned::from(2_000_000_000_000_000_000u64)),
        b"ae_mainnet",
        Generation::Bounded(Default::default()),
        Limits::default(),
    )
    .unwrap();
    let raw = p.encode(Limits::default()).unwrap();
    let Value::List(fields) = Value::decode(&raw).unwrap() else {
        panic!()
    };
    assert_eq!(fields.len(), 13);
    for (index, n) in [(0, 251), (1, 22001), (2, 32), (3, 1)] {
        let mut changed = fields.clone();
        let Value::List(policy) = &mut changed[12] else {
            panic!()
        };
        policy[index] = Value::Bytes(Unsigned::from(n as u64).as_bytes().to_vec());
        assert!(Prepared::decode(&Value::List(changed).encode(), Limits::default()).is_err());
    }
    let mut changed = fields;
    changed[0] = Value::Bytes(vec![1]);
    assert!(Prepared::decode(&Value::List(changed).encode(), Limits::default()).is_err());
    assert!(
        Prepared::decode(
            &raw,
            Limits {
                record_bytes: raw.len() - 1,
                ..Default::default()
            }
        )
        .is_err()
    );
    for policy in [
        GasPolicy {
            initial: 0,
            ..Default::default()
        },
        GasPolicy {
            per_transfer: 0,
            ..Default::default()
        },
        GasPolicy {
            max_transfers: 0,
            ..Default::default()
        },
        GasPolicy {
            block_gas_limit: 0,
            ..Default::default()
        },
        GasPolicy {
            initial: u64::MAX,
            ..Default::default()
        },
        GasPolicy {
            per_transfer: u64::MAX,
            ..Default::default()
        },
    ] {
        assert_eq!(
            policy.call(&transfers(1), &env, b"ae_mainnet", Limits::default()),
            Err(payout::Error::GasPolicy)
        );
    }
}
#[test]
fn bounded_amount_words_and_signatures_cannot_wrap_or_cross_generation_policies() {
    let mut env = environment();
    let secret = ae_crypto::Ed25519SecretKey::from_seed(&key(7));
    env.caller = secret.public_key();
    let p = GasPolicy::default();
    let mut values = transfers(1);
    *values.values_mut().next().unwrap() = BigInt::from(-1);
    assert_eq!(
        p.call(&values, &env, b"ae_mainnet", Limits::default()),
        Err(payout::Error::NegativeAmount)
    );
    *values.values_mut().next().unwrap() = BigInt::from(1u8) << 256;
    assert_eq!(
        p.call(&values, &env, b"ae_mainnet", Limits::default()),
        Err(payout::Error::AmountWord)
    );
    let payment = pay(33, BigInt::from(1_000_000_000_000_000_000u64));
    let balance = Unsigned::from(2_000_000_000_000_000_000u64);
    let native = payout::prepare(
        &payment,
        &env,
        Some(&balance),
        b"ae_mainnet",
        Limits::default(),
    )
    .unwrap();
    let bounded = payout::prepare_with_generation(
        &payment,
        &env,
        Some(&balance),
        b"ae_mainnet",
        Generation::Bounded(p),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        bounded.attach_signature(secret.sign(native.signing_bytes())),
        Err(payout::Error::Signature)
    );
    let restored = Prepared::decode(
        &bounded.encode(Limits::default()).unwrap(),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        restored
            .attach_signature(secret.sign(bounded.signing_bytes()))
            .unwrap(),
        bounded
            .attach_signature(secret.sign(bounded.signing_bytes()))
            .unwrap()
    );
}
