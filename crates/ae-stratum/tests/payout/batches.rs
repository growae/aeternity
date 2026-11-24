use super::*;
use ae_stratum::{
    accounting::{Payment, Record},
    payout::{Batching, GasPolicy, Generation, Prepared},
    rewards,
};
use ae_types::Unsigned;
fn settings() -> Batching {
    Batching {
        input: Default::default(),
        payout: Default::default(),
        maximum_index: 10,
        maximum_batches: 4096,
    }
}
fn pay(count: u64) -> Payment {
    rewards::Payment {
        id: (100, 0),
        total: BigInt::from(1_000_000_000_000_000_000u64),
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
fn plan(
    p: &Payment,
    policy: GasPolicy,
    settings: Batching,
) -> Result<payout::Batches, payout::Error> {
    payout::prepare_batches(
        p,
        &environment(),
        Some(&Unsigned::from(2_000_000_000_000_000_000u64)),
        b"ae_mainnet",
        Generation::Bounded(policy),
        settings,
    )
}
#[test]
fn partition_preserves_each_gross_entitlement_and_prepares_every_bounded_batch() {
    for count in [201, 225, 226, 400, 1000, 1001] {
        let p = pay(count);
        let expected = rewards::absolute(&p.relative, &p.total, Default::default()).unwrap();
        let result = plan(&p, Default::default(), settings()).unwrap();
        let mut all = rewards::Amounts::new();
        assert!(result.payments.len() > 1);
        assert_eq!(result.payments[0].id, p.id);
        assert_eq!(
            result.payments.iter().map(|p| &p.total).sum::<BigInt>(),
            p.total
        );
        for (i, p) in result.payments.iter().enumerate() {
            assert_eq!(p.id.1, if i == 0 { 0 } else { 10 + i as u32 });
            let gross = p.gross.as_ref().unwrap();
            assert!(gross.len() <= 200);
            for (k, n) in gross {
                assert!(all.insert(k.clone(), n.clone()).is_none());
            }
            let q = plan(p, Default::default(), settings()).unwrap();
            assert_eq!(q.payments, vec![p.clone()]);
            assert_eq!(
                q.first.absolute().keys().collect::<Vec<_>>(),
                gross.keys().collect::<Vec<_>>()
            );
            for (k, n) in q.first.absolute() {
                assert!(n > &0.into() && n <= &gross[k]);
            }
            assert!(q.first.call().transaction().gas_limit() <= 6_000_000.into());
            let encoded = q.first.encode(Default::default()).unwrap();
            assert_eq!(
                Prepared::decode(&encoded, Default::default()).unwrap(),
                q.first
            );
            let row = Record::Payment(p.clone());
            assert_eq!(
                Record::decode(
                    &row.key(),
                    &row.encode(Default::default()).unwrap(),
                    Default::default()
                )
                .unwrap(),
                row
            );
        }
        assert_eq!(all, expected);
    }
}
#[test]
fn partition_fits_actual_gas_and_refuses_dirty_exhausted_or_unpayable_obligations() {
    let p = pay(201);
    let policy = GasPolicy {
        max_transfers: 1000,
        block_gas_limit: 1_000_000,
        ..Default::default()
    };
    let result = plan(&p, policy, settings()).unwrap();
    assert!(result.payments.len() > 2);
    let initial = plan(&p, Default::default(), settings()).unwrap();
    let remaining = &initial.payments[1];
    let changed = plan(
        remaining,
        GasPolicy {
            max_transfers: 25,
            ..Default::default()
        },
        Batching {
            maximum_index: 20,
            ..settings()
        },
    )
    .unwrap();
    assert_eq!(changed.payments.len(), 4);
    assert_eq!(changed.payments[0].id, remaining.id);
    assert_eq!(changed.payments[1].id.1, 21);
    let recovered: rewards::Amounts = changed
        .payments
        .iter()
        .flat_map(|p| p.gross.clone().unwrap())
        .collect();
    assert_eq!(recovered, *remaining.gross.as_ref().unwrap());
    for p in result.payments {
        assert!(
            plan(&p, policy, settings())
                .unwrap()
                .first
                .call()
                .transaction()
                .gas_limit()
                <= 1_000_000.into()
        );
    }
    for mut s in [
        Batching {
            maximum_index: u32::MAX,
            ..settings()
        },
        Batching {
            maximum_batches: 1,
            ..settings()
        },
        Batching {
            maximum_batches: 4097,
            ..settings()
        },
    ] {
        assert!(plan(&p, Default::default(), s).is_err());
        s.input.entries = 200;
        assert!(plan(&p, Default::default(), s).is_err());
    }
    assert!(
        plan(
            &p,
            GasPolicy {
                block_gas_limit: 1,
                ..Default::default()
            },
            settings()
        )
        .is_err()
    );
    for i in 0..7 {
        let mut p = p.clone();
        match i {
            0 => p.absolute = Some(Default::default()),
            1 => p.tx_hash = Some(ae_types::Hash256::ZERO),
            2 => p.nonce = Some(1.into()),
            3 => p.fee = Some(1.into()),
            4 => p.gas = Some(1.into()),
            5 => p.run_gas = Some(1.into()),
            _ => p.date = Some(ae_stratum::accounting::Date([2026, 1, 1, 0, 0, 0])),
        }
        assert!(plan(&p, Default::default(), settings()).is_err());
    }
    let mut tiny = p.clone();
    tiny.total = 100.into();
    assert!(plan(&tiny, Default::default(), settings()).is_err());
}
#[test]
fn split_payment_and_prepared_versions_bind_exact_allocation_and_refuse_native_reinterpretation() {
    use ae_trie::Value;
    let result = plan(&pay(201), Default::default(), settings()).unwrap();
    let p = &result.payments[0];
    assert_eq!(
        payout::prepare(
            p,
            &environment(),
            Some(&Unsigned::from(2_000_000_000_000_000_000u64)),
            b"ae_mainnet",
            Default::default()
        ),
        Err(payout::Error::ExactAllocationRequiresBounded)
    );
    let row = Record::Payment(p.clone());
    let raw = row.encode(Default::default()).unwrap();
    let Value::List(fields) = Value::decode(&raw).unwrap() else {
        panic!()
    };
    assert_eq!(fields[0], Value::Bytes(vec![2]));
    let mut old = fields.clone();
    old[0] = Value::Bytes(vec![1]);
    assert!(Record::decode(&row.key(), &Value::List(old).encode(), Default::default()).is_err());
    for n in [-1, 0, 1] {
        let mut p = p.clone();
        *p.gross.as_mut().unwrap().values_mut().next().unwrap() = n.into();
        assert!(Record::Payment(p).encode(Default::default()).is_err());
    }
    let raw = result.first.encode(Default::default()).unwrap();
    let Value::List(fields) = Value::decode(&raw).unwrap() else {
        panic!()
    };
    assert_eq!(fields[0], Value::Bytes(vec![3]));
    for version in [1, 2, 4] {
        let mut changed = fields.clone();
        changed[0] = Value::Bytes(vec![version]);
        assert!(Prepared::decode(&Value::List(changed).encode(), Default::default()).is_err());
    }
    let limits = Limits {
        record_bytes: raw.len() - 1,
        ..Default::default()
    };
    assert!(Prepared::decode(&raw, limits).is_err());
    let mut wide = pay(1000);
    wide.gross = Some(rewards::absolute(&wide.relative, &wide.total, Default::default()).unwrap());
    wide.absolute = wide.gross.clone();
    for score in wide.relative.values_mut() {
        *score = ae_stratum::proof::Number::Integer(1.into());
    }
    let wide = Record::Payment(wide);
    let encoded = wide.encode(Default::default()).unwrap();
    assert_eq!(
        Record::decode(&wide.key(), &encoded, Default::default()).unwrap(),
        wide
    );
}
#[test]
fn unsplit_selection_preserves_both_existing_generation_recipes_byte_for_byte() {
    let p = pay(33);
    let env = environment();
    let balance = Unsigned::from(2_000_000_000_000_000_000u64);
    for generation in [Generation::Native, Generation::Bounded(Default::default())] {
        let original = payout::prepare_with_generation(
            &p,
            &env,
            Some(&balance),
            b"ae_mainnet",
            generation,
            Default::default(),
        )
        .unwrap();
        let batch = payout::prepare_batches(
            &p,
            &env,
            Some(&balance),
            b"ae_mainnet",
            generation,
            settings(),
        )
        .unwrap();
        assert_eq!(batch.payments, vec![p.clone()]);
        assert_eq!(batch.first, original);
        assert_eq!(
            batch.first.encode(Default::default()).unwrap(),
            original.encode(Default::default()).unwrap()
        );
    }
}
