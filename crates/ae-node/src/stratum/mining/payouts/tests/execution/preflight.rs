use super::*;
use crate::stratum::mining::payouts::preflight::{self, Error, ReceiptFailure};
use ae_chain::repository::Roots;
fn limits() -> preflight::Limits {
    preflight::Limits {
        vm: corpus_limits(),
        ..Default::default()
    }
}
#[test]
fn preflight_matches_every_captured_native_and_bounded_prepared_outcome_without_changing_input() {
    let mut count = 0;
    for (data, generation) in [
        (&*DATA, payout::Generation::Native),
        (
            &*gas::GAS_DATA,
            payout::Generation::Bounded(Default::default()),
        ),
    ] {
        for row in data["runs"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| !r["prepared"].is_null())
        {
            let before = from_snapshot(&row["before"]);
            let roots = Roots::of(&before);
            let prepared = payout::prepare_with_generation(
                &prepared_payment(row),
                &environment(row),
                Some(&unsigned("1000000000000000000000000000")),
                b"ae_mainnet",
                generation,
                Default::default(),
            )
            .unwrap();
            let checked = preflight::evaluate(
                &prepared,
                &before,
                &context(row),
                &vm_config(b"ae_mainnet"),
                limits(),
            )
            .unwrap();
            let expected = from_snapshot(&row["result"][1][1]);
            assert_eq!(checked.before(), &roots);
            assert_eq!(checked.after(), &Roots::of(&expected));
            assert_eq!(
                checked.preparation_hash(),
                ae_crypto::blake2b_256(&prepared.encode(Default::default()).unwrap())
            );
            assert_eq!(
                expected
                    .calls
                    .lookup(&checked.call().contract, checked.call().id)
                    .unwrap()
                    .unwrap(),
                *checked.call()
            );
            assert_eq!(Roots::of(&before), roots);
            assert_eq!(
                pairs(before.accounts.tree()),
                *field(&row["before"], "accounts")
            );
            count += 1;
        }
    }
    assert_eq!(count, 36);
    println!(
        "Payout preflight: 36 captured original prepared outcomes match with unchanged auxiliary roots"
    );
}
#[test]
fn preflight_preserves_exact_inputs_on_missing_contract_wrong_nonce_funds_and_resource_stops() {
    let row = gas::gas_find(8, "prepared_two");
    let original = from_snapshot(&row["before"]);
    let env = environment(row);
    let prepared = payout::prepare_with_generation(
        &prepared_payment(row),
        &env,
        Some(&unsigned("1000000000000000000000000000")),
        b"ae_mainnet",
        payout::Generation::Bounded(Default::default()),
        Default::default(),
    )
    .unwrap();
    for case in 0..5 {
        let mut input = original.clone();
        let mut limits = limits();
        match case {
            0 => input.contracts = Default::default(),
            1 => {
                let mut a = input.accounts.lookup(env.caller).unwrap().unwrap();
                a.set_nonce(999.into());
                input.accounts.enter(&a).unwrap();
            }
            2 => input
                .accounts
                .enter(&Account::new(env.caller, 1.into()))
                .unwrap(),
            3 => limits.vm.max_work = 0,
            _ => limits.overlays.bytes = 0,
        }
        let before = Roots::of(&input);
        assert!(matches!(
            preflight::evaluate(
                &prepared,
                &input,
                &context(row),
                &vm_config(b"ae_mainnet"),
                limits
            ),
            Err(Error::Execution(_))
        ));
        assert_eq!(Roots::of(&input), before);
    }
    let mut low = env.clone();
    low.minimum_miner_gas_price = 1_000_000.into();
    let policy = payout::GasPolicy {
        initial: 1,
        per_transfer: 1,
        ..Default::default()
    };
    let low = payout::prepare_with_generation(
        &prepared_payment(row),
        &low,
        Some(&unsigned("1000000000000000000000000000")),
        b"ae_mainnet",
        payout::Generation::Bounded(policy),
        Default::default(),
    )
    .unwrap();
    assert!(matches!(
        preflight::evaluate(
            &low,
            &original,
            &context(row),
            &vm_config(b"ae_mainnet"),
            limits()
        ),
        Err(Error::Receipt(ReceiptFailure::Return(ReturnType::Error)))
    ));
}
#[test]
fn preflight_uses_current_execution_context_but_rejects_authorization_and_network_overrides() {
    let row = gas::gas_find(8, "prepared_one");
    let input = from_snapshot(&row["before"]);
    let env = environment(row);
    let p = payout::prepare_with_generation(
        &prepared_payment(row),
        &env,
        Some(&unsigned("1000000000000000000000000000")),
        b"ae_mainnet",
        payout::Generation::Bounded(Default::default()),
        Default::default(),
    )
    .unwrap();
    let mut current = context(row);
    current.height = 51;
    assert_eq!(
        preflight::evaluate(&p, &input, &current, &vm_config(b"ae_mainnet"), limits())
            .unwrap()
            .call()
            .height,
        51.into()
    );
    for case in 0..8 {
        let mut c = context(row);
        let mut vm = vm_config(b"ae_mainnet");
        match case {
            0 => c.dry_run = true,
            1 => c.origin = ae_execution::Origin::Contract,
            2 => c.payer = Some(env.caller),
            3 => {
                c.ga_authenticated.insert(env.caller);
            }
            4 => {
                c.ga_nonces.insert(env.caller, vec![0; 32]);
            }
            5 => c.ga_transaction_hash = Some(Hash256::ZERO),
            6 => vm.network_id = Arc::from(b"ae_other".as_slice()),
            _ => vm.environment.auth_tx_hash = Some(vec![0; 32]),
        }
        assert!(matches!(
            preflight::evaluate(&p, &input, &c, &vm, limits()),
            Err(Error::Context)
        ));
    }
    let mut vm = vm_config(b"ae_mainnet");
    vm.environment.block_gas_limit = 1.into();
    assert!(matches!(
        preflight::evaluate(&p, &input, &context(row), &vm, limits()),
        Err(Error::BlockGas)
    ));
    let mut budget = limits();
    budget.payout.record_bytes = 1;
    assert!(matches!(
        preflight::evaluate(&p, &input, &context(row), &vm_config(b"ae_mainnet"), budget),
        Err(Error::Input(_))
    ));
}
#[test]
fn preflight_validates_split_and_role_overlapping_payouts_with_read_only_account_sources() {
    #[derive(Clone, Debug)]
    struct ReadOnly(MemoryStore);
    impl NodeReader for ReadOnly {
        fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            self.0.node(h)
        }
    }
    let row = gas::gas_find(8, "prepared_two_hundred");
    let input = from_snapshot(&row["before"]);
    let env = environment(row);
    let mut p = prepared_payment(row);
    p.relative.insert(
        ae_codec::api::encode(ae_codec::api::ApiType::AccountPubkey, &env.caller.0).into_bytes(),
        Number::Float((1.0 / 202.0f64).to_bits()),
    );
    p.relative.insert(
        ae_codec::api::encode(ae_codec::api::ApiType::AccountPubkey, &env.contract.0).into_bytes(),
        Number::Float((1.0 / 202.0f64).to_bits()),
    );
    let weight = Number::Float((1.0 / p.relative.len() as f64).to_bits());
    for v in p.relative.values_mut() {
        *v = weight.clone();
    }
    let batches = payout::prepare_batches(
        &p,
        &env,
        Some(&unsigned("1000000000000000000000000000")),
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
    let read = Trees {
        accounts: Accounts::from_root(
            input.accounts.root_hash(),
            ReadOnly(input.accounts.tree().store().clone()),
        )
        .unwrap(),
        calls: input.calls.clone(),
        contracts: input.contracts.clone(),
        channels: input.channels.clone(),
        names: input.names.clone(),
        oracles: input.oracles.clone(),
    };
    for p in batches.payments {
        let prepared = payout::prepare_with_generation(
            &p,
            &env,
            Some(&unsigned("1000000000000000000000000000")),
            b"ae_mainnet",
            payout::Generation::Bounded(Default::default()),
            Default::default(),
        )
        .unwrap();
        let before = Roots::of(&read);
        preflight::evaluate(
            &prepared,
            &read,
            &context(row),
            &vm_config(b"ae_mainnet"),
            limits(),
        )
        .unwrap();
        assert_eq!(Roots::of(&read), before);
    }
}
