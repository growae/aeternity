use super::*;
use ae_state::calls::Call;
use ae_stratum::{accounting::Payment, proof::Number, rewards};
fn setup() -> (payout::Prepared, Trees, Call, Trees) {
    let caller = AccountPublicKey([7; 32]);
    let recipient = AccountPublicKey([8; 32]);
    let contract = ae_types::ContractPublicKey([9; 32]);
    let p: Payment = rewards::Payment {
        id: (1, 0),
        total: 1_000_000_000_000_000_000u64.into(),
        relative: [(
            api::encode(ApiType::AccountPubkey, &recipient.0).into_bytes(),
            Number::Integer(1.into()),
        )]
        .into(),
    }
    .into();
    let p = payout::prepare_with_generation(
        &p,
        &payout::Environment {
            caller,
            contract,
            account_nonce: 0.into(),
            minimum_miner_gas_price: 1.into(),
            height: 1,
            protocol: ae_protocol::Protocol::Ceres,
        },
        Some(&Unsigned::from(2_000_000_000_000_000_000u64)),
        b"ae_mainnet",
        payout::Generation::Bounded(Default::default()),
        Default::default(),
    )
    .unwrap();
    let mut input = Trees {
        accounts: Default::default(),
        calls: Default::default(),
        contracts: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    input
        .accounts
        .enter(&Account::new(caller, 2_000_000_000_000_000_000u64.into()))
        .unwrap();
    let tx = p.call().transaction();
    let mut call = Call::new(
        tx.actor(),
        tx.nonce().clone(),
        contract.0,
        None,
        1.into(),
        tx.gas_price().clone(),
    );
    call.gas_used = 10.into();
    call.return_value = vec![0; 32];
    let amount = tx.amount().as_bytes();
    call.return_value[32 - amount.len()..].copy_from_slice(amount);
    let mut output = input.clone();
    let cost = big(tx.amount()) + big(tx.fee()) + BigInt::from(10) * big(tx.gas_price());
    let mut paid = Account::new(
        caller,
        uint(&(BigInt::from(2_000_000_000_000_000_000u64) - cost)).unwrap(),
    );
    paid.set_nonce(1.into());
    output.accounts.enter(&paid).unwrap();
    output
        .accounts
        .enter(&Account::new(recipient, tx.amount().clone()))
        .unwrap();
    output.calls.insert(&call).unwrap();
    (p, input, call, output)
}
#[test]
fn successful_return_without_payments_or_with_extra_account_changes_is_rejected() {
    let (p, input, call, output) = setup();
    effects(&p, &input, &call, &Roots::of(&output), Default::default()).unwrap();
    let mut fake = input.clone();
    fake.calls.insert(&call).unwrap();
    assert!(matches!(
        effects(&p, &input, &call, &Roots::of(&fake), Default::default()),
        Err(Error::Effects { .. })
    ));
    let mut extra = output.clone();
    extra
        .accounts
        .enter(&Account::new(AccountPublicKey([55; 32]), 1.into()))
        .unwrap();
    assert!(matches!(
        effects(&p, &input, &call, &Roots::of(&extra), Default::default()),
        Err(Error::Effects { .. })
    ));
    let mut flags = output;
    let mut recipient = flags
        .accounts
        .lookup(AccountPublicKey([8; 32]))
        .unwrap()
        .unwrap();
    recipient.set_non_payable(true);
    flags.accounts.enter(&recipient).unwrap();
    assert!(matches!(
        effects(&p, &input, &call, &Roots::of(&flags), Default::default()),
        Err(Error::Effects { .. })
    ));
}
#[test]
fn payout_receipt_identity_return_and_gas_are_checked_independently() {
    let (p, _, call, _) = setup();
    let c = Context::transaction(1, ae_protocol::Protocol::Ceres);
    receipt::<MemoryStore, MemoryStore>(&p, &c, &call).unwrap();
    for case in 0..11 {
        let mut r = call.clone();
        match case {
            0 => r.id = [0; 32],
            1 => r.caller.value = [0; 32],
            2 => r.nonce = 9.into(),
            3 => r.contract = [0; 32],
            4 => r.call_target = Some(r.contract.to_vec()),
            5 => r.height = 9.into(),
            6 => r.gas_price = 9.into(),
            7 => r.gas_used = p.call().transaction().gas().add(&1.into()),
            8 => r.return_type = ReturnType::Error,
            9 => r.return_type = ReturnType::Revert,
            _ => r.return_value.push(0),
        };
        assert!(receipt::<MemoryStore, MemoryStore>(&p, &c, &r).is_err());
    }
}
#[test]
fn auxiliary_and_nonpayment_roots_are_part_of_the_full_effect_guard() {
    let (p, input, call, output) = setup();
    let base = Roots::of(&output);
    for index in 0..6 {
        let mut roots = base.clone();
        let value = Some(Hash256([44; 32]));
        match index {
            0 => roots.state.contracts = value,
            1 => roots.state.channels = value,
            2 => roots.state.names = value,
            3 => roots.state.oracles = value,
            4 => roots.names = value,
            _ => roots.oracles = value,
        };
        assert!(matches!(
            effects(&p, &input, &call, &roots, Default::default()),
            Err(Error::Effects { .. })
        ));
    }
}
