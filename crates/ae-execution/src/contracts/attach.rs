// aeprimop:ga_attach/assert_ga_attach_byte_code/ga_attach_success, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use ae_codec::{rlp, sophia::SophiaCode};
use ae_protocol::Protocol;
use ae_state::{
    accounts::Account,
    contracts::{Code as StoredCode, Contract, Nonce},
};
use ae_tx::GaAttachTransaction;

pub fn process_attach<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: &GaAttachTransaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    vm: &fate::Config<'_, O>,
    limits: Limits,
    maximum_steps: usize,
) -> Result<Receipt, Error> {
    let (prepared, code) = prepare(
        tx,
        trees,
        context,
        rlp::DecodeLimits {
            max_input_bytes: limits.max_bytes,
            max_items: limits.max_nodes,
            max_depth: limits.max_depth,
        },
        |v| v.is_fate(),
        |code, data| {
            let (code, _) =
                ae_vm_fate::code::Code::decode_native_bounded(&code.byte_code, vm.order, limits)
                    .map_err(super::create::preparation_error)?;
            match code.function_types(tx.auth_fun()) {
                Some((_, ae_vm_fate::types::Type::Boolean)) => {}
                Some(_) => return Err(Error::Rejected("bad_auth_function_return_type")),
                None => return Err(Error::Rejected("bad_function_hash")),
            }
            match ae_vm_fate::abi::verify_init_calldata(data, limits) {
                Ok(true) => Ok(()),
                Err(ae_vm_fate::Error::ResourceLimit) => Err(Error::ResourceLimit),
                _ => Err(Error::Rejected("bad_init_function")),
            }
        },
    )?;
    super::create::run_prepared(
        Invocation::Attach(tx),
        prepared,
        code,
        trees,
        context,
        vm,
        limits,
        maximum_steps,
    )
}
fn check(tx: &GaAttachTransaction, context: &Context) -> Result<(), Error> {
    if context.origin != Origin::Contract {
        if tx.fee() < &tx.minimum_fee(context.protocol, context.origin == Origin::Generalized) {
            return Err(crate::Error::TooLowFee.into());
        }
        if tx.gas_price() < &context.protocol.minimum_gas_price().into() {
            return Err(Error::Rejected("too_low_gas_price"));
        }
        if tx.ttl() != &0.into() && tx.ttl() < &context.height.into() {
            return Err(crate::Error::TtlExpired.into());
        }
        if !tx.valid_at_protocol(context.protocol) {
            return Err(crate::Error::InvalidAtProtocol.into());
        }
    }
    Ok(())
}
pub(super) fn prepare<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &GaAttachTransaction,
    trees: &Trees<A, C>,
    context: &Context,
    limits: rlp::DecodeLimits,
    accepts: impl FnOnce(ae_protocol::contracts::Version) -> bool,
    verify: impl FnOnce(&SophiaCode, &[u8]) -> Result<(), Error>,
) -> Result<(PreparedCall<A, C>, SophiaCode), Error> {
    check(tx, context)?;
    let owner = tx.owner();
    let mut next = trees.clone();
    let mut changes = BTreeMap::new();
    crate::prepare_sender(&next.accounts, &mut changes, owner, tx.nonce(), context)?;
    next.accounts
        .apply(&changes.into_values().collect::<Vec<_>>())?;
    if context.protocol < Protocol::Fortuna {
        return Err(crate::Error::GeneralizedAccountsUnavailable.into());
    }
    let rollback = next.clone();
    let prepaid = tx.fee().add(&product(tx.gas(), tx.gas_price()));
    let mut events = Vec::new();
    let owner_amount = if let Some(payer) = context.payer.filter(|_| prepaid != 0.into()) {
        spend(&mut next.accounts, payer, &prepaid)?;
        delta(&mut events, payer, prepaid, true, "Chain.fee");
        0.into()
    } else {
        prepaid
    };
    let account = account(&next.accounts, owner)?;
    if account.authorization().is_some() {
        return Err(crate::Error::NotBasicAccount.into());
    }
    if account.balance() < &owner_amount {
        return Err(crate::Error::InsufficientFunds.into());
    }
    if !tx.version_available(context.protocol) {
        return Err(Error::Rejected("illegal_vm_version"));
    }
    let version = tx.version();
    let code = SophiaCode::decode_with_limits(tx.code(), limits).map_err(|e| match e {
        ae_codec::object::ObjectError::Rlp(rlp::DecodeError::ResourceLimit) => Error::ResourceLimit,
        _ => Error::Rejected("bad_sophia_code"),
    })?;
    if !ae_protocol::contracts::serialization_available(
        &u64::from(version.abi).into(),
        &u64::from(code.version).into(),
        context.protocol,
    )
    .map_err(|_| Error::Rejected("bad_sophia_code"))?
    {
        return Err(Error::Rejected("illegal_contract_compiler_version"));
    }
    // Exceptions in the Erlang `of` body escape the deserialization catch.
    verify(&code, tx.call_data())?;
    if owner_amount != 0.into() {
        spend(&mut next.accounts, owner, &owner_amount)?;
        delta(&mut events, owner, owner_amount, true, "Chain.fee");
    }
    let init = Contract::new(
        owner,
        Nonce::Number(tx.nonce()),
        version,
        StoredCode::Inline(tx.code().to_vec()),
        0.into(),
    )
    .map_err(|e| {
        if e == ae_state::contracts::Error::InvalidVersion {
            Error::IllegalContractVersion(version)
        } else {
            e.into()
        }
    })?;
    if !accepts(version) {
        return Err(Error::UnsupportedVm(version.vm));
    }
    let key = AccountPublicKey(init.public_key);
    let existing = next.accounts.lookup(key)?;
    let is_new = existing.is_none();
    let mut account = existing.unwrap_or_else(|| Account::new(key, 0.into()));
    if is_new || context.protocol > Protocol::Lima {
        account.set_flags(u64::from(!code.payable).into());
    }
    next.accounts.enter(&account)?;
    let call = Call::new(
        Id::new(IdTag::Account, owner.0),
        tx.nonce().clone(),
        init.public_key,
        None,
        context.height.into(),
        tx.gas_price().clone(),
    );
    Ok((
        PreparedCall {
            contract: init,
            call,
            trees: next,
            rollback,
            events,
        },
        code,
    ))
}
