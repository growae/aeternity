// aeprimop:ga_meta/ga_set_meta_res, aeternity fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_protocol::Protocol;
use ae_tx::GaMeta;

pub(super) fn prepare<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &GaMeta,
    trees: &Trees<A, C>,
    context: &Context,
    relevant: impl FnOnce() -> Result<(), Error>,
    verify_data: impl FnOnce(&[u8]) -> Result<(), Error>,
) -> Result<PreparedCall<A, C>, Error> {
    if context.protocol < Protocol::Fortuna {
        return Err(crate::Error::GeneralizedAccountsUnavailable.into());
    }
    let mut next = trees.clone();
    let prepaid = tx.fee.add(&product(&tx.gas, &tx.gas_price));
    let mut events = Vec::new();
    let owner_amount = if let Some(payer) = context.payer.filter(|_| prepaid != 0.into()) {
        spend(&mut next.accounts, payer, &prepaid)?;
        delta(&mut events, payer, prepaid, true, "Chain.fee");
        0.into()
    } else {
        prepaid
    };
    let owner = account(&next.accounts, tx.account)?;
    let auth = owner
        .authorization()
        .ok_or(crate::Error::NotGeneralizedAccount)?;
    relevant()?;
    if owner.balance() < &owner_amount {
        return Err(crate::Error::InsufficientFunds.into());
    }
    if auth.contract.tag != IdTag::Contract {
        return Err(Error::Exception(Exception::Native("function_clause")));
    }
    let contract = next
        .contracts
        .record(auth.contract.value)?
        .ok_or(Error::Rejected("contract_does_not_exist"))?;
    if !contract.version.can_call(context.protocol) {
        return Err(Error::Rejected("wrong_vm_version"));
    }
    if tx.abi_version != Unsigned::from(u64::from(contract.version.abi)) {
        return Err(Error::Rejected("wrong_abi_version"));
    }
    verify_data(&auth.function)?;
    if owner_amount != 0.into() {
        spend(&mut next.accounts, tx.account, &owner_amount)?;
        delta(&mut events, tx.account, owner_amount, true, "GA.amount");
    }
    let call = Call::new(
        Id::new(IdTag::Account, tx.account.0),
        0.into(),
        contract.public_key,
        None,
        context.height.into(),
        tx.gas_price.clone(),
    );
    Ok(PreparedCall {
        contract,
        call,
        trees: next,
        rollback: trees.clone(),
        events,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn process<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: &GaMeta,
    size: usize,
    trees: &mut Trees<A, C>,
    context: &Context,
    vm: &fate::Config<'_, O>,
    limits: Limits,
    maximum_steps: usize,
    relevant: impl FnOnce() -> Result<(), Error>,
) -> Result<Receipt, Error> {
    let p = prepare(tx, trees, context, relevant, |hash| {
        let hash = hash
            .try_into()
            .map_err(|_| Error::Exception(Exception::Native("badmatch")))?;
        ae_vm_fate::abi::check_auth_function(&tx.auth_data, hash, limits).map_err(|e| {
            use ae_vm_fate::abi::AuthError;
            match e {
                AuthError::BadData => Error::Rejected("bad_auth_data"),
                AuthError::WrongFunction => Error::Rejected("wrong_auth_function"),
                AuthError::ResourceLimit => Error::ResourceLimit,
            }
        })
    })?;
    let completed = fate::execute(
        Invocation::Auth(tx, size),
        &tx.auth_data,
        &p.contract,
        p.call,
        false,
        p.trees,
        context,
        vm,
        limits,
        maximum_steps,
    )?;
    let (r, next) =
        finish(
            tx,
            size,
            context,
            completed,
            p.events,
            |data| match ae_vm_fate::encoding::decode(data, limits) {
                Ok(ae_vm_fate::value::Value::Boolean(b)) => Ok(b),
                Err(ae_vm_fate::Error::ResourceLimit) => Err(Error::ResourceLimit),
                _ => Ok(false),
            },
        )?;
    *trees = next;
    Ok(r)
}

pub(super) fn finish<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &GaMeta,
    size: usize,
    context: &Context,
    completed: Completion<A, C>,
    mut events: Vec<Event>,
    decode: impl FnOnce(&[u8]) -> Result<bool, Error>,
) -> Result<(Receipt, Trees<A, C>), Error> {
    let mut call = completed.call;
    if call.return_type != ReturnType::Ok || !decode(&call.return_value)? {
        return Err(Error::Rejected("authentication_failed"));
    }
    let mut next = completed.trees;
    events.splice(0..0, completed.events);
    let refund = product(
        &tx.gas
            .checked_sub(&call.gas_used)
            .map_err(|_| Error::InvalidVmGas)?,
        &tx.gas_price,
    );
    let payer = context.payer.unwrap_or(tx.account);
    earn(&mut next.accounts, payer, &refund)?;
    delta(&mut events, payer, refund, false, "Chain.refund");
    call.id = ae_state::calls::ga_call_id(tx.auth_id().0, call.contract).0;
    if next.calls.lookup(&tx.account.0, call.id)?.is_some() {
        return Err(Error::Rejected("auth_call_object_already_exist"));
    }
    next.calls.enter_auth(&call)?;
    let gas = Invocation::Auth(tx, size).used_gas(&call.gas_used);
    Ok((
        Receipt {
            transaction_hash: None,
            call,
            gas,
            events,
        },
        next,
    ))
}

pub(crate) fn fail_inner<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &GaMeta,
    trees: &mut Trees<A, C>,
) -> Result<Call, Error> {
    let owner = account(&trees.accounts, tx.account)?;
    let auth = owner
        .authorization()
        .ok_or(crate::Error::NotGeneralizedAccount)?;
    let id = ae_state::calls::ga_call_id(tx.auth_id().0, auth.contract.value).0;
    let mut call = trees
        .calls
        .lookup(&tx.account.0, id)?
        .ok_or(Error::Exception(Exception::Native("not_found")))?;
    call.return_type = ReturnType::Error;
    call.return_value = b"inner_transaction_failed".to_vec();
    trees.calls.enter_auth(&call)?;
    Ok(call)
}
