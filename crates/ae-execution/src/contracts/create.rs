// aeprimop:contract_create/prepare_init_call, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use ae_codec::{rlp, sophia::SophiaCode};
use ae_protocol::Protocol;
use ae_state::{
    accounts::Account,
    contracts::{Code as StoredCode, Contract, Nonce},
};
use ae_vm_fate::{
    abi,
    code::{Code, CodeError},
    encoding,
    value::Value,
};

#[allow(clippy::too_many_arguments)]
pub fn execute_signed_create<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    signed: &ae_tx::SignedTransaction<ContractTransaction>,
    trees: &mut Trees<A, C>,
    network: &ae_protocol::NetworkProfile,
    context: &Context,
    vm: &fate::Config<'_, O>,
    limits: Limits,
    maximum_steps: usize,
) -> Result<Receipt, Error> {
    let caller = signed.transaction().signer();
    let signers = if context.ga_authenticated.contains(&caller) {
        vec![]
    } else {
        vec![caller]
    };
    signed
        .verify_signers(&signers, network, context.protocol, context.payer.is_some())
        .map_err(crate::Error::from)?;
    let mut receipt = process_create(
        signed.transaction(),
        trees,
        context,
        vm,
        limits,
        maximum_steps,
    )?;
    receipt.transaction_hash = Some(signed.hash());
    Ok(receipt)
}

pub fn process_create<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: &ContractTransaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    vm: &fate::Config<'_, O>,
    limits: Limits,
    maximum_steps: usize,
) -> Result<Receipt, Error> {
    let ContractOperation::Create {
        version,
        code: wrapped,
        deposit,
        call_data,
    } = tx.operation()
    else {
        return Err(Error::UnsupportedOperation);
    };
    check(tx, context)?;
    let owner = tx.signer();
    let mut next = trees.clone();
    let mut changes = BTreeMap::new();
    crate::prepare_sender(&next.accounts, &mut changes, owner, tx.nonce(), context)?;
    next.accounts
        .apply(&changes.into_values().collect::<Vec<_>>())?;
    let rollback = next.clone();
    let owner_value = tx.amount().add(deposit);
    let prepaid = tx.fee().add(&product(tx.gas(), tx.gas_price()));
    let mut events = Vec::new();
    let owner_amount = if let Some(payer) = context.payer.filter(|_| prepaid != 0.into()) {
        spend(&mut next.accounts, payer, &prepaid)?;
        delta(&mut events, payer, prepaid, true, "Chain.fee");
        owner_value
    } else {
        owner_value.add(&prepaid)
    };
    if account(&next.accounts, owner)?.balance() < &owner_amount {
        return Err(crate::Error::InsufficientFunds.into());
    }
    if !version.can_create(context.protocol) {
        return Err(Error::Rejected("illegal_vm_version"));
    }
    if !version.is_fate() {
        return Err(Error::UnsupportedVm(version.vm));
    }
    let mut code = SophiaCode::decode_with_limits(
        wrapped,
        rlp::DecodeLimits {
            max_input_bytes: limits.max_bytes,
            max_items: limits.max_nodes,
            max_depth: limits.max_depth,
        },
    )
    .map_err(|e| match e {
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
    match abi::verify_init_calldata(call_data, limits) {
        Ok(true) => {}
        Err(ae_vm_fate::Error::ResourceLimit) => return Err(Error::ResourceLimit),
        _ => return Err(Error::Rejected("bad_init_function")),
    }
    spend(&mut next.accounts, owner, &owner_amount)?;
    delta(&mut events, owner, owner_amount, true, "Contract.amount");
    let nonce = match context.ga_nonces.get(&owner) {
        Some(n) => Nonce::Hash(
            n.as_slice()
                .try_into()
                .map_err(|_| Error::Exception(Exception::Native("function_clause")))?,
        ),
        None => Nonce::Number(tx.nonce()),
    };
    let init = Contract::new(
        owner,
        nonce,
        *version,
        StoredCode::Inline(wrapped.clone()),
        deposit.clone(),
    )?;
    let key = AccountPublicKey(init.public_key);
    let existing = next.accounts.lookup(key)?;
    let is_new = existing.is_none();
    let mut account = existing.unwrap_or_else(|| Account::new(key, 0.into()));
    if is_new || context.protocol > Protocol::Lima {
        account.set_flags(u64::from(!code.payable).into());
    }
    account.earn(tx.amount());
    next.accounts.enter(&account)?;
    delta(
        &mut events,
        key,
        tx.amount().clone(),
        false,
        "Contract.amount",
    );

    let (mut bytecode, mut remaining) = decode_for_preparation(&code.byte_code, limits)?;
    if version.vm == 5 {
        bytecode
            .strip_init()
            .map_err(|_| Error::Exception(Exception::Native("badmap")))?;
    }
    code.byte_code = bytecode.encode(remaining).map_err(preparation_error)?;
    remaining.max_bytes = remaining
        .max_bytes
        .checked_sub(code.byte_code.len())
        .ok_or(Error::ResourceLimit)?;
    code.version = 3;
    if context.protocol <= Protocol::Lima {
        code.compiler_version = Some(b"unknown".to_vec());
    }
    let normalized = code
        .encode()
        .map_err(|e| Error::State(ae_state::contracts::Error::Object(e)))?;
    remaining.max_bytes = remaining
        .max_bytes
        .checked_sub(normalized.len())
        .ok_or(Error::ResourceLimit)?;
    let mut chain = init.clone();
    chain.code = StoredCode::Inline(normalized);
    let metadata = encoding::encode(&Value::Map(BTreeMap::new()), remaining)
        .map_err(|e| Error::Vm(ae_vm_fate::machine::Error::Data(e)))?;
    let writes = BTreeMap::from([(vec![0, 0], metadata)]);
    if next.contracts.is_contract(init.public_key)? {
        next.contracts.enter(&chain, &writes)?;
    } else {
        next.contracts
            .insert(&chain, &writes)
            .map_err(|e| match e {
                ae_state::contracts::Error::AlreadyPresent => {
                    Error::Exception(Exception::Native("already_present"))
                }
                e => e.into(),
            })?;
    }
    let call = Call::new(
        Id::new(IdTag::Account, owner.0),
        tx.nonce().clone(),
        init.public_key,
        None,
        context.height.into(),
        tx.gas_price().clone(),
    );
    let completed = fate::execute(
        tx,
        call_data,
        &init,
        call,
        true,
        next,
        context,
        vm,
        remaining,
        maximum_steps,
    )?;
    settle(tx, trees, rollback, context, completed, events)
}

fn preparation_error(e: CodeError) -> Error {
    match e {
        CodeError::Data(ae_vm_fate::Error::ResourceLimit) => Error::ResourceLimit,
        e => Error::BytecodePreparation(e),
    }
}
fn decode_for_preparation(bytes: &[u8], limits: Limits) -> Result<(Code, Limits), Error> {
    // The original reads all three RLP sections before interpreting code. Keep
    // native RLP failures distinct from unsupported deeper diagnostic payloads.
    let mut rest = bytes;
    for _ in 0..3 {
        let (_, next) = rlp::decode_one_with_limits(
            rest,
            rlp::DecodeLimits {
                max_input_bytes: limits.max_bytes,
                max_items: limits.max_nodes,
                max_depth: limits.max_depth,
            },
        )
        .map_err(|e| match e {
            rlp::DecodeError::ResourceLimit | rlp::DecodeError::LengthOverflow => {
                Error::ResourceLimit
            }
            rlp::DecodeError::EmptyInput => Error::Exception(Exception::Native("function_clause")),
            rlp::DecodeError::Truncated => Error::Exception(Exception::Native("badmatch")),
            rlp::DecodeError::LeadingZeroLength => {
                Error::Exception(Exception::Native("leading_zeroes_in_size"))
            }
            rlp::DecodeError::TrailingBytes => Error::Exception(Exception::Native("trailing")),
        })?;
        rest = next;
    }
    if !rest.is_empty() {
        return Err(Error::Exception(Exception::Native("badmatch")));
    }
    Code::decode_bounded(bytes, limits).map_err(preparation_error)
}
