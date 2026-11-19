// aeprimop AEVM prepare_init_call/contract_init_call_success, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_vm_aevm::{runtime_heap::Error as HeapError, store};

pub fn process_create<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &ContractTransaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config,
    limits: Limits,
) -> Result<Receipt, Error<A, C>> {
    if !matches!(tx.operation(), ContractOperation::Create { .. }) {
        return Err(transaction_fault(super::super::Error::UnsupportedOperation).into());
    }
    process_budgeted(tx, trees, context, config, &mut Budget::new(limits))
}
#[allow(clippy::too_many_arguments)]
pub fn execute_signed_create<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    signed: &ae_tx::SignedTransaction<ContractTransaction>,
    trees: &mut Trees<A, C>,
    network: &ae_protocol::NetworkProfile,
    context: &Context,
    config: &Config,
    limits: Limits,
) -> Result<Receipt, Error<A, C>> {
    let caller = signed.transaction().signer();
    let signers = if context.ga_authenticated.contains(&caller) {
        vec![]
    } else {
        vec![caller]
    };
    signed
        .verify_signers(&signers, network, context.protocol, context.payer.is_some())
        .map_err(|e| transaction_fault(crate::Error::from(e).into()))?;
    let mut receipt = process_create(signed.transaction(), trees, context, config, limits)?;
    receipt.transaction_hash = Some(signed.hash());
    Ok(receipt)
}
pub(super) fn prepare<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &ContractTransaction,
    trees: &Trees<A, C>,
    context: &Context,
    b: &mut Budget,
) -> Result<(super::super::PreparedCall<A, C>, Contract), Error<A, C>> {
    let remaining = b.remaining();
    b.work(tx.wire_size())?;
    b.bytes(tx.wire_size().checked_mul(16).ok_or(Stop::ResourceLimit)?)?;
    let (prepared, mut code) = super::super::create::prepare(
        tx,
        trees,
        context,
        ae_codec::rlp::DecodeLimits {
            max_input_bytes: remaining.max_bytes,
            max_items: remaining.max_work,
            max_depth: remaining.max_depth,
        },
        |v| v.is_aevm(),
        |code, data| abi::verify_init(code, data, b),
    )
    .map_err(transaction_fault)?;
    let mut stored = prepared.contract.clone();
    if stored.version.vm >= 6 {
        // lists:keydelete removes only the first matching name.
        for i in 0..code.type_info.len() {
            b.work(1)?;
            if code.type_info[i].name == b"init" {
                code.type_info.remove(i);
                break;
            }
        }
        code.version = 3;
        if context.protocol <= ae_protocol::Protocol::Lima {
            code.compiler_version = Some(b"unknown".to_vec());
        }
        let bytes = code
            .encode()
            .map_err(|e| transaction_fault(ae_state::contracts::Error::Object(e).into()))?;
        b.bytes(bytes.len())?;
        stored.code = Code::Inline(bytes);
    }
    Ok((prepared, stored))
}
pub(super) fn finish<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    contract: Contract,
    mut completed: Completion<A, C>,
    config: &Config,
    b: &mut Budget,
) -> Result<Completion<A, C>, Error<A, C>> {
    if completed.call.return_type != ReturnType::Ok {
        return Ok(completed);
    }
    let version = Version::from_id(u32::from(contract.version.vm)).expect("admitted AEVM creation");
    let store = match store::initialize_budgeted(
        version,
        config.store_order,
        &completed.call.return_value,
        b,
    ) {
        Ok(store) => store,
        Err(
            e @ (HeapError::ResourceLimit
            | HeapError::Stop(_)
            | HeapError::NativeFunctionClause
            | HeapError::NativeBadKey(_)
            | HeapError::NativeBadAlignment(_)),
        ) => return Err(RuntimeFault::from(e).into()),
        Err(_) => {
            completed.call.return_type = ReturnType::Error;
            completed.call.return_value = b"out_of_gas".to_vec();
            return Ok(completed);
        }
    };
    // Insert also installs the store subtree marker. Existing contract records
    // follow aeprimop's update path without discarding untouched store entries.
    let contracts = &mut completed.trees.contracts;
    if contracts
        .is_contract(contract.public_key)
        .map_err(|e| transaction_fault(e.into()))?
    {
        contracts.enter(&contract, &store)
    } else {
        contracts.insert(&contract, &store)
    }
    .map_err(|e| transaction_fault(e.into()))?;
    Ok(completed)
}
