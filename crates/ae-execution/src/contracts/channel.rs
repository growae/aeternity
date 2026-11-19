// aect_channel_contract, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::channel_auth::{error, native};
use super::*;
use crate::transactions::{Config, Error as TxError};
use ae_state::contracts::{Code as StoredCode, Contract, LoadOptions};
use ae_vm_fate::native::Term;

pub(crate) fn load<A: NodeReader, C: NodeReader>(
    key: [u8; 32],
    trees: &Trees<A, C>,
) -> Result<Contract, TxError<A, C>> {
    let lookup = |k| {
        trees
            .contracts
            .lookup(k, LoadOptions::default())
            .map_err(|e| match e {
                ae_state::contracts::Error::NoSuchStore => native(Term::tagged(
                    "badmatch",
                    vec![Term::tagged("error", vec![Term::Atom("no_such_subtree")])],
                )),
                e => error(e.into()),
            })
            .map(|l| l.map(|l| l.contract))
    };
    let mut c = lookup(key)?.ok_or_else(|| {
        native(Term::tagged(
            "not_present",
            vec![Term::Binary(key.to_vec())],
        ))
    })?;
    if let StoredCode::Reference(id) = c.code {
        if id.tag != IdTag::Contract {
            return Err(native(Term::Atom("function_clause")));
        }
        let reference = lookup(id.value)?
            .ok_or_else(|| native(Term::tagged("badmatch", vec![Term::Atom("none")])))?;
        let StoredCode::Inline(bytes) = reference.code else {
            let StoredCode::Reference(id) = reference.code else {
                unreachable!()
            };
            let tag = match id.tag {
                IdTag::Account => "account",
                IdTag::Name => "name",
                IdTag::Commitment => "commitment",
                IdTag::Oracle => "oracle",
                IdTag::Contract => "contract",
                IdTag::Channel => "channel",
            };
            return Err(native(Term::tagged(
                "badmatch",
                vec![Term::tagged(
                    "ref",
                    vec![Term::tagged(
                        "id",
                        vec![Term::Atom(tag), Term::Binary(id.value.to_vec())],
                    )],
                )],
            )));
        };
        c.code = StoredCode::Inline(bytes);
    }
    Ok(c)
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn run<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: &ContractTransaction,
    contract: &Contract,
    call: Call,
    mut trees: Trees<A, C>,
    onchain: &Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
) -> Result<Completion<A, C>, TxError<A, C>> {
    let init = matches!(tx.operation(), ContractOperation::Create { .. });
    if contract.version.is_aevm() {
        return aevm::channel(
            tx,
            contract,
            call,
            trees,
            onchain,
            context,
            config.aevm.ok_or(TxError::MissingVmConfiguration)?,
            config.aevm_limits,
        )
        .map_err(|e| TxError::Aevm(Box::new(e)));
    }
    if !contract.version.is_fate() {
        return Err(native(Term::Atom("wrong_vm_version")));
    }
    let vm = config.fate.ok_or(TxError::MissingVmConfiguration)?;
    let mut limits = config.fate_limits;
    if init {
        match ae_vm_fate::abi::verify_init_calldata(tx.operation().call_data(), limits) {
            Ok(true) => {}
            Err(ae_vm_fate::Error::ResourceLimit) => return Err(error(Error::ResourceLimit)),
            _ => return Err(native(Term::Atom("contract_init_failed"))),
        }
        let StoredCode::Inline(bytes) = &contract.code else {
            unreachable!()
        };
        let mut code = fate::decode_code(bytes, limits).map_err(error)?;
        if contract.version.vm == 5 {
            let (mut bytecode, remaining) =
                ae_vm_fate::code::Code::decode_native_bounded(&code.byte_code, vm.order, limits)
                    .map_err(|e| error(create::preparation_error(e)))?;
            limits = bytecode
                .strip_init_native(remaining)
                .map_err(|e| error(create::preparation_error(e)))?;
            (code.byte_code, limits) = bytecode
                .encode_native_bounded(vm.order, limits)
                .map_err(|e| error(create::preparation_error(e)))?;
        }
        code.version = 3;
        if context.protocol <= ae_protocol::Protocol::Lima {
            code.compiler_version = Some(b"unknown".to_vec());
        }
        let mut stored = contract.clone();
        let bytes = code
            .encode()
            .map_err(|e| error(Error::State(ae_state::contracts::Error::Object(e))))?;
        limits.max_bytes = limits
            .max_bytes
            .checked_sub(bytes.len())
            .ok_or_else(|| error(Error::ResourceLimit))?;
        stored.code = StoredCode::Inline(bytes);
        let metadata =
            ae_vm_fate::encoding::encode(&ae_vm_fate::value::Value::Map(BTreeMap::new()), limits)
                .map_err(|e| error(Error::Vm(ae_vm_fate::machine::Error::Data(e))))?;
        trees
            .contracts
            .enter(&stored, &BTreeMap::from([(vec![0, 0], metadata)]))
            .map_err(|e| error(e.into()))?;
    }
    let completed = fate::execute_with_views(
        Invocation::Contract(tx),
        tx.operation().call_data(),
        contract,
        call,
        init,
        trees,
        context,
        vm,
        limits,
        config.maximum_fate_steps,
        None,
        Some(onchain),
    )
    .map_err(error)?;
    if init && completed.call.return_type != ReturnType::Ok {
        return Err(native(Term::Atom("contract_init_failed")));
    }
    Ok(completed)
}
