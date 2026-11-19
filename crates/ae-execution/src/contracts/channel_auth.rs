// aesc_utils:verify_meta_tx. Authentication executes with a supplied store,
// without transaction admission, settlement, or publication of VM changes.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use crate::transactions::{Config, Error as TxError};
use ae_state::contracts::Contract;
use ae_tx::GaMeta;
use ae_vm_fate::native::{Class, Exception as NativeException, Term};
pub(crate) fn error<A, C>(e: Error) -> TxError<A, C> {
    TxError::Contract(Box::new(e))
}
pub(crate) fn native<A, C>(reason: Term) -> TxError<A, C> {
    error(Error::NativeException(Box::new(NativeException {
        class: Class::Error,
        reason,
    })))
}
pub(crate) fn reject<A, C>(s: &'static str) -> TxError<A, C> {
    error(Error::Rejected(s))
}
pub(crate) fn verify_data<A, C, O>(
    meta: &GaMeta,
    hash: &[u8],
    onchain: bool,
    config: &Config<'_, O>,
) -> Result<(), TxError<A, C>> {
    if meta.abi_version == 1.into() {
        aevm::verify_payload_auth_data(&meta.auth_data, hash, config.aevm_limits).map_err(error)
    } else if meta.abi_version == 3.into() {
        let hash = hash
            .try_into()
            .map_err(|_| native(Term::tagged("badmatch", vec![Term::Binary(hash.to_vec())])))?;
        ae_vm_fate::abi::check_auth_function(&meta.auth_data, hash, config.fate_limits).map_err(
            |e| {
                use ae_vm_fate::abi::AuthError;
                match e {
                    AuthError::BadData => reject("bad_auth_data"),
                    AuthError::WrongFunction => reject("wrong_auth_function"),
                    AuthError::ResourceLimit => error(Error::ResourceLimit),
                }
            },
        )
    } else {
        Err(native(if onchain {
            Term::tagged("case_clause", vec![Term::Integer(big(&meta.abi_version))])
        } else {
            Term::Atom("function_clause")
        }))
    }
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn verify_call<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    meta: &GaMeta,
    gas: Unsigned,
    contract: Contract,
    store_key: [u8; 32],
    trees: &Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
) -> Result<(), TxError<A, C>> {
    let mut invocation = meta.clone();
    invocation.gas = gas;
    let call = match contract.version.vm {
        5 | 7 | 8 => {
            let vm = config.fate.ok_or(TxError::MissingVmConfiguration)?;
            let mut environment = vm.environment.clone();
            environment.auth_tx_hash = context.ga_transaction_hash.map(|h| h.0.to_vec());
            let vm = fate::Config {
                context: vm.context,
                order: vm.order,
                overrides: vm.overrides,
                environment: &environment,
                history: vm.history.clone(),
                auth_tx: vm.auth_tx.clone(),
                metering_floor: vm.metering_floor,
                diagnostic_profile: vm.diagnostic_profile,
            };
            let call = Call::new(
                Id::new(IdTag::Account, meta.account.0),
                0.into(),
                contract.public_key,
                None,
                context.height.into(),
                meta.gas_price.clone(),
            );
            fate::execute_with_store_key(
                Invocation::Auth(&invocation, 0),
                &meta.auth_data,
                &contract,
                call,
                false,
                trees.clone(),
                context,
                &vm,
                config.fate_limits,
                config.maximum_fate_steps,
                Some(store_key),
            )
            .map_err(error)?
            .call
        }
        _ => {
            let mut vm = config.aevm.ok_or(TxError::MissingVmConfiguration)?.clone();
            vm.environment.auth_tx_hash = context.ga_transaction_hash.map(|h| h.0.to_vec());
            aevm::detached_auth(
                &invocation,
                contract.clone(),
                store_key,
                trees,
                context,
                &vm,
                config.aevm_limits,
            )
            .map_err(|e| TxError::Aevm(Box::new(e)))?
        }
    };
    if call.return_type != ReturnType::Ok {
        return Err(reject("signature_verification_failed_contract_error"));
    }
    let authenticated = match contract.version.abi {
        1 => aevm::payload_auth_result(&call.return_value, config.aevm_limits).map_err(error)?,
        3 => match ae_vm_fate::encoding::decode(&call.return_value, config.fate_limits) {
            Ok(ae_vm_fate::value::Value::Boolean(b)) => b,
            Err(ae_vm_fate::Error::ResourceLimit) => return Err(error(Error::ResourceLimit)),
            _ => false,
        },
        _ => return Err(native(Term::Atom("function_clause"))),
    };
    if authenticated {
        Ok(())
    } else {
        Err(reject("signature_verification_failed_authenticate_false"))
    }
}
