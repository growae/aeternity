// aect_channel_contract AEVM dispatch and init, fa2a6d7.
use super::*;
fn init_failed<A, C>() -> Error<A, C> {
    RuntimeFault::Native(Native::ChainException {
        class: "error",
        reason: "contract_init_failed",
    })
    .into()
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn run<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &ContractTransaction,
    contract: &Contract,
    call: Call,
    trees: Trees<A, C>,
    onchain: &Trees<A, C>,
    context: &Context,
    config: &Config,
    limits: Limits,
) -> Result<Completion<A, C>, Error<A, C>> {
    let mut b = Budget::new(limits);
    let init = matches!(tx.operation(), ContractOperation::Create { .. });
    let mut stored = contract.clone();
    if init {
        let Code::Inline(bytes) = &contract.code else {
            unreachable!()
        };
        let mut code = match aevm_chain::decode_code(bytes, &mut b) {
            Ok(code) => code,
            Err(RuntimeFault::Stop(e)) => return Err(e.into()),
            Err(_) => return Err(init_failed()),
        };
        match abi::verify_init(&code, tx.operation().call_data(), &mut b) {
            Ok(()) => {}
            Err(super::super::Error::ResourceLimit) => return Err(Stop::ResourceLimit.into()),
            Err(_) => return Err(init_failed()),
        }
        if contract.version.vm >= 6 {
            for i in 0..code.type_info.len() {
                b.work(1)?;
                if code.type_info[i].name == b"init" {
                    code.type_info.remove(i);
                    break;
                }
            }
            if context.protocol <= ae_protocol::Protocol::Lima {
                code.compiler_version = Some(b"unknown".to_vec());
            }
            let bytes = code
                .encode()
                .map_err(|e| transaction_fault(ae_state::contracts::Error::Object(e).into()))?;
            b.bytes(bytes.len())?;
            stored.code = Code::Inline(bytes);
        }
    }
    let prepared = super::super::PreparedCall {
        contract: contract.clone(),
        call,
        rollback: trees.clone(),
        trees,
        events: vec![],
    };
    let mut completed = driver::channel(tx, prepared, onchain, context, config, &mut b)?;
    if init {
        if completed.call.return_type != ReturnType::Ok {
            return Err(init_failed());
        }
        use ae_vm_aevm::{runtime_heap::Error as HeapError, store};
        let version = Version::from_id(u32::from(contract.version.vm)).expect("AEVM channel init");
        let contents = match store::initialize_budgeted(
            version,
            config.store_order,
            &completed.call.return_value,
            &mut b,
        ) {
            Ok(contents) => contents,
            Err(
                e @ (HeapError::ResourceLimit
                | HeapError::Stop(_)
                | HeapError::NativeFunctionClause
                | HeapError::NativeBadKey(_)
                | HeapError::NativeBadAlignment(_)),
            ) => return Err(RuntimeFault::from(e).into()),
            Err(_) => return Err(init_failed()),
        };
        completed
            .trees
            .contracts
            .enter(&stored, &contents)
            .map_err(|e| transaction_fault(e.into()))?;
    }
    Ok(completed)
}
