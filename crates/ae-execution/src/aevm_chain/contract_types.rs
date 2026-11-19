// aec_vm_chain:get_contract_fun_types/get_contract_with_code, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_codec::{object::ObjectError, rlp, sophia::SophiaCode};
use ae_state::contracts::Code;
use ae_trie::NodeWriter;
use ae_vm_aevm::{
    calls::{ContractVersion, FunctionTypes, InputHost, TypeError},
    execution::{Budget, Stop},
    primops::HostResult,
    runtime::{Fault, Native},
    runtime_heap::{Error as HeapError, TypeRef},
    store,
};

impl<A: NodeReader, C: NodeReader> Reads<'_, A, C> {
    pub fn function_types(
        &self,
        target: Word,
        caller: ContractVersion,
        function: Word,
        budget: &mut Budget,
    ) -> FunctionTypes<Error> {
        // Unlike oracle/address reads, this callback only sees the top trees.
        let load = |key, b: &mut Budget| {
            b.work(1)?;
            self.top
                .contracts
                .record(key)
                .map_err(|e| Fault::Host(Error::Contracts(e)))
        };
        let Some(contract) = load(target.0, budget)? else {
            return Ok(Err(TypeError::NoSuchContract(target)));
        };
        // Resolve exactly one reference, before checking the VM/ABI pair.
        let code = match contract.code {
            Code::Inline(bytes) => bytes,
            Code::Reference(id) => {
                if id.tag != ae_types::IdTag::Contract {
                    return Err(Fault::Native(Native::ChainException {
                        class: "error",
                        reason: "function_clause",
                    }));
                }
                let reference =
                    load(id.value, budget)?.ok_or(Fault::Native(Native::BadMatchNone))?;
                match reference.code {
                    Code::Inline(bytes) => bytes,
                    Code::Reference(id) => {
                        return Err(Fault::Native(Native::BadMatchCodeReference(id)));
                    }
                }
            }
        };
        let version = contract.version;
        if u32::from(version.abi) != caller.abi
            || !version.is_aevm()
            || caller.vm == ae_vm_aevm::opcode::Version::Solidity1
            || caller.vm.id() < u32::from(version.vm)
        {
            return Ok(Err(TypeError::WrongVmVersion {
                vm: version.vm,
                abi: version.abi,
            }));
        }
        let wrapped = decode_code(&code, budget)?;
        // lists:keyfind selects the first matching entry, including malformed
        // type bytes. Payability and function names do not affect this lookup.
        for entry in wrapped.type_info {
            budget.work(1)?;
            if entry.hash != function.0 {
                continue;
            }
            let argument = store::decode_type(&entry.arguments, budget);
            let output = store::decode_type(&entry.returns, budget);
            for result in [&argument, &output] {
                match result {
                    Err(HeapError::ResourceLimit) => return Err(Stop::ResourceLimit.into()),
                    Err(HeapError::Stop(e)) => return Err((*e).into()),
                    _ => (),
                }
            }
            return Ok(match (argument, output) {
                (Ok(a), Ok(o)) => Ok((a, o)),
                _ => Err(TypeError::BadTypeData),
            });
        }
        Ok(Err(TypeError::UnknownFunction))
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> InputHost for State<A, C> {
    type Error = Error;
    fn function_types(
        &self,
        target: Word,
        version: ContractVersion,
        function: Word,
        budget: &mut Budget,
    ) -> FunctionTypes<Error> {
        self.reads()
            .function_types(target, version, function, budget)
    }
    fn oracle_format(
        &self,
        oracle: Word,
        response: bool,
        budget: &mut Budget,
    ) -> HostResult<TypeRef, Error> {
        ae_vm_aevm::primops::oracle_format(&Host::default(), self, oracle, response, budget)
    }
}

pub fn decode_code(code: &[u8], budget: &mut Budget) -> Result<SophiaCode, Fault<Error>> {
    // Bound the wrapper's allocations and traversal separately from gas.
    let limits = budget.remaining();
    budget.work(code.len())?;
    budget.bytes(code.len().checked_mul(16).ok_or(Stop::ResourceLimit)?)?;
    let wrapped = match SophiaCode::decode_with_limits(
        code,
        rlp::DecodeLimits {
            max_input_bytes: limits.max_bytes,
            max_items: limits.max_work,
            max_depth: limits.max_depth,
        },
    ) {
        Ok(w) => w,
        Err(ObjectError::Rlp(rlp::DecodeError::ResourceLimit)) => {
            return Err(Stop::ResourceLimit.into());
        }
        Err(ObjectError::ExpectedList) => {
            let rlp::Rlp::Bytes(bytes) = rlp::decode_with_limits(
                code,
                rlp::DecodeLimits {
                    max_input_bytes: limits.max_bytes,
                    max_items: limits.max_work,
                    max_depth: limits.max_depth,
                },
            )
            .expect("same bounded decoder") else {
                unreachable!()
            };
            return Err(Fault::Native(Native::BadMatchBinary(bytes.to_vec())));
        }
        // Other malformed wrappers remain a non-consensus host fault until
        // their exact native exception payload is modeled.
        Err(e) => return Err(Fault::Host(Error::Contracts(contracts::Error::Object(e)))),
    };
    Ok(wrapped)
}
