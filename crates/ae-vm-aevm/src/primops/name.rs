// aevm_ae_primops aens_call_resolve, fa2a6d7. ISC: repository LICENSE.
// Copyright (c) 2018, Aeternity Anstalt.
use super::*;
use crate::heap::{self, Type, Value};

pub(super) fn resolve<H: ChainHost>(
    s: &State,
    allowance: &BigUint,
    data: &[u8],
    chain: &H::Chain,
    host: &H,
    b: &mut Budget,
) -> Result<(Outcome, H::Chain), Fault<H::Error>> {
    let op = Word::from(200);
    let base = BigUint::from(12000u16);
    let state = Box::new(execution::snapshot(s, b, true)?);
    let error = |e| Ok((Outcome::Error(e), chain.clone()));
    if allowance < &base || s.auth_tx_hash.is_some() {
        return error(Error::BaseGas {
            op,
            required: base,
            state,
        });
    }
    let ty = Type::Tuple(vec![Type::Word, Type::String, Type::String, Type::TypeRep]);
    let Value::Tuple(args) = heap::decode_with_budget(&ty, data, b).map_err(oracle::heap_fault)?
    else {
        unreachable!()
    };
    let [_, Value::String(name), Value::String(key), Value::Type(ty)] = args.as_slice() else {
        unreachable!()
    };
    b.work(1)?;
    let data = match host.resolve_name(name, key, ty, chain, b)? {
        Ok(value) => heap::encode_with_budget(&value, b).map_err(oracle::heap_fault)?,
        Err(reason) => {
            b.bytes(reason.len())?;
            return error(Error::Chain(reason));
        }
    };
    Ok((
        Outcome::Complete {
            data,
            gas_spent: base,
            state,
        },
        chain.clone(),
    ))
}
