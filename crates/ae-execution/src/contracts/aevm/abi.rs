// aeb_aevm_abi:check_calldata, 009e036. ISC: repository LICENSE.
use super::*;
use ae_codec::sophia::SophiaCode;
use ae_vm_aevm::{
    heap::{self, Type, Value},
    runtime_heap::{Type as RuntimeType, TypeRef},
    store,
};
pub(super) fn verify_init(
    code: &SophiaCode,
    data: &[u8],
    budget: &mut Budget,
) -> Result<(), super::super::Error> {
    use super::super::Error;
    let value = match heap::decode_with_budget(&Type::Tuple(vec![Type::Word]), data, budget) {
        Ok(v) => v,
        Err(heap::Error::ResourceLimit) => return Err(Error::ResourceLimit),
        Err(_) => return Err(Error::Rejected("bad_init_function")),
    };
    let Value::Tuple(values) = value else {
        unreachable!()
    };
    let Value::Integer(hash) = &values[0] else {
        unreachable!()
    };
    let hash = Word::wrapping(hash);
    for f in &code.type_info {
        budget.work(1).map_err(|_| Error::ResourceLimit)?;
        if f.hash == hash.0 {
            return if f.name == b"init" {
                Ok(())
            } else {
                Err(Error::Rejected("bad_init_function"))
            };
        }
    }
    Err(Error::Rejected("bad_init_function"))
}
pub(super) fn check(
    data: &[u8],
    code: &SophiaCode,
    payable: bool,
    budget: &mut Budget,
) -> Result<Result<(TypeRef, TypeRef), &'static str>, RuntimeFault<HostError>> {
    let decode =
        |ty: &Type, data: &[u8], b: &mut Budget| match heap::decode_with_budget(ty, data, b) {
            Ok(v) => Ok(Some(v)),
            Err(heap::Error::ResourceLimit) => Err(RuntimeFault::Stop(Stop::ResourceLimit)),
            Err(_) => Ok(None),
        };
    let Some(Value::Tuple(first)) = decode(&Type::Tuple(vec![Type::Word]), data, budget)? else {
        return Ok(Err("bad_call_data"));
    };
    let Value::Integer(hash) = &first[0] else {
        unreachable!()
    };
    let hash = Word::wrapping(hash);
    let mut found = None;
    for f in &code.type_info {
        budget.work(1)?;
        if f.hash == hash.0 {
            found = Some(f);
            break;
        }
    }
    let Some(f) = found else {
        return Ok(Err("unknown_function"));
    };
    if payable && f.payable == Some(false) {
        return Ok(Err("function_is_not_payable"));
    }
    let arg = decode(&Type::TypeRep, &f.arguments, budget)?;
    let out = decode(&Type::TypeRep, &f.returns, budget)?;
    let (Some(Value::Type(arg)), Some(Value::Type(_))) = (arg, out) else {
        return Ok(Err("unknown_function"));
    };
    if decode(&Type::Tuple(vec![Type::Word, arg]), data, budget)?.is_none() {
        return Ok(Err("bad_call_data"));
    }
    let arg = store::decode_type(&f.arguments, budget)?;
    let out = store::decode_type(&f.returns, budget)?;
    budget.bytes(3 * std::mem::size_of::<RuntimeType>())?;
    Ok(Ok((
        Arc::new(RuntimeType::Tuple(vec![Arc::new(RuntimeType::Word), arg])),
        out,
    )))
}

pub(super) fn verify_auth(
    code: &SophiaCode,
    hash: &[u8],
    budget: &mut Budget,
) -> Result<(), super::super::Error> {
    use super::super::Error;
    for f in &code.type_info {
        budget.work(1).map_err(|_| Error::ResourceLimit)?;
        if f.hash == hash {
            let decode = |b: &[u8], budget: &mut Budget| match heap::decode_with_budget(
                &Type::TypeRep,
                b,
                budget,
            ) {
                Ok(Value::Type(t)) => Ok(t),
                Err(heap::Error::ResourceLimit) => Err(Error::ResourceLimit),
                _ => Err(Error::Rejected("bad_function_hash")),
            };
            decode(&f.arguments, budget)?;
            return if decode(&f.returns, budget)? == Type::Word {
                Ok(())
            } else {
                Err(Error::Rejected("bad_auth_function_return_type"))
            };
        }
    }
    Err(Error::Rejected("bad_function_hash"))
}

pub(super) fn verify_auth_data(
    data: &[u8],
    hash: &[u8],
    b: &mut Budget,
) -> Result<(), super::super::Error> {
    use super::super::Error;
    match heap::decode_with_budget(&Type::Tuple(vec![Type::Word]), data, b) {
        Ok(Value::Tuple(v)) => {
            let Value::Integer(n) = &v[0] else {
                unreachable!()
            };
            if Word::wrapping(n).0 == hash {
                Ok(())
            } else {
                Err(Error::Rejected("wrong_auth_function"))
            }
        }
        Err(heap::Error::ResourceLimit) => Err(Error::ResourceLimit),
        _ => Err(Error::Rejected("bad_auth_data")),
    }
}
pub(super) fn auth_result(data: &[u8], b: &mut Budget) -> Result<bool, super::super::Error> {
    use super::super::Error;
    match heap::decode_with_budget(&Type::Word, data, b) {
        Ok(Value::Integer(n)) if n == 0.into() => Ok(false),
        Ok(Value::Integer(n)) if n == 1.into() => Ok(true),
        Ok(Value::Integer(n)) => Err(Error::AuthenticationResultCase(n)),
        Err(heap::Error::ResourceLimit) => Err(Error::ResourceLimit),
        _ => Ok(false),
    }
}
