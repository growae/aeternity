// aevm_ae_primops oracle reads, fa2a6d7. ISC: repository LICENSE.
// Copyright (c) 2018, Aeternity Anstalt.
use super::*;
use crate::{heap, runtime::Native};

#[derive(Debug, Clone, Copy)]
pub enum OracleRequest<'a> {
    QueryFormat(Word),
    ResponseFormat(Word),
    QueryFee(Word),
    Question(Word, Word),
    Answer(Word, Word),
    ResponseTtl(Word, Word),
    Check(Word, &'a heap::Type, &'a heap::Type),
    CheckQuery(Word, Word, &'a heap::Type, &'a heap::Type),
}
impl OracleRequest<'_> {
    pub fn operation(self) -> Word {
        Word::from(match self {
            Self::QueryFormat(_) => 101,
            Self::ResponseFormat(_) | Self::ResponseTtl(..) => 102,
            Self::Answer(..) => 104,
            Self::Question(..) => 105,
            Self::QueryFee(_) => 106,
            Self::Check(..) => 110,
            Self::CheckQuery(..) => 111,
        })
    }
}
pub fn format<H: ChainHost>(
    host: &H,
    chain: &H::Chain,
    key: Word,
    response: bool,
    b: &mut Budget,
) -> HostResult<TypeRef, H::Error> {
    let request = if response {
        OracleRequest::ResponseFormat(key)
    } else {
        OracleRequest::QueryFormat(key)
    };
    match host.oracle(request, chain, b)? {
        Err(e) => Ok(Err(e)),
        Ok(heap::Value::Type(t)) => {
            let bytes = heap::encode_with_budget(&heap::Value::Type(t), b).map_err(heap_fault)?;
            let ptr = BigUint::from_bytes_be(&bytes[..32]);
            Ok(Ok(store::parse_type(&bytes, ptr, None, b)?))
        }
        Ok(_) => Err(Fault::Native(Native::ChainException {
            class: "error",
            reason: "bad_oracle_type",
        })),
    }
}
pub(crate) fn heap_fault<E>(e: heap::Error) -> Fault<E> {
    if e == heap::Error::Interrupted {
        Stop::Interrupted.into()
    } else if e == heap::Error::ResourceLimit {
        Stop::ResourceLimit.into()
    } else {
        Fault::Native(Native::ChainException {
            class: "error",
            reason: "bad_primop_argument",
        })
    }
}
pub(super) fn call<H: ChainHost>(
    s: &State,
    allowance: &BigUint,
    data: &[u8],
    chain: &H::Chain,
    host: &H,
    b: &mut Budget,
) -> Result<(Outcome, H::Chain), Fault<H::Error>> {
    let op = calls::words(data, 1, b).map_err(Fault::widen)?[0];
    let id = op.unsigned().to_u16().expect("oracle read ID");
    let base: BigUint = if id >= 110 { 5000u16 } else { 12000 }.into();
    let state = Box::new(execution::snapshot(s, b, true)?);
    let error = |e| Ok((Outcome::Error(e), chain.clone()));
    if allowance < &base || s.auth_tx_hash.is_some() {
        return error(Error::BaseGas {
            op,
            required: base,
            state,
        });
    }
    if id >= 110 && !matches!(s.version, Version::Sophia3 | Version::Sophia4) {
        return error(Error::IllegalOracle(op));
    }
    let (words, reps) = match id {
        106 => (2, 0),
        110 => (2, 2),
        111 => (3, 2),
        _ => (3, 0),
    };
    let mut ts = vec![heap::Type::Word; words];
    ts.extend(vec![heap::Type::TypeRep; reps]);
    let heap::Value::Tuple(args) =
        heap::decode_with_budget(&heap::Type::Tuple(ts), data, b).map_err(heap_fault)?
    else {
        unreachable!()
    };
    let word = |i| {
        if let heap::Value::Integer(n) = &args[i] {
            Word::wrapping(n)
        } else {
            unreachable!()
        }
    };
    let ty = |i| {
        if let heap::Value::Type(t) = &args[i] {
            t
        } else {
            unreachable!()
        }
    };
    let request = match id {
        104 => OracleRequest::Answer(word(1), word(2)),
        105 => OracleRequest::Question(word(1), word(2)),
        106 => OracleRequest::QueryFee(word(1)),
        110 => OracleRequest::Check(word(1), ty(2), ty(3)),
        _ => OracleRequest::CheckQuery(word(1), word(2), ty(3), ty(4)),
    };
    b.work(1)?;
    let output = match host.oracle(request, chain, b)? {
        Ok(value) => heap::encode_with_budget(&value, b).map_err(heap_fault)?,
        Err(e) => {
            b.bytes(e.len())?;
            return error(Error::Chain(e));
        }
    };
    Ok((
        Outcome::Complete {
            data: output,
            gas_spent: base,
            state,
        },
        chain.clone(),
    ))
}
