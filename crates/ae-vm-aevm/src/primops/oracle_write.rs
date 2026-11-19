// aevm_ae_primops oracle mutation branches, fa2a6d7. ISC: repository LICENSE.
// Copyright (c) 2018, Aeternity Anstalt.
use super::*;
use crate::{
    heap::{self, Type as T, Value as V},
    runtime::Native,
};
use num_bigint::BigInt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleTtl {
    pub absolute: bool,
    pub value: BigUint,
}
#[derive(Debug, Clone, Copy)]
pub enum OracleWrite<'a> {
    Register {
        account: Word,
        fee: Word,
        ttl: &'a OracleTtl,
        query: &'a T,
        response: &'a T,
        abi: u32,
    },
    Query {
        oracle: Word,
        value: &'a V,
        fee: &'a BigInt,
        query_ttl: &'a OracleTtl,
        response_ttl: &'a OracleTtl,
    },
    Respond {
        oracle: Word,
        query: Word,
        value: &'a V,
        ttl: &'a BigUint,
    },
    Extend {
        oracle: Word,
        ttl: &'a OracleTtl,
    },
}
impl OracleWrite<'_> {
    pub fn operation(self) -> Word {
        Word::from(match self {
            Self::Register { .. } => 100,
            Self::Query { .. } => 101,
            Self::Respond { .. } => 102,
            Self::Extend { .. } => 103,
        })
    }
}
fn invalid<E>() -> Fault<E> {
    Fault::Native(Native::ChainException {
        class: "error",
        reason: "bad_primop_argument",
    })
}
fn decode<E>(types: Vec<T>, data: &[u8], b: &mut Budget) -> Result<Vec<V>, Fault<E>> {
    let mut ts = vec![T::Word];
    ts.extend(types);
    match heap::decode_with_budget(&T::Tuple(ts), data, b).map_err(oracle::heap_fault)? {
        V::Tuple(v) => Ok(v),
        _ => unreachable!(),
    }
}
fn word(v: &V) -> Word {
    if let V::Integer(n) = v {
        Word::wrapping(n)
    } else {
        unreachable!()
    }
}
fn ty(v: &V) -> &T {
    if let V::Type(t) = v {
        t
    } else {
        unreachable!()
    }
}
fn ttl<E>(v: &V) -> Result<OracleTtl, Fault<E>> {
    match v {
        V::Variant { tag, values } if *tag <= 1 && values.len() == 1 => {
            let V::Integer(n) = &values[0] else {
                return Err(invalid());
            };
            Ok(OracleTtl {
                absolute: *tag == 1,
                value: n.to_biguint().ok_or_else(invalid)?,
            })
        }
        _ => Err(invalid()),
    }
}
fn signature(v: &V) -> [u8; 64] {
    let V::Tuple(ws) = v else { unreachable!() };
    let mut out = [0; 64];
    out[..32].copy_from_slice(&word(&ws[0]).0);
    out[32..].copy_from_slice(&word(&ws[1]).0);
    out
}
fn delta<H: ChainHost>(
    op: Word,
    t: &OracleTtl,
    h: &H,
    c: &H::Chain,
    b: &mut Budget,
) -> HostResult<BigUint, H::Error> {
    b.work(1)?;
    let height = h.oracle_height(op, c, b)?;
    if t.absolute {
        if t.value < height {
            Ok(Err("too_low_abs_ttl".into()))
        } else {
            Ok(Ok(&t.value - height))
        }
    } else {
        Ok(Ok(t.value.clone()))
    }
}
pub(super) fn call<H: ChainHost>(
    s: &State,
    allowance: &BigUint,
    value: &BigInt,
    data: &[u8],
    chain: &H::Chain,
    host: &H,
    b: &mut Budget,
) -> Result<(Outcome, H::Chain), Fault<H::Error>> {
    let op = calls::words(data, 1, b).map_err(Fault::widen)?[0];
    let id = op.unsigned().to_u16().expect("oracle write ID");
    let state = Box::new(execution::snapshot(s, b, true)?);
    let error = |e| Ok((Outcome::Error(e), chain.clone()));
    let base = BigUint::from(12000u16);
    if allowance < &base || s.auth_tx_hash.is_some() {
        return error(Error::BaseGas {
            op,
            required: base,
            state,
        });
    }
    macro_rules! returned {
        ($e:expr) => {
            match $e? {
                Ok(v) => v,
                Err(e) => {
                    b.bytes(e.len())?;
                    return error(Error::Chain(e));
                }
            }
        };
    }
    let sign = T::Tuple(vec![T::Word; 2]);
    let tt = T::Variant(vec![vec![T::Word]; 2]);
    let mut response_delta = None;
    let types = match id {
        100 => vec![T::Word, sign, T::Word, tt.clone(), T::TypeRep, T::TypeRep],
        103 => vec![T::Word, sign, tt.clone()],
        101 => {
            let args = decode(vec![T::Word], data, b)?;
            b.work(1)?;
            let V::Type(t) =
                returned!(host.oracle(OracleRequest::QueryFormat(word(&args[1])), chain, b))
            else {
                return Err(invalid());
            };
            vec![T::Word, t, tt.clone(), tt.clone()]
        }
        _ => {
            let args = decode(vec![T::Word; 2], data, b)?;
            b.work(1)?;
            let t = ttl(&returned!(host.oracle(
                OracleRequest::ResponseTtl(word(&args[1]), word(&args[2])),
                chain,
                b
            )))?;
            response_delta = Some(returned!(delta(op, &t, host, chain, b)));
            b.work(1)?;
            let V::Type(t) =
                returned!(host.oracle(OracleRequest::ResponseFormat(word(&args[1])), chain, b))
            else {
                return Err(invalid());
            };
            vec![T::Word, T::Word, sign, t]
        }
    };
    let args = decode(types, data, b)?;
    let qt = if id == 102 {
        None
    } else {
        Some(ttl(&args[if id == 100 { 4 } else { 3 }])?)
    };
    let d = if let Some(t) = &qt {
        returned!(delta(op, t, host, chain, b))
    } else {
        response_delta.expect("response TTL")
    };
    let rt = if id == 101 {
        Some(ttl(&args[4])?)
    } else {
        None
    };
    let request = match id {
        100 => OracleWrite::Register {
            account: word(&args[1]),
            fee: word(&args[3]),
            ttl: qt.as_ref().unwrap(),
            query: ty(&args[5]),
            response: ty(&args[6]),
            abi: s.abi_version,
        },
        101 => OracleWrite::Query {
            oracle: word(&args[1]),
            value: &args[2],
            fee: value,
            query_ttl: qt.as_ref().unwrap(),
            response_ttl: rt.as_ref().unwrap(),
        },
        102 => OracleWrite::Respond {
            oracle: word(&args[1]),
            query: word(&args[2]),
            value: &args[4],
            ttl: &d,
        },
        _ => OracleWrite::Extend {
            oracle: word(&args[1]),
            ttl: qt.as_ref().unwrap(),
        },
    };
    b.work(1)?;
    let (tx, size) = returned!(host.prepare_oracle(request, chain, b));
    let mut dynamic = (&d * 32000u32 + 175199u32) / 175200u32;
    if id != 103 {
        dynamic += BigUint::from(size) * 20u8;
    }
    if &base + &dynamic > *allowance {
        return error(Error::DynamicGas {
            op,
            required: dynamic,
            state,
        });
    }
    let sig = if id == 101 {
        None
    } else {
        Some(signature(&args[if id == 102 { 3 } else { 2 }]))
    };
    b.work(1)?;
    let (out, next) = returned!(host.apply_oracle(op, tx, sig, chain, b));
    let output = if id == 102 || id == 103 {
        b.bytes(32)?;
        vec![0; 32]
    } else {
        heap::encode_with_budget(&out, b).map_err(oracle::heap_fault)?
    };
    Ok((
        Outcome::Complete {
            data: output,
            gas_spent: base + dynamic,
            state,
        },
        next,
    ))
}
