// aec_vm_chain oracle getters, fa2a6d7. ISC: repository LICENSE.
// Copyright (c) 2018, Aeternity Anstalt.
use super::*;
use ae_state::oracles::Oracle;
use ae_vm_aevm::{
    execution::{Budget, Stop},
    heap::{self, Type, Value},
    opcode::Version,
    primops::{HostResult, OracleRequest},
    runtime::{Fault, Native},
};
use num_bigint::BigUint;
fn integer(n: &Unsigned) -> Value {
    Value::Integer(BigUint::from_bytes_be(n.as_bytes()).into())
}
fn decode(ty: &Type, bytes: &[u8], failure: &str, b: &mut Budget) -> HostResult<Value, Error> {
    b.bytes(bytes.len())?;
    match heap::decode_with_budget(ty, bytes, b) {
        Ok(v) => Ok(Ok(v)),
        Err(heap::Error::ResourceLimit) => Err(Stop::ResourceLimit.into()),
        Err(_) => Ok(Err(failure.into())),
    }
}
fn format(o: &Oracle, response: bool, b: &mut Budget) -> HostResult<Type, Error> {
    if o.abi_version == 0.into() {
        return Ok(Ok(Type::String));
    }
    if o.abi_version != 1.into() {
        return Err(Fault::Native(Native::ChainException {
            class: "error",
            reason: "function_clause",
        }));
    }
    match decode(
        &Type::TypeRep,
        if response {
            &o.response_format
        } else {
            &o.query_format
        },
        "bad_typerep",
        b,
    )? {
        Ok(Value::Type(t)) => Ok(Ok(t)),
        Ok(_) => unreachable!(),
        Err(e) => Ok(Err(e)),
    }
}
impl<A: NodeReader, C: NodeReader> Reads<'_, A, C> {
    // The VM version belongs to the chain adapter, independently of protocol.
    // Response TTLs use the AEVM delta constructor (variant tag zero).
    pub fn oracle(
        &self,
        request: OracleRequest<'_>,
        version: Version,
        b: &mut Budget,
    ) -> HostResult<Value, Error> {
        use OracleRequest as R;
        let tree = &self.onchain.as_ref().unwrap_or(&self.top).oracles;
        let oracle = |key: Word, b: &mut Budget| {
            b.work(1)?;
            tree.oracle(key.0)
                .map_err(|e| Fault::Host(Error::OracleState(e)))
        };
        let query = |key: Word, id: Word, b: &mut Budget| {
            b.work(1)?;
            tree.query(key.0, id.0)
                .map_err(|e| Fault::Host(Error::OracleState(e)))
        };
        let check = |key, qt: &Type, rt: &Type, b: &mut Budget| -> HostResult<Value, Error> {
            let Some(o) = oracle(key, b)? else {
                return Ok(Ok(Value::Integer(0.into())));
            };
            let mut get = |response| match format(&o, response, b)? {
                Ok(t) => Ok(t),
                Err(e) => Err(Fault::Native(Native::BadMatchChainError(e))),
            };
            // Original response format is decoded before the query format.
            let r = get(true)?;
            let q = get(false)?;
            Ok(Ok(Value::Integer(u8::from(q == *qt && r == *rt).into())))
        };
        match request {
            R::QueryFormat(key) | R::ResponseFormat(key) => {
                let Some(o) = oracle(key, b)? else {
                    return Ok(Err("no_such_oracle".into()));
                };
                Ok(format(&o, matches!(request, R::ResponseFormat(_)), b)?.map(Value::Type))
            }
            R::QueryFee(key) => match oracle(key, b)? {
                Some(o) => {
                    b.bytes(o.query_fee.as_bytes().len())?;
                    Ok(Ok(integer(&o.query_fee)))
                }
                None => match version {
                    Version::Sophia1 => Ok(Ok(Value::Option(None))),
                    Version::Solidity1 => Err(Fault::Native(Native::CaseClauseNone)),
                    _ => Ok(Err("no_such_oracle".into())),
                },
            },
            R::ResponseTtl(key, id) => match query(key, id, b)? {
                Some(q) => {
                    b.bytes(q.response_ttl.as_bytes().len())?;
                    Ok(Ok(Value::Variant {
                        tag: 0,
                        values: vec![integer(&q.response_ttl)],
                    }))
                }
                None => Ok(Err("no_such_oracle_query".into())),
            },
            R::Question(key, id) | R::Answer(key, id) => {
                let answer = matches!(request, R::Answer(..));
                let Some(q) = query(key, id, b)? else {
                    return if answer {
                        Ok(Ok(Value::Option(None)))
                    } else {
                        Ok(Err("no_such_question".into()))
                    };
                };
                let bytes = if answer {
                    let Some(bytes) = &q.response else {
                        return Ok(Ok(Value::Option(None)));
                    };
                    bytes
                } else {
                    &q.query
                };
                let o = oracle(key, b)?.ok_or(Fault::Native(Native::BadMatchNone))?;
                let ty = match format(&o, answer, b)? {
                    Ok(t) => t,
                    Err(e) => return Ok(Err(e)),
                };
                let v = if o.abi_version == 0.into() {
                    b.bytes(bytes.len())?;
                    Ok(Value::String(bytes.clone()))
                } else {
                    decode(
                        &ty,
                        bytes,
                        if answer { "bad_answer" } else { "bad_question" },
                        b,
                    )?
                };
                Ok(v.map(|v| {
                    if answer {
                        Value::Option(Some(Box::new(v)))
                    } else {
                        v
                    }
                }))
            }
            R::Check(key, qt, rt) => check(key, qt, rt, b),
            R::CheckQuery(key, id, qt, rt) => {
                if query(key, id, b)?.is_none() {
                    Ok(Ok(Value::Integer(0.into())))
                } else {
                    check(key, qt, rt, b)
                }
            }
        }
    }
}
