// aefa_chain_api oracle read helpers, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::Error;
use ae_state::oracles::{Oracle, Oracles, Query};
use ae_trie::{Nibbles, NodeReader};
use ae_vm_fate::{
    Limits, encoding,
    machine::{
        Exception,
        program::remote::{
            HostFailure,
            hosts::{OracleOperation, OracleRead},
        },
    },
    types::Type,
    value::Value,
};
use num_bigint::{BigInt, Sign};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub struct Cache {
    pub oracles: BTreeMap<[u8; 32], Oracle>,
    pub queries: BTreeMap<([u8; 32], [u8; 32]), Query>,
}
impl Cache {
    fn budget(&self, mut limits: Limits) -> Result<Limits, HostFailure<Error>> {
        let mut take = |parts: &[&[u8]]| {
            limits.max_nodes = limits
                .max_nodes
                .checked_sub(1)
                .ok_or(HostFailure::ResourceLimit)?;
            for part in parts {
                limits.max_bytes = limits
                    .max_bytes
                    .checked_sub(part.len())
                    .ok_or(HostFailure::ResourceLimit)?;
            }
            Ok::<_, HostFailure<Error>>(())
        };
        for (key, o) in &self.oracles {
            take(&[
                key,
                &o.public_key,
                &o.query_format,
                &o.response_format,
                o.query_fee.as_bytes(),
                o.expires_by.as_bytes(),
                o.abi_version.as_bytes(),
            ])?;
        }
        for ((oracle, id), q) in &self.queries {
            take(&[
                oracle,
                id,
                &q.id,
                &q.sender.value,
                &q.oracle.value,
                &q.query,
                q.response.as_deref().unwrap_or_default(),
                q.sender_nonce.as_bytes(),
                q.expires_by.as_bytes(),
                q.response_ttl.as_bytes(),
                q.fee.as_bytes(),
            ])?;
        }
        Ok(limits)
    }
}
fn backend(e: ae_state::oracles::Error) -> HostFailure<Error> {
    HostFailure::Backend(Error::Oracles(e))
}
fn option(v: Option<Value>) -> Value {
    Value::Variant {
        arities: vec![0.into(), 1.into()],
        tag: usize::from(v.is_some()),
        values: v.into_iter().collect(),
    }
}
fn scalar(n: &ae_types::Unsigned) -> Value {
    Value::Integer(BigInt::from_bytes_be(Sign::Plus, n.as_bytes()))
}
fn fixed<S: NodeReader, C: NodeReader>(
    tree: &Oracles<S, C>,
    key: &[u8],
) -> Result<Option<[u8; 32]>, HostFailure<Error>> {
    if let Ok(key) = key.try_into() {
        return Ok(Some(key));
    }
    match tree
        .tree()
        .get(&Nibbles::from_bytes(key))
        .map_err(|e| backend(e.into()))?
    {
        None => Ok(None),
        Some(_) => Err(HostFailure::Backend(Error::UnsupportedOracleKey(
            key.to_vec(),
        ))),
    }
}
pub fn lookup<S: NodeReader, C: NodeReader>(
    tree: &Oracles<S, C>,
    cache: &mut Cache,
    key: &[u8],
) -> Result<Option<Oracle>, HostFailure<Error>> {
    let Some(key) = fixed(tree, key)? else {
        return Ok(None);
    };
    if let Some(v) = cache.oracles.get(&key) {
        return Ok(Some(v.clone()));
    }
    let v = tree.oracle(key).map_err(backend)?;
    if let Some(v) = &v {
        cache.oracles.insert(key, v.clone());
    }
    Ok(v)
}
fn query<S: NodeReader, C: NodeReader>(
    tree: &Oracles<S, C>,
    cache: &mut Cache,
    oracle: [u8; 32],
    id: &[u8],
    limits: Limits,
) -> Result<Option<Query>, HostFailure<Error>> {
    let id = match <[u8; 32]>::try_from(id) {
        Ok(id) => id,
        Err(_) => {
            let raw = [oracle.as_slice(), id].concat();
            return match tree
                .tree()
                .get(&Nibbles::from_bytes(&raw))
                .map_err(|e| backend(e.into()))?
            {
                None => Ok(None),
                Some(ae_trie::Value::Bytes(bytes)) => Err(short_query_failure(&bytes, raw, limits)),
                Some(_) => Err(HostFailure::Backend(Error::UnsupportedOracleKey(raw))),
            };
        }
    };
    if let Some(v) = cache.queries.get(&(oracle, id)) {
        return Ok(Some(v.clone()));
    }
    let v = tree.query(oracle, id).map_err(backend)?;
    if let Some(v) = &v {
        cache.queries.insert((oracle, id), v.clone());
    }
    Ok(v)
}
fn short_query_failure(bytes: &[u8], key: Vec<u8>, limits: Limits) -> HostFailure<Error> {
    use ae_codec::{
        object::{self, FieldType as F},
        rlp::{self, Rlp},
    };
    // An empty query ID can address the oracle record itself. The original
    // serializer decodes fields before comparing tag/version or field count.
    let decoded = rlp::decode_with_limits(
        bytes,
        rlp::DecodeLimits {
            max_input_bytes: limits.max_bytes,
            max_items: limits.max_nodes,
            max_depth: limits.max_depth,
        },
    );
    let fields = match decoded {
        Ok(Rlp::List(fields)) => fields,
        Err(rlp::DecodeError::ResourceLimit) => return HostFailure::ResourceLimit,
        other => {
            return HostFailure::Exception(Exception::Host {
                operation: "deserialize_oracle_query",
                detail: format!("{other:?}"),
            });
        }
    };
    let schema = [
        F::Integer,
        F::Integer,
        F::Id,
        F::Integer,
        F::Id,
        F::Binary,
        F::Boolean,
        F::Binary,
        F::Integer,
        F::Integer,
        F::Integer,
    ];
    for (kind, field) in schema.iter().zip(&fields) {
        if object::decode_field(kind, field).is_err() {
            return HostFailure::Exception(Exception::Native("illegal_field"));
        }
    }
    if fields.len() != schema.len() {
        return HostFailure::Exception(Exception::Native("illegal_template_or_values"));
    }
    if object::decode_field(&F::Integer, &fields[0]).ok()
        != Some(object::Value::Integer(
            (object::ObjectTag::OracleQuery as u64).into(),
        ))
        || object::decode_field(&F::Integer, &fields[1]).ok()
            != Some(object::Value::Integer(1.into()))
    {
        return HostFailure::Exception(Exception::Native("illegal_serialization"));
    }
    HostFailure::Backend(Error::UnsupportedOracleKey(key))
}
fn encode(v: &Value, limits: Limits) -> Result<Vec<u8>, HostFailure<Error>> {
    encoding::encode_for_vm(v, limits).map_err(|e| {
        if e.error == ae_vm_fate::Error::ResourceLimit {
            HostFailure::ResourceLimit
        } else {
            HostFailure::Exception(Exception::Native(e.native_exception))
        }
    })
}
pub fn read<S: NodeReader, C: NodeReader>(
    tree: &Oracles<S, C>,
    cache: &mut Cache,
    request: OracleRead<'_>,
    limits: Limits,
) -> Result<Value, HostFailure<Error>> {
    use OracleOperation::*;
    let op = request.operation;
    let error = |reason| HostFailure::Primop {
        operation: op.name(),
        reason,
    };
    let limits = cache.budget(limits)?;
    let mut staged = cache.clone();
    let oracle = lookup(tree, &mut staged, request.oracle)?;
    if matches!(op, Fee | Expiry) {
        let oracle = oracle.ok_or_else(|| error("oracle_does_not_exist"))?;
        let result = scalar(if op == Fee {
            &oracle.query_fee
        } else {
            &oracle.expires_by
        });
        *cache = staged;
        return Ok(result);
    }
    let q = request
        .question_type
        .ok_or(HostFailure::Exception(Exception::Native("function_clause")))?;
    let r = request
        .response_type
        .ok_or(HostFailure::Exception(Exception::Native("function_clause")))?;
    let mismatch = match &oracle {
        None => Some("oracle_not_found"),
        Some(o)
            if o.abi_version == 0.into()
                && *q == Value::TypeRep(Type::String)
                && *r == Value::TypeRep(Type::String) =>
        {
            None
        }
        Some(o) if o.abi_version == 3.into() => {
            let qb = encode(q, limits)?;
            let rb = encode(r, limits)?;
            if qb == o.query_format && rb == o.response_format {
                None
            } else {
                Some("wrong_oracle_types")
            }
        }
        _ => Some("wrong_abi_version"),
    };
    if let Some(reason) = mismatch {
        return if matches!(op, Check | CheckQuery) {
            Ok(Value::Boolean(false))
        } else {
            Err(error(reason))
        };
    }
    let oracle = oracle.expect("a matched oracle exists");
    if op == Check {
        *cache = staged;
        return Ok(Value::Boolean(true));
    }
    let query = query(
        tree,
        &mut staged,
        oracle.public_key,
        request
            .query
            .ok_or(HostFailure::Exception(Exception::Native("function_clause")))?,
        limits,
    )?;
    if op == CheckQuery {
        let result = Value::Boolean(query.is_some());
        *cache = staged;
        return Ok(result);
    }
    let Some(query) = query else {
        // Missing answers return the state before even the oracle lookup.
        return if op == Answer {
            Ok(option(None))
        } else {
            Err(error("query_not_found"))
        };
    };
    let bytes = if op == Question {
        Some(&query.query)
    } else {
        query.response.as_ref()
    };
    let value = match bytes {
        None => option(None),
        Some(bytes) => {
            if bytes.len() > limits.max_bytes {
                return Err(HostFailure::ResourceLimit);
            }
            let v = if oracle.abi_version == 0.into() {
                Value::String(bytes.clone())
            } else {
                encoding::decode(bytes, limits).map_err(|e| {
                    if e == ae_vm_fate::Error::ResourceLimit {
                        HostFailure::ResourceLimit
                    } else {
                        error(if op == Question {
                            "bad_question"
                        } else {
                            "bad_response"
                        })
                    }
                })?
            };
            if op == Answer { option(Some(v)) } else { v }
        }
    };
    ae_vm_fate::value::check_limits(&value, limits).map_err(|_| HostFailure::ResourceLimit)?;
    *cache = staged;
    Ok(value)
}
