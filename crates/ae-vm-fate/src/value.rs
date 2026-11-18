// Behavioral reference: aebytecode 009e036, aeb_fate_data / aeb_fate_encoding.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use crate::{Budget, Error, Limits, types::Type};
use num_bigint::{BigInt, Sign};
use std::{cmp::Ordering, collections::BTreeMap};
mod resource;
pub use resource::check_limits;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectKind {
    Address,
    Contract,
    Oracle,
    OracleQuery,
    Channel,
}
impl ObjectKind {
    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::Address => 0,
            Self::Contract => 2,
            Self::Oracle => 3,
            Self::OracleQuery => 4,
            Self::Channel => 5,
        }
    }
    pub(crate) fn from_tag(tag: u8) -> Result<Self, Error> {
        match tag {
            0 => Ok(Self::Address),
            2 => Ok(Self::Contract),
            3 => Ok(Self::Oracle),
            4 => Ok(Self::OracleQuery),
            5 => Ok(Self::Channel),
            _ => Err(Error::InvalidEncoding),
        }
    }
}

// The original decoder admits RLP lists as object payloads. Preserve them even
// though the serializer, ordering and runtime type inference reject them.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectPayload {
    Bytes(Vec<u8>),
    List(Vec<ObjectPayload>),
}

// Derived ordering is only a deterministic internal map index. FATE ordering
// is fallible and is implemented by compare(), not by this Ord implementation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Integer(BigInt),
    Boolean(bool),
    Bits(BigInt),
    String(Vec<u8>),
    Bytes(Vec<u8>),
    Object(ObjectKind, ObjectPayload),
    ContractBytearray(Vec<u8>),
    Tuple(Vec<Value>),
    List(Vec<Value>),
    Map(BTreeMap<Value, Value>),
    StoreMap {
        id: BigInt,
        cache: BTreeMap<Value, Value>,
    },
    // Runtime-only deletion marker in a persistent map's write cache.
    Tombstone,
    // Runtime-only empty accumulator marker. Pre-Iris stack underflow can
    // expose it inside the engine; it has no canonical FATE wire encoding.
    Void,
    Variant {
        arities: Vec<u8>,
        tag: usize,
        values: Vec<Value>,
    },
    TypeRep(Type),
}

pub fn compare(a: &Value, b: &Value, limits: Limits) -> Result<Ordering, Error> {
    cmp(a, b, &mut Budget::new(limits), 0)
}
fn ordinal(v: &Value) -> Result<u8, Error> {
    Ok(match v {
        Value::Integer(_) => 0,
        Value::Boolean(_) => 1,
        Value::Object(k, ObjectPayload::Bytes(_)) => match k {
            ObjectKind::Address => 2,
            ObjectKind::Channel => 3,
            ObjectKind::Contract => 4,
            ObjectKind::Oracle => 5,
            ObjectKind::OracleQuery => 13,
        },
        Value::Bytes(_) => 6,
        Value::Bits(_) => 7,
        Value::String(_) => 8,
        Value::Tuple(_) => 9,
        Value::Map(_) => 10,
        Value::List(_) => 11,
        Value::Variant { .. } => 12,
        Value::ContractBytearray(_) => 14,
        _ => return Err(Error::UnorderedValue),
    })
}
fn elements(
    a: &[Value],
    b: &[Value],
    budget: &mut Budget,
    depth: usize,
) -> Result<Ordering, Error> {
    for (a, b) in a.iter().zip(b) {
        budget.visit(depth)?;
        if a != b {
            return cmp(a, b, budget, depth + 1);
        }
    }
    Ok(a.len().cmp(&b.len()))
}
pub(crate) fn cmp(
    a: &Value,
    b: &Value,
    budget: &mut Budget,
    depth: usize,
) -> Result<Ordering, Error> {
    budget.visit(depth)?;
    let order = ordinal(a)?.cmp(&ordinal(b)?);
    if order != Ordering::Equal {
        return Ok(order);
    }
    Ok(match (a, b) {
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        (Value::Bits(a), Value::Bits(b)) => (a.sign() == Sign::Minus)
            .cmp(&(b.sign() == Sign::Minus))
            .then(a.cmp(b)),
        (Value::Object(_, ObjectPayload::Bytes(a)), Value::Object(_, ObjectPayload::Bytes(b))) => {
            a.cmp(b)
        }
        (Value::String(a), Value::String(b))
        | (Value::Bytes(a), Value::Bytes(b))
        | (Value::ContractBytearray(a), Value::ContractBytearray(b)) => {
            a.len().cmp(&b.len()).then(a.cmp(b))
        }
        (Value::Tuple(a), Value::Tuple(b)) => {
            if a.len() != b.len() {
                a.len().cmp(&b.len())
            } else {
                elements(a, b, budget, depth + 1)?
            }
        }
        (Value::List(a), Value::List(b)) => elements(a, b, budget, depth + 1)?,
        (
            Value::Variant {
                arities: aa,
                tag: at,
                values: av,
            },
            Value::Variant {
                arities: ba,
                tag: bt,
                values: bv,
            },
        ) => {
            let order = aa
                .len()
                .cmp(&ba.len())
                .then(aa.cmp(ba))
                .then(at.cmp(bt))
                .then(av.len().cmp(&bv.len()));
            if order == Ordering::Equal {
                elements(av, bv, budget, depth + 1)?
            } else {
                order
            }
        }
        (Value::Map(a), Value::Map(b)) => {
            if a.len() != b.len() {
                return Ok(a.len().cmp(&b.len()));
            }
            let a = sorted(a, budget, depth + 1)?;
            let b = sorted(b, budget, depth + 1)?;
            let mut order = Ordering::Equal;
            for ((ak, av), (bk, bv)) in a.into_iter().zip(b) {
                order = cmp(ak, bk, budget, depth + 1)?;
                if order == Ordering::Equal {
                    order = cmp(av, bv, budget, depth + 1)?;
                }
                if order != Ordering::Equal {
                    break;
                }
            }
            order
        }
        _ => return Err(Error::UnorderedValue),
    })
}
pub(crate) fn sorted<'a>(
    map: &'a BTreeMap<Value, Value>,
    budget: &mut Budget,
    depth: usize,
) -> Result<Vec<(&'a Value, &'a Value)>, Error> {
    let mut rows: Vec<_> = map.iter().collect();
    let mut error = None;
    rows.sort_by(|(a, _), (b, _)| {
        if error.is_some() || a == b {
            return Ordering::Equal;
        }
        match cmp(a, b, budget, depth) {
            Ok(o) => o,
            Err(e) => {
                error = Some(e);
                Ordering::Equal
            }
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(rows)
}
pub(crate) fn valid_key(v: &Value, budget: &mut Budget, depth: usize) -> Result<(), Error> {
    budget.visit(depth)?;
    match v {
        Value::Map(_) | Value::StoreMap { .. } => Err(Error::InvalidMapKey),
        Value::List(vs) | Value::Tuple(vs) | Value::Variant { values: vs, .. } => {
            for v in vs {
                valid_key(v, budget, depth + 1)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
