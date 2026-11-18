// Behavioral reference: aefa_fate.erl, fa2a6d7; aeb_fate_encoding, 009e036.
// Copyright (c) 2019, Aeternity Anstalt; (c) 2017, aeternity developers.
// ISC notice in repository LICENSE.
use crate::{
    Budget, Error, Limits,
    value::{ObjectKind, ObjectPayload, Value},
};
use num_bigint::{BigInt, BigUint};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Type {
    Integer,
    Boolean,
    Bits,
    String,
    Object(ObjectKind),
    Bytes(Option<BigUint>),
    ContractBytearray,
    Any,
    Variable(u8),
    List(Box<Type>),
    Tuple(Vec<Type>),
    Map(Box<Type>, Box<Type>),
    Variant(Vec<Type>),
    // Inferred, unvisited variant constructors have an arity, not a full type.
    // This is not a serializable type representation.
    Arity(BigInt),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Version {
    One = 5,
    Two = 7,
    Three = 8,
}
pub type Bindings = BTreeMap<u8, Type>;

pub fn infer(value: &Value, limits: Limits) -> Result<Type, Error> {
    infer_inner(value, &mut Budget::new(limits), 0)
}
fn infer_inner(v: &Value, budget: &mut Budget, depth: usize) -> Result<Type, Error> {
    budget.visit(depth)?;
    Ok(match v {
        Value::Integer(_) => Type::Integer,
        Value::Boolean(_) => Type::Boolean,
        Value::Bits(_) => Type::Bits,
        Value::String(_) => Type::String,
        Value::Bytes(v) => Type::Bytes(Some(v.len().into())),
        Value::Object(k, ObjectPayload::Bytes(_)) if *k != ObjectKind::Channel => Type::Object(*k),
        Value::List(vs) => Type::List(Box::new(infer_elements(vs.iter(), budget, depth + 1)?)),
        Value::Tuple(vs) => Type::Tuple(
            vs.iter()
                .map(|v| infer_inner(v, budget, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
        Value::Map(vs) => Type::Map(
            Box::new(infer_elements(vs.keys(), budget, depth + 1)?),
            Box::new(infer_elements(vs.values(), budget, depth + 1)?),
        ),
        Value::StoreMap { .. } => Type::Map(Box::new(Type::Any), Box::new(Type::Any)),
        Value::Variant {
            arities,
            tag,
            values,
        } => {
            if *tag >= arities.len() {
                return Err(Error::InvalidValue);
            }
            // Runtime arities can be large signed integers. Bound the graph
            // before copying it, then charge each inferred constructor node.
            crate::value::check_limits(v, budget.limits)?;
            let mut ts: Vec<_> = arities
                .iter()
                .map(|n| {
                    budget.visit(depth + 1)?;
                    Ok(Type::Arity(n.clone()))
                })
                .collect::<Result<_, Error>>()?;
            ts[*tag] = Type::Tuple(
                values
                    .iter()
                    .map(|v| infer_inner(v, budget, depth + 1))
                    .collect::<Result<_, _>>()?,
            );
            Type::Variant(ts)
        }
        _ => return Err(Error::InvalidValue),
    })
}
fn infer_elements<'a>(
    vs: impl Iterator<Item = &'a Value>,
    budget: &mut Budget,
    depth: usize,
) -> Result<Type, Error> {
    let mut result = Type::Any;
    for v in vs {
        let t = infer_inner(v, budget, depth)?;
        result = intersect(&t, &result, budget, depth)?;
    }
    Ok(result)
}
fn intersect(a: &Type, b: &Type, budget: &mut Budget, depth: usize) -> Result<Type, Error> {
    budget.visit(depth)?;
    if *a == Type::Any {
        return Ok(b.clone());
    }
    if *b == Type::Any || a == b {
        return Ok(a.clone());
    }
    Ok(match (a, b) {
        (Type::List(a), Type::List(b)) => Type::List(Box::new(intersect(a, b, budget, depth + 1)?)),
        (Type::Tuple(a), Type::Tuple(b)) if a.len() == b.len() => Type::Tuple(
            a.iter()
                .zip(b)
                .map(|(a, b)| intersect(a, b, budget, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
        (Type::Map(ak, av), Type::Map(bk, bv)) => Type::Map(
            Box::new(intersect(ak, bk, budget, depth + 1)?),
            Box::new(intersect(av, bv, budget, depth + 1)?),
        ),
        (Type::Variant(a), Type::Variant(b)) if a.len() == b.len() => Type::Variant(
            a.iter()
                .zip(b)
                .map(|(a, b)| match (a, b) {
                    (Type::Arity(n), Type::Tuple(ts)) | (Type::Tuple(ts), Type::Arity(n))
                        if *n == BigInt::from(ts.len()) =>
                    {
                        Ok(Type::Tuple(ts.clone()))
                    }
                    _ => intersect(a, b, budget, depth + 1),
                })
                .collect::<Result<_, _>>()?,
        ),
        _ => return Err(Error::InvalidType),
    })
}

// None is the reference's `false`; resource exhaustion is a separate error.
pub fn check_type(
    expected: &Type,
    value: &Value,
    version: Version,
    limits: Limits,
) -> Result<Option<Bindings>, Error> {
    let mut budget = Budget::new(limits);
    let result = infer_inner(value, &mut budget, 0)
        .and_then(|actual| match_inner(expected, &actual, version, &mut budget, 0));
    match result {
        Ok(bindings) => Ok(Some(bindings)),
        Err(Error::ResourceLimit) => Err(Error::ResourceLimit),
        Err(_) => Ok(None),
    }
}
pub fn match_type(
    expected: &Type,
    actual: &Type,
    version: Version,
    limits: Limits,
) -> Result<Option<Bindings>, Error> {
    match match_inner(expected, actual, version, &mut Budget::new(limits), 0) {
        Ok(bindings) => Ok(Some(bindings)),
        Err(Error::ResourceLimit) => Err(Error::ResourceLimit),
        Err(_) => Ok(None),
    }
}
fn match_inner(
    a: &Type,
    b: &Type,
    version: Version,
    budget: &mut Budget,
    depth: usize,
) -> Result<Bindings, Error> {
    budget.visit(depth)?;
    if a == b || *a == Type::Any || *b == Type::Any {
        return Ok(Bindings::new());
    }
    match (a, b) {
        (Type::Bytes(None), Type::Bytes(_)) if version >= Version::Three => Ok(Bindings::new()),
        (Type::Bytes(None), _) => Err(Error::InvalidType),
        (Type::Variable(n), t) => Ok(BTreeMap::from([(*n, t.clone())])),
        (Type::List(a), Type::List(b)) => match_inner(a, b, version, budget, depth + 1),
        (Type::Tuple(a), Type::Tuple(b)) if a.len() == b.len() => {
            let mut result = Bindings::new();
            for (a, b) in a.iter().zip(b) {
                merge(
                    &mut result,
                    match_inner(a, b, version, budget, depth + 1)?,
                    budget,
                    depth + 1,
                )?;
            }
            Ok(result)
        }
        (Type::Map(ak, av), Type::Map(bk, bv)) => {
            let mut result = match_inner(ak, bk, version, budget, depth + 1)?;
            merge(
                &mut result,
                match_inner(av, bv, version, budget, depth + 1)?,
                budget,
                depth + 1,
            )?;
            Ok(result)
        }
        (Type::Variant(a), Type::Variant(b)) if a.len() == b.len() => {
            let mut result = Bindings::new();
            for (a, b) in a.iter().zip(b) {
                match (a, b) {
                    (Type::Tuple(ts), Type::Arity(n)) if BigInt::from(ts.len()) == *n => {}
                    (Type::Tuple(_), Type::Tuple(_)) => merge(
                        &mut result,
                        match_inner(a, b, version, budget, depth + 1)?,
                        budget,
                        depth + 1,
                    )?,
                    _ => return Err(Error::InvalidType),
                }
            }
            Ok(result)
        }
        _ => Err(Error::InvalidType),
    }
}
fn merge(
    result: &mut Bindings,
    other: Bindings,
    budget: &mut Budget,
    depth: usize,
) -> Result<(), Error> {
    for (id, ty) in other {
        let ty = match result.get(&id) {
            Some(old) => intersect(old, &ty, budget, depth)?,
            None => ty,
        };
        result.insert(id, ty);
    }
    Ok(())
}

pub fn instantiate(bindings: &Bindings, ty: &Type, limits: Limits) -> Result<Type, Error> {
    fn go(
        bindings: &Bindings,
        ty: &Type,
        budget: &mut Budget,
        depth: usize,
    ) -> Result<Type, Error> {
        budget.visit(depth)?;
        Ok(match ty {
            Type::Variable(id) => bindings.get(id).cloned().unwrap_or(Type::Any),
            Type::List(t) => Type::List(Box::new(go(bindings, t, budget, depth + 1)?)),
            Type::Tuple(ts) => Type::Tuple(
                ts.iter()
                    .map(|t| go(bindings, t, budget, depth + 1))
                    .collect::<Result<_, _>>()?,
            ),
            Type::Variant(ts) => Type::Variant(
                ts.iter()
                    .map(|t| go(bindings, t, budget, depth + 1))
                    .collect::<Result<_, _>>()?,
            ),
            Type::Map(k, v) => Type::Map(
                Box::new(go(bindings, k, budget, depth + 1)?),
                Box::new(go(bindings, v, budget, depth + 1)?),
            ),
            Type::Arity(_) => return Err(Error::InvalidType),
            _ => ty.clone(),
        })
    }
    go(bindings, ty, &mut Budget::new(limits), 0)
}
