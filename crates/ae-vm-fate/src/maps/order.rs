// SPDX-License-Identifier: Apache-2.0
// Rust adaptation of OTP-26.2.5.21 term comparison, hashing and map traversal.
// Copyright Ericsson AB 2014-2026. All Rights Reserved.
// Modified for the FATE value domain and explicit immutable runtime profiles.
// License text: licenses/Apache-2.0.txt at the repository root.
use super::{MapOrder, Traversal};
use crate::{
    Budget, Error, Limits,
    types::Type,
    value::{ObjectKind, ObjectPayload, Value},
};
use num_bigint::BigInt;
use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};
mod hash;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Atom {
    Deleted,
    Address,
    Any,
    Bits,
    Boolean,
    Bytes,
    Channel,
    Contract,
    ContractBytearray,
    False,
    Integer,
    List,
    Map,
    Oracle,
    OracleQuery,
    StoreMap,
    String,
    True,
    Tuple,
    TypeVariable,
    TypeRep,
    Variant,
    Void,
    MaxTtl,
}
impl Atom {
    pub const ALL: [Self; 24] = [
        Self::Deleted,
        Self::Address,
        Self::Any,
        Self::Bits,
        Self::Boolean,
        Self::Bytes,
        Self::Channel,
        Self::Contract,
        Self::ContractBytearray,
        Self::False,
        Self::Integer,
        Self::List,
        Self::Map,
        Self::Oracle,
        Self::OracleQuery,
        Self::StoreMap,
        Self::String,
        Self::True,
        Self::Tuple,
        Self::TypeVariable,
        Self::TypeRep,
        Self::Variant,
        Self::Void,
        Self::MaxTtl,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::Deleted => "__DELETED__",
            Self::Address => "address",
            Self::Any => "any",
            Self::Bits => "bits",
            Self::Boolean => "boolean",
            Self::Bytes => "bytes",
            Self::Channel => "channel",
            Self::Contract => "contract",
            Self::ContractBytearray => "contract_bytearray",
            Self::False => "false",
            Self::Integer => "integer",
            Self::List => "list",
            Self::Map => "map",
            Self::Oracle => "oracle",
            Self::OracleQuery => "oracle_query",
            Self::StoreMap => "store_map",
            Self::String => "string",
            Self::True => "true",
            Self::Tuple => "tuple",
            Self::TypeVariable => "tvar",
            Self::TypeRep => "typerep",
            Self::Variant => "variant",
            Self::Void => "void",
            Self::MaxTtl => "max_ttl",
        }
    }
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|a| a.name() == name)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WordSize {
    Bits32,
    Bits64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mixing {
    Jenkins,
    Crc32c,
}
// No process-local atom allocation, CPU detection, or network default. A caller
// must select an attested profile for the chain/runtime it is reproducing.
#[derive(Debug, Clone)]
pub struct Otp26 {
    word_size: WordSize,
    mixing: Mixing,
    atoms: BTreeMap<Atom, u32>,
}
impl Otp26 {
    pub fn new(
        word_size: WordSize,
        mixing: Mixing,
        atoms: BTreeMap<Atom, u32>,
    ) -> Result<Self, Error> {
        // Earlier captures omit runtime-only void/max_ttl atoms. They remain
        // usable for their original values; hashing a new atom requires its
        // explicit index and reports InvalidOrderProfile if it is absent.
        if Atom::ALL
            .iter()
            .any(|a| !matches!(a, Atom::Void | Atom::MaxTtl) && !atoms.contains_key(a))
            || atoms.values().copied().collect::<BTreeSet<_>>().len() != atoms.len()
            || (word_size == WordSize::Bits32 && atoms.values().any(|&i| i >= (1 << 26)))
        {
            return Err(Error::InvalidOrderProfile);
        }
        Ok(Self {
            word_size,
            mixing,
            atoms,
        })
    }
    pub fn hash(&self, value: &Value, limits: Limits) -> Result<u32, Error> {
        let mut budget = Budget::new(limits);
        let term = value_term(value, &mut budget, &mut 0, 0)?;
        hash::term(self, &term, &mut budget, 0)
    }
}
impl MapOrder for Otp26 {
    fn indices(
        &self,
        keys: &[&Value],
        mode: Traversal,
        limits: Limits,
    ) -> Result<Vec<usize>, Error> {
        let mut budget = Budget::new(limits);
        if keys.len() > limits.max_nodes {
            return Err(Error::ResourceLimit);
        }
        let mut bytes = 0;
        let terms: Vec<_> = keys
            .iter()
            .map(|v| value_term(v, &mut budget, &mut bytes, 0))
            .collect::<Result<_, _>>()?;
        let mut indices = term_indices(&terms.iter().collect::<Vec<_>>(), &mut budget, 0)?;
        if mode != Traversal::TermSorted && keys.len() > 32 {
            let hashes: Vec<_> = terms
                .iter()
                .map(|v| hash::term(self, v, &mut budget, 0))
                .collect::<Result<_, _>>()?;
            // Stable sorting retains term order for complete hash collisions.
            indices.sort_by_key(|&i| hash::nibble_key(hashes[i]));
            // OTP26 maps:to_list/keys/values builds the reverse of iterator order.
            if mode == Traversal::ToList {
                indices.reverse();
            }
        }
        Ok(indices)
    }
}
// OTP26 maps with binary-only keys have no atom-index, word-size or CPU
// mixing dependency. Reuse the term hash here for non-VM protocols as well.
pub fn binary_indices(
    keys: &[&[u8]],
    mode: Traversal,
    limits: Limits,
) -> Result<Vec<usize>, Error> {
    if keys.len() > limits.max_nodes {
        return Err(Error::ResourceLimit);
    }
    let mut bytes = 0usize;
    for key in keys {
        bytes = bytes.checked_add(key.len()).ok_or(Error::ResourceLimit)?;
        if bytes > limits.max_bytes || key.len() > u32::MAX as usize {
            return Err(Error::ResourceLimit);
        }
    }
    let mut indices: Vec<_> = (0..keys.len()).collect();
    indices.sort_by_key(|&i| keys[i]);
    if indices.windows(2).any(|w| keys[w[0]] == keys[w[1]]) {
        return Err(Error::InvalidMapKey);
    }
    if mode != Traversal::TermSorted && keys.len() > 32 {
        let hashes: Vec<_> = keys
            .iter()
            .map(|k| hash::nibble_key(hash::binary_term(k)))
            .collect();
        indices.sort_by_key(|&i| hashes[i]);
        if mode == Traversal::ToList {
            indices.reverse();
        }
    }
    Ok(indices)
}

pub fn compare(a: &Value, b: &Value, limits: Limits) -> Result<Ordering, Error> {
    compare_counted(a, b, limits).map(|(order, _)| order)
}
pub(crate) fn compare_counted(
    a: &Value,
    b: &Value,
    limits: Limits,
) -> Result<(Ordering, Limits), Error> {
    let mut budget = Budget::new(limits);
    let mut bytes = 0;
    let a = value_term(a, &mut budget, &mut bytes, 0)?;
    let b = value_term(b, &mut budget, &mut bytes, 0)?;
    let order = compare_terms(&a, &b, &mut budget, 0)?;
    Ok((
        order,
        Limits {
            max_nodes: limits.max_nodes - budget.nodes,
            max_bytes: limits.max_bytes - bytes,
            ..limits
        },
    ))
}
#[derive(Debug)]
enum Term<'a> {
    Integer(Cow<'a, BigInt>),
    Atom(Atom),
    Binary(&'a [u8]),
    Tuple(Vec<Term<'a>>),
    List(Vec<Term<'a>>),
    Map(Vec<(Term<'a>, Term<'a>)>),
}
impl Term<'_> {
    fn rank(&self) -> u8 {
        match self {
            Self::Integer(_) => 0,
            Self::Atom(_) => 1,
            Self::Tuple(_) => 2,
            Self::Map(_) => 3,
            Self::List(v) if v.is_empty() => 4,
            Self::List(_) => 5,
            Self::Binary(_) => 6,
        }
    }
}
fn compare_terms(
    a: &Term<'_>,
    b: &Term<'_>,
    budget: &mut Budget,
    depth: usize,
) -> Result<Ordering, Error> {
    budget.visit(depth)?;
    let rank = a.rank().cmp(&b.rank());
    if rank != Ordering::Equal {
        return Ok(rank);
    }
    Ok(match (a, b) {
        (Term::Integer(a), Term::Integer(b)) => a.cmp(b),
        (Term::Atom(a), Term::Atom(b)) => a.name().cmp(b.name()),
        (Term::Binary(a), Term::Binary(b)) => a.cmp(b),
        (Term::Tuple(a), Term::Tuple(b)) => {
            let n = a.len().cmp(&b.len());
            if n != Ordering::Equal {
                n
            } else {
                compare_slice(a, b, budget, depth + 1)?
            }
        }
        (Term::List(a), Term::List(b)) => compare_slice(a, b, budget, depth + 1)?,
        (Term::Map(a), Term::Map(b)) => {
            let n = a.len().cmp(&b.len());
            if n != Ordering::Equal {
                return Ok(n);
            }
            for i in 0..a.len() {
                let c = compare_terms(&a[i].0, &b[i].0, budget, depth + 1)?;
                if c != Ordering::Equal {
                    return Ok(c);
                }
            }
            for i in 0..a.len() {
                let c = compare_terms(&a[i].1, &b[i].1, budget, depth + 1)?;
                if c != Ordering::Equal {
                    return Ok(c);
                }
            }
            Ordering::Equal
        }
        _ => unreachable!(),
    })
}
fn compare_slice(
    a: &[Term<'_>],
    b: &[Term<'_>],
    budget: &mut Budget,
    depth: usize,
) -> Result<Ordering, Error> {
    for (a, b) in a.iter().zip(b) {
        let c = compare_terms(a, b, budget, depth)?;
        if c != Ordering::Equal {
            return Ok(c);
        }
    }
    Ok(a.len().cmp(&b.len()))
}
fn term_indices(
    keys: &[&Term<'_>],
    budget: &mut Budget,
    depth: usize,
) -> Result<Vec<usize>, Error> {
    if keys.len() > budget.limits.max_nodes.saturating_sub(budget.nodes) {
        return Err(Error::ResourceLimit);
    }
    let mut indices: Vec<_> = (0..keys.len()).collect();
    let mut error = None;
    indices.sort_by(|&a, &b| {
        if error.is_some() {
            return Ordering::Equal;
        }
        match compare_terms(keys[a], keys[b], budget, depth) {
            Ok(c) => c,
            Err(e) => {
                error = Some(e);
                Ordering::Equal
            }
        }
    });
    if let Some(e) = error {
        return Err(e);
    }
    Ok(indices)
}
fn bytes_used(n: usize, budget: &Budget, bytes: &mut usize) -> Result<(), Error> {
    *bytes = bytes.checked_add(n).ok_or(Error::ResourceLimit)?;
    if *bytes > budget.limits.max_bytes {
        Err(Error::ResourceLimit)
    } else {
        Ok(())
    }
}
fn integer<'a>(
    n: &'a BigInt,
    budget: &mut Budget,
    bytes: &mut usize,
    depth: usize,
) -> Result<Term<'a>, Error> {
    budget.visit(depth)?;
    bytes_used(
        usize::try_from(n.magnitude().bits().div_ceil(8)).map_err(|_| Error::ResourceLimit)?,
        budget,
        bytes,
    )?;
    Ok(Term::Integer(Cow::Borrowed(n)))
}
fn small(n: impl Into<BigInt>) -> Term<'static> {
    Term::Integer(Cow::Owned(n.into()))
}
fn object_atom(k: ObjectKind) -> Atom {
    match k {
        ObjectKind::Address => Atom::Address,
        ObjectKind::Contract => Atom::Contract,
        ObjectKind::Oracle => Atom::Oracle,
        ObjectKind::OracleQuery => Atom::OracleQuery,
        ObjectKind::Channel => Atom::Channel,
    }
}
fn payload<'a>(
    v: &'a ObjectPayload,
    budget: &mut Budget,
    bytes: &mut usize,
    depth: usize,
) -> Result<Term<'a>, Error> {
    budget.visit(depth)?;
    Ok(match v {
        ObjectPayload::Bytes(v) => {
            bytes_used(v.len(), budget, bytes)?;
            Term::Binary(v)
        }
        ObjectPayload::List(v) => Term::List(
            v.iter()
                .map(|v| payload(v, budget, bytes, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
    })
}
fn values<'a>(
    vs: &'a [Value],
    budget: &mut Budget,
    bytes: &mut usize,
    depth: usize,
) -> Result<Vec<Term<'a>>, Error> {
    if vs.len() > budget.limits.max_nodes.saturating_sub(budget.nodes) {
        return Err(Error::ResourceLimit);
    }
    vs.iter()
        .map(|v| value_term(v, budget, bytes, depth))
        .collect()
}
fn map<'a>(
    vs: &'a super::Map,
    budget: &mut Budget,
    bytes: &mut usize,
    depth: usize,
) -> Result<Term<'a>, Error> {
    budget.visit(depth)?;
    if vs.len() > budget.limits.max_nodes.saturating_sub(budget.nodes) {
        return Err(Error::ResourceLimit);
    }
    let terms: Vec<_> = vs
        .iter()
        .map(|(k, v)| {
            Ok((
                value_term(k, budget, bytes, depth + 1)?,
                value_term(v, budget, bytes, depth + 1)?,
            ))
        })
        .collect::<Result<_, Error>>()?;
    let indices = term_indices(
        &terms.iter().map(|v| &v.0).collect::<Vec<_>>(),
        budget,
        depth + 1,
    )?;
    let mut terms: Vec<_> = terms.into_iter().map(Some).collect();
    Ok(Term::Map(
        indices
            .into_iter()
            .map(|i| terms[i].take().unwrap())
            .collect(),
    ))
}
fn value_term<'a>(
    v: &'a Value,
    budget: &mut Budget,
    bytes: &mut usize,
    depth: usize,
) -> Result<Term<'a>, Error> {
    budget.visit(depth)?;
    Ok(match v {
        Value::Integer(n) => integer(n, budget, bytes, depth)?,
        Value::Boolean(b) => Term::Atom(if *b { Atom::True } else { Atom::False }),
        Value::Tombstone => Term::Atom(Atom::Deleted),
        Value::Void => Term::Atom(Atom::Void),
        Value::MaxTtl => Term::Atom(Atom::MaxTtl),
        Value::Bits(n) => Term::Tuple(vec![
            Term::Atom(Atom::Bits),
            integer(n, budget, bytes, depth + 1)?,
        ]),
        Value::BitsNil => Term::Tuple(vec![Term::Atom(Atom::Bits), Term::List(vec![])]),
        Value::String(v) => {
            bytes_used(v.len(), budget, bytes)?;
            Term::Binary(v)
        }
        Value::Bytes(data) | Value::ContractBytearray(data) => {
            bytes_used(data.len(), budget, bytes)?;
            Term::Tuple(vec![
                Term::Atom(if matches!(v, Value::Bytes(_)) {
                    Atom::Bytes
                } else {
                    Atom::ContractBytearray
                }),
                Term::Binary(data),
            ])
        }
        Value::Object(k, v) => Term::Tuple(vec![
            Term::Atom(object_atom(*k)),
            payload(v, budget, bytes, depth + 1)?,
        ]),
        Value::Tuple(v) => Term::Tuple(vec![
            Term::Atom(Atom::Tuple),
            Term::Tuple(values(v, budget, bytes, depth + 1)?),
        ]),
        Value::List(v) => Term::List(values(v, budget, bytes, depth + 1)?),
        Value::Map(v) => map(v, budget, bytes, depth + 1)?,
        Value::StoreMap { id, cache } => Term::Tuple(vec![
            Term::Atom(Atom::StoreMap),
            map(cache, budget, bytes, depth + 1)?,
            integer(id, budget, bytes, depth + 1)?,
        ]),
        Value::Variant {
            arities,
            tag,
            values: vs,
        } => {
            if arities.len() > budget.limits.max_nodes.saturating_sub(budget.nodes) {
                return Err(Error::ResourceLimit);
            }
            Term::Tuple(vec![
                Term::Atom(Atom::Variant),
                Term::List(
                    arities
                        .iter()
                        .map(|n| integer(n, budget, bytes, depth + 1))
                        .collect::<Result<_, _>>()?,
                ),
                small(*tag),
                Term::Tuple(values(vs, budget, bytes, depth + 1)?),
            ])
        }
        Value::TypeRep(t) => Term::Tuple(vec![
            Term::Atom(Atom::TypeRep),
            type_term(t, budget, bytes, depth + 1)?,
        ]),
    })
}
fn type_term<'a>(
    t: &'a Type,
    budget: &mut Budget,
    bytes: &mut usize,
    depth: usize,
) -> Result<Term<'a>, Error> {
    budget.visit(depth)?;
    Ok(match t {
        Type::Integer => Term::Atom(Atom::Integer),
        Type::Boolean => Term::Atom(Atom::Boolean),
        Type::Bits => Term::Atom(Atom::Bits),
        Type::String => Term::Atom(Atom::String),
        Type::Any => Term::Atom(Atom::Any),
        Type::ContractBytearray => Term::Atom(Atom::ContractBytearray),
        Type::Object(k) => Term::Atom(object_atom(*k)),
        Type::Arity(n) => integer(n, budget, bytes, depth + 1)?,
        Type::Variable(n) => Term::Tuple(vec![Term::Atom(Atom::TypeVariable), small(*n)]),
        Type::Bytes(n) => Term::Tuple(vec![
            Term::Atom(Atom::Bytes),
            match n {
                None => Term::Atom(Atom::Any),
                Some(n) => {
                    bytes_used(
                        usize::try_from(n.bits().div_ceil(8)).map_err(|_| Error::ResourceLimit)?,
                        budget,
                        bytes,
                    )?;
                    small(n.clone())
                }
            },
        ]),
        Type::List(v) => Term::Tuple(vec![
            Term::Atom(Atom::List),
            type_term(v, budget, bytes, depth + 1)?,
        ]),
        Type::Tuple(v) | Type::Variant(v) => {
            if v.len() > budget.limits.max_nodes.saturating_sub(budget.nodes) {
                return Err(Error::ResourceLimit);
            }
            Term::Tuple(vec![
                Term::Atom(if matches!(t, Type::Tuple(_)) {
                    Atom::Tuple
                } else {
                    Atom::Variant
                }),
                Term::List(
                    v.iter()
                        .map(|v| type_term(v, budget, bytes, depth + 1))
                        .collect::<Result<_, _>>()?,
                ),
            ])
        }
        Type::Map(k, v) => Term::Tuple(vec![
            Term::Atom(Atom::Map),
            type_term(k, budget, bytes, depth + 1)?,
            type_term(v, budget, bytes, depth + 1)?,
        ]),
    })
}
