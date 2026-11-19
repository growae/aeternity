// aebytecode 009e036, aeb_heap and aeb_typerep_def.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
// Memory word padding: Copyright (c) 2018, Aeternity Anstalt.
use num_bigint::{BigInt, BigUint, Sign};
use num_traits::{One, ToPrimitive, Zero};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Type {
    Word,
    SignedWord,
    Bool,
    String,
    List(Box<Type>),
    Tuple(Vec<Type>),
    Variant(Vec<Vec<Type>>),
    Option(Box<Type>),
    Map(Box<Type>, Box<Type>),
    TypeRep,
    Function,
    ContractBytearray,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Integer(BigInt),
    Boolean(bool),
    String(Vec<u8>),
    ContractBytearray(Vec<u8>),
    Tuple(Vec<Value>),
    List(Vec<Value>),
    Variant { tag: usize, values: Vec<Value> },
    Option(Option<Box<Value>>),
    Map(BTreeMap<Value, Value>),
    Type(Type),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    BinaryTooShort,
    Bounds,
    CircularReferences,
    InvalidBoolean,
    InvalidTypeTag,
    InvalidVariantTag,
    UnsupportedType,
    UnsupportedValue,
    NonCanonical,
    ResourceLimit,
}
// Operational budgets are separate from malformed/consensus-invalid data.
// Callers must propagate ResourceLimit as an execution resource failure.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_depth: usize,
    pub max_nodes: usize,
    pub max_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_depth: 256,
            max_nodes: 1_000_000,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}
struct Budget<'a> {
    limits: Limits,
    nodes: usize,
    bytes: usize,
    execution: Option<&'a mut crate::execution::Budget>,
}
impl<'a> Budget<'a> {
    fn new(limits: Limits) -> Self {
        Self {
            limits,
            nodes: 0,
            bytes: 0,
            execution: None,
        }
    }
    fn shared(execution: &'a mut crate::execution::Budget) -> Self {
        Self {
            limits: Limits {
                max_depth: execution.depth,
                max_nodes: usize::MAX,
                max_bytes: usize::MAX,
            },
            nodes: 0,
            bytes: 0,
            execution: Some(execution),
        }
    }
    fn node(&mut self, depth: usize) -> Result<(), Error> {
        if let Some(b) = &mut self.execution {
            b.work(1).map_err(|_| Error::ResourceLimit)?;
        }
        self.nodes = self.nodes.checked_add(1).ok_or(Error::ResourceLimit)?;
        if depth > self.limits.max_depth || self.nodes > self.limits.max_nodes {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
    fn bytes(&mut self, n: usize) -> Result<(), Error> {
        if let Some(b) = &mut self.execution {
            b.bytes(n).map_err(|_| Error::ResourceLimit)?;
        }
        self.bytes = self.bytes.checked_add(n).ok_or(Error::ResourceLimit)?;
        if self.bytes > self.limits.max_bytes {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
}

// Host decoding and marshalling spend the same operational allowance as the VM.
pub fn decode_with_budget(
    ty: &Type,
    heap: &[u8],
    budget: &mut crate::execution::Budget,
) -> Result<Value, Error> {
    if heap.len() < 32 {
        return Err(Error::BinaryTooShort);
    }
    decode_value(
        ty,
        heap,
        &BigUint::from_bytes_be(&heap[..32]),
        &mut BTreeSet::new(),
        &mut Budget::shared(budget),
        0,
    )
}
pub fn encode_with_budget(
    value: &Value,
    budget: &mut crate::execution::Budget,
) -> Result<Vec<u8>, Error> {
    let mut b = Budget::shared(budget);
    b.bytes(32)?;
    let (head, body) = encode_value(value, &32u8.into(), &mut b, 0)?;
    let mut out = head.to_vec();
    out.extend(body);
    Ok(out)
}

pub fn decode(ty: &Type, heap: &[u8], limits: Limits) -> Result<Value, Error> {
    if heap.len() < 32 {
        return Err(Error::BinaryTooShort);
    }
    let ptr = BigUint::from_bytes_be(&heap[..32]);
    decode_value(
        ty,
        heap,
        &ptr,
        &mut BTreeSet::new(),
        &mut Budget::new(limits),
        0,
    )
}
pub fn decode_from_heap(
    ty: &Type,
    heap: &[u8],
    pointer: &BigUint,
    limits: Limits,
) -> Result<Value, Error> {
    decode_value(
        ty,
        heap,
        pointer,
        &mut BTreeSet::new(),
        &mut Budget::new(limits),
        0,
    )
}
pub fn decode_at(ty: &Type, heap: &[u8], base: usize, limits: Limits) -> Result<Value, Error> {
    if heap.len() < 32 {
        return Err(Error::BinaryTooShort);
    }
    let size = base.checked_add(heap.len()).ok_or(Error::ResourceLimit)?;
    if size > limits.max_bytes {
        return Err(Error::ResourceLimit);
    }
    let mut expanded = vec![0; base];
    expanded.extend_from_slice(heap);
    decode_from_heap(ty, &expanded, &BigUint::from_bytes_be(&heap[..32]), limits)
}
pub fn encode(value: &Value, limits: Limits) -> Result<Vec<u8>, Error> {
    encode_at(value, &BigUint::zero(), limits)
}
pub fn encode_at(value: &Value, base: &BigUint, limits: Limits) -> Result<Vec<u8>, Error> {
    let mut budget = Budget::new(limits);
    let (head, body) = encode_value(value, &(base + 32u8), &mut budget, 0)?;
    let mut out = head.to_vec();
    out.extend(body);
    if out.len() > limits.max_bytes {
        return Err(Error::ResourceLimit);
    }
    Ok(out)
}
pub fn decode_type(format: &[u8], limits: Limits) -> Result<Type, Error> {
    match decode(&Type::TypeRep, format, limits)? {
        Value::Type(ty) => Ok(ty),
        _ => unreachable!(),
    }
}
pub fn validate_type(format: &[u8], limits: Limits) -> Result<Type, Error> {
    let ty = decode_type(format, limits)?;
    if encode(&Value::Type(ty.clone()), limits)? != format {
        return Err(Error::NonCanonical);
    }
    Ok(ty)
}
pub fn validate_value(ty: &Type, heap: &[u8], limits: Limits) -> Result<Value, Error> {
    let value = decode(ty, heap, limits)?;
    if encode(&value, limits)? != heap {
        return Err(Error::NonCanonical);
    }
    Ok(value)
}

fn word(heap: &[u8], ptr: &BigUint) -> Result<BigUint, Error> {
    let pos = ptr.to_usize().ok_or(Error::Bounds)?;
    let end = pos.checked_add(32).ok_or(Error::Bounds)?;
    Ok(BigUint::from_bytes_be(
        heap.get(pos..end).ok_or(Error::Bounds)?,
    ))
}
fn chunk<'a>(heap: &'a [u8], ptr: &BigUint, len: &BigUint) -> Result<&'a [u8], Error> {
    let pos = ptr.to_usize().ok_or(Error::Bounds)?;
    let len = len.to_usize().ok_or(Error::Bounds)?;
    heap.get(pos..pos.checked_add(len).ok_or(Error::Bounds)?)
        .ok_or(Error::Bounds)
}
fn decode_value(
    ty: &Type,
    heap: &[u8],
    ptr: &BigUint,
    visited: &mut BTreeSet<BigUint>,
    budget: &mut Budget,
    depth: usize,
) -> Result<Value, Error> {
    budget.node(depth)?;
    let result = match ty {
        Type::Word => Value::Integer(BigInt::from(ptr.clone())),
        Type::SignedWord => {
            let modulus = BigUint::one() << 256usize;
            let n = ptr % &modulus;
            let mut i = BigInt::from(n.clone());
            if n.bit(255) {
                i -= BigInt::from(modulus);
            }
            Value::Integer(i)
        }
        Type::Bool => match ptr.to_u8() {
            Some(0) => Value::Boolean(false),
            Some(1) => Value::Boolean(true),
            _ => return Err(Error::InvalidBoolean),
        },
        Type::String | Type::ContractBytearray => {
            let len = word(heap, ptr)?;
            let data = chunk(heap, &(ptr + 32u8), &len)?;
            budget.bytes(data.len())?;
            if *ty == Type::String {
                Value::String(data.to_vec())
            } else {
                Value::ContractBytearray(data.to_vec())
            }
        }
        Type::Tuple(types) if types.is_empty() => Value::Tuple(vec![]),
        Type::Tuple(types) => {
            if !visited.insert(ptr.clone()) {
                return Err(Error::CircularReferences);
            }
            let mut out = Vec::new();
            for (i, t) in types.iter().enumerate() {
                let p = word(heap, &(ptr + BigUint::from(i) * 32u8))?;
                out.push(decode_value(t, heap, &p, visited, budget, depth + 1)?);
            }
            visited.remove(ptr);
            Value::Tuple(out)
        }
        Type::List(element) => {
            let nil = (BigUint::one() << 256usize) - 1u8;
            let mut p = ptr.clone();
            let mut heads = Vec::new();
            let mut added = Vec::new();
            while p != nil {
                budget.node(depth + 1)?;
                if !visited.insert(p.clone()) {
                    return Err(Error::CircularReferences);
                }
                added.push(p.clone());
                heads.push(decode_value(
                    element,
                    heap,
                    &word(heap, &p)?,
                    visited,
                    budget,
                    depth + 2,
                )?);
                p = word(heap, &(&p + 32u8))?;
            }
            for p in added {
                visited.remove(&p);
            }
            Value::List(heads)
        }
        Type::Option(t) => {
            let value = decode_value(
                &Type::Variant(vec![vec![], vec![*t.clone()]]),
                heap,
                ptr,
                visited,
                budget,
                depth + 1,
            )?;
            let Value::Variant { tag, mut values } = value else {
                unreachable!()
            };
            Value::Option(if tag == 0 {
                None
            } else {
                Some(Box::new(values.remove(0)))
            })
        }
        Type::Variant(constructors) => {
            let tag = word(heap, ptr)?
                .to_usize()
                .ok_or(Error::InvalidVariantTag)?;
            let args = constructors.get(tag).ok_or(Error::InvalidVariantTag)?;
            // The original inserts this location without checking it first.
            let inserted = visited.insert(ptr.clone());
            let value = decode_value(
                &Type::Tuple(args.clone()),
                heap,
                &(ptr + 32u8),
                visited,
                budget,
                depth + 1,
            )?;
            if inserted {
                visited.remove(ptr);
            }
            let Value::Tuple(values) = value else {
                unreachable!()
            };
            Value::Variant { tag, values }
        }
        Type::TypeRep => {
            if !visited.insert(ptr.clone()) {
                return Err(Error::CircularReferences);
            }
            let tag = word(heap, ptr)?.to_u8().ok_or(Error::InvalidTypeTag)?;
            let mut arg = |i: usize, t: Type| -> Result<Value, Error> {
                let p = word(heap, &(ptr + BigUint::from(i) * 32u8))?;
                decode_value(&t, heap, &p, visited, budget, depth + 1)
            };
            let ty = match tag {
                0 => Type::Word,
                1 => Type::String,
                5 => Type::TypeRep,
                7 => Type::Function,
                8 => Type::ContractBytearray,
                2 => Type::List(Box::new(as_type(arg(1, Type::TypeRep)?)?)),
                3 => Type::Tuple(as_types(arg(1, Type::List(Box::new(Type::TypeRep)))?)?),
                4 => {
                    let Value::List(rows) =
                        arg(1, Type::List(Box::new(Type::List(Box::new(Type::TypeRep)))))?
                    else {
                        return Err(Error::InvalidTypeTag);
                    };
                    Type::Variant(rows.into_iter().map(as_types).collect::<Result<_, _>>()?)
                }
                6 => Type::Map(
                    Box::new(as_type(arg(1, Type::TypeRep)?)?),
                    Box::new(as_type(arg(2, Type::TypeRep)?)?),
                ),
                _ => return Err(Error::InvalidTypeTag),
            };
            visited.remove(ptr);
            Value::Type(ty)
        }
        Type::Map(key_type, value_type) => {
            let count = word(heap, ptr)?;
            // This surprising cap is in the reference, not a resource policy.
            let n = count.to_usize().unwrap_or(usize::MAX).min(heap.len() / 64);
            let mut p = ptr + 32u8;
            let mut entries = BTreeMap::new();
            for _ in 0..n {
                let klen = word(heap, &p)?;
                p += 32u8;
                let kb = chunk(heap, &p, &klen)?;
                p += klen;
                let vlen = word(heap, &p)?;
                p += 32u8;
                let vb = chunk(heap, &p, &vlen)?;
                p += vlen;
                let k = decode_embedded(key_type, kb, budget, depth + 1)?;
                let v = decode_embedded(value_type, vb, budget, depth + 1)?;
                entries.insert(k, v);
            }
            Value::Map(entries)
        }
        Type::Function => return Err(Error::UnsupportedType),
    };
    Ok(result)
}
fn decode_embedded(t: &Type, b: &[u8], budget: &mut Budget, depth: usize) -> Result<Value, Error> {
    if b.len() < 32 {
        return Err(Error::BinaryTooShort);
    }
    decode_value(
        t,
        b,
        &BigUint::from_bytes_be(&b[..32]),
        &mut BTreeSet::new(),
        budget,
        depth,
    )
}
fn as_type(v: Value) -> Result<Type, Error> {
    if let Value::Type(t) = v {
        Ok(t)
    } else {
        Err(Error::InvalidTypeTag)
    }
}
fn as_types(v: Value) -> Result<Vec<Type>, Error> {
    if let Value::List(ts) = v {
        ts.into_iter().map(as_type).collect()
    } else {
        Err(Error::InvalidTypeTag)
    }
}
fn bits(value: &BigUint) -> [u8; 32] {
    let b = value.to_bytes_be();
    let n = b.len().min(32);
    let mut out = [0; 32];
    out[32 - n..].copy_from_slice(&b[b.len() - n..]);
    out
}
fn int_bits(value: &BigInt) -> [u8; 32] {
    let modulus = BigInt::one() << 256usize;
    let n = ((value % &modulus) + &modulus) % &modulus;
    bits(&n.to_biguint().expect("nonnegative remainder"))
}
fn encode_value(
    value: &Value,
    address: &BigUint,
    budget: &mut Budget,
    depth: usize,
) -> Result<([u8; 32], Vec<u8>), Error> {
    budget.node(depth)?;
    match value {
        Value::Integer(i) => Ok((int_bits(i), vec![])),
        Value::Boolean(_) => Err(Error::UnsupportedValue),
        Value::String(data) | Value::ContractBytearray(data) => {
            let size = data.len().checked_add(31).ok_or(Error::ResourceLimit)? / 32 * 32;
            budget.bytes(size.checked_add(32).ok_or(Error::ResourceLimit)?)?;
            let mut out = bits(&BigUint::from(data.len())).to_vec();
            out.extend(data);
            out.resize(size + 32, 0);
            Ok((bits(address), out))
        }
        Value::Tuple(values) => encode_tuple(values, address, budget, depth),
        Value::List(values) => {
            if values.is_empty() {
                return Ok(([255; 32], vec![]));
            }
            // Each cons cell precedes its head payload and then its tail cell.
            let mut out = Vec::new();
            let mut pos = address.clone();
            for (i, value) in values.iter().enumerate() {
                let (head, payload) = encode_value(value, &(&pos + 64u8), budget, depth + 1)?;
                budget.bytes(64)?;
                let next = &pos + 64u8 + BigUint::from(payload.len());
                out.extend(head);
                out.extend(if i + 1 == values.len() {
                    [255; 32]
                } else {
                    bits(&next)
                });
                out.extend(payload);
                pos = next;
            }
            Ok((bits(address), out))
        }
        Value::Variant { tag, values } => {
            let mut fields = vec![Value::Integer(BigInt::from(*tag))];
            fields.extend(values.iter().cloned());
            encode_tuple(&fields, address, budget, depth + 1)
        }
        Value::Option(value) => {
            let (tag, values) = match value {
                None => (0, vec![]),
                Some(v) => (1, vec![*v.clone()]),
            };
            encode_value(&Value::Variant { tag, values }, address, budget, depth + 1)
        }
        Value::Type(ty) => encode_value(&type_value(ty)?, address, budget, depth + 1),
        Value::Map(map) => {
            let mut pairs = Vec::new();
            for (key, value) in map {
                let (k, kbody) = encode_value(key, &32u8.into(), budget, depth + 1)?;
                let (v, vbody) = encode_value(value, &32u8.into(), budget, depth + 1)?;
                let mut kb = k.to_vec();
                kb.extend(kbody);
                let mut vb = v.to_vec();
                vb.extend(vbody);
                pairs.push((kb, vb));
            }
            pairs.sort();
            budget.bytes(32)?;
            let mut out = bits(&BigUint::from(map.len())).to_vec();
            for (k, v) in pairs {
                budget.bytes(128)?;
                out.extend(bits(&BigUint::from(k.len())));
                out.extend(k);
                out.extend(bits(&BigUint::from(v.len())));
                out.extend(v);
            }
            Ok((bits(address), out))
        }
    }
}
fn encode_tuple(
    values: &[Value],
    address: &BigUint,
    budget: &mut Budget,
    depth: usize,
) -> Result<([u8; 32], Vec<u8>), Error> {
    if values.is_empty() {
        return Ok(([0; 32], vec![]));
    }
    let headers = values.len().checked_mul(32).ok_or(Error::ResourceLimit)?;
    budget.bytes(headers)?;
    let mut heads = Vec::new();
    let mut tail = Vec::new();
    let start = address + BigUint::from(headers);
    for v in values {
        let (head, body) =
            encode_value(v, &(&start + BigUint::from(tail.len())), budget, depth + 1)?;
        heads.extend(head);
        tail.extend(body);
    }
    heads.extend(tail);
    Ok((bits(address), heads))
}
fn type_value(ty: &Type) -> Result<Value, Error> {
    let fields = match ty {
        Type::Word => vec![integer(0)],
        Type::String => vec![integer(1)],
        Type::TypeRep => vec![integer(5)],
        Type::Function => vec![integer(7)],
        Type::ContractBytearray => vec![integer(8)],
        Type::List(t) => vec![integer(2), Value::Type(*t.clone())],
        Type::Tuple(ts) => vec![
            integer(3),
            Value::List(ts.iter().cloned().map(Value::Type).collect()),
        ],
        Type::Variant(cs) => vec![
            integer(4),
            Value::List(
                cs.iter()
                    .map(|c| Value::List(c.iter().cloned().map(Value::Type).collect()))
                    .collect(),
            ),
        ],
        Type::Option(t) => return type_value(&Type::Variant(vec![vec![], vec![*t.clone()]])),
        Type::Map(k, v) => vec![integer(6), Value::Type(*k.clone()), Value::Type(*v.clone())],
        Type::SignedWord | Type::Bool => return Err(Error::UnsupportedValue),
    };
    Ok(Value::Tuple(fields))
}
fn integer(n: u8) -> Value {
    Value::Integer(BigInt::from_bytes_be(Sign::Plus, &[n]))
}
