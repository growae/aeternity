// Behavioral reference: aebytecode 009e036, aeb_fate_encoding.erl.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use crate::{
    Budget, Error, Limits,
    types::Type,
    value::{self, ObjectKind, ObjectPayload, Value},
};
use ae_codec::rlp::{self, Rlp};
use num_bigint::{BigInt, BigUint, Sign};
use num_traits::{ToPrimitive, Zero};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeException {
    Error(&'static str),
    Exit(&'static str),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodingFailure {
    pub error: Error,
    // Operational exhaustion has no original VM exception.
    pub exception: Option<DecodeException>,
}
// VM invocation deserializes before constructing an engine state. Preserve
// native error/exit categories rather than converting them into gas aborts.
pub fn decode_for_vm(bytes: &[u8], limits: Limits) -> Result<Value, DecodingFailure> {
    let (value, rest) = decode_one_for_vm(bytes, limits)?;
    if !rest.is_empty() {
        return Err(DecodingFailure {
            error: Error::TrailingBytes,
            exception: Some(DecodeException::Error("badmatch")),
        });
    }
    Ok(value)
}
pub fn decode_one_for_vm(bytes: &[u8], limits: Limits) -> Result<(Value, &[u8]), DecodingFailure> {
    let mut decoder = Decoder::new(bytes, limits).map_err(operational_decode)?;
    let value = decoder.value(0).map_err(|error| decoder.failure(error))?;
    Ok((value, &bytes[decoder.pos..]))
}
pub fn decode_type_one_for_vm(
    bytes: &[u8],
    limits: Limits,
) -> Result<(Type, &[u8]), DecodingFailure> {
    let mut decoder = Decoder::new(bytes, limits).map_err(operational_decode)?;
    let ty = decoder.ty(0).map_err(|error| decoder.failure(error))?;
    Ok((ty, &bytes[decoder.pos..]))
}
fn operational_decode(error: Error) -> DecodingFailure {
    DecodingFailure {
        error,
        exception: None,
    }
}

pub fn decode(bytes: &[u8], limits: Limits) -> Result<Value, Error> {
    let (value, rest) = decode_one(bytes, limits)?;
    if !rest.is_empty() {
        return Err(Error::TrailingBytes);
    }
    Ok(value)
}
pub fn decode_one(bytes: &[u8], limits: Limits) -> Result<(Value, &[u8]), Error> {
    let (value, rest, _) = decode_one_counted(bytes, limits)?;
    Ok((value, rest))
}
pub(crate) fn decode_one_counted(
    bytes: &[u8],
    limits: Limits,
) -> Result<(Value, &[u8], usize), Error> {
    let mut decoder = Decoder::new(bytes, limits)?;
    let value = decoder.value(0)?;
    Ok((value, &bytes[decoder.pos..], decoder.budget.nodes))
}
pub fn decode_type(bytes: &[u8], limits: Limits) -> Result<Type, Error> {
    let (ty, rest) = decode_type_one(bytes, limits)?;
    if !rest.is_empty() {
        return Err(Error::TrailingBytes);
    }
    Ok(ty)
}
pub fn decode_type_one(bytes: &[u8], limits: Limits) -> Result<(Type, &[u8]), Error> {
    let (ty, rest, _) = decode_type_one_counted(bytes, limits)?;
    Ok((ty, rest))
}
pub(crate) fn decode_type_one_counted(
    bytes: &[u8],
    limits: Limits,
) -> Result<(Type, &[u8], usize), Error> {
    let mut decoder = Decoder::new(bytes, limits)?;
    let ty = decoder.ty(0)?;
    Ok((ty, &bytes[decoder.pos..], decoder.budget.nodes))
}
pub fn encode(value: &Value, limits: Limits) -> Result<Vec<u8>, Error> {
    Ok(encode_counted(value, limits)?.0)
}
pub(crate) fn encode_counted(value: &Value, limits: Limits) -> Result<(Vec<u8>, usize), Error> {
    encode_vm_counted(value, limits).map_err(|e| e.error)
}
pub struct EncodingFailure {
    pub error: Error,
    pub native_exception: &'static str,
}
// Preserve the original serialization exception category for VM host helpers
// whose serialization occurs outside a catch boundary.
pub fn encode_for_vm(value: &Value, limits: Limits) -> Result<Vec<u8>, EncodingFailure> {
    encode_vm_counted(value, limits).map(|(bytes, _)| bytes)
}
pub(crate) fn encode_vm_counted(
    value: &Value,
    limits: Limits,
) -> Result<(Vec<u8>, usize), EncodingFailure> {
    let mut encoder = Encoder::new(limits);
    encoder.value(value, 0).map_err(|error| EncodingFailure {
        error,
        native_exception: encoder.native_exception,
    })?;
    Ok((encoder.bytes, encoder.budget.nodes))
}
pub fn encode_type(ty: &Type, limits: Limits) -> Result<Vec<u8>, Error> {
    Ok(encode_type_counted(ty, limits)?.0)
}
pub(crate) fn encode_type_counted(ty: &Type, limits: Limits) -> Result<(Vec<u8>, usize), Error> {
    let mut encoder = Encoder::new(limits);
    encoder.ty(ty, 0)?;
    Ok((encoder.bytes, encoder.budget.nodes))
}
pub(crate) fn encode_tuple_type_counted(
    fields: &[Type],
    limits: Limits,
) -> Result<(Vec<u8>, usize), Error> {
    let mut encoder = Encoder::new(limits);
    encoder.budget.visit(0)?;
    encoder.type_fields(fields, true, 0)?;
    Ok((encoder.bytes, encoder.budget.nodes))
}

struct Decoder<'a> {
    bytes: &'a [u8],
    pos: usize,
    budget: Budget,
    native_exception: DecodeException,
}
impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8], limits: Limits) -> Result<Self, Error> {
        if bytes.len() > limits.max_bytes {
            return Err(Error::ResourceLimit);
        }
        Ok(Self {
            bytes,
            pos: 0,
            budget: Budget::new(limits),
            native_exception: DecodeException::Error("function_clause"),
        })
    }
    fn failure(&self, error: Error) -> DecodingFailure {
        DecodingFailure {
            error,
            exception: (error != Error::ResourceLimit).then_some(self.native_exception),
        }
    }
    fn fail<T>(&mut self, error: Error, reason: &'static str) -> Result<T, Error> {
        self.native_exception = DecodeException::Error(reason);
        Err(error)
    }
    fn exit<T>(&mut self, reason: &'static str) -> Result<T, Error> {
        self.native_exception = DecodeException::Exit(reason);
        Err(Error::InvalidEncoding)
    }
    fn string(&mut self, n: &BigUint) -> Result<Vec<u8>, Error> {
        let Some(n) = n.to_usize().filter(|n| *n <= self.bytes.len() - self.pos) else {
            return self.fail(Error::Truncated, "badarg");
        };
        Ok(self.take(n)?.to_vec())
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self.pos.checked_add(n).ok_or(Error::Truncated)?;
        let bytes = self.bytes.get(self.pos..end).ok_or(Error::Truncated)?;
        self.pos = end;
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn rlp(&mut self, depth: usize) -> Result<Rlp<'a>, Error> {
        self.budget.visit(depth)?;
        let limits = rlp::DecodeLimits {
            max_input_bytes: self.budget.limits.max_bytes,
            max_depth: self.budget.limits.max_depth.saturating_sub(depth),
            max_items: self
                .budget
                .limits
                .max_nodes
                .saturating_sub(self.budget.nodes),
        };
        let (value, rest) =
            rlp::decode_one_with_limits(&self.bytes[self.pos..], limits).map_err(|e| {
                self.native_exception = DecodeException::Error(match e {
                    rlp::DecodeError::EmptyInput => "function_clause",
                    rlp::DecodeError::LeadingZeroLength => "leading_zeroes_in_size",
                    _ => "badmatch",
                });
                match e {
                    rlp::DecodeError::ResourceLimit => Error::ResourceLimit,
                    _ => Error::InvalidEncoding,
                }
            })?;
        self.pos = self.bytes.len() - rest.len();
        Ok(value)
    }
    fn rlp_integer(&mut self, depth: usize) -> Result<BigUint, Error> {
        let start = self.pos;
        let Rlp::Bytes(bytes) = self.rlp(depth)? else {
            return self.fail(Error::InvalidEncoding, "badarg");
        };
        let n = BigUint::from_bytes_be(bytes);
        if rlp::encode(&Rlp::Bytes(&n.to_bytes_be())) != self.bytes[start..self.pos] {
            return self.fail(Error::NonCanonical, "none_unique_encoding");
        }
        Ok(n)
    }
    fn integer(&mut self, depth: usize) -> Result<BigInt, Error> {
        match self.value(depth)? {
            Value::Integer(n) => Ok(n),
            _ => self.fail(Error::InvalidEncoding, "badmatch"),
        }
    }
    fn count(&self, n: &BigUint) -> usize {
        // A collection cannot fit more values than remaining bytes. Still
        // visit its prefix: a malformed nested value fails before exhaustion.
        let bound = (self.bytes.len() - self.pos).saturating_add(1);
        n.to_usize().unwrap_or(bound).min(bound)
    }
    fn elements(&mut self, n: usize, depth: usize) -> Result<Vec<Value>, Error> {
        let mut values = Vec::new();
        for _ in 0..n {
            values.push(self.value(depth)?);
        }
        Ok(values)
    }
    fn value(&mut self, depth: usize) -> Result<Value, Error> {
        self.budget.visit(depth)?;
        let tag = self.byte()?;
        if tag & 1 == 0 {
            if tag == 0x80 {
                return self.fail(Error::InvalidEncoding, "illegal_sign");
            }
            let n = BigInt::from((tag & 0x7f) >> 1);
            return Ok(Value::Integer(if tag & 0x80 == 0 { n } else { -n }));
        }
        Ok(match tag {
            0x6f | 0xef | 0x4f | 0xcf => {
                let n = self.rlp_integer(depth + 1)?;
                if tag == 0xcf && n.is_zero() {
                    return self.fail(Error::InvalidEncoding, "illegal_parameter");
                }
                let n = BigInt::from(n);
                match tag {
                    0x6f => Value::Integer(n + 64),
                    0xef => Value::Integer(-n - 64),
                    0x4f => Value::Bits(n),
                    _ => Value::Bits(-n),
                }
            }
            0x01 | 0x8f => {
                let Some(n) = self.integer(depth + 1)?.to_biguint() else {
                    return self.fail(Error::InvalidEncoding, "badmatch");
                };
                let n = if tag == 1 { n + 64u8 } else { n };
                let bytes = self.string(&n)?;
                if tag == 1 {
                    Value::String(bytes)
                } else {
                    Value::ContractBytearray(bytes)
                }
            }
            0x5f => Value::String(vec![]),
            0x3f => Value::Tuple(vec![]),
            0x7f => Value::Boolean(false),
            0xff => Value::Boolean(true),
            0x9f => {
                let object = self.byte()?;
                if object == 1 {
                    match self.value(depth + 1)? {
                        Value::String(bytes) => Value::Bytes(bytes),
                        _ => return self.fail(Error::InvalidEncoding, "badmatch"),
                    }
                } else {
                    let rlp = self.rlp(depth + 1)?;
                    let kind = ObjectKind::from_tag(object).inspect_err(|_| {
                        self.native_exception = DecodeException::Error("case_clause");
                    })?;
                    Value::Object(kind, own_payload(rlp, &mut self.budget, depth + 1)?)
                }
            }
            0x0b | 0x1f => {
                let n = self.rlp_integer(depth + 1)? + 16u8;
                let n = self.count(&n);
                let values = self.elements(n, depth + 1)?;
                if tag == 0x0b {
                    Value::Tuple(values)
                } else {
                    Value::List(values)
                }
            }
            0x2f => {
                let n = self.rlp_integer(depth + 1)? * 2u8;
                let n = self.count(&n);
                let values = self.elements(n, depth + 1)?;
                let mut map = BTreeMap::new();
                let mut keys = Vec::new();
                let mut values = values.into_iter();
                while let Some(k) = values.next() {
                    value::valid_key(&k, &mut self.budget, depth + 1).inspect_err(|_| {
                        self.native_exception = DecodeException::Error("map_as_key_in_map");
                    })?;
                    keys.push(k.clone());
                    map.insert(k, values.next().ok_or(Error::InvalidEncoding)?);
                }
                let rows = value::sorted(&map, &mut self.budget, depth + 1).inspect_err(|_| {
                    self.native_exception = DecodeException::Error("function_clause");
                })?;
                if rows.iter().map(|(k, _)| *k).ne(keys.iter()) {
                    return self.fail(Error::NonCanonical, "unknown_map_serialization_format");
                }
                Value::Map(map)
            }
            0xbf => Value::StoreMap {
                id: self.rlp_integer(depth + 1)?.into(),
                cache: BTreeMap::new(),
            },
            0xaf => {
                let arities = self.rlp(depth + 1)?;
                let Some(&tag) = self.bytes.get(self.pos) else {
                    return self.fail(Error::Truncated, "badmatch");
                };
                self.pos += 1;
                let Rlp::Bytes(arities) = arities else {
                    return self.fail(Error::InvalidEncoding, "badarg");
                };
                let tag = usize::from(tag);
                if tag > arities.len() {
                    return self.exit("too_large_tag_in_variant");
                }
                let Value::Tuple(values) = self.value(depth + 1)? else {
                    return self.fail(Error::InvalidEncoding, "badmatch");
                };
                if tag == arities.len() {
                    return self.fail(Error::InvalidEncoding, "function_clause");
                }
                if arities.get(tag).map(|n| usize::from(*n)) != Some(values.len()) {
                    return self.exit("tag_does_not_match_type_in_variant");
                }
                if arities.len()
                    > self
                        .budget
                        .limits
                        .max_nodes
                        .saturating_sub(self.budget.nodes)
                {
                    return Err(Error::ResourceLimit);
                }
                self.budget.nodes += arities.len();
                Value::Variant {
                    arities: arities.iter().map(|n| BigInt::from(*n)).collect(),
                    tag,
                    values,
                }
            }
            t if t & 3 == 1 => {
                let n = usize::from(t >> 2);
                Value::String(self.string(&n.into())?)
            }
            t if t & 15 == 3 => Value::List(self.elements(usize::from(t >> 4), depth + 1)?),
            t if t & 15 == 11 => Value::Tuple(self.elements(usize::from(t >> 4), depth + 1)?),
            t if is_type_tag(t) => {
                self.pos -= 1;
                Value::TypeRep(self.ty(depth + 1)?)
            }
            _ => return Err(Error::InvalidEncoding),
        })
    }
    fn ty(&mut self, depth: usize) -> Result<Type, Error> {
        self.budget.visit(depth)?;
        Ok(match self.byte()? {
            0x07 => Type::Integer,
            0x17 => Type::Boolean,
            0x57 => Type::Bits,
            0x77 => Type::String,
            0xa7 => Type::ContractBytearray,
            0xf7 => Type::Any,
            0xe7 => Type::Variable(self.byte()?),
            0x27 => Type::List(Box::new(self.ty(depth + 1)?)),
            tag @ (0x37 | 0x87) => {
                let n = self.byte()?;
                let mut ts = Vec::new();
                for _ in 0..n {
                    ts.push(self.ty(depth + 1)?);
                }
                if tag == 0x37 {
                    Type::Tuple(ts)
                } else {
                    Type::Variant(ts)
                }
            }
            0x47 => {
                let tag = self.byte()?;
                Type::Object(ObjectKind::from_tag(tag).inspect_err(|_| {
                    self.native_exception = DecodeException::Error("case_clause");
                })?)
            }
            0x67 => Type::Map(Box::new(self.ty(depth + 1)?), Box::new(self.ty(depth + 1)?)),
            0x97 => {
                let n = self.integer(depth + 1)?;
                if n == BigInt::from(-1) {
                    Type::Bytes(None)
                } else {
                    let Some(n) = n.to_biguint() else {
                        return self.fail(Error::InvalidEncoding, "if_clause");
                    };
                    Type::Bytes(Some(n))
                }
            }
            _ => return Err(Error::InvalidEncoding),
        })
    }
}
fn own_payload(rlp: Rlp<'_>, budget: &mut Budget, depth: usize) -> Result<ObjectPayload, Error> {
    budget.visit(depth)?;
    Ok(match rlp {
        Rlp::Bytes(bytes) => ObjectPayload::Bytes(bytes.to_vec()),
        Rlp::List(values) => ObjectPayload::List(
            values
                .into_iter()
                .map(|v| own_payload(v, budget, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
    })
}
fn is_type_tag(tag: u8) -> bool {
    matches!(
        tag,
        0x07 | 0x17 | 0x27 | 0x37 | 0x47 | 0x57 | 0x67 | 0x77 | 0x87 | 0x97 | 0xa7 | 0xe7 | 0xf7
    )
}

struct Encoder {
    bytes: Vec<u8>,
    budget: Budget,
    native_exception: &'static str,
}
impl Encoder {
    fn new(limits: Limits) -> Self {
        Self {
            bytes: Vec::new(),
            budget: Budget::new(limits),
            native_exception: "function_clause",
        }
    }
    fn fail(&mut self, error: Error, native_exception: &'static str) -> Result<(), Error> {
        self.native_exception = native_exception;
        Err(error)
    }
    fn put(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if bytes.len()
            > self
                .budget
                .limits
                .max_bytes
                .saturating_sub(self.bytes.len())
        {
            return Err(Error::ResourceLimit);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn rlp_bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if bytes.len()
            > self
                .budget
                .limits
                .max_bytes
                .saturating_sub(self.bytes.len())
        {
            return Err(Error::ResourceLimit);
        }
        self.put(&rlp::encode(&Rlp::Bytes(bytes)))
    }
    fn rlp_integer(&mut self, n: &BigUint) -> Result<(), Error> {
        if n.bits().div_ceil(8) > self.budget.limits.max_bytes as u64 {
            return Err(Error::ResourceLimit);
        }
        self.rlp_bytes(&n.to_bytes_be())
    }
    fn integer(&mut self, n: &BigInt) -> Result<(), Error> {
        let negative = n.sign() == Sign::Minus;
        let magnitude = n.magnitude();
        if magnitude < &BigUint::from(64u8) {
            self.put(&[(magnitude.to_u8().ok_or(Error::InvalidValue)? << 1)
                | if negative { 0x80 } else { 0 }])
        } else {
            self.put(&[if negative { 0xef } else { 0x6f }])?;
            self.rlp_integer(&(magnitude - 64u8))
        }
    }
    fn string(&mut self, bytes: &[u8]) -> Result<(), Error> {
        match bytes.len() {
            0 => return self.put(&[0x5f]),
            n @ 1..64 => self.put(&[((n as u8) << 2) | 1])?,
            n => {
                self.put(&[1])?;
                self.integer(&BigInt::from(n - 64))?;
            }
        }
        self.put(bytes)
    }
    fn elements(&mut self, values: &[Value], tuple: bool, depth: usize) -> Result<(), Error> {
        match values.len() {
            0 if tuple => self.put(&[0x3f])?,
            n @ 0..16 => self.put(&[((n as u8) << 4) | if tuple { 0x0b } else { 3 }])?,
            n => {
                self.put(&[if tuple { 0x0b } else { 0x1f }])?;
                self.rlp_integer(&(n - 16).into())?;
            }
        }
        for value in values {
            self.value(value, depth)?;
        }
        Ok(())
    }
    fn value(&mut self, value: &Value, depth: usize) -> Result<(), Error> {
        self.budget.visit(depth)?;
        match value {
            Value::Integer(n) => self.integer(n),
            Value::Boolean(b) => self.put(&[if *b { 0xff } else { 0x7f }]),
            Value::Bits(n) => {
                self.put(&[if n.sign() == Sign::Minus { 0xcf } else { 0x4f }])?;
                self.rlp_integer(n.magnitude())
            }
            Value::String(bytes) => self.string(bytes),
            Value::Bytes(bytes) => {
                self.put(&[0x9f, 1])?;
                self.string(bytes)
            }
            Value::Object(kind, ObjectPayload::Bytes(bytes)) => {
                self.put(&[0x9f, kind.tag()])?;
                self.rlp_bytes(bytes)
            }
            Value::Object(_, ObjectPayload::List(_)) => Err(Error::InvalidValue),
            Value::ContractBytearray(bytes) => {
                self.put(&[0x8f])?;
                self.integer(&bytes.len().into())?;
                self.put(bytes)
            }
            Value::Tuple(values) => self.elements(values, true, depth + 1),
            Value::List(values) => self.elements(values, false, depth + 1),
            Value::Map(map) => {
                for key in map.keys() {
                    if let Err(error) = value::valid_key(key, &mut self.budget, depth + 1) {
                        return self.fail(error, "map_as_key_in_map");
                    }
                }
                let rows = value::sorted(map, &mut self.budget, depth + 1)?;
                self.put(&[0x2f])?;
                self.rlp_integer(&map.len().into())?;
                for (key, value) in rows {
                    self.value(key, depth + 1)?;
                    self.value(value, depth + 1)?;
                }
                Ok(())
            }
            Value::StoreMap { id, cache } if cache.is_empty() => {
                let id = id.to_biguint().ok_or(Error::InvalidValue)?;
                self.put(&[0xbf])?;
                self.rlp_integer(&id)
            }
            Value::StoreMap { .. }
            | Value::Tombstone
            | Value::Void
            | Value::MaxTtl
            | Value::BitsNil => Err(Error::InvalidValue),
            Value::Variant {
                arities,
                tag,
                values,
            } => {
                if arities.len()
                    > self
                        .budget
                        .limits
                        .max_nodes
                        .saturating_sub(self.budget.nodes)
                    || arities.len()
                        > self
                            .budget
                            .limits
                            .max_bytes
                            .saturating_sub(self.bytes.len())
                {
                    return Err(Error::ResourceLimit);
                }
                self.budget.nodes += arities.len();
                // Original encoding first asserts that filtering out arities
                // >=256 did not change the list, then checks tag/field count,
                // and only then converts negative arities to binary bytes.
                if arities.iter().any(|n| n >= &BigInt::from(256)) {
                    return self.fail(Error::InvalidValue, "badmatch");
                }
                if arities.get(*tag) != Some(&BigInt::from(values.len())) {
                    return self.fail(Error::InvalidValue, "if_clause");
                }
                if arities.iter().any(|n| n.sign() == Sign::Minus) {
                    return self.fail(Error::InvalidValue, "badarg");
                }
                self.put(&[0xaf])?;
                let arities = arities
                    .iter()
                    .map(|n| n.to_u8().ok_or(Error::InvalidValue))
                    .collect::<Result<Vec<_>, _>>()?;
                self.rlp_bytes(&arities)?;
                self.put(&[*tag as u8])?;
                self.elements(values, true, depth + 1)
            }
            Value::TypeRep(ty) => self.ty(ty, depth + 1),
        }
    }
    fn ty(&mut self, ty: &Type, depth: usize) -> Result<(), Error> {
        self.budget.visit(depth)?;
        match ty {
            Type::Integer => self.put(&[0x07]),
            Type::Boolean => self.put(&[0x17]),
            Type::Bits => self.put(&[0x57]),
            Type::String => self.put(&[0x77]),
            Type::ContractBytearray => self.put(&[0xa7]),
            Type::Any => self.put(&[0xf7]),
            Type::Variable(n) => self.put(&[0xe7, *n]),
            Type::Object(kind) => self.put(&[0x47, kind.tag()]),
            Type::List(ty) => {
                self.put(&[0x27])?;
                self.ty(ty, depth + 1)
            }
            Type::Map(k, v) => {
                self.put(&[0x67])?;
                self.ty(k, depth + 1)?;
                self.ty(v, depth + 1)
            }
            Type::Tuple(ts) => self.type_fields(ts, true, depth),
            Type::Variant(ts) => self.type_fields(ts, false, depth),
            Type::Bytes(n) => {
                self.put(&[0x97])?;
                self.integer(
                    &n.as_ref()
                        .map_or_else(|| BigInt::from(-1), |n| BigInt::from(n.clone())),
                )
            }
            Type::Arity(_) => Err(Error::InvalidType),
        }
    }
    fn type_fields(&mut self, fields: &[Type], tuple: bool, depth: usize) -> Result<(), Error> {
        let Ok(n) = u8::try_from(fields.len()) else {
            return self.fail(
                Error::InvalidType,
                if tuple { "case_clause" } else { "if_clause" },
            );
        };
        self.put(&[if tuple { 0x37 } else { 0x87 }, n])?;
        for t in fields {
            self.ty(t, depth + 1)?;
        }
        Ok(())
    }
}
