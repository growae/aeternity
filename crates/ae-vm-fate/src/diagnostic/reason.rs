// Native FATE reason/value/type shapes, Aeternity fa2a6d7 / aebytecode 009e036.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use crate::{
    machine::{Reason, Register},
    maps::{MapOrder, Traversal},
    types::Type,
    value::{ObjectKind, ObjectPayload, Value},
};

fn atom(s: &'static str) -> Term<'static> {
    Term::Atom(Cow::Borrowed(s))
}
fn number(n: impl Into<BigInt>) -> Term<'static> {
    Term::Integer(Cow::Owned(n.into()))
}
fn tuple<'a>(tag: &'static str, mut v: Vec<Term<'a>>) -> Term<'a> {
    v.insert(0, atom(tag));
    Term::Tuple(v)
}
fn object(k: ObjectKind) -> &'static str {
    match k {
        ObjectKind::Address => "address",
        ObjectKind::Contract => "contract",
        ObjectKind::Oracle => "oracle",
        ObjectKind::OracleQuery => "oracle_query",
        ObjectKind::Channel => "channel",
    }
}

// None means a native diagnostic payload was not retained by an upstream
// adapter. It is an operational stop, not a substitute error message.
pub fn reason(
    reason: &Reason,
    order: &impl MapOrder,
    profile: Profile,
    limits: Limits,
) -> Result<Option<(Vec<u8>, Limits)>, Error> {
    let mut c = Convert {
        order,
        work: Work::new(limits),
    };
    let mut value = |v| c.value(v, 0);
    let term = match reason {
        Reason::OutOfGas => atom("out_of_gas"),
        Reason::EmptyStack(n) => tuple("pop_empty_stack", vec![number(*n)]),
        Reason::MissingMapKey => atom("missing_map_key"),
        Reason::HeadEmpty => atom("hd_on_empty_list"),
        Reason::TailEmpty => atom("tl_on_empty_list"),
        Reason::Arithmetic(s) => tuple("arithmetic_error", vec![atom(s)]),
        Reason::DivisionByZero => atom("division_by_zero"),
        Reason::ModuloByZero => atom("mod_by_zero"),
        Reason::Undefined(r) => {
            let (k, v) = match r {
                Register::Argument(v) => ("arg", v),
                Register::Variable(v) => ("var", v),
            };
            tuple("undefined_var", vec![tuple(k, vec![value(v)?])])
        }
        Reason::Type {
            operation,
            arguments,
        } => tuple(
            "type_error",
            vec![
                atom(operation),
                Term::List(arguments.iter().map(&mut value).collect::<Result<_, _>>()?),
            ],
        ),
        Reason::ValueType { value: v, expected } => {
            let v = value(v)?;
            let ty = if *expected == "list(char)" {
                tuple("list", vec![atom("char")])
            } else {
                atom(expected)
            };
            tuple("value_does_not_match_type", vec![v, ty])
        }
        Reason::BadMap {
            operation,
            value: v,
        } => tuple("bad_map", vec![atom(operation), value(v)?]),
        Reason::InvalidTupleSize(v) => tuple("invalid_tuple_size", vec![value(v)?]),
        Reason::ElementBounds(v) => tuple("element_index_out_of_bounds", vec![value(v)?]),
        Reason::BadVariantSize(n) => tuple("bad_variant_size", vec![number(*n)]),
        Reason::BadVariantTag(n) => tuple("bad_variant_tag", vec![number(*n)]),
        Reason::Unavailable(op) => tuple("primop_error", vec![atom(op), atom("not_supported")]),
        Reason::AuthTransactionType(s) => tuple("auth_tx_type_not_handled", vec![atom(s)]),
        Reason::Disabled(op) => tuple("disabled_operation", vec![atom(op.info().name)]),
        Reason::NotAllowedInAuth(op) => {
            tuple("not_allowed_in_auth_context", vec![atom(op.info().name)])
        }
        Reason::NotAllowedOffchain(op) => tuple("not_allowed_offchain", vec![atom(op.info().name)]),
        Reason::UserExit(s) => {
            c.work.visit(0)?;
            c.work.bytes(s.len())?;
            return Ok(Some((s.clone(), c.work.remaining())));
        }
        Reason::MissingFunction(v) => tuple("trying_to_call_function", vec![value(v)?]),
        Reason::MissingBlock(v) => tuple("trying_to_reach_bb", vec![value(v)?]),
        Reason::InvalidInitCall => atom("invalid_init_call"),
        Reason::PrivateFunction(v) => tuple("function_is_private", vec![value(v)?]),
        Reason::NonPayableFunction(v) => tuple("function_is_not_payable", vec![value(v)?]),
        Reason::FunctionArity { given, expected } => tuple(
            "function_arity_mismatch",
            vec![number(*given), number(*expected)],
        ),
        Reason::CallType {
            arguments,
            expected,
        } => {
            let args = arguments.iter().map(&mut value).collect::<Result<_, _>>()?;
            let types = expected
                .iter()
                .map(|t| c.ty(t, 0))
                .collect::<Result<_, _>>()?;
            tuple(
                "value_does_not_match_type",
                vec![Term::List(args), Term::List(types)],
            )
        }
        Reason::MissingContract(s) => tuple(
            "trying_to_call_contract",
            vec![Term::Binary(Cow::Borrowed(s))],
        ),
        Reason::ReentrantCall => atom("reentrant_call"),
        Reason::RemoteTypeMismatch => atom("remote_type_mismatch"),
        Reason::BadGasCap(n) => tuple(
            "call_error",
            vec![tuple("bad_gas_cap", vec![Term::Integer(Cow::Borrowed(n))])],
        ),
        Reason::CallError(s) => tuple("call_error", vec![atom(s)]),
        Reason::Primop { operation, reason } => {
            tuple("primop_error", vec![atom(operation), atom(reason)])
        }
        Reason::NameEncoding { operation, error } => {
            let Some((r, limits)) = idna_error(error, c.work.remaining())? else {
                return Ok(None);
            };
            return native_abort(
                &tuple("primop_error", vec![atom(operation), r]),
                profile,
                limits,
            )
            .map(Some);
        }
        Reason::BadReturnType { value: v, expected } => {
            let v = value(v)?;
            let t = c.ty(expected, 0)?;
            tuple("bad_return_type", vec![v, t])
        }
        Reason::LogIllegalInt(n) => tuple("log_illegal_int", vec![Term::Integer(Cow::Borrowed(n))]),
        Reason::LogIllegalBits => atom("log_illegal_bits"),
        Reason::NotImplemented(s) => tuple("op_not_implemented", vec![atom(s)]),
        Reason::DisabledName(s) => tuple("disabled_operation", vec![atom(s)]),
    };
    native_abort(&term, profile, c.work.remaining()).map(Some)
}
struct Convert<'a, O> {
    order: &'a O,
    work: Work,
}
impl<O: MapOrder> Convert<'_, O> {
    fn values<'a>(&mut self, vs: &'a [Value], d: usize) -> Result<Vec<Term<'a>>, Error> {
        vs.iter().map(|v| self.value(v, d)).collect()
    }
    fn map<'a>(&mut self, map: &'a crate::maps::Map, d: usize) -> Result<Term<'a>, Error> {
        self.work.visit(d)?;
        if map.len() > self.work.remaining().max_nodes {
            return Err(Error::ResourceLimit);
        }
        self.work.bytes(
            map.len()
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or(Error::ResourceLimit)?,
        )?;
        let keys: Vec<_> = map.keys().collect();
        let indices = crate::maps::ordered_indices::<std::convert::Infallible>(
            &keys,
            self.order,
            Traversal::Fold,
            &mut self.work.budget,
        )
        .map_err(|e| match e {
            crate::maps::MapError::Data(e) => e,
            _ => Error::InvalidOrderProfile,
        })?;
        let entries: Vec<_> = map.iter().collect();
        Ok(Term::Map(
            indices
                .into_iter()
                .map(|i| {
                    let (k, v) = entries[i];
                    Ok((self.value(k, d + 1)?, self.value(v, d + 1)?))
                })
                .collect::<Result<_, Error>>()?,
        ))
    }
    fn payload<'a>(&mut self, p: &'a ObjectPayload, d: usize) -> Result<Term<'a>, Error> {
        self.work.visit(d)?;
        Ok(match p {
            ObjectPayload::Bytes(s) => Term::Binary(Cow::Borrowed(s)),
            ObjectPayload::List(v) => Term::List(
                v.iter()
                    .map(|p| self.payload(p, d + 1))
                    .collect::<Result<_, _>>()?,
            ),
        })
    }
    fn value<'a>(&mut self, v: &'a Value, d: usize) -> Result<Term<'a>, Error> {
        self.work.visit(d)?;
        Ok(match v {
            Value::Integer(n) => Term::Integer(Cow::Borrowed(n)),
            Value::Boolean(b) => atom(if *b { "true" } else { "false" }),
            Value::Bits(n) => tuple("bits", vec![Term::Integer(Cow::Borrowed(n))]),
            Value::BitsNil => tuple("bits", vec![Term::List(vec![])]),
            Value::String(s) => Term::Binary(Cow::Borrowed(s)),
            Value::Bytes(s) => tuple("bytes", vec![Term::Binary(Cow::Borrowed(s))]),
            Value::ContractBytearray(s) => {
                tuple("contract_bytearray", vec![Term::Binary(Cow::Borrowed(s))])
            }
            Value::Object(k, p) => tuple(object(*k), vec![self.payload(p, d + 1)?]),
            Value::Tuple(v) => tuple("tuple", vec![Term::Tuple(self.values(v, d + 1)?)]),
            Value::List(v) => Term::List(self.values(v, d + 1)?),
            Value::Map(m) => self.map(m, d + 1)?,
            Value::StoreMap { id, cache } => tuple(
                "store_map",
                vec![self.map(cache, d + 1)?, Term::Integer(Cow::Borrowed(id))],
            ),
            Value::Tombstone => atom("__DELETED__"),
            Value::Void => atom("void"),
            Value::MaxTtl => atom("max_ttl"),
            Value::Variant {
                arities,
                tag,
                values,
            } => {
                let mut a = Vec::new();
                for n in arities {
                    self.work.visit(d + 1)?;
                    a.push(Term::Integer(Cow::Borrowed(n)));
                }
                tuple(
                    "variant",
                    vec![
                        Term::List(a),
                        number(*tag),
                        Term::Tuple(self.values(values, d + 1)?),
                    ],
                )
            }
            Value::TypeRep(t) => tuple("typerep", vec![self.ty(t, d + 1)?]),
        })
    }
    fn ty<'a>(&mut self, t: &'a Type, d: usize) -> Result<Term<'a>, Error> {
        self.work.visit(d)?;
        Ok(match t {
            Type::Integer => atom("integer"),
            Type::Boolean => atom("boolean"),
            Type::Bits => atom("bits"),
            Type::String => atom("string"),
            Type::ContractBytearray => atom("contract_bytearray"),
            Type::Any => atom("any"),
            Type::Object(k) => atom(object(*k)),
            Type::Bytes(n) => tuple(
                "bytes",
                vec![match n {
                    None => atom("any"),
                    Some(n) => {
                        self.work.bytes(
                            usize::try_from(n.bits().div_ceil(8))
                                .map_err(|_| Error::ResourceLimit)?,
                        )?;
                        number(n.clone())
                    }
                }],
            ),
            Type::Variable(n) => tuple("tvar", vec![number(*n)]),
            Type::List(t) => tuple("list", vec![self.ty(t, d + 1)?]),
            Type::Tuple(v) | Type::Variant(v) => tuple(
                if matches!(t, Type::Tuple(_)) {
                    "tuple"
                } else {
                    "variant"
                },
                vec![Term::List(
                    v.iter()
                        .map(|t| self.ty(t, d + 1))
                        .collect::<Result<_, _>>()?,
                )],
            ),
            Type::Map(k, v) => tuple("map", vec![self.ty(k, d + 1)?, self.ty(v, d + 1)?]),
            Type::Arity(n) => Term::Integer(Cow::Borrowed(n)),
        })
    }
}
