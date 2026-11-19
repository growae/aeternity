// Native exception terms from aebytecode 009e036 and Aeternity fa2a6d7.
// Copyright (c) 2017, 2019, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    Error, Limits,
    types::Type,
    value::{self, Value},
};
use num_bigint::BigInt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Error,
    Exit,
}

// Data and Type retain native shapes without imposing a map traversal profile.
// Rendering chooses the explicit runtime profile at the diagnostic boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Term {
    Atom(&'static str),
    Integer(BigInt),
    Binary(Vec<u8>),
    Tuple(Vec<Term>),
    List(Vec<Term>),
    Data(Box<Value>),
    Type(Box<Type>),
}
impl Term {
    pub fn tagged(tag: &'static str, mut fields: Vec<Self>) -> Self {
        fields.insert(0, Self::Atom(tag));
        Self::Tuple(fields)
    }
    pub fn number(n: impl Into<BigInt>) -> Self {
        Self::Integer(n.into())
    }
    pub fn category(&self) -> Option<&'static str> {
        match self {
            Self::Atom(s) => Some(s),
            Self::Tuple(v) => v.first().and_then(Self::category),
            _ => None,
        }
    }
    pub fn check_limits(&self, limits: Limits) -> Result<Limits, Error> {
        fn walk(t: &Term, remaining: &mut Limits, depth: usize) -> Result<(), Error> {
            if depth > remaining.max_depth || remaining.max_nodes == 0 {
                return Err(Error::ResourceLimit);
            }
            remaining.max_nodes -= 1;
            let bytes = match t {
                Term::Atom(_) => 0,
                Term::Integer(n) => {
                    usize::try_from(n.bits().div_ceil(8)).map_err(|_| Error::ResourceLimit)?
                }
                Term::Binary(b) => b.len(),
                Term::Tuple(v) | Term::List(v) => {
                    for t in v {
                        walk(t, remaining, depth + 1)?;
                    }
                    0
                }
                Term::Data(v) => {
                    let left = value::check_limits(
                        v,
                        Limits {
                            max_depth: remaining.max_depth - depth,
                            ..*remaining
                        },
                    )?;
                    remaining.max_nodes = left.max_nodes;
                    remaining.max_bytes = left.max_bytes;
                    0
                }
                Term::Type(v) => {
                    let left = value::check_type_limits(
                        v,
                        Limits {
                            max_depth: remaining.max_depth - depth,
                            ..*remaining
                        },
                    )?;
                    remaining.max_nodes = left.max_nodes;
                    remaining.max_bytes = left.max_bytes;
                    0
                }
            };
            remaining.max_bytes = remaining
                .max_bytes
                .checked_sub(bytes)
                .ok_or(Error::ResourceLimit)?;
            Ok(())
        }
        let mut remaining = limits;
        walk(self, &mut remaining, 0)?;
        Ok(remaining)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exception {
    pub class: Class,
    pub reason: Term,
}
impl Exception {
    pub fn error(reason: Term) -> Self {
        Self {
            class: Class::Error,
            reason,
        }
    }
    pub fn exit(reason: Term) -> Self {
        Self {
            class: Class::Exit,
            reason,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure<E> {
    pub error: E,
    // None is an operational failure, never a native rejection.
    pub exception: Option<Exception>,
}
