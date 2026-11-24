use crate::{Error, integer};
use num_bigint::BigInt;
use num_traits::{FromPrimitive, ToPrimitive};
use serde_json::Value;
use std::{cmp::Ordering, str::FromStr};

pub const MAX_ELEMENTS: usize = 1024;
pub const MAX_BYTES: usize = 65536;

// Float bits are retained exactly. Construction of a Proof checks finiteness
// and integer/resource bounds before these values enter a share record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Number {
    Integer(BigInt),
    Float(u64),
}
impl Number {
    fn compare_integer(&self, bound: &BigInt) -> Ordering {
        match self {
            Self::Integer(i) => i.cmp(bound),
            Self::Float(bits) => {
                let f = f64::from_bits(*bits);
                match BigInt::from_f64(f).expect("finite proof number").cmp(bound) {
                    Ordering::Equal => f.fract().partial_cmp(&0.0).expect("finite fraction"),
                    other => other,
                }
            }
        }
    }
    fn native_equal(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a == b,
            // Pinned OTP 26 exact matching distinguishes integers and floats,
            // but considers both signs of floating zero equal.
            (Self::Float(a), Self::Float(b)) => f64::from_bits(*a) == f64::from_bits(*b),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proof {
    numbers: Box<[Number]>,
    retained_bytes: usize,
}
impl Proof {
    pub fn new(numbers: Vec<Number>) -> Result<Self, Error> {
        if numbers.len() > MAX_ELEMENTS {
            return Err(Error::Limit("proof elements"));
        }
        let mut retained_bytes = 0;
        for n in &numbers {
            // Fixed structural charge plus numeric payload; an operational
            // budget, independent of platform allocator layout.
            retained_bytes += 64
                + match n {
                    Number::Integer(i) => {
                        integer(i)?;
                        i.bits().div_ceil(8) as usize
                    }
                    Number::Float(bits) => {
                        if !f64::from_bits(*bits).is_finite() {
                            return Err(Error::Float);
                        }
                        8
                    }
                };
            if retained_bytes > MAX_BYTES {
                return Err(Error::Limit("proof bytes"));
            }
        }
        Ok(Self {
            numbers: numbers.into_boxed_slice(),
            retained_bytes,
        })
    }
    // Called after wire validation. This preserves its numeric domain; it
    // deliberately performs neither wire-option nor Cuckoo validation.
    pub fn from_json(value: &Value) -> Result<Self, Error> {
        let a = value.as_array().ok_or(Error::Proof)?;
        if a.len() > MAX_ELEMENTS {
            return Err(Error::Limit("proof elements"));
        }
        let mut numbers = Vec::with_capacity(a.len());
        for v in a {
            let n = v.as_number().ok_or(Error::Proof)?;
            let s = n.as_str();
            if s.len() > 5000 {
                return Err(Error::Limit("proof number text"));
            }
            numbers.push(if s.contains(['.', 'e', 'E']) {
                Number::Float(
                    n.as_f64()
                        .filter(|f| f.is_finite())
                        .ok_or(Error::Float)?
                        .to_bits(),
                )
            } else {
                Number::Integer(BigInt::from_str(s).map_err(|_| Error::Integer)?)
            });
        }
        Self::new(numbers)
    }
    pub fn numbers(&self) -> &[Number] {
        &self.numbers
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    pub fn to_json(&self) -> Value {
        Value::Array(
            self.numbers
                .iter()
                .map(|n| match n {
                    Number::Integer(i) => {
                        serde_json::from_str(&i.to_string()).expect("bounded integer")
                    }
                    Number::Float(bits) => Value::from(f64::from_bits(*bits)),
                })
                .collect(),
        )
    }
    pub fn duplicates(&self, other: &Self) -> bool {
        self.numbers.len() == other.numbers.len()
            && self
                .numbers
                .iter()
                .zip(other.numbers.iter())
                .all(|(a, b)| a.native_equal(b))
    }
    pub fn integer_edges(&self) -> Option<[u64; 42]> {
        if self.numbers.len() != 42 {
            return None;
        }
        let mut edges = [0; 42];
        for (edge, n) in edges.iter_mut().zip(self.numbers.iter()) {
            let Number::Integer(i) = n else {
                return None;
            };
            *edge = i.to_u64()?;
        }
        Some(edges)
    }
    pub(crate) fn checked_edges(&self, edge_bits: u8) -> Result<Option<[u64; 42]>, Error> {
        let mask = BigInt::from((1u64 << edge_bits) - 1);
        let mut previous = BigInt::from(-1);
        // Original verification folds every numeric element before checking
        // cycle topology. A float can fail arithmetically even at non-42 length.
        for n in &self.numbers {
            if n.compare_integer(&mask) == Ordering::Greater
                || n.compare_integer(&previous) != Ordering::Greater
            {
                return Ok(None);
            }
            match n {
                Number::Integer(i) => previous = i.clone(),
                Number::Float(_) => return Err(Error::ProofArithmetic),
            }
        }
        Ok(self.integer_edges())
    }
}
impl From<[u32; 42]> for Proof {
    fn from(edges: [u32; 42]) -> Self {
        Self::new(
            edges
                .into_iter()
                .map(|n| Number::Integer(n.into()))
                .collect(),
        )
        .expect("fixed u32 proof fits budgets")
    }
}
