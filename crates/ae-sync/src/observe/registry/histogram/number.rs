use super::{Error, Value};
use num_bigint::BigInt;
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Number {
    Integer(BigInt),
    Float(f64),
}
impl Number {
    pub fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Integer(n) => Some(Self::Integer(n.clone())),
            Value::Float(n) => Some(Self::Float(*n)),
            _ => None,
        }
    }
    pub fn value(&self) -> Value {
        match self {
            Self::Integer(n) => Value::Integer(n.clone()),
            Self::Float(n) => Value::Float(*n),
        }
    }
    pub fn zero() -> Self {
        Self::Integer(0.into())
    }
    pub fn float(&self) -> Result<f64, Error> {
        let f = match self {
            Self::Integer(n) => n.to_string().parse().map_err(|_| Error::Arithmetic)?,
            Self::Float(f) => *f,
        };
        finite(f)
    }
    pub fn add(&self, b: &Self) -> Result<Self, Error> {
        Ok(match (self, b) {
            (Self::Integer(a), Self::Integer(b)) => Self::Integer(a + b),
            _ => Self::Float(finite(self.float()? + b.float()?)?),
        })
    }
    pub fn divide(&self, n: usize) -> Result<Self, Error> {
        Ok(Self::Float(finite(self.float()? / n as f64)?))
    }
    pub fn bytes(&self) -> usize {
        match self {
            Self::Integer(n) => 32 + n.to_signed_bytes_le().len(),
            Self::Float(_) => 32,
        }
    }
    pub fn cmp(&self, b: &Self) -> Ordering {
        match (self, b) {
            (Self::Integer(a), Self::Integer(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b).expect("finite sample"),
            (Self::Integer(a), Self::Float(b)) => integer_float(a, *b),
            (Self::Float(a), Self::Integer(b)) => integer_float(b, *a).reverse(),
        }
    }
    pub fn min(&self, b: &Self) -> Self {
        if self.cmp(b).is_gt() {
            b.clone()
        } else {
            self.clone()
        }
    }
    pub fn max(&self, b: &Self) -> Self {
        if self.cmp(b).is_lt() {
            b.clone()
        } else {
            self.clone()
        }
    }
    pub fn integer(&self, round: bool) -> Self {
        let Self::Float(f) = self else {
            return self.clone();
        };
        let f = if round { f.round() } else { f.trunc() };
        let (negative, mantissa, shift) = components(f);
        let n = if shift >= 0 {
            BigInt::from(mantissa) << shift as usize
        } else {
            BigInt::from(mantissa) >> (-shift) as usize
        };
        Self::Integer(if negative { -n } else { n })
    }
}
fn finite(f: f64) -> Result<f64, Error> {
    if f.is_finite() {
        Ok(f)
    } else {
        Err(Error::Arithmetic)
    }
}
fn components(f: f64) -> (bool, u64, i32) {
    let bits = f.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1u64 << 52) - 1);
    let (mantissa, shift) = if exponent == 0 {
        (fraction, -1074)
    } else {
        ((1u64 << 52) | fraction, exponent - 1075)
    };
    (bits >> 63 != 0, mantissa, shift)
}
fn integer_float(integer: &BigInt, float: f64) -> Ordering {
    let (negative, mantissa, shift) = components(float);
    let mantissa = BigInt::from(mantissa);
    let mantissa = if negative { -mantissa } else { mantissa };
    if shift >= 0 {
        integer.cmp(&(mantissa << shift as usize))
    } else {
        (integer << (-shift) as usize).cmp(&mantissa)
    }
}
