#![forbid(unsafe_code)]

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeError {
    InvalidSize { expected: usize, actual: usize },
    UnknownIdTag(u8),
    LeadingZero,
    IntegerOverflow,
    IntegerUnderflow,
    InvalidDecimal,
}

impl fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for TypeError {}

macro_rules! fixed_bytes {
    ($($name:ident: $size:literal),+ $(,)?) => {$(
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub [u8; $size]);
        impl $name {
            pub const ZERO: Self = Self([0; $size]);
            pub const fn as_bytes(&self) -> &[u8; $size] { &self.0 }
        }
        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] { &self.0 }
        }
        impl TryFrom<&[u8]> for $name {
            type Error = TypeError;
            fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
                bytes.try_into().map(Self).map_err(|_| TypeError::InvalidSize {
                    expected: $size, actual: bytes.len()
                })
            }
        }
    )+};
}

fixed_bytes! {
    Hash256: 32, KeyBlockHash: 32, MicroBlockHash: 32, StateHash: 32,
    TransactionsHash: 32, AccountPublicKey: 32, ContractPublicKey: 32,
    PeerPublicKey: 32, Signature: 64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum IdTag {
    Account = 1,
    Name = 2,
    Commitment = 3,
    Oracle = 4,
    Contract = 5,
    Channel = 6,
}

impl TryFrom<u8> for IdTag {
    type Error = TypeError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Account),
            2 => Ok(Self::Name),
            3 => Ok(Self::Commitment),
            4 => Ok(Self::Oracle),
            5 => Ok(Self::Contract),
            6 => Ok(Self::Channel),
            other => Err(TypeError::UnknownIdTag(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Id {
    pub tag: IdTag,
    pub value: [u8; 32],
}

impl Id {
    pub const fn new(tag: IdTag, value: [u8; 32]) -> Self {
        Self { tag, value }
    }

    pub fn to_bytes(self) -> [u8; 33] {
        let mut bytes = [0; 33];
        bytes[0] = self.tag as u8;
        bytes[1..].copy_from_slice(&self.value);
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TypeError> {
        let raw: &[u8; 33] = bytes.try_into().map_err(|_| TypeError::InvalidSize {
            expected: 33,
            actual: bytes.len(),
        })?;
        let mut value = [0; 32];
        value.copy_from_slice(&raw[1..]);
        Ok(Self {
            tag: raw[0].try_into()?,
            value,
        })
    }
}

// Canonical nonnegative integer bytes, without a machine-word size restriction.
// Aeternity encodes zero as [0], but its decoder also accepts the empty binary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Unsigned(Vec<u8>);

impl Unsigned {
    pub fn mul_u64(&self, multiplier: u64) -> Self {
        if multiplier == 0 {
            return 0.into();
        }
        let mut output = Vec::with_capacity(self.0.len() + 8);
        let mut carry = 0u128;
        for b in self.0.iter().rev() {
            let product = u128::from(*b) * u128::from(multiplier) + carry;
            output.push(product as u8);
            carry = product >> 8;
        }
        while carry != 0 {
            output.push(carry as u8);
            carry >>= 8;
        }
        output.reverse();
        Self(output)
    }
    pub fn from_decimal(text: &str) -> Result<Self, TypeError> {
        if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
            return Err(TypeError::InvalidDecimal);
        }
        let mut bytes = vec![0u8];
        for digit in text.bytes() {
            let mut carry = u16::from(digit - b'0');
            for byte in bytes.iter_mut().rev() {
                let next = u16::from(*byte) * 10 + carry;
                *byte = next as u8;
                carry = next >> 8;
            }
            if carry != 0 {
                bytes.insert(0, carry as u8);
            }
        }
        Ok(Self(bytes))
    }

    pub fn add(&self, other: &Self) -> Self {
        let size = self.0.len().max(other.0.len());
        let mut output = vec![0u8; size + 1];
        let mut carry = 0u16;
        for n in 0..size {
            let a = self.0.len().checked_sub(n + 1).map_or(0, |i| self.0[i]);
            let b = other.0.len().checked_sub(n + 1).map_or(0, |i| other.0[i]);
            let sum = u16::from(a) + u16::from(b) + carry;
            output[size - n] = sum as u8;
            carry = sum >> 8;
        }
        output[0] = carry as u8;
        if carry == 0 {
            output.remove(0);
        }
        Self(output)
    }

    pub fn checked_sub(&self, other: &Self) -> Result<Self, TypeError> {
        if self < other {
            return Err(TypeError::IntegerUnderflow);
        }
        let mut output = self.0.clone();
        let size = output.len();
        let mut borrow = 0i16;
        for n in 0..size {
            let b = other.0.len().checked_sub(n + 1).map_or(0, |i| other.0[i]);
            let difference = i16::from(output[size - n - 1]) - i16::from(b) - borrow;
            output[size - n - 1] = difference as u8;
            borrow = i16::from(difference < 0);
        }
        let start = output.iter().position(|b| *b != 0).unwrap_or(size - 1);
        Ok(Self(output[start..].to_vec()))
    }

    pub fn low_bit(&self) -> bool {
        self.0.last().is_some_and(|b| b & 1 != 0)
    }

    pub fn with_low_bit(&self, set: bool) -> Self {
        let mut bytes = self.0.clone();
        let last = bytes
            .last_mut()
            .expect("canonical integer always has bytes");
        *last = (*last & !1) | u8::from(set);
        Self(bytes)
    }
    pub fn from_field_bytes(bytes: &[u8]) -> Result<Self, TypeError> {
        if bytes.len() > 1 && bytes[0] == 0 {
            return Err(TypeError::LeadingZero);
        }
        Ok(Self(if bytes.is_empty() {
            vec![0]
        } else {
            bytes.to_vec()
        }))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_u64(&self) -> Result<u64, TypeError> {
        if self.0.len() > 8 {
            return Err(TypeError::IntegerOverflow);
        }
        Ok(self.0.iter().fold(0, |n, b| (n << 8) | u64::from(*b)))
    }
}

impl From<u64> for Unsigned {
    fn from(value: u64) -> Self {
        let bytes = value.to_be_bytes();
        let start = bytes.iter().position(|b| *b != 0).unwrap_or(7);
        Self(bytes[start..].to_vec())
    }
}

impl Ord for Unsigned {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.cmp(&other.0))
    }
}
impl PartialOrd for Unsigned {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arithmetic_preserves_large_balances_without_narrowing() {
        let a = Unsigned::from_decimal("10000000000000000000000000000000").unwrap();
        let b = Unsigned::from_decimal("9999999999999999999999999999999").unwrap();
        assert_eq!(a.checked_sub(&b).unwrap(), 1.into());
        assert_eq!(b.add(&1.into()), a);
        assert_eq!(a.checked_sub(&a).unwrap(), 0.into());
        assert_eq!(b.checked_sub(&a), Err(TypeError::IntegerUnderflow));
        assert_eq!(Unsigned::from_decimal("00000").unwrap(), 0.into());
        for a in [0, 1, 255, 256, 65535, u32::MAX as u64, u64::MAX] {
            for multiplier in [0, 1, 255, 256, u64::MAX] {
                assert_eq!(
                    Unsigned::from(a).mul_u64(multiplier),
                    Unsigned::from_decimal(&(u128::from(a) * u128::from(multiplier)).to_string())
                        .unwrap()
                );
            }
            for b in [0, 1, 255, 256, 65535] {
                let sum = Unsigned::from(a).add(&Unsigned::from(b));
                assert_eq!(
                    sum,
                    Unsigned::from_decimal(&(u128::from(a) + u128::from(b)).to_string()).unwrap()
                );
                assert_eq!(sum.checked_sub(&b.into()).unwrap(), a.into());
            }
        }
    }
    #[test]
    fn integer_domain_is_not_a_machine_word() {
        let large = Unsigned::from_field_bytes(&[1; 128]).unwrap();
        assert_eq!(large.to_u64(), Err(TypeError::IntegerOverflow));
        assert!(large > Unsigned::from(u64::MAX));
        assert_eq!(Unsigned::from_field_bytes(&[]).unwrap(), Unsigned::from(0));
        assert_eq!(Unsigned::from(0).as_bytes(), &[0]);
        assert_eq!(
            Unsigned::from_field_bytes(&[0, 1]),
            Err(TypeError::LeadingZero)
        );
    }
    #[test]
    fn id_domain_is_tagged_and_exactly_33_bytes() {
        for tag in 1..=6 {
            let id = Id::new(tag.try_into().unwrap(), [17; 32]);
            assert_eq!(Id::from_bytes(&id.to_bytes()), Ok(id));
        }
        assert!(Id::from_bytes(&[0; 33]).is_err());
        assert!(Id::from_bytes(&[1; 32]).is_err());
        assert!(Id::from_bytes(&[1; 34]).is_err());
    }
}
