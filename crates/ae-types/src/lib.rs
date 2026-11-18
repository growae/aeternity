#![forbid(unsafe_code)]

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeError {
    InvalidSize { expected: usize, actual: usize },
    UnknownIdTag(u8),
    LeadingZero,
    IntegerOverflow,
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
