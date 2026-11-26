// aehttp_logic gas observations, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use ae_execution::transactions::accounting;
use ae_protocol::Protocol;
use ae_state::Trees;
use ae_trie::NodeReader;
use ae_tx::{SignedTransaction, Transaction};
use ae_types::{Hash256, Unsigned};
use num_bigint::BigInt;
use num_traits::{FromPrimitive, ToPrimitive};

pub mod repository;

pub const WINDOWS: [u32; 4] = [1, 5, 15, 60];

#[derive(Debug)]
pub enum Error {
    MissingHeader(Hash256),
    // Only a missing block/state record is a native negative cache value.
    // Exceptions and local work/cancellation failures must not be cached.
    MissingState(Hash256),
    Arithmetic,
    InvalidObject,
    ResourceLimit,
    Backend(crate::repository::Error),
    Accounting(accounting::Error),
}
impl From<crate::repository::Error> for Error {
    fn from(error: crate::repository::Error) -> Self {
        use crate::repository::Error as E;
        match error {
            E::ResourceLimit
            | E::Trie(ae_trie::Error::ResourceLimit)
            | E::Trie(ae_trie::Error::Decode(ae_codec::rlp::DecodeError::ResourceLimit))
            | E::TransactionStorage(ae_tx::storage::Error::ResourceLimit) => Self::ResourceLimit,
            error => Self::Backend(error),
        }
    }
}
impl From<accounting::Error> for Error {
    fn from(error: accounting::Error) -> Self {
        use ae_codec::{object::ObjectError, rlp::DecodeError};
        use ae_state::{accounts::AccountError, contracts};
        let tree = match &error {
            accounting::Error::Account(AccountError::Tree(e))
            | accounting::Error::Call(contracts::Error::Tree(e)) => Some(e),
            _ => None,
        };
        if matches!(
            tree,
            Some(
                ae_trie::Error::ResourceLimit | ae_trie::Error::Decode(DecodeError::ResourceLimit)
            )
        ) || matches!(
            error,
            accounting::Error::Call(contracts::Error::ResourceLimit)
                | accounting::Error::Call(contracts::Error::Object(ObjectError::Rlp(
                    DecodeError::ResourceLimit
                )))
                | accounting::Error::Account(AccountError::Object(ObjectError::Rlp(
                    DecodeError::ResourceLimit
                )))
        ) {
            Self::ResourceLimit
        } else {
            Self::Accounting(error)
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    pub minimum: Option<BigInt>,
    pub used: BigInt,
    pub total: BigInt,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub minutes: u32,
    pub minimum: BigInt,
    pub utilization: BigInt,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub steps: usize,
    pub integer_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            steps: 32_768,
            integer_bytes: 64 << 10,
        }
    }
}
impl Stats {
    pub fn check(&self, limits: Limits) -> Result<(), Error> {
        for n in self.minimum.iter().chain([&self.used, &self.total]) {
            if n.bits().div_ceil(8) > limits.integer_bytes as u64 {
                return Err(Error::ResourceLimit);
            }
        }
        Ok(())
    }
    fn add(&mut self, other: Self, limits: Limits) -> Result<(), Error> {
        other.check(limits)?;
        if let Some(price) = other.minimum {
            self.minimum = Some(match self.minimum.take() {
                Some(p) => p.min(price),
                None => price,
            });
        }
        self.used += other.used;
        self.total += other.total;
        self.check(limits)
    }
    pub fn summary(&self, minutes: u32) -> Result<Summary, Error> {
        // BEAM converts the integer numerator and denominator separately to
        // binary64. Exact rational rounding changes large-number results.
        let total = if self.used == BigInt::ZERO && self.total == BigInt::ZERO {
            1.0
        } else {
            self.total
                .to_f64()
                .filter(|n| n.is_finite())
                .ok_or(Error::Arithmetic)?
        };
        let numerator = (&self.used * BigInt::from(100))
            .to_f64()
            .filter(|n| n.is_finite())
            .ok_or(Error::Arithmetic)?;
        let utilization = BigInt::from_f64((numerator / total).round()).ok_or(Error::Arithmetic)?;
        Ok(Summary {
            minutes,
            minimum: self.minimum.clone().unwrap_or_default(),
            utilization,
        })
    }
    pub fn transactions<A: NodeReader, C: NodeReader>(
        transactions: &[SignedTransaction<Transaction>],
        height: &Unsigned,
        protocol: Protocol,
        trees: &Trees<A, C>,
        block_gas_limit: &BigInt,
        limits: Limits,
        mut checkpoint: impl FnMut() -> Result<(), Error>,
    ) -> Result<Self, Error> {
        let mut stats = Self {
            total: block_gas_limit.clone(),
            ..Default::default()
        };
        stats.check(limits)?;
        for signed in transactions {
            checkpoint()?;
            let tx = signed.transaction().view();
            let pricing = tx.pricing(height, protocol);
            let used = if protocol < Protocol::Ceres {
                pricing.gas_limit
            } else {
                accounting::used_gas(tx, height, protocol, trees)?
            };
            stats.add(
                Self {
                    minimum: Some(pricing.minimum_gas_price),
                    used,
                    total: BigInt::ZERO,
                },
                limits,
            )?;
        }
        Ok(stats)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub previous: Hash256,
    pub time: u64,
    pub protocol: u32,
    pub micro: bool,
}
pub trait Source {
    fn header(&mut self, hash: Hash256) -> Result<Option<Header>, Error>;
    // The runtime adapter supplies its shared native-compatible cache here.
    fn stats(&mut self, hash: Hash256, protocol: u32) -> Result<Stats, Error>;
}

// Time belongs to the client query, not deterministic chain execution. Do not
// silently stop at genesis or return a partial window on a missing ancestor.
pub fn recent(
    source: &mut impl Source,
    mut hash: Hash256,
    now_ms: i128,
    limits: Limits,
    mut checkpoint: impl FnMut() -> Result<(), Error>,
) -> Result<[Summary; 4], Error> {
    let mut stats = Stats::default();
    let mut result = Vec::with_capacity(WINDOWS.len());
    let mut steps = limits.steps;
    for minutes in WINDOWS {
        let cutoff = now_ms
            .checked_sub(i128::from(minutes) * 60_000)
            .ok_or(Error::Arithmetic)?;
        loop {
            checkpoint()?;
            steps = steps.checked_sub(1).ok_or(Error::ResourceLimit)?;
            let header = source.header(hash)?.ok_or(Error::MissingHeader(hash))?;
            if i128::from(header.time) < cutoff {
                result.push(stats.summary(minutes)?);
                break;
            }
            if header.micro {
                stats.add(source.stats(hash, header.protocol)?, limits)?;
            }
            hash = header.previous;
        }
    }
    result.try_into().map_err(|_| Error::InvalidObject)
}

#[cfg(test)]
mod tests;
