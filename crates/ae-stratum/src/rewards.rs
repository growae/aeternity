// aestratum_reward/fn/conv, fa2a6d7; OTP26 binary-key map traversal.
// Floating arithmetic and iteration order are observable payout policy.
use crate::{integer, proof::Number};
use ae_vm_fate::maps::{Traversal, order::binary_indices};
use num_bigint::BigInt;
use num_traits::{FromPrimitive, ToPrimitive, Zero};
use std::collections::BTreeMap;
pub type Map = BTreeMap<Vec<u8>, Number>;
pub type Amounts = BTreeMap<Vec<u8>, BigInt>;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Arithmetic,
    BadMatch,
    Limit,
    Order,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub entries: usize,
    pub key_bytes: usize,
    pub bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            entries: 100_000,
            key_bytes: 4096,
            bytes: 64 << 20,
        }
    }
}
impl Limits {
    pub fn validate(self) -> Result<(), Error> {
        if self.entries > 1_000_000 || self.key_bytes > 65536 || self.bytes > 1 << 30 {
            return Err(Error::Limit);
        }
        Ok(())
    }
}
pub fn check_number(n: &Number) -> Result<(), Error> {
    match n {
        Number::Integer(n) => integer(n).map_err(|_| Error::Limit),
        Number::Float(n) => {
            if f64::from_bits(*n).is_finite() {
                Ok(())
            } else {
                Err(Error::Arithmetic)
            }
        }
    }
}
pub fn check_map(map: &Map, limits: Limits) -> Result<(), Error> {
    limits.validate()?;
    if map.len() > limits.entries {
        return Err(Error::Limit);
    }
    let mut bytes = 0usize;
    for (key, value) in map {
        check_number(value)?;
        bytes = bytes
            .checked_add(
                96 + key.len()
                    + match value {
                        Number::Integer(n) => n.bits().div_ceil(8) as usize,
                        Number::Float(_) => 8,
                    },
            )
            .ok_or(Error::Limit)?;
        if key.len() > limits.key_bytes || bytes > limits.bytes {
            return Err(Error::Limit);
        }
    }
    Ok(())
}
fn float(n: &Number) -> Result<f64, Error> {
    let f = match n {
        Number::Integer(n) => n.to_f64().ok_or(Error::Arithmetic)?,
        Number::Float(bits) => f64::from_bits(*bits),
    };
    if f.is_finite() {
        Ok(f)
    } else {
        Err(Error::Arithmetic)
    }
}
fn finite(n: f64) -> Result<Number, Error> {
    if n.is_finite() {
        Ok(Number::Float(n.to_bits()))
    } else {
        Err(Error::Arithmetic)
    }
}
fn add(a: &Number, b: &Number) -> Result<Number, Error> {
    match (a, b) {
        (Number::Integer(a), Number::Integer(b)) => {
            let n = a + b;
            integer(&n).map_err(|_| Error::Limit)?;
            Ok(Number::Integer(n))
        }
        _ => finite(float(a)? + float(b)?),
    }
}
fn mul(a: &Number, b: &Number) -> Result<Number, Error> {
    match (a, b) {
        (Number::Integer(a), Number::Integer(b)) => {
            let n = a * b;
            integer(&n).map_err(|_| Error::Limit)?;
            Ok(Number::Integer(n))
        }
        _ => finite(float(a)? * float(b)?),
    }
}
fn div(a: &Number, b: &Number) -> Result<Number, Error> {
    finite(float(a)? / float(b)?)
}
fn round(n: &Number) -> Result<BigInt, Error> {
    match n {
        Number::Integer(n) => Ok(n.clone()),
        Number::Float(_) => BigInt::from_f64(float(n)?.round()).ok_or(Error::Arithmetic),
    }
}
pub fn indices(map: &Map, mode: Traversal, limits: Limits) -> Result<Vec<usize>, Error> {
    check_map(map, limits)?;
    let keys: Vec<_> = map.keys().map(Vec::as_slice).collect();
    binary_indices(
        &keys,
        mode,
        ae_vm_fate::Limits {
            max_depth: 0,
            max_nodes: limits.entries,
            max_bytes: limits.bytes,
        },
    )
    .map_err(|_| Error::Order)
}
pub fn sum(map: &Map, limits: Limits) -> Result<Number, Error> {
    let values: Vec<_> = map.values().collect();
    indices(map, Traversal::ToList, limits)?
        .into_iter()
        .try_fold(Number::Integer(0.into()), |total, i| add(&total, values[i]))
}
pub fn relative(map: &Map, total: &Number, limits: Limits) -> Result<Map, Error> {
    check_map(map, limits)?;
    check_number(total)?;
    map.iter()
        .map(|(k, v)| Ok((k.clone(), div(v, total)?)))
        .collect()
}
pub fn absolute(map: &Map, amount: &BigInt, limits: Limits) -> Result<Amounts, Error> {
    integer(amount).map_err(|_| Error::Limit)?;
    let order = indices(map, Traversal::Fold, limits)?;
    let entries: Vec<_> = map.iter().collect();
    let mut left = amount.clone();
    let mut result = BTreeMap::new();
    for (i, entry) in order.into_iter().enumerate() {
        let (key, score) = entries[entry];
        // Last recipient gets the exact remainder; no float coercion occurs.
        let tokens = if i + 1 == map.len() {
            left.clone()
        } else {
            round(&mul(score, &Number::Integer(amount.clone()))?)?.min(left.clone())
        };
        if !tokens.is_zero() {
            left -= &tokens;
            result.insert(key.clone(), tokens);
        }
    }
    if !left.is_zero() {
        return Err(Error::BadMatch);
    }
    Ok(result)
}
pub fn chunks(map: &Map, size: usize, limits: Limits) -> Result<Vec<Map>, Error> {
    if size == 0 {
        return Err(Error::Limit);
    }
    let order = indices(map, Traversal::ToList, limits)?;
    let entries: Vec<_> = map.iter().collect();
    let mut result: Vec<_> = order
        .chunks(size)
        .map(|indices| {
            indices
                .iter()
                .map(|&i| {
                    let (k, v) = entries[i];
                    (k.clone(), v.clone())
                })
                .collect()
        })
        .collect();
    result.reverse();
    Ok(result)
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payment {
    pub id: (u64, u32),
    pub total: BigInt,
    pub relative: Map,
}
pub fn payments(
    height: u64,
    amount: &BigInt,
    pool: &Map,
    miners: &Map,
    percent: &Number,
    limits: Limits,
) -> Result<Vec<Payment>, Error> {
    integer(amount).map_err(|_| Error::Limit)?;
    check_number(percent)?;
    check_map(pool, limits)?;
    check_map(miners, limits)?;
    let pool_amount = round(&div(
        &mul(&Number::Integer(amount.clone()), percent)?,
        &Number::Integer(100.into()),
    )?)?;
    let miner_amount = amount - &pool_amount;
    let mut left = miner_amount.clone();
    let batches = chunks(miners, 1000, limits)?;
    let mut result = Vec::new();
    for (i, batch) in batches.into_iter().enumerate().rev() {
        let total = sum(&batch, limits)?;
        let amount =
            round(&mul(&total, &Number::Integer(miner_amount.clone()))?)?.min(left.clone());
        if amount.is_zero() {
            continue;
        }
        let relative = relative(&batch, &total, limits)?;
        left -= &amount;
        result.push(Payment {
            id: (height, u32::try_from(i + 1).map_err(|_| Error::Limit)?),
            total: amount,
            relative,
        });
    }
    if pool_amount > BigInt::zero() {
        result.push(Payment {
            id: (height, 0),
            total: pool_amount,
            relative: pool.clone(),
        });
    }
    result.reverse();
    Ok(result)
}
// Input share order is the original ordered-table selection order. Floating
// accumulation must not be regrouped or parallel-reduced.
pub fn miner_shares(
    shares: &[(Vec<u8>, BigInt)],
    block_target: &BigInt,
    limits: Limits,
) -> Result<Map, Error> {
    limits.validate()?;
    integer(block_target).map_err(|_| Error::Limit)?;
    if shares.len() > limits.entries {
        return Err(Error::Limit);
    }
    let mut groups = Map::new();
    let mut total = Number::Float(0.0f64.to_bits());
    let mut bytes = 0usize;
    for (miner, target) in shares {
        integer(target).map_err(|_| Error::Limit)?;
        bytes = bytes
            .checked_add(96 + miner.len() + target.bits().div_ceil(8) as usize)
            .ok_or(Error::Limit)?;
        if miner.len() > limits.key_bytes || bytes > limits.bytes {
            return Err(Error::Limit);
        }
        let score = div(
            &Number::Integer(target.clone()),
            &Number::Integer(block_target.clone()),
        )?;
        total = add(&total, &score)?;
        let accumulated = add(
            groups.get(miner).unwrap_or(&Number::Integer(0.into())),
            &score,
        )?;
        groups.insert(miner.clone(), accumulated);
    }
    relative(&groups, &total, limits)
}
