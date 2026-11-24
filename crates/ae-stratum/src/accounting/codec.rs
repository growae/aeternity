// Versioned Rust-owned records. This is not the original Mnesia disk format.
use super::*;
use ae_codec::rlp::{DecodeLimits, decode_with_limits};
use ae_trie::Value;
use num_bigint::Sign;
use num_traits::ToPrimitive;

fn bytes(b: impl AsRef<[u8]>) -> Value {
    Value::Bytes(b.as_ref().to_vec())
}
fn uint(n: u64) -> Value {
    let b = n.to_be_bytes();
    bytes(&b[b.iter().position(|v| *v != 0).unwrap_or(8)..])
}
fn int(n: &BigInt) -> Value {
    let (sign, mag) = n.to_bytes_be();
    Value::List(vec![
        uint(u64::from(sign == Sign::Minus)),
        bytes(if n == &BigInt::from(0) {
            &[][..]
        } else {
            mag.as_slice()
        }),
    ])
}
fn number(n: &Number) -> Value {
    match n {
        Number::Integer(n) => Value::List(vec![uint(0), int(n)]),
        Number::Float(n) => Value::List(vec![uint(1), bytes(n.to_be_bytes())]),
    }
}
fn map(m: &Map) -> Value {
    Value::List(
        m.iter()
            .map(|(k, v)| Value::List(vec![bytes(k), number(v)]))
            .collect(),
    )
}
fn amounts(m: &Amounts) -> Value {
    Value::List(
        m.iter()
            .map(|(k, v)| Value::List(vec![bytes(k), int(v)]))
            .collect(),
    )
}
fn shares(s: &Shares) -> Value {
    Value::List(match s {
        Shares::Relative(m) => vec![map(m)],
        Shares::Transformed => vec![],
    })
}
fn date(d: Date) -> Value {
    Value::List(d.0.into_iter().map(|v| uint(v.into())).collect())
}
fn option<T>(v: &Option<T>, f: impl FnOnce(&T) -> Value) -> Value {
    Value::List(match v {
        Some(v) => vec![f(v)],
        None => vec![],
    })
}
fn list(v: &Value) -> Result<&[Value], Error> {
    match v {
        Value::List(v) => Ok(v),
        _ => Err(Error::Corrupt("accounting list")),
    }
}
fn fields<const N: usize>(v: &Value) -> Result<&[Value; N], Error> {
    list(v)?
        .try_into()
        .map_err(|_| Error::Corrupt("accounting arity"))
}
fn bin(v: &Value) -> Result<&[u8], Error> {
    match v {
        Value::Bytes(v) => Ok(v),
        _ => Err(Error::Corrupt("accounting bytes")),
    }
}
fn read_uint(v: &Value) -> Result<u64, Error> {
    let b = bin(v)?;
    if b.len() > 8 || b.first() == Some(&0) {
        return Err(Error::Corrupt("accounting integer"));
    }
    let mut n = 0u64;
    for b in b {
        n = (n << 8) | u64::from(*b);
    }
    Ok(n)
}
fn read_int(v: &Value) -> Result<BigInt, Error> {
    let [sign, mag] = fields(v)?;
    let sign = read_uint(sign)?;
    let mag = bin(mag)?;
    if sign > 1 || mag.first() == Some(&0) || (sign == 1 && mag.is_empty()) {
        return Err(Error::Corrupt("accounting signed integer"));
    }
    if mag.len() > crate::MAX_INTEGER_BITS.div_ceil(8) as usize {
        return Err(Error::Limit);
    }
    Ok(BigInt::from_bytes_be(
        if sign == 1 { Sign::Minus } else { Sign::Plus },
        mag,
    ))
}
fn read_number(v: &Value) -> Result<Number, Error> {
    let [tag, value] = fields(v)?;
    let n = match read_uint(tag)? {
        0 => Number::Integer(read_int(value)?),
        1 => Number::Float(u64::from_be_bytes(
            bin(value)?
                .try_into()
                .map_err(|_| Error::Corrupt("accounting float"))?,
        )),
        _ => return Err(Error::Corrupt("accounting number tag")),
    };
    rewards::check_number(&n)?;
    Ok(n)
}
fn read_map<T>(
    v: &Value,
    limits: Limits,
    read: impl Fn(&Value) -> Result<T, Error>,
) -> Result<BTreeMap<Vec<u8>, T>, Error> {
    let entries = list(v)?;
    if entries.len() > limits.maps.entries {
        return Err(Error::Limit);
    }
    let mut result = BTreeMap::new();
    let mut previous: Option<&[u8]> = None;
    for entry in entries {
        let [k, v] = fields(entry)?;
        let k = bin(k)?;
        if k.len() > limits.maps.key_bytes {
            return Err(Error::Limit);
        }
        if previous.is_some_and(|p| p >= k) {
            return Err(Error::Corrupt("accounting map order"));
        }
        result.insert(k.to_vec(), read(v)?);
        previous = Some(k);
    }
    Ok(result)
}
fn read_shares(v: &Value, limits: Limits) -> Result<Shares, Error> {
    match list(v)? {
        [] => Ok(Shares::Transformed),
        [m] => Ok(Shares::Relative(read_map(m, limits, read_number)?)),
        _ => Err(Error::Corrupt("accounting shares")),
    }
}
fn read_date(v: &Value) -> Result<Date, Error> {
    let mut d = [0; 6];
    for (i, n) in fields::<6>(v)?.iter().enumerate() {
        d[i] = read_uint(n)?
            .try_into()
            .map_err(|_| Error::Corrupt("accounting date"))?;
    }
    let d = Date(d);
    d.validate()?;
    Ok(d)
}
fn read_option<T>(
    v: &Value,
    f: impl FnOnce(&Value) -> Result<T, Error>,
) -> Result<Option<T>, Error> {
    match list(v)? {
        [] => Ok(None),
        [v] => Ok(Some(f(v)?)),
        _ => Err(Error::Corrupt("accounting option")),
    }
}
fn hash(v: &Value) -> Result<Hash256, Error> {
    bin(v)?
        .try_into()
        .map_err(|_| Error::Corrupt("accounting hash"))
}

struct Budget {
    used: usize,
    limits: Limits,
}
impl Budget {
    fn add(&mut self, n: usize) -> Result<(), Error> {
        self.used = self.used.checked_add(n).ok_or(Error::Limit)?;
        if self.used > self.limits.record_bytes {
            return Err(Error::Limit);
        }
        Ok(())
    }
    fn int(&mut self, n: &BigInt) -> Result<(), Error> {
        crate::integer(n).map_err(|_| Error::Limit)?;
        self.add(24 + n.bits().div_ceil(8) as usize)
    }
    fn map(&mut self, m: &Map) -> Result<(), Error> {
        rewards::check_map(m, self.limits.maps)?;
        for (k, v) in m {
            self.add(96 + k.len())?;
            if let Number::Integer(n) = v {
                self.int(n)?;
            }
        }
        Ok(())
    }
    fn amounts(&mut self, m: &Amounts) -> Result<(), Error> {
        if m.len() > self.limits.maps.entries {
            return Err(Error::Limit);
        }
        let mut size = 0usize;
        for (k, v) in m {
            if k.len() > self.limits.maps.key_bytes {
                return Err(Error::Limit);
            }
            let n = 96 + k.len() + v.bits().div_ceil(8) as usize;
            size = size.checked_add(n).ok_or(Error::Limit)?;
            if size > self.limits.maps.bytes {
                return Err(Error::Limit);
            }
            self.add(n)?;
            self.int(v)?;
        }
        Ok(())
    }
    fn shares(&mut self, s: &Shares) -> Result<(), Error> {
        match s {
            Shares::Relative(m) => self.map(m),
            Shares::Transformed => Ok(()),
        }
    }
}
impl Record {
    fn validate(&self, limits: Limits) -> Result<(), Error> {
        limits.validate()?;
        let mut b = Budget { used: 0, limits };
        b.add(256)?;
        match self {
            Self::Share(s) => {
                if s.miner.len() > limits.maps.key_bytes {
                    return Err(Error::Limit);
                }
                b.add(s.miner.len())?;
                b.int(&s.target)?;
            }
            Self::Reward(r) => {
                b.int(&r.amount)?;
                b.shares(&r.pool)?;
                b.shares(&r.miners)?;
            }
            Self::Payment(p) => {
                b.int(&p.total)?;
                b.map(&p.relative)?;
                if let Some(m) = &p.gross {
                    b.amounts(m)?;
                    if m.is_empty()
                        || !m.keys().eq(p.relative.keys())
                        || m.values().any(|v| v.sign() != Sign::Plus || v.bits() > 256)
                        || m.values().sum::<BigInt>() != p.total
                    {
                        return Err(Error::Corrupt("payment gross allocation"));
                    }
                }
                if let Some(m) = &p.absolute {
                    b.amounts(m)?;
                }
                for n in [&p.nonce, &p.fee, &p.gas, &p.run_gas].into_iter().flatten() {
                    b.int(n)?;
                }
                if let Some(d) = p.date {
                    d.validate()?;
                }
            }
            Self::Candidate(c) => {
                c.date.validate()?;
                b.add(c.header.len())?;
                b.add(c.record.len())?;
            }
            Self::Hash { .. } | Self::Round(_) => {}
        }
        Ok(())
    }
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        self.validate(limits)?;
        let mut payload = match self {
            Self::Hash { hash, key } => vec![bytes(hash), uint(*key)],
            Self::Round(k) => vec![uint(*k)],
            Self::Share(s) => vec![uint(s.key), bytes(s.hash), int(&s.target), bytes(&s.miner)],
            Self::Reward(r) => vec![
                uint(r.height),
                bytes(r.hash),
                shares(&r.pool),
                shares(&r.miners),
                int(&r.amount),
                uint(r.round_key),
            ],
            Self::Payment(p) => vec![
                uint(p.id.0),
                uint(p.id.1.into()),
                int(&p.total),
                map(&p.relative),
                option(&p.absolute, amounts),
                option(&p.tx_hash, |h| bytes(h)),
                option(&p.nonce, int),
                option(&p.fee, int),
                option(&p.gas, int),
                option(&p.run_gas, int),
                option(&p.date, |d| date(*d)),
            ],
            Self::Candidate(c) => vec![
                bytes(c.hash),
                bytes(&c.header),
                bytes(&c.record),
                date(c.date),
            ],
        };
        let version = if let Self::Payment(Payment {
            gross: Some(gross), ..
        }) = self
        {
            payload.push(amounts(gross));
            2
        } else {
            1
        };
        let encoded = Value::List(vec![
            uint(version),
            uint(self.table() as u64),
            Value::List(payload),
        ])
        .encode();
        if encoded.len() > limits.record_bytes {
            return Err(Error::Limit);
        }
        Ok(encoded)
    }
    pub fn decode(key: &[u8], raw: &[u8], limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        let value = Value::from_rlp(
            &decode_with_limits(
                raw,
                DecodeLimits {
                    max_input_bytes: limits.record_bytes,
                    max_items: limits
                        .maps
                        .entries
                        .saturating_mul(24)
                        .saturating_add(256)
                        .min(limits.record_bytes.saturating_mul(2)),
                    max_depth: 10,
                },
            )
            .map_err(|e| match e {
                ae_codec::rlp::DecodeError::ResourceLimit => Error::Limit,
                _ => Error::Corrupt("accounting rlp"),
            })?,
        );
        let [version, table, payload] = fields(&value)?;
        let version = read_uint(version)?;
        if !matches!(version, 1 | 2) {
            return Err(Error::Corrupt("accounting version"));
        }
        let table = Table::from_byte(
            read_uint(table)?
                .try_into()
                .map_err(|_| Error::Corrupt("accounting table"))?,
        )?;
        if version == 2 && table != Table::Payments {
            return Err(Error::Corrupt("accounting version table"));
        }
        let record = match table {
            Table::Hashes => {
                let [h, k] = fields(payload)?;
                Self::Hash {
                    hash: hash(h)?,
                    key: read_uint(k)?,
                }
            }
            Table::Rounds => {
                let [k] = fields(payload)?;
                Self::Round(read_uint(k)?)
            }
            Table::Shares => {
                let [k, h, t, m] = fields(payload)?;
                Self::Share(Share {
                    key: read_uint(k)?,
                    hash: hash(h)?,
                    target: read_int(t)?,
                    miner: bin(m)?.to_vec(),
                })
            }
            Table::Rewards => {
                let [height, h, pool, miners, amount, round_key] = fields(payload)?;
                Self::Reward(Reward {
                    height: read_uint(height)?,
                    hash: hash(h)?,
                    pool: read_shares(pool, limits)?,
                    miners: read_shares(miners, limits)?,
                    amount: read_int(amount)?,
                    round_key: read_uint(round_key)?,
                })
            }
            Table::Payments => {
                let (payload, gross) = if version == 2 {
                    let entries = list(payload)?;
                    if entries.len() != 12 {
                        return Err(Error::Corrupt("payment allocation arity"));
                    }
                    (
                        Value::List(entries[..11].to_vec()),
                        Some(read_map(&entries[11], limits, read_int)?),
                    )
                } else {
                    (payload.clone(), None)
                };
                let [
                    height,
                    index,
                    total,
                    relative,
                    absolute,
                    h,
                    nonce,
                    fee,
                    gas,
                    run_gas,
                    date,
                ] = fields(&payload)?;
                Self::Payment(Payment {
                    id: (
                        read_uint(height)?,
                        read_uint(index)?
                            .to_u32()
                            .ok_or(Error::Corrupt("payment index"))?,
                    ),
                    total: read_int(total)?,
                    relative: read_map(relative, limits, read_number)?,
                    gross,
                    absolute: read_option(absolute, |m| read_map(m, limits, read_int))?,
                    tx_hash: read_option(h, hash)?,
                    nonce: read_option(nonce, read_int)?,
                    fee: read_option(fee, read_int)?,
                    gas: read_option(gas, read_int)?,
                    run_gas: read_option(run_gas, read_int)?,
                    date: read_option(date, read_date)?,
                })
            }
            Table::Candidates => {
                let [h, header, record, d] = fields(payload)?;
                Self::Candidate(Candidate {
                    hash: hash(h)?,
                    header: bin(header)?.to_vec(),
                    record: bin(record)?.to_vec(),
                    date: read_date(d)?,
                })
            }
        };
        if record.key() != key || record.encode(limits)? != raw {
            return Err(Error::Corrupt("noncanonical accounting record"));
        }
        Ok(record)
    }
}
