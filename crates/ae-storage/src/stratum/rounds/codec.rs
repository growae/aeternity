use super::*;
fn list(v: &Value) -> Result<&[Value], Failure> {
    match v {
        Value::List(v) => Ok(v),
        _ => Err(Error::Corrupt("Stratum rounds list").into()),
    }
}
fn fields<const N: usize>(v: &Value) -> Result<&[Value; N], Failure> {
    list(v)?
        .try_into()
        .map_err(|_| Error::Corrupt("Stratum rounds arity").into())
}
fn bytes(v: &Value) -> Result<&[u8], Failure> {
    match v {
        Value::Bytes(b) => Ok(b),
        _ => Err(Error::Corrupt("Stratum rounds bytes").into()),
    }
}
fn bigint(n: &BigInt) -> Value {
    let (sign, b) = n.to_bytes_be();
    Value::List(vec![
        number(u64::from(sign == Sign::Minus)),
        Value::Bytes(if n == &BigInt::from(0) { vec![] } else { b }),
    ])
}
fn read_bigint(v: &Value) -> Result<BigInt, Failure> {
    let [s, b] = fields(v)?;
    let s = integer(s)?;
    let b = bytes(b)?;
    if s > 1 || b.first() == Some(&0) || (s == 1 && b.is_empty()) || b.len() > 2048 {
        return Err(Error::Corrupt("Stratum rounds integer").into());
    }
    Ok(BigInt::from_bytes_be(
        if s == 1 { Sign::Minus } else { Sign::Plus },
        b,
    ))
}
fn numeric(n: &Number) -> Value {
    match n {
        Number::Integer(n) => Value::List(vec![number(0), bigint(n)]),
        Number::Float(n) => Value::List(vec![number(1), Value::Bytes(n.to_be_bytes().to_vec())]),
    }
}
fn read_numeric(v: &Value) -> Result<Number, Failure> {
    let [t, v] = fields(v)?;
    let n = match integer(t)? {
        0 => Number::Integer(read_bigint(v)?),
        1 => Number::Float(u64::from_be_bytes(
            bytes(v)?
                .try_into()
                .map_err(|_| Error::Corrupt("Stratum round float"))?,
        )),
        _ => return Err(Error::Corrupt("Stratum round number").into()),
    };
    rewards::check_number(&n).map_err(core::Error::from)?;
    Ok(n)
}
fn config(c: &Config) -> Value {
    Value::List(vec![
        Value::List(
            c.pool
                .iter()
                .map(|(k, v)| Value::List(vec![Value::Bytes(k.clone()), numeric(v)]))
                .collect(),
        ),
        numeric(&c.percent),
        number(c.last_n as u64),
        number(c.delay),
    ])
}
fn read_config(v: &Value) -> Result<Config, Failure> {
    let [m, p, n, d] = fields(v)?;
    let mut pool = Map::new();
    let mut prior: Option<Vec<u8>> = None;
    for row in list(m)? {
        let [k, v] = fields(row)?;
        let k = bytes(k)?.to_vec();
        if prior.as_ref().is_some_and(|p| p >= &k) {
            return Err(Error::Corrupt("Stratum pool map order").into());
        }
        pool.insert(k.clone(), read_numeric(v)?);
        prior = Some(k);
    }
    Ok(Config {
        pool,
        percent: read_numeric(p)?,
        last_n: usize::try_from(integer(n)?).map_err(|_| Error::ResourceLimit)?,
        delay: integer(d)?,
    })
}
fn header(h: &KeyHeader) -> Result<Value, Failure> {
    Ok(Value::Bytes(
        Header::Key(h.clone())
            .encode()
            .map_err(|_| Error::Corrupt("Stratum round header"))?,
    ))
}
fn read_header(v: &Value) -> Result<KeyHeader, Failure> {
    match Header::decode(bytes(v)?, HeaderMode::ProofOfWork)
        .map_err(|_| Error::Corrupt("Stratum round header bytes"))?
    {
        Header::Key(h) => Ok(h),
        _ => Err(Error::Corrupt("Stratum round key required").into()),
    }
}
fn job(j: &Job) -> Result<Value, Failure> {
    Ok(Value::List(match j {
        Job::Top {
            revision,
            header: h,
        } => vec![number(0), number(*revision), header(h)?],
        Job::Round { revision } => vec![number(1), number(*revision)],
        Job::Compute {
            revision,
            height,
            round,
        } => vec![
            number(2),
            number(*revision),
            number(*height),
            number(*round),
        ],
    }))
}
fn read_job(v: &Value) -> Result<Job, Failure> {
    let v = list(v)?;
    let Some(t) = v.first() else {
        return Err(Error::Corrupt("Stratum round job tag").into());
    };
    Ok(match (integer(t)?, &v[1..]) {
        (0, [r, h]) => Job::Top {
            revision: integer(r)?,
            header: Box::new(read_header(h)?),
        },
        (1, [r]) => Job::Round {
            revision: integer(r)?,
        },
        (2, [r, h, k]) => Job::Compute {
            revision: integer(r)?,
            height: integer(h)?,
            round: integer(k)?,
        },
        _ => return Err(Error::Corrupt("Stratum round job fields").into()),
    })
}
fn outcome(o: &Outcome) -> Value {
    Value::List(match o {
        Outcome::Top => vec![number(0)],
        Outcome::Delay => vec![number(1)],
        Outcome::Existing => vec![number(2)],
        Outcome::NotOurShare => vec![number(3)],
        Outcome::NoRange => vec![number(4)],
        Outcome::Reward {
            hash,
            amount,
            payments,
        } => vec![
            number(5),
            Value::Bytes(hash.0.to_vec()),
            bigint(amount),
            number(*payments as u64),
        ],
    })
}
fn read_outcome(v: &Value) -> Result<Outcome, Failure> {
    let v = list(v)?;
    let Some(t) = v.first() else {
        return Err(Error::Corrupt("Stratum round outcome tag").into());
    };
    Ok(match (integer(t)?, &v[1..]) {
        (0, []) => Outcome::Top,
        (1, []) => Outcome::Delay,
        (2, []) => Outcome::Existing,
        (3, []) => Outcome::NotOurShare,
        (4, []) => Outcome::NoRange,
        (5, [h, a, n]) => Outcome::Reward {
            hash: Hash256(
                bytes(h)?
                    .try_into()
                    .map_err(|_| Error::Corrupt("Stratum reward hash"))?,
            ),
            amount: read_bigint(a)?,
            payments: usize::try_from(integer(n)?).map_err(|_| Error::ResourceLimit)?,
        },
        _ => return Err(Error::Corrupt("Stratum round outcome").into()),
    })
}
fn receipt(r: &Receipt) -> Value {
    Value::List(vec![
        number(r.sequence),
        number(r.event),
        number(r.observed),
        number(r.height),
        Value::List(r.round.into_iter().map(number).collect()),
        outcome(&r.outcome),
    ])
}
fn read_receipt(v: &Value) -> Result<Receipt, Failure> {
    let [s, e, o, h, r, v] = fields(v)?;
    let round = match list(r)? {
        [] => None,
        [r] => Some(integer(r)?),
        _ => return Err(Error::Corrupt("Stratum round option").into()),
    };
    Ok(Receipt {
        sequence: integer(s)?,
        event: integer(e)?,
        observed: integer(o)?,
        height: integer(h)?,
        round,
        outcome: read_outcome(v)?,
    })
}
impl Info {
    pub(super) fn encode(
        &self,
        limits: Limits,
        accounting: core::Limits,
    ) -> Result<Vec<u8>, Failure> {
        self.validate(limits, accounting)?;
        let v = Value::List(vec![
            number(1),
            config(&self.config),
            number(self.cursor),
            header(&self.header)?,
            number(self.protocol.into()),
            number(self.next),
            Value::List(self.pending.iter().map(job).collect::<Result<_, _>>()?),
            Value::List(self.last.iter().map(receipt).collect()),
        ])
        .encode();
        if v.len() > limits.bytes {
            return Err(Error::ResourceLimit.into());
        }
        Ok(v)
    }
    pub(super) fn decode(
        raw: &[u8],
        limits: Limits,
        accounting: core::Limits,
    ) -> Result<Self, Failure> {
        let v = ae_codec::rlp::decode_with_limits(
            raw,
            ae_codec::rlp::DecodeLimits {
                max_input_bytes: limits.bytes,
                max_items: 1_000_000,
                max_depth: 16,
            },
        )
        .map_err(|_| Error::Corrupt("Stratum round RLP"))?;
        let v = Value::from_rlp(&v);
        let [version, c, cursor, h, p, n, j, last] = fields(&v)?;
        if integer(version)? != 1 {
            return Err(Error::Corrupt("Stratum round metadata version").into());
        }
        let last = match list(last)? {
            [] => None,
            [r] => Some(read_receipt(r)?),
            _ => return Err(Error::Corrupt("Stratum round last receipt").into()),
        };
        let info = Self {
            config: read_config(c)?,
            cursor: integer(cursor)?,
            header: read_header(h)?,
            protocol: u32::try_from(integer(p)?)
                .map_err(|_| Error::Corrupt("Stratum captured protocol"))?,
            next: integer(n)?,
            pending: list(j)?.iter().map(read_job).collect::<Result<_, _>>()?,
            last,
        };
        if info.encode(limits, accounting)? != raw {
            return Err(Error::Corrupt("Stratum rounds noncanonical").into());
        }
        Ok(info)
    }
}
