use super::*;
fn bytes(v: impl AsRef<[u8]>) -> Value {
    Value::Bytes(v.as_ref().to_vec())
}
fn blob(v: &Value) -> Result<&[u8], Failure> {
    if let Value::Bytes(b) = v {
        Ok(b)
    } else {
        Err(Error::Corrupt("payout bytes").into())
    }
}
fn fields<const N: usize>(v: &Value) -> Result<&[Value; N], Failure> {
    if let Value::List(v) = v {
        v.as_slice()
            .try_into()
            .map_err(|_| Error::Corrupt("payout arity").into())
    } else {
        Err(Error::Corrupt("payout list").into())
    }
}
fn flat(raw: &[u8], max: usize) -> Result<Value, Failure> {
    ae_codec::rlp::decode_with_limits(
        raw,
        ae_codec::rlp::DecodeLimits {
            max_input_bytes: max,
            max_items: 64,
            max_depth: 4,
        },
    )
    .map(|v| Value::from_rlp(&v))
    .map_err(|e| match e {
        ae_codec::rlp::DecodeError::ResourceLimit => Error::ResourceLimit.into(),
        _ => Error::Corrupt("payout RLP").into(),
    })
}
fn boolean(v: &Value) -> Result<bool, Failure> {
    match integer(v)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Error::Corrupt("payout boolean").into()),
    }
}
fn hash(v: &Value) -> Result<Hash256, Failure> {
    Ok(Hash256(
        blob(v)?
            .try_into()
            .map_err(|_| Error::Corrupt("payout hash"))?,
    ))
}
fn id(v: &Value) -> Result<core::PaymentId, Failure> {
    let [h, i] = fields(v)?;
    Ok((
        integer(h)?,
        u32::try_from(integer(i)?).map_err(|_| Error::Corrupt("payout payment index"))?,
    ))
}
fn id_value(p: core::PaymentId) -> Value {
    Value::List(vec![number(p.0), number(p.1.into())])
}
fn optional<T>(
    v: Option<&T>,
    encode: impl FnOnce(&T) -> Result<Vec<u8>, Failure>,
) -> Result<Value, Failure> {
    Ok(bytes(v.map(encode).transpose()?.unwrap_or_default()))
}
fn read_optional<T>(
    v: &Value,
    decode: impl FnOnce(&[u8]) -> Result<T, Failure>,
) -> Result<Option<T>, Failure> {
    let b = blob(v)?;
    if b.is_empty() {
        Ok(None)
    } else {
        decode(b).map(Some)
    }
}
fn outcome(o: &Outcome) -> Vec<u8> {
    Value::List(match o {
        Outcome::Accepted {
            chain,
            revision,
            inserted,
        } => vec![
            number(0),
            bytes(chain.encode()),
            number(*revision),
            number(u64::from(*inserted)),
        ],
        Outcome::Rejected(s) => vec![number(1), bytes(s)],
        Outcome::Observed {
            chain,
            revision,
            location,
        } => {
            let (kind, hash) = match location {
                Location::Deleted => (0, vec![]),
                Location::Pool => (1, vec![]),
                Location::Block(h) => (2, h.0.to_vec()),
                Location::Unknown => (3, vec![]),
            };
            vec![
                number(2),
                bytes(chain.encode()),
                number(*revision),
                number(kind),
                bytes(hash),
            ]
        }
    })
    .encode()
}
fn read_outcome(raw: &[u8]) -> Result<Outcome, Failure> {
    let Value::List(v) = flat(raw, 4352)? else {
        return Err(Error::Corrupt("payout outcome").into());
    };
    match v.as_slice() {
        [tag, chain, revision, inserted] if integer(tag)? == 0 => Ok(Outcome::Accepted {
            chain: repo::Status::decode(blob(chain)?)?,
            revision: integer(revision)?,
            inserted: boolean(inserted)?,
        }),
        [tag, reason] if integer(tag)? == 1 => Ok(Outcome::Rejected(
            String::from_utf8(blob(reason)?.to_vec())
                .map_err(|_| Error::Corrupt("payout reason"))?,
        )),
        [tag, chain, revision, kind, h] if integer(tag)? == 2 => Ok(Outcome::Observed {
            chain: repo::Status::decode(blob(chain)?)?,
            revision: integer(revision)?,
            location: match integer(kind)? {
                0 if blob(h)?.is_empty() => Location::Deleted,
                1 if blob(h)?.is_empty() => Location::Pool,
                2 => Location::Block(hash(h)?),
                _ => return Err(Error::Corrupt("payout observation location").into()),
            },
        }),
        _ => Err(Error::Corrupt("payout outcome fields").into()),
    }
}
fn dispatch(s: &Dispatch) -> Vec<u8> {
    Value::List(match s {
        Dispatch::Ready(n) => vec![number(0), number(*n)],
        Dispatch::Sending(n) => vec![number(1), number(*n)],
        Dispatch::Done {
            attempt,
            outcome: o,
        } => vec![number(2), number(*attempt), bytes(outcome(o))],
    })
    .encode()
}
fn read_dispatch(raw: &[u8]) -> Result<Dispatch, Failure> {
    let Value::List(v) = flat(raw, 4400)? else {
        return Err(Error::Corrupt("payout dispatch").into());
    };
    match v.as_slice() {
        [tag, n] if integer(tag)? == 0 => Ok(Dispatch::Ready(integer(n)?)),
        [tag, n] if integer(tag)? == 1 => Ok(Dispatch::Sending(integer(n)?)),
        [tag, n, o] if integer(tag)? == 2 => Ok(Dispatch::Done {
            attempt: integer(n)?,
            outcome: read_outcome(blob(o)?)?,
        }),
        _ => Err(Error::Corrupt("payout dispatch fields").into()),
    }
}
fn signed(s: &Signed) -> Result<Vec<u8>, Failure> {
    Ok(Value::List(vec![
        bytes(s.signature.0),
        Value::List(s.date.0.into_iter().map(|x| number(x.into())).collect()),
        bytes(&s.transaction),
        bytes(dispatch(&s.dispatch)),
    ])
    .encode())
}
fn read_signed(raw: &[u8], limits: Limits) -> Result<Signed, Failure> {
    let v = flat(raw, limits.bytes)?;
    let [sig, date, wire, dispatch] = fields(&v)?;
    let date = fields::<6>(date)?
        .iter()
        .map(|v| u32::try_from(integer(v)?).map_err(|_| Error::Corrupt("payout UTC field").into()))
        .collect::<Result<Vec<_>, Failure>>()?;
    if blob(wire)?.len() > limits.payout.transaction_bytes + 128 {
        return Err(Error::ResourceLimit.into());
    }
    Ok(Signed {
        signature: Signature(
            blob(sig)?
                .try_into()
                .map_err(|_| Error::Corrupt("payout signature size"))?,
        ),
        date: core::Date(date.try_into().expect("six date fields")),
        transaction: blob(wire)?.to_vec(),
        dispatch: read_dispatch(blob(dispatch)?)?,
    })
}
fn request(q: &Request, limits: Limits) -> Result<Vec<u8>, Failure> {
    Ok(Value::List(vec![
        bytes(q.chain.encode()),
        bytes(q.prepared.encode(limits.payout)?),
    ])
    .encode())
}
fn read_request(raw: &[u8], limits: Limits) -> Result<Box<Request>, Failure> {
    let v = flat(raw, limits.bytes)?;
    let [c, p] = fields(&v)?;
    Ok(Box::new(Request {
        chain: repo::Status::decode(blob(c)?)?,
        prepared: payout::Prepared::decode(blob(p)?, limits.payout)?,
    }))
}
impl Record {
    pub(super) fn encode(
        &self,
        limits: Limits,
        accounting: core::Limits,
    ) -> Result<Vec<u8>, Failure> {
        self.validate(limits, accounting)?;
        let p = core::Record::Payment(self.payment.clone());
        let raw = Value::List(vec![
            number(1),
            number(self.sequence),
            bytes(p.key()),
            bytes(p.encode(accounting)?),
            optional(self.request.as_deref(), |q| request(q, limits))?,
            optional(self.signed.as_ref(), signed)?,
        ])
        .encode();
        if raw.len() > limits.bytes {
            return Err(Error::ResourceLimit.into());
        }
        Ok(raw)
    }
    fn decode(raw: &[u8], limits: Limits, accounting: core::Limits) -> Result<Self, Failure> {
        let v = flat(raw, limits.bytes)?;
        let [version, seq, key, pay, q, s] = fields(&v)?;
        if integer(version)? != 1 {
            return Err(Error::Corrupt("payout record version").into());
        }
        let core::Record::Payment(payment) =
            core::Record::decode(blob(key)?, blob(pay)?, accounting)?
        else {
            return Err(Error::Corrupt("payout source record").into());
        };
        let r = Self {
            sequence: integer(seq)?,
            payment,
            request: read_optional(q, |q| read_request(q, limits))?,
            signed: read_optional(s, |s| read_signed(s, limits))?,
        };
        if r.encode(limits, accounting)? != raw {
            return Err(Error::Corrupt("payout record encoding").into());
        }
        Ok(r)
    }
}
fn context(c: &Context) -> Vec<u8> {
    Value::List(vec![
        bytes(c.chain.encode()),
        number(c.height),
        number(c.protocol as u64),
        Value::List(c.balance.iter().map(|b| bytes(b.as_bytes())).collect()),
    ])
    .encode()
}
fn read_context(raw: &[u8]) -> Result<Context, Failure> {
    let v = flat(raw, 2300)?;
    let [chain, height, protocol, balance] = fields(&v)?;
    let Value::List(balance) = balance else {
        return Err(Error::Corrupt("payout balance option").into());
    };
    let balance = match balance.as_slice() {
        [] => None,
        [v] if blob(v)?.len() <= 2048 => Some(
            Unsigned::from_field_bytes(blob(v)?).map_err(|_| Error::Corrupt("payout balance"))?,
        ),
        _ => return Err(Error::Corrupt("payout balance fields").into()),
    };
    Ok(Context {
        chain: repo::Status::decode(blob(chain)?)?,
        height: integer(height)?,
        protocol: Protocol::try_from(
            u32::try_from(integer(protocol)?)
                .map_err(|_| Error::Corrupt("payout protocol width"))?,
        )
        .map_err(|_| Error::Corrupt("payout protocol"))?,
        balance,
    })
}
fn completion(c: &Completion) -> Result<Vec<u8>, Failure> {
    Ok(Value::List(vec![
        bytes(c.record.0),
        number(c.sequence),
        id_value(c.payment),
        bytes(c.hash.0),
        bytes(c.block.0),
        number(c.height),
        bytes(c.chain.encode()),
        number(u64::from(c.pruned)),
    ])
    .encode())
}
fn read_completion(raw: &[u8]) -> Result<Completion, Failure> {
    let v = flat(raw, 512)?;
    let [r, s, p, h, b, height, c, pruned] = fields(&v)?;
    Ok(Completion {
        record: hash(r)?,
        sequence: integer(s)?,
        payment: id(p)?,
        hash: hash(h)?,
        block: hash(b)?,
        height: integer(height)?,
        chain: repo::Status::decode(blob(c)?)?,
        pruned: boolean(pruned)?,
    })
}
impl Info {
    pub(super) fn encode(
        &self,
        limits: Limits,
        accounting: core::Limits,
    ) -> Result<Vec<u8>, Failure> {
        self.validate(limits, accounting)?;
        let raw = Value::List(vec![
            number(1),
            Value::List(vec![
                bytes(self.config.caller.0),
                bytes(self.config.contract.0),
                number(self.config.delay),
            ]),
            bytes(&self.network),
            number(self.epoch),
            bytes(context(&self.context)),
            number(self.next),
            optional(self.pending.as_ref(), |r| r.encode(limits, accounting))?,
            optional(self.last.as_ref(), completion)?,
        ])
        .encode();
        if raw.len() > limits.bytes {
            return Err(Error::ResourceLimit.into());
        }
        Ok(raw)
    }
    pub(super) fn decode(
        raw: &[u8],
        limits: Limits,
        accounting: core::Limits,
    ) -> Result<Self, Failure> {
        limits.validate()?;
        let v = flat(raw, limits.bytes)?;
        let [version, config, network, epoch, c, next, pending, last] = fields(&v)?;
        if integer(version)? != 1 {
            return Err(Error::Corrupt("payout metadata version").into());
        }
        let [caller, contract, delay] = fields(config)?;
        let info = Self {
            config: Config {
                caller: AccountPublicKey(hash(caller)?.0),
                contract: ContractPublicKey(hash(contract)?.0),
                delay: integer(delay)?,
            },
            network: blob(network)?.to_vec(),
            epoch: integer(epoch)?,
            context: read_context(blob(c)?)?,
            next: integer(next)?,
            pending: read_optional(pending, |r| Record::decode(r, limits, accounting))?,
            last: read_optional(last, read_completion)?,
        };
        if info.encode(limits, accounting)? != raw {
            return Err(Error::Corrupt("payout metadata encoding").into());
        }
        Ok(info)
    }
}
