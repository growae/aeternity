use super::*;
fn bytes(v: impl AsRef<[u8]>) -> Value {
    Value::Bytes(v.as_ref().to_vec())
}
fn blob(v: &Value) -> Result<&[u8], Failure> {
    match v {
        Value::Bytes(b) => Ok(b),
        _ => Err(Error::Corrupt("payout history bytes").into()),
    }
}
fn fields<const N: usize>(v: &Value) -> Result<&[Value; N], Failure> {
    match v {
        Value::List(v) => v
            .as_slice()
            .try_into()
            .map_err(|_| Error::Corrupt("payout history fields").into()),
        _ => Err(Error::Corrupt("payout history list").into()),
    }
}
fn hash(v: &Value) -> Result<Hash256, Failure> {
    Hash256::try_from(blob(v)?).map_err(|_| Error::Corrupt("payout history hash").into())
}
fn decode(raw: &[u8], max: usize) -> Result<Value, Failure> {
    ae_codec::rlp::decode_with_limits(
        raw,
        ae_codec::rlp::DecodeLimits {
            max_input_bytes: max,
            max_items: 32,
            max_depth: 2,
        },
    )
    .map(|v| Value::from_rlp(&v))
    .map_err(|e| match e {
        ae_codec::rlp::DecodeError::ResourceLimit => Error::ResourceLimit.into(),
        _ => Error::Corrupt("payout history encoding").into(),
    })
}
impl Head {
    pub(super) fn encode(&self) -> Vec<u8> {
        Value::List(vec![
            number(1),
            bytes(&self.identity),
            number(self.summary.records),
            number(self.summary.bytes),
            bytes(self.summary.tip.0),
            number(u64::from(self.summary.partial)),
        ])
        .encode()
    }
    pub(super) fn decode(raw: &[u8]) -> Result<Self, Failure> {
        let v = decode(raw, 131072)?;
        let [version, identity, records, size, tip, partial] = fields(&v)?;
        let h = Self {
            identity: blob(identity)?.to_vec(),
            summary: Summary {
                records: integer(records)?,
                bytes: integer(size)?,
                tip: hash(tip)?,
                partial: match integer(partial)? {
                    0 => false,
                    1 => true,
                    _ => return Err(Error::Corrupt("payout history partial flag").into()),
                },
            },
        };
        if integer(version)? != 1
            || h.summary.records == 0
            || h.summary.records > MAX_RECORDS
            || h.summary.bytes == 0
            || h.summary.bytes > MAX_BYTES
            || h.summary.tip == Hash256::ZERO
            || h.encode() != raw
        {
            return Err(Error::Corrupt("payout history head").into());
        }
        Ok(h)
    }
}
impl Entry {
    pub fn encode(&self) -> Result<Vec<u8>, Failure> {
        if self
            .receipt
            .as_ref()
            .is_some_and(|r| r.len() > retention::MAX_BYTES)
        {
            return Err(Error::ResourceLimit.into());
        }
        if self.index == 0
            || self.index > MAX_RECORDS
            || (self.index == 1) != (self.previous == Hash256::ZERO)
            || !matches!(
                (&self.kind, &self.completed_request, &self.receipt),
                (Kind::Baseline | Kind::Transition, None, None)
                    | (Kind::Completed, Some(_), Some(_))
            )
            || (self.kind == Kind::Baseline && self.index != 1)
            || (self.kind == Kind::Completed && self.information.pending.is_some())
        {
            return Err(Error::Corrupt("payout history entry shape").into());
        }
        let l = Limits::maximum();
        let a = super::super::super::Limits::maximum().transaction;
        let raw = Value::List(vec![
            number(1),
            number(self.index),
            bytes(self.previous.0),
            number(match self.kind {
                Kind::Baseline => 0,
                Kind::Transition => 1,
                Kind::Completed => 2,
            }),
            bytes(self.information.encode(l, a)?),
            bytes(
                self.completed_request
                    .as_ref()
                    .map(|r| r.encode(l, a))
                    .transpose()?
                    .unwrap_or_default(),
            ),
            bytes(self.receipt.as_deref().unwrap_or_default()),
        ])
        .encode();
        if raw.len() > MAX_ENTRY {
            return Err(Error::ResourceLimit.into());
        }
        Ok(raw)
    }
    pub fn digest(&self) -> Result<Hash256, Failure> {
        Ok(ae_crypto::blake2b_256(&self.encode()?))
    }
    pub(super) fn decode(raw: &[u8]) -> Result<Self, Failure> {
        let v = decode(raw, MAX_ENTRY)?;
        let [version, index, previous, kind, info, completed, receipt] = fields(&v)?;
        let l = Limits::maximum();
        let a = super::super::super::Limits::maximum().transaction;
        let completed = blob(completed)?;
        let receipt = blob(receipt)?;
        let e = Self {
            index: integer(index)?,
            previous: hash(previous)?,
            kind: match integer(kind)? {
                0 => Kind::Baseline,
                1 => Kind::Transition,
                2 => Kind::Completed,
                _ => return Err(Error::Corrupt("payout history kind").into()),
            },
            information: Info::decode(blob(info)?, l, a)?,
            completed_request: if completed.is_empty() {
                None
            } else {
                Some(Record::decode(completed, l, a)?)
            },
            receipt: (!receipt.is_empty()).then(|| receipt.to_vec()),
        };
        if integer(version)? != 1
            || e.index == 0
            || e.index > MAX_RECORDS
            || (e.index == 1) != (e.previous == Hash256::ZERO)
            || e.encode()? != raw
        {
            return Err(Error::Corrupt("payout history entry").into());
        }
        Ok(e)
    }
}
