use super::*;
fn flat(raw: &[u8], bytes: usize, items: usize, depth: usize) -> Result<Value, Failure> {
    let v = ae_codec::rlp::decode_with_limits(
        raw,
        ae_codec::rlp::DecodeLimits {
            max_input_bytes: bytes,
            max_items: items,
            max_depth: depth,
        },
    )
    .map_err(|_| Error::Corrupt("Stratum mining RLP"))?;
    Ok(Value::from_rlp(&v))
}
fn date(date: core::Date) -> Value {
    Value::List(date.0.into_iter().map(|v| number(u64::from(v))).collect())
}
fn read_date(v: &Value) -> Result<core::Date, Failure> {
    let Value::List(v) = v else {
        return Err(Error::Corrupt("Stratum mining date").into());
    };
    if v.len() != 6 {
        return Err(Error::Corrupt("Stratum mining date length").into());
    }
    let mut values = [0u32; 6];
    for (slot, v) in values.iter_mut().zip(v) {
        *slot =
            u32::try_from(integer(v)?).map_err(|_| Error::Corrupt("Stratum mining date value"))?;
    }
    let d = core::Date(values);
    d.validate()?;
    Ok(d)
}
fn outcome(state: &State) -> Value {
    let fields = match state {
        State::Ready => vec![number(0)],
        State::Submitting => vec![number(1)],
        State::Done(Outcome::MissingCandidate) => vec![number(2)],
        State::Done(Outcome::Inactive) => vec![number(3)],
        State::Done(Outcome::Accepted(Accepted::Committed(r))) => vec![number(4), number(*r)],
        State::Done(Outcome::Accepted(Accepted::Present)) => vec![number(5)],
        State::Done(Outcome::Accepted(Accepted::Observed(r))) => vec![number(6), number(*r)],
        State::Done(Outcome::Rejected { reason, detail }) => vec![
            number(7),
            number(match reason {
                Rejection::Invalid => 0,
                Rejection::Stale => 1,
                Rejection::Policy => 2,
            }),
            Value::Bytes(detail.as_bytes().to_vec()),
        ],
    };
    Value::List(fields)
}
fn read_state(v: &Value) -> Result<State, Failure> {
    let Value::List(v) = v else {
        return Err(Error::Corrupt("Stratum mining state").into());
    };
    let Some(tag) = v.first() else {
        return Err(Error::Corrupt("Stratum mining state tag").into());
    };
    Ok(match (integer(tag)?, &v[1..]) {
        (0, []) => State::Ready,
        (1, []) => State::Submitting,
        (2, []) => State::Done(Outcome::MissingCandidate),
        (3, []) => State::Done(Outcome::Inactive),
        (4, [revision]) => State::Done(Outcome::Accepted(Accepted::Committed(integer(revision)?))),
        (5, []) => State::Done(Outcome::Accepted(Accepted::Present)),
        (6, [revision]) => State::Done(Outcome::Accepted(Accepted::Observed(integer(revision)?))),
        (7, [reason, Value::Bytes(detail)]) => State::Done(Outcome::Rejected {
            reason: match integer(reason)? {
                0 => Rejection::Invalid,
                1 => Rejection::Stale,
                2 => Rejection::Policy,
                _ => return Err(Error::Corrupt("Stratum mining rejection").into()),
            },
            detail: String::from_utf8(detail.clone())
                .map_err(|_| Error::Corrupt("Stratum mining diagnostic"))?,
        }),
        _ => return Err(Error::Corrupt("Stratum mining state fields").into()),
    })
}
impl Record {
    fn validate(&self) -> Result<(), Failure> {
        self.receipt.id.validate()?;
        self.receipt.input.validate()?;
        if self.epoch == 0 || self.receipt.input.solution.is_none() {
            return Err(Error::Corrupt("Stratum mining source").into());
        }
        match &self.state {
            State::Done(Outcome::MissingCandidate)
                if self.candidate.is_none()
                    && self.accounted_key.is_none()
                    && self.attempts == 0 =>
            {
                return Ok(());
            }
            State::Done(Outcome::MissingCandidate) => {
                return Err(Error::Corrupt("Stratum missing candidate state").into());
            }
            State::Done(Outcome::Inactive) if self.attempts != 0 => {
                return Err(Error::Corrupt("Stratum inactive attempts").into());
            }
            State::Submitting | State::Done(Outcome::Accepted(_) | Outcome::Rejected { .. })
                if self.attempts == 0 =>
            {
                return Err(Error::Corrupt("Stratum missing attempt").into());
            }
            _ => {}
        }
        if self.candidate.is_none() || self.accounted_key.is_none() {
            return Err(Error::Corrupt("Stratum missing work binding").into());
        }
        self.solution()?;
        match &self.state {
            State::Done(Outcome::Accepted(Accepted::Committed(0) | Accepted::Observed(0))) => {
                return Err(Error::Corrupt("Stratum zero observed revision").into());
            }
            State::Done(Outcome::Rejected { detail, .. }) if detail.len() > 4096 => {
                return Err(Error::ResourceLimit.into());
            }
            _ => {}
        }
        Ok(())
    }
    pub(super) fn encode(&self) -> Result<Vec<u8>, Failure> {
        self.validate()?;
        let raw = Value::List(vec![
            number(1),
            Value::Bytes(self.receipt.encode()?),
            number(self.epoch),
            Value::List(
                self.candidate
                    .as_ref()
                    .map(|c| Value::Bytes(c.sealing_bytes().to_vec()))
                    .into_iter()
                    .collect(),
            ),
            Value::List(self.accounted_key.into_iter().map(number).collect()),
            number(self.attempts),
            outcome(&self.state),
        ])
        .encode();
        if raw.len() > RECORD_BYTES {
            return Err(Error::ResourceLimit.into());
        }
        Ok(raw)
    }
    pub(super) fn decode(raw: &[u8]) -> Result<Self, Failure> {
        let Value::List(v) = flat(raw, RECORD_BYTES, 24, 3)? else {
            return Err(Error::Corrupt("Stratum mining record").into());
        };
        let [
            version,
            Value::Bytes(receipt),
            epoch,
            Value::List(candidate),
            Value::List(accounted),
            attempts,
            state,
        ] = v.as_slice()
        else {
            return Err(Error::Corrupt("Stratum mining record fields").into());
        };
        let candidate = match candidate.as_slice() {
            [] => None,
            [Value::Bytes(b)] => Some(Candidate::decode(b)?),
            _ => return Err(Error::Corrupt("Stratum mining candidate fields").into()),
        };
        let accounted_key = match accounted.as_slice() {
            [] => None,
            [v] => Some(integer(v)?),
            _ => return Err(Error::Corrupt("Stratum mining share key").into()),
        };
        let r = Self {
            receipt: Receipt::decode(receipt)?,
            epoch: integer(epoch)?,
            candidate,
            accounted_key,
            attempts: integer(attempts)?,
            state: read_state(state)?,
        };
        if integer(version)? != 1 || r.encode()? != raw {
            return Err(Error::Corrupt("Stratum mining canonical record").into());
        }
        Ok(r)
    }
}
impl Publication {
    fn encode(&self) -> Result<Vec<u8>, Failure> {
        if self.sequence == 0 {
            return Err(Error::Corrupt("Stratum publication sequence").into());
        }
        self.date.validate()?;
        Ok(Value::List(vec![
            number(self.sequence),
            Value::Bytes(self.candidate.sealing_bytes().to_vec()),
            date(self.date),
            number(u64::from(self.published)),
        ])
        .encode())
    }
    fn decode(raw: &[u8]) -> Result<Self, Failure> {
        let Value::List(v) = flat(raw, 512, 12, 2)? else {
            return Err(Error::Corrupt("Stratum publication RLP").into());
        };
        let [sequence, Value::Bytes(candidate), date, published] = v.as_slice() else {
            return Err(Error::Corrupt("Stratum publication fields").into());
        };
        let p = Self {
            sequence: integer(sequence)?,
            candidate: Candidate::decode(candidate)?,
            date: read_date(date)?,
            published: match integer(published)? {
                0 => false,
                1 => true,
                _ => return Err(Error::Corrupt("Stratum publication flag").into()),
            },
        };
        if p.encode()? != raw {
            return Err(Error::Corrupt("Stratum publication encoding").into());
        }
        Ok(p)
    }
}
impl Info {
    pub(super) fn encode(&self) -> Result<Vec<u8>, Failure> {
        self.check(Limits::maximum())?;
        if self.epoch == 0 {
            return Err(Error::Corrupt("Stratum mining epoch").into());
        }
        Ok(Value::List(vec![
            number(1),
            number(self.epoch),
            Value::List(
                self.last
                    .as_ref()
                    .map(|p| p.encode().map(Value::Bytes))
                    .transpose()?
                    .into_iter()
                    .collect(),
            ),
            Value::List(
                self.active
                    .iter()
                    .map(|c| Value::Bytes(c.sealing_bytes().to_vec()))
                    .collect(),
            ),
            Value::List(self.counts.into_iter().map(number).collect()),
            number(self.bytes),
        ])
        .encode())
    }
    pub(super) fn decode(raw: Option<&[u8]>) -> Result<Self, Failure> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        let Value::List(v) = flat(raw, 2 << 20, 4120, 2)? else {
            return Err(Error::Corrupt("Stratum mining metadata").into());
        };
        let [
            version,
            epoch,
            Value::List(last),
            Value::List(active),
            Value::List(counts),
            bytes,
        ] = v.as_slice()
        else {
            return Err(Error::Corrupt("Stratum mining metadata fields").into());
        };
        if active.len() > 4096 || counts.len() != 3 {
            return Err(Error::ResourceLimit.into());
        }
        let last = match last.as_slice() {
            [] => None,
            [Value::Bytes(b)] => Some(Publication::decode(b)?),
            _ => return Err(Error::Corrupt("Stratum publication metadata").into()),
        };
        let active = active
            .iter()
            .map(|v| match v {
                Value::Bytes(b) => Candidate::decode(b).map_err(Into::into),
                _ => Err(Failure::from(Error::Corrupt("Stratum active candidate"))),
            })
            .collect::<Result<Vec<_>, Failure>>()?;
        let mut totals = [0; 3];
        for (n, v) in totals.iter_mut().zip(counts) {
            *n = integer(v)?;
        }
        let m = Self {
            epoch: integer(epoch)?,
            last,
            active,
            counts: totals,
            bytes: integer(bytes)?,
        };
        if integer(version)? != 1 || m.encode()? != raw {
            return Err(Error::Corrupt("Stratum mining canonical metadata").into());
        }
        Ok(m)
    }
}
