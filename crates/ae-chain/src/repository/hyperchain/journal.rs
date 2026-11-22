// Rust-owned, versioned HC cache checkpoints and per-commit consensus events.
// These records share the block/state/index WAL batch; they are not wire data.
use crate::{
    publication::{BlockInfo, Context, Kind, Origin},
    repository::*,
};
use ae_hyperchain::{
    block::{EpochStart, Event, State},
    chain::{EpochInfo, Staker},
    leader,
};
use ae_vm_fate::{
    Limits, encoding,
    value::{ObjectPayload, Value as V},
};
use num_bigint::BigInt;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub revision: u64,
    pub first_revision: u64,
    pub state: State,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub revision: u64,
    pub context: Context,
    pub events: Vec<Event>,
}
fn corrupt() -> Error {
    Error::Corrupt("HC metadata")
}
fn codec_error(e: ae_vm_fate::Error) -> Error {
    if e == ae_vm_fate::Error::ResourceLimit {
        Error::ResourceLimit
    } else {
        corrupt()
    }
}
fn tuple(v: Vec<V>) -> V {
    V::Tuple(v)
}
fn integer(v: impl Into<BigInt>) -> V {
    V::Integer(v.into())
}
fn parts(v: &V, n: usize) -> Result<&[V], Error> {
    match v {
        V::Tuple(v) if v.len() == n => Ok(v),
        _ => Err(corrupt()),
    }
}
fn rows(v: &V) -> Result<&[V], Error> {
    match v {
        V::List(v) => Ok(v),
        _ => Err(corrupt()),
    }
}
fn big(v: &V) -> Result<BigInt, Error> {
    match v {
        V::Integer(v) => Ok(v.clone()),
        _ => Err(corrupt()),
    }
}
fn u64_value(v: &V) -> Result<u64, Error> {
    big(v)?.try_into().map_err(|_| corrupt())
}
fn bytes(v: &V) -> Result<Vec<u8>, Error> {
    match v {
        V::Bytes(v) => Ok(v.clone()),
        _ => Err(corrupt()),
    }
}
fn hash_value(v: &V) -> Result<Hash256, Error> {
    bytes(v)?.try_into().map(Hash256).map_err(|_| corrupt())
}
fn option(v: Option<V>) -> V {
    tuple(v.into_iter().collect())
}
fn some(v: &V) -> Result<Option<&V>, Error> {
    match v {
        V::Tuple(xs) if xs.len() <= 1 => Ok(xs.first()),
        _ => Err(corrupt()),
    }
}
fn payload(p: &ObjectPayload, depth: usize, limits: Limits) -> Result<V, Error> {
    if depth > limits.max_depth {
        return Err(Error::ResourceLimit);
    }
    Ok(match p {
        ObjectPayload::Bytes(b) => V::Bytes(b.clone()),
        ObjectPayload::List(xs) => V::List(
            xs.iter()
                .map(|p| payload(p, depth + 1, limits))
                .collect::<Result<_, _>>()?,
        ),
    })
}
fn object(v: &V) -> Result<ObjectPayload, Error> {
    match v {
        V::Bytes(b) => Ok(ObjectPayload::Bytes(b.clone())),
        V::List(xs) => Ok(ObjectPayload::List(
            xs.iter().map(object).collect::<Result<_, _>>()?,
        )),
        _ => Err(corrupt()),
    }
}
fn epoch(e: &EpochInfo, limits: Limits) -> Result<V, Error> {
    Ok(tuple(vec![
        e.epoch.clone(),
        integer(e.first.clone()),
        integer(e.length.clone()),
        integer(e.last.clone()),
        option(e.seed.clone().map(V::Bytes)),
        option(
            e.validators
                .as_ref()
                .map(|vs| {
                    vs.iter()
                        .map(|v| {
                            Ok(tuple(vec![
                                payload(&v.address, 0, limits)?,
                                v.stake.clone(),
                            ]))
                        })
                        .collect::<Result<Vec<_>, Error>>()
                        .map(V::List)
                })
                .transpose()?,
        ),
    ]))
}
fn read_epoch(v: &V) -> Result<EpochInfo, Error> {
    let p = parts(v, 6)?;
    Ok(EpochInfo {
        epoch: p[0].clone(),
        first: big(&p[1])?,
        length: big(&p[2])?,
        last: big(&p[3])?,
        seed: some(&p[4])?.map(bytes).transpose()?,
        validators: some(&p[5])?
            .map(|vs| {
                rows(vs)?
                    .iter()
                    .map(|v| {
                        let p = parts(v, 2)?;
                        Ok(Staker {
                            address: object(&p[0])?,
                            stake: p[1].clone(),
                        })
                    })
                    .collect::<Result<_, Error>>()
            })
            .transpose()?,
    })
}
fn event(e: &Event, limits: Limits) -> Result<V, Error> {
    Ok(match e {
        Event::NoPin => tuple(vec![integer(0)]),
        Event::IncorrectPin => tuple(vec![integer(1)]),
        Event::PinAccepted {
            reward,
            recipient,
            epoch,
            height,
        } => tuple(vec![
            integer(2),
            integer(reward.clone()),
            payload(recipient, 0, limits)?,
            epoch.clone(),
            integer(height.clone()),
        ]),
        Event::NewEpoch(e) => tuple(vec![integer(3), epoch(e, limits)?]),
    })
}
fn read_event(v: &V) -> Result<Event, Error> {
    let V::Tuple(p) = v else {
        return Err(corrupt());
    };
    match p.first().map(u64_value).transpose()? {
        Some(0) if p.len() == 1 => Ok(Event::NoPin),
        Some(1) if p.len() == 1 => Ok(Event::IncorrectPin),
        Some(2) if p.len() == 5 => Ok(Event::PinAccepted {
            reward: big(&p[1])?,
            recipient: object(&p[2])?,
            epoch: p[3].clone(),
            height: big(&p[4])?,
        }),
        Some(3) if p.len() == 2 => Ok(Event::NewEpoch(read_epoch(&p[1])?)),
        _ => Err(corrupt()),
    }
}
impl Checkpoint {
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        let s = &self.state;
        let leaders = s
            .leaders
            .schedule()
            .iter()
            .map(|(h, p)| Ok(tuple(vec![integer(h.clone()), payload(p, 0, limits)?])))
            .collect::<Result<Vec<_>, Error>>()?;
        let value = tuple(vec![
            integer(1),
            integer(self.revision),
            integer(self.first_revision),
            V::List(s.leaders.epochs().iter().cloned().map(integer).collect()),
            V::List(
                s.leaders
                    .ranges()
                    .iter()
                    .map(|(e, (f, l))| {
                        tuple(vec![
                            integer(e.clone()),
                            integer(f.clone()),
                            integer(l.clone()),
                        ])
                    })
                    .collect(),
            ),
            V::List(leaders),
            V::List(
                s.epoch_starts
                    .iter()
                    .map(|s| {
                        tuple(vec![
                            integer(s.epoch.clone()),
                            integer(s.height.clone()),
                            integer(s.time.clone()),
                        ])
                    })
                    .collect(),
            ),
        ]);
        encoding::encode(&value, limits).map_err(codec_error)
    }
    pub fn decode(b: &[u8], limits: Limits) -> Result<Self, Error> {
        let value = encoding::decode(b, limits).map_err(codec_error)?;
        let p = parts(&value, 7)?;
        if u64_value(&p[0])? != 1 {
            return Err(corrupt());
        }
        let epochs = rows(&p[3])?.iter().map(big).collect::<Result<_, _>>()?;
        let ranges = rows(&p[4])?
            .iter()
            .map(|r| {
                let r = parts(r, 3)?;
                Ok((big(&r[0])?, (big(&r[1])?, big(&r[2])?)))
            })
            .collect::<Result<_, Error>>()?;
        let schedule = rows(&p[5])?
            .iter()
            .map(|r| {
                let r = parts(r, 2)?;
                Ok((big(&r[0])?, object(&r[1])?))
            })
            .collect::<Result<_, Error>>()?;
        let epoch_starts: Vec<_> = rows(&p[6])?
            .iter()
            .map(|r| {
                let r = parts(r, 3)?;
                Ok(EpochStart {
                    epoch: big(&r[0])?,
                    height: big(&r[1])?,
                    time: big(&r[2])?,
                })
            })
            .collect::<Result<_, Error>>()?;
        if epoch_starts.len() > 5 {
            return Err(corrupt());
        }
        let leaders = leader::Cache::restore(
            epochs,
            ranges,
            schedule,
            leader::Limits {
                maximum_slots: limits.max_nodes,
                maximum_gc_span: limits.max_nodes,
                maximum_address_bytes: limits.max_bytes,
            },
        )
        .map_err(|_| Error::ResourceLimit)?;
        let checkpoint = Self {
            revision: u64_value(&p[1])?,
            first_revision: u64_value(&p[2])?,
            state: State {
                leaders,
                epoch_starts,
            },
        };
        if checkpoint.first_revision == 0
            || (checkpoint.first_revision > checkpoint.revision
                && !(checkpoint.revision == 1
                    && checkpoint.first_revision == 2
                    && checkpoint.state == State::default()))
            || checkpoint.encode(limits)? != b
        {
            return Err(corrupt());
        }
        Ok(checkpoint)
    }
}
fn origin(o: Origin) -> u64 {
    match o {
        Origin::Bootstrap => 0,
        Origin::Rollback => 1,
        Origin::Synced => 2,
        Origin::Received => 3,
        Origin::Created => 4,
    }
}
impl Record {
    pub fn emitted(&self) -> impl Iterator<Item = &Event> {
        self.events
            .iter()
            .filter(|_| matches!(self.context.origin, Origin::Received | Origin::Created))
    }
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        let block = self.context.block.map(|b| {
            tuple(vec![
                V::Bytes(b.hash.0.to_vec()),
                integer(match b.kind {
                    Kind::Key => 0,
                    Kind::Micro => 1,
                }),
                V::Bytes(b.previous.0.to_vec()),
                integer(b.height),
            ])
        });
        encoding::encode(
            &tuple(vec![
                integer(1),
                integer(self.revision),
                integer(origin(self.context.origin)),
                option(block),
                V::List(
                    self.events
                        .iter()
                        .map(|e| event(e, limits))
                        .collect::<Result<_, _>>()?,
                ),
            ]),
            limits,
        )
        .map_err(codec_error)
    }
    pub fn decode(b: &[u8], limits: Limits) -> Result<Self, Error> {
        let value = encoding::decode(b, limits).map_err(codec_error)?;
        let p = parts(&value, 5)?;
        if u64_value(&p[0])? != 1 {
            return Err(corrupt());
        }
        let origin = match u64_value(&p[2])? {
            0 => Origin::Bootstrap,
            1 => Origin::Rollback,
            2 => Origin::Synced,
            3 => Origin::Received,
            4 => Origin::Created,
            _ => return Err(corrupt()),
        };
        let block = some(&p[3])?
            .map(|v| {
                let p = parts(v, 4)?;
                Ok(BlockInfo {
                    hash: hash_value(&p[0])?,
                    kind: match u64_value(&p[1])? {
                        0 => Kind::Key,
                        1 => Kind::Micro,
                        _ => return Err(corrupt()),
                    },
                    previous: hash_value(&p[2])?,
                    height: u64_value(&p[3])?,
                })
            })
            .transpose()?;
        let record = Self {
            revision: u64_value(&p[1])?,
            context: Context { origin, block },
            events: rows(&p[4])?
                .iter()
                .map(read_event)
                .collect::<Result<_, _>>()?,
        };
        if record.revision == 0
            || (origin == Origin::Rollback) != block.is_none()
            || (!record.events.is_empty()
                && !matches!(
                    block,
                    Some(BlockInfo {
                        kind: Kind::Key,
                        ..
                    })
                ))
            || record.encode(limits)? != b
        {
            return Err(corrupt());
        }
        match record.context {
            Context {
                origin: Origin::Bootstrap,
                block:
                    Some(BlockInfo {
                        kind: Kind::Key,
                        height: 0,
                        ..
                    }),
            } if record.events.is_empty() => (),
            Context {
                origin: Origin::Rollback,
                block: None,
            } if record.events.is_empty() => (),
            Context {
                origin: Origin::Synced | Origin::Received | Origin::Created,
                block: Some(b),
            } if b.height > 0 => (),
            _ => return Err(corrupt()),
        }
        Ok(record)
    }
}
impl<S: Read> View<S> {
    pub fn hyperchain_checkpoint(&self) -> Result<Checkpoint, Error> {
        if self.identity.consensus != ConsensusType::Pos {
            return Err(Error::Identity);
        }
        match self.source.get(HC_STATE)? {
            Some(b) => {
                let c = Checkpoint::decode(&b, self.config.hyperchain_metadata)?;
                if c.revision != self.status.revision {
                    return Err(Error::Corrupt("HC checkpoint revision"));
                }
                Ok(c)
            }
            None if self.status.tip.is_none()
                || (self.status.revision == 1
                    && self.status.tip == Some(self.identity.genesis)
                    && self.source.get(IDENTITY)?.as_deref()
                        == Some(self.identity.encode_version(10).as_slice())) =>
            {
                Ok(Checkpoint {
                    revision: self.status.revision,
                    first_revision: self
                        .status
                        .revision
                        .checked_add(1)
                        .ok_or(Error::ResourceLimit)?,
                    state: Default::default(),
                })
            }
            None => Err(Error::Corrupt("missing HC checkpoint")),
        }
    }
    pub fn hyperchain_event(&self, revision: u64) -> Result<Option<Record>, Error> {
        let checkpoint = self.hyperchain_checkpoint()?;
        if revision < checkpoint.first_revision || revision > self.status.revision {
            return Ok(None);
        }
        let b = self
            .source
            .get(&key(HC_EVENTS, revision.to_be_bytes()))?
            .ok_or(Error::Corrupt("missing HC event"))?;
        let r = Record::decode(&b, self.config.hyperchain_metadata)?;
        let change = self
            .event(revision)?
            .ok_or(Error::Corrupt("HC event without change"))?;
        if r.revision != revision || r.context.block.map(|b| b.hash) != change.inserted {
            return Err(Error::Corrupt("HC event binding"));
        }
        if let Some(info) = r.context.block
            && let Some(block) = self.block(info.hash)?
            && info != BlockInfo::of(&block, info.hash)
        {
            return Err(Error::Corrupt("HC event block context"));
        }
        Ok(Some(r))
    }
}
