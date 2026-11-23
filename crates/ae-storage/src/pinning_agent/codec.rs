use super::*;
use ae_vm_fate::{encoding, value::Value as V};
fn bad() -> Error {
    Error::Corrupt("pinning agent checkpoint")
}
fn t(v: Vec<V>) -> V {
    V::Tuple(v)
}
fn n(v: impl Into<BigInt>) -> V {
    V::Integer(v.into())
}
fn b(v: impl AsRef<[u8]>) -> V {
    V::Bytes(v.as_ref().to_vec())
}
fn opt(v: Option<V>) -> V {
    t(v.into_iter().collect())
}
fn parts(v: &V, len: usize) -> Result<&[V], Error> {
    match v {
        V::Tuple(v) if v.len() == len => Ok(v),
        _ => Err(bad()),
    }
}
fn rows(v: &V) -> Result<&[V], Error> {
    match v {
        V::List(v) => Ok(v),
        _ => Err(bad()),
    }
}
fn some(v: &V) -> Result<Option<&V>, Error> {
    match v {
        V::Tuple(v) if v.len() <= 1 => Ok(v.first()),
        _ => Err(bad()),
    }
}
fn blob(v: &V) -> Result<&[u8], Error> {
    match v {
        V::Bytes(b) => Ok(b),
        _ => Err(bad()),
    }
}
fn big(v: &V) -> Result<BigInt, Error> {
    match v {
        V::Integer(n) => Ok(n.clone()),
        _ => Err(bad()),
    }
}
fn u(v: &V) -> Result<u64, Error> {
    big(v)?.try_into().map_err(|_| bad())
}
fn flag(v: &V) -> Result<bool, Error> {
    match u(v)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(bad()),
    }
}
fn hash(v: &V) -> Result<Hash256, Error> {
    Ok(Hash256(blob(v)?.try_into().map_err(|_| bad())?))
}
fn account(v: &V) -> Result<AccountPublicKey, Error> {
    Ok(AccountPublicKey(hash(v)?.0))
}
fn unsigned(v: &V) -> Result<Unsigned, Error> {
    Unsigned::from_field_bytes(blob(v)?).map_err(|_| bad())
}
fn reason(v: &V) -> Result<String, Error> {
    String::from_utf8(blob(v)?.to_vec()).map_err(|_| bad())
}
fn limits(l: Limits) -> ae_vm_fate::Limits {
    ae_vm_fate::Limits {
        max_depth: 16,
        max_nodes: l.events.saturating_mul(8).saturating_add(256),
        max_bytes: l.bytes,
    }
}
fn error(e: ae_vm_fate::Error) -> Error {
    if e == ae_vm_fate::Error::ResourceLimit {
        Error::ResourceLimit
    } else {
        bad()
    }
}
fn state(s: &core::State) -> V {
    t(vec![
        n(u64::from(s.enabled)),
        opt(s.active.as_ref().map(|a| {
            t(vec![
                n(a.next_last.clone()),
                opt(a.parent_hash.as_ref().map(b)),
                n(u64::from(a.child_note)),
                b(a.leader.0),
            ])
        })),
    ])
}
fn read_state(v: &V) -> Result<core::State, Error> {
    let v = parts(v, 2)?;
    Ok(core::State {
        enabled: flag(&v[0])?,
        active: some(&v[1])?
            .map(|v| {
                let a = parts(v, 4)?;
                Ok::<_, Error>(core::Active {
                    next_last: big(&a[0])?,
                    parent_hash: some(&a[1])?.map(|v| blob(v).map(Vec::from)).transpose()?,
                    child_note: flag(&a[2])?,
                    leader: account(&a[3])?,
                })
            })
            .transpose()?,
    })
}
fn event(e: &core::Event) -> V {
    match e {
        core::Event::Epoch { epoch, first, last } => t(vec![
            n(0),
            n(epoch.clone()),
            n(first.clone()),
            n(last.clone()),
        ]),
        core::Event::Top { height } => t(vec![n(1), n(height.clone())]),
    }
}
fn read_event(v: &V) -> Result<core::Event, Error> {
    match v {
        V::Tuple(v) => match v.as_slice() {
            [tag, epoch, first, last] if u(tag)? == 0 => Ok(core::Event::Epoch {
                epoch: big(epoch)?,
                first: big(first)?,
                last: big(last)?,
            }),
            [tag, height] if u(tag)? == 1 => Ok(core::Event::Top {
                height: big(height)?,
            }),
            _ => Err(bad()),
        },
        _ => Err(bad()),
    }
}
fn post(p: &Post) -> V {
    t(vec![
        n(p.pin.epoch.clone()),
        n(p.pin.height.clone()),
        b(&p.pin.block_hash),
        b(p.account.0),
        b(p.amount.as_bytes()),
        b(p.fee.as_bytes()),
    ])
}
fn read_post(v: &V) -> Result<Post, Error> {
    let v = parts(v, 6)?;
    Ok(Post {
        pin: pin::Parent {
            epoch: big(&v[0])?,
            height: big(&v[1])?,
            block_hash: blob(&v[2])?.to_vec(),
        },
        account: account(&v[3])?,
        amount: unsigned(&v[4])?,
        fee: unsigned(&v[5])?,
    })
}
fn parent(p: &ParentResult) -> V {
    match p {
        ParentResult::Submitted { hash, intent } => t(vec![n(0), b(hash), b(intent.0)]),
        ParentResult::Unknown { intent, reason } => t(vec![n(1), b(intent.0), b(reason)]),
        ParentResult::NotSubmitted(reason) => t(vec![n(2), b(reason)]),
    }
}
fn read_parent(v: &V) -> Result<ParentResult, Error> {
    match v {
        V::Tuple(v) => match v.as_slice() {
            [tag, value, id] if u(tag)? == 0 => Ok(ParentResult::Submitted {
                hash: blob(value)?.to_vec(),
                intent: hash(id)?,
            }),
            [tag, id, value] if u(tag)? == 1 => Ok(ParentResult::Unknown {
                intent: hash(id)?,
                reason: reason(value)?,
            }),
            [tag, value] if u(tag)? == 2 => Ok(ParentResult::NotSubmitted(reason(value)?)),
            _ => Err(bad()),
        },
        _ => Err(bad()),
    }
}
fn child_result(c: &ChildResult) -> V {
    match c {
        ChildResult::Completed(id) => t(vec![n(0), b(id.0)]),
        ChildResult::Skipped(e) => t(vec![
            n(1),
            n(match e {
                core::Error::Signing => 0,
                core::Error::Encoding => 1,
                core::Error::ResourceLimit => 2,
            }),
        ]),
    }
}
fn read_child(v: &V) -> Result<ChildResult, Error> {
    let v = parts(v, 2)?;
    match u(&v[0])? {
        0 => Ok(ChildResult::Completed(hash(&v[1])?)),
        1 => Ok(ChildResult::Skipped(match u(&v[1])? {
            0 => core::Error::Signing,
            1 => core::Error::Encoding,
            _ => return Err(bad()),
        })),
        _ => Err(bad()),
    }
}
fn operation(o: &Operation) -> V {
    t(vec![
        opt(o.leader.map(|(l, m)| t(vec![b(l.0), n(u64::from(m))]))),
        opt(o.post.as_ref().map(post)),
        opt(o.parent.as_ref().map(parent)),
        opt(o.included.map(|v| n(u64::from(v)))),
        opt(o.note.as_ref().map(child_result)),
        opt(o.proof.as_ref().map(child_result)),
    ])
}
fn read_operation(v: &V) -> Result<Operation, Error> {
    let v = parts(v, 6)?;
    Ok(Operation {
        leader: some(&v[0])?
            .map(|v| {
                let v = parts(v, 2)?;
                Ok::<_, Error>((account(&v[0])?, flag(&v[1])?))
            })
            .transpose()?,
        post: some(&v[1])?.map(read_post).transpose()?,
        parent: some(&v[2])?.map(read_parent).transpose()?,
        included: some(&v[3])?.map(flag).transpose()?,
        note: some(&v[4])?.map(read_child).transpose()?,
        proof: some(&v[5])?.map(read_child).transpose()?,
    })
}
fn batch(p: &Batch) -> V {
    t(vec![
        n(p.revision),
        opt(p.old_tip.map(|h| b(h.0))),
        b(p.tip.0),
        b(p.source.0),
        V::List(p.events.iter().map(event).collect()),
        n(p.next as u64),
        operation(&p.operation),
    ])
}
fn read_batch(v: &V) -> Result<Batch, Error> {
    let v = parts(v, 7)?;
    Ok(Batch {
        revision: u(&v[0])?,
        old_tip: some(&v[1])?.map(hash).transpose()?,
        tip: hash(&v[2])?,
        source: hash(&v[3])?,
        events: rows(&v[4])?
            .iter()
            .map(read_event)
            .collect::<Result<_, _>>()?,
        next: u(&v[5])?.try_into().map_err(|_| bad())?,
        operation: read_operation(&v[6])?,
    })
}
impl Checkpoint {
    fn validate(&self, l: Limits) -> Result<(), Error> {
        l.validate()?;
        if self.version == 0 {
            return Err(bad());
        }
        Status::decode(&self.consumed.encode())?;
        let integer = |v: &BigInt| {
            if v.bits().div_ceil(8) > l.integer_bytes as u64 {
                Err(Error::ResourceLimit)
            } else {
                Ok(())
            }
        };
        if self.state.enabled && self.state.active.is_none() {
            return Err(bad());
        }
        if let Some(a) = &self.state.active {
            integer(&a.next_last)?;
            if a.child_note && a.parent_hash.is_none() {
                return Err(bad());
            }
            if a.parent_hash
                .as_ref()
                .is_some_and(|h| h.len() > l.hash_bytes)
            {
                return Err(Error::ResourceLimit);
            }
        }
        if let Some(p) = &self.pending {
            if p.revision
                != self
                    .consumed
                    .revision
                    .checked_add(1)
                    .ok_or(Error::ResourceLimit)?
                || p.old_tip != self.consumed.tip
                || p.next > p.events.len()
            {
                return Err(bad());
            }
            if p.events.len() > l.events {
                return Err(Error::ResourceLimit);
            }
            for e in &p.events {
                match e {
                    core::Event::Epoch { epoch, first, last } => {
                        integer(epoch)?;
                        integer(first)?;
                        integer(last)?;
                    }
                    core::Event::Top { height } => integer(height)?,
                }
            }
            let o = &p.operation;
            if p.next == p.events.len() {
                if *o != Operation::default() {
                    return Err(bad());
                }
            } else {
                match &p.events[p.next] {
                    core::Event::Epoch { .. } => {
                        if o.included.is_some()
                            || o.note.is_some()
                            || o.proof.is_some()
                            || (o.post.is_some() && !o.leader.is_some_and(|(_, m)| m))
                            || (o.parent.is_some() && o.post.is_none())
                        {
                            return Err(bad());
                        }
                    }
                    core::Event::Top { height } => {
                        if o.leader.is_some() || o.post.is_some() || o.parent.is_some() {
                            return Err(bad());
                        }
                        if !self.state.enabled && *o != Operation::default() {
                            return Err(bad());
                        }
                        if let Some(a) = &self.state.active {
                            if o.included == Some(true) && a.parent_hash.is_none() {
                                return Err(bad());
                            }
                            if o.note.is_some()
                                && (a.child_note
                                    || o.included != Some(true)
                                    || a.parent_hash.is_none())
                            {
                                return Err(bad());
                            }
                            if o.proof.is_some()
                                && (height != &a.next_last
                                    || a.parent_hash.is_none()
                                    || !(a.child_note || o.note.is_some()))
                            {
                                return Err(bad());
                            }
                        }
                    }
                }
            }
            if let Some(post) = &o.post {
                integer(&post.pin.epoch)?;
                integer(&post.pin.height)?;
                if post.pin.block_hash.len() != 32 {
                    return Err(bad());
                }
                if post.amount.as_bytes().len() > l.integer_bytes
                    || post.fee.as_bytes().len() > l.integer_bytes
                {
                    return Err(Error::ResourceLimit);
                }
            }
            if let Some(result) = &o.parent {
                match result {
                    ParentResult::Submitted { hash, .. } if hash.len() > l.hash_bytes => {
                        return Err(Error::ResourceLimit);
                    }
                    ParentResult::Unknown { reason, .. } | ParentResult::NotSubmitted(reason)
                        if reason.is_empty() || reason.len() > 4096 =>
                    {
                        return Err(Error::ResourceLimit);
                    }
                    _ => (),
                }
            }
            if [&o.note, &o.proof]
                .into_iter()
                .any(|c| matches!(c, Some(ChildResult::Skipped(core::Error::ResourceLimit))))
            {
                return Err(bad());
            }
        }
        Ok(())
    }
    pub(super) fn encode(&self, l: Limits) -> Result<Vec<u8>, Error> {
        self.validate(l)?;
        encoding::encode(
            &t(vec![
                n(1),
                n(self.version),
                b(self.binding.0),
                b(self.consumed.encode()),
                state(&self.state),
                n(self.completed),
                opt(self.pending.as_ref().map(batch)),
            ]),
            limits(l),
        )
        .map_err(error)
    }
    pub(super) fn decode(raw: &[u8], l: Limits) -> Result<Self, Error> {
        l.validate()?;
        let v = encoding::decode(raw, limits(l)).map_err(error)?;
        let v = parts(&v, 7)?;
        if u(&v[0])? != 1 {
            return Err(bad());
        }
        let c = Self {
            version: u(&v[1])?,
            binding: hash(&v[2])?,
            consumed: Status::decode(blob(&v[3])?)?,
            state: read_state(&v[4])?,
            completed: u(&v[5])?,
            pending: some(&v[6])?.map(read_batch).transpose()?,
        };
        if c.encode(l)? != raw {
            return Err(bad());
        }
        Ok(c)
    }
}
