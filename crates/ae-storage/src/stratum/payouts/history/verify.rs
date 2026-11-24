use super::*;
impl Entry {
    pub(super) fn validate(&self, v: &repo::View<Snapshot<'_>>) -> Result<(), Failure> {
        self.information.validate(
            Limits::maximum(),
            super::super::super::Limits::maximum().transaction,
        )?;
        if self.information.network != v.identity().network
            || v.identity().consensus != ae_protocol::ConsensusType::Pow
            || self.information.context.chain.revision > v.status().revision
            || self
                .information
                .pending
                .as_ref()
                .is_some_and(|r| r.observed_revision() > v.status().revision)
            || self
                .information
                .last
                .as_ref()
                .is_some_and(|c| c.chain.revision > v.status().revision)
            || self.index == 0
            || self.index > MAX_RECORDS
            || (self.index == 1) != (self.previous == Hash256::ZERO)
        {
            return Err(Error::Corrupt("payout history identity").into());
        }
        match (&self.kind, &self.completed_request, &self.receipt) {
            (Kind::Baseline, None, None) if self.index == 1 => {}
            (Kind::Transition, None, None) => {}
            (Kind::Completed, Some(r), Some(raw)) => {
                let c = self
                    .information
                    .last
                    .as_ref()
                    .ok_or(Error::Corrupt("payout history completion"))?;
                if self.information.pending.is_some()
                    || c.sequence != r.sequence
                    || c.payment != r.payment.id
                    || Some(c.hash) != r.hash()?
                    || c.record
                        != r.fingerprint(
                            Limits::maximum(),
                            super::super::super::Limits::maximum().transaction,
                        )?
                {
                    return Err(Error::Corrupt("payout history completed request").into());
                }
                retention::historical(v, &self.information.config, r, c, raw)?;
            }
            _ => return Err(Error::Corrupt("payout history evidence kind").into()),
        }
        Ok(())
    }
}
pub(super) fn all(s: &Stored<'_>) -> Result<(), Failure> {
    let info = metadata(s)?;
    current(s, info.as_ref())?;
    let Some(h) = head(s)? else {
        return Ok(());
    };
    let v = view(s, Default::default())?;
    let mut it = s.source.snapshot.raw_iterator();
    it.seek(PREFIX);
    let mut count = 0u64;
    let mut bytes = 0u64;
    let mut previous = Hash256::ZERO;
    let mut last: Option<Entry> = None;
    while let Some(k) = it.key().filter(|k| k.starts_with(PREFIX)) {
        count = count.checked_add(1).ok_or(Error::ResourceLimit)?;
        if count > h.summary.records || k != key(count) {
            return Err(Error::Corrupt("payout history ordering").into());
        }
        let raw = it
            .value()
            .ok_or(Error::Corrupt("payout history iterator"))?;
        bytes = bytes
            .checked_add((k.len() + raw.len()) as u64)
            .filter(|n| *n <= MAX_BYTES)
            .ok_or(Error::ResourceLimit)?;
        let e = Entry::decode(raw)?;
        e.validate(&v)?;
        if e.index != count
            || e.previous != previous
            || (e.kind == Kind::Baseline && !h.summary.partial)
        {
            return Err(Error::Corrupt("payout history linkage").into());
        }
        if count == 1 && h.summary.partial != (e.kind == Kind::Baseline) {
            return Err(Error::Corrupt("payout history baseline flag").into());
        }
        if let Some(old) = &last {
            follows(old, &e)?;
        }
        previous = ae_crypto::blake2b_256(raw);
        last = Some(e);
        it.next();
    }
    it.status().map_err(backend)?;
    if count != h.summary.records || bytes != h.summary.bytes || previous != h.summary.tip {
        return Err(Error::Corrupt("payout history totals").into());
    }
    Ok(())
}

pub(super) fn follows(old: &Entry, next: &Entry) -> Result<(), Failure> {
    let a = &old.information;
    let b = &next.information;
    let invalid = || Failure::Storage(Error::Corrupt("payout history transition"));
    if old.index.checked_add(1) != Some(next.index)
        || a.config != b.config
        || a.network != b.network
        || payload(a, b)
    {
        return Err(invalid());
    }
    match (&a.pending, &b.pending) {
        (None, Some(r))
            if next.kind == Kind::Transition
                && a.next.checked_add(1) == Some(b.next)
                && r.sequence == a.next
                && a.last == b.last => {}
        (Some(r), None)
            if next.kind == Kind::Completed
                && next.completed_request.as_ref() == Some(r)
                && a.next == b.next => {}
        (Some(r), Some(n))
            if next.kind == Kind::Transition
                && a.next == b.next
                && a.last == b.last
                && r.sequence == n.sequence
                && r.payment == n.payment
                && r.request == n.request =>
        {
            match (&r.signed, &n.signed) {
                (None, Some(s)) if s.dispatch == Dispatch::Ready(0) => {}
                (Some(s), Some(t))
                    if s.signature == t.signature
                        && s.date == t.date
                        && s.transaction == t.transaction =>
                {
                    let valid = match (&s.dispatch, &t.dispatch) {
                        (Dispatch::Ready(n), Dispatch::Sending(m)) => n.checked_add(1) == Some(*m),
                        (
                            Dispatch::Sending(n),
                            Dispatch::Ready(m) | Dispatch::Done { attempt: m, .. },
                        ) => n == m,
                        _ => false,
                    };
                    if !valid {
                        return Err(invalid());
                    }
                }
                _ => return Err(invalid()),
            }
        }
        _ => return Err(invalid()),
    }
    Ok(())
}
