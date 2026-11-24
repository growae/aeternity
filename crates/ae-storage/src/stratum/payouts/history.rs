// Immutable operational payout transitions, committed with their accounting
// effects. These records are not part of any canonical chain state tree.
use super::*;
mod codec;
mod verify;
pub(super) const HEAD: &[u8] = b"\0stratum-payout-history";
pub(super) const PREFIX: &[u8] = b"\0stratum-payout-history/";
pub const MAX_RECORDS: u64 = 65_536;
pub const MAX_BYTES: u64 = 256 << 20;
const MAX_ENTRY: usize = MAX_META + retention::MAX_BYTES + 16384;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Baseline,
    Transition,
    Completed,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub index: u64,
    pub previous: Hash256,
    pub kind: Kind,
    pub information: Info,
    pub completed_request: Option<Record>,
    // Exact operational receipt archive, including detached inclusion proofs.
    // Historical inclusion is evidence, not a claim of present finality.
    pub receipt: Option<Vec<u8>>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub records: u64,
    pub bytes: u64,
    pub tip: Hash256,
    pub partial: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    pub summary: Option<Summary>,
    pub entries: Vec<Entry>,
    pub next: Option<u64>,
}
struct Head {
    identity: Vec<u8>,
    summary: Summary,
}
fn key(index: u64) -> Vec<u8> {
    [PREFIX, &index.to_be_bytes()].concat()
}
fn payload(a: &Info, b: &Info) -> bool {
    a.config == b.config
        && a.network == b.network
        && a.next == b.next
        && a.pending == b.pending
        && a.last == b.last
}
fn metadata(s: &Stored<'_>) -> Result<Option<Info>, Failure> {
    s.source
        .snapshot
        .get_pinned(META)
        .map_err(backend)?
        .map(|r| {
            Info::decode(
                &r,
                Limits::maximum(),
                super::super::Limits::maximum().transaction,
            )
        })
        .transpose()
}
fn head(s: &Stored<'_>) -> Result<Option<Head>, Failure> {
    s.source
        .snapshot
        .get_pinned(HEAD)
        .map_err(backend)?
        .map(|r| Head::decode(&r))
        .transpose()
}
fn entry(s: &Stored<'_>, index: u64, max: usize) -> Result<(Entry, Hash256, usize), Failure> {
    let raw = s
        .source
        .snapshot
        .get_pinned(key(index))
        .map_err(backend)?
        .ok_or(Error::Corrupt("missing payout history entry"))?;
    if raw.len() > max {
        return Err(Error::ResourceLimit.into());
    }
    let e = Entry::decode(&raw)?;
    if e.index != index {
        return Err(Error::Corrupt("payout history index").into());
    }
    Ok((
        e,
        ae_crypto::blake2b_256(&raw),
        raw.len() + key(index).len(),
    ))
}
fn legacy(s: &Stored<'_>) -> Result<bool, Failure> {
    let raw = s
        .source
        .snapshot
        .get_pinned(repo::IDENTITY)
        .map_err(backend)?
        .ok_or(Error::Uninitialized)?;
    let Value::List(v) = Value::decode(&raw).map_err(|_| Error::Corrupt("identity encoding"))?
    else {
        return Err(Error::Corrupt("identity fields").into());
    };
    Ok(integer(v.first().ok_or(Error::Corrupt("identity version"))?)? < 21)
}
fn has_entries(s: &Stored<'_>) -> Result<bool, Failure> {
    let mut it = s.source.snapshot.raw_iterator();
    it.seek(PREFIX);
    let found = it.key().is_some_and(|k| k.starts_with(PREFIX));
    it.status().map_err(backend)?;
    Ok(found)
}
pub(super) fn current(s: &Stored<'_>, info: Option<&Info>) -> Result<(), Failure> {
    let Some(h) = head(s)? else {
        if has_entries(s)? || (info.is_some() && !legacy(s)?) {
            return Err(Error::Corrupt("missing payout history head").into());
        }
        return Ok(());
    };
    let info = info.ok_or(Error::Corrupt("payout history without metadata"))?;
    let v = view(s, Default::default())?;
    if h.identity != v.identity().binding() {
        return Err(Error::Identity.into());
    }
    let (last, hash, _) = entry(s, h.summary.records, MAX_ENTRY)?;
    last.validate(&v)?;
    if hash != h.summary.tip || !payload(&last.information, info) {
        return Err(Error::Corrupt("payout history current binding").into());
    }
    Ok(())
}
impl Stored<'_> {
    // Cursor is the last returned index. The immutable Stored snapshot binds
    // every page to one head; a caller can retain it or compare summary tips.
    pub fn payout_history(&self, after: u64, limit: usize, bytes: usize) -> Result<Page, Failure> {
        if limit == 0 || limit > 1024 || bytes == 0 || bytes > MAX_BYTES as usize {
            return Err(Error::ResourceLimit.into());
        }
        let Some(h) = head(self)? else {
            if after != 0 {
                return Err(Error::Conflict.into());
            }
            if has_entries(self)?
                || (self
                    .source
                    .snapshot
                    .get_pinned(META)
                    .map_err(backend)?
                    .is_some()
                    && !legacy(self)?)
            {
                return Err(Error::Corrupt("missing payout history head").into());
            }
            return Ok(Page {
                summary: None,
                entries: vec![],
                next: None,
            });
        };
        if after > h.summary.records {
            return Err(Error::Conflict.into());
        }
        let v = view(self, Default::default())?;
        if h.identity != v.identity().binding() {
            return Err(Error::Identity.into());
        }
        if after == h.summary.records {
            return Ok(Page {
                summary: Some(h.summary),
                entries: vec![],
                next: None,
            });
        }
        let mut used = 0usize;
        let mut previous = if after == 0 {
            Hash256::ZERO
        } else {
            let raw = self
                .source
                .snapshot
                .get_pinned(key(after))
                .map_err(backend)?
                .ok_or(Error::Corrupt("missing payout history predecessor"))?;
            used = raw
                .len()
                .checked_add(key(after).len())
                .ok_or(Error::ResourceLimit)?;
            if used > bytes {
                return Err(Error::ResourceLimit.into());
            }
            ae_crypto::blake2b_256(&raw)
        };
        let mut entries = Vec::new();
        let mut cursor = after;
        while cursor < h.summary.records && entries.len() < limit {
            let k = key(cursor + 1);
            let raw = self
                .source
                .snapshot
                .get_pinned(&k)
                .map_err(backend)?
                .ok_or(Error::Corrupt("missing payout history entry"))?;
            let total = used
                .checked_add(k.len())
                .and_then(|n| n.checked_add(raw.len()))
                .ok_or(Error::ResourceLimit)?;
            if total > bytes {
                if entries.is_empty() {
                    return Err(Error::ResourceLimit.into());
                }
                break;
            }
            let e = Entry::decode(&raw)?;
            e.validate(&v)?;
            if e.index != cursor + 1 || e.previous != previous {
                return Err(Error::Corrupt("payout history continuity").into());
            }
            if let Some(prior) = entries.last() {
                verify::follows(prior, &e)?;
            }
            previous = ae_crypto::blake2b_256(&raw);
            used = total;
            cursor += 1;
            entries.push(e);
        }
        if cursor == h.summary.records && previous != h.summary.tip {
            return Err(Error::Corrupt("payout history tip").into());
        }
        Ok(Page {
            next: (cursor < h.summary.records).then_some(cursor),
            summary: Some(h.summary),
            entries,
        })
    }
}
fn append(
    s: &Stored<'_>,
    old: Option<&Head>,
    kind: Kind,
    info: Info,
    completed: Option<Record>,
    receipt: Option<Vec<u8>>,
    partial: bool,
) -> Result<Vec<Extra>, Failure> {
    let summary = old.map(|h| &h.summary);
    let index = summary
        .map_or(Some(1), |h| h.records.checked_add(1))
        .ok_or(Error::ResourceLimit)?;
    if index > MAX_RECORDS {
        return Err(Error::ResourceLimit.into());
    }
    let e = Entry {
        index,
        previous: summary.map_or(Hash256::ZERO, |h| h.tip),
        kind,
        information: info,
        completed_request: completed,
        receipt,
    };
    let v = view(s, Default::default())?;
    e.validate(&v)?;
    if let Some(h) = old {
        verify::follows(&entry(s, h.summary.records, MAX_ENTRY)?.0, &e)?;
    }
    let raw = e.encode()?;
    let k = key(index);
    let bytes = summary
        .map_or(0, |h| h.bytes)
        .checked_add((k.len() + raw.len()) as u64)
        .filter(|n| *n <= MAX_BYTES)
        .ok_or(Error::ResourceLimit)?;
    if s.source.snapshot.get_pinned(&k).map_err(backend)?.is_some() {
        return Err(Error::Corrupt("payout history append collision").into());
    }
    let h = Head {
        identity: v.identity().binding(),
        summary: Summary {
            records: index,
            bytes,
            tip: ae_crypto::blake2b_256(&raw),
            partial: partial || summary.is_some_and(|h| h.partial),
        },
    };
    Ok(vec![
        Extra {
            key: HEAD.to_vec(),
            before: s.source.snapshot.get(HEAD).map_err(backend)?,
            after: Some(h.encode()),
        },
        Extra {
            key: k,
            before: None,
            after: Some(raw),
        },
    ])
}
pub(in crate::stratum) fn transition(
    s: &Stored<'_>,
    changes: &[Extra],
) -> Result<Vec<Extra>, Failure> {
    let Some(change) = changes.iter().find(|e| e.key == META) else {
        return Ok(vec![]);
    };
    let next = Info::decode(
        change
            .after
            .as_deref()
            .ok_or(Error::Corrupt("payout metadata deletion"))?,
        Limits::maximum(),
        super::super::Limits::maximum().transaction,
    )?;
    let old = s.payouts(Limits::maximum())?;
    if old.as_ref().is_some_and(|i| payload(i, &next)) {
        return Ok(vec![]);
    }
    let h = head(s)?;
    if old.is_some() && h.is_none() {
        return Err(Error::Corrupt("payout history migration missing").into());
    }
    let completed = old
        .as_ref()
        .and_then(|i| i.pending.as_ref())
        .filter(|_| next.pending.is_none())
        .cloned();
    let receipt = if let Some(r) = &completed {
        let c = next
            .last
            .as_ref()
            .ok_or(Error::Corrupt("payout completion missing"))?;
        if c.record
            != r.fingerprint(
                Limits::maximum(),
                super::super::Limits::maximum().transaction,
            )?
        {
            return Err(Error::Corrupt("payout history completion request").into());
        }
        Some(retention::completed(
            &view(s, Default::default())?,
            old.as_ref().expect("pending"),
            c.block,
        )?)
    } else {
        None
    };
    let partial = old.is_none() && next.pending.as_ref().is_some_and(|r| r.request.is_none());
    let kind = if completed.is_some() {
        Kind::Completed
    } else if partial {
        Kind::Baseline
    } else {
        Kind::Transition
    };
    append(s, h.as_ref(), kind, next, completed, receipt, partial)
}
pub(crate) fn upgrade(db: &Database, batch: &mut WriteBatch) -> Result<(), Error> {
    let result = (|| {
        let s = db.stratum(super::super::Limits::maximum())?;
        let info = s.payouts(Limits::maximum())?;
        if head(&s)?.is_none()
            && let Some(info) = info
        {
            for e in append(&s, None, Kind::Baseline, info, None, None, true)? {
                batch.put(e.key, e.after.expect("history append"));
            }
        }
        Ok::<_, Failure>(())
    })();
    result.map_err(|e| match e {
        Failure::Storage(e) => e,
        _ => Error::Corrupt("payout history upgrade"),
    })
}
pub(crate) fn verify_all(s: &Stored<'_>) -> Result<(), Failure> {
    verify::all(s)
}
