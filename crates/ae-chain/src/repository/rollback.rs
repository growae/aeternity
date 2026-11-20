// aec_consensus_bitcoin_ng rollback and startup community-fork enforcement,
// aeternity fa2a6d7. ISC notice in repository LICENSE.
use super::*;
use ae_codec::headers::Header;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy)]
pub enum Target {
    Height(u64),
    Key(Hash256),
    Micro(Hash256),
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub blocks: usize,
    pub ancestry: usize,
    pub transactions: usize,
    pub reference_scan: usize,
    pub commit_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            blocks: 100_000,
            ancestry: 100_000,
            transactions: 1_000_000,
            reference_scan: 1_000_000,
            commit_bytes: 512 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Repository(Error),
    UnknownHeight(u64),
    WrongType,
    AboveTip,
    NotCanonical,
}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Repository(e)
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Failure {}

struct Stored {
    header: Header,
    transactions: Vec<Hash256>,
}

fn height_range<S: Read>(
    view: &View<S>,
    first: u64,
    last: u64,
    limit: usize,
    transaction_limit: usize,
) -> Result<Vec<(Hash256, Stored)>, Error> {
    if first > last {
        return Ok(vec![]);
    }
    let start = key(HEIGHTS, first.to_be_bytes());
    let end = last
        .checked_add(1)
        .map_or_else(|| vec![HEIGHTS + 1], |h| key(HEIGHTS, h.to_be_bytes()));
    let mut transaction_count = 0usize;
    view.source
        .range(&start, &end, limit)?
        .into_iter()
        .map(|(k, v)| {
            if k.len() != 41 || !v.is_empty() {
                return Err(Error::Corrupt("rollback height index"));
            }
            let h = fixed_hash(&k[9..])?;
            let b = view.require_block(h)?;
            if b.header().common().height.to_be_bytes() != k[1..9] {
                return Err(Error::Corrupt("rollback indexed height"));
            }
            let header = b.header();
            let transactions = if let Block::Micro(m) = b {
                transaction_count = transaction_count
                    .checked_add(m.transactions.len())
                    .ok_or(Error::ResourceLimit)?;
                if transaction_count > transaction_limit {
                    return Err(Error::ResourceLimit);
                }
                m.transactions.into_iter().map(|tx| tx.hash()).collect()
            } else {
                vec![]
            };
            Ok((
                h,
                Stored {
                    header,
                    transactions,
                },
            ))
        })
        .collect()
}
fn previous(block: &Block) -> Hash256 {
    Hash256(block.header().common().previous_hash)
}
fn remove(w: &mut Writes, prefix: u8, suffix: impl AsRef<[u8]>) {
    w.insert(key(prefix, suffix), None);
}

// Explicit administrative mutation. Retained shared transaction records are
// protected, and Rust-owned indexes are reconciled with the resulting tip.
pub fn prepare<S: Read>(view: &View<S>, target: Target, limits: Limits) -> Result<Update, Failure> {
    let old = view.status.tip.ok_or(Error::Uninitialized)?;
    let top = view.require_block(old)?;
    let top_height = top.header().common().height;
    let (hash, by_height) = match target {
        Target::Height(h) => (
            view.canonical_key(h)?.ok_or(Failure::UnknownHeight(h))?,
            true,
        ),
        Target::Key(h) | Target::Micro(h) => (h, false),
    };
    let block = view.require_block(hash)?;
    let height = block.header().common().height;
    if matches!(
        (&target, &block),
        (Target::Height(_) | Target::Key(_), Block::Micro(_)) | (Target::Micro(_), Block::Key(_))
    ) {
        return Err(Failure::WrongType);
    }
    if height > top_height {
        return Err(Failure::AboveTip);
    }
    if !view.is_canonical(hash)? {
        return Err(Failure::NotCanonical);
    }
    let record = view.require_record(hash)?;
    if record.roots.state.hash().0 != block.header().common().state_hash {
        return Err(Error::Corrupt("rollback target state").into());
    }
    record.roots.load(view.source.clone())?;
    let mut detached = vec![];
    let mut cursor = old;
    let mut seen = BTreeSet::new();
    while cursor != hash {
        if detached.len() >= limits.ancestry {
            return Err(Error::ResourceLimit.into());
        }
        if !seen.insert(cursor) {
            return Err(Error::Corrupt("rollback ancestry cycle").into());
        }
        let b = view.require_block(cursor)?;
        if cursor == view.identity.genesis || b.header().common().height < height {
            return Err(Failure::NotCanonical);
        }
        detached.push(cursor);
        cursor = previous(&b);
    }
    let mut kept_generation = BTreeSet::new();
    if !by_height {
        cursor = hash;
        loop {
            if kept_generation.len() >= limits.ancestry {
                return Err(Error::ResourceLimit.into());
            }
            if !kept_generation.insert(cursor) {
                return Err(Error::Corrupt("rollback generation cycle").into());
            }
            let b = view.require_block(cursor)?;
            if b.header().common().height != height {
                return Err(Error::Corrupt("rollback generation height").into());
            }
            if matches!(b, Block::Key(_)) {
                break;
            }
            cursor = previous(&b);
        }
    }
    let first = if by_height {
        height.checked_add(1)
    } else {
        Some(height)
    };
    let removed: BTreeMap<_, _> = if let Some(first) = first {
        height_range(
            view,
            first,
            top_height.saturating_add(1000),
            limits.blocks,
            limits.transactions,
        )?
        .into_iter()
        .filter(|(h, _)| !kept_generation.contains(h))
        .collect()
    } else {
        BTreeMap::new()
    };
    if removed.contains_key(&hash) {
        return Err(Error::Corrupt("rollback deletes target").into());
    }
    let mut transactions = BTreeSet::new();
    let mut end_candidates = BTreeSet::new();
    let mut writes = Writes::new();
    for (h, b) in &removed {
        for prefix in [BLOCKS, RECORDS, CANONICAL, SIGNALS, PROOFS, ENDS] {
            remove(&mut writes, prefix, h.0);
        }
        let mut index = b.header.common().height.to_be_bytes().to_vec();
        index.extend(h.0);
        remove(&mut writes, HEIGHTS, index);
        let mut child = b.header.common().previous_hash.to_vec();
        child.extend(h.0);
        remove(&mut writes, CHILDREN, child);
        match &b.header {
            Header::Key(k) => {
                end_candidates.insert(Hash256(k.common.previous_key_hash));
            }
            Header::Micro(_) => {
                for tx in &b.transactions {
                    transactions.insert(*tx);
                    if transactions.len() > limits.transactions {
                        return Err(Error::ResourceLimit.into());
                    }
                    if view.location(*tx)? == Some(*h) {
                        remove(&mut writes, LOCATIONS, tx.0);
                    }
                }
            }
        }
    }
    // Original remove_tx deletes shared values blindly. Keep any value still
    // referenced by a surviving block so a rollback cannot corrupt that block.
    if !transactions.is_empty() {
        view.source.visit_range(
            &[BLOCKS],
            &[BLOCKS + 1],
            limits.reference_scan,
            &mut |k, _| {
                let h = fixed_hash(&k[1..])?;
                if !removed.contains_key(&h)
                    && let Block::Micro(m) = view.require_block(h)?
                {
                    for tx in m.transactions {
                        transactions.remove(&tx.hash());
                    }
                }
                Ok(())
            },
        )?;
    }
    for tx in &transactions {
        remove(&mut writes, TRANSACTIONS, tx.0);
        remove(&mut writes, LOCATIONS, tx.0);
    }
    for h in &detached {
        remove(&mut writes, CANONICAL, h.0);
        match view.require_block(*h)? {
            Block::Key(k) => remove(&mut writes, KEYS, k.common.height.to_be_bytes()),
            Block::Micro(m) => {
                for tx in m.transactions {
                    if view.location(tx.hash())? == Some(*h) {
                        remove(&mut writes, LOCATIONS, tx.hash().0);
                    }
                }
            }
        }
    }
    for h in end_candidates {
        if removed.contains_key(&h) {
            continue;
        }
        let Block::Key(k) = view.require_block(h)? else {
            return Err(Error::Corrupt("rollback key predecessor").into());
        };
        let remaining = if let Some(next) = k.common.height.checked_add(1) {
            height_range(view, next, next, limits.blocks, limits.transactions)?
                .into_iter()
                .any(|(candidate, b)| {
                    !removed.contains_key(&candidate)
                        && matches!(b.header,Header::Key(n) if n.common.previous_key_hash==h.0)
                })
        } else {
            false
        };
        if remaining {
            remove(&mut writes, ENDS, h.0);
        } else {
            writes.insert(key(ENDS, h.0), Some(vec![]));
        }
    }
    let change = Change {
        revision: view
            .status
            .revision
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?,
        inserted: None,
        old_tip: Some(old),
        new_tip: hash,
        ancestor: Some(hash),
        detached,
        attached: vec![],
        requeue: vec![],
        confirm: vec![],
        removed: removed.keys().copied().collect(),
        discarded: transactions.into_iter().collect(),
    };
    Ok(Update::rollback(view, change, writes, limits.commit_bytes)?)
}

pub const COMMUNITY_FORK_HEIGHT: u64 = 366_182;
pub fn community_fork_hash() -> Hash256 {
    let raw = ae_codec::api::decode_as(
        ae_codec::api::ApiType::KeyBlockHash,
        "kh_YGDYbJQL4TV84CSaEA4VgcppUkpVyxSWFyHbS3frKkPjHPw2m",
    )
    .expect("pinned community hash");
    Hash256(raw.try_into().expect("32-byte community hash"))
}
pub fn community_fork_target<S: Read>(view: &View<S>) -> Result<Option<Target>, Error> {
    if view.identity.network != b"ae_mainnet" {
        return Ok(None);
    }
    let Some(top) = view.status.tip else {
        return Ok(None);
    };
    if view.require_block(top)?.header().common().height <= COMMUNITY_FORK_HEIGHT {
        return Ok(None);
    }
    let selected = view
        .canonical_key(COMMUNITY_FORK_HEIGHT)?
        .ok_or(Error::Corrupt("missing community checkpoint"))?;
    Ok((selected != community_fork_hash()).then_some(Target::Height(COMMUNITY_FORK_HEIGHT)))
}
