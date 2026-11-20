// Snapshot queries corresponding to aec_chain generation/successor traversal.
use super::{Block, Error, HEIGHTS, Read, View, key};
use ae_codec::headers::HeaderMode;
use ae_types::{Hash256, Unsigned};

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub items: usize,
    pub bytes: usize,
}
#[derive(Debug)]
pub struct Generation {
    pub key: Block,
    pub micros: Vec<Block>,
}
fn key_block<S: Read>(view: &View<S>, hash: Hash256) -> Result<Block, Error> {
    let block = view.require_block(hash)?;
    if !matches!(block, Block::Key(_)) {
        return Err(Error::Corrupt("key index points to micro"));
    }
    Ok(block)
}

// N is a wire integer, not a machine-sized allocation request. Clamp it to the
// finite selected-chain distance before applying local response quotas.
pub fn successors<S: Read>(
    view: &View<S>,
    from: Hash256,
    n: &Unsigned,
    limits: Limits,
) -> Result<Option<Vec<(u64, Hash256)>>, Error> {
    let Some(top) = view.status().tip else {
        return Ok(None);
    };
    let top = view.require_block(top)?.header().common().height;
    let Some(from_block) = view.block(from)? else {
        return Ok(None);
    };
    let height = from_block.header().common().height;
    let Some(distance) = top.checked_sub(height) else {
        return Ok(None);
    };
    let delta = n.to_u64().unwrap_or(u64::MAX).min(distance);
    let count = usize::try_from(delta).map_err(|_| Error::ResourceLimit)?;
    if count > limits.items || count > limits.bytes / 40 {
        return Err(Error::ResourceLimit);
    }
    // Native code raises on an absent canonical end header (e.g. an HC micro
    // top without its closing key). Do not turn that into a successful prefix.
    let mut hash = view
        .canonical_key(height + delta)?
        .ok_or(Error::MissingCanonicalKey(height + delta))?;
    let mut out = Vec::new();
    for remaining in (0..=delta).rev() {
        let block = key_block(view, hash)?;
        let header = block.header();
        if remaining == 0 {
            if hash != from {
                return Ok(None);
            }
        } else {
            out.push((header.common().height, hash));
            hash = Hash256(header.common().previous_key_hash);
        }
    }
    out.reverse();
    Ok(Some(out))
}

fn charge(block: &Block, limits: &mut Limits) -> Result<(), Error> {
    limits.items = limits.items.checked_sub(1).ok_or(Error::ResourceLimit)?;
    let bytes = block
        .encode()
        .map_err(|_| Error::Corrupt("block encoding"))?;
    limits.bytes = limits
        .bytes
        .checked_sub(bytes.len())
        .ok_or(Error::ResourceLimit)?;
    Ok(())
}

pub fn generation<S: Read>(
    view: &View<S>,
    hash: Hash256,
    forward: bool,
    mut limits: Limits,
) -> Result<Option<Generation>, Error> {
    let Some(block @ Block::Key(_)) = view.block(hash)? else {
        return Ok(None);
    };
    let header = block.header();
    let height = header.common().height;
    let last = if !forward {
        Hash256(header.common().previous_hash)
    } else {
        let top = view.status().tip.ok_or(Error::Uninitialized)?;
        let top_block = view.require_block(top)?;
        let top_header = top_block.header();
        let top_height = top_header.common().height;
        if top_height < height {
            return Ok(None);
        }
        if top_height == height {
            let top_key = match top_block {
                Block::Key(_) => top,
                Block::Micro(_) => Hash256(top_header.common().previous_key_hash),
            };
            if hash != top_key {
                return Ok(None);
            }
            top
        } else {
            // top_height > height makes height + 1 representable.
            let Some(next) = view.canonical_key(height + 1)? else {
                return Ok(None);
            };
            let next = key_block(view, next)?.header();
            if next.common().previous_key_hash != hash.0 {
                return Ok(None);
            }
            Hash256(next.common().previous_hash)
        }
    };
    charge(&block, &mut limits)?;
    let mb_height = match (view.config().mode, forward) {
        (HeaderMode::ProofOfWork, false) => height.checked_sub(1),
        (HeaderMode::ProofOfWork, true) | (HeaderMode::Hyperchain, false) => Some(height),
        (HeaderMode::Hyperchain, true) => Some(height.checked_add(1).ok_or(Error::ResourceLimit)?),
    };
    let mut micros = Vec::new();
    if let Some(mb_height) = mb_height {
        let mut cursor = last;
        loop {
            let mut index = mb_height.to_be_bytes().to_vec();
            index.extend(cursor.0);
            // Membership is a point read; unrelated forks at this height do not
            // consume the result quota. Native traversal stops at a missing row.
            if view.source().get(&key(HEIGHTS, index))?.is_none() {
                break;
            }
            let block = view.require_block(cursor)?;
            if block.header().common().height != mb_height {
                return Err(Error::Corrupt("generation height index"));
            }
            let Block::Micro(ref micro) = block else {
                break;
            };
            cursor = Hash256(micro.header.common.previous_hash);
            charge(&block, &mut limits)?;
            micros.push(block);
        }
    }
    micros.reverse();
    Ok(Some(Generation { key: block, micros }))
}
