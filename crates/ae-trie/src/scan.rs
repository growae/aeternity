// aeu_mp_trees:visit_reachable_hashes at aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC notice: LICENSE.
use super::*;

// Operational budgets, independent of consensus. Visits include repeated
// references, as in the original walker; no unbounded in-memory visited set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub pending: usize,
    pub nodes: u64,
    pub bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            pending: 65_536,
            nodes: 10_000_000,
            bytes: 16 * 1024 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct PageLimits {
    pub nodes: usize,
    pub bytes: usize,
}
impl Default for PageLimits {
    fn default() -> Self {
        Self {
            nodes: 100,
            bytes: 16 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Progress {
    pub nodes: u64,
    pub bytes: u64,
    pub pending: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pending: Vec<Hash256>,
    nodes: u64,
    bytes: u64,
    limits: Limits,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeBytes {
    pub hash: Hash256,
    pub bytes: Vec<u8>,
}
#[derive(Debug)]
pub struct Page {
    pub nodes: Vec<NodeBytes>,
    pub next: Cursor,
}
impl Cursor {
    pub fn new(roots: impl IntoIterator<Item = Hash256>, limits: Limits) -> Result<Self, Error> {
        let mut pending = Vec::new();
        for root in roots {
            if pending.len() == limits.pending {
                return Err(Error::ResourceLimit);
            }
            pending.push(root);
        }
        pending.reverse();
        Ok(Self {
            pending,
            nodes: 0,
            bytes: 0,
            limits,
        })
    }
    pub fn progress(&self) -> Progress {
        Progress {
            nodes: self.nodes,
            bytes: self.bytes,
            pending: self.pending.len(),
        }
    }
    pub fn is_complete(&self) -> bool {
        self.pending.is_empty()
    }
    // Success returns a new cursor; errors never consume the caller's cursor.
    // Persist nodes and next together if using this to promote/mark GC nodes.
    // The reader must retain an immutable view for the duration of the scan.
    pub fn page<S: NodeReader>(&self, store: &S, page: PageLimits) -> Result<Page, Error> {
        if page.nodes == 0 || page.bytes == 0 {
            return Err(Error::ResourceLimit);
        }
        let mut next = self.clone();
        let mut nodes = Vec::new();
        let mut bytes = 0usize;
        while let Some(&hash) = next.pending.last() {
            if nodes.len() == page.nodes || bytes == page.bytes {
                break;
            }
            if next.nodes == next.limits.nodes {
                return Err(Error::ResourceLimit);
            }
            let raw = store.node(&hash)?.ok_or(Error::MissingNode(hash))?;
            let page_bytes = bytes.checked_add(raw.len()).ok_or(Error::ResourceLimit)?;
            if page_bytes > page.bytes {
                if nodes.is_empty() {
                    return Err(Error::ResourceLimit);
                }
                break;
            }
            // Integrity scanning must not inherit the historical PoI exception
            // that permits a reader to skip content authentication.
            if blake2b_256(&raw) != hash {
                return Err(Error::HashMismatch(hash));
            }
            let node_count = next.nodes.checked_add(1).ok_or(Error::ResourceLimit)?;
            let byte_count = next
                .bytes
                .checked_add(u64::try_from(raw.len()).map_err(|_| Error::ResourceLimit)?)
                .filter(|n| *n <= next.limits.bytes)
                .ok_or(Error::ResourceLimit)?;
            let children = children(&raw, store.decode_limits())?;
            let pending = next
                .pending
                .len()
                .checked_sub(1)
                .and_then(|n| n.checked_add(children.len()))
                .filter(|n| *n <= next.limits.pending)
                .ok_or(Error::ResourceLimit)?;
            next.pending.pop();
            next.pending.extend(children.into_iter().rev());
            debug_assert_eq!(next.pending.len(), pending);
            next.nodes = node_count;
            next.bytes = byte_count;
            bytes = page_bytes;
            nodes.push(NodeBytes { hash, bytes: raw });
        }
        Ok(Page { nodes, next })
    }
    // Checkpoints are local work state, not proofs of prior visitation. A
    // consumer must bind a trusted checkpoint to its roots and storage view.
    pub fn encode(&self) -> Vec<u8> {
        let n = integer_bytes(self.nodes);
        let b = integer_bytes(self.bytes);
        rlp::encode(&Rlp::List(vec![
            Rlp::Bytes(&[1]),
            Rlp::Bytes(&n),
            Rlp::Bytes(&b),
            Rlp::List(
                self.pending
                    .iter()
                    .map(|h| Rlp::Bytes(h.as_ref()))
                    .collect(),
            ),
        ]))
    }
    pub fn decode(bytes: &[u8], limits: Limits) -> Result<Self, Error> {
        let budget = rlp::DecodeLimits {
            max_input_bytes: limits.pending.saturating_mul(33).saturating_add(64),
            max_items: limits.pending.saturating_add(5),
            max_depth: 2,
        };
        let Rlp::List(fields) = rlp::decode_with_limits(bytes, budget)? else {
            return Err(Error::InvalidNode);
        };
        let [Rlp::Bytes([1]), n, b, Rlp::List(hashes)] = fields.as_slice() else {
            return Err(Error::InvalidNode);
        };
        let nodes = integer(n)?;
        let byte_count = integer(b)?;
        if hashes.len() > limits.pending || nodes > limits.nodes || byte_count > limits.bytes {
            return Err(Error::ResourceLimit);
        }
        let pending = hashes
            .iter()
            .map(|h| match h {
                Rlp::Bytes(b) => Hash256::try_from(*b).map_err(|_| Error::InvalidReference),
                _ => Err(Error::InvalidReference),
            })
            .collect::<Result<_, _>>()?;
        let cursor = Self {
            pending,
            nodes,
            bytes: byte_count,
            limits,
        };
        if cursor.encode() != bytes {
            return Err(Error::InvalidNode);
        }
        Ok(cursor)
    }
}
fn integer_bytes(value: u64) -> Vec<u8> {
    value
        .to_be_bytes()
        .into_iter()
        .skip_while(|b| *b == 0)
        .collect()
}
fn integer(value: &Rlp<'_>) -> Result<u64, Error> {
    let Rlp::Bytes(bytes) = value else {
        return Err(Error::InvalidNode);
    };
    if bytes.len() > 8 || bytes.first() == Some(&0) {
        return Err(Error::InvalidNode);
    }
    Ok(bytes.iter().fold(0, |n, b| (n << 8) | u64::from(*b)))
}
fn children(raw: &[u8], limits: rlp::DecodeLimits) -> Result<Vec<Hash256>, Error> {
    enum Work {
        Decoded(Node),
        Hash(Hash256),
    }
    let mut hashes = Vec::new();
    let mut pending = vec![Work::Decoded(decode_node(raw, limits)?)];
    while let Some(work) = pending.pop() {
        match work {
            Work::Hash(hash) => hashes.push(hash),
            Work::Decoded(Node::Extension { child, .. }) => hashes.push(child),
            Work::Decoded(Node::Branch { children, .. }) => {
                for child in children.into_iter().rev() {
                    match child {
                        Reference::Hash(hash) => pending.push(Work::Hash(hash)),
                        Reference::Inline(raw) => {
                            pending.push(Work::Decoded(decode_node(&raw, limits)?))
                        }
                        Reference::Empty => {}
                    }
                }
            }
            Work::Decoded(Node::Empty | Node::Leaf { .. }) => {}
        }
    }
    Ok(hashes)
}
