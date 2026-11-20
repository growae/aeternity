// aec_tx_pool_sync, aeternity fa2a6d7. Copyright (c) 2018,
// Aeternity Anstalt. ISC notice: LICENSE.
use ae_trie::{
    MemoryStore, Nibbles, NodeReader, NodeWriter, Trie, Value,
    sync::{Budget, Presence},
};
use ae_types::Hash256;
use std::collections::BTreeSet;
mod codec;
pub mod session;
pub use codec::{Unfold, decode_batch, encode_batch};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidMessage,
    ResourceLimit,
    Decode(ae_codec::rlp::DecodeError),
    Trie(ae_trie::Error),
}
impl From<ae_trie::Error> for Error {
    fn from(e: ae_trie::Error) -> Self {
        if e == ae_trie::Error::ResourceLimit {
            Self::ResourceLimit
        } else {
            Self::Trie(e)
        }
    }
}
impl From<ae_codec::rlp::DecodeError> for Error {
    fn from(e: ae_codec::rlp::DecodeError) -> Self {
        if e == ae_codec::rlp::DecodeError::ResourceLimit {
            Self::ResourceLimit
        } else {
            Self::Decode(e)
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub entries: usize,
    pub tree_nodes: usize,
    pub tree_bytes: usize,
    pub items: usize,
    pub wire_bytes: usize,
    pub work: ae_trie::sync::Limits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            entries: 100_000,
            tree_nodes: 2_000_000,
            tree_bytes: 256 * 1024 * 1024,
            items: 100_000,
            wire_bytes: 16 * 1024 * 1024,
            work: Default::default(),
        }
    }
}
#[derive(Debug, Clone)]
struct Store {
    nodes: MemoryStore,
    bytes: usize,
    limits: Limits,
}
impl NodeReader for Store {
    fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.nodes.node(h)
    }
}
impl NodeWriter for Store {
    fn put_node(&mut self, h: Hash256, bytes: Vec<u8>) -> Result<(), ae_trie::Error> {
        if self.nodes.nodes().contains_key(&h) {
            return self.nodes.put_node(h, bytes);
        }
        let total = self
            .bytes
            .checked_add(bytes.len())
            .ok_or(ae_trie::Error::ResourceLimit)?;
        if self.nodes.nodes().len() >= self.limits.tree_nodes || total > self.limits.tree_bytes {
            return Err(ae_trie::Error::ResourceLimit);
        }
        self.nodes.put_node(h, bytes)?;
        self.bytes = total;
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct Snapshot {
    tree: Trie<Store>,
    entries: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Analysis {
    pub unfold: Vec<Unfold>,
    pub get: Vec<Nibbles>,
}
impl Snapshot {
    pub fn build(hashes: impl IntoIterator<Item = Hash256>, limits: Limits) -> Result<Self, Error> {
        let mut tree = Trie::new(Store {
            nodes: Default::default(),
            bytes: 0,
            limits,
        });
        let mut seen = BTreeSet::new();
        for (work, h) in hashes.into_iter().enumerate() {
            if work >= limits.entries {
                return Err(Error::ResourceLimit);
            }
            if seen.insert(h) {
                tree.put(&Nibbles::from_bytes(&h.0), Value::List(vec![]))?;
            }
        }
        Ok(Self {
            tree,
            entries: seen.len(),
        })
    }
    pub fn len(&self) -> usize {
        self.entries
    }
    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }
    pub fn root(&self) -> Option<Hash256> {
        self.tree.root_hash()
    }
    pub fn initial(&self) -> Unfold {
        Unfold::Node {
            path: Nibbles::new(vec![]).expect("empty path"),
            node: self.root().map_or(vec![], |h| h.0.to_vec()),
        }
    }
    pub fn respond(&self, requests: &[Unfold], limits: Limits) -> Result<Vec<Unfold>, Error> {
        encode_batch(requests, limits)?;
        let mut budget = Budget::new(limits.work);
        let mut out = vec![];
        for request in requests {
            let part = match request {
                Unfold::Node { path, node } => self
                    .tree
                    .unfold(path, node, &mut budget)?
                    .into_iter()
                    .map(|u| match u {
                        ae_trie::sync::Unfold::Node { path, node } => Unfold::Node { path, node },
                        ae_trie::sync::Unfold::Leaf(p) => Unfold::Leaf(p),
                    })
                    .collect(),
                Unfold::Leaf(p) => vec![Unfold::Leaf(p.clone())],
                Unfold::Key(k) => vec![Unfold::Key(k.clone())],
                Unfold::Subtree(p) if p.as_slice().len() == 64 => {
                    vec![Unfold::Key(codec::bytes(p)?)]
                }
                Unfold::Subtree(p) => self
                    .tree
                    .sync_keys(p, 64, &mut budget)?
                    .iter()
                    .map(|p| codec::bytes(p).map(Unfold::Key))
                    .collect::<Result<Vec<_>, _>>()?,
            };
            if out
                .len()
                .checked_add(part.len())
                .is_none_or(|n| n > limits.items)
            {
                return Err(Error::ResourceLimit);
            }
            out.extend(part);
        }
        encode_batch(&out, limits)?;
        Ok(out)
    }
    pub fn respond_wire(
        &self,
        requests: &[Vec<u8>],
        limits: Limits,
    ) -> Result<Vec<Vec<u8>>, Error> {
        encode_batch(
            &self.respond(&decode_batch(requests, limits)?, limits)?,
            limits,
        )
    }
    pub fn analyze(&self, response: &[Unfold], limits: Limits) -> Result<Analysis, Error> {
        encode_batch(response, limits)?;
        let mut budget = Budget::new(limits.work);
        let mut unfold = vec![];
        let mut get = vec![];
        for u in response {
            match u {
                Unfold::Key(k) => get.push(Nibbles::from_bytes(k)),
                Unfold::Leaf(p) => match self.tree.get_for_sync(p, &mut budget)? {
                    None => get.push(p.clone()),
                    Some(Value::List(v)) if v.is_empty() => {}
                    _ => return Err(Error::InvalidMessage),
                },
                Unfold::Node { path, node } => match self.tree.has_node(path, node, &mut budget)? {
                    Presence::Yes => {}
                    Presence::No => unfold.push(Unfold::Subtree(path.clone())),
                    Presence::Maybe => unfold.push(u.clone()),
                },
                Unfold::Subtree(_) => return Err(Error::InvalidMessage),
            }
        }
        Ok(Analysis { unfold, get })
    }
}
