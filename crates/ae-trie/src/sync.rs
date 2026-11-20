// aeu_mp_trees:unfold/has_node, aeternity fa2a6d7.
// Copyright (c) 2017, aeternity developers. ISC notice: LICENSE.
use super::*;

fn decode_sync_node(raw: &[u8], limits: rlp::DecodeLimits) -> Result<Node, Error> {
    // Synchronization accepts an advertised empty-valued leaf. The ordinary
    // authenticated state decoder continues to reject that stored form.
    match decode_node(raw, limits) {
        Err(Error::InvalidNode) => {
            if let Rlp::List(fields) = rlp::decode_with_limits(raw, limits)?
                && let [Rlp::Bytes(compact), value] = fields.as_slice()
                && let (path, true) = decode_compact_path(compact)?
            {
                return Ok(Node::Leaf {
                    path: path.0,
                    value: Value::from_rlp(value),
                });
            }
            Err(Error::InvalidNode)
        }
        result => result,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unfold {
    Node { path: Nibbles, node: Vec<u8> },
    Leaf(Nibbles),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    Yes,
    No,
    Maybe,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub nodes: usize,
    pub bytes: usize,
    pub path_nibbles: usize,
    pub outputs: usize,
    pub pending: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            nodes: 1_000_000,
            bytes: 64 * 1024 * 1024,
            path_nibbles: 4096,
            outputs: 100_000,
            pending: 4096,
        }
    }
}
// One budget may cover an entire request batch. Errors consume work but never
// mutate a trie; retries use a fresh budget. These are operational limits.
pub struct Budget {
    remaining: Limits,
}
impl Budget {
    pub fn new(limits: Limits) -> Self {
        Self { remaining: limits }
    }
    fn path(&self, path: &[u8]) -> Result<(), Error> {
        if path.len() > self.remaining.path_nibbles {
            Err(Error::ResourceLimit)
        } else {
            Ok(())
        }
    }
    fn append(&self, a: &[u8], b: &[u8]) -> Result<Vec<u8>, Error> {
        let len = a.len().checked_add(b.len()).ok_or(Error::ResourceLimit)?;
        if len > self.remaining.path_nibbles {
            return Err(Error::ResourceLimit);
        }
        let mut out = Vec::with_capacity(len);
        out.extend_from_slice(a);
        out.extend_from_slice(b);
        Ok(out)
    }
    fn bytes(&mut self, bytes: usize) -> Result<(), Error> {
        self.remaining.bytes = self
            .remaining
            .bytes
            .checked_sub(bytes)
            .ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    fn output(&mut self, bytes: usize) -> Result<(), Error> {
        self.remaining.outputs = self
            .remaining
            .outputs
            .checked_sub(1)
            .ok_or(Error::ResourceLimit)?;
        self.bytes(bytes)
    }
    fn read<S: NodeReader>(&mut self, reference: &Reference, store: &S) -> Result<Node, Error> {
        self.remaining.nodes = self
            .remaining
            .nodes
            .checked_sub(1)
            .ok_or(Error::ResourceLimit)?;
        let raw = match reference {
            Reference::Empty => return Ok(Node::Empty),
            Reference::Inline(bytes) => {
                self.bytes(bytes.len())?;
                return decode_sync_node(bytes, store.decode_limits());
            }
            Reference::Hash(hash) => {
                let bytes = store.node(hash)?.ok_or(Error::MissingNode(*hash))?;
                self.bytes(bytes.len())?;
                if store.verify_node_hashes() && blake2b_256(&bytes) != *hash {
                    return Err(Error::HashMismatch(*hash));
                }
                bytes
            }
        };
        decode_sync_node(&raw, store.decode_limits())
    }
}
impl<S: NodeReader> Trie<S> {
    pub fn get_for_sync(&self, key: &Nibbles, budget: &mut Budget) -> Result<Option<Value>, Error> {
        self.sync_get(key.as_slice(), budget)
    }
    pub fn unfold(
        &self,
        path: &Nibbles,
        node: &[u8],
        budget: &mut Budget,
    ) -> Result<Vec<Unfold>, Error> {
        budget.path(path.as_slice())?;
        let root = self.root_reference();
        if path.0.is_empty() && node == root.as_bytes() {
            return Ok(vec![]);
        }
        let mut reference = if path.0.is_empty() {
            root
        } else {
            Reference::from_bytes(node)?
        };
        let mut prefix = path.0.clone();
        loop {
            match budget.read(&reference, &self.store)? {
                Node::Empty => return Ok(vec![]),
                Node::Leaf { path, .. } => {
                    let path = budget.append(&prefix, &path)?;
                    budget.output(path.len())?;
                    return Ok(vec![Unfold::Leaf(Nibbles(path))]);
                }
                Node::Extension { path, child } => {
                    prefix = budget.append(&prefix, &path)?;
                    reference = Reference::Hash(child);
                }
                Node::Branch { children, .. } => {
                    let mut out = vec![];
                    for (n, child) in children.iter().enumerate().filter(|(_, c)| !c.is_empty()) {
                        let path = budget.append(&prefix, &[n as u8])?;
                        budget.output(path.len() + child.as_bytes().len())?;
                        out.push(Unfold::Node {
                            path: Nibbles(path),
                            node: child.as_bytes().to_vec(),
                        });
                    }
                    return Ok(out);
                }
            }
        }
    }
    fn sync_get(&self, key: &[u8], budget: &mut Budget) -> Result<Option<Value>, Error> {
        budget.path(key)?;
        let mut reference = self.root_reference();
        let mut key = key;
        loop {
            match budget.read(&reference, &self.store)? {
                Node::Empty => return Ok(None),
                Node::Leaf { path, value } => return Ok((path == key).then_some(value)),
                Node::Extension { path, child } => {
                    if !key.starts_with(&path) {
                        return Ok(None);
                    }
                    key = &key[path.len()..];
                    reference = Reference::Hash(child);
                }
                Node::Branch { children, value } => {
                    let Some((&first, rest)) = key.split_first() else {
                        return Ok((!value.is_empty()).then_some(value));
                    };
                    reference = children[usize::from(first)].clone();
                    key = rest;
                }
            }
        }
    }
    pub fn has_node(
        &self,
        path: &Nibbles,
        node: &[u8],
        budget: &mut Budget,
    ) -> Result<Presence, Error> {
        budget.path(path.as_slice())?;
        let decoded = Reference::from_bytes(node).and_then(|r| budget.read(&r, &self.store));
        match decoded {
            Ok(Node::Leaf { path: tail, value }) => {
                let key = budget.append(&path.0, &tail)?;
                return Ok(
                    if self.sync_get(&key, budget)?.unwrap_or_else(Value::empty) == value {
                        Presence::Yes
                    } else {
                        Presence::No
                    },
                );
            }
            Err(e @ (Error::Backend(_) | Error::ResourceLimit | Error::HashMismatch(_))) => {
                return Err(e);
            }
            Err(Error::Decode(rlp::DecodeError::ResourceLimit)) => {
                return Err(Error::ResourceLimit);
            }
            _ => {}
        }
        // Unknown/invalid advertised references fall back to the local path,
        // as the original catch does. Local backend/budget failures surface.
        let mut path = path.as_slice();
        let mut reference = self.root_reference();
        loop {
            match budget.read(&reference, &self.store)? {
                Node::Empty => return Ok(Presence::No),
                Node::Branch { children, .. } => {
                    let Some((&n, rest)) = path.split_first() else {
                        return Ok(Presence::Maybe);
                    };
                    let child = &children[usize::from(n)];
                    if rest.is_empty() {
                        return Ok(if child.is_empty() {
                            Presence::No
                        } else if child.as_bytes() == node {
                            Presence::Yes
                        } else {
                            Presence::Maybe
                        });
                    }
                    reference = child.clone();
                    path = rest;
                }
                Node::Extension { path: tail, child } if path.starts_with(&tail) => {
                    path = &path[tail.len()..];
                    reference = Reference::Hash(child);
                }
                Node::Extension { path: tail, .. } | Node::Leaf { path: tail, .. } => {
                    return Ok(if tail.starts_with(path) {
                        Presence::Maybe
                    } else {
                        Presence::No
                    });
                }
            }
        }
    }
    // Same ordered prefix walk used by pool sync. Stop at the first matching
    // key of a different width; exact-prefix values are excluded by iterator_from.
    pub fn sync_keys(
        &self,
        prefix: &Nibbles,
        width: usize,
        budget: &mut Budget,
    ) -> Result<Vec<Nibbles>, Error> {
        budget.path(&prefix.0)?;
        if budget.remaining.pending == 0 {
            return Err(Error::ResourceLimit);
        }
        let mut pending = vec![(vec![], self.root_reference())];
        let mut out = vec![];
        while let Some((path, reference)) = pending.pop() {
            if !path.starts_with(&prefix.0) && !prefix.0.starts_with(&path) {
                continue;
            }
            let found = match budget.read(&reference, &self.store)? {
                Node::Empty => None,
                Node::Leaf { path: tail, .. } => Some(budget.append(&path, &tail)?),
                Node::Extension { path: tail, child } => {
                    if pending.len() >= budget.remaining.pending {
                        return Err(Error::ResourceLimit);
                    }
                    pending.push((budget.append(&path, &tail)?, Reference::Hash(child)));
                    None
                }
                Node::Branch { children, value } => {
                    for (n, child) in children
                        .into_iter()
                        .enumerate()
                        .rev()
                        .filter(|(_, c)| !c.is_empty())
                    {
                        if pending.len() >= budget.remaining.pending {
                            return Err(Error::ResourceLimit);
                        }
                        pending.push((budget.append(&path, &[n as u8])?, child));
                    }
                    (!value.is_empty()).then_some(path)
                }
            };
            if let Some(key) = found
                && key.starts_with(&prefix.0)
                && key > prefix.0
            {
                if key.len() != width {
                    break;
                }
                budget.output(key.len())?;
                out.push(Nibbles(key));
            }
        }
        Ok(out)
    }
}
