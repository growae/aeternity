use super::*;

impl<S: NodeReader> Trie<S> {
    pub fn iter(&self) -> TrieIterator<'_, S> {
        self.iter_with_options(IteratorOptions::default())
    }
    pub fn iter_with_options(&self, options: IteratorOptions) -> TrieIterator<'_, S> {
        TrieIterator {
            store: &self.store,
            stack: vec![(Vec::new(), self.root_reference())],
            options,
            failed: false,
        }
    }

    pub fn reachable_nodes(&self) -> Result<MemoryStore, Error> {
        let mut output = MemoryStore::default();
        let mut pending = vec![self.root_reference()];
        while let Some(reference) = pending.pop() {
            if let Reference::Hash(hash) = reference {
                if output.nodes.contains_key(&hash) {
                    continue;
                }
                let bytes = self.store.node(&hash)?.ok_or(Error::MissingNode(hash))?;
                output.put_node(hash, bytes)?;
            }
            match read_node(&reference, &self.store)? {
                Node::Extension { child, .. } => pending.push(Reference::Hash(child)),
                Node::Branch { children, .. } => {
                    pending.extend(children.iter().filter(|r| !r.is_empty()).cloned())
                }
                _ => {}
            }
        }
        Ok(output)
    }

    pub fn read_only_subtree(&self, prefix: &Nibbles) -> Result<Trie<ReadView<'_, S>>, Error> {
        let mut reference = self.root_reference();
        let mut remaining = prefix.as_slice();
        let mut overlay = MemoryStore::default();
        let root = loop {
            match read_node(&reference, &self.store)? {
                Node::Empty if remaining.is_empty() => break None,
                Node::Empty => return Err(Error::NoSuchSubtree),
                Node::Leaf { path, .. } => {
                    if path == remaining {
                        break None;
                    }
                    return Err(Error::NoSuchSubtree);
                }
                Node::Extension { path, child } => {
                    if !remaining.starts_with(&path) {
                        return Err(Error::NoSuchSubtree);
                    }
                    remaining = &remaining[path.len()..];
                    reference = Reference::Hash(child);
                }
                Node::Branch { children, .. } => {
                    if let Some((first, rest)) = remaining.split_first() {
                        reference = children[usize::from(*first)].clone();
                        remaining = rest;
                    } else {
                        let node = store_node(
                            Node::Branch {
                                children,
                                value: Value::empty(),
                            },
                            &mut overlay,
                        )?;
                        break Some(force_hash(node, &mut overlay)?);
                    }
                }
            }
        };
        Ok(Trie {
            root,
            store: ReadView {
                overlay,
                backing: &self.store,
            },
        })
    }
}

pub struct ReadView<'a, S> {
    overlay: MemoryStore,
    backing: &'a S,
}
// Retain the synthetic subtree root and a backing-store handle without
// enumerating/copying the reachable subtree. Cloning the handle must preserve
// access to immutable content-addressed nodes.
#[derive(Debug, Clone)]
pub struct OwnedReadView<S> {
    overlay: MemoryStore,
    backing: S,
}
impl<S> OwnedReadView<S> {
    pub fn new(backing: S) -> Self {
        Self {
            overlay: MemoryStore::default(),
            backing,
        }
    }
}
impl<S: NodeReader> NodeReader for OwnedReadView<S> {
    fn verify_node_hashes(&self) -> bool {
        self.backing.verify_node_hashes()
    }
    fn decode_limits(&self) -> rlp::DecodeLimits {
        self.backing.decode_limits()
    }

    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, Error> {
        match self.overlay.node(hash)? {
            Some(bytes) => Ok(Some(bytes)),
            None => self.backing.node(hash),
        }
    }
}
impl<S: NodeReader + Clone> Trie<S> {
    pub fn owned_read_only_subtree(
        &self,
        prefix: &Nibbles,
    ) -> Result<Trie<OwnedReadView<S>>, Error> {
        let subtree = self.read_only_subtree(prefix)?;
        Ok(Trie {
            root: subtree.root,
            store: OwnedReadView {
                overlay: subtree.store.overlay,
                backing: self.store.clone(),
            },
        })
    }
}
impl<S: NodeReader> NodeReader for ReadView<'_, S> {
    fn verify_node_hashes(&self) -> bool {
        self.backing.verify_node_hashes()
    }
    fn decode_limits(&self) -> rlp::DecodeLimits {
        self.backing.decode_limits()
    }
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, Error> {
        match self.overlay.node(hash)? {
            Some(bytes) => Ok(Some(bytes)),
            None => self.backing.node(hash),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct IteratorOptions {
    pub after: Option<Nibbles>,
    pub prefix: Option<Nibbles>,
    pub max_nibbles: Option<usize>,
}
pub struct TrieIterator<'a, S> {
    store: &'a S,
    stack: Vec<(Vec<u8>, Reference)>,
    options: IteratorOptions,
    failed: bool,
}
impl<S: NodeReader> TrieIterator<'_, S> {
    fn accepted(&self, key: &[u8]) -> bool {
        self.options
            .after
            .as_ref()
            .is_none_or(|k| key > k.as_slice())
            && self
                .options
                .prefix
                .as_ref()
                .is_none_or(|p| key.starts_with(p.as_slice()))
            && self.options.max_nibbles.is_none_or(|n| key.len() <= n)
    }
}
impl<S: NodeReader> Iterator for TrieIterator<'_, S> {
    type Item = Result<(Nibbles, Value), Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        while let Some((mut path, reference)) = self.stack.pop() {
            // Seek past whole subtrees below an exclusive cursor. A prefix of
            // the cursor still needs traversal to find its later descendants.
            if self.options.after.as_ref().is_some_and(|after| {
                path.as_slice() < after.as_slice() && !after.as_slice().starts_with(&path)
            }) {
                continue;
            }
            if self.options.max_nibbles.is_some_and(|n| path.len() > n) {
                continue;
            }
            if self
                .options
                .prefix
                .as_ref()
                .is_some_and(|p| !p.0.starts_with(&path) && !path.starts_with(&p.0))
            {
                continue;
            }
            let node = match read_node(&reference, self.store) {
                Ok(node) => node,
                Err(e) => {
                    self.failed = true;
                    return Some(Err(e));
                }
            };
            match node {
                Node::Empty => {}
                Node::Leaf { path: tail, value } => {
                    path.extend(tail);
                    if self.accepted(&path) {
                        return Some(Ok((Nibbles(path), value)));
                    }
                }
                Node::Extension { path: tail, child } => {
                    path.extend(tail);
                    self.stack.push((path, Reference::Hash(child)));
                }
                Node::Branch { children, value } => {
                    for (n, child) in children.into_iter().enumerate().rev() {
                        if !child.is_empty() {
                            let mut next = path.clone();
                            next.push(n as u8);
                            self.stack.push((next, child));
                        }
                    }
                    if !value.is_empty() && self.accepted(&path) {
                        return Some(Ok((Nibbles(path), value)));
                    }
                }
            }
        }
        None
    }
}

pub fn transaction_root<'a>(
    signed_transactions: impl IntoIterator<Item = &'a [u8]>,
) -> Result<Hash256, Error> {
    let mut tree = Trie::default();
    for (position, transaction) in signed_transactions.into_iter().enumerate() {
        let position = u64::try_from(position).map_err(|_| Error::InvalidNode)?;
        let key = ae_types::Unsigned::from(position);
        tree.put(
            &Nibbles::from_bytes(key.as_bytes()),
            Value::Bytes(transaction.to_vec()),
        )?;
    }
    Ok(tree.root_hash().unwrap_or(Hash256::ZERO))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> Trie {
        let mut tree = Trie::default();
        for key in [vec![1], vec![1, 2], vec![1, 2, 3], vec![1, 3], vec![2]] {
            tree.put(&Nibbles::new(key.clone()).unwrap(), Value::Bytes(key))
                .unwrap();
        }
        tree
    }
    #[test]
    fn ordered_ranges_and_read_only_subtree_exclude_prefix_value() {
        let tree = fixture();
        let keys: Vec<_> = tree.iter().map(|x| x.unwrap().0.0).collect();
        assert_eq!(
            keys,
            vec![vec![1], vec![1, 2], vec![1, 2, 3], vec![1, 3], vec![2]]
        );
        let options = IteratorOptions {
            after: Some(Nibbles(vec![1])),
            prefix: Some(Nibbles(vec![1])),
            max_nibbles: Some(2),
        };
        let keys: Vec<_> = tree
            .iter_with_options(options)
            .map(|x| x.unwrap().0.0)
            .collect();
        assert_eq!(keys, vec![vec![1, 2], vec![1, 3]]);
        let subtree = tree.read_only_subtree(&Nibbles(vec![1])).unwrap();
        let keys: Vec<_> = subtree.iter().map(|x| x.unwrap().0.0).collect();
        assert_eq!(keys, vec![vec![2], vec![2, 3], vec![3]]);
        assert_eq!(
            tree.read_only_subtree(&Nibbles(vec![0]))
                .unwrap()
                .root_hash(),
            None
        );
        assert!(tree.read_only_subtree(&Nibbles(vec![0, 1])).is_err());
    }
    #[test]
    fn exclusive_cursor_seeks_without_reading_earlier_subtrees() {
        use std::cell::Cell;
        struct Counted {
            store: MemoryStore,
            reads: Cell<usize>,
        }
        impl NodeReader for Counted {
            fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, Error> {
                self.reads.set(self.reads.get() + 1);
                self.store.node(hash)
            }
        }
        let mut tree = Trie::default();
        for n in 0u16..4096 {
            tree.put(
                &Nibbles::from_bytes(&n.to_be_bytes()),
                Value::Bytes(vec![42; 64]),
            )
            .unwrap();
        }
        let counted = Counted {
            store: tree.store().clone(),
            reads: Cell::new(0),
        };
        let tree = Trie::from_root(tree.root_hash(), counted).unwrap();
        tree.store().reads.set(0);
        let rows = tree
            .iter_with_options(IteratorOptions {
                after: Some(Nibbles::from_bytes(&4090u16.to_be_bytes())),
                ..Default::default()
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
            (4091u16..4096)
                .map(|n| Nibbles::from_bytes(&n.to_be_bytes()))
                .collect::<Vec<_>>()
        );
        assert!(
            tree.store().reads.get() < 20,
            "{}",
            tree.store().reads.get()
        );
        let small = fixture();
        let all = small.iter().collect::<Result<Vec<_>, _>>().unwrap();
        for cursor in [
            vec![],
            vec![0],
            vec![1],
            vec![1, 1],
            vec![1, 2],
            vec![1, 2, 2],
            vec![1, 2, 3],
            vec![1, 2, 3, 0],
            vec![1, 4],
            vec![2],
            vec![3],
        ] {
            for prefix in [None, Some(Nibbles(vec![1])), Some(Nibbles(vec![1, 2]))] {
                for max in [None, Some(1), Some(2), Some(3)] {
                    let expected = all
                        .iter()
                        .filter(|(k, _)| {
                            k.0 > cursor
                                && prefix.as_ref().is_none_or(|p| k.0.starts_with(&p.0))
                                && max.is_none_or(|m| k.0.len() <= m)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    let got = small
                        .iter_with_options(IteratorOptions {
                            after: Some(Nibbles(cursor.clone())),
                            prefix: prefix.clone(),
                            max_nibbles: max,
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    assert_eq!(got, expected, "{cursor:?} {prefix:?} {max:?}");
                }
            }
        }
    }
    #[test]
    fn reachability_drops_obsolete_nodes_without_losing_current_values() {
        let mut tree = fixture();
        tree.put(&Nibbles(vec![1]), Value::Bytes(vec![42; 100]))
            .unwrap();
        let compact = tree.reachable_nodes().unwrap();
        assert!(compact.nodes().len() < tree.store().nodes().len());
        let reopened = Trie::from_root(tree.root_hash(), compact).unwrap();
        assert_eq!(
            tree.iter().collect::<Vec<_>>(),
            reopened.iter().collect::<Vec<_>>()
        );
    }
    #[test]
    fn empty_transaction_root_is_zero_and_order_is_committed() {
        assert_eq!(transaction_root([]).unwrap(), Hash256::ZERO);
        assert_ne!(
            transaction_root([&b"a"[..], &b"b"[..]]).unwrap(),
            transaction_root([&b"b"[..], &b"a"[..]]).unwrap()
        );
    }
}
