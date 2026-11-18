// aect_contracts_store.erl, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::Error;
use ae_trie::{MemoryStore, Nibbles, NodeReader, Trie, Value};
use std::collections::{BTreeMap, BTreeSet};
pub type Contents = BTreeMap<Vec<u8>, Vec<u8>>;
#[derive(Debug, Clone)]
pub struct Store<S = MemoryStore> {
    tree: Trie<S>,
    writes: Contents,
    reads: Contents,
    subtrees: BTreeSet<Vec<u8>>,
}
impl Default for Store {
    fn default() -> Self {
        Self::new(Trie::new(MemoryStore::default()))
    }
}
impl<S: NodeReader> Store<S> {
    pub fn new(tree: Trie<S>) -> Self {
        Self {
            tree,
            writes: Contents::new(),
            reads: Contents::new(),
            subtrees: BTreeSet::new(),
        }
    }
    pub fn tree(&self) -> &Trie<S> {
        &self.tree
    }
    pub fn write_cache(&self) -> &Contents {
        &self.writes
    }
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.writes.insert(key, value);
    }
    pub fn put_map(&mut self, values: Contents) {
        self.writes.extend(values);
    }
    pub fn remove(&mut self, key: Vec<u8>) {
        self.put(key, vec![]);
    }
    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, Error> {
        if let Some(value) = self.writes.get(key).or_else(|| self.reads.get(key)) {
            return Ok(value.clone());
        }
        match self.tree.get(&Nibbles::from_bytes(key))? {
            Some(Value::Bytes(bytes)) => Ok(bytes),
            None => Ok(vec![]),
            _ => Err(Error::InvalidValue),
        }
    }
    pub fn get_cached(&mut self, key: &[u8]) -> Result<Vec<u8>, Error> {
        let value = self.get(key)?;
        if !self.writes.contains_key(key) {
            self.reads
                .entry(key.to_vec())
                .or_insert_with(|| value.clone());
        }
        Ok(value)
    }
    fn read_subtree(&self, prefix: &[u8]) -> Result<Contents, Error> {
        if self.subtrees.contains(prefix) {
            return Ok(from_cache(prefix, &self.reads));
        }
        let tree = match self.tree.read_only_subtree(&Nibbles::from_bytes(prefix)) {
            Ok(t) => t,
            Err(ae_trie::Error::NoSuchSubtree) => return Ok(Contents::new()),
            Err(e) => return Err(e.into()),
        };
        let mut values = Contents::new();
        for row in tree.iter() {
            let (k, v) = row?;
            let k = byte_key(&k)?;
            if k.is_empty() {
                continue;
            }
            values.insert(k, value_bytes(v)?);
        }
        Ok(values)
    }
    pub fn subtree(&self, prefix: &[u8]) -> Result<Contents, Error> {
        let mut values = self.read_subtree(prefix)?;
        values.extend(from_cache(prefix, &self.writes));
        Ok(values)
    }
    pub fn subtree_cached(&mut self, prefix: &[u8]) -> Result<Contents, Error> {
        let mut values = self.read_subtree(prefix)?;
        if !self.subtrees.contains(prefix) {
            for (key, value) in &values {
                self.reads.insert([prefix, key].concat(), value.clone());
            }
            self.subtrees.insert(prefix.to_vec());
        }
        values.extend(from_cache(prefix, &self.writes));
        Ok(values)
    }
    pub fn contents(&self) -> Result<Contents, Error> {
        self.subtree(&[])
    }
    pub fn size(&self) -> Result<usize, Error> {
        self.contents()?.values().try_fold(0usize, |n, v| {
            n.checked_add(v.len()).ok_or(Error::ResourceLimit)
        })
    }
    // This service budget counts suffix-key bytes plus value bytes. Failure
    // never caches a partial subtree or changes consensus data.
    pub fn subtree_within_bytes(
        &self,
        prefix: &[u8],
        maximum: usize,
    ) -> Result<(Contents, usize), Error> {
        let mut values = from_cache(prefix, &self.writes);
        let mut size = map_bytes(&values)?;
        if size > maximum {
            return Err(Error::ResourceLimit);
        }
        if self.subtrees.contains(prefix) {
            let mut base = from_cache(prefix, &self.reads);
            base.extend(values);
            let size = map_bytes(&base)?;
            return if size > maximum {
                Err(Error::ResourceLimit)
            } else {
                Ok((base, size))
            };
        }
        let tree = match self.tree.read_only_subtree(&Nibbles::from_bytes(prefix)) {
            Ok(t) => t,
            Err(ae_trie::Error::NoSuchSubtree) => return Ok((values, size)),
            Err(e) => return Err(e.into()),
        };
        for row in tree.iter() {
            let (k, v) = row?;
            let key = byte_key(&k)?;
            if key.is_empty() || values.contains_key(&key) {
                continue;
            }
            let value = value_bytes(v)?;
            size = size
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.len()))
                .ok_or(Error::ResourceLimit)?;
            if size > maximum {
                return Err(Error::ResourceLimit);
            }
            values.insert(key, value);
        }
        Ok((values, size))
    }
}
fn from_cache(prefix: &[u8], cache: &Contents) -> Contents {
    cache
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix(prefix)
                .filter(|k| !k.is_empty())
                .map(|k| (k.to_vec(), v.clone()))
        })
        .collect()
}
fn map_bytes(values: &Contents) -> Result<usize, Error> {
    values.iter().try_fold(0usize, |n, (k, v)| {
        n.checked_add(k.len())
            .and_then(|n| n.checked_add(v.len()))
            .ok_or(Error::ResourceLimit)
    })
}
pub(super) fn byte_key(key: &Nibbles) -> Result<Vec<u8>, Error> {
    if !key.as_slice().len().is_multiple_of(2) {
        return Err(Error::InvalidValue);
    }
    Ok(key
        .as_slice()
        .chunks_exact(2)
        .map(|p| p[0] << 4 | p[1])
        .collect())
}
pub(super) fn value_bytes(value: Value) -> Result<Vec<u8>, Error> {
    match value {
        Value::Bytes(b) => Ok(b),
        _ => Err(Error::InvalidValue),
    }
}
