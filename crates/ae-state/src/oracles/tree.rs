// Aeternity fa2a6d7, aeo_state_tree. Oracle and query objects share one trie.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{
    Error, Oracle, Query,
    cache::{self, Key},
};
use ae_trie::{IteratorOptions, MemoryStore, Nibbles, NodeReader, NodeWriter, Trie, Value};
use ae_types::{AccountPublicKey, Hash256, IdTag, Unsigned};

#[derive(Debug, Clone)]
pub struct Oracles<S = MemoryStore, C = MemoryStore> {
    tree: Trie<S>,
    cache: Trie<C>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryFilter {
    Open,
    Closed,
    All,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refund {
    pub account: AccountPublicKey,
    pub amount: Unsigned,
    // Oracle deletion refunds open queries through an eventless helper.
    pub emit_event: bool,
}
impl Default for Oracles {
    fn default() -> Self {
        Self::new(MemoryStore::default(), MemoryStore::default())
    }
}
impl<S: NodeReader, C: NodeReader> Oracles<S, C> {
    pub fn new(store: S, cache_store: C) -> Self {
        Self {
            tree: Trie::new(store),
            cache: Trie::new(cache_store),
        }
    }
    pub fn from_roots(
        root: Option<Hash256>,
        cache_root: Option<Hash256>,
        store: S,
        cache_store: C,
    ) -> Result<Self, Error> {
        Ok(Self {
            tree: Trie::from_root(root, store)?,
            cache: Trie::from_root(cache_root, cache_store)?,
        })
    }
    pub fn root_hash(&self) -> Option<Hash256> {
        self.tree.root_hash()
    }
    pub fn cache_root_hash(&self) -> Option<Hash256> {
        self.cache.root_hash()
    }
    pub fn tree(&self) -> &Trie<S> {
        &self.tree
    }
    pub fn cache(&self) -> &Trie<C> {
        &self.cache
    }
    pub fn oracle(&self, public_key: [u8; 32]) -> Result<Option<Oracle>, Error> {
        bytes(&self.tree, &public_key)?
            .map(|b| Oracle::deserialize(public_key, &b))
            .transpose()
    }
    pub fn query(&self, oracle: [u8; 32], id: [u8; 32]) -> Result<Option<Query>, Error> {
        bytes(&self.tree, &query_key(oracle, id))?
            .map(|b| Query::deserialize(id, &b))
            .transpose()
    }
    // IDs are returned in reverse lexicographic order in the original API.
    pub fn query_ids(&self, oracle: [u8; 32]) -> Result<Vec<[u8; 32]>, Error> {
        let prefix = Nibbles::from_bytes(&oracle);
        let mut ids = self
            .tree
            .iter_with_options(IteratorOptions {
                after: Some(prefix.clone()),
                prefix: Some(prefix),
                max_nibbles: None,
            })
            .map(|r| {
                let (key, _) = r?;
                let key = byte_key(&key)?;
                key.get(32..)
                    .ok_or(Error::InvalidValue)?
                    .try_into()
                    .map_err(|_| Error::InvalidValue)
            })
            .collect::<Result<Vec<_>, _>>()?;
        ids.reverse();
        Ok(ids)
    }
    pub fn oracles(&self, after: Option<[u8; 32]>, limit: usize) -> Result<Vec<Oracle>, Error> {
        self.tree
            .iter_with_options(IteratorOptions {
                after: after.map(|a| Nibbles::from_bytes(&a)),
                max_nibbles: Some(64),
                prefix: None,
            })
            .take(limit)
            .map(|r| {
                let (key, value) = r?;
                Oracle::deserialize(
                    byte_key(&key)?
                        .try_into()
                        .map_err(|_| Error::InvalidValue)?,
                    &value_bytes(value)?,
                )
            })
            .collect()
    }
    pub fn queries(
        &self,
        oracle: [u8; 32],
        after: Option<[u8; 32]>,
        filter: QueryFilter,
        limit: usize,
    ) -> Result<Vec<Query>, Error> {
        let after = after.map_or_else(|| oracle.to_vec(), |id| query_key(oracle, id));
        let mut out = Vec::new();
        if limit == 0 {
            return Ok(out);
        }
        // Preserve the legacy range: stop at the next 32-byte oracle key.
        // It does not additionally constrain the prefix when orphan rows exist.
        for r in self.tree.iter_with_options(IteratorOptions {
            after: Some(Nibbles::from_bytes(&after)),
            ..Default::default()
        }) {
            let (key, value) = r?;
            let key = byte_key(&key)?;
            if key.len() <= 32 {
                break;
            }
            let q = Query::deserialize(
                key[32..].try_into().map_err(|_| Error::InvalidValue)?,
                &value_bytes(value)?,
            )?;
            if matches!(filter, QueryFilter::All)
                || (q.is_open() == matches!(filter, QueryFilter::Open))
            {
                out.push(q);
                if out.len() == limit {
                    break;
                }
            }
        }
        Ok(out)
    }
}
impl<S: NodeWriter, C: NodeWriter> Oracles<S, C> {
    pub fn transaction<T, E>(
        &mut self,
        action: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<T, E> {
        let (main, cache) = (self.tree.savepoint(), self.cache.savepoint());
        let result = action(self);
        if result.is_err() {
            self.tree.rollback(main);
            self.cache.rollback(cache);
        }
        result
    }
    fn enter(
        &mut self,
        key: &[u8],
        encoded: Vec<u8>,
        ttl: &Unsigned,
        cache_key: Key,
        insert: bool,
    ) -> Result<(), Error> {
        self.transaction(|s| {
            let key = Nibbles::from_bytes(key);
            if insert && s.tree.get(&key)?.is_some() {
                return Err(Error::AlreadyPresent);
            }
            s.tree.put(&key, Value::Bytes(encoded))?;
            s.cache.put(
                &Nibbles::from_bytes(&cache::encode(ttl, cache_key)),
                Value::Bytes(vec![0]),
            )?;
            Ok(())
        })
    }
    pub fn enter_oracle(&mut self, o: &Oracle) -> Result<(), Error> {
        self.put_oracle(o, false)
    }
    pub fn insert_oracle(&mut self, o: &Oracle) -> Result<(), Error> {
        self.put_oracle(o, true)
    }
    fn put_oracle(&mut self, o: &Oracle, insert: bool) -> Result<(), Error> {
        self.enter(
            &o.public_key,
            o.serialize()?,
            &o.expires_by,
            Key::Oracle(o.public_key),
            insert,
        )
    }
    pub fn enter_query(&mut self, q: &Query) -> Result<(), Error> {
        self.put_query(q, false)
    }
    pub fn insert_query(&mut self, q: &Query) -> Result<(), Error> {
        self.put_query(q, true)
    }
    fn put_query(&mut self, q: &Query, insert: bool) -> Result<(), Error> {
        if q.oracle.tag != IdTag::Oracle {
            return Err(Error::InvalidIdType);
        }
        let oracle = q.oracle.value;
        self.enter(
            &query_key(oracle, q.id),
            q.serialize()?,
            &q.expires_by,
            Key::Query(oracle, q.id),
            insert,
        )
    }
    // Callers compose this root journal with the account journal. Refunds run
    // synchronously in the original order, preserving error and event priority.
    pub fn prune_with_refunds<E: From<Error>>(
        &mut self,
        next_height: u64,
        mut refund: impl FnMut(Refund) -> Result<(), E>,
    ) -> Result<(), E> {
        let Some(previous) = next_height.checked_sub(1) else {
            return Ok(());
        };
        let previous: Unsigned = previous.into();
        self.transaction(|s| {
            loop {
                let next = s.cache.iter().next().transpose().map_err(Error::from)?;
                let Some((key, value)) = next else {
                    break;
                };
                if value != Value::Bytes(vec![0]) {
                    return Err(Error::InvalidCache.into());
                }
                let (height, id) = cache::decode(&byte_key(&key)?)?;
                if height > previous {
                    break;
                }
                // Unlike AENS, the reference has no clause to skip old heights.
                if height < previous {
                    return Err(Error::SkippedExpiryHeight.into());
                }
                s.cache.delete(&key).map_err(Error::from)?;
                match id {
                    Key::Oracle(id) => {
                        if s.oracle(id)?.is_some_and(|o| o.expires_by <= previous) {
                            let ids = s.query_ids(id)?;
                            s.tree
                                .delete(&Nibbles::from_bytes(&id))
                                .map_err(Error::from)?;
                            for q in ids {
                                s.delete_query(id, q, false, &mut refund)?;
                            }
                        }
                    }
                    Key::Query(oracle, id) => {
                        if s.query(oracle, id)?
                            .is_some_and(|q| q.expires_by <= previous)
                        {
                            s.delete_query(oracle, id, true, &mut refund)?;
                        }
                    }
                }
            }
            Ok(())
        })
    }
    fn delete_query<E: From<Error>>(
        &mut self,
        oracle: [u8; 32],
        id: [u8; 32],
        emit_event: bool,
        refund: &mut impl FnMut(Refund) -> Result<(), E>,
    ) -> Result<(), E> {
        if let Some(q) = self.query(oracle, id)?
            && q.is_open()
        {
            if q.sender.tag != IdTag::Account {
                return Err(Error::InvalidIdType.into());
            }
            refund(Refund {
                account: AccountPublicKey(q.sender.value),
                amount: q.fee,
                emit_event,
            })?;
        }
        self.tree
            .delete(&Nibbles::from_bytes(&query_key(oracle, id)))
            .map_err(Error::from)?;
        Ok(())
    }
}
fn query_key(oracle: [u8; 32], id: [u8; 32]) -> Vec<u8> {
    [oracle.as_slice(), &id].concat()
}
fn bytes<S: NodeReader>(tree: &Trie<S>, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    tree.get(&Nibbles::from_bytes(key))?
        .map(value_bytes)
        .transpose()
}
fn value_bytes(value: Value) -> Result<Vec<u8>, Error> {
    match value {
        Value::Bytes(b) => Ok(b),
        _ => Err(Error::InvalidValue),
    }
}
fn byte_key(key: &Nibbles) -> Result<Vec<u8>, Error> {
    if !key.as_slice().len().is_multiple_of(2) {
        return Err(Error::InvalidValue);
    }
    Ok(key
        .as_slice()
        .chunks_exact(2)
        .map(|p| p[0] << 4 | p[1])
        .collect())
}
