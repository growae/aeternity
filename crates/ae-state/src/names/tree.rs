// Aeternity fa2a6d7, aens_state_tree. Both main and expiry-index roots matter
// for persistence; only the main root enters the six-tree state commitment.
// Copyright (c) 2018, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{
    Auction, Commitment, Error, Name, NameResolver,
    cache::{self, Kind},
};
use ae_protocol::{Protocol, names as rules};
use ae_trie::{MemoryStore, Nibbles, NodeReader, NodeWriter, Trie, Value};
use ae_types::{AccountPublicKey, Hash256, IdTag, Unsigned};

#[derive(Debug, Clone)]
pub struct Names<S = MemoryStore, C = MemoryStore> {
    tree: Trie<S>,
    cache: Trie<C>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockedFee {
    pub amount: Unsigned,
}
impl Default for Names {
    fn default() -> Self {
        Self::new(MemoryStore::default(), MemoryStore::default())
    }
}
impl<S: NodeReader, C: NodeReader> Names<S, C> {
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
    pub fn name(&self, hash: [u8; 32]) -> Result<Option<Name>, Error> {
        bytes(&self.tree, &hash)?
            .map(|b| Name::deserialize(hash, &b))
            .transpose()
    }
    pub fn commitment(&self, hash: [u8; 32]) -> Result<Option<Commitment>, Error> {
        bytes(&self.tree, &hash)?
            .map(|b| Commitment::deserialize(hash, &b))
            .transpose()
    }
    pub fn auction(&self, hash: [u8; 33]) -> Result<Option<Auction>, Error> {
        bytes(&self.tree, &hash)?
            .map(|b| Auction::deserialize(hash, &b))
            .transpose()
    }
    pub fn auctions(&self) -> impl Iterator<Item = Result<Auction, Error>> + '_ {
        self.tree.iter().filter_map(|item| match item {
            Err(e) => Some(Err(e.into())),
            Ok((key, value)) if key.as_slice().len() > 64 => Some((|| {
                let hash = byte_key(&key)?
                    .try_into()
                    .map_err(|_| Error::InvalidValue)?;
                let Value::Bytes(encoded) = value else {
                    return Err(Error::InvalidValue);
                };
                Auction::deserialize(hash, &encoded)
            })()),
            Ok(_) => None,
        })
    }
}
impl<S: NodeReader, C: NodeReader> NameResolver for Names<S, C> {
    fn resolve_account(&self, hash: [u8; 32]) -> Result<AccountPublicKey, Error> {
        self.tree.resolve_account(hash)
    }
}
impl<S: NodeWriter, C: NodeWriter> Names<S, C> {
    // Compose with the account transaction when applying auction fee locks.
    // Failed operations retain old roots; unreachable writes can later be GC'd.
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
        hash: &[u8],
        encoded: Vec<u8>,
        ttl: &Unsigned,
        kind: Kind,
    ) -> Result<(), Error> {
        self.transaction(|state| {
            state.cache.put(
                &Nibbles::from_bytes(&cache::encode(ttl, hash, kind)),
                Value::Bytes(vec![0]),
            )?;
            state
                .tree
                .put(&Nibbles::from_bytes(hash), Value::Bytes(encoded))?;
            Ok(())
        })
    }
    pub fn enter_name(&mut self, name: &Name) -> Result<(), Error> {
        self.enter(&name.hash, name.serialize()?, &name.expires_by, Kind::Name)
    }
    pub fn insert_commitment(&mut self, commitment: &Commitment) -> Result<(), Error> {
        if self
            .tree
            .get(&Nibbles::from_bytes(&commitment.hash))?
            .is_some()
        {
            return Err(Error::AlreadyPresent);
        }
        self.enter(
            &commitment.hash,
            commitment.serialize()?,
            &commitment.expires_by,
            Kind::Commitment,
        )
    }
    pub fn enter_auction(&mut self, auction: &Auction) -> Result<(), Error> {
        self.enter(
            &auction.hash(),
            auction.serialize()?,
            &auction.expires_by,
            Kind::Auction,
        )
    }
    // Deletions deliberately leave old expiry-index entries, as in Erlang.
    pub fn delete_name(&mut self, hash: [u8; 32]) -> Result<(), Error> {
        self.delete(&hash)
    }
    pub fn delete_commitment(&mut self, hash: [u8; 32]) -> Result<(), Error> {
        self.delete(&hash)
    }
    pub fn delete_auction(&mut self, hash: [u8; 33]) -> Result<(), Error> {
        self.delete(&hash)
    }
    fn delete(&mut self, hash: &[u8]) -> Result<(), Error> {
        self.tree.delete(&Nibbles::from_bytes(hash))?;
        Ok(())
    }
    pub fn prune(&mut self, next_height: u64, protocol: Protocol) -> Result<Vec<LockedFee>, Error> {
        let Some(previous) = next_height.checked_sub(1) else {
            return Ok(Vec::new());
        };
        let previous: Unsigned = previous.into();
        self.transaction(|state| {
            let mut expired = Vec::new();
            // Capture all values before applying actions. Re-reading updated
            // values while pruning would change the original stale-entry rules.
            for item in state.cache.iter() {
                let (key, value) = item?;
                if value != Value::Bytes(vec![0]) {
                    return Err(Error::InvalidCache);
                }
                let (height, hash, kind) = cache::decode(&byte_key(&key)?)?;
                if height > previous {
                    break;
                }
                expired.push((key, hash.clone(), kind, bytes(&state.tree, &hash)?));
            }
            for (key, _, _, _) in &expired {
                state.cache.delete(key)?;
            }
            let mut locks = Vec::new();
            for (_, hash, kind, encoded) in expired {
                let Some(encoded) = encoded else {
                    continue;
                };
                match kind {
                    Kind::Commitment => {
                        let hash = hash.try_into().map_err(|_| Error::InvalidValue)?;
                        Commitment::deserialize(hash, &encoded)?;
                        state.delete_commitment(hash)?;
                    }
                    Kind::Name => {
                        let hash = hash.try_into().map_err(|_| Error::InvalidValue)?;
                        let expired = Name::deserialize(hash, &encoded)?;
                        if expired.expires_by != previous {
                            continue;
                        }
                        match expired.status.as_slice() {
                            b"claimed" => {
                                let mut current = state.name(hash)?.ok_or(Error::NameNotFound)?;
                                current.status = b"revoked".to_vec();
                                current.expires_by = previous.add(&rules::PROTECTION_PERIOD.into());
                                state.enter_name(&current)?;
                            }
                            b"revoked" => state.delete_name(hash)?,
                            _ => return Err(Error::InvalidValue),
                        }
                    }
                    Kind::Auction => {
                        let hash = hash.try_into().map_err(|_| Error::InvalidValue)?;
                        let auction = Auction::deserialize(hash, &encoded)?;
                        if auction.expires_by != previous {
                            continue;
                        }
                        if auction.bidder.tag != IdTag::Account {
                            return Err(Error::InvalidValue);
                        }
                        let name = Name::new(
                            auction.name_hash,
                            AccountPublicKey(auction.bidder.value),
                            previous.add(&rules::max_expiration(protocol).into()),
                        );
                        state.delete_auction(hash)?;
                        state.enter_name(&name)?;
                        locks.push(LockedFee {
                            amount: auction.bid,
                        });
                    }
                }
            }
            Ok(locks)
        })
    }
}
fn bytes<S: NodeReader>(tree: &Trie<S>, hash: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    match tree.get(&Nibbles::from_bytes(hash))? {
        None => Ok(None),
        Some(Value::Bytes(b)) => Ok(Some(b)),
        Some(_) => Err(Error::InvalidValue),
    }
}
pub(super) fn byte_key(key: &Nibbles) -> Result<Vec<u8>, Error> {
    if !key.as_slice().len().is_multiple_of(2) {
        return Err(Error::InvalidValue);
    }
    Ok(key
        .as_slice()
        .chunks_exact(2)
        .map(|p| (p[0] << 4) | p[1])
        .collect())
}
