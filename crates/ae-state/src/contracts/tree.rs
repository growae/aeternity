// aect_state_tree.erl, fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{
    Code, Contract, Error,
    store::{Contents, Store, value_bytes},
    store_id,
};
use ae_trie::{MemoryStore, Nibbles, NodeReader, NodeWriter, ReadView, Trie, Value};
use ae_types::{Hash256, IdTag};

#[derive(Debug, Clone, Copy, Default)]
pub struct LoadOptions {
    pub no_store: bool,
    pub full_store_cache: bool,
}
pub struct LoadedContract<S> {
    pub contract: Contract,
    pub store: Option<Store<S>>,
}
pub type ContractWithCode<S> = (LoadedContract<S>, Vec<u8>);
#[derive(Debug, Clone)]
pub struct Contracts<S = MemoryStore> {
    tree: Trie<S>,
}
impl Default for Contracts {
    fn default() -> Self {
        Self::new(MemoryStore::default())
    }
}
impl<S: NodeReader> Contracts<S> {
    pub fn new(store: S) -> Self {
        Self {
            tree: Trie::new(store),
        }
    }
    pub fn from_root(root: Option<Hash256>, store: S) -> Result<Self, Error> {
        Ok(Self {
            tree: Trie::from_root(root, store)?,
        })
    }
    pub fn root_hash(&self) -> Option<Hash256> {
        self.tree.root_hash()
    }
    pub fn tree(&self) -> &Trie<S> {
        &self.tree
    }
    pub fn is_contract(&self, key: [u8; 32]) -> Result<bool, Error> {
        Ok(self.tree.get(&Nibbles::from_bytes(&key))?.is_some())
    }
    pub fn record(&self, key: [u8; 32]) -> Result<Option<Contract>, Error> {
        self.tree
            .get(&Nibbles::from_bytes(&key))?
            .map(|v| Contract::deserialize(key, &value_bytes(v)?))
            .transpose()
    }
    pub fn read_store(&self, key: [u8; 32]) -> Result<Store<ReadView<'_, S>>, Error> {
        match self
            .tree
            .read_only_subtree(&Nibbles::from_bytes(&store_id(key)))
        {
            Ok(tree) => Ok(Store::new(tree)),
            Err(ae_trie::Error::NoSuchSubtree) => Err(Error::NoSuchStore),
            Err(e) => Err(e.into()),
        }
    }
    pub fn lookup(
        &self,
        key: [u8; 32],
        options: LoadOptions,
    ) -> Result<Option<LoadedContract<ReadView<'_, S>>>, Error> {
        let Some(contract) = self.record(key)? else {
            return Ok(None);
        };
        let store = if options.no_store {
            None
        } else {
            let mut store = self.read_store(key)?;
            if options.full_store_cache {
                store.put_map(store.contents()?);
            }
            contract.validate_writes(store.write_cache())?;
            Some(store)
        };
        Ok(Some(LoadedContract { contract, store }))
    }
    pub fn lookup_with_code(
        &self,
        key: [u8; 32],
        options: LoadOptions,
    ) -> Result<Option<ContractWithCode<ReadView<'_, S>>>, Error> {
        let Some(loaded) = self.lookup(key, options)? else {
            return Ok(None);
        };
        let code = match &loaded.contract.code {
            Code::Inline(code) => code.clone(),
            Code::Reference(id) => {
                if id.tag != IdTag::Contract {
                    return Err(Error::InvalidIdType);
                }
                let reference = self.lookup(id.value, options)?.ok_or(Error::NotPresent)?;
                let Code::Inline(code) = reference.contract.code else {
                    return Err(Error::InvalidCodeReference);
                };
                code
            }
        };
        Ok(Some((loaded, code)))
    }
    pub fn transaction<T, E>(
        &mut self,
        action: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<T, E> {
        let root = self.tree.savepoint();
        let result = action(self);
        if result.is_err() {
            self.tree.rollback(root);
        }
        result
    }
}
impl<S: NodeWriter> Contracts<S> {
    pub fn insert(&mut self, contract: &Contract, writes: &Contents) -> Result<(), Error> {
        self.transaction(|s| {
            s.put(&contract.public_key, contract.serialize()?, true)?;
            let prefix = store_id(contract.public_key);
            s.put(&prefix, vec![0], true)?;
            s.write_store(&prefix, writes, true)
        })
    }
    pub fn enter(&mut self, contract: &Contract, writes: &Contents) -> Result<(), Error> {
        self.transaction(|s| {
            s.put(&contract.public_key, contract.serialize()?, false)?;
            s.write_store(&store_id(contract.public_key), writes, false)
        })
    }
    // Deletion intentionally leaves the store marker and all storage rows.
    pub fn delete(&mut self, key: [u8; 32]) -> Result<(), Error> {
        self.tree.delete(&Nibbles::from_bytes(&key))?;
        Ok(())
    }
    pub fn copy_store<R: NodeReader>(
        &mut self,
        source: &Store<R>,
        target: [u8; 32],
    ) -> Result<(), Error> {
        self.copy_contents(&source.contents()?, target)
    }
    pub fn copy_contents(&mut self, contents: &Contents, target: [u8; 32]) -> Result<(), Error> {
        self.transaction(|s| {
            let prefix = store_id(target);
            s.put(&prefix, vec![0], true)?;
            s.write_store(&prefix, contents, true)
        })
    }
    fn write_store(&mut self, prefix: &[u8], writes: &Contents, insert: bool) -> Result<(), Error> {
        for (key, value) in writes {
            if !key.is_empty() {
                self.put(&[prefix, key].concat(), value.clone(), insert)?;
            }
        }
        Ok(())
    }
    fn put(&mut self, key: &[u8], value: Vec<u8>, insert: bool) -> Result<(), Error> {
        let key = Nibbles::from_bytes(key);
        if insert && self.tree.get(&key)?.is_some() {
            return Err(Error::AlreadyPresent);
        }
        self.tree.put(&key, Value::Bytes(value))?;
        Ok(())
    }
}
