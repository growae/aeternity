// aect_state_tree:add_poi/verify_poi/lookup_poi and aec_poi, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{Contract, Contracts, Error, LoadOptions, store::Contents, store_id};
use ae_trie::{IteratorOptions, MemoryStore, Nibbles, NodeReader, NodeWriter};
use ae_types::Hash256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractProof {
    root: Hash256,
    nodes: MemoryStore,
}
impl ContractProof {
    pub fn new(root: Hash256) -> Self {
        Self {
            root,
            nodes: MemoryStore::default(),
        }
    }
    pub fn from_nodes(root: Hash256, nodes: MemoryStore) -> Self {
        Self { root, nodes }
    }
    pub fn root_hash(&self) -> Hash256 {
        self.root
    }
    pub fn nodes(&self) -> &MemoryStore {
        &self.nodes
    }

    // Materialize the entire store before returning. A proof containing only
    // the record (or a partial storage path) cannot stand in for full state.
    pub fn lookup(&self, key: [u8; 32]) -> Result<Option<(Contract, Contents)>, Error> {
        let tree = Contracts::from_root(Some(self.root), Borrowed(&self.nodes))?;
        let Some(loaded) = tree.lookup(key, LoadOptions::default())? else {
            return Ok(None);
        };
        let contents = loaded.store.ok_or(Error::NoSuchStore)?.contents()?;
        Ok(Some((loaded.contract, contents)))
    }
    pub fn verify(
        &self,
        key: [u8; 32],
        contract: &Contract,
        contents: &Contents,
    ) -> Result<(), Error> {
        if key != contract.public_key {
            return Err(Error::BadProof);
        }
        let tree = Contracts::from_root(Some(self.root), Borrowed(&self.nodes))?;
        let actual = tree.tree().get(&Nibbles::from_bytes(&key))?;
        if actual != Some(ae_trie::Value::Bytes(contract.serialize()?)) {
            return Err(Error::BadProof);
        }
        let proven = tree.read_store(key)?.contents()?;
        if proven != *contents || contract.validate_writes(contents).is_err() {
            return Err(Error::BadProof);
        }
        Ok(())
    }
}
struct Borrowed<'a>(&'a MemoryStore);
impl NodeReader for Borrowed<'_> {
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.0.node(hash)
    }
}
impl<S: NodeReader> Contracts<S> {
    pub fn proof(&self, key: [u8; 32]) -> Result<ContractProof, Error> {
        let mut proof = ContractProof::new(self.root_hash().ok_or(Error::WrongRootHash)?);
        self.add_to_proof(key, &mut proof)?;
        Ok(proof)
    }
    // Existing nodes survive a failed addition, just as the immutable Erlang
    // proof does. The caller may accumulate several contracts at one root.
    pub fn add_to_proof(&self, key: [u8; 32], proof: &mut ContractProof) -> Result<(), Error> {
        if self.root_hash() != Some(proof.root) {
            return Err(Error::WrongRootHash);
        }
        let (record, mut additional) = self.tree().lookup_with_proof(&Nibbles::from_bytes(&key))?;
        if record.is_none() {
            return Err(Error::NotPresent);
        }
        let prefix = Nibbles::from_bytes(&store_id(key));
        // iterator_from is exclusive: the store marker itself is not added.
        for row in self.tree().iter_with_options(IteratorOptions {
            after: Some(prefix.clone()),
            prefix: Some(prefix),
            max_nibbles: None,
        }) {
            let (k, _) = row?;
            let (_, nodes) = self.tree().lookup_with_proof(&k)?;
            for (hash, bytes) in nodes.nodes() {
                additional.put_node(*hash, bytes.clone())?;
            }
        }
        for (hash, bytes) in additional.nodes() {
            proof.nodes.put_node(*hash, bytes.clone())?;
        }
        Ok(())
    }
}
