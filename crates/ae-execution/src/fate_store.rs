use ae_state::contracts::{Error, store::Store};
use ae_trie::{MemoryStore, NodeReader};

// Deterministic host adapter; no VM dependency on the state implementation.
#[derive(Debug, Clone)]
pub struct Backend<S = MemoryStore>(pub Store<S>);
impl<S: NodeReader + Clone> ae_vm_fate::store::Backend for Backend<S> {
    type Error = Error;
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Error> {
        self.0.get(key)
    }
    fn get_cached(&mut self, key: &[u8]) -> Result<Vec<u8>, Error> {
        self.0.get_cached(key)
    }
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.0.put(key, value);
    }
}

impl<S: NodeReader + Clone> ae_vm_fate::store::write::SubtreeBackend for Backend<S> {
    fn subtree(
        &self,
        prefix: &[u8],
        maximum_bytes: usize,
    ) -> Result<Option<(ae_vm_fate::store::write::RawMap, usize)>, Error> {
        match self.0.subtree_within_bytes(prefix, maximum_bytes) {
            Ok(found) => Ok(Some(found)),
            Err(Error::ResourceLimit) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl<S: NodeReader + Clone> ae_vm_fate::store::read::CachedSubtreeBackend for Backend<S> {
    fn subtree_cached(
        &mut self,
        prefix: &[u8],
        maximum_bytes: usize,
    ) -> Result<Option<(ae_vm_fate::store::write::RawMap, usize)>, Error> {
        match self.0.subtree_cached_within_bytes(prefix, maximum_bytes) {
            Ok(found) => Ok(Some(found)),
            Err(Error::ResourceLimit) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
