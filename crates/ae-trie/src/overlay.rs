use crate::{Error, NodeReader, NodeWriter};
use ae_crypto::blake2b_256;
use ae_types::Hash256;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, Copy)]
pub struct OverlayLimits {
    pub nodes: usize,
    pub bytes: usize,
}
impl Default for OverlayLimits {
    fn default() -> Self {
        Self {
            nodes: 1_000_000,
            bytes: 256 * 1024 * 1024,
        }
    }
}
// Clones share immutable staged nodes until a write. A discarded execution
// cannot leak its changes into its parent or another transaction snapshot.
#[derive(Debug, Clone)]
pub struct Overlay<S> {
    base: S,
    nodes: Arc<BTreeMap<Hash256, Vec<u8>>>,
    bytes: usize,
    limits: OverlayLimits,
}
impl<S> Overlay<S> {
    pub fn new(base: S, limits: OverlayLimits) -> Self {
        Self {
            base,
            nodes: Arc::default(),
            bytes: 0,
            limits,
        }
    }
    pub fn pending(&self) -> &BTreeMap<Hash256, Vec<u8>> {
        &self.nodes
    }
    pub fn base(&self) -> &S {
        &self.base
    }
}
impl<S: NodeReader> NodeReader for Overlay<S> {
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, Error> {
        match self.nodes.get(hash) {
            Some(b) => Ok(Some(b.clone())),
            None => self.base.node(hash),
        }
    }
    fn decode_limits(&self) -> ae_codec::rlp::DecodeLimits {
        self.base.decode_limits()
    }
}
impl<S: NodeReader> NodeWriter for Overlay<S> {
    fn put_node(&mut self, hash: Hash256, bytes: Vec<u8>) -> Result<(), Error> {
        if blake2b_256(&bytes) != hash {
            return Err(Error::HashMismatch(hash));
        }
        if self.nodes.contains_key(&hash) {
            return Ok(());
        }
        let size = self
            .bytes
            .checked_add(bytes.len())
            .ok_or(Error::ResourceLimit)?;
        if self.nodes.len() >= self.limits.nodes || size > self.limits.bytes {
            return Err(Error::ResourceLimit);
        }
        Arc::make_mut(&mut self.nodes).insert(hash, bytes);
        self.bytes = size;
        Ok(())
    }
}
