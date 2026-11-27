// Read-only access to one captured immutable state source. The protocol has no
// durable-write message. Child writes go into its own ae_trie::Overlay.
use super::{Broker, wire};
use ae_trie::{Error as TrieError, NodeReader};
use ae_types::Hash256;
use std::{
    io::{Read, Write},
    sync::{Arc, Mutex},
};

pub struct Store<S> {
    pub source: Arc<S>,
    maximum: usize,
}
#[derive(Debug)]
pub enum Error {
    Configuration,
    Query,
    Capacity,
    Trie(TrieError),
}
impl<S: NodeReader> Store<S> {
    pub fn new(source: Arc<S>, maximum: usize) -> Result<Self, Error> {
        if maximum == 0 || maximum >= wire::MAX_FRAME {
            return Err(Error::Configuration);
        }
        Ok(Self { source, maximum })
    }
}
impl<S: NodeReader> Broker for Store<S> {
    type Error = Error;
    fn read(&mut self, query: &[u8]) -> Result<Vec<u8>, Self::Error> {
        let [0, rest @ ..] = query else {
            return Err(Error::Query);
        };
        let hash = Hash256(rest.try_into().map_err(|_| Error::Query)?);
        match self.source.node(&hash).map_err(Error::Trie)? {
            None => Ok(vec![0]),
            Some(mut value) => {
                if value.len() > self.maximum {
                    return Err(Error::Capacity);
                }
                if ae_crypto::blake2b_256(&value) != hash {
                    return Err(Error::Trie(TrieError::HashMismatch(hash)));
                }
                value.try_reserve_exact(1).map_err(|_| Error::Capacity)?;
                value.insert(0, 1);
                Ok(value)
            }
        }
    }
}
pub struct Reader<R, W> {
    session: Arc<Mutex<wire::Session<R, W>>>,
    limits: ae_codec::rlp::DecodeLimits,
}
impl<R, W> Clone for Reader<R, W> {
    fn clone(&self) -> Self {
        Self {
            session: self.session.clone(),
            limits: self.limits,
        }
    }
}
impl<R: Read, W: Write> Reader<R, W> {
    pub fn new(
        session: Arc<Mutex<wire::Session<R, W>>>,
        limits: ae_codec::rlp::DecodeLimits,
    ) -> Self {
        Self { session, limits }
    }
}
impl<R: Read, W: Write> NodeReader for Reader<R, W> {
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, TrieError> {
        let mut query = [0; 33];
        query[1..].copy_from_slice(&hash.0);
        let mut response = self
            .session
            .lock()
            .map_err(|_| TrieError::Backend("dry-run reader poisoned".into()))?
            .read(&query)
            .map_err(|_| TrieError::Backend("dry-run read transport".into()))?;
        match response.first() {
            Some(0) if response.len() == 1 => Ok(None),
            Some(1) => {
                if response.len() - 1 > self.limits.max_input_bytes {
                    return Err(TrieError::ResourceLimit);
                }
                response.remove(0);
                if ae_crypto::blake2b_256(&response) != *hash {
                    return Err(TrieError::HashMismatch(*hash));
                }
                Ok(Some(response))
            }
            _ => Err(TrieError::Backend("invalid dry-run node response".into())),
        }
    }
    fn decode_limits(&self) -> ae_codec::rlp::DecodeLimits {
        self.limits
    }
}
