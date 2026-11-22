use super::{Host, Pin, unsigned};
use crate::chain::source::Read;
use crate::{chain, eoe::local_host};
use ae_trie::{MemoryStore, NodeReader};
use num_bigint::BigInt;

// The connector supplies decoded parent-chain pin proofs. Backend failures
// retain their operational/native distinction at the consensus boundary.
pub trait PinSource: Clone {
    type Error;
    fn is_operational(error: &Self::Error) -> bool;
    fn get_pin(&mut self, hash: &[u8]) -> Result<Option<Pin>, Self::Error>;
}

#[derive(Debug)]
pub enum Error<E> {
    Entropy(local_host::Error),
    Pin(E),
    UnsupportedEpoch(BigInt),
    Selection(chain::LookupError),
}

#[derive(Clone)]
pub struct LiveHost<P, A = MemoryStore, C = MemoryStore, S = chain::Selected<A, C>> {
    pub entropy: local_host::LocalHost<A, C>,
    pub selected: S,
    pub pins: P,
}
impl<P: PinSource, A: NodeReader + Clone, C: NodeReader + Clone, S: chain::Source<A, C>> Host
    for LiveHost<P, A, C, S>
{
    type Error = Error<P::Error>;
    fn is_operational(error: &Self::Error) -> bool {
        match error {
            Error::Entropy(e) => e.is_operational(),
            Error::Pin(e) => P::is_operational(e),
            Error::Selection(e) => e.is_operational(),
            // LocalHost uses unsigned epoch notification records. Do not
            // classify an unsupported signed input as consensus invalidity.
            Error::UnsupportedEpoch(_) => true,
        }
    }
    fn entropy(&mut self, epoch: &BigInt) -> Result<Option<Vec<u8>>, Self::Error> {
        let epoch = unsigned(epoch).ok_or_else(|| Error::UnsupportedEpoch(epoch.clone()))?;
        self.entropy.entropy_hash(&epoch).map_err(Error::Entropy)
    }
    fn pin(&mut self, hash: &[u8]) -> Result<Option<Pin>, Self::Error> {
        self.pins.get_pin(hash).map_err(Error::Pin)
    }
    fn key_hash(&mut self, height: &BigInt) -> Result<Option<Vec<u8>>, Self::Error> {
        self.selected
            .capture()
            .and_then(|s| s.key_hash(height))
            .map_err(Error::Selection)
    }
}
