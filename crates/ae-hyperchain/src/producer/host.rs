use super::{Host, Negotiation};
use crate::chain::source::Read;
use crate::{
    block::{self, Pin, host::PinSource},
    chain,
    eoe::{self, coordinator::Coordinator},
    signing::Keys,
};
use ae_codec::headers::KeyHeader;
use ae_trie::{MemoryStore, NodeReader};
use ae_types::{AccountPublicKey, Unsigned};
use ae_vm_fate::value::{ObjectPayload, Value};
use num_bigint::{BigInt, Sign};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug)]
pub enum Error<E> {
    Coordinator(eoe::coordinator::Error),
    Pin(E),
    MissingHeader(BigInt),
    UnsupportedInput,
    Selection(chain::LookupError),
}
// The chain owner supplies an immutable key-header snapshot corresponding to
// the selected transaction view. Durable lookup and top-change subscriptions
// remain owned by the node. No timestamp is inferred from VM environment time.
#[derive(Clone)]
pub struct LiveHost<P, A = MemoryStore, C = MemoryStore, S = chain::Selected<A, C>> {
    pub coordinator: Coordinator<A, C>,
    pub selected: S,
    pub headers: Arc<BTreeMap<BigInt, KeyHeader>>,
    pub keys: Keys,
    pub pins: P,
}
impl<P, A: NodeReader + Clone, C: NodeReader + Clone, S> LiveHost<P, A, C, S> {
    pub fn new(
        coordinator: Coordinator<A, C>,
        selected: S,
        headers: Arc<BTreeMap<BigInt, KeyHeader>>,
        pins: P,
    ) -> Self {
        let keys = Keys::from_shared(coordinator.host().signing_keys());
        Self {
            coordinator,
            selected,
            headers,
            keys,
            pins,
        }
    }
}
fn unsigned(n: &BigInt) -> Option<Unsigned> {
    if n.sign() == Sign::Minus {
        None
    } else {
        Unsigned::from_field_bytes(&n.to_bytes_be().1).ok()
    }
}
fn address(p: &ObjectPayload) -> Option<AccountPublicKey> {
    match p {
        ObjectPayload::Bytes(b) => b.as_slice().try_into().ok().map(AccountPublicKey),
        _ => None,
    }
}
fn integer(v: &Value) -> Option<Unsigned> {
    match v {
        Value::Integer(n) => unsigned(n),
        _ => None,
    }
}
impl<P: PinSource, A: NodeReader + Clone, C: NodeReader + Clone, S: chain::Source<A, C>> block::Host
    for LiveHost<P, A, C, S>
{
    type Error = Error<P::Error>;
    fn is_operational(e: &Self::Error) -> bool {
        match e {
            Error::Coordinator(eoe::coordinator::Error::Host(e)) => e.is_operational(),
            Error::Coordinator(eoe::coordinator::Error::Stopped) => false,
            Error::Pin(e) => P::is_operational(e),
            Error::Selection(e) => e.is_operational(),
            _ => true,
        }
    }
    fn entropy(&mut self, epoch: &BigInt) -> Result<Option<Vec<u8>>, Self::Error> {
        let epoch = unsigned(epoch).ok_or(Error::UnsupportedInput)?;
        self.coordinator
            .entropy_hash(&epoch)
            .map_err(Error::Coordinator)
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
impl<P: PinSource, A: NodeReader + Clone, C: NodeReader + Clone, S: chain::Source<A, C>> Host
    for LiveHost<P, A, C, S>
{
    fn key_time(&mut self, height: &BigInt) -> Result<BigInt, Self::Error> {
        self.headers
            .get(height)
            .map(|h| h.time.into())
            .ok_or_else(|| Error::MissingHeader(height.clone()))
    }
    fn key_present(&mut self, key: &ObjectPayload) -> Result<bool, Self::Error> {
        Ok(address(key).is_some_and(|k| self.keys.contains(k)))
    }
    fn set_candidate(&mut self, key: &ObjectPayload) -> Result<bool, Self::Error> {
        Ok(address(key).is_some_and(|k| self.keys.set_candidate(k).is_ok()))
    }
    fn negotiate(&mut self, n: Negotiation) -> Result<(), Self::Error> {
        let i = n.info;
        let epoch = integer(&i.epoch).ok_or(Error::UnsupportedInput)?;
        let height = unsigned(&n.slot).ok_or(Error::UnsupportedInput)?;
        let current_length = unsigned(&i.length).ok_or(Error::UnsupportedInput)?;
        let leader = address(&n.leader).ok_or(Error::UnsupportedInput)?;
        let validators = i
            .validators
            .ok_or(Error::UnsupportedInput)?
            .into_iter()
            .map(|s| {
                Ok((
                    address(&s.address).ok_or(Error::UnsupportedInput)?,
                    integer(&s.stake).ok_or(Error::UnsupportedInput)?,
                ))
            })
            .collect::<Result<Vec<_>, Self::Error>>()?;
        self.coordinator
            .negotiate(eoe::Negotiation {
                epoch,
                height,
                hash: n.hash,
                leader,
                validators,
                seed: i.seed,
                current_length,
            })
            .map_err(Error::Coordinator)
    }
}
