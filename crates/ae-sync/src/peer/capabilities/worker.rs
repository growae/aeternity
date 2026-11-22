use super::{CHAIN_POI, Error as RegistryError, Limits, PeerValue, Registry, Value, poi_bytes};
use crate::{bindings, owned};
use ae_p2p::{message::ChainPoi, runtime as net};
use ae_types::PeerPublicKey;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

pub use crate::owned::{Fault, Limits as QueueLimits};
pub type Info = owned::Info<Error>;
pub type Ticket<T> = owned::Ticket<T, Error>;
#[derive(Debug, Clone, Copy, Default)]
pub struct Config {
    pub registry: Limits,
    pub queue: QueueLimits,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Registry(RegistryError),
    Worker(Fault),
}
impl From<RegistryError> for Error {
    fn from(e: RegistryError) -> Self {
        Self::Registry(e)
    }
}
impl From<Fault> for Error {
    fn from(e: Fault) -> Self {
        Self::Worker(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

// Only the lane owns the lease. Queued requests and worker state hold tokens;
// neither can keep a retired lane alive or enqueue an unbounded cleanup task.
pub(crate) struct Lease(Arc<AtomicBool>);
#[derive(Clone)]
pub(crate) struct Token(Arc<AtomicBool>);
impl Lease {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }
    pub fn token(&self) -> Token {
        Token(self.0.clone())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
struct Connection {
    client: net::Client,
    token: Token,
}
impl Connection {
    fn live(&self) -> bool {
        self.token.0.load(Ordering::Acquire)
            && !self.client.is_closed()
            && !self.client.is_disconnecting()
    }
}
struct State {
    registry: Registry<net::Generation>,
    connections: BTreeMap<PeerPublicKey, Connection>,
}
impl owned::Actor for State {
    type Error = Error;
    fn before(&mut self) -> Result<(), Error> {
        self.connections.retain(|peer, c| {
            if c.live() {
                true
            } else {
                self.registry.remove(*peer, c.client.generation());
                false
            }
        });
        Ok(())
    }
}
#[derive(Clone)]
pub struct Client {
    inner: owned::Client<State>,
    limits: Limits,
}
impl Client {
    pub fn info(&self) -> Info {
        self.inner.info()
    }
    pub fn limits(&self) -> Limits {
        self.limits
    }
    fn key(&self, key: &str) -> Result<usize, Error> {
        if key.len() > self.limits.key_bytes {
            return Err(RegistryError::Capacity.into());
        }
        Ok(128 + key.len())
    }
    pub fn set(&self, key: &str, value: &Value) -> Result<Ticket<()>, Error> {
        let bytes = self
            .key(key)?
            .checked_add(
                value
                    .bytes()
                    .filter(|n| *n <= self.limits.value_bytes)
                    .ok_or(RegistryError::Capacity)?,
            )
            .ok_or(RegistryError::Capacity)?;
        let (key, value) = (key.to_owned(), value.clone());
        self.inner.submit(bytes, move |s| {
            s.registry.set(&key, &value).map_err(Into::into)
        })
    }
    pub fn delete(&self, key: &str) -> Result<Ticket<()>, Error> {
        let bytes = self.key(key)?;
        let key = key.to_owned();
        self.inner
            .submit(bytes, move |s| s.registry.delete(&key).map_err(Into::into))
    }
    pub fn get(&self, key: &str) -> Result<Ticket<Option<Value>>, Error> {
        let bytes = self.key(key)?;
        let key = key.to_owned();
        self.inner
            .submit(bytes, move |s| Ok(s.registry.get(&key).cloned()))
    }
    pub fn local(&self) -> Result<Ticket<BTreeMap<String, Value>>, Error> {
        self.inner.submit(128, |s| Ok(s.registry.local().clone()))
    }
    pub fn peers_with(&self, key: &str) -> Result<Ticket<Vec<PeerValue<net::Generation>>>, Error> {
        let bytes = self.key(key)?;
        let key = key.to_owned();
        self.inner
            .submit(bytes, move |s| Ok(s.registry.peers_with(&key)))
    }
    pub fn wire(&self) -> Result<Ticket<Vec<ChainPoi>>, Error> {
        self.inner
            .submit(128, |s| s.registry.wire().map_err(Into::into))
    }
    pub(crate) fn receive(
        &self,
        client: net::Client,
        token: Token,
        records: &[ChainPoi],
    ) -> Result<Ticket<()>, Error> {
        let bytes = records
            .last()
            .map(poi_bytes)
            .unwrap_or(Some(0))
            .and_then(|n| n.checked_add(256))
            .ok_or(RegistryError::Capacity)?;
        if bytes.saturating_sub(192) > self.limits.value_bytes {
            return Err(RegistryError::Capacity.into());
        }
        let records: Vec<_> = records.last().cloned().into_iter().collect();
        self.inner.submit(bytes, move |s| {
            let c = Connection { client, token };
            if !c.live() {
                return Err(RegistryError::Stale.into());
            }
            let peer = c.client.peer();
            // Admission and registration execute on the same incarnation. A
            // restart between them cannot erase an authenticated connection.
            s.registry.attach(peer, c.client.generation().clone())?;
            s.connections.insert(peer, c);
            s.registry
                .receive(peer, s.connections[&peer].client.generation(), &records)?;
            Ok(())
        })
    }
}
pub struct Worker {
    inner: owned::Worker<State>,
    limits: Limits,
}
impl Worker {
    pub fn spawn(config: Config) -> Result<Self, Error> {
        Self::seeded(config, None)
    }
    pub(crate) fn seeded(config: Config, initial: Option<ChainPoi>) -> Result<Self, Error> {
        config.registry.validate()?;
        let inner = owned::Worker::spawn("ae-capabilities", config.queue, move || {
            let mut registry = Registry::new(config.registry)?;
            if let Some(record) = initial {
                registry.set(CHAIN_POI, &Value::ChainPoi(vec![record]))?;
            }
            Ok(State {
                registry,
                connections: BTreeMap::new(),
            })
        })?;
        Ok(Self {
            inner,
            limits: config.registry,
        })
    }
    pub fn client(&self) -> Client {
        Client {
            inner: self.inner.client(),
            limits: self.limits,
        }
    }
    pub fn info(&self) -> Info {
        self.inner.info()
    }
    pub fn request_shutdown(&self) {
        self.inner.request_shutdown();
    }
    pub fn shutdown(self) -> Result<(), Error> {
        self.inner.shutdown()
    }
}

pub(crate) type Binding = bindings::Binding<Client>;
// Waiting is allowed only before acceptance. Once a ticket exists the caller
// observes that exact worker's reply or failure, even after a replacement.
pub(crate) async fn call<T>(
    binding: &Binding,
    mut submit: impl FnMut(&Client) -> Result<Ticket<T>, Error>,
) -> Result<T, Error> {
    let mut binding = binding.clone();
    loop {
        if binding.is_closed() {
            return Err(Fault::Stopped.into());
        }
        if let Some(client) = binding.current() {
            match submit(&client) {
                Ok(ticket) => return ticket.await,
                Err(Error::Worker(Fault::Stopped)) if binding.managed() => (),
                Err(error) => return Err(error),
            }
        }
        binding.changed().await;
    }
}

#[cfg(test)]
mod tests;
