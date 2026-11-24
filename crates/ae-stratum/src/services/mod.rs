// Synchronous operational ownership. Commands and data queries use try-lock
// backpressure. Status, cleanup and owner shutdown wait for admitted operations;
// shutdown invalidates every connection.
// TCP, timers, pool accounting and application restart policy live above this.
use crate::{
    nonce::{Part, cache::Cache},
    registry::{self, Registry},
    session::{self, Chain, Membership, Operation, Reply, WorkerRegistration},
};
use num_bigint::BigInt;
use rand::{RngCore, rngs::OsRng};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex, MutexGuard, TryLockError, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    Clock,
    Entropy,
    Panicked,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Closed,
    Stale,
    Lease,
    Sequence,
    Limit,
    Fault(Fault),
    State(crate::Error),
}
impl From<crate::Error> for Error {
    fn from(e: crate::Error) -> Self {
        Self::State(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone)]
pub struct Config {
    pub registry: registry::Limits,
    pub connections: usize,
    pub max_workers: usize,
    pub notifications_per_connection: usize,
    pub notification_bytes_per_connection: usize,
    pub retained_reply_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            registry: Default::default(),
            connections: 4096,
            max_workers: 20,
            notifications_per_connection: 64,
            notification_bytes_per_connection: 1 << 20,
            retained_reply_bytes: 16 << 20,
        }
    }
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        if self.connections == 0
            || self.connections > 1_000_000
            || self.max_workers > 1_000_000
            || self.notifications_per_connection == 0
            || self.notifications_per_connection > 65536
            || self.notification_bytes_per_connection == 0
            || self.notification_bytes_per_connection > 1 << 30
            || self.retained_reply_bytes == 0
            || self.retained_reply_bytes > 1 << 30
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
// Providers must not reenter this owner. Shutdown waits for any active provider.
// Deterministic registry/cache state receives only the resulting explicit data.
pub trait Sources: Send {
    fn now(&mut self) -> Result<BigInt, Fault>;
    fn draw(&mut self, maximum: u64) -> Result<u64, Fault>;
}
struct System;
impl Sources for System {
    fn now(&mut self) -> Result<BigInt, Fault> {
        Ok(BigInt::from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Fault::Clock)?
                .as_millis(),
        ))
    }
    fn draw(&mut self, maximum: u64) -> Result<u64, Fault> {
        let mut bytes = [0; 8];
        OsRng
            .try_fill_bytes(&mut bytes)
            .map_err(|_| Fault::Entropy)?;
        // Nonce ranges have power-of-two cardinality, so masking is unbiased.
        Ok(u64::from_le_bytes(bytes) & maximum)
    }
}
struct Budget {
    used: AtomicUsize,
    maximum: usize,
}
struct Credit {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Credit {
    fn reserve(budget: &Arc<Budget>, bytes: usize) -> Result<Self, Error> {
        budget
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(bytes).filter(|n| *n <= budget.maximum)
            })
            .map_err(|_| Error::Busy)?;
        Ok(Self {
            budget: budget.clone(),
            bytes,
        })
    }
}
impl Drop for Credit {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
struct Retained<T> {
    value: T,
    _credit: Credit,
}
pub struct View<T>(Arc<Retained<T>>);
impl<T> Clone for View<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> std::ops::Deref for View<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0.value
    }
}
impl<T> View<T> {
    fn new(value: T, credit: Credit) -> Self {
        Self(Arc::new(Retained {
            value,
            _credit: credit,
        }))
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Account {
    pub account: Vec<u8>,
    pub entry: registry::Entry,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Info {
    pub closed: bool,
    pub connections: usize,
    pub accounts: usize,
    pub nonces: usize,
    pub registry_bytes: usize,
    pub retained_reply_bytes: usize,
    pub failure: Option<Fault>,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Broadcast {
    pub queued: usize,
    pub closed: usize,
}
struct Record {
    token: Weak<Token>,
    nonce: Option<Part>,
    last_command: u64,
    notifications: VecDeque<View<Chain>>,
    notification_bytes: usize,
}
struct Data {
    registry: Registry,
    nonces: Cache,
    connections: BTreeMap<u64, Record>,
    sequence: u64,
    maximum_workers: usize,
    failure: Option<Fault>,
    sources: Box<dyn Sources>,
}
impl Data {
    fn remove(&mut self, id: u64) {
        if let Some(r) = self.connections.remove(&id) {
            if let Some(t) = r.token.upgrade() {
                t.closed.store(true, Ordering::Release);
            }
            if let Some(n) = r.nonce {
                self.nonces.free(n);
            }
            self.registry.delete_connection(id);
        }
    }
    fn execute(&mut self, id: u64, operation: Operation) -> Result<Reply, Error> {
        Ok(match operation {
            Operation::AllocateNonce { bytes } => {
                if self.connections[&id].nonce.is_some() {
                    return Err(Error::Lease);
                }
                let source = &mut self.sources;
                let nonce = self
                    .nonces
                    .allocate(bytes, |maximum| {
                        let value = source.draw(maximum).map_err(|_| crate::Error::Entropy)?;
                        if value > maximum {
                            return Err(crate::Error::Entropy);
                        }
                        Ok(value)
                    })
                    .map_err(|e| {
                        if e == crate::Error::Entropy {
                            Error::Fault(Fault::Entropy)
                        } else {
                            Error::State(e)
                        }
                    })?;
                self.connections.get_mut(&id).expect("connection").nonce = nonce;
                Reply::Nonce(nonce)
            }
            Operation::ReleaseNonce { nonce } => {
                if self.connections[&id].nonce != Some(nonce) {
                    return Err(Error::Lease);
                }
                self.nonces.free(nonce);
                self.connections.get_mut(&id).expect("connection").nonce = None;
                Reply::Released
            }
            Operation::Membership { account } => {
                Reply::Membership(self.registry.membership(account.as_bytes(), id))
            }
            Operation::Register { user } => {
                let now = if self.registry.member_account(user.account.as_bytes())
                    || self.registry.member_connection(id)
                {
                    BigInt::from(0)
                } else {
                    self.sources.now().map_err(Error::Fault)?
                };
                Reply::Registered(self.registry.add(
                    user.account.as_bytes(),
                    user.worker.as_bytes(),
                    id,
                    now,
                )?)
            }
            Operation::AddWorker { user } => {
                let now = if self.registry.worker_status(
                    user.account.as_bytes(),
                    user.worker.as_bytes(),
                    self.maximum_workers,
                ) == WorkerRegistration::Added
                {
                    self.sources.now().map_err(Error::Fault)?
                } else {
                    BigInt::from(0)
                };
                Reply::Worker(self.registry.add_worker(
                    user.account.as_bytes(),
                    user.worker.as_bytes(),
                    self.maximum_workers,
                    now,
                )?)
            }
            Operation::DeleteConnection => {
                self.registry.delete_connection(id);
                Reply::Deleted
            }
        })
    }
}
struct Shared {
    data: Mutex<Data>,
    closed: AtomicBool,
    budget: Arc<Budget>,
    config: Config,
}
impl Shared {
    fn lock(&self) -> MutexGuard<'_, Data> {
        self.data.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn try_open(&self) -> Result<MutexGuard<'_, Data>, Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let data = match self.data.try_lock() {
            Ok(d) => d,
            Err(TryLockError::WouldBlock) => return Err(Error::Busy),
            Err(TryLockError::Poisoned(e)) => e.into_inner(),
        };
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok(data)
    }
    fn close_data(&self, data: &mut Data) {
        self.closed.store(true, Ordering::Release);
        while let Some(id) = data.connections.keys().next().copied() {
            data.remove(id);
        }
    }
    fn info(&self, data: &Data) -> Info {
        Info {
            closed: self.closed.load(Ordering::Acquire),
            connections: data.connections.len(),
            accounts: data.registry.len(),
            nonces: data.nonces.values().len(),
            registry_bytes: data.registry.retained_bytes(),
            retained_reply_bytes: self.budget.used.load(Ordering::Acquire),
            failure: data.failure.clone(),
        }
    }
}
struct Token {
    id: u64,
    shared: Arc<Shared>,
    closed: AtomicBool,
}
impl Token {
    fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) && !self.shared.closed.load(Ordering::Acquire)
        {
            self.shared.lock().remove(self.id);
        }
    }
}
impl Drop for Token {
    fn drop(&mut self) {
        self.close();
    }
}
#[derive(Clone)]
pub struct Connection(Arc<Token>);
impl Connection {
    pub fn id(&self) -> u64 {
        self.0.id
    }
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::Acquire) || self.0.shared.closed.load(Ordering::Acquire)
    }
    pub fn close(&self) {
        self.0.close();
    }
    pub fn execute(&self, command: session::Command) -> Result<session::Event, Error> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let shared = &self.0.shared;
        let mut data = shared.try_open()?;
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let record = data.connections.get(&self.id()).ok_or(Error::Closed)?;
        if command.id <= record.last_command {
            return Err(Error::Stale);
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let limit = shared.config.registry.field_bytes;
            let oversized = match &command.operation {
                Operation::Membership { account } => account.len() > limit,
                Operation::Register { user } | Operation::AddWorker { user } => {
                    user.account.len() > limit || user.worker.len() > limit
                }
                _ => false,
            };
            if oversized {
                return Err(Error::State(crate::Error::Limit("registry field")));
            }
            data.execute(self.id(), command.operation)
        }))
        .unwrap_or(Err(Error::Fault(Fault::Panicked)));
        match result {
            Ok(reply) => {
                data.connections
                    .get_mut(&self.id())
                    .expect("connection")
                    .last_command = command.id;
                Ok(session::Event::Complete {
                    id: command.id,
                    reply,
                })
            }
            Err(Error::Fault(f)) => {
                data.failure = Some(f.clone());
                shared.close_data(&mut data);
                Err(Error::Fault(f))
            }
            Err(e @ Error::State(_)) => {
                data.remove(self.id());
                Err(e)
            }
            Err(e) => Err(e),
        }
    }
    pub fn next_notification(&self) -> Result<Option<View<Chain>>, Error> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let mut data = self.0.shared.try_open()?;
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let r = data.connections.get_mut(&self.id()).ok_or(Error::Closed)?;
        let next = r.notifications.pop_front();
        if let Some(n) = &next {
            r.notification_bytes -= n.0._credit.bytes;
        }
        Ok(next)
    }
}
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
}
impl Client {
    pub fn info(&self) -> Info {
        self.shared.info(&self.shared.lock())
    }
    pub fn connect(&self) -> Result<Connection, Error> {
        let mut data = self.shared.try_open()?;
        if data.connections.len() >= self.shared.config.connections {
            return Err(Error::Limit);
        }
        let id = data.sequence.checked_add(1).ok_or(Error::Sequence)?;
        let connection = Connection(Arc::new(Token {
            id,
            shared: self.shared.clone(),
            closed: AtomicBool::new(false),
        }));
        data.connections.insert(
            id,
            Record {
                token: Arc::downgrade(&connection.0),
                nonce: None,
                last_command: 0,
                notifications: VecDeque::new(),
                notification_bytes: 0,
            },
        );
        data.sequence = id;
        Ok(connection)
    }
    pub fn set_max_workers(&self, maximum: usize) -> Result<(), Error> {
        if maximum > 1_000_000 {
            return Err(Error::Configuration);
        }
        self.shared.try_open()?.maximum_workers = maximum;
        Ok(())
    }
    pub fn delete_account(&self, account: &[u8]) -> Result<bool, Error> {
        Ok(self.shared.try_open()?.registry.delete_account(account))
    }
    pub fn member_account(&self, account: &[u8]) -> Result<bool, Error> {
        Ok(self.shared.try_open()?.registry.member_account(account))
    }
    pub fn member_connection(&self, connection: &Connection) -> Result<bool, Error> {
        self.owns(connection)?;
        Ok(self
            .shared
            .try_open()?
            .registry
            .member_connection(connection.id()))
    }
    pub fn registered_connections(&self) -> Result<View<Vec<u64>>, Error> {
        let data = self.shared.try_open()?;
        let credit = Credit::reserve(&self.shared.budget, 128 + data.registry.len() * 8)?;
        Ok(View::new(data.registry.connections().collect(), credit))
    }
    pub fn membership(&self, account: &[u8], connection: &Connection) -> Result<Membership, Error> {
        self.owns(connection)?;
        Ok(self
            .shared
            .try_open()?
            .registry
            .membership(account, connection.id()))
    }
    fn owns(&self, connection: &Connection) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.shared, &connection.0.shared) {
            return Err(Error::Stale);
        }
        if connection.is_closed() {
            return Err(Error::Closed);
        }
        Ok(())
    }
    pub fn find_account(&self, account: &[u8]) -> Result<Option<View<Account>>, Error> {
        let data = self.shared.try_open()?;
        data.registry
            .find_account(account)
            .map(|e| self.view(account, e))
            .transpose()
    }
    pub fn find_connection(&self, connection: &Connection) -> Result<Option<View<Account>>, Error> {
        self.owns(connection)?;
        let data = self.shared.try_open()?;
        data.registry
            .find_connection(connection.id())
            .map(|(a, e)| self.view(a, e))
            .transpose()
    }
    fn view(&self, account: &[u8], entry: &registry::Entry) -> Result<View<Account>, Error> {
        let credit = Credit::reserve(
            &self.shared.budget,
            account.len() + entry.retained_bytes() + 128,
        )?;
        Ok(View::new(
            Account {
                account: account.to_vec(),
                entry: entry.clone(),
            },
            credit,
        ))
    }
    pub fn broadcast(&self, event: Chain) -> Result<Broadcast, Error> {
        let bytes = chain_bytes(&event)?;
        let mut data = self.shared.try_open()?;
        let ids = data.registry.connections().collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(Broadcast::default());
        }
        let credit = Credit::reserve(&self.shared.budget, bytes)?;
        let event = View::new(event, credit);
        let mut result = Broadcast::default();
        for id in ids {
            let r = data
                .connections
                .get_mut(&id)
                .expect("registered connection");
            if r.notifications.len() >= self.shared.config.notifications_per_connection
                || bytes
                    > self
                        .shared
                        .config
                        .notification_bytes_per_connection
                        .saturating_sub(r.notification_bytes)
            {
                data.remove(id);
                result.closed += 1;
            } else {
                r.notifications.push_back(event.clone());
                r.notification_bytes += bytes;
                result.queued += 1;
            }
        }
        Ok(result)
    }
}
fn chain_bytes(event: &Chain) -> Result<usize, Error> {
    let integers = match event {
        Chain::SetTarget => Vec::new(),
        Chain::Block(w) => vec![&w.block_target],
        Chain::Notify(i) => {
            if i.id.len() > 4096 {
                return Err(Error::Limit);
            }
            vec![
                &i.work.block_target,
                &i.share_target,
                &i.desired,
                &i.maximum,
            ]
        }
    };
    for n in &integers {
        crate::integer(n)?;
    }
    Ok(512
        + integers
            .iter()
            .map(|n| n.bits().div_ceil(8) as usize)
            .sum::<usize>()
        + match event {
            Chain::Notify(i) => i.id.len(),
            _ => 0,
        })
}
pub struct Owner {
    client: Client,
}
impl Owner {
    pub fn new(config: Config) -> Result<Self, Error> {
        Self::with_sources(config, System)
    }
    pub fn with_sources(config: Config, sources: impl Sources + 'static) -> Result<Self, Error> {
        config.validate()?;
        let registry = Registry::new(config.registry)?;
        let nonces = Cache::new(config.connections)?;
        let data = Data {
            registry,
            nonces,
            connections: BTreeMap::new(),
            sequence: 0,
            maximum_workers: config.max_workers,
            failure: None,
            sources: Box::new(sources),
        };
        let budget = Arc::new(Budget {
            used: AtomicUsize::new(0),
            maximum: config.retained_reply_bytes,
        });
        Ok(Self {
            client: Client {
                shared: Arc::new(Shared {
                    data: Mutex::new(data),
                    closed: AtomicBool::new(false),
                    budget,
                    config,
                }),
            },
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn shutdown(self) -> Info {
        self.close();
        self.client.info()
    }
    fn close(&self) {
        self.client.shared.closed.store(true, Ordering::Release);
        let mut data = self.client.shared.lock();
        self.client.shared.close_data(&mut data);
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.close();
    }
}
#[cfg(test)]
mod tests;
