use crate::{bindings, owned};
use ae_mempool::{hc_votes as policy, pool::Signed};
use ae_protocol::{ConsensusType, NetworkProfile, Protocol};
use ae_storage::{Database, pool::worker::GossipCache};
use ae_tx::HcVote;
use ae_types::Unsigned;
use std::sync::{Arc, Weak};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

mod publications;
pub use publications::{Consumer, Publication, Publications};

pub use crate::owned::{Fault, Limits as QueueLimits};
pub type Info = owned::Info<Error>;
pub type Ticket<T> = owned::Ticket<T, Error>;
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub pool: policy::Limits,
    pub queue: QueueLimits,
    pub events: usize,
    pub event_bytes: usize,
    pub reply_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            pool: Default::default(),
            queue: Default::default(),
            events: 64,
            event_bytes: 64 << 20,
            reply_bytes: 64 << 20,
        }
    }
}
impl Config {
    fn validate(self) -> Result<(), Error> {
        if self.events == 0
            || self.events > 65536
            || self.event_bytes == 0
            || self.event_bytes > 1 << 30
            || self.reply_bytes == 0
            || self.reply_bytes > 1 << 30
            || self.pool.maximum_entries > 1_000_000
            || self.pool.maximum_retained_wire_bytes > 1 << 30
            || self.pool.maximum_encoded_bytes == 0
            || self.pool.maximum_encoded_bytes > 1 << 30
            || self.pool.maximum_encoding_work_bytes == 0
            || self.pool.maximum_encoding_work_bytes > 1 << 30
        {
            return Err(Fault::Configuration.into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Worker(Fault),
    Pool(policy::Error),
    Protocol,
    Storage,
}
impl From<Fault> for Error {
    fn from(e: Fault) -> Self {
        Self::Worker(e)
    }
}
impl From<policy::Error> for Error {
    fn from(e: policy::Error) -> Self {
        Self::Pool(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug)]
pub struct Stats {
    pub epoch: Unsigned,
    pub entries: usize,
    pub retained_wire_bytes: usize,
    _bytes: OwnedSemaphorePermit,
}
// The reply retains its byte charge until the caller drops it, including while
// an already completed ticket remains unread.
#[derive(Debug)]
pub struct Batch {
    votes: Vec<HcVote>,
    _bytes: OwnedSemaphorePermit,
}
impl std::ops::Deref for Batch {
    type Target = [HcVote];
    fn deref(&self) -> &Self::Target {
        &self.votes
    }
}
pub(crate) fn snapshot(
    pool: &policy::VotePool,
    epoch: &Unsigned,
    kind: Option<&Unsigned>,
    maximum: usize,
    replies: &Arc<Semaphore>,
) -> Result<Batch, Error> {
    let weight = pool
        .peek(epoch, kind)
        .take(maximum)
        .try_fold(0usize, |sum, v| {
            sum.checked_add(v.wire_size())
                .and_then(|n| n.checked_add(128))
        })
        .ok_or(Fault::Busy)?;
    let amount = u32::try_from(weight).map_err(|_| Fault::Busy)?;
    let bytes = replies
        .clone()
        .try_acquire_many_owned(amount)
        .map_err(|_| Fault::Busy)?;
    Ok(Batch {
        votes: pool.peek(epoch, kind).take(maximum).cloned().collect(),
        _bytes: bytes,
    })
}
struct State {
    database: Arc<Database>,
    network: NetworkProfile,
    cache: GossipCache,
    config: Config,
    pool: policy::VotePool,
    decoding: ae_chain::repository::ReadConfig,
    replies: Arc<Semaphore>,
    _claim: publications::Claim,
}
impl owned::Actor for State {
    type Error = Error;
}
#[derive(Clone)]
pub struct Client {
    inner: owned::Client<State>,
    database: Weak<Database>,
    publications: Publications,
    config: Config,
}
impl Client {
    pub fn info(&self) -> Info {
        self.inner.info()
    }
    pub fn publications(&self) -> Publications {
        self.publications.clone()
    }
    pub fn uses_database(&self, db: &Arc<Database>) -> bool {
        self.database.ptr_eq(&Arc::downgrade(db))
    }
    pub fn push(
        &self,
        bytes: Vec<u8>,
        event: policy::Event,
    ) -> Result<Ticket<policy::PushOutcome>, Error> {
        if !self.info().accepting {
            return Err(Fault::Stopped.into());
        }
        if bytes.len() > self.config.pool.maximum_encoded_bytes {
            return Err(policy::Error::ResourceLimit.into());
        }
        let weight = bytes.len().checked_add(128).ok_or(Fault::Busy)?;
        let reservation = self.publications.reserve(weight)?;
        self.inner.submit(weight, move |s| {
            let config = s.decoding.transaction_record();
            let tx = Arc::new(
                Signed::decode_with_state_config(&bytes, config.transaction, config.state)
                    .map_err(|_| Error::Protocol)?,
            );
            let hash = policy::admission_hash(&tx, s.config.pool)?;
            if s.cache.check(hash) {
                return Ok(policy::PushOutcome::Cached);
            }
            let view = s.database.view().map_err(|_| Error::Storage)?;
            let protocol = match view.status().tip {
                Some(tip) => Protocol::try_from(
                    view.require_block(tip)
                        .map_err(|_| Error::Storage)?
                        .header()
                        .common()
                        .version,
                )
                .map_err(|_| Error::Storage)?,
                None => s.network.protocol_at(0).map_err(|_| Error::Storage)?,
            };
            let result = s.pool.push(
                &tx,
                event,
                &policy::Admission {
                    network: &s.network,
                    protocol,
                    consensus: ConsensusType::Pos,
                    limits: s.config.pool,
                },
                |_| false,
            )?;
            if matches!(result, policy::PushOutcome::Accepted { .. }) {
                reservation.send(tx, event);
            }
            Ok(result)
        })
    }
    pub fn advance_epoch(&self, epoch: Unsigned) -> Result<Ticket<()>, Error> {
        let weight = epoch.as_bytes().len().checked_add(128).ok_or(Fault::Busy)?;
        self.inner.submit(weight, move |s| {
            s.pool.update_epoch(epoch);
            Ok(())
        })
    }
    pub fn peek(
        &self,
        epoch: Unsigned,
        kind: Option<Unsigned>,
        maximum: usize,
    ) -> Result<Ticket<Batch>, Error> {
        if maximum > self.config.pool.maximum_entries {
            return Err(Fault::Busy.into());
        }
        let weight = epoch
            .as_bytes()
            .len()
            .saturating_add(kind.as_ref().map_or(0, |v| v.as_bytes().len()))
            .saturating_add(128);
        self.inner.submit(weight, move |s| {
            snapshot(&s.pool, &epoch, kind.as_ref(), maximum, &s.replies)
        })
    }
    pub fn stats(&self) -> Result<Ticket<Stats>, Error> {
        self.inner.submit(128, |s| {
            let weight = s
                .pool
                .epoch()
                .as_bytes()
                .len()
                .checked_add(128)
                .ok_or(Fault::Busy)?;
            let bytes = s
                .replies
                .clone()
                .try_acquire_many_owned(u32::try_from(weight).map_err(|_| Fault::Busy)?)
                .map_err(|_| Fault::Busy)?;
            Ok(Stats {
                epoch: s.pool.epoch().clone(),
                entries: s.pool.len(),
                retained_wire_bytes: s.pool.retained_wire_bytes(),
                _bytes: bytes,
            })
        })
    }
    #[cfg(test)]
    pub(crate) fn fail(&self) {
        drop(
            self.inner
                .submit::<()>(0, |_| panic!("injected vote pool failure"))
                .unwrap(),
        );
    }
}
pub struct Worker {
    inner: owned::Worker<State>,
    client: Client,
}
impl Worker {
    pub fn spawn(
        database: Arc<Database>,
        network: NetworkProfile,
        cache: GossipCache,
        config: Config,
        publications: Publications,
    ) -> Result<Self, Error> {
        config.validate()?;
        if !publications.uses_database(&database) {
            return Err(Fault::Configuration.into());
        }
        let view = database.view().map_err(|_| Error::Storage)?;
        if !view.identity().matches(&network) {
            return Err(Fault::Configuration.into());
        }
        let decoding = view.config().clone();
        drop(view);
        let weak = Arc::downgrade(&database);
        let replies = publications.replies(config)?;
        let claim = publications.claim()?;
        let inner = owned::Worker::spawn("ae-hc-vote-pool", config.queue, move || {
            Ok(State {
                database,
                network,
                cache,
                config,
                pool: Default::default(),
                decoding,
                replies,
                _claim: claim,
            })
        })?;
        let client = Client {
            inner: inner.client(),
            database: weak,
            publications,
            config,
        };
        Ok(Self { inner, client })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
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
pub type Binding = bindings::Binding<Client>;

pub(crate) async fn call<T>(
    binding: &bindings::Binding<Client>,
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
