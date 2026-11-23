// Independent aec_hc_vote_pool state and transient aec_events delivery.
// Pool replacement, EOE subscriptions and publication ownership have distinct
// lifetimes. The gate protects only bounded in-memory operations, never I/O.
use super::*;
use ae_mempool::hc_votes::{self, PushOutcome, VotePool};
use ae_protocol::{ConsensusType, NetworkProfile, Protocol};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

mod wake;
pub use wake::Wake;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub maximum_publications: usize,
    pub maximum_publication_bytes: usize,
    pub maximum_inputs: usize,
    pub maximum_input_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            maximum_publications: 4096,
            maximum_publication_bytes: 64 << 20,
            maximum_inputs: 4096,
            maximum_input_bytes: 64 << 20,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Closed,
    ResourceLimit,
    Admission(hc_votes::Error),
}
#[derive(Debug)]
struct Budget {
    maximum: usize,
    used: AtomicUsize,
}
impl Budget {
    fn new(maximum: usize) -> Arc<Self> {
        Arc::new(Self {
            maximum,
            used: AtomicUsize::new(0),
        })
    }
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Charge, Error> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|n| *n <= self.maximum)
            })
            .map_err(|_| Error::ResourceLimit)?;
        Ok(Charge {
            budget: self.clone(),
            bytes,
        })
    }
}
#[derive(Debug)]
struct Charge {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
#[derive(Debug)]
struct Published {
    signed: SignedTransaction<Transaction>,
    event: hc_votes::Event,
    _charge: Charge,
}
// A read or clone keeps the publication's byte reservation until it is dropped.
#[derive(Debug, Clone)]
pub struct Publication(Arc<Published>);
impl Publication {
    pub fn signed(&self) -> &SignedTransaction<Transaction> {
        &self.0.signed
    }
    pub fn event(&self) -> hc_votes::Event {
        self.0.event
    }
}
#[derive(Debug, Clone)]
pub enum Event {
    Vote(Publication),
    NewEpoch(Unsigned),
}
#[derive(Debug)]
struct Delivered {
    event: Event,
    generations: [u64; 2],
    recipient: Arc<()>,
    _charge: Charge,
}
#[derive(Debug, Clone)]
pub struct Input(Arc<Delivered>);
impl Input {
    pub fn event(&self) -> &Event {
        &self.0.event
    }
    pub fn generations(&self) -> [u64; 2] {
        self.0.generations
    }
}

struct Pool {
    generation: u64,
    votes: VotePool,
    limits: hc_votes::Limits,
}
struct Subscriber {
    generation: u64,
    children: [u64; 2],
    identity: Arc<()>,
    queue: VecDeque<Input>,
    budget: Arc<Budget>,
    wake: Wake,
    shutdown: Option<Arc<AtomicBool>>,
}
#[derive(Default)]
struct State {
    closed: bool,
    sequence: u64,
    pool: Option<Pool>,
    subscriber: Option<Subscriber>,
    publications: VecDeque<Publication>,
    publication_owner: Option<u64>,
}
struct Shared {
    state: Mutex<State>,
    config: Config,
    network: NetworkProfile,
    publications: Arc<Budget>,
}
#[derive(Clone)]
pub struct Hub(Arc<Shared>);
impl Hub {
    pub fn new(network: NetworkProfile, config: Config) -> Result<Self, Error> {
        if !(1..=65536).contains(&config.maximum_publications)
            || !(1..=65536).contains(&config.maximum_inputs)
            || !(1..=1 << 30).contains(&config.maximum_publication_bytes)
            || !(1..=1 << 30).contains(&config.maximum_input_bytes)
        {
            return Err(Error::Configuration);
        }
        Ok(Self(Arc::new(Shared {
            state: Mutex::new(State::default()),
            network,
            publications: Budget::new(config.maximum_publication_bytes),
            config,
        })))
    }
    fn state(&self) -> MutexGuard<'_, State> {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn config(&self) -> Config {
        self.0.config
    }
    pub fn is_closed(&self) -> bool {
        self.state().closed
    }
    // Terminal loss of the authoritative epoch source closes admission and
    // wakes the currently subscribed runtime. Accepted outputs remain owned.
    pub fn close(&self) {
        let mut state = self.state();
        state.closed = true;
        let wake = state.subscriber.as_ref().map(|subscriber| {
            if let Some(stop) = &subscriber.shutdown {
                stop.store(true, Ordering::Release);
            }
            subscriber.wake.clone()
        });
        drop(state);
        if let Some(wake) = wake {
            wake.notify();
        }
    }
    pub fn network(&self) -> &NetworkProfile {
        &self.0.network
    }
    pub fn same_hub(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn start_pool(&self, limits: hc_votes::Limits) -> Result<Owner, Error> {
        let mut state = self.state();
        if state.closed {
            return Err(Error::Closed);
        }
        if state.pool.is_some() {
            return Err(Error::Busy);
        }
        let generation = next(&mut state)?;
        state.pool = Some(Pool {
            generation,
            votes: VotePool::default(),
            limits,
        });
        Ok(Owner {
            client: Client {
                hub: self.clone(),
                generation,
            },
            live: true,
        })
    }
    pub fn client(&self) -> Option<Client> {
        let state = self.state();
        if state.closed {
            return None;
        }
        state.pool.as_ref().map(|pool| Client {
            hub: self.clone(),
            generation: pool.generation,
        })
    }
    pub fn subscribe(&self, wake: Wake) -> Result<Inbox, Error> {
        let mut state = self.state();
        if state.closed {
            return Err(Error::Closed);
        }
        if state.subscriber.is_some() {
            return Err(Error::Busy);
        }
        let generation = next(&mut state)?;
        state.subscriber = Some(Subscriber {
            generation,
            children: [0; 2],
            identity: Arc::new(()),
            queue: VecDeque::new(),
            budget: Budget::new(self.0.config.maximum_input_bytes),
            wake,
            shutdown: None,
        });
        Ok(Inbox {
            hub: self.clone(),
            generation,
        })
    }
    pub fn publications(&self) -> Result<Publications, Error> {
        let mut state = self.state();
        if state.publication_owner.is_some() {
            return Err(Error::Busy);
        }
        let generation = next(&mut state)?;
        state.publication_owner = Some(generation);
        Ok(Publications {
            hub: self.clone(),
            generation,
        })
    }
    pub fn retained_publication_bytes(&self) -> usize {
        self.0.publications.used.load(Ordering::Acquire)
    }
    // A committed epoch is an event, independently observed by the pool and
    // EOE subscribers. Absent children miss it; a new child gets no replay.
    pub fn new_epoch(&self, epoch: Unsigned) -> Result<(), Error> {
        let bytes = epoch
            .as_bytes()
            .len()
            .checked_add(128)
            .ok_or(Error::ResourceLimit)?;
        if bytes > self.0.config.maximum_input_bytes {
            return Err(Error::ResourceLimit);
        }
        let mut state = self.state();
        if state.closed {
            return Err(Error::Closed);
        }
        let input = if let Some(subscriber) = &state.subscriber {
            if subscriber.queue.len() >= self.0.config.maximum_inputs {
                return Err(Error::ResourceLimit);
            }
            Some(Input(Arc::new(Delivered {
                event: Event::NewEpoch(epoch.clone()),
                generations: subscriber.children,
                recipient: subscriber.identity.clone(),
                _charge: subscriber.budget.reserve(bytes)?,
            })))
        } else {
            None
        };
        if let Some(pool) = &mut state.pool {
            pool.votes.update_epoch(epoch);
        }
        let wake = if let Some(input) = input {
            let subscriber = state
                .subscriber
                .as_mut()
                .expect("reserved epoch subscriber");
            subscriber.queue.push_back(input);
            Some(subscriber.wake.clone())
        } else {
            None
        };
        drop(state);
        if let Some(wake) = wake {
            wake.notify();
        }
        Ok(())
    }
}
fn next(state: &mut State) -> Result<u64, Error> {
    state.sequence = state.sequence.checked_add(1).ok_or(Error::ResourceLimit)?;
    Ok(state.sequence)
}

pub struct Owner {
    client: Client,
    live: bool,
}
impl Owner {
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn retire(mut self) -> VotePool {
        self.live = false;
        self.client
            .hub
            .state()
            .pool
            .take()
            .expect("owned pool generation")
            .votes
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        if self.live {
            self.client.hub.state().pool.take();
        }
    }
}
#[derive(Clone)]
pub struct Client {
    hub: Hub,
    generation: u64,
}
#[derive(Clone)]
pub struct Admission {
    pub signed: SignedTransaction<Transaction>,
    pub event: hc_votes::Event,
    pub protocol: Protocol,
    pub consensus: ConsensusType,
}
impl Client {
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn same_pool(&self, other: &Self) -> bool {
        self.generation == other.generation && self.hub.same_hub(&other.hub)
    }
    pub fn is_closed(&self) -> bool {
        let state = self.hub.state();
        state.closed
            || state
                .pool
                .as_ref()
                .is_none_or(|p| p.generation != self.generation)
    }
    // The inspector only borrows immutable state. Node callers use it for
    // bounded reply capture and do not reenter the hub or perform I/O.
    pub fn inspect<T>(&self, inspect: impl FnOnce(&VotePool) -> T) -> Result<T, Error> {
        let state = self.hub.state();
        if state.closed {
            return Err(Error::Closed);
        }
        let pool = state
            .pool
            .as_ref()
            .filter(|p| p.generation == self.generation)
            .ok_or(Error::Closed)?;
        Ok(inspect(&pool.votes))
    }
    pub fn admit(&self, admission: Admission) -> Result<PushOutcome, Error> {
        Ok(self
            .admit_batch(vec![admission])?
            .pop()
            .expect("one admission"))
    }
    // A callback stages all local vote effects before committing. Capacity or
    // validation failure leaves the pool, both outboxes and all credits intact.
    // Cache admission and selected-state capture precede this pure operation.
    pub fn admit_batch(&self, admissions: Vec<Admission>) -> Result<Vec<PushOutcome>, Error> {
        self.hub.commit(Some(self), admissions, None)
    }
    pub(super) fn snapshot(&self) -> Result<(VotePool, hc_votes::Limits), Error> {
        let state = self.hub.state();
        if state.closed {
            return Err(Error::Closed);
        }
        let pool = state
            .pool
            .as_ref()
            .filter(|p| p.generation == self.generation)
            .ok_or(Error::Closed)?;
        Ok((pool.votes.clone(), pool.limits))
    }
}
#[derive(Clone)]
pub(super) struct Delivery {
    hub: Hub,
    generation: u64,
    identity: Arc<()>,
}
impl Delivery {
    pub(super) fn accepts(&self, input: &Input) -> bool {
        Arc::ptr_eq(&self.identity, &input.0.recipient)
    }
}
impl Hub {
    // Publication and the inner-worker generation change share one boundary.
    // Pre-exception votes target the old children; later input targets the new
    // children. Neither mailbox is replayed after its recipient has exited.
    pub(super) fn commit(
        &self,
        client: Option<&Client>,
        admissions: Vec<Admission>,
        delivery: Option<(&Delivery, [u64; 2])>,
    ) -> Result<Vec<PushOutcome>, Error> {
        let mut state = self.state();
        if state.closed {
            return Err(Error::Closed);
        }
        if let Some((delivery, _)) = delivery
            && (!self.same_hub(&delivery.hub)
                || state
                    .subscriber
                    .as_ref()
                    .is_none_or(|s| s.generation != delivery.generation))
        {
            return Err(Error::Closed);
        }
        let selected = match client {
            Some(client) => Some(
                state
                    .pool
                    .as_ref()
                    .filter(|p| self.same_hub(&client.hub) && p.generation == client.generation)
                    .ok_or(Error::Closed)?,
            ),
            None if admissions.is_empty() => None,
            None => return Err(Error::Closed),
        };
        let count = admissions.len();
        let config = self.0.config;
        if state.publications.len().saturating_add(count) > config.maximum_publications
            || state
                .subscriber
                .as_ref()
                .is_some_and(|s| s.queue.len().saturating_add(count) > config.maximum_inputs)
        {
            return Err(Error::ResourceLimit);
        }
        let mut staged = if count == 0 {
            None
        } else {
            let pool = selected.expect("admission pool");
            Some((pool.votes.clone(), pool.limits))
        };
        let mut outcomes = Vec::with_capacity(count);
        let mut outputs = Vec::with_capacity(count);
        let mut inputs = Vec::with_capacity(count);
        for admission in admissions {
            let (votes, limits) = staged.as_mut().expect("staged pool");
            let result = votes
                .push(
                    &admission.signed,
                    admission.event,
                    &hc_votes::Admission {
                        network: &self.0.network,
                        protocol: admission.protocol,
                        consensus: admission.consensus,
                        limits: *limits,
                    },
                    |_| false,
                )
                .map_err(Error::Admission)?;
            let bytes = admission
                .signed
                .encode()
                .len()
                .checked_add(128)
                .ok_or(Error::ResourceLimit)?;
            let publication = Publication(Arc::new(Published {
                signed: admission.signed,
                event: admission.event,
                _charge: self.0.publications.reserve(bytes)?,
            }));
            if let Some(subscriber) = &state.subscriber {
                inputs.push(Input(Arc::new(Delivered {
                    event: Event::Vote(publication.clone()),
                    generations: subscriber.children,
                    recipient: subscriber.identity.clone(),
                    _charge: subscriber.budget.reserve(bytes)?,
                })));
            }
            outcomes.push(result);
            outputs.push(publication);
        }
        if let Some((votes, _)) = staged {
            state.pool.as_mut().expect("captured live pool").votes = votes;
        }
        state.publications.extend(outputs);
        let wake = if let Some(subscriber) = &mut state.subscriber {
            let wake = (!inputs.is_empty()).then(|| subscriber.wake.clone());
            subscriber.queue.extend(inputs);
            if let Some((_, children)) = delivery {
                subscriber.children = children;
            }
            wake
        } else {
            None
        };
        drop(state);
        if let Some(wake) = wake {
            wake.notify();
        }
        Ok(outcomes)
    }
}

pub struct Inbox {
    hub: Hub,
    generation: u64,
}
#[derive(Debug)]
pub struct RetiredInbox {
    pub generation: u64,
    pub pending: Vec<Input>,
}
impl Inbox {
    pub(super) fn shutdown_with(&self, shutdown: Arc<AtomicBool>) {
        let mut state = self.hub.state();
        if state.closed {
            shutdown.store(true, Ordering::Release);
        }
        if let Some(subscriber) = state
            .subscriber
            .as_mut()
            .filter(|s| s.generation == self.generation)
        {
            subscriber.shutdown = Some(shutdown);
        }
    }
    pub(super) fn delivery(&self) -> Delivery {
        Delivery {
            hub: self.hub.clone(),
            generation: self.generation,
            identity: self
                .hub
                .state()
                .subscriber
                .as_ref()
                .expect("owned subscriber")
                .identity
                .clone(),
        }
    }
    pub fn peek(&self) -> Option<Input> {
        self.hub
            .state()
            .subscriber
            .as_ref()
            .filter(|s| s.generation == self.generation)
            .and_then(|s| s.queue.front().cloned())
    }
    pub fn acknowledge(&mut self) -> Option<Input> {
        self.hub
            .state()
            .subscriber
            .as_mut()
            .filter(|s| s.generation == self.generation)
            .and_then(|s| s.queue.pop_front())
    }
    pub fn retained_bytes(&self) -> usize {
        self.hub
            .state()
            .subscriber
            .as_ref()
            .filter(|s| s.generation == self.generation)
            .map_or(0, |s| s.budget.used.load(Ordering::Acquire))
    }
    pub fn retire(self) -> RetiredInbox {
        let pending = self
            .hub
            .state()
            .subscriber
            .take()
            .expect("owned subscriber")
            .queue
            .into();
        RetiredInbox {
            generation: self.generation,
            pending,
        }
    }
}
impl Drop for Inbox {
    fn drop(&mut self) {
        let mut state = self.hub.state();
        if state
            .subscriber
            .as_ref()
            .is_some_and(|s| s.generation == self.generation)
        {
            state.subscriber = None;
        }
    }
}
pub struct Publications {
    hub: Hub,
    generation: u64,
}
impl Publications {
    pub fn peek(&self) -> Option<Publication> {
        self.hub.state().publications.front().cloned()
    }
    pub fn acknowledge(&mut self) -> Option<Publication> {
        self.hub.state().publications.pop_front()
    }
}
impl Drop for Publications {
    fn drop(&mut self) {
        let mut state = self.hub.state();
        if state.publication_owner == Some(self.generation) {
            state.publication_owner = None;
        }
    }
}
