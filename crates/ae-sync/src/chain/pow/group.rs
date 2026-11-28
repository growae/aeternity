// Actual PoW children under aec_conductor_sup's one-for-all policy,
// aeternity fa2a6d7. ISC copyright: repository LICENSE.
use super::{generator, packing, producer};
use crate::{chain, keys, production_group as supervisor};
use ae_execution::transactions::owned::Owned;
use ae_storage::{pool::worker as pool, pow::GroupLease, snapshots};
use ae_vm_fate::maps::MapOrder;
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
mod factory;
mod retirement;
mod service;
pub use retirement::{GeneratorExit, ProducerExit, Released, Report, Retired, Retirement};

pub trait Clocks: Send + Sync + 'static {
    fn generator(&self, generation: u64) -> Arc<dyn generator::Clock>;
    fn producer(&self, generation: u64) -> Box<dyn producer::Clock>;
    fn preparation(&self, generation: u64) -> Arc<dyn generator::Clock> {
        self.generator(generation)
    }
    fn commands(&self, generation: u64) -> Arc<dyn generator::Clock> {
        self.generator(generation)
    }
    fn second(&self) -> u64;
}
pub struct SystemClocks(Instant);
impl Default for SystemClocks {
    fn default() -> Self {
        Self(Instant::now())
    }
}
impl Clocks for SystemClocks {
    fn generator(&self, _: u64) -> Arc<dyn generator::Clock> {
        Arc::new(generator::SystemClock)
    }
    fn producer(&self, _: u64) -> Box<dyn producer::Clock> {
        Box::new(producer::SystemClock)
    }
    fn second(&self) -> u64 {
        self.0.elapsed().as_secs()
    }
}
#[derive(Clone)]
pub struct Config<O> {
    pub generator: generator::Config<O>,
    pub producer: producer::Config,
    pub mining: Option<producer::mining::Config>,
    pub poll: Duration,
    pub outputs: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Binding,
    Closed,
    Sequence,
    Ownership(snapshots::Error),
    Spawn(String),
    Panicked,
    Startup(supervisor::Member),
    Supervision(supervisor::Error),
    MissingRetirement(supervisor::Member),
    Packing(packing::Error),
    Producer(producer::Error),
    Commands(chain::on_demand::service::Error),
}
#[derive(Debug)]
pub enum StartError {
    Generator(generator::Error),
    Producer(producer::Error),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationId {
    pub generator: u64,
    pub producer: u64,
}
pub struct Generation<O> {
    pub id: GenerationId,
    pub generator: generator::Client<O>,
    pub producer: producer::Client,
}
impl<O> Clone for Generation<O> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            generator: self.generator.clone(),
            producer: self.producer.clone(),
        }
    }
}
pub enum Event {
    Ready(GenerationId),
    Produced {
        generation: GenerationId,
        output: Arc<producer::Output>,
    },
    Command {
        generation: GenerationId,
        completion: Arc<chain::on_demand::service::Completion>,
    },
    Retired(Box<Retired>),
    StartFailed(supervisor::StartFailure<StartError>),
}
pub struct Output {
    pub sequence: u64,
    pub event: Event,
    _credit: Credit,
}
struct Credit(Arc<Mutex<usize>>);
impl Drop for Credit {
    fn drop(&mut self) {
        *lock(&self.0) -= 1;
    }
}
#[derive(Debug, Clone)]
pub struct Status {
    pub running: bool,
    pub accepting: bool,
    pub supervisor: supervisor::Status,
    pub requested_enabled: bool,
    pub request_revision: u64,
    pub applied_revision: Option<u64>,
    pub retiring: bool,
    pub published: u64,
    pub failure: Option<Error>,
}
struct State<O> {
    status: Status,
    shutdown: bool,
    current: Option<Generation<O>>,
    outputs: VecDeque<Arc<Output>>,
}
struct Shared<O> {
    database: Arc<ae_storage::Database>,
    state: Mutex<State<O>>,
    wake: Condvar,
    credits: Arc<Mutex<usize>>,
    limit: usize,
    start_allowed: bool,
}
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
pub struct Client<O>(Arc<Shared<O>>);
impl<O> Clone for Client<O> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<O> Client<O> {
    pub fn uses_database(&self, database: &Arc<ae_storage::Database>) -> bool {
        Arc::ptr_eq(&self.0.database, database)
    }
    pub fn uses_snapshots(&self, snapshots: &snapshots::Owner) -> bool {
        snapshots.uses_database(&self.0.database)
    }
    pub fn status(&self) -> Status {
        lock(&self.0.state).status.clone()
    }
    pub fn current(&self) -> Option<Generation<O>> {
        lock(&self.0.state).current.clone()
    }
    // Desired state survives pair replacement. Fence the published producer
    // synchronously, even when the supervisor is blocked joining another child.
    pub fn set_enabled(&self, enabled: bool) -> Result<u64, Error> {
        let mut s = lock(&self.0.state);
        if !s.status.accepting {
            return Err(Error::Closed);
        }
        if enabled && !s.status.requested_enabled && !self.0.start_allowed {
            return Err(Error::Producer(producer::Error::BeneficiaryNotConfigured));
        }
        if s.status.requested_enabled != enabled {
            s.status.request_revision = s
                .status
                .request_revision
                .checked_add(1)
                .ok_or(Error::Sequence)?;
            s.status.requested_enabled = enabled;
            s.status.applied_revision = None;
        }
        if let Some(current) = &s.current {
            match current.producer.set_enabled(enabled) {
                Ok(_) | Err(producer::Error::Closed) => (),
                Err(e) => return Err(Error::Producer(e)),
            }
        }
        self.0.wake.notify_all();
        Ok(s.status.request_revision)
    }
    pub fn front(&self) -> Option<Arc<Output>> {
        lock(&self.0.state).outputs.front().cloned()
    }
    pub fn acknowledge(&self, output: &Arc<Output>) -> bool {
        let mut s = lock(&self.0.state);
        if s.outputs.front().is_some_and(|v| Arc::ptr_eq(v, output)) {
            s.outputs.pop_front();
            self.0.wake.notify_all();
            true
        } else {
            false
        }
    }
    pub fn request_shutdown(&self) {
        let mut s = lock(&self.0.state);
        s.status.accepting = false;
        s.shutdown = true;
        if let Some(current) = &s.current {
            current.producer.request_shutdown();
        }
        self.0.wake.notify_all();
    }
}
pub struct Worker<O> {
    client: Client<O>,
    thread: Option<JoinHandle<Report>>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Worker<O> {
    pub fn spawn(
        importer: &chain::Importer<O>,
        snapshots: snapshots::Owner,
        pool: pool::Client,
        keys: keys::Client,
        config: Config<O>,
        clocks: Arc<dyn Clocks>,
    ) -> Result<Self, Error> {
        Self::spawn_inner(importer, snapshots, pool, keys, config, clocks, None, None)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_owned(
        importer: &chain::Importer<O>,
        snapshots: snapshots::Owner,
        pool: pool::Client,
        keys: keys::Client,
        config: Config<O>,
        clocks: Arc<dyn Clocks>,
        execution: Arc<Owned<O>>,
    ) -> Result<Self, Error> {
        Self::spawn_inner(
            importer,
            snapshots,
            pool,
            keys,
            config,
            clocks,
            Some(execution),
            None,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_owned_with_commands(
        importer: &chain::Importer<O>,
        snapshots: snapshots::Owner,
        pool: pool::Client,
        keys: keys::Client,
        config: Config<O>,
        clocks: Arc<dyn Clocks>,
        execution: Arc<Owned<O>>,
        commands: chain::on_demand::Config,
    ) -> Result<Self, Error> {
        Self::spawn_inner(
            importer,
            snapshots,
            pool,
            keys,
            config,
            clocks,
            Some(execution),
            Some(commands),
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn spawn_inner(
        importer: &chain::Importer<O>,
        snapshots: snapshots::Owner,
        pool: pool::Client,
        keys: keys::Client,
        config: Config<O>,
        clocks: Arc<dyn Clocks>,
        execution: Option<Arc<Owned<O>>>,
        commands: Option<chain::on_demand::Config>,
    ) -> Result<Self, Error> {
        if execution.as_ref().is_some_and(|e| {
            e.network() != importer.network() || e.consensus() != importer.consensus()
        }) {
            return Err(Error::Binding);
        }
        if config.poll.is_zero()
            || config.poll > Duration::from_secs(1)
            || !(1..=128).contains(&config.outputs)
        {
            return Err(Error::Configuration);
        }
        if importer.consensus() != ae_protocol::ConsensusType::Pow
            || config.generator.preparation.micro_cycle_ms != config.producer.micro_cycle_ms
            || (commands.is_some()
                && importer.algorithm() != ae_protocol::ConsensusAlgorithm::OnDemand)
        {
            return Err(Error::Binding);
        }
        let start_allowed = commands
            .as_ref()
            .is_none_or(|c| c.controller.beneficiary.is_some());
        if config.producer.enabled && !start_allowed {
            return Err(Error::Producer(producer::Error::BeneficiaryNotConfigured));
        }
        let lease = GroupLease::acquire(importer.database()).map_err(Error::Ownership)?;
        let status = Status {
            running: true,
            accepting: true,
            supervisor: supervisor::Status {
                phase: supervisor::Phase::Starting,
                generator_generation: 0,
                conductor_generation: 0,
                attempts: 0,
                failure: None,
                pending_notice: false,
            },
            requested_enabled: config.producer.enabled,
            request_revision: 0,
            applied_revision: None,
            retiring: false,
            published: 0,
            failure: None,
        };
        let shared = Arc::new(Shared {
            start_allowed,
            database: importer.database().clone(),
            state: Mutex::new(State {
                status,
                shutdown: false,
                current: None,
                outputs: VecDeque::new(),
            }),
            wake: Condvar::new(),
            credits: Arc::new(Mutex::new(0)),
            limit: config.outputs,
        });
        let factory = factory::Factory::new(
            importer,
            snapshots,
            pool.clone(),
            keys,
            &config,
            clocks.clone(),
            execution,
            commands,
        );
        let service =
            service::Service::new(factory, shared.clone(), pool, clocks, config.poll, lease);
        let thread = thread::Builder::new()
            .name("ae-pow-production-group".into())
            .spawn(move || service.run())
            .map_err(|e| Error::Spawn(e.to_string()))?;
        Ok(Self {
            client: Client(shared),
            thread: Some(thread),
        })
    }
    pub fn client(&self) -> Client<O> {
        self.client.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub fn shutdown(mut self) -> thread::Result<Report> {
        self.client.request_shutdown();
        self.thread.take().expect("group thread").join()
    }
}
impl<O> Drop for Worker<O> {
    fn drop(&mut self) {
        self.client.request_shutdown();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
