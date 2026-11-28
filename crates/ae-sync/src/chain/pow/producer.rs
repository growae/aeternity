// Live PoW microblock signing and admission. Native decision references:
// aec_conductor:handle_info(candidate_block), start_micro_signing/setup_loop,
// aeternity fa2a6d7. ISC copyright: repository LICENSE.
use super::generator;
use crate::chain::on_demand;
use crate::{chain, keys};
use ae_chain::{
    block::{Block, MicroBlock},
    publication, repository as repo,
};
use ae_storage::{events, pow::ProducerLease};
use ae_types::{AccountPublicKey, Hash256};
use ae_vm_fate::maps::MapOrder;
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub mod mining;
mod policy;
mod retirement;
mod service;
pub use retirement::{Completion, Report};

pub trait Clock: Send + 'static {
    fn now(&self) -> Result<(u64, Instant), Error>;
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Result<(u64, Instant), Error> {
        Ok((
            u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| Error::Time)?
                    .as_millis(),
            )
            .map_err(|_| Error::Time)?,
            Instant::now(),
        ))
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    // Automatic key production. Later selected-block events can establish
    // microblock leadership while this state remains stopped.
    pub enabled: bool,
    // Stratum supplies key-block work externally. Promotion still runs while
    // local micro production is stopped; no local miner is constructed.
    pub external_key_work: bool,
    // Explicit restart recovery: resume an owned selected generation from
    // the initial snapshot. Native conductor init starts with leader=false.
    pub resume_current: bool,
    pub strictly_follow_top: bool,
    pub micro_cycle_ms: u64,
    pub poll: Duration,
    pub events_per_step: usize,
    pub outputs: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            external_key_work: false,
            resume_current: false,
            strictly_follow_top: false,
            micro_cycle_ms: 3000,
            poll: Duration::from_millis(10),
            events_per_step: 64,
            outputs: 16,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    BeneficiaryNotConfigured,
    Binding,
    Closed,
    Time,
    Sequence,
    Panicked,
    Spawn(String),
    Repository(repo::Error),
    Ownership(ae_storage::snapshots::Error),
    Events(events::Error),
    SourceClosed,
    GeneratorClosed,
    Generator(generator::RequestError),
    Keys(keys::Error),
    KeyReply,
    Admission(chain::Error),
    Block(ae_chain::block::Error),
    Mining(mining::Error),
    CommandStart(String),
    Commands(on_demand::service::Error),
}
#[derive(Debug, Clone)]
pub struct Status {
    pub running: bool,
    pub accepting: bool,
    pub activated: bool,
    pub requested_enabled: bool,
    pub request_revision: u64,
    pub applied_revision: u64,
    pub enabled: bool,
    pub leader: bool,
    pub top: Hash256,
    pub key: Hash256,
    pub miner: AccountPublicKey,
    pub sleeping: bool,
    pub pending: bool,
    pub promotion_pending: bool,
    pub generator_pending: bool,
    pub signatures: u64,
    pub admissions: u64,
    pub promotions: u64,
    pub published: u64,
    pub observed_revision: u64,
    pub discarded: u64,
    pub failure: Option<Error>,
    pub mining: Option<mining::Status>,
}
pub struct Output {
    pub sequence: u64,
    pub completion: Completion,
    _credit: Credit,
}
struct Credit(Arc<Mutex<usize>>);
impl Drop for Credit {
    fn drop(&mut self) {
        *lock(&self.0) -= 1;
    }
}
struct State {
    status: Status,
    shutdown: bool,
    outputs: VecDeque<Arc<Output>>,
    control_after: u64,
    resume: bool,
}
struct Shared {
    database: Arc<ae_storage::Database>,
    keys: keys::Client,
    state: Mutex<State>,
    wake: Condvar,
    credits: Arc<Mutex<usize>>,
    limit: usize,
    commands: Mutex<Option<on_demand::service::Client>>,
    start_allowed: bool,
    resume_current: bool,
}
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
#[derive(Clone)]
pub struct Client(Arc<Shared>);
impl Client {
    pub fn commands(&self) -> Option<on_demand::service::Client> {
        lock(&self.0.commands).clone()
    }
    pub(in crate::chain) fn observed(&self, revision: u64) -> Result<bool, Error> {
        let s = lock(&self.0.state);
        if s.shutdown || !s.status.running || !s.status.accepting {
            return Err(Error::Closed);
        }
        Ok(s.status.activated
            && s.status.request_revision == s.status.applied_revision
            && s.status.observed_revision >= revision
            && !s.status.promotion_pending)
    }
    pub fn uses_snapshots(&self, snapshots: &ae_storage::snapshots::Owner) -> bool {
        snapshots.uses_database(&self.0.database)
    }
    pub fn status(&self) -> Status {
        lock(&self.0.state).status.clone()
    }
    // A Stratum preparer must observe this owner's selected top and complete
    // key promotion before capturing the next candidate identity.
    pub fn candidate_key(&self, parent: Hash256) -> Result<Option<AccountPublicKey>, Error> {
        let state = lock(&self.0.state);
        if state.shutdown || !state.status.running || !state.status.accepting {
            return Err(Error::Closed);
        }
        if !state.status.activated
            || state.status.request_revision != state.status.applied_revision
            || state.status.top != parent
            || state.status.promotion_pending
        {
            return Ok(None);
        }
        let keys = self.0.keys.info();
        if !keys.running || !keys.accepting {
            return Err(Error::Keys(keys::Error::Stopped));
        }
        Ok(Some(keys.keys.candidate))
    }
    // Idempotent desired-state control, not an unbounded queue of commands.
    // Applied revision observes the coordinator's gate. Already accepted key
    // or admission work is retained and may complete after disabling.
    pub fn set_enabled(&self, enabled: bool) -> Result<u64, Error> {
        self.desired(enabled, false)
    }
    // A supervised pair remains dormant until its owning group publishes it.
    pub(in crate::chain::pow) fn activate(&self, enabled: bool) -> Result<u64, Error> {
        self.desired(enabled, true)
    }
    fn desired(&self, enabled: bool, activate: bool) -> Result<u64, Error> {
        let mut s = lock(&self.0.state);
        if !s.status.accepting {
            return Err(Error::Closed);
        }
        policy::start(s.status.requested_enabled, enabled, self.0.start_allowed)?;
        let changed = s.status.requested_enabled != enabled;
        let activating = activate && !s.status.activated;
        if changed || activating {
            let revision = s
                .status
                .request_revision
                .checked_add(1)
                .ok_or(Error::Sequence)?;
            // Already published blocks precede this control, including events
            // the producer has not consumed yet. Later blocks may restore
            // leadership even while automatic key production remains stopped.
            s.control_after = self.0.database.events().info().status.revision;
            s.resume = enabled && self.0.resume_current;
            s.status.request_revision = revision;
            s.status.requested_enabled = enabled;
            s.status.activated |= activating;
            self.0.wake.notify_all();
        }
        Ok(s.status.request_revision)
    }
    pub fn front(&self) -> Option<Arc<Output>> {
        lock(&self.0.state).outputs.front().cloned()
    }
    pub fn acknowledge(&self, output: &Arc<Output>) -> bool {
        let mut s = lock(&self.0.state);
        if s.outputs.front().is_some_and(|o| Arc::ptr_eq(o, output)) {
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
        self.0.wake.notify_all();
        drop(s);
        if let Some(commands) = self.commands() {
            commands.request_shutdown();
        }
    }
}
pub struct Worker {
    client: Client,
    thread: Option<JoinHandle<Report>>,
    commands: Option<on_demand::service::Worker>,
}
pub struct CommandInputs {
    pub snapshots: ae_storage::snapshots::Owner,
    pub pool: ae_storage::pool::worker::Client,
    pub config: on_demand::Config,
    pub clock: Arc<dyn generator::Clock>,
}
#[derive(Clone, Copy)]
struct Selected {
    top: Hash256,
    key: Hash256,
    miner: AccountPublicKey,
}
fn selected<S: repo::Read>(view: &repo::View<S>, top: Hash256) -> Result<Selected, Error> {
    let (key, header) = match view.require_block(top).map_err(Error::Repository)? {
        Block::Key(header) => (top, header),
        Block::Micro(m) => {
            let key = Hash256(m.header.common.previous_key_hash);
            let Block::Key(header) = view.require_block(key).map_err(Error::Repository)? else {
                return Err(Error::Binding);
            };
            (key, header)
        }
    };
    Ok(Selected {
        top,
        key,
        miner: AccountPublicKey(header.miner),
    })
}
impl Worker {
    pub fn spawn<O: MapOrder + Clone + Send + Sync + 'static>(
        importer: &chain::Importer<O>,
        generator: generator::Client<O>,
        keys: keys::Client,
        config: Config,
        clock: impl Clock,
    ) -> Result<Self, Error> {
        Self::spawn_inner(importer, generator, keys, config, clock, None, None, true)
    }
    pub fn spawn_with_mining<O: MapOrder + Clone + Send + Sync + 'static>(
        importer: &chain::Importer<O>,
        generator: generator::Client<O>,
        keys: keys::Client,
        config: Config,
        clock: impl Clock,
        mining: mining::Inputs,
    ) -> Result<Self, Error> {
        Self::spawn_inner(
            importer,
            generator,
            keys,
            config,
            clock,
            Some(mining),
            None,
            true,
        )
    }
    pub fn spawn_with_commands<O: MapOrder + Clone + Send + Sync + 'static>(
        importer: &chain::Importer<O>,
        generator: generator::Client<O>,
        keys: keys::Client,
        config: Config,
        clock: impl Clock,
        mining: Option<mining::Inputs>,
        commands: CommandInputs,
    ) -> Result<Self, Error> {
        Self::spawn_inner(
            importer,
            generator,
            keys,
            config,
            clock,
            mining,
            Some(commands),
            true,
        )
    }
    pub(in crate::chain::pow) fn spawn_group<O: MapOrder + Clone + Send + Sync + 'static>(
        importer: &chain::Importer<O>,
        generator: generator::Client<O>,
        keys: keys::Client,
        config: Config,
        clock: impl Clock,
        mining: Option<mining::Inputs>,
        commands: Option<CommandInputs>,
    ) -> Result<Self, Error> {
        Self::spawn_inner(
            importer, generator, keys, config, clock, mining, commands, false,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn spawn_inner<O: MapOrder + Clone + Send + Sync + 'static>(
        importer: &chain::Importer<O>,
        generator: generator::Client<O>,
        keys: keys::Client,
        config: Config,
        clock: impl Clock,
        mining: Option<mining::Inputs>,
        commands: Option<CommandInputs>,
        activated: bool,
    ) -> Result<Self, Error> {
        if let Some(commands) = &commands {
            if importer.algorithm() != ae_protocol::ConsensusAlgorithm::OnDemand
                || commands.config.controller.beneficiary
                    != mining.as_ref().map(|m| m.config.beneficiary)
                || mining.as_ref().is_some_and(|m| {
                    commands.config.controller.release_info != m.config.release_info
                })
            {
                return Err(Error::Binding);
            }
            commands
                .config
                .service
                .validate()
                .map_err(Error::Commands)?;
        }
        let start_allowed = commands
            .as_ref()
            .is_none_or(|c| c.config.controller.beneficiary.is_some());
        if config.enabled && !start_allowed {
            return Err(Error::BeneficiaryNotConfigured);
        }
        if (config.external_key_work && mining.is_some())
            || config.poll.is_zero()
            || config.poll > Duration::from_secs(1)
            || !(1..=1024).contains(&config.events_per_step)
            || !(1..=128).contains(&config.outputs)
            || config.micro_cycle_ms > 86_400_000
        {
            return Err(Error::Configuration);
        }
        let db = importer.database().clone();
        let (view, cursor) = db.event_snapshot().map_err(Error::Repository)?;
        if config.external_key_work
            && view.identity().algorithm != ae_protocol::ConsensusAlgorithm::Cuckoo
        {
            return Err(Error::Binding);
        }
        if importer.consensus() != ae_protocol::ConsensusType::Pow
            || !generator.uses_source(repo::Read::source_id(view.source()))
            || generator.micro_cycle_ms() != config.micro_cycle_ms
        {
            return Err(Error::Binding);
        }
        importer.check_open().map_err(Error::Admission)?;
        let selected = selected(&view, view.status().tip.ok_or(Error::Binding)?)?;
        let lease = ProducerLease::acquire(&db).map_err(Error::Ownership)?;
        drop(view);
        let mining = mining
            .map(|inputs| mining::Mining::new(importer, inputs))
            .transpose()
            .map_err(Error::Mining)?;
        let status = Status {
            running: true,
            accepting: true,
            activated,
            requested_enabled: config.enabled,
            request_revision: 0,
            applied_revision: 0,
            enabled: config.enabled,
            leader: false,
            top: selected.top,
            key: selected.key,
            miner: selected.miner,
            sleeping: false,
            pending: false,
            promotion_pending: config.resume_current,
            generator_pending: false,
            signatures: 0,
            admissions: 0,
            promotions: 0,
            published: 0,
            observed_revision: cursor.revision(),
            discarded: 0,
            failure: None,
            mining: mining.as_ref().map(mining::Mining::status),
        };
        let shared = Arc::new(Shared {
            database: db.clone(),
            keys: keys.clone(),
            state: Mutex::new(State {
                status: status.clone(),
                shutdown: false,
                outputs: VecDeque::new(),
                control_after: cursor.revision(),
                resume: activated && config.resume_current,
            }),
            wake: Condvar::new(),
            credits: Arc::new(Mutex::new(0)),
            limit: config.outputs,
            commands: Mutex::new(None),
            start_allowed,
            resume_current: config.resume_current,
        });
        let commands = commands
            .map(|inputs| {
                let controller = on_demand::runtime::Controller::shared(
                    importer,
                    inputs.snapshots,
                    inputs.pool,
                    keys.clone(),
                    inputs.clock,
                    inputs.config.controller,
                    (Client(shared.clone()), lease.clone()),
                )
                .map_err(|e| Error::CommandStart(format!("{e:?}")))?;
                let worker = on_demand::service::Worker::spawn(controller, inputs.config.service)
                    .map_err(|e| Error::Commands(e.error.clone()))?;
                *lock(&shared.commands) = Some(worker.client());
                Ok::<_, Error>(worker)
            })
            .transpose()?;
        let service = service::Service::new(
            importer.borrowed(),
            generator,
            keys,
            config,
            Box::new(clock),
            shared.clone(),
            cursor,
            selected,
            status,
            lease,
            mining,
        );
        db.publish_selected_height().map_err(Error::Repository)?;
        let thread = thread::Builder::new()
            .name("ae-pow-micro-producer".into())
            .spawn(move || service.run())
            .map_err(|e| Error::Spawn(e.to_string()))?;
        Ok(Self {
            client: Client(shared),
            thread: Some(thread),
            commands,
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
            || self
                .commands
                .as_ref()
                .is_some_and(on_demand::service::Worker::is_finished)
    }
    pub fn shutdown(mut self) -> thread::Result<Report> {
        self.client.request_shutdown();
        let commands = self.commands.take().map(|w| Box::new(w.shutdown()));
        match self.thread.take().expect("producer thread").join() {
            Ok(mut report) => {
                if let Some(failure) = commands.as_ref().and_then(|r| r.status.failure.clone()) {
                    report
                        .status
                        .failure
                        .get_or_insert(Error::Commands(failure));
                }
                report.commands = commands;
                Ok(report)
            }
            Err(panic) => {
                // Keep an uncertain command receipt even if the producer's
                // thread failed outside its normal panic boundary.
                if commands.is_some() {
                    Err(Box::new((panic, commands)))
                } else {
                    Err(panic)
                }
            }
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.client.request_shutdown();
        drop(self.commands.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
impl crate::production_group::Child for Worker {
    type Exit = Report;
    fn is_finished(&self) -> bool {
        self.is_finished()
    }
    fn shutdown(self) -> thread::Result<Report> {
        self.shutdown()
    }
}
