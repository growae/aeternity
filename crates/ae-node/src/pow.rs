// One configured PoW root. Subordinate worker, connection and production
// owners retain their own native restart policies; root failures are terminal.
use crate::bootstrap::configured::pow::{self as configured, Prepared, Services};
use crate::configuration::resolution::ApplicationMode;
use ae_storage::pool::worker::GossipCache;
use ae_sync::{
    chain::{
        on_demand::service,
        pending::service as pending,
        pow::{generator, group},
    },
    observe,
    runtime::{connection, workers},
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};
mod production;
mod stratum;
pub use production::{CommandGeneration, Commands};
use production::{Input, Live, Retired, Retirement};
pub use stratum::{StratumExit, StratumStatus};

#[derive(Clone)]
pub struct Config<P = configured::Production> {
    pub workers: workers::Config,
    pub connections: connection::Config,
    pub production: P,
    pub stratum: Option<Arc<crate::stratum::configured::Config>>,
    pub pending_key: pending::Limits,
    pub cache_entries: usize,
    pub cache_limit: usize,
    pub startup_timeout: Duration,
    pub poll: Duration,
    pub outputs: usize,
}
impl<P: Default> Default for Config<P> {
    fn default() -> Self {
        Self::with_production(P::default())
    }
}
impl<P> Config<P> {
    pub fn with_production(production: P) -> Self {
        Self {
            workers: Default::default(),
            connections: Default::default(),
            production,
            stratum: None,
            pending_key: Default::default(),
            cache_entries: 1000,
            cache_limit: 100_000,
            startup_timeout: Duration::from_secs(30),
            poll: Duration::from_millis(10),
            outputs: 16,
        }
    }
    fn into_parts(self) -> (P, Config<()>) {
        (
            self.production,
            Config {
                workers: self.workers,
                connections: self.connections,
                production: (),
                stratum: self.stratum,
                pending_key: self.pending_key,
                cache_entries: self.cache_entries,
                cache_limit: self.cache_limit,
                startup_timeout: self.startup_timeout,
                poll: self.poll,
                outputs: self.outputs,
            },
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Prepared,
    Workers,
    Importer,
    Identity,
    Connections,
    Production,
    Running,
    Rebinding,
    Stopping,
    Stopped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Child {
    Workers,
    Connections,
    Production,
    PendingKey,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    NoRuntime,
    Closed,
    ReceiptTaken,
    NotReady,
    UnsupportedControl,
    StartupTimeout(Phase),
    Start(String),
    Workers(workers::Error),
    Connections(connection::Error),
    Production(group::Error),
    Commands(service::Error),
    PendingKey(pending::Error),
    Stratum(crate::stratum::configured::Failure),
    ChildStopped(Child),
    ChildTask(Child),
    Unsettled,
    RootTask,
}
#[derive(Debug, Clone)]
pub struct Status {
    pub phase: Phase,
    pub running: bool,
    pub ready: bool,
    pub joined: bool,
    pub failure: Option<Error>,
    pub workers: Option<workers::Status>,
    pub connections: Option<connection::Status>,
    pub production: Option<group::Status>,
    pub commands: Option<service::Status>,
    pub pending_key: Option<pending::Status>,
    pub production_starts: u64,
    pub stratum: Option<StratumStatus>,
    pub stratum_starts: u64,
}
pub enum Output {
    Stratum(ae_stratum::runtime::Lease<ae_stratum::runtime::Output>),
    StratumRetired(Box<StratumExit>),
    Production(Arc<group::Output>),
    Retired(Box<group::Released>),
    Command {
        generation: u64,
        completion: Arc<service::Completion>,
    },
    CommandsRetired {
        generation: u64,
        report: Box<service::Released>,
    },
}
pub struct Exit {
    pub stratum: Option<StratumExit>,
    pub status: Status,
    pub production: Option<Result<group::Report, Error>>,
    pub commands: Option<Result<service::Report, Error>>,
    pub retained: Vec<Output>,
    pub services: Option<Services>,
    pub prepared: Option<Prepared>,
}
struct Controls {
    stratum: Option<crate::stratum::configured::Handle>,
    importer: Option<Arc<ae_sync::chain::Importer>>,
    workers: Option<workers::Handle>,
    connections: Option<connection::Handle>,
    production: Option<group::Client<ae_vm_fate::maps::order::Otp26>>,
    commands: Option<CommandGeneration>,
    pending_key: Option<pending::Client>,
    enabled: Option<bool>,
}
struct Shared {
    stop: watch::Sender<bool>,
    published: watch::Sender<Status>,
    controls: Mutex<Controls>,
    outputs: Mutex<VecDeque<Output>>,
    exit: Mutex<Option<Exit>>,
}
#[derive(Clone)]
pub struct Handle {
    shared: Arc<Shared>,
    observed: watch::Receiver<Status>,
}
impl Handle {
    pub fn request_shutdown(&self) {
        self.shared.stop.send_replace(true);
    }
    pub fn status(&self) -> Status {
        self.shared.published.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.observed.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.observed.borrow_and_update().clone())
    }
    pub fn workers(&self) -> Option<workers::Handle> {
        lock(&self.shared.controls).workers.clone()
    }
    // Stable across subordinate replacements. Shutdown closes this capability
    // before the root publishes its terminal receipt, including retained clones.
    pub fn importer(&self) -> Option<Arc<ae_sync::chain::Importer>> {
        lock(&self.shared.controls).importer.clone()
    }
    pub fn pending_key(&self) -> Option<pending::Client> {
        lock(&self.shared.controls).pending_key.clone()
    }
    pub fn connections(&self) -> Option<connection::Handle> {
        lock(&self.shared.controls).connections.clone()
    }
    pub fn production(&self) -> Option<group::Client<ae_vm_fate::maps::order::Otp26>> {
        lock(&self.shared.controls).production.clone()
    }
    pub fn commands(&self) -> Option<CommandGeneration> {
        let controls = lock(&self.shared.controls);
        if let Some(commands) = &controls.commands {
            return Some(commands.clone());
        }
        let generation = controls.production.as_ref()?.current()?;
        Some(CommandGeneration {
            number: self.status().production_starts,
            production: Some(generation.id),
            client: generation.producer.commands()?,
        })
    }
    pub fn stratum(&self) -> Option<crate::stratum::configured::Handle> {
        lock(&self.shared.controls).stratum.clone()
    }
    // Desired production state is retained while worker dependencies rebind.
    pub fn set_enabled(&self, enabled: bool) -> Result<(), Error> {
        if *self.shared.stop.borrow() || !self.status().running {
            return Err(Error::Closed);
        }
        let mut c = lock(&self.shared.controls);
        if c.enabled.is_none() {
            return Err(Error::UnsupportedControl);
        }
        if let Some(p) = &c.production {
            p.set_enabled(enabled).map_err(Error::Production)?;
        }
        c.enabled = Some(enabled);
        Ok(())
    }
    pub fn try_output(&self) -> Option<Output> {
        lock(&self.shared.outputs).pop_front()
    }
    pub fn pending_outputs(&self) -> usize {
        lock(&self.shared.outputs).len()
    }
    pub fn take_exit(&self) -> Option<Exit> {
        lock(&self.shared.exit).take()
    }
}
pub struct Running {
    handle: Handle,
    task: Option<JoinHandle<()>>,
}
impl Running {
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    pub async fn wait(mut self) -> Result<Exit, Error> {
        self.task
            .take()
            .expect("root task")
            .await
            .map_err(|_| Error::RootTask)?;
        self.handle.take_exit().ok_or(Error::ReceiptTaken)
    }
    pub async fn shutdown(self) -> Result<Exit, Error> {
        self.handle.request_shutdown();
        self.wait().await
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.handle.request_shutdown();
    }
}
pub struct StartError {
    pub error: Error,
    pub prepared: Prepared,
}
impl std::fmt::Debug for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
struct Owner {
    shared: Arc<Shared>,
    stop: watch::Receiver<bool>,
    status: Status,
    config: Config<()>,
    input: Input,
    prepared: Option<Prepared>,
    services: Option<Services>,
    workers: Option<workers::Handle>,
    worker_task: Option<JoinHandle<workers::Status>>,
    connections: Option<connection::Handle>,
    connection_task: Option<JoinHandle<connection::Status>>,
    production: Option<Live>,
    stratum: Option<stratum::Generation>,
    stratum_source: Option<crate::stratum::mining::production::Source>,
    pending_stratum: Option<StratumExit>,
    collect_stratum_first: bool,
    stratum_epoch: Instant,
    stratum_last_restart: Option<u64>,
    pending_key: Option<pending::Worker>,
    pending_retirement: Option<Retirement>,
    dependencies: (u64, u64),
}
pub fn launch<F>(
    prepared: Prepared,
    config: Config,
    clocks: Arc<dyn group::Clocks>,
    outputs: F,
) -> Result<Running, Box<StartError>>
where
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    let (mut production, config) = config.into_parts();
    if config.stratum.is_some() {
        production.producer.external_key_work = true;
    }
    launch_inner(
        prepared,
        config,
        Input::Automatic {
            algorithm: ae_protocol::ConsensusAlgorithm::Cuckoo,
            config: Box::new(production),
            clocks,
            commands: None,
        },
        outputs,
    )
}
pub fn launch_on_demand<F>(
    prepared: Prepared,
    config: Config,
    clocks: Arc<dyn group::Clocks>,
    outputs: F,
) -> Result<Running, Box<StartError>>
where
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    let (production, config) = config.into_parts();
    let commands = production.mining.as_ref().map(|mining| {
        let mut commands = Commands::new(mining.beneficiary);
        commands.controller.release_info = mining.release_info;
        commands.controller.key = mining.preparation;
        commands.controller.micro = production.generator.candidate;
        commands.controller.resume_current = production.producer.resume_current;
        Box::new(commands)
    });
    launch_inner(
        prepared,
        config,
        Input::Automatic {
            algorithm: ae_protocol::ConsensusAlgorithm::OnDemand,
            config: Box::new(production),
            clocks,
            commands,
        },
        outputs,
    )
}
pub fn launch_commands<F>(
    prepared: Prepared,
    config: Config<Commands>,
    clock: Arc<dyn generator::Clock>,
    outputs: F,
) -> Result<Running, Box<StartError>>
where
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    let (production, config) = config.into_parts();
    launch_inner(
        prepared,
        config,
        Input::Commands {
            config: Box::new(production),
            clock,
        },
        outputs,
    )
}
fn launch_inner<F>(
    prepared: Prepared,
    config: Config<()>,
    input: Input,
    outputs: F,
) -> Result<Running, Box<StartError>>
where
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    let error = if prepared.configured().chain().identity().algorithm != input.algorithm()
        || config.workers.keys.is_none()
        || config.poll.is_zero()
        || config.poll > Duration::from_secs(1)
        || config.startup_timeout.is_zero()
        || config.startup_timeout > Duration::from_secs(300)
        || !(1..=128).contains(&config.outputs)
        || !config.pending_key.valid()
        || (config.stratum.is_some() && !input.stratum_compatible())
    {
        Some(Error::Configuration)
    } else if let Err(error) = input.validate(prepared.rules().admission.micro_cycle_ms) {
        Some(error)
    } else if tokio::runtime::Handle::try_current().is_err() {
        Some(Error::NoRuntime)
    } else {
        None
    };
    if let Some(error) = error {
        return Err(Box::new(StartError { error, prepared }));
    }
    let status = Status {
        phase: Phase::Prepared,
        running: true,
        ready: false,
        joined: false,
        failure: None,
        workers: None,
        connections: None,
        production: None,
        commands: None,
        pending_key: None,
        production_starts: 0,
        stratum: None,
        stratum_starts: 0,
    };
    let (published, observed) = watch::channel(status.clone());
    let (stop, stopped) = watch::channel(false);
    let shared = Arc::new(Shared {
        stop,
        published,
        controls: Mutex::new(Controls {
            stratum: None,
            importer: None,
            workers: None,
            connections: None,
            production: None,
            commands: None,
            pending_key: None,
            enabled: input.enabled(),
        }),
        outputs: Mutex::new(VecDeque::new()),
        exit: Mutex::new(None),
    });
    let handle = Handle {
        shared: shared.clone(),
        observed,
    };
    let mut owner = Owner {
        shared,
        stop: stopped,
        status,
        config,
        input,
        prepared: Some(prepared),
        services: None,
        workers: None,
        worker_task: None,
        connections: None,
        connection_task: None,
        production: None,
        stratum: None,
        stratum_source: None,
        pending_stratum: None,
        collect_stratum_first: true,
        stratum_epoch: Instant::now(),
        stratum_last_restart: None,
        pending_key: None,
        pending_retirement: None,
        dependencies: (0, 0),
    };
    let task = tokio::spawn(async move {
        let result = match owner.start(outputs).await {
            Ok(()) => owner.run().await,
            Err(e) => Err(e),
        };
        if let Err(error) = result
            && (error != Error::Closed || !owner.stopping())
        {
            owner.status.failure = Some(error);
        }
        owner.finish().await;
    });
    Ok(Running {
        handle,
        task: Some(task),
    })
}
impl Owner {
    fn stopping(&self) -> bool {
        *self.stop.borrow()
    }
    fn publish(&mut self) {
        self.status.stratum = self.stratum.as_ref().map(stratum::Generation::status);
        self.status.workers = self.workers.as_ref().map(workers::Handle::status);
        self.status.connections = self.connections.as_ref().map(connection::Handle::status);
        if let Some(pending) = &self.pending_key {
            self.status.pending_key = Some(pending.client().status());
        }
        if let Some(p) = &self.production {
            p.update(&mut self.status);
        }
        self.status.ready = self.status.phase == Phase::Running
            && self.status.workers.as_ref().is_some_and(|w| w.ready)
            && self.status.connections.as_ref().is_none_or(|c| c.ready)
            && self
                .status
                .pending_key
                .as_ref()
                .is_some_and(|p| p.running && p.accepting)
            && self.production.as_ref().is_some_and(Live::ready)
            && (self.config.stratum.is_none()
                || self.status.stratum.as_ref().is_some_and(|s| s.healthy));
        self.shared.published.send_replace(self.status.clone());
    }
    fn phase(&mut self, phase: Phase) {
        self.status.phase = phase;
        self.publish();
    }
    async fn pause(&mut self) {
        tokio::select! {biased;_=self.stop.changed()=>(),_=tokio::time::sleep(self.config.poll)=>()}
    }
    fn startup_check(&self, end: Instant) -> Result<(), Error> {
        if self.stopping() {
            Err(Error::Closed)
        } else if Instant::now() >= end {
            Err(Error::StartupTimeout(self.status.phase))
        } else {
            Ok(())
        }
    }
    fn worker_health(&self) -> Result<(), Error> {
        if let Some(pending) = &self.pending_key {
            let status = pending.client().status();
            if let Some(error) = status.failure {
                return Err(Error::PendingKey(error));
            }
            if !status.running {
                return Err(Error::ChildStopped(Child::PendingKey));
            }
        }
        let w = self.workers.as_ref().ok_or(Error::NotReady)?.status();
        if let Some(e) = w.error {
            return Err(Error::Workers(e));
        }
        if !w.running
            || self
                .worker_task
                .as_ref()
                .is_some_and(JoinHandle::is_finished)
        {
            return Err(Error::ChildStopped(Child::Workers));
        }
        Ok(())
    }
    fn connection_health(&self) -> Result<(), Error> {
        if let Some(c) = &self.connections {
            let s = c.status();
            if let Some(e) = s.error {
                return Err(Error::Connections(e));
            }
            if !s.running
                || self
                    .connection_task
                    .as_ref()
                    .is_some_and(JoinHandle::is_finished)
            {
                return Err(Error::ChildStopped(Child::Connections));
            }
        }
        Ok(())
    }
    async fn workers_ready(&mut self, end: Instant) -> Result<(), Error> {
        loop {
            self.startup_check(end)?;
            self.worker_health()?;
            self.publish();
            if self.workers.as_ref().unwrap().status().ready {
                return Ok(());
            }
            self.pause().await;
        }
    }
    async fn start<F>(&mut self, outputs: F) -> Result<(), Error>
    where
        F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
    {
        let end = Instant::now() + self.config.startup_timeout;
        self.startup_check(end)?;
        let network = self
            .prepared
            .as_ref()
            .unwrap()
            .configured()
            .mode()
            .application()
            == ApplicationMode::Normal;
        let (cache, controller) =
            GossipCache::managed(self.config.cache_entries, self.config.cache_limit)
                .map_err(|e| Error::Start(format!("{e:?}")))?;
        // The native cache belongs to aesync, absent outside normal mode.
        // Dropping the unused controller preserves unavailable-cache misses;
        // a fixed in-memory default would incorrectly retain duplicate hits.
        let controller = network.then_some(controller);
        self.phase(Phase::Workers);
        let (h, service) = self
            .prepared
            .as_ref()
            .unwrap()
            .workers(self.config.workers.clone(), cache)
            .map_err(|e| Error::Start(format!("{e:?}")))?;
        lock(&self.shared.controls).workers = Some(h.clone());
        self.workers = Some(h);
        self.worker_task = Some(tokio::spawn(service.run()));
        self.workers_ready(end).await?;
        self.phase(Phase::Importer);
        match self
            .prepared
            .take()
            .unwrap()
            .activate(self.workers.as_ref().unwrap())
        {
            Ok(s) => {
                lock(&self.shared.controls).importer = Some(s.shared_importer());
                self.services = Some(s);
            }
            Err(e) => {
                let configured::ActivationError { error, prepared } = *e;
                self.prepared = Some(prepared);
                return Err(Error::Start(format!("{error:?}")));
            }
        }
        let keys = self
            .workers
            .as_ref()
            .unwrap()
            .bindings()
            .keys
            .ok_or(Error::NotReady)?;
        let (config, clock) = self
            .input
            .pending(self.config.pending_key, self.config.stratum.as_deref());
        let pending = self
            .services
            .as_ref()
            .unwrap()
            .pending_key(keys, config, clock)
            .map_err(Error::PendingKey)?;
        lock(&self.shared.controls).pending_key = Some(pending.client());
        self.pending_key = Some(pending);
        if let Some(controller) = controller {
            self.phase(Phase::Identity);
            let keys = self
                .workers
                .as_ref()
                .unwrap()
                .bindings()
                .keys
                .and_then(|k| k.current())
                .ok_or(Error::NotReady)?;
            let ticket = keys
                .peer_identity()
                .map_err(|e| Error::Start(format!("{e:?}")))?;
            let identity = tokio::select! {biased;_=self.stop.changed()=>return Err(Error::Closed),
            _=tokio::time::sleep_until(end)=>return Err(Error::StartupTimeout(Phase::Identity)),
            r=ticket=>r.map_err(|e|Error::Start(format!("{e:?}")))?};
            self.startup_check(end)?;
            self.phase(Phase::Connections);
            let (h, service) = self
                .services
                .as_ref()
                .unwrap()
                .connections(
                    self.workers.as_ref().unwrap(),
                    identity,
                    controller,
                    self.config.connections.clone(),
                    outputs,
                )
                .map_err(|e| Error::Start(format!("{e:?}")))?;
            lock(&self.shared.controls).connections = Some(h.clone());
            self.connections = Some(h);
            self.connection_task = Some(tokio::spawn(service.run()));
            loop {
                self.startup_check(end)?;
                self.worker_health()?;
                self.connection_health()?;
                self.publish();
                if self.connections.as_ref().unwrap().status().ready {
                    break;
                }
                self.pause().await;
            }
        }
        self.phase(Phase::Production);
        self.start_production(end).await?;
        self.phase(Phase::Running);
        Ok(())
    }
    async fn start_production(&mut self, end: Instant) -> Result<(), Error> {
        self.workers_ready(end).await?;
        let w = self.workers.as_ref().unwrap();
        let status = w.status();
        let b = w.bindings();
        let pool = b.admission.current().ok_or(Error::NotReady)?.client;
        let keys = b.keys.and_then(|k| k.current()).ok_or(Error::NotReady)?;
        let number = self
            .status
            .production_starts
            .checked_add(1)
            .ok_or(Error::Configuration)?;
        let production = match self
            .input
            .spawn(self.services.as_ref().unwrap(), pool, keys)
        {
            Ok(p) => p,
            Err(failure) => {
                let production::StartFailure { error, retired } = *failure;
                if let Some(value) = retired {
                    value.update(&mut self.status);
                    self.pending_retirement = Some(Retirement { number, value });
                }
                return Err(error);
            }
        };
        self.production = Some(production);
        self.dependencies = (status.admission_generation, status.keys_generation);
        self.status.production_starts = number;
        {
            let mut controls = lock(&self.shared.controls);
            match self.production.as_ref().unwrap() {
                Live::Automatic(worker) => {
                    let client = worker.client();
                    client
                        .set_enabled(controls.enabled.expect("automatic production control"))
                        .map_err(Error::Production)?;
                    controls.production = Some(client);
                }
                Live::Commands(worker) => {
                    controls.commands = Some(CommandGeneration {
                        number,
                        production: None,
                        client: worker.client(),
                    });
                }
            }
        }
        if self.config.stratum.is_some()
            && let Some(Live::Automatic(group)) = &self.production
        {
            let database = self
                .services
                .as_ref()
                .unwrap()
                .configured()
                .chain()
                .database()
                .clone();
            let source = self.stratum_source.get_or_insert_with(|| {
                crate::stratum::mining::production::Source::managed(database)
            });
            source
                .install(number, group.client())
                .map_err(|_| Error::Stratum(crate::stratum::configured::Failure::Configuration))?;
        }
        loop {
            self.startup_check(end)?;
            self.worker_health()?;
            self.connection_health()?;
            self.publish();
            let production = self.production.as_ref().unwrap();
            production.check()?;
            if production.ready() && self.refresh_stratum().await? {
                return Ok(());
            }
            self.pause().await;
        }
    }
    fn collect(&mut self) {
        if self.collect_stratum_first {
            self.collect_stratum();
        }
        if let Some(p) = &self.production {
            p.collect(
                &mut lock(&self.shared.outputs),
                self.config.outputs,
                self.status.production_starts,
            );
        }
        if !self.collect_stratum_first {
            self.collect_stratum();
        }
        self.collect_stratum_first = !self.collect_stratum_first;
    }
    fn changed_dependencies(&self) -> bool {
        let w = self.workers.as_ref().unwrap();
        let s = w.status();
        let b = w.bindings();
        (s.admission_generation, s.keys_generation) != self.dependencies
            || b.admission.current().is_none()
            || b.keys.as_ref().is_none_or(|k| k.current().is_none())
    }
    async fn stop_production(&mut self) -> Result<(), Error> {
        if let Some(source) = &self.stratum_source {
            source.withdraw();
        }
        {
            let mut controls = lock(&self.shared.controls);
            controls.production = None;
            controls.commands = None;
        }
        if let Some(p) = self.production.take() {
            p.request_shutdown();
            let value = p.stop().await?;
            value.update(&mut self.status);
            self.pending_retirement = Some(Retirement {
                number: self.status.production_starts,
                value,
            });
        }
        Ok(())
    }
    async fn rebind(&mut self) -> Result<(), Error> {
        self.phase(Phase::Rebinding);
        self.stop_production().await?;
        let end = Instant::now() + self.config.startup_timeout;
        loop {
            if self.stopping() {
                return Err(Error::Closed);
            }
            self.worker_health()?;
            self.connection_health()?;
            let stratum_released = self.release_stratum()?;
            let retired = &mut self.pending_retirement.as_mut().unwrap().value;
            // A command with a recovery-required or unknown outcome must not
            // be retried by constructing a replacement controller.
            if matches!(retired, Retired::Commands(_))
                && let Some(e) = retired.failure()
            {
                return Err(e);
            }
            let settled = retired.poll()?;
            if settled && stratum_released && lock(&self.shared.outputs).len() < self.config.outputs
            {
                break;
            }
            // A slow reader applies bounded backpressure, not a child failure.
            // Only unsettled accepted work is subject to the retirement bound.
            if !settled {
                self.startup_check(end)?;
            }
            self.publish();
            self.pause().await;
        }
        let report = self.pending_retirement.take().unwrap();
        let released = match report.release() {
            Ok(r) => r,
            Err(r) => {
                self.pending_retirement = Some(r);
                return Err(Error::Unsettled);
            }
        };
        lock(&self.shared.outputs).push_back(released);
        self.start_production(Instant::now() + self.config.startup_timeout)
            .await?;
        self.phase(Phase::Running);
        Ok(())
    }
    async fn run(&mut self) -> Result<(), Error> {
        while !self.stopping() {
            self.worker_health()?;
            self.collect();
            if self.changed_dependencies() {
                self.rebind().await?;
                continue;
            }
            self.connection_health()?;
            self.production.as_ref().unwrap().check()?;
            self.refresh_stratum().await?;
            self.publish();
            self.pause().await;
        }
        Ok(())
    }
    async fn finish(&mut self) {
        self.phase(Phase::Stopping);
        let mut joined = true;
        if let Err(error) = self.stop_stratum().await {
            joined = false;
            self.status.failure.get_or_insert(error);
        }
        if let Some(production) = &self.production {
            production.request_shutdown();
        }
        if let Some(pending) = self.pending_key.take() {
            pending.request_shutdown();
            match tokio::task::spawn_blocking(move || pending.shutdown()).await {
                Ok(status) => {
                    joined &= status.joined;
                    if let Some(error) = &status.failure {
                        self.status
                            .failure
                            .get_or_insert(Error::PendingKey(error.clone()));
                    }
                    self.status.pending_key = Some(status);
                }
                Err(_) => {
                    joined = false;
                    self.status
                        .failure
                        .get_or_insert(Error::ChildTask(Child::PendingKey));
                }
            }
        }
        let mut production_failure = None;
        let mut command_failure = None;
        if let Err(e) = self.stop_production().await {
            joined = false;
            self.status.failure.get_or_insert(e.clone());
            match self.input {
                Input::Automatic { .. } => production_failure = Some(e),
                Input::Commands { .. } => command_failure = Some(e),
            }
        }
        if let Some(c) = &self.connections {
            c.shutdown();
        }
        if let Some(t) = self.connection_task.take() {
            match t.await {
                Ok(s) => {
                    joined &= s.joined;
                    if let Some(e) = &s.error {
                        self.status
                            .failure
                            .get_or_insert(Error::Connections(e.clone()));
                    }
                    self.status.connections = Some(s);
                }
                Err(_) => {
                    joined = false;
                    self.status
                        .failure
                        .get_or_insert(Error::ChildTask(Child::Connections));
                }
            }
        }
        if let Some(w) = &self.workers {
            w.shutdown();
        }
        if let Some(t) = self.worker_task.take() {
            match t.await {
                Ok(s) => {
                    joined &= s.joined;
                    if let Some(e) = &s.error {
                        self.status.failure.get_or_insert(Error::Workers(e.clone()));
                    }
                    self.status.workers = Some(s);
                }
                Err(_) => {
                    joined = false;
                    self.status
                        .failure
                        .get_or_insert(Error::ChildTask(Child::Workers));
                }
            }
        }
        if let Some(s) = &self.services {
            s.stop_and_join().await;
        }
        if let Some(r) = &mut self.pending_retirement {
            if let Some(e) = r.value.failure() {
                self.status.failure.get_or_insert(e);
            }
            match r.value.poll() {
                Ok(true) => (),
                Ok(false) => {
                    joined = false;
                    self.status.failure.get_or_insert(Error::Unsettled);
                }
                Err(e) => {
                    joined = false;
                    self.status.failure.get_or_insert(e);
                }
            }
        }
        let mut production = production_failure.map(Err);
        let mut commands = command_failure.map(Err);
        if let Some(retirement) = self.pending_retirement.take() {
            match retirement.value {
                Retired::Automatic(report) => production = Some(Ok(*report)),
                Retired::Commands(report) => commands = Some(Ok(*report)),
            }
        }
        if let Some(s) = &self.pending_stratum {
            joined &= s.report.settled();
            if let Some(failure) = s.report.failure() {
                self.status.failure.get_or_insert(Error::Stratum(failure));
            }
        }
        self.status.phase = Phase::Stopped;
        self.status.running = false;
        self.status.ready = false;
        self.status.joined = joined;
        let retained = lock(&self.shared.outputs).drain(..).collect();
        *lock(&self.shared.exit) = Some(Exit {
            stratum: self.pending_stratum.take(),
            status: self.status.clone(),
            production,
            commands,
            retained,
            services: self.services.take(),
            prepared: self.prepared.take(),
        });
        self.shared.published.send_replace(self.status.clone());
    }
}
// Even an unexpected root task drop fences children; normal shutdown above
// additionally joins them and retains the single terminal receipt.
impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(s) = &self.stratum {
            s.worker.handle().request_shutdown();
        }
        if let Some(pending) = &self.pending_key {
            pending.request_shutdown();
        }
        if let Some(p) = &self.production {
            p.request_shutdown();
        }
        if let Some(c) = &self.connections {
            c.shutdown();
        }
        if let Some(w) = &self.workers {
            w.shutdown();
        }
        if let Some(s) = &self.services {
            s.importer().stop();
        }
    }
}
