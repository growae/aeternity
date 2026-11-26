// Root ownership follows aecore_sup's zero-restart boundary (fa2a6d7).
// Subordinate worker/connection owners retain their existing restart policies.
use crate::bootstrap::configured::{self, hyperchain as configured_hc};
use crate::configuration::resolution::ApplicationMode;
use ae_p2p::noise;
use ae_storage::pool::worker::CacheController;
use ae_sync::{
    chain::hyperchain::conductor::{self, Conductor, Output, runtime as producer},
    chain::pending::service as pending,
    observe,
    runtime::{connection, workers},
};
use ae_vm_fate::maps::MapOrder;
use producer::supervision as production;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};
mod driving;

pub enum Identity {
    Fixed(Arc<noise::Identity>),
    Managed,
}
pub struct Inputs<O: MapOrder + Clone + Send + Sync + 'static> {
    pub conductor: Conductor<O>,
    pub cache: CacheController,
    pub identity: Identity,
}
#[derive(Clone)]
pub struct Config {
    pub workers: workers::hyperchain::Config,
    pub connections: connection::Config,
    pub producer: producer::Settings,
    pub pending_key: pending::Limits,
    pub startup_timeout: Duration,
    pub poll: Duration,
    pub outputs: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            workers: Default::default(),
            connections: Default::default(),
            producer: Default::default(),
            pending_key: Default::default(),
            startup_timeout: Duration::from_secs(30),
            poll: Duration::from_millis(10),
            outputs: 16,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Prepared,
    Workers,
    Identity,
    Connections,
    Producer,
    Running,
    Stopping,
    Stopped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Child {
    Workers,
    Connections,
    Producer,
    PendingKey,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Start(String),
    NoRuntime,
    Closed,
    ReceiptTaken,
    NotReady,
    Identity,
    StartupTimeout(Phase),
    Workers(workers::Error),
    Connections(connection::Error),
    Producer(conductor::Error),
    Production(production::Error),
    ProducerSpawn(String),
    ProducerCleanup,
    PendingKey(pending::Error),
    ChildStopped(Child),
    ChildTask(Child),
    RootTask,
}
#[derive(Debug, Clone)]
pub struct Status {
    pub phase: Phase,
    pub ready: bool,
    pub running: bool,
    pub joined: bool,
    pub failure: Option<Error>,
    pub workers: Option<workers::Status>,
    pub connections: Option<connection::Status>,
    pub producer: Option<conductor::Status>,
    pub production: Option<production::Status>,
    pub pending_key: Option<pending::Status>,
}
pub struct Exit<O> {
    pub configured: Option<configured::Open>,
    pub status: Status,
    pub producer: Option<Result<conductor::Shutdown<O>, Error>>,
    pub retained: Vec<Output>,
    pub production: Option<production::Report>,
    pub production_notices: Vec<production::Notice>,
}
struct Controls<O> {
    importer: Option<Arc<ae_sync::chain::Importer<O>>>,
    pending_key: Option<pending::Client>,
    producer: Option<producer::Handle>,
    connections: Option<connection::Handle>,
    workers: Option<workers::hyperchain::Handle<O>>,
}
struct Shared<O> {
    stop: watch::Sender<bool>,
    published: watch::Sender<Status>,
    controls: Mutex<Controls<O>>,
    outputs: Mutex<VecDeque<Output>>,
    notices: Mutex<VecDeque<production::Notice>>,
    exit: Mutex<Option<Exit<O>>>,
}
pub struct Handle<O>(Arc<Shared<O>>, watch::Receiver<Status>);
impl<O> Clone for Handle<O> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}
impl<O> Handle<O> {
    pub fn pending_key(&self) -> Option<pending::Client> {
        self.0
            .controls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending_key
            .clone()
    }
    pub fn importer(&self) -> Option<Arc<ae_sync::chain::Importer<O>>> {
        self.0
            .controls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .importer
            .clone()
    }
    pub fn request_shutdown(&self) {
        self.0.stop.send_replace(true);
    }
    pub fn status(&self) -> Status {
        self.0.published.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.1.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.1.borrow_and_update().clone())
    }
    pub fn connections(&self) -> Option<connection::Handle> {
        self.0
            .controls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .connections
            .clone()
    }
    pub fn workers(&self) -> Option<workers::hyperchain::Handle<O>> {
        self.0
            .controls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .workers
            .clone()
    }
    pub fn try_output(&self) -> Option<Output> {
        self.0
            .outputs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front()
    }
    pub fn take_exit(&self) -> Option<Exit<O>> {
        self.0.exit.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
    pub fn try_production_notice(&self) -> Option<production::Notice> {
        self.0
            .notices
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front()
    }
    fn control(&self, start: bool) -> Result<(), Error> {
        if *self.0.stop.borrow() || !self.status().running {
            return Err(Error::Closed);
        }
        if self
            .status()
            .production
            .is_some_and(|p| p.phase != production::Phase::Running)
        {
            return Err(Error::Producer(conductor::Error::Backpressured));
        }
        let control = self.0.controls.lock().unwrap_or_else(|e| e.into_inner());
        let p = control.producer.as_ref().ok_or(Error::NotReady)?;
        if start {
            p.start_production()
        } else {
            p.stop_production()
        }
        .map_err(Error::Producer)
    }
    pub fn start_production(&self) -> Result<(), Error> {
        self.control(true)
    }
    pub fn stop_production(&self) -> Result<(), Error> {
        self.control(false)
    }
}
pub struct Running<O> {
    handle: Handle<O>,
    task: Option<JoinHandle<()>>,
}
impl<O> Running<O> {
    pub fn handle(&self) -> Handle<O> {
        self.handle.clone()
    }
    pub async fn wait(mut self) -> Result<Exit<O>, Error> {
        self.task
            .take()
            .expect("root task")
            .await
            .map_err(|_| Error::RootTask)?;
        self.handle.take_exit().ok_or(Error::ReceiptTaken)
    }
    pub async fn shutdown(self) -> Result<Exit<O>, Error> {
        self.handle.request_shutdown();
        self.wait().await
    }
}
// Dropping requests teardown. The supervisor continues joining its children;
// a retained Handle can observe completion and take the single Exit.
impl<O> Drop for Running<O> {
    fn drop(&mut self) {
        self.handle.request_shutdown();
    }
}
pub struct StartError<O: MapOrder + Clone + Send + Sync + 'static> {
    pub error: Error,
    pub inputs: Inputs<O>,
}
pub struct ConfiguredInputs {
    pub prepared: configured_hc::Prepared,
    pub runtime: conductor::Config,
    pub services: ae_sync::chain::hyperchain::configuration::Services,
    pub cache: CacheController,
    pub identity: Identity,
}
pub struct ConfiguredStartError {
    pub error: Error,
    pub inputs: ConfiguredInputs,
}
struct Activated<O: MapOrder + Clone + Send + Sync + 'static> {
    conductor: Conductor<O>,
    configured: configured::Open,
}
type Activate<O> =
    Box<dyn FnOnce(bool) -> Result<Activated<O>, Box<configured_hc::Failure>> + Send>;
struct Startup<O: MapOrder + Clone + Send + Sync + 'static> {
    conductor: Option<Conductor<O>>,
    activate: Option<Activate<O>>,
    cache: CacheController,
    identity: Identity,
    network: bool,
}
struct Owner<O: MapOrder + Clone + Send + Sync + 'static> {
    activate: Option<Activate<O>>,
    configured: Option<configured::Open>,
    network: bool,
    shared: Arc<Shared<O>>,
    stop: watch::Receiver<bool>,
    status: Status,
    config: Config,
    conductor: Option<Conductor<O>>,
    producer: Option<driving::Runtime<O>>,
    worker: Option<workers::hyperchain::Handle<O>>,
    worker_task: Option<JoinHandle<workers::Status>>,
    connection: Option<connection::Handle>,
    connection_task: Option<JoinHandle<connection::Status>>,
    pending_key: Option<pending::Worker>,
}
pub fn launch<O, F>(
    inputs: Inputs<O>,
    config: Config,
    clock: impl producer::Clock,
    outputs: F,
) -> Result<Running<O>, Box<StartError<O>>>
where
    O: MapOrder + Clone + Send + Sync + 'static,
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    if let Some(error) = validate(&config) {
        return Err(Box::new(StartError { error, inputs }));
    }
    Ok(spawn(
        Startup {
            conductor: Some(inputs.conductor),
            activate: None,
            cache: inputs.cache,
            identity: inputs.identity,
            network: true,
        },
        config,
        driving::Input::Single(Box::new(clock)),
        outputs,
    ))
}
pub fn launch_supervised<O, F>(
    inputs: Inputs<O>,
    config: Config,
    clocks: impl production::Factory,
    outputs: F,
) -> Result<Running<O>, Box<StartError<O>>>
where
    O: MapOrder + Clone + Send + Sync + 'static,
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    if let Some(error) = validate(&config) {
        return Err(Box::new(StartError { error, inputs }));
    }
    Ok(spawn(
        Startup {
            conductor: Some(inputs.conductor),
            activate: None,
            cache: inputs.cache,
            identity: inputs.identity,
            network: true,
        },
        config,
        driving::Input::Supervised(Box::new(clocks)),
        outputs,
    ))
}
fn validate(config: &Config) -> Option<Error> {
    if config.poll.is_zero()
        || config.poll > Duration::from_secs(1)
        || config.startup_timeout.is_zero()
        || config.startup_timeout > Duration::from_secs(300)
        || !(1..=65536).contains(&config.outputs)
        || !config.pending_key.valid()
    {
        Some(Error::Configuration)
    } else if tokio::runtime::Handle::try_current().is_err() {
        Some(Error::NoRuntime)
    } else {
        None
    }
}
pub fn launch_configured<F>(
    inputs: ConfiguredInputs,
    config: Config,
    clock: impl producer::Clock,
    outputs: F,
) -> Result<Running<ae_vm_fate::maps::order::Otp26>, Box<ConfiguredStartError>>
where
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    configured_launch(
        inputs,
        config,
        driving::Input::Single(Box::new(clock)),
        outputs,
    )
}
pub fn launch_configured_supervised<F>(
    inputs: ConfiguredInputs,
    config: Config,
    clocks: impl production::Factory,
    outputs: F,
) -> Result<Running<ae_vm_fate::maps::order::Otp26>, Box<ConfiguredStartError>>
where
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    configured_launch(
        inputs,
        config,
        driving::Input::Supervised(Box::new(clocks)),
        outputs,
    )
}
fn configured_launch<F>(
    inputs: ConfiguredInputs,
    config: Config,
    clock: driving::Input,
    outputs: F,
) -> Result<Running<ae_vm_fate::maps::order::Otp26>, Box<ConfiguredStartError>>
where
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    if let Some(error) = validate(&config) {
        return Err(Box::new(ConfiguredStartError { error, inputs }));
    }
    let network = inputs.prepared.configured().mode().application() == ApplicationMode::Normal;
    let ConfiguredInputs {
        prepared,
        runtime,
        services,
        cache,
        identity,
    } = inputs;
    let activate = Box::new(move |stopping| {
        if stopping {
            return Err(Box::new(configured_hc::Failure {
                error: configured_hc::Error::Closed,
                open: prepared.into_open(),
            }));
        }
        prepared
            .activate(runtime, services)
            .map(|active| Activated {
                conductor: active.conductor,
                configured: active.open,
            })
    });
    Ok(spawn(
        Startup {
            conductor: None,
            activate: Some(activate),
            cache,
            identity,
            network,
        },
        config,
        clock,
        outputs,
    ))
}
fn spawn<O, F>(inputs: Startup<O>, config: Config, clock: driving::Input, outputs: F) -> Running<O>
where
    O: MapOrder + Clone + Send + Sync + 'static,
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    let status = Status {
        phase: Phase::Prepared,
        ready: false,
        running: true,
        joined: false,
        failure: None,
        workers: None,
        connections: None,
        producer: inputs.conductor.as_ref().map(Conductor::status),
        production: None,
        pending_key: None,
    };
    let (published, observed) = watch::channel(status.clone());
    let (stop, stopped) = watch::channel(false);
    let shared = Arc::new(Shared {
        stop,
        published,
        controls: Mutex::new(Controls {
            importer: None,
            pending_key: None,
            producer: None,
            connections: None,
            workers: None,
        }),
        outputs: Mutex::new(VecDeque::new()),
        notices: Mutex::new(VecDeque::new()),
        exit: Mutex::new(None),
    });
    let handle = Handle(shared.clone(), observed);
    let mut owner = Owner {
        activate: inputs.activate,
        configured: None,
        network: inputs.network,
        shared,
        stop: stopped,
        status,
        config,
        conductor: inputs.conductor,
        producer: None,
        worker: None,
        worker_task: None,
        connection: None,
        connection_task: None,
        pending_key: None,
    };
    let task = tokio::spawn(async move {
        if let Err(error) = owner
            .start(inputs.cache, inputs.identity, clock, outputs)
            .await
        {
            if error != Error::Closed || !*owner.stop.borrow() {
                owner.status.failure = Some(error);
            }
        } else if let Err(error) = owner.run().await {
            owner.status.failure = Some(error);
        }
        owner.finish().await;
    });
    Running {
        handle,
        task: Some(task),
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Owner<O> {
    fn publish(&mut self) {
        if let Some(pending) = &self.pending_key {
            self.status.pending_key = Some(pending.client().status());
        }
        if let Some(w) = &self.worker {
            self.status.workers = Some(w.status());
        }
        if let Some(c) = &self.connection {
            self.status.connections = Some(c.status());
        }
        if let Some(p) = &self.producer {
            self.status.producer = Some(p.status());
            self.status.production = p.production_status();
        }
        if self.status.phase == Phase::Running {
            self.status.ready = self.status.workers.as_ref().is_some_and(|s| s.ready)
                && self
                    .status
                    .pending_key
                    .as_ref()
                    .is_some_and(|s| s.running && s.accepting)
                && (!self.network || self.status.connections.as_ref().is_some_and(|s| s.ready))
                && self
                    .status
                    .production
                    .as_ref()
                    .is_none_or(|p| p.phase == production::Phase::Running);
        }
        self.shared.published.send_replace(self.status.clone());
    }
    fn phase(&mut self, p: Phase) {
        self.status.phase = p;
        self.publish();
    }
    fn stopping(&self) -> bool {
        *self.stop.borrow()
    }
    async fn pause(&mut self) {
        tokio::select! { biased; _=self.stop.changed()=>(), _=tokio::time::sleep(self.config.poll)=>() }
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
    async fn start<F>(
        &mut self,
        cache: CacheController,
        identity: Identity,
        clock: driving::Input,
        outputs: F,
    ) -> Result<(), Error>
    where
        F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
    {
        let end = Instant::now() + self.config.startup_timeout;
        if let Some(activate) = self.activate.take() {
            let stopping = self.stopping();
            // Consume accepted startup before observing cancellation. The
            // failure/active result retains the exact opened chain either way.
            match tokio::task::spawn_blocking(move || activate(stopping)).await {
                Ok(Ok(active)) => {
                    self.status.producer = Some(active.conductor.status());
                    self.conductor = Some(active.conductor);
                    self.configured = Some(active.configured);
                }
                Ok(Err(failure)) => {
                    let configured_hc::Failure { error, open } = *failure;
                    self.configured = Some(open);
                    return Err(match error {
                        configured_hc::Error::Closed => Error::Closed,
                        error => Error::Start(format!("{error:?}")),
                    });
                }
                Err(_) => return Err(Error::ChildTask(Child::Producer)),
            }
        }
        self.startup_check(end)?;
        self.phase(Phase::Workers);
        let conductor = self.conductor.as_ref().expect("startup conductor");
        let pending = pending::Worker::spawn_hyperchain(
            &conductor.importer(),
            conductor.status().block_producer,
            self.config.pending_key,
        )
        .map_err(Error::PendingKey)?;
        self.shared
            .controls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending_key = Some(pending.client());
        self.pending_key = Some(pending);
        self.shared
            .controls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .importer = Some(
            self.conductor
                .as_ref()
                .expect("startup conductor")
                .importer(),
        );
        let (handle, service) = workers::hyperchain::create(
            self.conductor.as_mut().expect("startup conductor"),
            self.config.workers.clone(),
        )
        .map_err(Error::Workers)?;
        self.shared
            .controls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .workers = Some(handle.clone());
        self.worker = Some(handle);
        self.worker_task = Some(tokio::spawn(service.run()));
        loop {
            self.startup_check(end)?;
            self.publish();
            let w = self.worker.as_ref().expect("workers").status();
            if let Some(error) = w.error {
                return Err(Error::Workers(error));
            }
            if self
                .worker_task
                .as_ref()
                .is_some_and(JoinHandle::is_finished)
                || !w.running
            {
                return Err(Error::ChildStopped(Child::Workers));
            }
            if w.ready {
                break;
            }
            self.pause().await;
        }
        if self.network {
            self.phase(Phase::Identity);
            let identity = match identity {
                Identity::Fixed(identity) => identity,
                Identity::Managed => {
                    let keys = self
                        .worker
                        .as_ref()
                        .expect("workers")
                        .bindings()
                        .keys
                        .and_then(|k| k.current())
                        .ok_or(Error::Identity)?;
                    let ticket = keys.peer_identity().map_err(|_| Error::Identity)?;
                    tokio::select! {
                        biased;
                        _=self.stop.changed()=>return Err(Error::Closed),
                        _=tokio::time::sleep_until(end)=>return Err(Error::StartupTimeout(Phase::Identity)),
                        result=ticket=>result.map_err(|_|Error::Identity)?,
                    }
                }
            };
            self.startup_check(end)?;
            self.phase(Phase::Connections);
            let (handle, service) = connection::create_hyperchain_with_workers(
                self.conductor.as_ref().expect("conductor"),
                self.worker.as_ref().expect("workers"),
                identity,
                cache,
                self.config.connections.clone(),
                outputs,
            )
            .map_err(Error::Connections)?;
            self.shared
                .controls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .connections = Some(handle.clone());
            self.connection = Some(handle);
            self.connection_task = Some(tokio::spawn(service.run()));
            loop {
                self.startup_check(end)?;
                self.publish();
                self.worker_health()?;
                let c = self.connection.as_ref().expect("connections").status();
                if let Some(error) = c.error {
                    return Err(Error::Connections(error));
                }
                if self
                    .connection_task
                    .as_ref()
                    .is_some_and(JoinHandle::is_finished)
                    || !c.running
                {
                    return Err(Error::ChildStopped(Child::Connections));
                }
                if c.ready {
                    break;
                }
                self.pause().await;
            }
        }
        self.startup_check(end)?;
        self.phase(Phase::Producer);
        let conductor = self.conductor.take().expect("conductor");
        let settings = self.config.producer;
        match tokio::task::spawn_blocking(move || {
            driving::Runtime::spawn(conductor, clock, settings)
        })
        .await
        .map_err(|_| Error::ChildTask(Child::Producer))?
        {
            Ok(runtime) => {
                self.shared
                    .controls
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .producer = Some(runtime.handle());
                self.producer = Some(runtime);
            }
            Err(e) => {
                let producer::SpawnError { error, conductor } = *e;
                self.conductor = Some(conductor);
                return Err(Error::ProducerSpawn(error));
            }
        }
        self.startup_check(end)?;
        self.status.ready = true;
        self.phase(Phase::Running);
        Ok(())
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
        let w = self
            .worker
            .as_ref()
            .ok_or(Error::ChildStopped(Child::Workers))?
            .status();
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
    async fn run(&mut self) -> Result<(), Error> {
        while !self.stopping() {
            self.worker_health()?;
            if self.network {
                let c = self
                    .connection
                    .as_ref()
                    .ok_or(Error::ChildStopped(Child::Connections))?
                    .status();
                if let Some(e) = c.error {
                    return Err(Error::Connections(e));
                }
                if !c.running
                    || self
                        .connection_task
                        .as_ref()
                        .is_some_and(JoinHandle::is_finished)
                {
                    return Err(Error::ChildStopped(Child::Connections));
                }
            }
            if self
                .producer
                .as_ref()
                .expect("running producer")
                .constructs_clock()
            {
                let mut p = self.producer.take().expect("running producer");
                // Constructor completion is accepted work. Await and retain
                // its result even when shutdown arrives; never detach it or
                // block the independent async workers on a factory call.
                let (runtime, result) = tokio::task::spawn_blocking(move || {
                    let result = p.poll();
                    (p, result)
                })
                .await
                .map_err(|_| Error::ChildTask(Child::Producer))?;
                self.producer = Some(runtime);
                result?;
            } else {
                self.producer.as_mut().expect("running producer").poll()?;
            }
            let p = self.producer.as_mut().expect("running producer");
            {
                let mut out = self
                    .shared
                    .outputs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                while out.len() < self.config.outputs {
                    let Some(o) = p.try_output() else { break };
                    out.push_back(o);
                }
            }
            {
                let mut notices = self
                    .shared
                    .notices
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if notices.len() < self.config.outputs
                    && let Some(n) = p.take_notice()
                {
                    notices.push_back(n);
                }
            }
            self.publish();
            self.pause().await;
        }
        Ok(())
    }
    async fn finish(&mut self) {
        self.status.ready = false;
        self.phase(Phase::Stopping);
        self.shared.stop.send_replace(true);
        let mut pending_joined = true;
        if let Some(pending) = self.pending_key.take() {
            pending.request_shutdown();
            match tokio::task::spawn_blocking(move || pending.shutdown()).await {
                Ok(status) => {
                    pending_joined = status.joined;
                    if let Some(error) = &status.failure {
                        self.status
                            .failure
                            .get_or_insert(Error::PendingKey(error.clone()));
                    }
                    self.status.pending_key = Some(status);
                }
                Err(_) => {
                    pending_joined = false;
                    self.status
                        .failure
                        .get_or_insert(Error::ChildTask(Child::PendingKey));
                }
            }
        }
        if let Some(control) = self.producer.as_ref().map(|p| p.handle()) {
            // The fence has independent capacity, so unread control receipts
            // and a blocked producer clock cannot prevent service quiescence.
            control.quiesce();
            while !control.quiesced()
                && !self
                    .producer
                    .as_ref()
                    .expect("producer")
                    .service_is_finished()
            {
                tokio::time::sleep(self.config.poll).await;
            }
        }
        // Peers drain accepted shared writes while the producer/pool authority
        // remains alive. Workers and producer then receive stop together.
        if let Some(c) = &self.connection {
            c.shutdown();
        }
        let mut joined =
            pending_joined && !matches!(self.status.failure, Some(Error::ChildTask(_)));
        if let Some(task) = self.connection_task.take() {
            match task.await {
                Ok(status) => {
                    joined &= status.joined;
                    if let Some(e) = &status.error {
                        self.status
                            .failure
                            .get_or_insert(Error::Connections(e.clone()));
                    }
                    self.status.connections = Some(status);
                }
                Err(_) => {
                    joined = false;
                    self.status
                        .failure
                        .get_or_insert(Error::ChildTask(Child::Connections));
                }
            }
        }
        if let Some(w) = &self.worker {
            w.shutdown();
        }
        let runtime = self.producer.take();
        let conductor = self.conductor.take();
        let producer = (runtime.is_some() || conductor.is_some()).then(|| {
            tokio::task::spawn_blocking(move || {
                if let Some(p) = runtime {
                    p.shutdown()
                } else {
                    (Ok(conductor.expect("unstarted conductor").shutdown()), None)
                }
            })
        });
        if let Some(task) = self.worker_task.take() {
            match task.await {
                Ok(status) => {
                    joined &= status.joined;
                    if let Some(e) = &status.error {
                        self.status.failure.get_or_insert(Error::Workers(e.clone()));
                    }
                    self.status.workers = Some(status);
                }
                Err(_) => {
                    joined = false;
                    self.status
                        .failure
                        .get_or_insert(Error::ChildTask(Child::Workers));
                }
            }
        }
        let (report, production) = match producer {
            Some(producer) => {
                let (result, group) = producer
                    .await
                    .unwrap_or((Err(Error::ChildTask(Child::Producer)), None));
                (Some(result), group)
            }
            None => (None, None),
        };
        if let Some(group) = &production {
            self.status.production = Some(group.status.clone());
            if let Some(error) = &group.status.failure {
                self.status
                    .failure
                    .get_or_insert(Error::Production(error.clone()));
            }
            if let Some(driver) = &group.driver {
                if driver.exit.is_err() {
                    joined = false;
                    self.status
                        .failure
                        .get_or_insert(Error::ChildTask(Child::Producer));
                } else if let Some(error) = &driver.failure {
                    self.status
                        .failure
                        .get_or_insert(Error::Producer(error.clone()));
                }
            }
        }
        match &report {
            Some(Ok(s)) => {
                self.status.producer = Some(s.status.clone());
                if let Some(e) = &s.status.failure {
                    self.status
                        .failure
                        .get_or_insert(Error::Producer(e.clone()));
                }
                if s.production_driver
                    .as_ref()
                    .is_some_and(|d| d.exit.is_err())
                {
                    joined = false;
                    self.status
                        .failure
                        .get_or_insert(Error::ChildTask(Child::Producer));
                }
                if s.pool.is_err()
                    || s.parent.as_ref().is_some_and(Result::is_err)
                    || !s.connector_joined
                    || !s.pins_joined
                    || !s.owner.errors.is_empty()
                    || s.pinning.as_ref().is_some_and(Result::is_err)
                    || s.resilience.as_ref().is_some_and(Result::is_err)
                {
                    joined = false;
                    self.status.failure.get_or_insert(Error::ProducerCleanup);
                }
            }
            Some(Err(e)) => {
                joined = false;
                self.status.failure.get_or_insert(e.clone());
            }
            None => (),
        }
        let mut retained: Vec<_> = self
            .shared
            .outputs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        for out in &mut retained {
            out.poll_pinning();
        }
        self.status.phase = Phase::Stopped;
        self.status.running = false;
        self.status.joined = joined;
        *self.shared.exit.lock().unwrap_or_else(|e| e.into_inner()) = Some(Exit {
            status: self.status.clone(),
            producer: report,
            retained,
            configured: self.configured.take(),
            production,
            production_notices: self
                .shared
                .notices
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .drain(..)
                .collect(),
        });
        self.shared.published.send_replace(self.status.clone());
    }
}

impl<O: MapOrder + Clone + Send + Sync + 'static> Drop for Owner<O> {
    fn drop(&mut self) {
        if let Some(pending) = &self.pending_key {
            pending.request_shutdown();
        }
        if self.status.phase != Phase::Stopped {
            self.shared.stop.send_replace(true);
            if let Some(c) = &self.connection {
                c.shutdown();
            }
            if let Some(w) = &self.worker {
                w.shutdown();
            }
            if let Some(p) = &self.producer {
                p.handle().request_shutdown();
            }
            self.status.phase = Phase::Stopped;
            self.status.running = false;
            self.status.ready = false;
            self.status.joined = false;
            self.status.failure.get_or_insert(Error::RootTask);
            self.shared.published.send_replace(self.status.clone());
        }
    }
}
