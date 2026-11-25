use super::*;

pub struct Owner {
    client: Client,
    thread: Option<thread::JoinHandle<()>>,
}
impl Owner {
    pub fn spawn(
        limits: Limits,
        source: Arc<dyn Source>,
        output: statsd::Client,
    ) -> Result<Self, Error> {
        Self::spawn_with_clock(limits, source, output, Arc::new(SystemClock))
    }
    pub fn spawn_with_clock(
        limits: Limits,
        source: Arc<dyn Source>,
        output: statsd::Client,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        Self::start(limits, source, output, clock, None)
    }
    pub fn spawn_with_parent(
        limits: Limits,
        source: Arc<dyn Source>,
        output: statsd::Client,
        parent: supervision::Budget,
    ) -> Result<Self, Error> {
        Self::spawn_with_clock_and_parent(limits, source, output, Arc::new(SystemClock), parent)
    }
    pub fn spawn_with_clock_and_parent(
        limits: Limits,
        source: Arc<dyn Source>,
        output: statsd::Client,
        clock: Arc<dyn Clock>,
        parent: supervision::Budget,
    ) -> Result<Self, Error> {
        Self::start(limits, source, output, clock, Some(parent))
    }
    fn start(
        limits: Limits,
        source: Arc<dyn Source>,
        output: statsd::Client,
        clock: Arc<dyn Clock>,
        parent: Option<supervision::Budget>,
    ) -> Result<Self, Error> {
        limits.validate()?;
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                commands: VecDeque::new(),
                info: Info::default(),
                closing: false,
                woken: false,
                retiring: false,
                restart: None,
            }),
            wake: Condvar::new(),
            changed: watch::channel(()).0,
            done: watch::channel(false).0,
            limits,
            clock,
            parent,
        });
        if let Some(parent) = &shared.parent {
            parent
                .bind(supervision::Group::Report, shared.clone())
                .map_err(|e| match e {
                    supervision::BindError::Exists => Error::AlreadyRunning,
                    supervision::BindError::Exhausted => Error::RestartLimit,
                })?;
        }
        source.watch(Signal(Arc::downgrade(&shared)))?;
        let engine = Arc::new(Mutex::new(Engine {
            shared: shared.clone(),
            source,
            output,
            reporters: BTreeMap::new(),
            subs: vec![],
            timer: 0,
            catalog_revision: None,
            catalog: BTreeMap::new(),
            info: Info::default(),
        }));
        let client = Client {
            updates: shared.changed.subscribe(),
            shared: shared.clone(),
        };
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("ae-metrics-report-owner".into())
            .spawn(move || {
                supervise(shared, engine, ready);
            })
            .map_err(|_| Error::Spawn)?;
        let mut owner = Self {
            client,
            thread: Some(thread),
        };
        match started.recv().unwrap_or(Err(Error::Panicked)) {
            Ok(()) => Ok(owner),
            Err(e) => {
                owner.client.shutdown();
                owner.join();
                Err(e)
            }
        }
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub async fn finish(&mut self) {
        self.client.shutdown();
        let mut done = self.client.shared.done.subscribe();
        while !*done.borrow_and_update() {
            let _ = done.changed().await;
        }
        self.join();
    }
    fn join(&mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.client.shared.lock().info.joined = true;
        self.client.shared.changed.send_replace(());
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.client.shutdown();
        self.join();
    }
}

fn manager(
    shared: &Arc<Shared>,
    engine: &Arc<Mutex<Engine>>,
    generation: u64,
    recovering: bool,
) -> Result<thread::JoinHandle<()>, Error> {
    let state = shared.clone();
    let retained = engine.clone();
    let (ready, started) = std::sync::mpsc::sync_channel(1);
    let thread = thread::Builder::new()
        .name("ae-metrics-report-manager".into())
        .spawn(move || {
            let mut ready = Some(ready);
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<(), Error> {
                    let mut engine = retained.lock().unwrap_or_else(|e| e.into_inner());
                    if state.lock().closing {
                        return Err(Error::Closed);
                    }
                    if recovering {
                        engine.recover_manager()?;
                    }
                    {
                        let mut q = state.lock();
                        if q.closing {
                            return Err(Error::Closed);
                        }
                        q.info.running = true;
                        q.info.recovering = false;
                        q.info.manager_generation = generation;
                        q.info.manager_previous_joined = recovering;
                        q.retiring = false;
                    }
                    let _ = ready.take().expect("startup acknowledgement").send(Ok(()));
                    state.changed.send_replace(());
                    run(&state, &mut engine);
                    Ok(())
                }))
                .unwrap_or(Err(Error::Panicked));
            let discarded = {
                let mut q = state.lock();
                q.info.running = false;
                q.info.recovering = state.parent.is_some() && !q.closing;
                if let Err(e) = &result {
                    q.info.last_manager_error = Some(e.clone());
                    if q.info.last_error != Some(Error::RestartLimit) {
                        q.info.last_error = Some(e.clone());
                    }
                }
                std::mem::take(&mut q.commands)
            };
            drop(discarded);
            if let Some(ready) = ready {
                let _ = ready.send(result);
            }
            state.changed.send_replace(());
        })
        .map_err(|_| Error::Spawn)?;
    match started.recv().unwrap_or(Err(Error::Panicked)) {
        Ok(()) => Ok(thread),
        Err(e) => {
            let _ = thread.join();
            Err(e)
        }
    }
}
fn reply(request: Option<ManagerRestart>, result: Result<u64, Error>) {
    if let Some(request) = request {
        let _ = request.reply.send(result);
    }
}
fn supervise(
    shared: Arc<Shared>,
    engine: Arc<Mutex<Engine>>,
    ready: std::sync::mpsc::SyncSender<Result<(), Error>>,
) {
    let mut ready = Some(ready);
    let mut pending = None;
    let mut generation = 1;
    let mut recovering = false;
    loop {
        let result = manager(&shared, &engine, generation, recovering);
        if let Some(ready) = ready.take() {
            let _ = ready.send(result.as_ref().map(|_| ()).map_err(Clone::clone));
        }
        match result {
            Ok(thread) => {
                reply(pending.take(), Ok(generation));
                let _ = thread.join();
            }
            Err(e) => {
                reply(pending.take(), Err(e.clone()));
                shared.lock().info.last_error = Some(e);
                if !recovering {
                    break;
                }
            }
        }
        let mut q = shared.lock();
        pending = q.restart.take();
        if q.closing || shared.parent.is_none() {
            break;
        }
        q.info.recovering = true;
        q.info.manager_restart_attempts = q.info.manager_restart_attempts.saturating_add(1);
        let Some(next) = q.info.manager_generation.checked_add(1) else {
            q.info.last_error = Some(Error::Sequence);
            break;
        };
        generation = next;
        drop(q);
        if !shared
            .parent
            .as_ref()
            .expect("configured parent")
            .record(supervision::Child::Report)
        {
            reply(pending.take(), Err(Error::RestartLimit));
            break;
        }
        recovering = true;
        shared.changed.send_replace(());
    }
    let discarded = {
        let mut q = shared.lock();
        q.closing = true;
        q.info.running = false;
        q.info.recovering = false;
        std::mem::take(&mut q.commands)
    };
    drop(discarded);
    reply(pending, Err(Error::Closed));
    let remaining = shared.lock().restart.take();
    reply(remaining, Err(Error::Closed));
    {
        let mut engine = engine.lock().unwrap_or_else(|e| e.into_inner());
        let abort = shared.parent.as_ref().is_some_and(|p| p.info().exhausted);
        for r in engine.reporters.values_mut() {
            if let Some(mut worker) = r.worker.take() {
                worker.stop(abort);
            }
        }
        engine.reporters.clear();
        engine.subs.clear();
        engine.catalog.clear();
    }
    shared.done.send_replace(true);
    shared.changed.send_replace(());
}
