use super::*;
use crate::observe::supervision;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Condvar, Weak},
    task::{Context, Poll},
    thread,
    time::Instant,
};
use tokio::sync::{oneshot, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Manager {
    Admin,
    Cache,
    Alias,
}
const MANAGERS: [Manager; 3] = [Manager::Admin, Manager::Cache, Manager::Alias];

#[derive(Debug, Clone)]
pub struct ManagerInfo {
    pub manager: Manager,
    pub generation: u64,
    pub running: bool,
    pub joined: bool,
    pub previous_joined: bool,
    pub completed: u64,
    pub last_error: Option<Error>,
}
#[derive(Debug, Clone, Default)]
pub struct ServiceInfo {
    pub running: bool,
    pub joined: bool,
    pub closing: bool,
    pub restart_attempts: u64,
    pub pending: usize,
    pub pending_bytes: usize,
    pub managers: Vec<ManagerInfo>,
    pub last_error: Option<Error>,
    pub parent: supervision::Info,
}

struct Credit {
    parent: Weak<Parent>,
    bytes: usize,
}
impl Drop for Credit {
    fn drop(&mut self) {
        if let Some(parent) = self.parent.upgrade() {
            let mut state = parent.lock();
            state.info.pending -= 1;
            state.info.pending_bytes -= self.bytes;
            drop(state);
            parent.changed.send_replace(());
        }
    }
}
pub struct Ticket<T> {
    receiver: oneshot::Receiver<Result<T, Error>>,
    _credit: Arc<Credit>,
}
impl<T> Future for Ticket<T> {
    type Output = Result<T, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(cx)
            .map(|r| r.unwrap_or(Err(Error::Closed)))
    }
}
impl<T> Ticket<T> {
    pub fn blocking_wait(self) -> Result<T, Error> {
        self.receiver.blocking_recv().unwrap_or(Err(Error::Closed))
    }
}
type Completion = Box<dyn FnOnce() + Send>;
struct Task {
    admitted: Instant,
    _credit: Arc<Credit>,
    action: Box<dyn FnOnce(&Store) -> Completion + Send>,
}
struct Queue {
    tasks: VecDeque<Task>,
    info: ManagerInfo,
    closing: bool,
    abort: bool,
    woken: bool,
}
struct WorkerState {
    queue: Mutex<Queue>,
    wake: Condvar,
}
impl WorkerState {
    fn lock(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn stop(&self, abort: bool) {
        let discarded = {
            let mut q = self.lock();
            q.closing = true;
            q.abort |= abort;
            self.wake.notify_all();
            if abort {
                std::mem::take(&mut q.tasks)
            } else {
                VecDeque::new()
            }
        };
        // Task credits acquire the parent lock on drop, never under this lock.
        drop(discarded);
    }
}
#[derive(Clone)]
pub(super) struct Signal(Weak<WorkerState>);
impl Signal {
    pub(super) fn notify(&self) {
        if let Some(state) = self.0.upgrade() {
            state.lock().woken = true;
            state.wake.notify_one();
        }
    }
}
struct Restart {
    manager: Manager,
    generation: u64,
    reply: oneshot::Sender<Result<u64, Error>>,
    _credit: Arc<Credit>,
}
struct ParentState {
    info: ServiceInfo,
    slots: BTreeMap<Manager, Arc<WorkerState>>,
    restarts: VecDeque<Restart>,
}
struct Parent {
    state: Mutex<ParentState>,
    wake: Condvar,
    changed: watch::Sender<()>,
    done: watch::Sender<bool>,
    store: Arc<Store>,
    budget: supervision::Budget,
}
impl supervision::Halt for Parent {
    fn halt(&self) {
        let mut s = self.lock();
        s.info.closing = true;
        s.info.last_error = Some(Error::RestartLimit);
        drop(s);
        self.notify();
    }
}
impl Parent {
    fn lock(&self) -> std::sync::MutexGuard<'_, ParentState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn notify(&self) {
        // Synchronize with the supervisor's predicate check and condvar wait.
        let state = self.lock();
        self.wake.notify_all();
        drop(state);
        self.changed.send_replace(());
    }
    fn reserve(
        self: &Arc<Self>,
        state: &mut ParentState,
        bytes: usize,
    ) -> Result<Arc<Credit>, Error> {
        if state.info.closing || !state.info.running {
            return Err(Error::Closed);
        }
        let limits = self.store.limits;
        if state.info.pending >= limits.requests
            || bytes
                > limits
                    .pending_bytes
                    .saturating_sub(state.info.pending_bytes)
        {
            return Err(Error::Busy);
        }
        state.info.pending += 1;
        state.info.pending_bytes += bytes;
        Ok(Arc::new(Credit {
            parent: Arc::downgrade(self),
            bytes,
        }))
    }
}
#[derive(Clone)]
pub struct Client {
    parent: Arc<Parent>,
    updates: watch::Receiver<()>,
}
impl Client {
    pub fn info(&self) -> ServiceInfo {
        let state = self.parent.lock();
        let mut info = state.info.clone();
        info.parent = self.parent.budget.info();
        info.managers = state
            .slots
            .values()
            .map(|s| s.lock().info.clone())
            .collect();
        info
    }
    pub async fn changed(&mut self) {
        let _ = self.updates.changed().await;
    }
    pub fn shutdown(&self) {
        self.parent.lock().info.closing = true;
        self.parent.notify();
    }
    fn open(&self) -> Result<(), Error> {
        let state = self.parent.lock();
        if state.info.closing || !state.info.running {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
    fn call<T: Send + 'static>(
        &self,
        manager: Manager,
        bytes: usize,
        action: impl FnOnce(&Store) -> Result<T, Error> + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        let admitted = self.parent.store.clock.now().monotonic;
        let mut state = self.parent.lock();
        let worker = state.slots.get(&manager).ok_or(Error::Closed)?.clone();
        let mut q = worker.lock();
        if q.closing || !q.info.running {
            return Err(Error::Closed);
        }
        let credit = self.parent.reserve(&mut state, bytes)?;
        let (reply, receiver) = oneshot::channel();
        q.tasks.push_back(Task {
            admitted,
            _credit: credit.clone(),
            action: Box::new(move |store| {
                let result = action(store);
                Box::new(move || {
                    let _ = reply.send(result);
                })
            }),
        });
        worker.wake.notify_one();
        Ok(Ticket {
            receiver,
            _credit: credit,
        })
    }
    pub fn create(&self, name: &Name, kind: Kind, enabled: bool) -> Result<Ticket<()>, Error> {
        self.register(name, kind, enabled, false)
    }
    pub fn ensure(&self, name: &Name, kind: Kind, enabled: bool) -> Result<Ticket<()>, Error> {
        self.register(name, kind, enabled, true)
    }
    fn register(
        &self,
        name: &Name,
        kind: Kind,
        enabled: bool,
        ensure: bool,
    ) -> Result<Ticket<()>, Error> {
        let bytes = 128usize.saturating_add(self.parent.store.scalar_name(name)?);
        let name = name.clone();
        self.call(Manager::Admin, bytes, move |store| {
            if ensure {
                store.ensure(&name, kind, enabled)
            } else {
                store.create(&name, kind, enabled)
            }
        })
    }
    pub fn delete(&self, name: &Name) -> Result<Ticket<()>, Error> {
        let bytes = 128usize.saturating_add(self.parent.store.scalar_name(name)?);
        let name = name.clone();
        self.call(Manager::Admin, bytes, move |store| {
            store.remove(&name).map_err(|e| match e {
                StoreError::NotFound => Error::NotFound,
                StoreError::Closed => Error::Closed,
                StoreError::Sequence => Error::Sequence,
                _ => Error::Limit,
            })
        })
    }
    pub fn add_alias(&self, alias: &Alias, name: &Name, point: &Part) -> Result<Ticket<()>, Error> {
        let store = &self.parent.store;
        let key = alias.key(store.limits)?;
        let bytes = 128usize
            .saturating_add(key.len())
            .saturating_add(store.scalar_name(name)?)
            .saturating_add(store.limits.report.part(point).map_err(|_| Error::Limit)?);
        // Validate before admission and normalize capacities through cloning.
        if !matches!(point, Part::Atom(_) | Part::Integer(_)) {
            return Err(Error::InvalidAlias);
        }
        let (alias, name, point) = (Alias::Binary(key), name.clone(), point.clone());
        self.call(Manager::Alias, bytes, move |store| {
            store.add_alias(&alias, &name, &point)
        })
    }
    pub fn delete_alias(&self, alias: &Alias) -> Result<Ticket<()>, Error> {
        let key = alias.key(self.parent.store.limits)?;
        let bytes = 128usize.saturating_add(key.len());
        let alias = Alias::Binary(key);
        self.call(Manager::Alias, bytes, move |store| {
            store.delete_alias(&alias)
        })
    }
    pub fn barrier(&self, manager: Manager) -> Result<Ticket<()>, Error> {
        self.call(manager, 128, |_| Ok(()))
    }
    pub fn restart(&self, manager: Manager, generation: u64) -> Result<Ticket<u64>, Error> {
        let mut state = self.parent.lock();
        let observed = state
            .slots
            .get(&manager)
            .ok_or(Error::Closed)?
            .lock()
            .info
            .generation;
        if generation != observed {
            return Err(Error::Stale);
        }
        let credit = self.parent.reserve(&mut state, 128)?;
        let (reply, receiver) = oneshot::channel();
        state.restarts.push_back(Restart {
            manager,
            generation,
            reply,
            _credit: credit.clone(),
        });
        self.parent.wake.notify_one();
        Ok(Ticket {
            receiver,
            _credit: credit,
        })
    }
    pub fn update(&self, name: &Name, value: &Value) -> Result<(), Error> {
        self.open()?;
        self.parent.store.update(name, value)
    }
    pub fn reset(&self, name: &Name) -> Result<(), Error> {
        self.open()?;
        self.parent.store.reset(name)
    }
    pub fn get_value(&self, name: &Name, points: &Points) -> Result<Reading, Error> {
        self.open()?;
        self.parent.store.get_value(name, points)
    }
    pub fn metric_info(&self, name: &Name) -> Result<Info, Error> {
        self.open()?;
        self.parent.store.info(name)
    }
    pub fn set_enabled(&self, name: &Name, enabled: bool) -> Result<(), Error> {
        self.open()?;
        self.parent.store.set_enabled(name, enabled)
    }
    pub fn set_global_enabled(&self, enabled: bool) -> Result<bool, Error> {
        self.open()?;
        self.parent.store.set_global_enabled(enabled)
    }
    pub fn set_cache(&self, name: &Name, ttl_ms: u64) -> Result<(), Error> {
        self.open()?;
        self.parent.store.set_cache(name, ttl_ms)
    }
    pub fn resolve_alias(&self, alias: &Alias) -> Result<Option<(Name, Part)>, Error> {
        self.open()?;
        self.parent.store.resolve_alias(alias)
    }
    pub fn alias_value(&self, alias: &Alias) -> Result<Reading, Error> {
        self.open()?;
        self.parent.store.alias_value(alias)
    }
    pub fn update_alias(&self, alias: &Alias, value: &Value) -> Result<(), Error> {
        self.open()?;
        self.parent.store.update_alias(alias, value)
    }
    pub fn find_entries(&self, prefix: &Name) -> Result<Vec<Entry>, Error> {
        self.open()?;
        self.parent.store.find_entries(prefix)
    }
    pub fn aggregate(
        &self,
        prefix: &Name,
        points: &Points,
    ) -> Result<Vec<(Part, num_bigint::BigInt)>, Error> {
        self.open()?;
        self.parent.store.aggregate(prefix, points)
    }
    pub fn cache_write(
        &self,
        name: &Name,
        point: &Part,
        value: &Value,
        ttl_ms: u64,
    ) -> Result<u64, Error> {
        self.open()?;
        self.parent.store.cache_write(name, point, value, ttl_ms)
    }
    pub fn cache_read(&self, name: &Name, point: &Part) -> Result<Option<Value>, Error> {
        self.open()?;
        self.parent.store.cache_read(name, point)
    }
    pub fn cache_info(&self, name: &Name, point: &Part) -> Result<Option<CacheInfo>, Error> {
        self.open()?;
        self.parent.store.cache_info(name, point)
    }
    pub fn cache_delete(&self, name: &Name, point: Option<&Part>) -> Result<(), Error> {
        self.open()?;
        self.parent.store.cache_delete(name, point)
    }
}

struct Worker {
    state: Arc<WorkerState>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Worker {
    fn spawn(parent: &Arc<Parent>, manager: Manager, generation: u64) -> Result<Self, Error> {
        let state = Arc::new(WorkerState {
            queue: Mutex::new(Queue {
                tasks: VecDeque::new(),
                closing: false,
                abort: false,
                woken: false,
                info: ManagerInfo {
                    manager,
                    generation,
                    running: true,
                    joined: false,
                    previous_joined: generation > 1,
                    completed: 0,
                    last_error: None,
                },
            }),
            wake: Condvar::new(),
        });
        let child = state.clone();
        let store = parent.store.clone();
        let supervisor = Arc::downgrade(parent);
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name(format!("ae-metrics-{manager:?}").to_lowercase())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || -> Result<(), Error> {
                        if manager == Manager::Cache {
                            store.cache_turn(
                                Some(Signal(Arc::downgrade(&child))),
                                true,
                                store.clock.now(),
                            )?;
                        }
                        let _ = ready.send(());
                        worker_loop(&store, &child, manager)
                    },
                ))
                .unwrap_or(Err(Error::Panicked));
                let discarded = {
                    let mut q = child.lock();
                    q.info.running = false;
                    q.info.last_error = result.err();
                    q.closing = true;
                    std::mem::take(&mut q.tasks)
                };
                drop(discarded);
                if let Some(parent) = supervisor.upgrade() {
                    parent.notify();
                }
            })
            .map_err(|_| Error::Spawn)?;
        let mut worker = Self {
            state,
            thread: Some(thread),
        };
        if started.recv().is_err() {
            worker.join();
            return Err(worker
                .state
                .lock()
                .info
                .last_error
                .clone()
                .unwrap_or(Error::Spawn));
        }
        Ok(worker)
    }
    fn join(&mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.state.lock().info.joined = true;
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.state.stop(true);
        self.join();
    }
}
fn worker_loop(store: &Store, state: &Arc<WorkerState>, manager: Manager) -> Result<(), Error> {
    loop {
        let time = store.clock.now();
        let due = if manager == Manager::Cache {
            store.cache_turn(None, false, time)?
        } else {
            None
        };
        let mut q = state.lock();
        if q.abort || (q.closing && q.tasks.is_empty()) {
            return Ok(());
        }
        if q.woken {
            q.woken = false;
            drop(q);
            continue;
        }
        let task_first = q
            .tasks
            .front()
            .is_some_and(|task| due.is_none_or(|due| task.admitted <= due));
        if task_first {
            let task = q.tasks.pop_front().expect("queued task");
            drop(q);
            let complete = (task.action)(store);
            state.lock().info.completed += 1;
            complete();
            continue;
        }
        if let Some(due) = due {
            if due <= time.monotonic {
                drop(q);
                store.expire_next(time)?;
                continue;
            }
            drop(
                state
                    .wake
                    .wait_timeout(q, due.saturating_duration_since(time.monotonic))
                    .unwrap_or_else(|e| e.into_inner()),
            );
        } else {
            drop(state.wake.wait(q).unwrap_or_else(|e| e.into_inner()));
        }
    }
}

pub struct Owner {
    client: Client,
    thread: Option<thread::JoinHandle<()>>,
}
impl Owner {
    pub fn spawn(store: Arc<Store>) -> Result<Self, Error> {
        Self::spawn_with_parent(store, supervision::Budget::default())
    }
    pub fn spawn_with_parent(
        store: Arc<Store>,
        budget: supervision::Budget,
    ) -> Result<Self, Error> {
        store.claim()?;
        let result = Self::start(store.clone(), budget);
        if result.is_err() {
            store.close();
        }
        result
    }
    fn start(store: Arc<Store>, budget: supervision::Budget) -> Result<Self, Error> {
        let parent = Arc::new(Parent {
            state: Mutex::new(ParentState {
                info: ServiceInfo {
                    running: true,
                    ..Default::default()
                },
                slots: BTreeMap::new(),
                restarts: VecDeque::new(),
            }),
            wake: Condvar::new(),
            changed: watch::channel(()).0,
            done: watch::channel(false).0,
            store,
            budget,
        });
        parent
            .budget
            .bind(supervision::Group::Registry, parent.clone())
            .map_err(|e| match e {
                supervision::BindError::Exists => Error::AlreadyRunning,
                supervision::BindError::Exhausted => Error::RestartLimit,
            })?;
        let mut workers = BTreeMap::new();
        for manager in MANAGERS {
            let worker = Worker::spawn(&parent, manager, 1)?;
            parent.lock().slots.insert(manager, worker.state.clone());
            workers.insert(manager, worker);
        }
        let state = parent.clone();
        let thread = thread::Builder::new()
            .name("ae-metrics-registry-owner".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    supervise(&state, &mut workers)
                }))
                .unwrap_or(Err(Error::Panicked));
                let discarded = {
                    let mut s = state.lock();
                    s.info.closing = true;
                    if let Err(e) = &result {
                        s.info.last_error = Some(e.clone());
                    }
                    std::mem::take(&mut s.restarts)
                };
                drop(discarded);
                // Close all admissions before waiting for any in-flight command.
                for worker in workers.values() {
                    worker.state.stop(result.is_err());
                }
                for worker in workers.values_mut().rev() {
                    worker.join();
                }
                state.lock().info.running = false;
                state.done.send_replace(true);
                state.changed.send_replace(());
            })
            .map_err(|_| Error::Spawn)?;
        Ok(Self {
            client: Client {
                updates: parent.changed.subscribe(),
                parent,
            },
            thread: Some(thread),
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub async fn finish(&mut self) {
        self.client.shutdown();
        let mut done = self.client.parent.done.subscribe();
        while !*done.borrow_and_update() {
            let _ = done.changed().await;
        }
        self.join();
    }
    fn join(&mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        // Stable tables outlive manager shutdown until the enclosing owner has
        // joined its reporters and producers and explicitly releases the store.
        self.client.parent.store.close();
        self.client.parent.lock().info.joined = true;
        self.client.parent.changed.send_replace(());
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.client.shutdown();
        self.join();
    }
}
fn supervise(parent: &Arc<Parent>, workers: &mut BTreeMap<Manager, Worker>) -> Result<(), Error> {
    loop {
        let mut s = parent.lock();
        if s.info.closing {
            return if parent.budget.info().exhausted {
                Err(Error::RestartLimit)
            } else {
                Ok(())
            };
        }
        let restart = s.restarts.pop_front();
        let failed = workers
            .iter()
            .find(|(_, w)| !w.state.lock().info.running)
            .map(|(m, _)| *m);
        let manager = match (&restart, failed) {
            (Some(restart), _) => restart.manager,
            (None, Some(manager)) => manager,
            (None, None) => {
                drop(parent.wake.wait(s).unwrap_or_else(|e| e.into_inner()));
                continue;
            }
        };
        drop(s);
        let worker = workers.get_mut(&manager).expect("owned manager");
        let generation = worker.state.lock().info.generation;
        if restart.as_ref().is_some_and(|r| r.generation != generation) {
            if let Some(restart) = restart {
                let _ = restart.reply.send(Err(Error::Stale));
            }
            continue;
        }
        worker.state.stop(true);
        worker.join();
        parent.lock().info.restart_attempts += 1;
        let child = match manager {
            Manager::Admin => supervision::Child::Admin,
            Manager::Cache => supervision::Child::Cache,
            Manager::Alias => supervision::Child::Alias,
        };
        let result = if !parent.budget.record(child) {
            Err(Error::RestartLimit)
        } else if parent.lock().info.closing {
            Err(Error::Closed)
        } else {
            generation
                .checked_add(1)
                .ok_or(Error::Sequence)
                .and_then(|next| {
                    let replacement = Worker::spawn(parent, manager, next)?;
                    parent
                        .lock()
                        .slots
                        .insert(manager, replacement.state.clone());
                    workers.insert(manager, replacement);
                    Ok(next)
                })
        };
        if let Some(restart) = restart {
            let _ = restart.reply.send(result.clone());
        }
        parent.changed.send_replace(());
        match result {
            Ok(_) => (),
            Err(Error::Closed) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests;
