use super::*;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex, Weak},
    task::{Context, Poll},
    thread::{self, JoinHandle},
};
use tokio::sync::oneshot;

pub struct Ticket<T>(oneshot::Receiver<Result<T, Error>>);
impl<T> Ticket<T> {
    pub fn blocking_wait(self) -> Result<T, Error> {
        self.0.blocking_recv().map_err(|_| Error::Disconnected)?
    }
}
impl<T> Future for Ticket<T> {
    type Output = Result<T, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|r| r.unwrap_or(Err(Error::Disconnected)))
    }
}
fn ready<T>(value: T) -> Ticket<T> {
    let (tx, rx) = oneshot::channel();
    let _ = tx.send(Ok(value));
    Ticket(rx)
}
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub running: bool,
    pub accepting: bool,
    pub pending: usize,
    pub pending_bytes: usize,
    pub submitted: u64,
    pub completed: u64,
    pub last_error: Option<Error>,
    pub failure: Option<Error>,
}
type Completion = Box<dyn FnOnce() + Send>;
type Action = Box<dyn FnOnce(Option<&mut Engine>) -> (Option<Error>, Completion) + Send>;
struct Command {
    bytes: usize,
    action: Action,
}
struct Queue {
    commands: VecDeque<Command>,
    info: Info,
}
struct Shared {
    queue: Mutex<Queue>,
    wake: Condvar,
    limits: Limits,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn stop(&self) {
        self.lock().info.accepting = false;
        self.wake.notify_all();
    }
}
struct Table {
    engine: Option<Engine>,
    worker: Weak<Shared>,
}
#[derive(Clone)]
pub struct Store(Arc<Mutex<Table>>);
impl Store {
    fn lock(&self) -> std::sync::MutexGuard<'_, Table> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn try_table(&self) -> Option<std::sync::MutexGuard<'_, Table>> {
        match self.0.try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::Poisoned(e)) => Some(e.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        }
    }
    // Inspecting this stable table does not redirect an accepted worker request.
    pub fn cells(&self) -> Result<Vec<Cell>, Error> {
        Ok(self.lock().engine.as_ref().ok_or(Error::Closed)?.cells())
    }
    pub fn cached(&self, key: &Key) -> Result<Destinations, Error> {
        self.lock()
            .engine
            .as_ref()
            .ok_or(Error::Closed)?
            .cached(key)
    }
    pub fn is_closed(&self) -> bool {
        self.lock().engine.is_none()
    }
}
pub struct Owner {
    store: Store,
}
impl Owner {
    pub fn new(setup: Setup) -> Result<Self, Error> {
        Ok(Self {
            store: Store(Arc::new(Mutex::new(Table {
                engine: Some(Engine::new(setup.config, setup.entries, setup.atoms)?),
                worker: Weak::new(),
            }))),
        })
    }
    pub fn store(&self) -> Store {
        self.store.clone()
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let mut table = self.store.lock();
        table.engine = None;
        if let Some(worker) = table.worker.upgrade() {
            worker.stop();
        }
    }
}
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
    store: Store,
}
impl Client {
    pub(crate) fn cached(&self, key: &Key) -> Result<Destinations, Error> {
        self.store.cached(key)
    }
    pub(crate) fn retained_destinations(&self, key: &Key) -> Result<Destinations, Error> {
        let mut snapshot = {
            let table = self.store.lock();
            let engine = table.engine.as_ref().ok_or(Error::Closed)?;
            if let Some(destinations) = engine.cached(key)? {
                return Ok(Some(destinations));
            }
            engine.clone()
        };
        // A read-only rule lookup belongs to the retained owner, including
        // while its command worker is withdrawn. Resolve misses on a bounded
        // snapshot; stale clients still cannot mutate a replacement's table.
        snapshot.resolve(key)
    }
    pub fn info(&self) -> Info {
        self.shared.lock().info.clone()
    }
    fn key_bytes(&self, key: &Key) -> Result<usize, Error> {
        let l = self.shared.limits;
        l.name(&key.name)?;
        l.name(&vec![key.datapoint.clone()])?;
        Ok(64 + key.name.iter().map(Part::bytes).sum::<usize>() + key.datapoint.bytes())
    }
    fn entries_bytes(&self, entries: &[Entry]) -> Result<usize, Error> {
        let l = self.shared.limits;
        if entries.len() > l.metrics {
            return Err(Error::Limit);
        }
        let mut total = 64usize;
        for e in entries {
            l.name(&e.name)?;
            if e.kind.chars().count() > 255 {
                return Err(Error::Limit);
            }
            total = total
                .saturating_add(64 + e.kind.len() + e.name.iter().map(Part::bytes).sum::<usize>());
        }
        Ok(total)
    }
    fn submit<T: Send + 'static>(
        &self,
        bytes: usize,
        work: impl FnOnce(&mut Engine) -> Result<T, Error> + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        let mut queue = self.shared.lock();
        if !queue.info.accepting {
            return Err(Error::Stopped);
        }
        let l = self.shared.limits;
        if queue.info.pending >= l.requests
            || bytes > l.pending_bytes.saturating_sub(queue.info.pending_bytes)
        {
            return Err(Error::Busy);
        }
        let (reply, result) = oneshot::channel();
        queue.commands.push_back(Command {
            bytes,
            action: Box::new(move |engine| {
                let result = engine.ok_or(Error::Closed).and_then(work);
                let error = result.as_ref().err().cloned();
                (
                    error,
                    Box::new(move || {
                        let _ = reply.send(result);
                    }),
                )
            }),
        });
        queue.info.pending += 1;
        queue.info.pending_bytes += bytes;
        queue.info.submitted += 1;
        self.shared.wake.notify_one();
        Ok(Ticket(result))
    }
    pub fn destinations(&self, key: Key) -> Result<Ticket<Destinations>, Error> {
        let bytes = self.key_bytes(&key)?;
        if let Some(table) = self.store.try_table() {
            let engine = table.engine.as_ref().ok_or(Error::Closed)?;
            if let Some(value) = engine.cached(&key)? {
                return Ok(ready(Some(value)));
            }
        }
        self.submit(bytes, move |s| s.destinations(&key))
    }
    pub fn add(&self, key: Key, destination: Destination) -> Result<Ticket<()>, Error> {
        let bytes = self.key_bytes(&key)?;
        self.submit(bytes, move |s| s.add(key, destination))
    }
    pub fn delete(&self, key: Key, destination: Destination) -> Result<Ticket<bool>, Error> {
        let bytes = self.key_bytes(&key)?;
        self.submit(bytes, move |s| s.delete(&key, destination))
    }
    pub fn register(&self, entries: Vec<Entry>) -> Result<Ticket<()>, Error> {
        let bytes = self.entries_bytes(&entries)?;
        self.submit(bytes, move |s| s.register(entries))
    }
    pub fn unregister(&self, name: Name) -> Result<Ticket<bool>, Error> {
        self.shared.limits.name(&name)?;
        let bytes = 64 + name.iter().map(Part::bytes).sum::<usize>();
        self.submit(bytes, move |s| s.unregister(&name))
    }
    pub fn check_config(&self, rules: Vec<Rule>) -> Result<Ticket<()>, Error> {
        let l = self.shared.limits;
        if rules.len() > l.rules {
            return Err(Error::Limit);
        }
        let mut bytes = 64usize;
        for r in &rules {
            l.rule(r)?;
            bytes = bytes.saturating_add(
                128 + r.name.len() + r.kind.len() + r.datapoints.len() + r.actions.len(),
            );
        }
        self.submit(bytes, move |s| s.check_config(rules))
    }
    pub fn route(&self, entries: Vec<Entry>, keys: Vec<Key>) -> Result<Ticket<Routes>, Error> {
        let mut bytes = self.entries_bytes(&entries)?;
        if keys.len() > self.shared.limits.metrics {
            return Err(Error::Limit);
        }
        for key in &keys {
            bytes = bytes.saturating_add(self.key_bytes(key)?);
        }
        if let Some(table) = self.store.try_table() {
            let engine = table.engine.as_ref().ok_or(Error::Closed)?;
            if let Some(values) = engine.cached_routes(&entries, &keys)? {
                return Ok(ready(values));
            }
        }
        self.submit(bytes, move |s| s.route(entries, keys))
    }
}
pub struct Worker {
    client: Client,
    thread: Option<JoinHandle<()>>,
}
impl Worker {
    pub fn spawn(store: Store) -> Result<Self, Error> {
        let mut table = store.lock();
        let engine = table.engine.as_ref().ok_or(Error::Closed)?;
        let limits = engine.config.limits;
        if table
            .worker
            .upgrade()
            .is_some_and(|s| s.lock().info.running)
        {
            return Err(Error::AlreadyRunning);
        }
        let shared = Arc::new(Shared {
            limits,
            queue: Mutex::new(Queue {
                commands: VecDeque::new(),
                info: Info {
                    running: true,
                    accepting: true,
                    ..Default::default()
                },
            }),
            wake: Condvar::new(),
        });
        let client = Client {
            shared: shared.clone(),
            store: store.clone(),
        };
        let owned = client.clone();
        table.worker = Arc::downgrade(&shared);
        let thread = thread::Builder::new()
            .name("ae-metric-routing".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&owned)));
                let mut queue = owned.shared.lock();
                queue.info.running = false;
                queue.info.accepting = false;
                queue.info.failure = result.err().map(|_| Error::Panicked);
                queue.info.pending = 0;
                queue.info.pending_bytes = 0;
                queue.commands.clear();
            })
            .map_err(|_| Error::Spawn)?;
        drop(table);
        Ok(Self {
            client,
            thread: Some(thread),
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn info(&self) -> Info {
        self.client.info()
    }
    pub fn request_shutdown(&self) {
        self.client.shared.stop()
    }
    fn join(&mut self) -> Result<(), Error> {
        self.request_shutdown();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| Error::Panicked)?;
        }
        self.info().failure.map_or(Ok(()), Err)
    }
    pub fn shutdown(mut self) -> Result<(), Error> {
        self.join()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.join();
    }
}
fn run(client: &Client) {
    loop {
        let command = {
            let queue = client.shared.lock();
            let mut queue = client
                .shared
                .wake
                .wait_while(queue, |q| q.info.accepting && q.commands.is_empty())
                .unwrap_or_else(|e| e.into_inner());
            let Some(command) = queue.commands.pop_front() else {
                break;
            };
            command
        };
        let (error, reply) = {
            let mut table = client.store.lock();
            (command.action)(table.engine.as_mut())
        };
        {
            let mut queue = client.shared.lock();
            queue.info.pending -= 1;
            queue.info.pending_bytes -= command.bytes;
            queue.info.completed += 1;
            queue.info.last_error = error;
        }
        reply();
    }
}
#[cfg(test)]
pub(super) fn pause(
    client: &Client,
) -> (
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::SyncSender<()>,
) {
    let (enter, entered) = std::sync::mpsc::sync_channel(1);
    let (release, released) = std::sync::mpsc::sync_channel(1);
    drop(
        client
            .submit(64, move |_| {
                enter.send(()).unwrap();
                released.recv().unwrap();
                Ok(())
            })
            .unwrap(),
    );
    (entered, release)
}
#[cfg(test)]
pub(super) fn fail(client: &Client) {
    drop(
        client
            .submit::<()>(64, |_| panic!("injected metrics worker failure"))
            .unwrap(),
    );
}
