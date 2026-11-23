use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll},
    thread::{self, JoinHandle},
    time::Duration,
};
use tokio::sync::oneshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    Configuration,
    Busy,
    Stopped,
    Disconnected,
    Panicked,
    Spawn,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub requests: usize,
    pub bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            requests: 128,
            bytes: 8 << 20,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Info<E> {
    pub running: bool,
    pub accepting: bool,
    pub joined: bool,
    pub pending: usize,
    pub control_pending: usize,
    pub bytes: usize,
    pub submitted: u64,
    pub completed: u64,
    pub last_error: Option<E>,
    pub failure: Option<E>,
}
impl<E> Default for Info<E> {
    fn default() -> Self {
        Self {
            running: false,
            accepting: false,
            joined: false,
            pending: 0,
            control_pending: 0,
            bytes: 0,
            submitted: 0,
            completed: 0,
            last_error: None,
            failure: None,
        }
    }
}
pub(crate) trait Actor: Send + 'static {
    type Error: From<Fault> + Clone + Send + 'static;
    const CONTROL_REQUESTS: usize = 0;
    // Bounded local cleanup also runs while idle. It never starts detached work.
    fn before(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn fatal(&self, _error: &Self::Error) -> bool {
        false
    }
}
pub struct Ticket<T, E>(oneshot::Receiver<Result<T, E>>);
impl<T, E: From<Fault>> Ticket<T, E> {
    pub fn blocking_wait(self) -> Result<T, E> {
        self.0
            .blocking_recv()
            .map_err(|_| E::from(Fault::Disconnected))?
    }
}
impl<T, E: From<Fault>> Future for Ticket<T, E> {
    type Output = Result<T, E>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|r| r.unwrap_or_else(|_| Err(Fault::Disconnected.into())))
    }
}
struct Completion<E> {
    error: Option<E>,
    reply: Box<dyn FnOnce() + Send>,
}
trait Work<A: Actor>: Send {
    fn run(self: Box<Self>, state: &mut A) -> Completion<A::Error>;
}
struct Job<T, F, E> {
    action: F,
    reply: oneshot::Sender<Result<T, E>>,
}
impl<A, T, F> Work<A> for Job<T, F, A::Error>
where
    A: Actor,
    T: Send + 'static,
    F: FnOnce(&mut A) -> Result<T, A::Error> + Send + 'static,
{
    fn run(self: Box<Self>, state: &mut A) -> Completion<A::Error> {
        let Self { action, reply } = *self;
        let result = state.before().and_then(|()| action(state));
        Completion {
            error: result.as_ref().err().cloned(),
            reply: Box::new(move || {
                let _ = reply.send(result);
            }),
        }
    }
}
struct Queued<A: Actor> {
    control: bool,
    bytes: usize,
    work: Box<dyn Work<A>>,
}
struct Control<A: Actor> {
    info: Info<A::Error>,
    queue: VecDeque<Queued<A>>,
    stop: bool,
}
struct Shared<A: Actor> {
    control: Mutex<Control<A>>,
    changed: Condvar,
    limits: Limits,
}
impl<A: Actor> Shared<A> {
    fn lock(&self) -> std::sync::MutexGuard<'_, Control<A>> {
        self.control.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn stop(&self) {
        let mut s = self.lock();
        s.stop = true;
        s.info.accepting = false;
        self.changed.notify_all();
    }
}
pub(crate) struct Client<A: Actor> {
    shared: Arc<Shared<A>>,
}
impl<A: Actor> Clone for Client<A> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}
impl<A: Actor> Client<A> {
    pub fn info(&self) -> Info<A::Error> {
        self.shared.lock().info.clone()
    }
    pub fn submit<T: Send + 'static>(
        &self,
        bytes: usize,
        action: impl FnOnce(&mut A) -> Result<T, A::Error> + Send + 'static,
    ) -> Result<Ticket<T, A::Error>, A::Error> {
        self.enqueue(bytes, false, action)
    }
    pub fn submit_control<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut A) -> Result<T, A::Error> + Send + 'static,
    ) -> Result<Ticket<T, A::Error>, A::Error> {
        self.enqueue(0, true, action)
    }
    fn enqueue<T: Send + 'static>(
        &self,
        bytes: usize,
        control: bool,
        action: impl FnOnce(&mut A) -> Result<T, A::Error> + Send + 'static,
    ) -> Result<Ticket<T, A::Error>, A::Error> {
        let mut s = self.shared.lock();
        if !s.info.accepting {
            return Err(Fault::Stopped.into());
        }
        let full = if control {
            s.info.control_pending >= A::CONTROL_REQUESTS
        } else {
            s.info.pending - s.info.control_pending >= self.shared.limits.requests
        };
        if full || bytes > self.shared.limits.bytes.saturating_sub(s.info.bytes) {
            return Err(Fault::Busy.into());
        }
        let (reply, receive) = oneshot::channel();
        s.queue.push_back(Queued {
            control,
            bytes,
            work: Box::new(Job { action, reply }),
        });
        s.info.pending += 1;
        s.info.control_pending += usize::from(control);
        s.info.bytes += bytes;
        s.info.submitted = s.info.submitted.saturating_add(1);
        self.shared.changed.notify_all();
        Ok(Ticket(receive))
    }
}
pub(crate) struct Worker<A: Actor> {
    client: Client<A>,
    thread: Option<JoinHandle<()>>,
}
impl<A: Actor> Worker<A> {
    pub fn spawn(
        name: &str,
        limits: Limits,
        init: impl FnOnce() -> Result<A, A::Error> + Send + 'static,
    ) -> Result<Self, A::Error> {
        Self::spawn_with_client(name, limits, move |_| init())
    }
    pub fn spawn_with_client(
        name: &str,
        limits: Limits,
        init: impl FnOnce(Client<A>) -> Result<A, A::Error> + Send + 'static,
    ) -> Result<Self, A::Error> {
        if limits.requests == 0
            || A::CONTROL_REQUESTS > 65_536
            || limits.requests > 65_536
            || limits.bytes == 0
            || limits.bytes > 1 << 30
        {
            return Err(Fault::Configuration.into());
        }
        let shared = Arc::new(Shared {
            control: Mutex::new(Control {
                info: Info::default(),
                queue: VecDeque::new(),
                stop: false,
            }),
            changed: Condvar::new(),
            limits,
        });
        let client = Client {
            shared: shared.clone(),
        };
        let init_client = client.clone();
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut state = match init(init_client) {
                        Ok(s) => s,
                        Err(e) => {
                            let _ = ready.send(Err(e.clone()));
                            return Err(e);
                        }
                    };
                    {
                        let mut s = shared.lock();
                        s.info.running = true;
                        s.info.accepting = true;
                    }
                    let _ = ready.send(Ok(()));
                    run(&shared, &mut state)
                }));
                let mut s = shared.lock();
                s.info.running = false;
                s.info.accepting = false;
                s.info.failure = match result {
                    Ok(result) => result.err(),
                    Err(_) => Some(Fault::Panicked.into()),
                };
                s.queue.clear();
                s.info.pending = 0;
                s.info.control_pending = 0;
                s.info.bytes = 0;
                shared.changed.notify_all();
            })
            .map_err(|_| A::Error::from(Fault::Spawn))?;
        match started.recv() {
            Ok(Ok(())) => Ok(Self {
                client,
                thread: Some(thread),
            }),
            other => {
                let _ = thread.join();
                Err(other
                    .ok()
                    .and_then(Result::err)
                    .unwrap_or_else(|| Fault::Panicked.into()))
            }
        }
    }
    pub fn client(&self) -> Client<A> {
        self.client.clone()
    }
    pub fn info(&self) -> Info<A::Error> {
        self.client.info()
    }
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub fn request_shutdown(&self) {
        self.client.shared.stop();
    }
    fn join(&mut self) -> Result<(), A::Error> {
        self.request_shutdown();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| A::Error::from(Fault::Panicked))?;
        }
        let mut s = self.client.shared.lock();
        s.info.joined = true;
        s.info.failure.clone().map_or(Ok(()), Err)
    }
    pub fn shutdown(mut self) -> Result<(), A::Error> {
        self.join()
    }
}
impl<A: Actor> Drop for Worker<A> {
    fn drop(&mut self) {
        let _ = self.join();
    }
}
fn run<A: Actor>(shared: &Shared<A>, state: &mut A) -> Result<(), A::Error> {
    loop {
        let next = {
            let mut s = shared.lock();
            if !s.stop && s.queue.is_empty() {
                s = shared
                    .changed
                    .wait_timeout(s, Duration::from_millis(100))
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            if s.stop && s.queue.is_empty() {
                return Ok(());
            }
            s.queue.pop_front()
        };
        if let Some(q) = next {
            let complete = q.work.run(state);
            let fatal = complete.error.as_ref().filter(|e| state.fatal(e)).cloned();
            {
                let mut s = shared.lock();
                s.info.pending -= 1;
                s.info.control_pending -= usize::from(q.control);
                s.info.bytes -= q.bytes;
                s.info.completed = s.info.completed.saturating_add(1);
                s.info.last_error = complete.error;
            }
            (complete.reply)();
            if let Some(error) = fatal {
                return Err(error);
            }
        } else if let Err(error) = state.before() {
            shared.lock().info.last_error = Some(error.clone());
            if state.fatal(&error) {
                return Err(error);
            }
        }
    }
}
