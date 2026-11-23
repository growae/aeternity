use super::*;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Condvar, Mutex, mpsc},
    task::{Context, Poll},
    thread::{self, JoinHandle},
};
use tokio::sync::oneshot;

pub struct Ticket<T>(oneshot::Receiver<Result<T, Error>>);
impl<T> Ticket<T> {
    // Poll exactly once to completion; Empty retains the accepted request.
    pub fn try_result(&mut self) -> Option<Result<T, Error>> {
        match self.0.try_recv() {
            Ok(value) => Some(value),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => Some(Err(Error::Disconnected)),
        }
    }
    // Waiting or dropping a reply never cancels accepted work. A lost reply
    // is not permission to replay promotion or signing against a new worker.
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
enum Command {
    #[cfg(test)]
    Pause {
        entered: mpsc::SyncSender<()>,
        release: mpsc::Receiver<()>,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Promote {
        expected: AccountPublicKey,
        reply: oneshot::Sender<Result<PublicKeys, Error>>,
    },
    Sign {
        header: Box<MicroHeader>,
        expected: Option<AccountPublicKey>,
        reply: oneshot::Sender<Result<SignedMicro, Error>>,
    },
    Identity {
        reply: oneshot::Sender<Result<Arc<noise::Identity>, Error>>,
    },
}
struct Queue {
    commands: VecDeque<Command>,
    info: Info,
}
struct Shared {
    queue: Mutex<Queue>,
    changed: Condvar,
    limit: usize,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn stop(&self) {
        self.lock().info.accepting = false;
        self.changed.notify_all();
    }
    fn complete(&self, keys: PublicKeys, promoted: bool, error: Option<Error>) {
        let mut queue = self.lock();
        queue.info.keys = keys;
        queue.info.pending -= 1;
        queue.info.completed += 1;
        queue.info.promotions += u64::from(promoted);
        queue.info.last_error = error;
    }
    fn finish(&self, error: Option<Error>) {
        let mut queue = self.lock();
        queue.info.accepting = false;
        queue.info.running = false;
        queue.info.pending = 0;
        queue.info.failure = error;
        queue.commands.clear();
        self.changed.notify_all();
    }
}
#[derive(Clone)]
pub struct Client(Arc<Shared>);
impl Client {
    pub fn info(&self) -> Info {
        self.0.lock().info.clone()
    }
    fn submit<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, Error>>) -> Command,
    ) -> Result<Ticket<T>, Error> {
        let mut queue = self.0.lock();
        if !queue.info.accepting {
            return Err(Error::Stopped);
        }
        if queue.info.pending >= self.0.limit {
            return Err(Error::Busy);
        }
        let (reply, ticket) = oneshot::channel();
        queue.commands.push_back(make(reply));
        queue.info.pending += 1;
        queue.info.submitted += 1;
        self.0.changed.notify_one();
        Ok(Ticket(ticket))
    }
    pub fn promote_candidate(
        &self,
        expected: AccountPublicKey,
    ) -> Result<Ticket<PublicKeys>, Error> {
        self.submit(|reply| Command::Promote { expected, reply })
    }
    pub fn sign_micro(&self, header: MicroHeader) -> Result<Ticket<SignedMicro>, Error> {
        self.submit(|reply| Command::Sign {
            header: Box::new(header),
            expected: None,
            reply,
        })
    }
    pub fn sign_micro_for(
        &self,
        expected: AccountPublicKey,
        header: MicroHeader,
    ) -> Result<Ticket<SignedMicro>, Error> {
        self.submit(|reply| Command::Sign {
            header: Box::new(header),
            expected: Some(expected),
            reply,
        })
    }
    // This capability contains the peer secret for Noise, not the signing key.
    // It can remain alive with a connection after the key worker stops.
    pub fn peer_identity(&self) -> Result<Ticket<Arc<noise::Identity>>, Error> {
        self.submit(|reply| Command::Identity { reply })
    }
}
pub struct Worker {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}
impl Worker {
    pub fn spawn(config: Config) -> Result<Self, Error> {
        config.validate()?;
        let (initialized, result) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("ae-keys".into())
            .spawn(move || {
                let mut rng = keeper::Os;
                let state = match keeper::State::open(&config, &mut rng) {
                    Ok(state) => state,
                    Err(error) => {
                        let _ = initialized.send(Err(error));
                        return;
                    }
                };
                let shared = Arc::new(Shared {
                    limit: config.requests,
                    queue: Mutex::new(Queue {
                        commands: Default::default(),
                        info: Info {
                            running: true,
                            accepting: true,
                            keys: state.public(),
                            pending: 0,
                            submitted: 0,
                            completed: 0,
                            promotions: 0,
                            last_error: None,
                            failure: None,
                        },
                    }),
                    changed: Condvar::new(),
                });
                drop(config);
                if initialized.send(Ok(shared.clone())).is_err() {
                    return;
                }
                let exit = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(&shared, state, rng)
                }));
                shared.finish(exit.err().map(|_| Error::Panicked));
            })
            .map_err(|_| Error::Spawn)?;
        match result.recv() {
            Ok(Ok(shared)) => Ok(Self {
                shared,
                thread: Some(thread),
            }),
            Ok(Err(error)) => {
                let _ = thread.join();
                Err(error)
            }
            Err(_) => {
                let _ = thread.join();
                Err(Error::Panicked)
            }
        }
    }
    pub fn client(&self) -> Client {
        Client(self.shared.clone())
    }
    pub fn info(&self) -> Info {
        self.shared.lock().info.clone()
    }
    pub fn request_shutdown(&self) {
        self.shared.stop();
    }
    pub fn shutdown(mut self) -> Result<(), Error> {
        self.join()
    }
    fn join(&mut self) -> Result<(), Error> {
        self.request_shutdown();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| Error::Panicked)?;
        }
        self.info().failure.map_or(Ok(()), Err)
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.join();
    }
}
fn run(shared: &Shared, mut state: keeper::State, mut rng: keeper::Os) {
    loop {
        let command = {
            let queue = shared.lock();
            let mut queue = shared
                .changed
                .wait_while(queue, |q| q.info.accepting && q.commands.is_empty())
                .unwrap_or_else(|e| e.into_inner());
            let Some(command) = queue.commands.pop_front() else {
                break;
            };
            command
        };
        match command {
            #[cfg(test)]
            Command::Pause {
                entered,
                release,
                reply,
            } => {
                entered.send(()).unwrap();
                release.recv().unwrap();
                shared.complete(state.public(), false, None);
                let _ = reply.send(Ok(()));
            }
            Command::Promote { expected, reply } => {
                let result = state.promote(expected, &mut rng);
                shared.complete(
                    state.public(),
                    result.is_ok(),
                    result.as_ref().err().cloned(),
                );
                let _ = reply.send(result);
            }
            Command::Sign {
                header,
                expected,
                reply,
            } => {
                let result = state.sign(*header, expected);
                shared.complete(state.public(), false, result.as_ref().err().cloned());
                let _ = reply.send(result);
            }
            Command::Identity { reply } => {
                let result = state.identity();
                shared.complete(state.public(), false, None);
                let _ = reply.send(Ok(result));
            }
        }
    }
}

#[cfg(test)]
pub(super) fn pause(client: &Client) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
    let (entered, ready) = mpsc::sync_channel(1);
    let (release, wait) = mpsc::sync_channel(1);
    drop(
        client
            .submit(|reply| Command::Pause {
                entered,
                release: wait,
                reply,
            })
            .unwrap(),
    );
    (ready, release)
}
