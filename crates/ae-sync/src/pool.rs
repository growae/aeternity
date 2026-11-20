use crate::{blocking, tasks::Tasks};
use ae_mempool::{
    pool::Signed,
    sync::{self as protocol, session},
};
use ae_p2p::{
    message::{self, Kind, Message},
    pool as wire, runtime as net,
};
use ae_storage::{Database, pool::worker};
use ae_types::{Hash256, PeerPublicKey};
use std::{collections::VecDeque, fmt, sync::Arc, time::Duration};
use tokio::{
    sync::{Semaphore, mpsc, oneshot, watch},
    time::Instant,
};

mod connections;
mod operations;
mod service;
pub use connections::Connection;
pub use service::Service;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Closed,
    NoPeer,
    InvalidConnection,
    IdExhausted,
    Worker,
    Protocol,
    Storage,
    Decode,
    Panicked,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<blocking::Error> for Error {
    fn from(error: blocking::Error) -> Self {
        match error {
            blocking::Error::Configuration => Self::Configuration,
            blocking::Error::Busy => Self::Busy,
            blocking::Error::Panicked => Self::Panicked,
            _ => Self::Worker,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub peers: usize,
    pub commands: usize,
    pub forwarded: usize,
    pub operations: usize,
    pub blocking_threads: usize,
    pub blocking_jobs: usize,
    pub blocking_input_bytes: usize,
    pub action_timeout: Duration,
    pub get_timeout: Duration,
    pub response_timeout: Duration,
    pub session: session::Limits,
    pub messages: message::Limits,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            peers: 32,
            commands: 64,
            forwarded: 64,
            operations: 64,
            blocking_threads: 2,
            blocking_jobs: 8,
            blocking_input_bytes: 64 * 1024 * 1024,
            action_timeout: Duration::from_millis(session::ACTION_TIMEOUT_MS),
            get_timeout: Duration::from_millis(session::GET_TIMEOUT_MS),
            response_timeout: Duration::from_secs(30),
            session: Default::default(),
            messages: Default::default(),
        }
    }
}
impl Config {
    fn validate(self) -> Result<(), Error> {
        for count in [self.peers, self.commands, self.forwarded, self.operations] {
            if count == 0 || count > Semaphore::MAX_PERMITS {
                return Err(Error::Configuration);
            }
        }
        for duration in [
            self.action_timeout,
            self.get_timeout,
            self.response_timeout,
            Duration::from_millis(self.session.resync_ms),
        ] {
            if Instant::now().checked_add(duration).is_none() {
                return Err(Error::Configuration);
            }
        }
        if self.action_timeout.is_zero()
            || self.get_timeout.is_zero()
            || self.response_timeout.is_zero()
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Local {
    Idle,
    Active {
        peer: PeerPublicKey,
        action: &'static str,
    },
    Cooldown,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counts {
    pub fetched: u64,
    pub admitted: u64,
    pub rejected: u64,
    pub completed: u64,
    pub failures: u64,
    pub connections_closed: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub running: bool,
    pub peers: usize,
    pub incoming: usize,
    pub operations: usize,
    pub local: Local,
    pub counts: Counts,
    pub last_failure: Option<session::Failure>,
}
impl Default for Status {
    fn default() -> Self {
        Self {
            running: true,
            peers: 0,
            incoming: 0,
            operations: 0,
            local: Local::Idle,
            counts: Counts::default(),
            last_failure: None,
        }
    }
}
pub struct Forwarded {
    pub client: net::Client,
    pub peer: PeerPublicKey,
    pub event: net::Event,
}
pub struct Events(mpsc::Receiver<Forwarded>);
impl Events {
    pub async fn recv(&mut self) -> Option<Forwarded> {
        self.0.recv().await
    }
}
pub struct Ticket<T>(oneshot::Receiver<Result<T, Error>>);
impl<T> Ticket<T> {
    pub async fn wait(self) -> Result<T, Error> {
        self.0.await.unwrap_or(Err(Error::Closed))
    }
}
enum Command {
    Peers(oneshot::Sender<Result<Vec<net::Client>, Error>>),
    Attach {
        connection: Box<Connection>,
        initiate: bool,
        permit: tokio::sync::OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<u64, Error>>,
    },
    Sync {
        peer: PeerPublicKey,
        reply: oneshot::Sender<Result<bool, Error>>,
    },
}
#[derive(Clone)]
pub struct Handle {
    commands: mpsc::Sender<Command>,
    slots: Arc<Semaphore>,
    stop: watch::Sender<bool>,
    status: watch::Receiver<Status>,
}
impl Handle {
    pub fn attach(&self, connection: Connection, initiate: bool) -> Result<Ticket<u64>, Error> {
        if *self.stop.borrow() || self.commands.is_closed() {
            return Err(Error::Closed);
        }
        let slot = self.commands.try_reserve().map_err(|_| Error::Busy)?;
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let (reply, receiver) = oneshot::channel();
        slot.send(Command::Attach {
            connection: Box::new(connection),
            initiate,
            permit,
            reply,
        });
        Ok(Ticket(receiver))
    }
    pub fn peers(&self) -> Result<Ticket<Vec<net::Client>>, Error> {
        if *self.stop.borrow() || self.commands.is_closed() {
            return Err(Error::Closed);
        }
        let (reply, receiver) = oneshot::channel();
        self.commands
            .try_send(Command::Peers(reply))
            .map_err(|_| Error::Busy)?;
        Ok(Ticket(receiver))
    }
    pub fn synchronize(&self, peer: PeerPublicKey) -> Result<Ticket<bool>, Error> {
        if *self.stop.borrow() || self.commands.is_closed() {
            return Err(Error::Closed);
        }
        let (reply, receiver) = oneshot::channel();
        self.commands
            .try_send(Command::Sync { peer, reply })
            .map_err(|_| Error::Busy)?;
        Ok(Ticket(receiver))
    }
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.status.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.status.borrow_and_update().clone())
    }
    pub fn shutdown(&self) {
        self.stop.send_replace(true);
    }
}
pub fn create(
    db: Arc<Database>,
    admission: worker::Client,
    config: Config,
) -> Result<(Handle, Events, Service), Error> {
    config.validate()?;
    let blocking = blocking::Pool::new(
        config.blocking_threads,
        config.blocking_jobs,
        config.blocking_input_bytes,
    )?;
    let (commands, receiver) = mpsc::channel(config.commands);
    let (forwarded, events) = mpsc::channel(config.forwarded);
    let (stop, stopped) = watch::channel(false);
    let (status, observed) = watch::channel(Status::default());
    let handle = Handle {
        commands,
        slots: Arc::new(Semaphore::new(config.peers)),
        stop,
        status: observed,
    };
    Ok((
        handle,
        Events(events),
        Service::new(
            db, admission, config, blocking, receiver, forwarded, stopped, status,
        ),
    ))
}

#[derive(Clone, PartialEq, Eq)]
enum Key {
    Action(session::Token),
    Incoming(u64),
}
struct Action {
    action: session::Action,
    deadline: Instant,
}
struct Incoming {
    id: u64,
    deadline: Instant,
}
struct Outcome {
    reply: Result<session::Reply, session::Failure>,
    fetched: usize,
    admitted: usize,
    rejected: usize,
}
impl Outcome {
    fn reply(reply: Result<session::Reply, session::Failure>) -> Self {
        Self {
            reply,
            fetched: 0,
            admitted: 0,
            rejected: 0,
        }
    }
}
enum Done {
    Action(Outcome),
    Wake(session::Action),
    Sent(Result<(), Error>),
}
