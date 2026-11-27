use crate::runtime;
use std::{collections::BTreeSet, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::watch,
    task::{JoinHandle, JoinSet},
};

mod accounts;
mod blocks;
mod channels;
mod contracts;
mod currency;
pub mod dry_run;
mod ends;
pub(crate) mod gas;
mod key;
mod model;
mod names;
mod oracles;
pub(crate) mod parameters;
mod pending;
mod post;
mod response;
mod settings;
mod status;
mod sync;
mod text;
pub mod transaction;
mod transactions;
mod transport;
pub use post::Limits as SubmissionLimits;
pub use response::Cors;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    External,
    Internal,
    Rosetta,
    RosettaOffline,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub target: Target,
    pub groups: BTreeSet<String>,
    pub cors: Cors,
    pub connections: usize,
    pub header_bytes: usize,
    pub body_bytes: usize,
    pub submissions: SubmissionLimits,
    pub timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            listen: ([0, 0, 0, 0], 8043).into(),
            target: Target::External,
            groups: BTreeSet::from(["chain".into(), "node_info".into()]),
            cors: Cors::default(),
            connections: 128,
            header_bytes: 16 << 10,
            body_bytes: 5_000_000,
            submissions: SubmissionLimits::default(),
            timeout: Duration::from_secs(30),
        }
    }
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        if self.connections == 0
            || self.connections > 4096
            || !(1024..=1 << 20).contains(&self.header_bytes)
            || self.body_bytes > 64 << 20
            || !self.submissions.valid(self.body_bytes)
            || self.timeout.is_zero()
            || tokio::time::Instant::now()
                .checked_add(self.timeout)
                .is_none()
            || self.groups.len() > 256
            || self.groups.iter().any(|g| g.len() > 256)
            || !self.cors.valid()
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    AlreadyListening,
    Io(String),
    Task,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone)]
pub struct Status {
    pub address: SocketAddr,
    pub running: bool,
    pub joined: bool,
    pub active: usize,
    pub accepted: u64,
    pub completed: u64,
    pub cancelled: u64,
    pub failure: Option<Error>,
}
#[derive(Clone)]
pub struct Handle {
    stop: watch::Sender<bool>,
    status: watch::Receiver<Status>,
}
impl Handle {
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub fn request_shutdown(&self) {
        self.stop.send_replace(true);
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.status.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.status())
    }
}
pub(crate) struct Running {
    handle: Handle,
    task: Option<JoinHandle<Status>>,
}
impl Running {
    pub async fn start(parent: runtime::Handle, config: Config) -> Result<Self, Error> {
        config.validate()?;
        if !parent.status().running {
            return Err(Error::Closed);
        }
        let listener = TcpListener::bind(config.listen)
            .await
            .map_err(|e| Error::Io(e.to_string()))?;
        let address = listener
            .local_addr()
            .map_err(|e| Error::Io(e.to_string()))?;
        let posts = post::Service::new(&config)?;
        let state = Status {
            address,
            running: true,
            joined: false,
            active: 0,
            accepted: 0,
            completed: 0,
            cancelled: 0,
            failure: None,
        };
        let (publish, status) = watch::channel(state);
        let (stop, stopping) = watch::channel(false);
        let handle = Handle { stop, status };
        let task = tokio::spawn(run(
            parent,
            Arc::new(config),
            posts,
            listener,
            stopping,
            publish,
        ));
        Ok(Self {
            handle,
            task: Some(task),
        })
    }
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    pub async fn finish(mut self) -> Status {
        self.handle.request_shutdown();
        match self.task.take().expect("HTTP owner task").await {
            Ok(status) => status,
            Err(_) => {
                let mut status = self.handle.status();
                status.running = false;
                status.joined = false;
                status.failure = Some(Error::Task);
                status
            }
        }
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.handle.request_shutdown();
    }
}
async fn run(
    mut parent: runtime::Handle,
    config: Arc<Config>,
    posts: post::Service,
    listener: TcpListener,
    mut stopping: watch::Receiver<bool>,
    publish: watch::Sender<Status>,
) -> Status {
    let mut state = publish.borrow().clone();
    let mut clients = JoinSet::new();
    while !*stopping.borrow() && parent.status().running {
        tokio::select! {
            biased;
            _ = stopping.changed() => break,
            result = parent.changed() => { if result.is_err() { break; } },
            result = clients.join_next(), if !clients.is_empty() => {
                state.active -= 1;
                match result {
                    Some(Ok(())) => state.completed = state.completed.saturating_add(1),
                    _ => { state.failure = Some(Error::Task); break; }
                }
                publish.send_replace(state.clone());
            },
            accepted = listener.accept(), if clients.len() < config.connections => {
                match accepted {
                    Ok((stream, _)) => {
                        state.accepted = state.accepted.saturating_add(1);
                        state.active += 1;
                        let config = config.clone(); let parent = parent.clone();
                        let posts = posts.clone();
                        clients.spawn(async move { transport::exchange(stream, parent, config, posts).await });
                        publish.send_replace(state.clone());
                    },
                    Err(error) => { state.failure = Some(Error::Io(error.to_string())); break; }
                }
            }
        }
    }
    drop(listener);
    state.running = false;
    publish.send_replace(state.clone());
    clients.abort_all();
    while let Some(result) = clients.join_next().await {
        state.active -= 1;
        match result {
            Ok(()) => state.completed = state.completed.saturating_add(1),
            Err(error) if error.is_cancelled() => {
                state.cancelled = state.cancelled.saturating_add(1)
            }
            Err(_) => state.failure = Some(Error::Task),
        }
    }
    posts.finish().await;
    // Accepted reads are owned and joined by the parent's bounded importer.
    // Dropping a client cancels its pending receipt; no detached storage job
    // or replacement-owner lookup is introduced by this listener.
    if state.failure.is_some() {
        parent.request_shutdown();
    }
    state.joined = true;
    publish.send_replace(state.clone());
    state
}
