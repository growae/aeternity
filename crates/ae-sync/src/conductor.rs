use crate::{blocking, chain::sync, network};
use ae_chain::publication::{Effect, Kind as BlockKind, Payload, Propagation};
use ae_p2p::{
    message::{self, Kind, Message, Value},
    runtime as net,
};
use ae_storage::{Database, events};
use std::{
    collections::{BTreeMap, hash_map::RandomState},
    hash::BuildHasher,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

#[derive(Debug, Clone)]
pub struct Config {
    pub received_peers: usize,
    pub peer_limit: usize,
    pub poll: Duration,
    pub timeout: Duration,
    pub read_bytes: usize,
    pub messages: message::Limits,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            received_peers: 16,
            peer_limit: 1024,
            poll: Duration::from_millis(100),
            timeout: Duration::from_secs(30),
            read_bytes: 256 << 20,
            messages: Default::default(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    SourceClosed,
    SyncClosed,
    NetworkClosed,
    Timeout,
    Worker,
    Protocol,
    Capacity,
    Source(events::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone)]
pub struct Status {
    pub running: bool,
    pub joined: bool,
    pub cursor: events::Cursor,
    pub records: u64,
    pub top_changes: u64,
    pub transaction_events: u64,
    pub created: u64,
    pub broadcasts: u64,
    pub sent: u64,
    pub failed: u64,
    pub last_error: Option<Error>,
}
#[derive(Clone)]
pub struct Handle {
    stop: watch::Sender<bool>,
    status: watch::Receiver<Status>,
    events: events::Reader,
}
impl Handle {
    pub fn shutdown(&self) {
        self.stop.send_replace(true);
    }
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub fn events(&self) -> events::Reader {
        self.events.clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.status.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.status.borrow_and_update().clone())
    }
}
pub struct Service {
    network: network::Handle,
    sync: sync::Handle,
    source: events::Reader,
    config: Config,
    jobs: blocking::Pool,
    weight: usize,
    stopped: watch::Receiver<bool>,
    published: watch::Sender<Status>,
    current: Status,
    random: RandomState,
}
pub fn create(
    database: Arc<Database>,
    network: network::Handle,
    sync: sync::Handle,
    after: Option<events::Cursor>,
    config: Config,
) -> Result<(Handle, Service), Error> {
    let source = database.events();
    let weight = config
        .messages
        .body_bytes
        .checked_mul(2)
        .and_then(|n| n.checked_add(config.messages.work_bytes))
        .and_then(|n| n.checked_add(source.info().limits.bytes))
        .ok_or(Error::Configuration)?;
    if !network.uses_database(&database)
        || !sync.uses_database(&database)
        || config.received_peers > config.peer_limit
        || config.peer_limit == 0
        || config.peer_limit > 65_536
        || config.read_bytes < weight
        || config.poll.is_zero()
        || config.poll > Duration::from_secs(1)
        || config.timeout.is_zero()
        || Instant::now().checked_add(config.timeout).is_none()
    {
        return Err(Error::Configuration);
    }
    let cursor = after.unwrap_or_else(|| source.cursor());
    source.after(&cursor, 1).map_err(Error::Source)?;
    let jobs = blocking::Pool::named("ae-block-publication", 1, 1, config.read_bytes)
        .map_err(|_| Error::Configuration)?;
    let current = Status {
        running: true,
        joined: false,
        cursor,
        records: 0,
        top_changes: 0,
        transaction_events: 0,
        created: 0,
        broadcasts: 0,
        sent: 0,
        failed: 0,
        last_error: None,
    };
    let (published, status) = watch::channel(current.clone());
    let (stop, stopped) = watch::channel(false);
    Ok((
        Handle {
            stop,
            status,
            events: source.clone(),
        },
        Service {
            network,
            sync,
            source,
            config,
            jobs,
            weight,
            stopped,
            published,
            current,
            random: RandomState::new(),
        },
    ))
}
struct Observed {
    payload: Option<Arc<Payload>>,
    cursor: events::Cursor,
    closed: bool,
}
impl Service {
    async fn next(&self) -> Result<Observed, Error> {
        let source = self.source.clone();
        let cursor = self.current.cursor.clone();
        let poll = self.config.poll;
        let batch = self
            .jobs
            .submit_wait(self.weight, move |_| source.wait(&cursor, 1, poll))
            .await
            .map_err(|_| Error::Worker)?
            .await
            .map_err(|_| Error::Worker)?
            .map_err(Error::Source)?;
        Ok(Observed {
            payload: batch.records.into_iter().next(),
            cursor: batch.cursor,
            closed: batch.closed,
        })
    }
    async fn forward(
        &self,
        payload: Arc<Payload>,
        origin: Propagation,
    ) -> Result<(u64, u64), Error> {
        let mut clients = self
            .network
            .peers()
            .peers()
            .map_err(|_| Error::NetworkClosed)?
            .wait()
            .await
            .map_err(|_| Error::NetworkClosed)?;
        clients.retain(|c| !c.is_closed() && !c.is_disconnecting());
        if clients.len() > self.config.peer_limit {
            return Err(Error::Capacity);
        }
        if origin == Propagation::Received {
            clients.sort_by_cached_key(|c| {
                self.random
                    .hash_one((payload.change().revision, c.peer().0))
            });
            clients.truncate(self.config.received_peers);
            // Native chooses its random subset before excluding all peers
            // associated with a sync task, including retained suspect tasks.
            let busy = self.sync.peers_in_sync().map_err(|_| Error::SyncClosed)?;
            clients.retain(|c| !busy.contains(&c.peer()));
        }
        if clients.is_empty() {
            return Ok((0, 0));
        }
        let limits = self.config.messages;
        let tickets = self
            .jobs
            .submit_wait(self.weight, move |cancel| {
                if cancel.cancelled() {
                    return Err(Error::Closed);
                }
                let body = payload.body().ok_or(Error::Protocol)?;
                if body.len() > limits.body_bytes {
                    return Err(Error::Capacity);
                }
                let block = payload.context().block.ok_or(Error::Protocol)?;
                let (kind, fields) = match block.kind {
                    BlockKind::Key => (
                        Kind::KeyBlock,
                        BTreeMap::from([("key_block".into(), Value::Binary(body.to_vec()))]),
                    ),
                    BlockKind::Micro => (
                        Kind::MicroBlock,
                        BTreeMap::from([
                            ("micro_block".into(), Value::Binary(body.to_vec())),
                            ("light".into(), Value::Boolean(true)),
                        ]),
                    ),
                };
                let message = Message::new(kind, 1, fields, limits).map_err(|_| Error::Protocol)?;
                let prepared = net::Prepared::new(&message, limits).map_err(|_| Error::Protocol)?;
                let mut tickets = Vec::with_capacity(clients.len());
                for client in clients {
                    if cancel.cancelled() {
                        return Err(Error::Closed);
                    }
                    tickets.push(client.send_prepared(&prepared));
                }
                Ok(tickets)
            })
            .await
            .map_err(|_| Error::Worker)?
            .await
            .map_err(|_| Error::Worker)??;
        let (mut sent, mut failed) = (0, 0);
        for ticket in tickets {
            match ticket {
                Ok(ticket) => match ticket.await {
                    Ok(net::Reply::Sent) => sent += 1,
                    _ => failed += 1,
                },
                _ => failed += 1,
            }
        }
        Ok((sent, failed))
    }
    pub async fn run(mut self) -> Status {
        let mut stopped = self.stopped.clone();
        loop {
            if *self.stopped.borrow() {
                break;
            }
            if !self.sync.status().running {
                self.current.last_error = Some(Error::SyncClosed);
                break;
            }
            if !self.network.status().running {
                self.current.last_error = Some(Error::NetworkClosed);
                break;
            }
            let next = tokio::select! { _=stopped.changed()=>break, result=self.next()=>result };
            let next = match next {
                Ok(value) => value,
                Err(error) => {
                    self.current.last_error = Some(error);
                    break;
                }
            };
            if let Some(payload) = next.payload {
                let mut publication = None;
                for effect in payload.effects() {
                    match effect {
                        Effect::Transaction { .. } => {
                            self.current.transaction_events =
                                self.current.transaction_events.saturating_add(1)
                        }
                        Effect::TopChanged(_) => {
                            self.current.top_changes = self.current.top_changes.saturating_add(1)
                        }
                        Effect::BlockCreated(_) => {
                            self.current.created = self.current.created.saturating_add(1)
                        }
                        Effect::BlockToPublish { origin, .. } => publication = Some(origin),
                        Effect::Reverted(_) | Effect::Consensus { .. } => (),
                    }
                }
                if let Some(origin) = publication {
                    self.current.broadcasts = self.current.broadcasts.saturating_add(1);
                    let result = tokio::select! { _=stopped.changed()=>break, result=tokio::time::timeout(self.config.timeout,self.forward(payload, origin))=>result.unwrap_or(Err(Error::Timeout)) };
                    match result {
                        Ok((sent, failed)) => {
                            self.current.sent = self.current.sent.saturating_add(sent);
                            self.current.failed = self.current.failed.saturating_add(failed);
                        }
                        Err(error) => {
                            self.current.last_error = Some(error);
                            break;
                        }
                    }
                }
                self.current.records = self.current.records.saturating_add(1);
            }
            self.current.cursor = next.cursor;
            self.published.send_replace(self.current.clone());
            if next.closed {
                self.current.last_error = Some(Error::SourceClosed);
                break;
            }
        }
        self.jobs.stop();
        self.jobs.join().await;
        self.current.running = false;
        self.current.joined = true;
        self.published.send_replace(self.current.clone());
        self.current.clone()
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.jobs.stop();
        if !self.current.joined {
            self.current.running = false;
            self.current.last_error.get_or_insert(Error::Closed);
            self.published.send_replace(self.current.clone());
        }
    }
}
