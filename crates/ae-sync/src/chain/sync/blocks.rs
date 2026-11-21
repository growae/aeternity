use super::super as chain;
use super::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
mod receive;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ignored {
    Height,
    Known,
    Duplicate,
    Invalid(Box<Error>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Accepted(chain::Report),
    Ignored(Ignored),
}
#[derive(Debug, Clone)]
pub struct Completion {
    pub peer: PeerPublicKey,
    pub generation: net::Generation,
    pub result: Result<Outcome, Error>,
}
#[derive(Debug, Clone, Default)]
pub struct Counts {
    pub received: u64,
    pub started: u64,
    pub refused: u64,
    pub accepted: u64,
    pub ignored: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub committed: u64,
    pub present: u64,
}
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub active: usize,
    pub available_bytes: usize,
    pub counts: Counts,
    pub last: Option<Completion>,
}
#[derive(Clone, PartialEq, Eq)]
struct ExpansionKey(PeerPublicKey, net::Generation, Hash256);
type Expansions = Arc<Mutex<Vec<ExpansionKey>>>;
struct Expansion {
    keys: Expansions,
    key: ExpansionKey,
}
impl Expansion {
    fn acquire(keys: &Expansions, client: &net::Client, hash: Hash256) -> Option<Self> {
        let key = ExpansionKey(client.peer(), client.generation().clone(), hash);
        let mut held = keys.lock().unwrap_or_else(|e| e.into_inner());
        if held.contains(&key) {
            return None;
        }
        held.push(key.clone());
        Some(Self {
            keys: keys.clone(),
            key,
        })
    }
}
impl Drop for Expansion {
    fn drop(&mut self) {
        self.keys
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|k| k != &self.key);
    }
}
struct Held {
    received: net::Received,
    reservation: blocking::Reservation,
    _bytes: OwnedSemaphorePermit,
}
pub(super) struct Owner<O: MapOrder + Send + Sync + 'static> {
    context: Arc<Context<O>>,
    bytes: Arc<Semaphore>,
    tasks: Tasks<u64, Completion>,
    clients: BTreeMap<u64, net::Client>,
    expansions: Expansions,
    sequence: u64,
    status: Status,
}
impl<O: MapOrder + Send + Sync + 'static> Owner<O> {
    pub fn new(context: Arc<Context<O>>) -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(context.config.block_bytes)),
            context,
            tasks: Tasks::new(),
            clients: BTreeMap::new(),
            expansions: Arc::default(),
            sequence: 0,
            status: Status::default(),
        }
    }
    pub fn handles(event: &peer::Event) -> bool {
        matches!(event.kind(),peer::EventKind::Unhandled{message,..} if matches!(message.message().kind(),Kind::KeyBlock|Kind::MicroBlock))
    }
    pub fn status(&self) -> Status {
        let mut status = self.status.clone();
        status.active = self.tasks.len();
        status.available_bytes = self.bytes.available_permits();
        status
    }
    pub fn route(&mut self, event: peer::Event) {
        self.status.counts.received = self.status.counts.received.saturating_add(1);
        let peer::EventKind::Unhandled { client, .. } = event.kind() else {
            unreachable!()
        };
        let client = client.clone();
        let result = (|| {
            if self.tasks.len() >= self.context.config.block_jobs {
                return Err(Error::Busy);
            }
            if client.is_closed() {
                return Err(Error::Stale);
            }
            let sequence = self.sequence.checked_add(1).ok_or(Error::ResourceLimit)?;
            let weight = self
                .context
                .config
                .block_weight()
                .expect("validated")
                .max(self.context.importer.download_weight() * 3);
            let bytes = self
                .bytes
                .clone()
                .try_acquire_many_owned(weight as u32)
                .map_err(|_| Error::Busy)?;
            let reserved = self
                .context
                .importer
                .reserve_download(&self.context.network, &client)
                .map_err(|error| {
                    Error::Import(chain::Failure {
                        progress: Some(Default::default()),
                        error,
                    })
                })?;
            self.sequence = sequence;
            Ok((sequence, bytes, reserved))
        })();
        let (sequence, bytes, reservation) = match result {
            Ok(held) => held,
            Err(error) => {
                self.status.counts.refused = self.status.counts.refused.saturating_add(1);
                self.status.last = Some(Completion {
                    peer: client.peer(),
                    generation: client.generation().clone(),
                    result: Err(error),
                });
                return;
            }
        };
        // Acquire the new aggregate budget before releasing the application's
        // publisher credit. The original transport receipt stays retained.
        let peer::EventKind::Unhandled { message, .. } = event.into_kind() else {
            unreachable!()
        };
        let held = Held {
            received: message,
            reservation,
            _bytes: bytes,
        };
        let height = self
            .context
            .shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sync_height(&client);
        self.clients.insert(sequence, client.clone());
        self.tasks.insert(
            sequence,
            receive::run(
                self.context.clone(),
                self.expansions.clone(),
                client,
                held,
                height,
            ),
        );
        self.status.counts.started = self.status.counts.started.saturating_add(1);
    }
    pub async fn next(&mut self) {
        let (id, completion) = self.tasks.next().await;
        self.clients.remove(&id);
        match &completion.result {
            Ok(Outcome::Accepted(report)) => {
                self.status.counts.accepted = self.status.counts.accepted.saturating_add(1);
                for item in &report.accepted {
                    match item.outcome {
                        chain::Outcome::Present => {
                            self.status.counts.present =
                                self.status.counts.present.saturating_add(1)
                        }
                        chain::Outcome::Committed { .. } => {
                            self.status.counts.committed =
                                self.status.counts.committed.saturating_add(1)
                        }
                    }
                }
            }
            Ok(Outcome::Ignored(_)) => {
                self.status.counts.ignored = self.status.counts.ignored.saturating_add(1)
            }
            Err(_) => self.status.counts.failed = self.status.counts.failed.saturating_add(1),
        }
        self.status.last = Some(completion);
    }
    pub fn cancel(&mut self) -> Vec<net::Monitor> {
        let monitors = self
            .clients
            .values()
            .map(|c| {
                c.shutdown();
                c.monitor()
            })
            .collect();
        self.status.counts.cancelled = self
            .status
            .counts
            .cancelled
            .saturating_add(self.tasks.len() as u64);
        self.tasks.clear();
        self.clients.clear();
        monitors
    }
}
