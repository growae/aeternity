use crate::{blocking, chain::sync, gossip, network, peer};
use ae_storage::{Database, pool::worker};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::watch, time::Instant};

mod feed;
mod file;
mod gc;
pub mod metrics;
mod sink;
pub use feed::{Cursor, Delivery, Record, Subscription};
pub use file::RotatingFile;
pub use sink::{Info as SinkInfo, Outputs};

#[derive(Debug, Clone)]
pub struct Config {
    pub interval: Duration,
    pub sample_timeout: Duration,
    pub records: usize,
    pub history_bytes: usize,
    pub record_bytes: usize,
    pub sink_records: usize,
    pub sink_bytes: usize,
    pub prefix: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            sample_timeout: Duration::from_secs(5),
            records: 1024,
            history_bytes: 1 << 20,
            record_bytes: 64 << 10,
            sink_records: 128,
            sink_bytes: 1 << 20,
            prefix: String::new(),
        }
    }
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        if self.interval.is_zero()
            || self.sample_timeout.is_zero()
            || Instant::now()
                .checked_add(self.interval.max(self.sample_timeout))
                .is_none()
            || self.records == 0
            || self.records > 65_536
            || self.sink_records == 0
            || self.sink_records > 65_536
            || self.record_bytes < 1024
            || self.record_bytes > 1 << 20
            || self.history_bytes < self.record_bytes
            || self.history_bytes > 1 << 30
            || self.sink_bytes < self.record_bytes + 4096
            || self.sink_bytes > 1 << 30
            || self.prefix.len() > 256
            || !self
                .prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
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
    InputClosed,
    ForeignCursor,
    Sequence,
    Time,
    Encoding,
    RecordTooLarge,
    Storage,
    Gc(ae_storage::gc::events::Error),
    GcClosed,
    Sample,
    Timeout,
    Io(std::io::ErrorKind),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Clone)]
pub struct Sources {
    pub database: Arc<Database>,
    pub network: network::Handle,
    pub peer: peer::Handle,
    pub gossip: gossip::Handle,
    pub sync: sync::Handle,
    pub admission: worker::Client,
}
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub running: bool,
    pub joined: bool,
    pub events: u64,
    pub gc_events: u64,
    pub gc_missed: u64,
    pub samples: u64,
    pub sample_errors: u64,
    pub rejected_records: u64,
    pub evicted: u64,
    pub retained_records: usize,
    pub retained_bytes: usize,
    pub log: Option<SinkInfo>,
    pub statsd: Option<SinkInfo>,
    pub metrics: Option<Arc<metrics::Snapshot>>,
    pub last_error: Option<Error>,
}
#[derive(Clone)]
pub struct Handle {
    stop: watch::Sender<bool>,
    status: watch::Receiver<Status>,
    ring: Arc<Mutex<feed::Ring>>,
}
impl Handle {
    pub fn shutdown(&self) {
        self.stop.send_replace(true);
    }
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.status.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.status.borrow_and_update().clone())
    }
    pub fn cursor(&self) -> Cursor {
        self.ring.lock().unwrap_or_else(|e| e.into_inner()).cursor()
    }
    pub fn subscribe(&self, after: Option<Cursor>) -> Result<Subscription, Error> {
        Subscription::new(self.ring.clone(), after)
    }
}
type Sample = Pin<Box<dyn Future<Output = Result<metrics::Snapshot, Error>> + Send>>;
pub struct Service {
    sources: Sources,
    admission: crate::bindings::Admission,
    incoming: sync::Events,
    gc: ae_storage::gc::events::Reader,
    gc_cursor: ae_storage::gc::events::Cursor,
    gc_pending: bool,
    config: Config,
    stopped: watch::Receiver<bool>,
    status: watch::Sender<Status>,
    current: Status,
    ring: Arc<Mutex<feed::Ring>>,
    pending: Option<Sample>,
    reads: blocking::Pool,
    log: Option<sink::Worker>,
    statsd: Option<sink::Worker>,
}
pub fn create(
    sources: Sources,
    incoming: sync::Events,
    config: Config,
    outputs: Outputs,
) -> Result<(Handle, Service), Error> {
    let admission = crate::bindings::Admission::fixed(&sources.database, sources.admission.clone());
    create_bound(sources, incoming, config, outputs, admission)
}
pub(crate) fn create_bound(
    sources: Sources,
    incoming: sync::Events,
    config: Config,
    outputs: Outputs,
    admission: crate::bindings::Admission,
) -> Result<(Handle, Service), Error> {
    config.validate()?;
    let db = &sources.database;
    if !admission.uses_database(db) {
        return Err(Error::Configuration);
    }
    if !sources.sync.owns(db, &incoming)
        || !sources.network.uses_database(db)
        || !sources.peer.uses_database(db)
        || !sources.gossip.uses_database(db)
        || !sources.admission.uses_database(db)
    {
        return Err(Error::Configuration);
    }
    let gc = db.gc_events();
    let gc_cursor = gc.cursor();
    gc.after(&gc_cursor, 1).map_err(Error::Gc)?;
    let reads =
        blocking::Pool::named("ae-observer-read", 1, 1, 4096).map_err(|_| Error::Configuration)?;
    let (log, statsd) = sink::Worker::outputs(outputs, &config)?;
    let ring = Arc::new(Mutex::new(feed::Ring::new()));
    let (stop, stopped) = watch::channel(false);
    let current = Status {
        running: true,
        ..Default::default()
    };
    let (status, view) = watch::channel(current.clone());
    Ok((
        Handle {
            stop,
            status: view,
            ring: ring.clone(),
        },
        Service {
            sources,
            admission,
            incoming,
            gc,
            gc_cursor,
            gc_pending: false,
            config,
            stopped,
            status,
            current,
            ring,
            pending: None,
            reads,
            log,
            statsd,
        },
    ))
}
impl Service {
    fn publish(&mut self) {
        let ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
        (self.current.retained_records, self.current.retained_bytes) = ring.retained();
        self.current.evicted = ring.evicted;
        self.current.log = self.log.as_ref().map(sink::Worker::info);
        self.current.statsd = self.statsd.as_ref().map(sink::Worker::info);
        self.status.send_replace(self.current.clone());
    }
    fn record(&mut self, category: &'static str, data: Value) {
        let result =
            self.ring
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(category, data, &self.config);
        match result {
            Ok(record) => {
                if let Some(log) = &self.log {
                    log.submit(sink::Batch::Log(record));
                }
            }
            Err(error) => {
                self.current.rejected_records = self.current.rejected_records.saturating_add(1);
                self.current.last_error = Some(error);
            }
        }
    }
    fn sample(&mut self) {
        if self.pending.is_some() {
            return;
        }
        let sources = self.sources.clone();
        let db = sources.database.clone();
        let read = self.reads.submit(4096, move |_| metrics::chain(&db));
        let timeout = self.config.sample_timeout;
        self.pending = Some(Box::pin(async move {
            tokio::time::timeout(timeout, async {
                let read = read.map_err(|_| Error::Sample)?;
                let request = sources.network.metrics().map_err(|_| Error::Sample)?;
                let (chain, registry) = tokio::join!(read, request.wait());
                Ok(metrics::snapshot(
                    &sources,
                    chain.map_err(|_| Error::Sample)??,
                    registry.map_err(|_| Error::Sample)?,
                ))
            })
            .await
            .map_err(|_| Error::Timeout)?
        }));
    }
    fn sampled(&mut self, result: Result<metrics::Snapshot, Error>) {
        match result.and_then(|metrics| {
            Ok((
                metrics.json()?,
                metrics.datagrams(&self.config.prefix)?,
                metrics,
            ))
        }) {
            Ok((json, datagrams, metrics)) => {
                self.current.samples = self.current.samples.saturating_add(1);
                self.current.metrics = Some(Arc::new(metrics));
                self.record("metrics", json);
                if let Some(statsd) = &self.statsd {
                    statsd.submit(sink::Batch::Metrics(datagrams));
                }
            }
            Err(error) => {
                self.current.sample_errors = self.current.sample_errors.saturating_add(1);
                self.current.last_error = Some(error.clone());
                self.record("observer", json!({"sample_error":error.to_string()}));
            }
        }
    }
    pub async fn run(mut self) -> Status {
        let mut interval = tokio::time::interval(self.config.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        self.record("observer", json!({"state":"started"}));
        loop {
            if *self.stopped.borrow() {
                break;
            }
            if self.admission.is_closed() {
                self.current.last_error = Some(Error::InputClosed);
                break;
            }
            if let Some(bound) = self.admission.snapshot().value {
                self.sources.admission = bound.client;
            }
            if let Err(error) = self.drain_gc() {
                self.current.last_error = Some(error);
                break;
            }
            self.publish();
            tokio::select! {
                _ = self.stopped.changed() => break,
                _ = interval.tick() => self.sample(),
                _ = self.admission.changed() => (),
                _ = tokio::task::yield_now(), if self.gc_pending => (),
                result = async { self.pending.as_mut().expect("active sample").await }, if self.pending.is_some() => {
                    self.pending = None; self.sampled(result);
                },
                event = self.incoming.recv() => match event {
                    Some(event) => {
                        let (category, data) = summarize(event);
                        self.current.events = self.current.events.saturating_add(1);
                        self.record(category, data);
                    }
                    None => { self.current.last_error = Some(Error::InputClosed); break; }
                }
            }
        }
        self.pending = None;
        self.reads.stop();
        self.current.running = false;
        self.record("observer", json!({"state":"stopped"}));
        self.ring.lock().unwrap_or_else(|e| e.into_inner()).close();
        self.publish();
        self.reads.join().await;
        if let Some(log) = &mut self.log {
            log.finish().await;
        }
        if let Some(statsd) = &mut self.statsd {
            statsd.finish().await;
        }
        self.current.joined = true;
        self.publish();
        self.current.clone()
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.reads.stop();
        self.ring.lock().unwrap_or_else(|e| e.into_inner()).close();
        if !self.current.joined {
            if let Some(log) = &mut self.log {
                log.abort();
            }
            if let Some(statsd) = &mut self.statsd {
                statsd.abort();
            }
            self.current.running = false;
            self.current.last_error.get_or_insert(Error::Closed);
            self.publish();
        }
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn hash(bytes: &[u8]) -> Option<String> {
    (bytes.len() == 32).then(|| hex(bytes))
}
fn summarize(event: peer::Event) -> (&'static str, Value) {
    // Consume the event before submitting any diagnostic I/O. Dropping its
    // incoming byte permit never depends on a subscriber or a sink.
    use peer::EventKind;
    match event.into_kind() {
        EventKind::Status {
            client,
            temporary,
            genesis,
            best,
            difficulty,
        } => (
            "peer",
            json!({"event":"announcement","peer":hex(&client.peer().0),"closing":client.is_disconnecting() || client.is_closed(),"temporary":temporary,"genesis":hash(&genesis),"best":hash(&best),"difficulty":difficulty.to_u64().ok(),"difficulty_bytes":difficulty.as_bytes().len()}),
        ),
        EventKind::Capabilities { client, chain_poi } => (
            "peer",
            json!({"event":"capabilities","peer":hex(&client.peer().0),"closing":client.is_disconnecting() || client.is_closed(),"height":chain_poi.height.to_u64().ok(),"proof_bytes":chain_poi.poi.len()}),
        ),
        EventKind::Chain { client, intent } => {
            let kind = match intent {
                peer::ChainIntent::Done => "peer_done",
                peer::ChainIntent::Generation(_) => "generation_announced",
                peer::ChainIntent::Start { .. } => "sync_announced",
            };
            (
                "sync",
                json!({"event":kind,"peer":hex(&client.peer().0),"closing":client.is_disconnecting() || client.is_closed()}),
            )
        }
        EventKind::Unhandled { client, message } => (
            "peer",
            json!({"event":"unhandled","peer":hex(&client.peer().0),"kind":format!("{:?}",message.message().kind()),"version":message.message().version()}),
        ),
        EventKind::Network(effect) => {
            use ae_p2p::registry::Effect;
            let data = match effect {
                Effect::Blocked(peer) => json!({"event":"blocked","peer":hex(&peer.0)}),
                Effect::Unblocked(peer) => json!({"event":"unblocked","peer":hex(&peer.0)}),
                Effect::ProbeStatus { peer, alive } => {
                    json!({"event":"probe","peer":hex(&peer.0),"alive":alive})
                }
                _ => json!({"event":"unexpected_notice"}),
            };
            ("network", data)
        }
    }
}
