// First-parent pin reads, aec_parent_connector/aehttpc_{aeternity,doge,btc},
// aeternity fa2a6d7. Network results become immutable execution inputs.
use ae_hyperchain::block::Pin;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
mod lookup;
pub(crate) mod transport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chain {
    Aeternity,
    Dogecoin,
    Bitcoin,
}
#[derive(Clone)]
pub struct Node {
    pub(crate) chain: Chain,
    origin: url::Url,
    pub(crate) credentials: Option<(Vec<u8>, Vec<u8>)>,
}
impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("chain", &self.chain)
            .finish_non_exhaustive()
    }
}
impl Node {
    pub fn new(
        chain: Chain,
        origin: &str,
        credentials: Option<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Self, Error> {
        if origin.len() > 4096 || origin.bytes().any(|b| b <= 32 || b >= 127 || b == b'\\') {
            return Err(Error::Configuration);
        }
        let origin = url::Url::parse(origin).map_err(|_| Error::Configuration)?;
        if !matches!(origin.scheme(), "http" | "https")
            || origin.host().is_none()
            || origin.port_or_known_default().is_none_or(|p| p == 0)
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
            || credentials
                .as_ref()
                .is_some_and(|(u, p)| u.len().saturating_add(p.len()) > 4096)
            || (chain == Chain::Dogecoin && credentials.is_none())
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            chain,
            origin,
            credentials,
        })
    }
}
#[derive(Debug, Clone)]
pub struct Config {
    pub request_timeout: Duration,
    pub lookup_timeout: Duration,
    pub maximum_pending: usize,
    pub maximum_hash_bytes: usize,
    pub maximum_header_bytes: usize,
    pub maximum_body_bytes: usize,
    pub maximum_payload_bytes: usize,
    pub maximum_number_digits: usize,
    pub maximum_block_transactions: usize,
    pub public_roots: bool,
    pub roots: Vec<Vec<u8>>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(5),
            lookup_timeout: Duration::from_secs(30),
            maximum_pending: 16,
            maximum_hash_bytes: 4096,
            maximum_header_bytes: 16 * 1024,
            maximum_body_bytes: 4 * 1024 * 1024,
            maximum_payload_bytes: 4096,
            maximum_number_digits: 4096,
            maximum_block_transactions: 4096,
            public_roots: true,
            roots: vec![],
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Spawn,
    Closed,
    Busy,
    ResourceLimit,
    Deadline,
    Worker,
    Redirect,
}
struct Job {
    hash: Vec<u8>,
    reply: SyncSender<Result<Option<Pin>, Error>>,
    cancel: Arc<AtomicBool>,
    deadline: Instant,
}
struct Pending(Arc<AtomicBool>);
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
#[derive(Clone)]
pub struct Client {
    requests: SyncSender<Job>,
    stopped: Arc<AtomicBool>,
    config: Arc<Config>,
}
impl Client {
    // Nonblocking lifetime check, safe under a storage writer. Never performs I/O.
    pub fn check(&self) -> Result<(), Error> {
        if self.stopped.load(Ordering::Acquire) {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
    // Synchronous capture; run on the import worker, never on an async event loop.
    pub fn fetch(&self, hash: &[u8]) -> Result<Option<Pin>, Error> {
        self.check()?;
        if hash.len() > self.config.maximum_hash_bytes {
            return Err(Error::ResourceLimit);
        }
        let deadline = Instant::now()
            .checked_add(self.config.lookup_timeout)
            .ok_or(Error::ResourceLimit)?;
        let (reply, result) = mpsc::sync_channel(1);
        let cancel = Pending(Arc::new(AtomicBool::new(false)));
        self.requests
            .try_send(Job {
                hash: hash.to_vec(),
                reply,
                cancel: cancel.0.clone(),
                deadline,
            })
            .map_err(|e| match e {
                TrySendError::Full(_) => Error::Busy,
                TrySendError::Disconnected(_) => Error::Closed,
            })?;
        loop {
            self.check()?;
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            match result.recv_timeout(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10)),
            ) {
                Ok(value) => {
                    self.check()?;
                    if Instant::now() >= deadline {
                        return Err(Error::Deadline);
                    }
                    return value;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => (),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.check()?;
                    return Err(Error::Worker);
                }
            }
        }
    }
}
pub struct Service {
    client: Client,
    worker: Option<JoinHandle<()>>,
}
impl Service {
    pub fn spawn(nodes: Vec<Node>, config: Config) -> Result<Self, Error> {
        if nodes.is_empty()
            || nodes.len() > 32
            || !(1..=1024).contains(&config.maximum_pending)
            || !(1..=65536).contains(&config.maximum_hash_bytes)
            || !(512..=65536).contains(&config.maximum_header_bytes)
            || !(1024..=16 * 1024 * 1024).contains(&config.maximum_body_bytes)
            || !(1..=65536).contains(&config.maximum_payload_bytes)
            || !(1..=65536).contains(&config.maximum_number_digits)
            || config.maximum_block_transactions > 65536
            || config.request_timeout.is_zero()
            || config.request_timeout > Duration::from_secs(60)
            || config.lookup_timeout.is_zero()
            || config.lookup_timeout > Duration::from_secs(300)
            || config.roots.len() > 32
            || config.roots.iter().any(|r| r.len() > 65536)
        {
            return Err(Error::Configuration);
        }
        // Native pin calls use only [NodeSpec | _], with no majority or fallback.
        let node = nodes.into_iter().next().expect("nonempty parents");
        let config = Arc::new(config);
        let transport = transport::Transport::new(config.clone())?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Error::Spawn)?;
        let (requests, receiver) = mpsc::sync_channel::<Job>(config.maximum_pending);
        let stopped = Arc::new(AtomicBool::new(false));
        let stop = stopped.clone();
        let worker = thread::Builder::new().name("ae-parent-pin".into()).spawn(move || {
            struct End(Arc<AtomicBool>);
            impl Drop for End { fn drop(&mut self) { self.0.store(true, Ordering::Release); } }
            let _end = End(stop.clone());
            while !stop.load(Ordering::Acquire) {
                let job = match receiver.recv_timeout(Duration::from_millis(10)) {
                    Ok(job) => job,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                };
                let value = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        error = transport::cancelled(&stop, &job.cancel, job.deadline) => Err(error),
                        result = lookup::fetch(&transport, &node, &job.hash) => result,
                    }
                });
                let _ = job.reply.try_send(value);
            }
        }).map_err(|_| Error::Spawn)?;
        Ok(Self {
            client: Client {
                requests,
                stopped,
                config,
            },
            worker: Some(worker),
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn stop(&self) {
        self.client.stopped.store(true, Ordering::Release);
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.stop();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
