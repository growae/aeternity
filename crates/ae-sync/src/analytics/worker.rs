use super::*;
use crate::tasks::Tasks;
use ae_p2p::{
    message::{Details, Kind, Message, Value},
    runtime as net,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread::{self, JoinHandle},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot, watch};

type Clock = Arc<dyn Fn() -> Result<i64, Error> + Send + Sync>;
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
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub running: bool,
    pub accepting: bool,
    pub joined: bool,
    pub pending: usize,
    pub pending_bytes: usize,
    pub peers: usize,
    pub probes: usize,
    pub submitted: u64,
    pub completed: u64,
    pub probe_completions: u64,
    pub deferred_probes: u64,
    pub last_error: Option<Error>,
    pub failure: Option<Error>,
}
struct Shared {
    info: Mutex<Info>,
    limits: Limits,
    enabled: bool,
    stop: watch::Sender<bool>,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Info> {
        self.info.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn stop(&self) {
        self.lock().accepting = false;
        self.stop.send_replace(true);
    }
}
struct Log {
    announcement: Announcement,
    target: Option<net::Client>,
    reply: oneshot::Sender<Result<Recorded, Error>>,
}
enum Command {
    Log(Box<Log>),
    Snapshot(oneshot::Sender<Result<Snapshot, Error>>),
    Sweep(oneshot::Sender<Result<usize, Error>>),
    #[cfg(test)]
    Pause(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    ),
    #[cfg(test)]
    Fail,
}
struct Queued {
    bytes: usize,
    command: Command,
}
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
    commands: mpsc::UnboundedSender<Queued>,
}
impl Client {
    pub fn info(&self) -> Info {
        self.shared.lock().clone()
    }
    pub fn enabled(&self) -> bool {
        self.shared.enabled
    }
    fn submit<T>(
        &self,
        bytes: usize,
        make: impl FnOnce(oneshot::Sender<Result<T, Error>>) -> Command,
    ) -> Result<Ticket<T>, Error> {
        let mut s = self.shared.lock();
        if !s.accepting {
            return Err(Error::Closed);
        }
        if s.pending >= self.shared.limits.requests
            || bytes
                > self
                    .shared
                    .limits
                    .request_bytes
                    .saturating_sub(s.pending_bytes)
        {
            return Err(Error::Busy);
        }
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Queued {
                bytes,
                command: make(tx),
            })
            .map_err(|_| Error::Closed)?;
        s.pending += 1;
        s.pending_bytes += bytes;
        s.submitted = s.submitted.saturating_add(1);
        Ok(Ticket(rx))
    }
    pub fn log(
        &self,
        a: Announcement,
        target: Option<net::Client>,
    ) -> Result<Ticket<Recorded>, Error> {
        self.shared.limits.announcement(&a)?;
        if target.as_ref().is_some_and(|t| t.peer() != a.peer) {
            return Err(Error::Stale);
        }
        self.submit(
            a.bytes()
                .saturating_add(std::mem::size_of::<net::Client>() + 128),
            |tx| {
                Command::Log(Box::new(Log {
                    announcement: a,
                    target,
                    reply: tx,
                }))
            },
        )
    }
    pub fn snapshot(&self) -> Result<Ticket<Snapshot>, Error> {
        self.submit(64, Command::Snapshot)
    }
    pub fn sweep(&self) -> Result<Ticket<usize>, Error> {
        self.submit(64, Command::Sweep)
    }
}
pub struct Worker {
    client: Client,
    thread: Option<JoinHandle<()>>,
}
impl Worker {
    pub fn spawn(config: Config) -> Result<Self, Error> {
        Self::with_clock(
            config,
            Arc::new(|| {
                let time = SystemTime::now();
                match time.duration_since(UNIX_EPOCH) {
                    Ok(d) => i64::try_from(d.as_secs()).map_err(|_| Error::Clock),
                    Err(e) => {
                        let d = e.duration();
                        let seconds = d
                            .as_secs()
                            .checked_add(u64::from(d.subsec_nanos() != 0))
                            .ok_or(Error::Clock)?;
                        i64::try_from(seconds)
                            .ok()
                            .and_then(i64::checked_neg)
                            .ok_or(Error::Clock)
                    }
                }
            }),
        )
    }
    pub(super) fn with_clock(config: Config, clock: Clock) -> Result<Self, Error> {
        let engine = Engine::new(config.clone())?;
        let (stop, stopped) = watch::channel(false);
        let shared = Arc::new(Shared {
            info: Mutex::new(Info {
                running: true,
                accepting: true,
                ..Default::default()
            }),
            limits: config.limits,
            enabled: config.enabled,
            stop,
        });
        let (commands, receiver) = mpsc::unbounded_channel();
        let client = Client {
            shared: shared.clone(),
            commands,
        };
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("ae-peer-analytics".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .build()
                    {
                        Ok(r) => r,
                        Err(_) => {
                            let _ = ready.send(Err(Error::Spawn));
                            return Err(Error::Spawn);
                        }
                    };
                    let _ = ready.send(Ok(()));
                    runtime.block_on(run(engine, shared.clone(), receiver, stopped, clock));
                    Ok(())
                }));
                let mut s = shared.lock();
                s.running = false;
                s.accepting = false;
                s.pending = 0;
                s.pending_bytes = 0;
                s.probes = 0;
                s.failure = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(e),
                    Err(_) => Some(Error::Panicked),
                };
            })
            .map_err(|_| Error::Spawn)?;
        match started.recv() {
            Ok(Ok(())) => Ok(Self {
                client,
                thread: Some(thread),
            }),
            result => {
                let _ = thread.join();
                Err(result.ok().and_then(Result::err).unwrap_or(Error::Panicked))
            }
        }
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn info(&self) -> Info {
        self.client.info()
    }
    pub fn request_shutdown(&self) {
        self.client.shared.stop();
    }
    fn join(&mut self) -> Result<(), Error> {
        self.request_shutdown();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| Error::Panicked)?;
        }
        let mut info = self.client.shared.lock();
        info.joined = true;
        info.failure.clone().map_or(Ok(()), Err)
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
fn publish(engine: &Engine, shared: &Shared) {
    let mut s = shared.lock();
    s.peers = engine.stats.len();
    s.probes = engine.pending.len();
}
async fn run(
    mut engine: Engine,
    shared: Arc<Shared>,
    mut commands: mpsc::UnboundedReceiver<Queued>,
    mut stopped: watch::Receiver<bool>,
    clock: Clock,
) {
    let mut probes = Tasks::new();
    let mut closing = false;
    let mut drained = false;
    let mut sweep =
        tokio::time::interval_at(tokio::time::Instant::now() + SWEEP_INTERVAL, SWEEP_INTERVAL);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if !closing && (*stopped.borrow() || stopped.has_changed().is_err()) {
            closing = true;
            commands.close();
        }
        if drained && probes.len() == 0 {
            break;
        }
        tokio::select! {
            _=stopped.changed(),if !closing=>{},
            queued=commands.recv(),if !drained=>match queued {
                None=>drained=true,
                Some(Queued{bytes,command})=>{
                    let (error,reply):(Option<Error>,Box<dyn FnOnce()>)=match command {
                        Command::Log(log)=>{
                            let Log {announcement:a,target,reply}=*log;
                            let result=if !engine.config.enabled {Ok(Recorded::default())} else {(||{let first=clock()?;let last=clock()?;engine.record(a,first,last)})()};
                            if let Ok(recorded)=&result {
                                if recorded.deferred {let mut s=shared.lock();s.deferred_probes=s.deferred_probes.saturating_add(1);}
                                if let Some(probe)=recorded.probe {let limits=engine.config.limits;probes.insert(probe,probe_peer(target,limits));}
                            }
                            (result.as_ref().err().cloned(),Box::new(move||{let _=reply.send(result);}))
                        },
                        Command::Snapshot(reply)=>{let snapshot=engine.snapshot();(None,Box::new(move||{let _=reply.send(Ok(snapshot));}))},
                        Command::Sweep(reply)=>{let removed=engine.sweep();(None,Box::new(move||{let _=reply.send(Ok(removed));}))},
                        #[cfg(test)]
                        Command::Pause(entered,release)=>{let _=entered.send(());let _=release.recv();(None,Box::new(||{}))},
                        #[cfg(test)]
                        Command::Fail=>panic!("injected analytics failure"),
                    };
                    {let mut s=shared.lock();s.pending-=1;s.pending_bytes-=bytes;s.completed=s.completed.saturating_add(1);s.last_error=error;}
                    publish(&engine,&shared);reply();
                }
            },
            (probe,result)=probes.next()=>{
                let error=engine.complete(probe,result).err();let mut s=shared.lock();s.probe_completions=s.probe_completions.saturating_add(1);s.last_error=error;drop(s);publish(&engine,&shared);
            },
            _=sweep.tick(),if !closing=>{engine.sweep();publish(&engine,&shared);},
        }
    }
}
async fn probe_peer(target: Option<net::Client>, limits: Limits) -> ProbeResult {
    let Some(client) = target.filter(|c| !c.is_closed()) else {
        return ProbeResult::NoConnection;
    };
    let request = match Message::new(
        Kind::GetNodeInfo,
        1,
        BTreeMap::new(),
        client.message_limits(),
    ) {
        Ok(m) => m,
        Err(_) => return ProbeResult::Unknown,
    };
    let ticket = match client.request(&request, Some(PROBE_TIMEOUT)) {
        Ok(t) => t,
        Err(_) => return ProbeResult::Unknown,
    };
    // The driver owns the wire deadline. This also bounds an undriven/retired
    // driver's ticket; abandoning that ticket closes only its exact connection.
    let reply = tokio::time::timeout(PROBE_TIMEOUT + Duration::from_millis(100), ticket).await;
    let Ok(Ok(net::Reply::Response(received))) = reply else {
        return ProbeResult::Unknown;
    };
    let Details::Response {
        message: Some(Ok(message)),
        ..
    } = received.message().details()
    else {
        return ProbeResult::Unknown;
    };
    if message.kind() != Kind::NodeInfo {
        return ProbeResult::Unknown;
    }
    let field = |name: &str| match message.fields().get(name) {
        Some(Value::Binary(b)) if b.len() <= limits.field_bytes => Some(b.clone()),
        _ => None,
    };
    let info = (|| {
        Some(NodeInfo {
            network_id: field("network_id")?,
            version: field("version")?,
            os: field("os")?,
            revision: field("revision")?,
            vendor: field("vendor")?,
        })
    })();
    info.map_or(ProbeResult::Unknown, ProbeResult::Resolved)
}
#[cfg(test)]
pub(super) fn pause(
    client: &Client,
) -> (
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::SyncSender<()>,
) {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let (release, released) = std::sync::mpsc::sync_channel(1);
    drop(
        client
            .submit::<()>(64, |_| Command::Pause(tx, released))
            .unwrap(),
    );
    (rx, release)
}
#[cfg(test)]
pub(super) fn fail(client: &Client) {
    drop(client.submit::<()>(64, |_| Command::Fail).unwrap());
}
