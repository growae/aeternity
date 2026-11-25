use super::*;
use std::{
    collections::VecDeque,
    io::{self, Write},
    net::SocketAddr,
    sync::Condvar,
    thread,
};

#[derive(Default)]
pub struct Outputs {
    pub log: Option<Box<dyn Write + Send>>,
    // Explicit opt-in; port zero disables transmission as in aec_metrics.
    pub statsd: Option<SocketAddr>,
    pub metrics: Option<statsd::Client>,
}
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub running: bool,
    pub ready: bool,
    pub joined: bool,
    pub connected: bool,
    pub target: Option<SocketAddr>,
    pub open_attempts: u64,
    pub skipped: u64,
    pub outstanding: usize,
    pub retained_bytes: usize,
    pub accepted: u64,
    pub written: u64,
    pub failed: u64,
    pub dropped: u64,
    pub panicked: bool,
    pub last_error: Option<io::ErrorKind>,
}
pub(super) enum Batch {
    Log(Arc<Record>),
    Metrics(Vec<Vec<u8>>),
}
impl Batch {
    fn bytes(&self) -> usize {
        64 + match self {
            Self::Log(r) => r.json_line().len(),
            Self::Metrics(m) => m.iter().map(|v| v.len() + 32).sum(),
        }
    }
}
enum Destination {
    Log(Box<dyn Write + Send>),
    Udp(statsd::Udp),
}
impl Destination {
    fn write(&mut self, batch: &Batch) -> io::Result<bool> {
        match (self, batch) {
            (Self::Log(w), Batch::Log(record)) => w.write_all(record.json_line()).map(|()| true),
            (Self::Udp(udp), Batch::Metrics(lines)) => udp.write(lines),
            _ => Err(io::ErrorKind::InvalidInput.into()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Self::Log(w) = self {
            w.flush()
        } else {
            Ok(())
        }
    }
}
fn connect(udp: &mut statsd::Udp, shared: &Shared) {
    let result = udp.connect();
    let mut s = shared.state.lock().unwrap_or_else(|e| e.into_inner());
    if udp.address.is_some_and(|a| a.port() != 0) {
        s.info.open_attempts = s.info.open_attempts.saturating_add(1);
    }
    s.info.target = udp.address;
    s.info.connected = udp.connected();
    if let Err(error) = result {
        s.info.failed = s.info.failed.saturating_add(1);
        s.info.last_error = Some(error.kind());
    }
    shared.done.send_replace(false);
}
struct State {
    info: Info,
    queue: VecDeque<Batch>,
    stop: bool,
    started: bool,
    #[cfg(test)]
    panic: bool,
}
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    done: watch::Sender<bool>,
}
struct Finished(Arc<Shared>);
impl Drop for Finished {
    fn drop(&mut self) {
        let mut s = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        s.info.running = false;
        s.info.connected = false;
        s.info.dropped = s.info.dropped.saturating_add(s.queue.len() as u64);
        s.queue.clear();
        s.info.outstanding = 0;
        s.info.retained_bytes = 0;
        if thread::panicking() {
            s.info.panicked = true;
            s.info.failed = s.info.failed.saturating_add(1);
        }
        self.0.done.send_replace(true);
    }
}
pub(super) struct Worker {
    shared: Arc<Shared>,
    join: Option<thread::JoinHandle<()>>,
    updates: watch::Receiver<bool>,
    count: usize,
    bytes: usize,
}
impl Worker {
    fn spawn(destination: Destination, config: &Config, name: &str) -> Result<Self, Error> {
        Self::prepared(destination, config, name, true)
    }
    fn prepared(
        mut destination: Destination,
        config: &Config,
        name: &str,
        started: bool,
    ) -> Result<Self, Error> {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                info: Info {
                    running: true,
                    ..Default::default()
                },
                queue: VecDeque::new(),
                stop: false,
                started,
                #[cfg(test)]
                panic: false,
            }),
            wake: Condvar::new(),
            done: watch::channel(false).0,
        });
        let updates = shared.done.subscribe();
        let owned = shared.clone();
        let join = thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                let _finished = Finished(owned.clone());
                {
                    let mut s = owned.state.lock().unwrap_or_else(|e| e.into_inner());
                    while !s.started && !s.stop {
                        s = owned.wake.wait(s).unwrap_or_else(|e| e.into_inner());
                    }
                    if s.stop && !(s.started && matches!(&destination, Destination::Log(_))) {
                        return;
                    }
                }
                if let Destination::Udp(udp) = &mut destination {
                    if let Err(error) = udp.resolve() {
                        let mut s = owned.state.lock().unwrap_or_else(|e| e.into_inner());
                        s.info.failed += 1;
                        s.info.last_error = Some(error.kind());
                        return;
                    }
                    if owned.state.lock().unwrap_or_else(|e| e.into_inner()).stop {
                        return;
                    }
                    connect(udp, &owned);
                }
                {
                    owned
                        .state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .info
                        .ready = true;
                    owned.done.send_replace(false);
                }
                loop {
                    let mut s = owned.state.lock().unwrap_or_else(|e| e.into_inner());
                    #[cfg(test)]
                    if s.panic {
                        drop(s);
                        panic!("controlled metrics worker panic");
                    }
                    if !s.stop
                        && let Destination::Udp(udp) = &mut destination
                        && udp
                            .next
                            .is_some_and(|next| next <= std::time::Instant::now())
                    {
                        drop(s);
                        connect(udp, &owned);
                        // Zero is a valid native delay. Yield between failed
                        // opens without changing it to an invented backoff.
                        thread::yield_now();
                        s = owned.state.lock().unwrap_or_else(|e| e.into_inner());
                    }
                    let batch = match s.queue.pop_front() {
                        Some(batch) => batch,
                        None if s.stop => break,
                        None => {
                            let next = match &destination {
                                Destination::Udp(u) => u.next,
                                _ => None,
                            };
                            if let Some(next) = next {
                                drop(
                                    owned
                                        .wake
                                        .wait_timeout(
                                            s,
                                            next.saturating_duration_since(
                                                std::time::Instant::now(),
                                            ),
                                        )
                                        .unwrap_or_else(|e| e.into_inner()),
                                );
                            } else {
                                drop(owned.wake.wait(s).unwrap_or_else(|e| e.into_inner()));
                            }
                            continue;
                        }
                    };
                    drop(s);
                    let result = destination.write(&batch);
                    let mut s = owned.state.lock().unwrap_or_else(|e| e.into_inner());
                    s.info.outstanding -= 1;
                    s.info.retained_bytes -= batch.bytes();
                    match result {
                        Ok(true) => s.info.written = s.info.written.saturating_add(1),
                        Ok(false) => s.info.skipped = s.info.skipped.saturating_add(1),
                        Err(e) => {
                            s.info.failed = s.info.failed.saturating_add(1);
                            s.info.last_error = Some(e.kind());
                        }
                    }
                    owned.done.send_replace(false);
                }
                if let Err(e) = destination.flush() {
                    let mut s = owned.state.lock().unwrap_or_else(|e| e.into_inner());
                    s.info.failed = s.info.failed.saturating_add(1);
                    s.info.last_error = Some(e.kind());
                }
            })
            .map_err(|e| Error::Io(e.kind()))?;
        Ok(Self {
            shared,
            join: Some(join),
            updates,
            count: config.sink_records,
            bytes: config.sink_bytes,
        })
    }
    pub fn outputs(
        outputs: Outputs,
        config: &Config,
    ) -> Result<(Option<Self>, Option<Self>), Error> {
        if outputs.statsd.is_some() && config.statsd.is_some() {
            return Err(Error::Configuration);
        }
        let log = outputs
            .log
            .map(|writer| Self::spawn(Destination::Log(writer), config, "ae-json-log"))
            .transpose()?;
        let target = config
            .statsd
            .clone()
            .map(statsd::Target::Configured)
            .or_else(|| {
                outputs
                    .statsd
                    .filter(|a| a.port() != 0)
                    .map(statsd::Target::Address)
            });
        let udp = target
            .map(|target| {
                Self::spawn(
                    Destination::Udp(statsd::Udp::new(target)),
                    config,
                    "ae-statsd",
                )
            })
            .transpose()?;
        Ok((log, udp))
    }
    pub fn metrics(config: &Config, started: bool) -> Result<Self, Error> {
        config.validate()?;
        let target = config.statsd.clone().ok_or(Error::Configuration)?;
        Self::prepared(
            Destination::Udp(statsd::Udp::new(statsd::Target::Configured(target))),
            config,
            "ae-statsd",
            started,
        )
    }
    #[cfg(test)]
    pub fn metrics_with_backend(
        config: &Config,
        backend: Box<dyn statsd::Backend>,
    ) -> Result<Self, Error> {
        config.validate()?;
        Self::prepared(
            Destination::Udp(statsd::Udp::with_backend(
                statsd::Target::Configured(config.statsd.clone().ok_or(Error::Configuration)?),
                backend,
            )),
            config,
            "ae-statsd-test",
            false,
        )
    }
    pub fn activate(&self) {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .started = true;
        self.shared.wake.notify_one();
    }
    pub fn client(&self) -> Client {
        Client {
            shared: self.shared.clone(),
            updates: self.shared.done.subscribe(),
            count: self.count,
            bytes: self.bytes,
        }
    }
    pub async fn changed(&mut self) {
        let _ = self.updates.changed().await;
    }
    pub fn submit(&self, batch: Batch) {
        self.client().submit(batch);
    }
    pub fn info(&self) -> Info {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .info
            .clone()
    }
    pub(super) fn stop(&self, drain: bool) {
        let mut s = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        s.stop = true;
        if !drain {
            while let Some(batch) = s.queue.pop_front() {
                s.info.outstanding -= 1;
                s.info.retained_bytes -= batch.bytes();
                s.info.dropped = s.info.dropped.saturating_add(1);
            }
        }
        self.shared.wake.notify_one();
    }
    pub async fn finish(&mut self) {
        self.stop(true);
        let mut done = self.shared.done.subscribe();
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                break;
            }
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
            self.shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .info
                .joined = true;
            self.shared.done.send_replace(true);
        }
    }
    pub fn abort(&mut self) {
        self.stop(false);
        if let Some(join) = self.join.take() {
            let _ = join.join();
            self.shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .info
                .joined = true;
            self.shared.done.send_replace(true);
        }
    }
}
#[derive(Clone)]
pub(super) struct Client {
    shared: Arc<Shared>,
    updates: watch::Receiver<bool>,
    count: usize,
    bytes: usize,
}
impl Client {
    #[cfg(test)]
    pub fn panic_for_test(&self) {
        self.shared.state.lock().unwrap().panic = true;
        self.shared.wake.notify_one();
    }
    pub fn stop(&self) {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stop = true;
        self.shared.wake.notify_one();
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
    pub fn submit(&self, batch: Batch) -> bool {
        let mut s = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        let bytes = batch.bytes();
        if !s.info.running
            || s.stop
            || s.info.outstanding == self.count
            || bytes > self.bytes.saturating_sub(s.info.retained_bytes)
        {
            s.info.dropped = s.info.dropped.saturating_add(1);
            return false;
        }
        s.info.accepted = s.info.accepted.saturating_add(1);
        s.info.outstanding += 1;
        s.info.retained_bytes += bytes;
        s.queue.push_back(batch);
        self.shared.wake.notify_one();
        true
    }
    pub fn info(&self) -> Info {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .info
            .clone()
    }
    pub async fn changed(&mut self) {
        let _ = self.updates.changed().await;
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(test)]
mod tests;
