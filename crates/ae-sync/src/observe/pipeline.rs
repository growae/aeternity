use super::{report, sink, statsd};
use crate::{bindings, routing};
use std::{
    collections::VecDeque,
    io::{self, Write},
    sync::{Arc, Condvar, Mutex},
    thread,
};
use tokio::sync::watch;

#[derive(Clone)]
pub enum Routing {
    Fixed(routing::Client),
    Managed(bindings::MetricsRouting),
}
impl Routing {
    fn destinations(&self, key: routing::Key) -> Result<routing::Destinations, Error> {
        let (client, available) = match self {
            Self::Fixed(c) => (c.clone(), true),
            Self::Managed(b) => {
                let snapshot = b.snapshot();
                if snapshot.closed {
                    return Err(Error::RoutingClosed);
                }
                (
                    snapshot.value.ok_or(Error::RoutingClosed)?,
                    snapshot.available,
                )
            }
        };
        // Native ETS lookups survive routing-server replacement. Inspect the
        // retained table even while its worker is withdrawn; only a cache miss
        // requires an admitted operation on the exact available generation.
        if let Some(destinations) = client.cached(&key).map_err(Error::Routing)? {
            return Ok(Some(destinations));
        }
        if !available {
            return Err(Error::RoutingClosed);
        }
        client
            .destinations(key)
            .map_err(Error::Routing)?
            .blocking_wait()
            .map_err(Error::Routing)
    }
}
type Writer = Arc<Mutex<Box<dyn Write + Send>>>;
#[derive(Clone)]
pub struct Config {
    pub limits: report::Limits,
    routing: Arc<Mutex<Option<Routing>>>,
    log: Arc<Mutex<Option<Writer>>>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            limits: report::Limits::default(),
            routing: Arc::new(Mutex::new(None)),
            log: Arc::new(Mutex::new(None)),
        }
    }
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl Config {
    pub fn set_routing(&self, source: Option<Routing>) {
        *self.routing.lock().unwrap_or_else(|e| e.into_inner()) = source;
    }
    pub fn set_log(&self, writer: Option<Box<dyn Write + Send>>) {
        *self.log.lock().unwrap_or_else(|e| e.into_inner()) =
            writer.map(|w| Arc::new(Mutex::new(w)));
    }
    fn destinations(&self, key: routing::Key) -> Result<routing::Destinations, Error> {
        let source = self
            .routing
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        match source {
            None => Ok(None),
            Some(source) => source.destinations(key),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Closed,
    Busy,
    Report(report::Error),
    RoutingClosed,
    Routing(routing::Error),
    Io(io::ErrorKind),
}
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub running: bool,
    pub accepting: bool,
    pub joined: bool,
    pub disabled: bool,
    pub accepted: u64,
    pub forwarded: u64,
    pub dropped: u64,
    pub pending: usize,
    pub retained_bytes: usize,
    pub filtered_log: u64,
    pub filtered_send: u64,
    pub logged: u64,
    pub sent: u64,
    pub skipped_log: u64,
    pub skipped_send: u64,
    pub last_error: Option<Error>,
}
struct Record {
    event: report::Event,
    bytes: usize,
}
struct State {
    info: Info,
    queue: VecDeque<Record>,
    closing: bool,
    abort: bool,
    disabled: Option<Result<(), Error>>,
    #[cfg(test)]
    panic: bool,
    #[cfg(test)]
    gate: Option<Gate>,
}
#[cfg(test)]
struct Gate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}
pub(super) struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    done: watch::Sender<bool>,
    limits: report::Limits,
    records: usize,
    bytes: usize,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn close(&self, abort: bool) {
        let mut s = self.lock();
        s.closing = true;
        s.abort |= abort;
        s.info.accepting = false;
        if abort {
            let count = s.queue.len();
            let bytes = s.queue.iter().map(|r| r.bytes).sum::<usize>();
            s.info.dropped = s.info.dropped.saturating_add(count as u64);
            s.queue.clear();
            s.info.pending = s.info.pending.saturating_sub(count);
            s.info.retained_bytes = s.info.retained_bytes.saturating_sub(bytes);
            if s.disabled.is_none() {
                s.disabled = Some(Err(Error::Closed));
            }
            self.done.send_modify(|_| ());
        }
        self.wake.notify_one();
    }
    fn join(&self) {
        if let Some(join) = self.join.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = join.join();
        }
        self.lock().info.joined = true;
    }
    fn fail(&self, e: Error) {
        self.lock().info.last_error = Some(e);
        self.close(true);
    }
}
struct Finished(Arc<Shared>, sink::Client);
impl Drop for Finished {
    fn drop(&mut self) {
        let mut s = self.0.lock();
        s.info.running = false;
        s.info.accepting = false;
        let dropped = s.queue.len() as u64;
        s.info.dropped = s.info.dropped.saturating_add(dropped);
        s.queue.clear();
        s.info.pending = 0;
        s.info.retained_bytes = 0;
        if thread::panicking() {
            s.info.last_error = Some(Error::Io(io::ErrorKind::Other));
            s.disabled = Some(Err(Error::Io(io::ErrorKind::Other)));
        }
        self.0.done.send_replace(true);
        drop(s);
        if thread::panicking() {
            self.1.input_failed();
        }
        // A retiring logger may already have exited while its input is still
        // draining. Wake the same owner watch so a cancelled retirement can
        // resume after this independently owned thread finishes.
        self.1.input_updated();
    }
}
#[derive(Clone)]
pub struct Input(Arc<Shared>);
pub struct Ticket {
    input: Input,
}
impl Ticket {
    pub async fn wait(self) -> Result<(), Error> {
        let mut done = self.input.0.done.subscribe();
        let result = loop {
            let result = self.input.0.lock().disabled.clone();
            if let Some(result) = result {
                break result;
            }
            let _ = done.changed().await;
        };
        while !*done.borrow_and_update() {
            let _ = done.changed().await;
        }
        self.input.0.join();
        result
    }
}
impl Input {
    #[cfg(test)]
    fn panic_for_test(&self) {
        self.0.lock().panic = true;
        self.0.wake.notify_one();
    }
    pub fn info(&self) -> Info {
        self.0.lock().info.clone()
    }
    pub fn report(&self, event: report::Event) -> Result<(), Error> {
        let bytes = event.retained_bytes(self.0.limits).map_err(Error::Report)?;
        let mut s = self.0.lock();
        if !s.info.accepting {
            return Err(Error::Closed);
        }
        if s.info.pending >= self.0.records
            || bytes > self.0.bytes.saturating_sub(s.info.retained_bytes)
        {
            s.info.dropped = s.info.dropped.saturating_add(1);
            return Err(Error::Busy);
        }
        // Clone only after admission. Vec/String clones discard caller spare
        // capacity, so retained queue storage follows the checked value size.
        s.queue.push_back(Record {
            event: event.clone(),
            bytes,
        });
        s.info.pending += 1;
        s.info.retained_bytes += bytes;
        s.info.accepted = s.info.accepted.saturating_add(1);
        self.0.wake.notify_one();
        Ok(())
    }
    pub fn prepare_stop(&self) -> Result<Ticket, Error> {
        let mut s = self.0.lock();
        if s.abort || (!s.info.running && !s.closing) {
            return Err(Error::Closed);
        }
        s.closing = true;
        s.info.accepting = false;
        self.0.wake.notify_one();
        Ok(Ticket {
            input: self.clone(),
        })
    }
}
pub(super) struct Owner(Arc<Shared>);
impl Owner {
    pub fn prepare(config: &Config, records: usize, bytes: usize) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                info: Info {
                    running: true,
                    accepting: true,
                    ..Default::default()
                },
                queue: VecDeque::new(),
                closing: false,
                abort: false,
                disabled: None,
                #[cfg(test)]
                panic: false,
                #[cfg(test)]
                gate: None,
            }),
            wake: Condvar::new(),
            join: Mutex::new(None),
            done: watch::channel(false).0,
            limits: config.limits,
            records,
            bytes,
        }))
    }
    pub fn input(&self) -> Input {
        Input(self.0.clone())
    }
    pub fn spawn(&self, sink: sink::Client) -> io::Result<()> {
        let shared = self.0.clone();
        let join = thread::Builder::new()
            .name("ae-metrics-input".into())
            .spawn(move || {
                let _finished = Finished(shared.clone(), sink.clone());
                loop {
                    let mut s = shared.lock();
                    #[cfg(test)]
                    if s.panic {
                        drop(s);
                        panic!("controlled metrics subscriber failure");
                    }
                    if s.abort {
                        return;
                    }
                    if let Some(record) = s.queue.pop_front() {
                        #[cfg(test)]
                        let gate = s.gate.take();
                        drop(s);
                        #[cfg(test)]
                        if let Some(gate) = gate {
                            let _ = gate.entered.send(());
                            let _ = gate.release.recv();
                        }
                        let accepted =
                            sink.submit(sink::Batch::Report(Box::new(record.event), record.bytes));
                        let mut s = shared.lock();
                        s.info.pending = s.info.pending.saturating_sub(1);
                        s.info.retained_bytes = s.info.retained_bytes.saturating_sub(record.bytes);
                        if accepted {
                            s.info.forwarded = s.info.forwarded.saturating_add(1);
                        } else {
                            s.info.dropped = s.info.dropped.saturating_add(1);
                        }
                    } else if s.closing {
                        drop(s);
                        sink.disable(Disable {
                            input: shared.clone(),
                            acknowledged: false,
                        });
                        return;
                    } else {
                        drop(shared.wake.wait(s).unwrap_or_else(|e| e.into_inner()));
                    }
                }
            })?;
        *self.0.join.lock().unwrap_or_else(|e| e.into_inner()) = Some(join);
        Ok(())
    }
    pub async fn finish(&self, abort: bool) {
        self.0.close(abort);
        let mut done = self.0.done.subscribe();
        if self
            .0
            .join
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
        {
            self.0.lock().info.running = false;
            self.0.done.send_replace(true);
        }
        while !*done.borrow_and_update() {
            let _ = done.changed().await;
        }
        self.0.join();
    }
    pub fn abort(&self) {
        self.0.close(true);
        self.0.join();
        self.0.lock().info.running = false;
        self.0.done.send_replace(true);
    }
}

#[cfg(test)]
mod tests;
impl Drop for Owner {
    fn drop(&mut self) {
        self.abort();
    }
}
pub(super) struct Disable {
    input: Arc<Shared>,
    acknowledged: bool,
}
impl Disable {
    pub fn acknowledge(mut self) {
        self.input.join();
        {
            let mut state = self.input.lock();
            state.info.disabled = true;
            state.disabled = Some(Ok(()));
        }
        self.acknowledged = true;
        self.input.done.send_modify(|_| ());
    }
}
impl Drop for Disable {
    fn drop(&mut self) {
        if !self.acknowledged {
            self.input.lock().disabled = Some(Err(Error::Closed));
            self.input.done.send_modify(|_| ());
        }
    }
}
pub(super) struct Output {
    pub udp: statsd::Udp,
    config: Config,
    input: Arc<Shared>,
    pub disabled: bool,
    pub failed: bool,
}
impl Output {
    pub fn new(config: Config, udp: statsd::Udp, input: &Owner) -> Self {
        Self {
            udp,
            config,
            input: input.0.clone(),
            disabled: false,
            failed: false,
        }
    }
    pub fn write(&mut self, event: &report::Event) -> io::Result<bool> {
        if self.disabled {
            return Ok(false);
        }
        let result = self.emit(event);
        match result {
            Ok(written) => Ok(written),
            Err(Error::Io(kind)) => {
                self.input.lock().info.last_error = Some(Error::Io(kind));
                Err(kind.into())
            }
            Err(error) => {
                self.failed = true;
                self.input.fail(error);
                Err(io::ErrorKind::InvalidData.into())
            }
        }
    }
    fn emit(&mut self, event: &report::Event) -> Result<bool, Error> {
        let destinations = self.config.destinations(event.key.clone())?;
        let selected = |name: &str| {
            destinations
                .as_ref()
                .is_none_or(|d| d.iter().any(|s| s == name))
        };
        let log = selected("log");
        let send = selected("send");
        {
            let mut s = self.input.lock();
            s.info.filtered_log += u64::from(!log);
            s.info.filtered_send += u64::from(!send);
        }
        if !log && !send {
            return Ok(false);
        }
        let line = event.line(self.config.limits).map_err(Error::Report)?;
        // Native output order is filter, StatsD, log. A send failure does not
        // prevent the log callback or replace the socket.
        let mut error = None;
        let mut written = false;
        if send {
            match self.udp.write(std::slice::from_ref(&line)) {
                Ok(true) => {
                    self.input.lock().info.sent += 1;
                    written = true;
                }
                Ok(false) => self.input.lock().info.skipped_send += 1,
                Err(e) => error = Some(Error::Io(e.kind())),
            }
        }
        if log {
            let writer = self
                .config
                .log
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(writer) = writer {
                let mut writer = writer.lock().unwrap_or_else(|e| e.into_inner());
                if let Err(e) = writer
                    .write_all(&line)
                    .and_then(|()| writer.write_all(b"\n"))
                {
                    error = Some(Error::Io(e.kind()));
                } else {
                    self.input.lock().info.logged += 1;
                    written = true;
                }
            } else {
                self.input.lock().info.skipped_log += 1;
            }
        }
        error.map_or(Ok(written), Err)
    }
    pub fn disable(&mut self, control: Disable) {
        self.disabled = true;
        self.udp.next = None;
        control.acknowledge();
    }
    pub fn flush(&mut self) -> io::Result<()> {
        let writer = self
            .config
            .log
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(writer) = writer {
            writer.lock().unwrap_or_else(|e| e.into_inner()).flush()
        } else {
            Ok(())
        }
    }
}
