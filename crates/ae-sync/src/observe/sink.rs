use super::*;
use std::{
    collections::VecDeque,
    io::{self, Write},
    net::{SocketAddr, UdpSocket},
    sync::Condvar,
    thread,
};

#[derive(Default)]
pub struct Outputs {
    pub log: Option<Box<dyn Write + Send>>,
    // Explicit opt-in; port zero disables transmission as in aec_metrics.
    pub statsd: Option<SocketAddr>,
}
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub running: bool,
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
    Udp {
        target: SocketAddr,
        socket: Option<UdpSocket>,
    },
}
impl Destination {
    fn write(&mut self, batch: &Batch) -> io::Result<()> {
        match (self, batch) {
            (Self::Log(w), Batch::Log(record)) => w.write_all(record.json_line()),
            (Self::Udp { target, socket }, Batch::Metrics(lines)) => {
                if socket.is_none() {
                    let bind: SocketAddr = if target.is_ipv4() {
                        "0.0.0.0:0"
                    } else {
                        "[::]:0"
                    }
                    .parse()
                    .unwrap();
                    let opened = UdpSocket::bind(bind)?;
                    opened.set_nonblocking(true)?;
                    *socket = Some(opened);
                }
                for line in lines {
                    match socket.as_ref().unwrap().send_to(line, *target) {
                        Ok(n) if n == line.len() => (),
                        Ok(_) => return Err(io::ErrorKind::WriteZero.into()),
                        Err(e) => {
                            *socket = None;
                            return Err(e);
                        }
                    }
                }
                Ok(())
            }
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
struct State {
    info: Info,
    queue: VecDeque<Batch>,
    stop: bool,
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
    count: usize,
    bytes: usize,
}
impl Worker {
    fn spawn(mut destination: Destination, config: &Config, name: &str) -> Result<Self, Error> {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                info: Info {
                    running: true,
                    ..Default::default()
                },
                queue: VecDeque::new(),
                stop: false,
            }),
            wake: Condvar::new(),
            done: watch::channel(false).0,
        });
        let owned = shared.clone();
        let join = thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                let _finished = Finished(owned.clone());
                loop {
                    let batch = {
                        let mut s = owned.state.lock().unwrap_or_else(|e| e.into_inner());
                        while s.queue.is_empty() && !s.stop {
                            s = owned.wake.wait(s).unwrap_or_else(|e| e.into_inner());
                        }
                        match s.queue.pop_front() {
                            Some(batch) => batch,
                            None => break,
                        }
                    };
                    let result = destination.write(&batch);
                    let mut s = owned.state.lock().unwrap_or_else(|e| e.into_inner());
                    s.info.outstanding -= 1;
                    s.info.retained_bytes -= batch.bytes();
                    match result {
                        Ok(()) => s.info.written = s.info.written.saturating_add(1),
                        Err(e) => {
                            s.info.failed = s.info.failed.saturating_add(1);
                            s.info.last_error = Some(e.kind());
                        }
                    }
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
            count: config.sink_records,
            bytes: config.sink_bytes,
        })
    }
    pub fn outputs(
        outputs: Outputs,
        config: &Config,
    ) -> Result<(Option<Self>, Option<Self>), Error> {
        let log = outputs
            .log
            .map(|writer| Self::spawn(Destination::Log(writer), config, "ae-json-log"))
            .transpose()?;
        let udp = outputs
            .statsd
            .filter(|a| a.port() != 0)
            .map(|target| {
                Self::spawn(
                    Destination::Udp {
                        target,
                        socket: None,
                    },
                    config,
                    "ae-statsd",
                )
            })
            .transpose()?;
        Ok((log, udp))
    }
    pub fn submit(&self, batch: Batch) {
        let mut s = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        let bytes = batch.bytes();
        if !s.info.running
            || s.stop
            || s.info.outstanding == self.count
            || bytes > self.bytes.saturating_sub(s.info.retained_bytes)
        {
            s.info.dropped = s.info.dropped.saturating_add(1);
            return;
        }
        s.info.accepted = s.info.accepted.saturating_add(1);
        s.info.outstanding += 1;
        s.info.retained_bytes += bytes;
        s.queue.push_back(batch);
        self.shared.wake.notify_one();
    }
    pub fn info(&self) -> Info {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .info
            .clone()
    }
    fn stop(&self, drain: bool) {
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
        }
    }
    pub fn abort(&mut self) {
        self.stop(false);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.abort();
    }
}
