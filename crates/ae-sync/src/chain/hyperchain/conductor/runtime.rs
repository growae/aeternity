use super::*;
use std::{
    sync::{Condvar, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub trait Clock: Send + 'static {
    fn now(&self) -> Result<(u64, Instant), Error>;
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Result<(u64, Instant), Error> {
        Ok((
            u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| Error::Time)?
                    .as_millis(),
            )
            .map_err(|_| Error::Time)?,
            Instant::now(),
        ))
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub poll: Duration,
    pub outputs: usize,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(10),
            outputs: 16,
        }
    }
}
struct Control {
    stop: bool,
    start: bool,
    status: Status,
    joined: bool,
}
struct Shared {
    control: Mutex<Control>,
    wake: Condvar,
}
#[derive(Clone)]
pub struct Handle(Arc<Shared>);
impl Handle {
    pub fn status(&self) -> Status {
        self.0
            .control
            .lock()
            .expect("conductor control")
            .status
            .clone()
    }
    pub fn joined(&self) -> bool {
        self.0.control.lock().expect("conductor control").joined
    }
    pub fn start_production(&self) -> Result<(), Error> {
        let mut c = self.0.control.lock().expect("conductor control");
        if c.stop || c.joined {
            return Err(Error::Closed);
        }
        if let Some(e) = &c.status.failure {
            return Err(e.clone());
        }
        if !c.status.block_producer {
            return Err(Error::NotBlockProducer);
        }
        c.start = true;
        self.0.wake.notify_all();
        Ok(())
    }
    pub fn request_shutdown(&self) {
        self.0.control.lock().expect("conductor control").stop = true;
        self.0.wake.notify_all();
    }
}
pub struct Runtime<O: MapOrder + Clone + Send + Sync + 'static> {
    handle: Handle,
    outputs: mpsc::Receiver<Output>,
    thread: Option<JoinHandle<Shutdown<O>>>,
}
pub struct SpawnError<O: MapOrder + Clone + Send + Sync + 'static> {
    pub error: String,
    pub conductor: Conductor<O>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Runtime<O> {
    pub fn spawn(
        conductor: Conductor<O>,
        clock: impl Clock,
        settings: Settings,
    ) -> Result<Self, Box<SpawnError<O>>> {
        if settings.poll.is_zero()
            || settings.poll > Duration::from_secs(1)
            || settings.outputs == 0
            || settings.outputs > 65_536
        {
            return Err(Box::new(SpawnError {
                error: "invalid conductor runtime settings".into(),
                conductor,
            }));
        }
        let handle = Handle(Arc::new(Shared {
            control: Mutex::new(Control {
                stop: false,
                start: false,
                status: conductor.status(),
                joined: false,
            }),
            wake: Condvar::new(),
        }));
        let shared = handle.0.clone();
        let (sender, outputs) = mpsc::sync_channel(settings.outputs);
        let input = Arc::new(Mutex::new(Some(conductor)));
        let moved = input.clone();
        let thread = match thread::Builder::new()
            .name("ae-hc-conductor".into())
            .spawn(move || {
                let mut conductor = moved
                    .lock()
                    .expect("conductor input")
                    .take()
                    .expect("conductor");
                loop {
                    let start = {
                        let mut c = shared.control.lock().expect("conductor control");
                        if c.stop {
                            break;
                        }
                        std::mem::take(&mut c.start)
                    };
                    if start && let Err(e) = conductor.start_production() {
                        conductor.status.failure = Some(e);
                    }
                    if conductor.status.failure.is_none() {
                        match clock.now() {
                            Ok((wall, now)) => {
                                let _ = conductor.step(wall, now);
                            }
                            Err(e) => conductor.status.failure = Some(e),
                        }
                    }
                    if let Some(output) = conductor.take_output() {
                        match sender.try_send(output) {
                            Ok(()) => (),
                            Err(mpsc::TrySendError::Full(o)) => conductor.pending = Some(o),
                            Err(mpsc::TrySendError::Disconnected(o)) => {
                                conductor.pending = Some(o);
                                break;
                            }
                        }
                    }
                    let mut c = shared.control.lock().expect("conductor control");
                    c.status = conductor.status();
                    if c.stop {
                        break;
                    }
                    c = shared
                        .wake
                        .wait_timeout(c, settings.poll)
                        .expect("conductor wait")
                        .0;
                    if c.stop {
                        break;
                    }
                }
                let shutdown = conductor.shutdown();
                let mut c = shared.control.lock().expect("conductor control");
                c.status = shutdown.status.clone();
                c.joined = true;
                c.stop = true;
                shutdown
            }) {
            Ok(t) => t,
            Err(e) => {
                return Err(Box::new(SpawnError {
                    error: e.to_string(),
                    conductor: input
                        .lock()
                        .expect("conductor input")
                        .take()
                        .expect("unspawned"),
                }));
            }
        };
        Ok(Self {
            handle,
            outputs,
            thread: Some(thread),
        })
    }
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    pub fn try_output(&self) -> Option<Output> {
        self.outputs.try_recv().ok()
    }
    pub fn shutdown(mut self) -> thread::Result<Shutdown<O>> {
        self.handle.request_shutdown();
        self.thread
            .take()
            .expect("conductor thread")
            .join()
            .map(|mut report| {
                report.queued_outputs.extend(self.outputs.try_iter());
                report
            })
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Drop for Runtime<O> {
    fn drop(&mut self) {
        self.handle.request_shutdown();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
