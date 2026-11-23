use super::{Epochs, Error, Progress};
use ae_storage::events;
use std::{
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct Status {
    pub running: bool,
    pub cursor: events::Cursor,
    pub acknowledged: u64,
    pub retained_bytes: usize,
    pub in_flight: bool,
    pub failure: Option<Error>,
}
struct Control {
    state: Mutex<(bool, Status)>,
    changed: Condvar,
}
pub struct SpawnError {
    pub error: std::io::Error,
    pub epochs: Epochs,
}
impl std::fmt::Debug for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpawnError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
pub struct Service {
    control: Arc<Control>,
    worker: Option<JoinHandle<Epochs>>,
}
fn status(epochs: &Epochs, running: bool) -> Status {
    Status {
        running,
        cursor: epochs.cursor().clone(),
        acknowledged: epochs.acknowledged(),
        retained_bytes: epochs.retained_bytes(),
        in_flight: epochs.in_flight(),
        failure: epochs.failure().cloned(),
    }
}
impl Service {
    pub fn spawn(epochs: Epochs, poll: Duration) -> Result<Self, Box<SpawnError>> {
        if poll.is_zero() || poll > Duration::from_secs(1) {
            return Err(Box::new(SpawnError {
                error: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "epoch poll must be positive and at most one second",
                ),
                epochs,
            }));
        }
        let control = Arc::new(Control {
            state: Mutex::new((false, status(&epochs, true))),
            changed: Condvar::new(),
        });
        let pending = Arc::new(Mutex::new(Some(epochs)));
        let work = pending.clone();
        let shared = control.clone();
        let worker = thread::Builder::new()
            .name("ae-hc-epochs".into())
            .spawn(move || {
                let mut epochs = work
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                    .expect("owned epoch feed");
                drop(work);
                struct End(Arc<Control>);
                impl Drop for End {
                    fn drop(&mut self) {
                        self.0
                            .state
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .1
                            .running = false;
                        self.0.changed.notify_all();
                    }
                }
                let _end = End(shared.clone());
                loop {
                    if shared.state.lock().unwrap_or_else(|e| e.into_inner()).0 {
                        break;
                    }
                    let result = epochs.step();
                    let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
                    state.1 = status(&epochs, result.is_ok() && !state.0);
                    if result.is_err() || state.0 {
                        break;
                    }
                    if matches!(
                        result,
                        Ok(Progress::Idle | Progress::Waiting | Progress::Backpressured)
                    ) {
                        drop(
                            shared
                                .changed
                                .wait_timeout(state, poll)
                                .unwrap_or_else(|e| e.into_inner()),
                        );
                    }
                }
                epochs
            });
        match worker {
            Ok(worker) => Ok(Self {
                control,
                worker: Some(worker),
            }),
            Err(error) => Err(Box::new(SpawnError {
                error,
                epochs: pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                    .expect("unstarted epoch feed"),
            })),
        }
    }
    pub fn status(&self) -> Status {
        self.control
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .1
            .clone()
    }
    fn stop(&self) {
        self.control
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .0 = true;
        self.control.changed.notify_all();
    }
    // Joined handoff retains the exact pending event/receipt for retry or
    // reconciliation. Dropping Epochs closes its runtime or independent Hub.
    pub fn shutdown(mut self) -> thread::Result<Epochs> {
        self.stop();
        self.worker.take().expect("live epoch worker").join()
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.stop();
        if let Some(worker) = self.worker.take() {
            drop(worker.join());
        }
    }
}
