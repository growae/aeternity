use super::*;
use std::{
    io,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub running: bool,
    pub acknowledged: u64,
    pub pending: usize,
    pub retained_bytes: usize,
    pub in_flight: bool,
    pub failure: Option<Error>,
}
struct State {
    status: Status,
    stop: bool,
}
struct Control {
    state: Mutex<State>,
    changed: Condvar,
}
pub struct SpawnError<O, A, C> {
    pub error: io::Error,
    pub delivery: Delivery<O, A, C>,
}
impl<O, A, C> std::fmt::Debug for SpawnError<O, A, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpawnError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
pub struct Service<O, A, C> {
    control: Arc<Control>,
    worker: Option<JoinHandle<Delivery<O, A, C>>>,
}
impl<
    O: MapOrder + Clone + Send + Sync + 'static,
    A: Send + Sync + 'static,
    C: Send + Sync + 'static,
> Service<O, A, C>
{
    pub fn spawn(
        delivery: Delivery<O, A, C>,
        poll_interval: Duration,
    ) -> Result<Self, Box<SpawnError<O, A, C>>> {
        if poll_interval.is_zero()
            || std::time::Instant::now()
                .checked_add(poll_interval)
                .is_none()
        {
            return Err(Box::new(SpawnError {
                error: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid parent delivery poll interval",
                ),
                delivery,
            }));
        }
        let control = Arc::new(Control {
            state: Mutex::new(State {
                status: Status {
                    running: true,
                    acknowledged: delivery.acknowledged(),
                    pending: delivery.pending().len(),
                    retained_bytes: delivery.retained_bytes(),
                    in_flight: delivery.in_flight(),
                    failure: delivery.failure().cloned(),
                },
                stop: false,
            }),
            changed: Condvar::new(),
        });
        let worker_control = control.clone();
        let pending = Arc::new(Mutex::new(Some(delivery)));
        let work = pending.clone();
        let worker = thread::Builder::new()
            .name("ae-hc-parent-delivery".into())
            .spawn(move || {
                let mut delivery = work
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                    .expect("owned delivery");
                drop(work);
                struct End(Arc<Control>);
                impl Drop for End {
                    fn drop(&mut self) {
                        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
                        state.status.running = false;
                        if thread::panicking() {
                            state.status.failure = Some(Error::WorkerPanicked);
                        }
                        self.0.changed.notify_all();
                    }
                }
                let _end = End(worker_control.clone());
                loop {
                    if worker_control
                        .state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .stop
                    {
                        break;
                    }
                    let result = delivery.step();
                    let mut state = worker_control
                        .state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    state.status.acknowledged = delivery.acknowledged();
                    state.status.pending = delivery.pending().len();
                    state.status.retained_bytes = delivery.retained_bytes();
                    state.status.in_flight = delivery.in_flight();
                    if let Err(error) = result {
                        state.status.failure = Some(error);
                        break;
                    }
                    if matches!(result, Ok(Progress::Acknowledged | Progress::Submitted)) {
                        continue;
                    }
                    drop(
                        worker_control
                            .changed
                            .wait_timeout_while(state, poll_interval, |s| !s.stop)
                            .unwrap_or_else(|e| e.into_inner()),
                    );
                }
                delivery
            });
        let worker = match worker {
            Ok(worker) => worker,
            Err(error) => {
                return Err(Box::new(SpawnError {
                    error,
                    delivery: pending
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .take()
                        .expect("unstarted delivery"),
                }));
            }
        };
        Ok(Self {
            control,
            worker: Some(worker),
        })
    }
}
impl<O, A, C> Service<O, A, C> {
    pub fn status(&self) -> Status {
        self.control
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .status
            .clone()
    }
    pub fn request_shutdown(&self) {
        self.control
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stop = true;
        self.control.changed.notify_all();
    }
    // Return the exact owned outbox, including any unresolved receipt. The
    // caller may continue observing that receipt or reconcile an unknown
    // outcome; shutdown never cancels a command already accepted downstream.
    pub fn shutdown(mut self) -> thread::Result<Delivery<O, A, C>> {
        self.request_shutdown();
        self.worker.take().expect("live delivery worker").join()
    }
}
impl<O, A, C> Drop for Service<O, A, C> {
    fn drop(&mut self) {
        self.request_shutdown();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
