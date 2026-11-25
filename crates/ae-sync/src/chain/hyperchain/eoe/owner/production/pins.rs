use super::*;
use std::thread::{self, JoinHandle};

// Exactly one key preparation per production lease. Cancellation interrupts
// parent reads; the owner retains and joins the task before releasing its lease.
pub(super) struct Pending<O> {
    worker: Option<JoinHandle<Result<owned::Prepared<O>, chain::Error>>>,
    cancel: Arc<AtomicBool>,
}
impl<O> Pending<O> {
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::Release);
    }
    pub fn poll(&mut self) -> Option<Result<owned::Prepared<O>, chain::Error>> {
        self.worker
            .as_ref()
            .is_some_and(|h| h.is_finished())
            .then(|| self.join())
    }
    pub fn join(&mut self) -> Result<owned::Prepared<O>, chain::Error> {
        self.worker
            .take()
            .expect("candidate worker")
            .join()
            .unwrap_or_else(|_| {
                Err(chain::Error::Validation {
                    operational: true,
                    reason: "Hyperchain key preparation worker panicked".into(),
                })
            })
    }
}
impl<O> Drop for Pending<O> {
    fn drop(&mut self) {
        self.cancel();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
pub(super) fn start<O: MapOrder + Clone + Send + Sync + 'static>(
    source: scoped::Producer<O>,
    snapshots: ae_storage::snapshots::Owner,
    request: owned::Request,
    pins: Option<crate::parent_pins::Client>,
    parent: ae_hyperchain::parent_cache::runtime::Client,
    client: super::super::Client,
) -> Result<Pending<O>, chain::Error> {
    let cancel = Arc::new(AtomicBool::new(false));
    let stopped = cancel.clone();
    let worker = thread::Builder::new()
        .name("ae-hc-key-inputs".into())
        .spawn(move || {
            source.candidate_with_parent(&snapshots, request, &parent, pins.as_ref(), || {
                if stopped.load(Ordering::Acquire) {
                    return Err(chain::Error::Cancelled);
                }
                if parent.is_closed() || client.is_closed() {
                    return Err(chain::Error::Closed);
                }
                Ok(())
            })
        })
        .map_err(|_| chain::Error::Validation {
            operational: true,
            reason: "cannot spawn Hyperchain key preparation worker".into(),
        })?;
    Ok(Pending {
        worker: Some(worker),
        cancel,
    })
}
