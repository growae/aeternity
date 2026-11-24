// One owned simulation per builder, including a completed but unjoined task.
// Tasks retain the exact request and release snapshot leases before completion.
use super::Checked as Evaluation;
mod failure;
use ae_storage::{Database, snapshots};
use ae_stratum::payout;
use ae_sync::chain::pow::stratum::execution::{self, Source};
pub use failure::{EvaluationFailure, ExecutionFailure};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Id(u64);
#[derive(Debug)]
pub enum Error {
    Busy,
    Sequence,
    Spawn,
    Cancelled,
    Panicked,
    Source(execution::Error),
    Context,
    Evaluation(Box<EvaluationFailure>),
}
#[derive(Debug, Clone)]
pub struct Checked {
    evaluation: Evaluation,
    metadata: snapshots::Metadata,
    owner: Arc<()>,
}
impl Checked {
    pub fn evaluation(&self) -> &Evaluation {
        &self.evaluation
    }
    pub fn metadata(&self) -> &snapshots::Metadata {
        &self.metadata
    }
}
#[derive(Debug)]
pub struct Completion {
    pub id: Id,
    pub prepared: Arc<payout::Prepared>,
    pub result: Result<Checked, Error>,
}
#[derive(Clone)]
pub struct Builder {
    source: Source,
    limits: payout::Limits,
    busy: Arc<AtomicBool>,
    owner: Arc<()>,
}
impl Builder {
    pub fn new(source: Source, limits: payout::Limits) -> Self {
        Self {
            source,
            limits,
            busy: Arc::new(AtomicBool::new(false)),
            owner: Arc::new(()),
        }
    }
    pub fn uses_database(&self, database: &Arc<Database>) -> bool {
        self.source.uses_database(database)
    }
    pub fn busy(&self) -> bool {
        self.busy.load(Ordering::Acquire)
    }
    pub fn is_current(&self, checked: &Checked) -> Result<bool, Error> {
        if !Arc::ptr_eq(&self.owner, &checked.owner) {
            return Ok(false);
        }
        self.source
            .is_current(&checked.metadata)
            .map_err(Error::Source)
    }
    pub fn start(&self, prepared: Arc<payout::Prepared>) -> Result<Task, Error> {
        self.busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Busy)?;
        let permit = Permit(self.busy.clone());
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = Id(NEXT
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| Error::Sequence)?);
        let builder = self.clone();
        let request = prepared.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let signal = cancel.clone();
        let worker = thread::Builder::new()
            .name("ae-payout-preflight".into())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    check(&signal)?;
                    let frame = builder.source.capture().map_err(Error::Source)?;
                    check(&signal)?;
                    let view = frame.view();
                    let evaluation = super::evaluate(
                        &request,
                        &view.trees,
                        &view.context,
                        view.aevm.as_ref().ok_or(Error::Context)?,
                        super::Limits {
                            payout: builder.limits,
                            overlays: builder.source.limits().overlay,
                            vm: frame.vm_limits(),
                        },
                    )
                    .map_err(|e| Error::Evaluation(Box::new(e.into())))?;
                    check(&signal)?;
                    Ok(Checked {
                        evaluation,
                        metadata: frame.metadata().clone(),
                        owner: builder.owner,
                    })
                }))
                .unwrap_or(Err(Error::Panicked));
                Completion {
                    id,
                    prepared: request,
                    result,
                }
            })
            .map_err(|_| Error::Spawn)?;
        Ok(Task {
            id,
            prepared,
            cancel,
            worker: Some(worker),
            permit: Some(permit),
        })
    }
}
fn check(signal: &AtomicBool) -> Result<(), Error> {
    if signal.load(Ordering::Acquire) {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}
struct Permit(Arc<AtomicBool>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
pub struct Task {
    id: Id,
    prepared: Arc<payout::Prepared>,
    cancel: Arc<AtomicBool>,
    worker: Option<JoinHandle<Completion>>,
    permit: Option<Permit>,
}
impl Task {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn prepared(&self) -> &Arc<payout::Prepared> {
        &self.prepared
    }
    // Cancellation fences the next boundary; it does not interrupt a VM step.
    // An already completed result retains its original outcome until joined.
    pub fn request_cancel(&self) {
        self.cancel.store(true, Ordering::Release);
    }
    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub fn try_result(&mut self) -> Option<Completion> {
        self.is_finished().then(|| self.join()).flatten()
    }
    pub fn wait(mut self) -> Option<Completion> {
        self.join()
    }
    fn join(&mut self) -> Option<Completion> {
        let worker = self.worker.take()?;
        let result = worker.join().unwrap_or_else(|_| Completion {
            id: self.id,
            prepared: self.prepared.clone(),
            result: Err(Error::Panicked),
        });
        self.permit = None;
        Some(result)
    }
}
impl Drop for Task {
    fn drop(&mut self) {
        self.request_cancel();
        let _ = self.join();
    }
}
