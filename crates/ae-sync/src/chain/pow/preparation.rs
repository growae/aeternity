use super::generator::Clock;
use ae_chain::{
    key::{Rules, candidate},
    repository::{Identity, pow::key},
};
use ae_codec::headers::KeyHeader;
use ae_execution::transactions::owned::Owned;
use ae_protocol::ConsensusType;
use ae_storage::{pow::key as storage, snapshots};
use ae_types::{AccountPublicKey, Hash256};
use ae_vm_fate::maps::MapOrder;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
};

#[derive(Clone)]
pub struct Config<O> {
    pub identity: Identity,
    pub execution: Arc<Owned<O>>,
    pub rules: Arc<Rules>,
    pub limits: key::Limits,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request {
    pub parent: Hash256,
    pub miner: AccountPublicKey,
    pub beneficiary: AccountPublicKey,
    pub release_info: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Id(u64);
#[derive(Debug)]
pub enum Error {
    Configuration,
    Busy,
    Sequence,
    Spawn,
    Time,
    Cancelled,
    Panicked,
    Preparation(storage::Error),
    Finish(key::Error<snapshots::Reader>),
}
pub struct Prepared {
    pub header: KeyHeader,
    pub state: storage::Prepared,
}
pub struct Completion {
    pub id: Id,
    pub request: Request,
    pub calculation_time: Option<u64>,
    pub final_time: Option<u64>,
    pub result: Result<Prepared, Error>,
}

// One preparation may be outstanding per builder, including a finished task
// whose receipt has not been joined. Clones share the same ownership bound.
// Completed candidates retain their separately bounded immutable snapshots.
#[derive(Clone)]
pub struct Builder<O> {
    snapshots: snapshots::Owner,
    config: Config<O>,
    busy: Arc<AtomicBool>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Builder<O> {
    pub fn new(snapshots: snapshots::Owner, config: Config<O>) -> Result<Self, Error> {
        if config.identity.consensus != ConsensusType::Pow
            || config.execution.consensus() != ConsensusType::Pow
            || !config.identity.matches(config.execution.network())
            || config.identity.genesis != config.rules.genesis_hash
            || config.limits.history == 0
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            snapshots,
            config,
            busy: Arc::new(AtomicBool::new(false)),
        })
    }
    pub fn busy(&self) -> bool {
        self.busy.load(Ordering::Acquire)
    }
    pub fn edge_bits(&self) -> ae_consensus::cuckoo::EdgeBits {
        self.config.rules.admission.edge_bits
    }
    pub fn start(&self, request: Request, clock: Arc<dyn Clock>) -> Result<Task, Error> {
        self.busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Busy)?;
        let permit = Permit(self.busy.clone());
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = Id(NEXT
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| Error::Sequence)?);
        let snapshots = self.snapshots.clone();
        let config = self.config.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let signal = cancel.clone();
        let worker = thread::Builder::new()
            .name("ae-pow-key-preparation".into())
            .spawn(move || {
                let mut calculation_time = None;
                let mut final_time = None;
                let result = catch_unwind(AssertUnwindSafe(|| {
                    check(&signal)?;
                    let calculation =
                        u64::try_from(clock.unix_millis()).map_err(|_| Error::Time)?;
                    calculation_time = Some(calculation);
                    check(&signal)?;
                    // Owned preserves both historical VMs and every execution
                    // limit. The key adapter binds temporary-key history and
                    // environment from this one captured view; no parent query
                    // environment is substituted for the candidate environment.
                    let state = config
                        .execution
                        .with_config(|execution| {
                            storage::prepare(
                                &snapshots,
                                &config.identity,
                                execution,
                                &config.rules,
                                key::Input {
                                    parent: request.parent,
                                    production: candidate::Input {
                                        miner: request.miner,
                                        beneficiary: request.beneficiary,
                                        calculation_time: calculation,
                                        release_info: request.release_info,
                                    },
                                },
                                config.limits,
                            )
                        })
                        .map_err(Error::Preparation)?;
                    check(&signal)?;
                    let final_read = u64::try_from(clock.unix_millis()).map_err(|_| Error::Time)?;
                    final_time = Some(final_read);
                    check(&signal)?;
                    // Native preparation reads the wall clock twice. A valid
                    // backwards wall-clock adjustment is not silently clamped.
                    let header = state.value.finish(final_read).map_err(Error::Finish)?;
                    check(&signal)?;
                    Ok(Prepared { header, state })
                }))
                .unwrap_or(Err(Error::Panicked));
                Completion {
                    id,
                    request,
                    calculation_time,
                    final_time,
                    result,
                }
            })
            .map_err(|_| Error::Spawn)?;
        Ok(Task {
            id,
            request,
            cancel,
            worker: Some(worker),
            permit: Some(permit),
        })
    }
}
fn check(cancel: &AtomicBool) -> Result<(), Error> {
    if cancel.load(Ordering::Acquire) {
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
    request: Request,
    cancel: Arc<AtomicBool>,
    worker: Option<JoinHandle<Completion>>,
    permit: Option<Permit>,
}
impl Task {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn request(&self) -> Request {
        self.request
    }
    // Cancellation cannot interrupt a deterministic state transition midway.
    // It fences publication at the next boundary; joining still owns cleanup.
    // A result completed before this request keeps its original outcome.
    pub fn request_cancel(&self) {
        self.cancel.store(true, Ordering::Release);
    }
    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub fn try_result(&mut self) -> Option<Completion> {
        if !self.is_finished() {
            return None;
        }
        self.join()
    }
    pub fn wait(mut self) -> Option<Completion> {
        self.join()
    }
    fn join(&mut self) -> Option<Completion> {
        let worker = self.worker.take()?;
        let result = worker.join().unwrap_or(Completion {
            id: self.id,
            request: self.request,
            calculation_time: None,
            final_time: None,
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
