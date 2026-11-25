use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

pub(super) enum Work<O> {
    Create(Hash256),
    Update(Arc<Candidate<O>>, Vec<Arc<Signed>>),
}
pub(super) enum Outcome<O> {
    Candidate(Box<owned::Prepared<O>>),
    Unchanged,
    Failed(Error),
    Cancelled,
}
pub(super) struct Result<O> {
    pub outcome: Outcome<O>,
    pub retirement: Option<packing::Cancelled>,
}
pub(super) struct Active<O> {
    cancel: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<O>>>,
    pub obsolete: bool,
}
impl<O> Active<O> {
    pub fn cancel(&mut self) {
        self.obsolete = true;
        self.cancel.store(true, Ordering::Release);
    }
    pub fn finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub fn join(mut self) -> Result<O> {
        self.worker
            .take()
            .expect("owned task")
            .join()
            .unwrap_or(Result {
                outcome: Outcome::Failed(Error::Panicked),
                retirement: None,
            })
    }
}
impl<O> Drop for Active<O> {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
pub(super) fn spawn<O: MapOrder + Clone + Send + Sync + 'static>(
    work: Work<O>,
    snapshots: snapshots::Owner,
    pool: pool::Client,
    config: Config<O>,
    clock: Arc<dyn Clock>,
    execution: Option<Arc<Owned<O>>>,
) -> std::result::Result<Active<O>, Error> {
    let cancel = Arc::new(AtomicBool::new(false));
    let signal = cancel.clone();
    let worker = thread::Builder::new()
        .name("ae-pow-candidate".into())
        .spawn(move || {
            // Keep a packing attempt outside the unwind boundary: even a failed
            // execution must hand accepted pool requests to the retiring owner.
            let mut packing = None;
            let result = catch_unwind(AssertUnwindSafe(
                || -> std::result::Result<Outcome<O>, Error> {
                    if signal.load(Ordering::Acquire) {
                        return Ok(Outcome::Cancelled);
                    }
                    match work {
                        Work::Create(parent) => {
                            let now = clock.unix_millis();
                            if signal.load(Ordering::Acquire) {
                                return Ok(Outcome::Cancelled);
                            }
                            let mut input = owned::prepare_micro(
                                &snapshots,
                                &config.preparation,
                                pow::Input {
                                    parent,
                                    now,
                                    limits: config.candidate,
                                },
                            )
                            .map_err(|e| Error::Preparation(bounded(&e)))?;
                            if let Some(execution) = &execution {
                                input.value.reader =
                                    input.value.reader.with_owned_execution(execution.clone());
                            }
                            if signal.load(Ordering::Acquire) {
                                return Ok(Outcome::Cancelled);
                            }
                            packing = Some(
                                packing::Packing::start(input, pool, config.packing)
                                    .map_err(|e| Error::Packing(e.error))?,
                            );
                            loop {
                                if signal.load(Ordering::Acquire) {
                                    return Ok(Outcome::Cancelled);
                                }
                                let p = packing.as_mut().expect("active packing");
                                if p.ready() {
                                    break;
                                }
                                if p.step().map_err(Error::Packing)? == packing::Progress::Waiting {
                                    thread::sleep(Duration::from_millis(1));
                                }
                            }
                            match packing.take().expect("finished packing").finish() {
                                Ok(p) => Ok(Outcome::Candidate(Box::new(p))),
                                Err(e) => {
                                    packing = Some(e.pending);
                                    Err(Error::Packing(e.error))
                                }
                            }
                        }
                        Work::Update(base, txs) => {
                            let base = base.prepared();
                            let mut candidate = base.value.candidate.clone();
                            let transactions: Vec<_> =
                                txs.iter().map(|t| t.as_ref().clone()).collect();
                            match candidate.update::<(), _, _>(&transactions, &base.value.reader) {
                                Ok(_) => Ok(Outcome::Candidate(Box::new(owned::Prepared {
                                    value: pow::Prepared {
                                        candidate,
                                        reader: base.value.reader.clone(),
                                        status: base.value.status,
                                        source_id: base.value.source_id,
                                    },
                                    metadata: base.metadata.clone(),
                                }))),
                                Err(core::Error::NoUpdate | core::Error::Full) => {
                                    Ok(Outcome::Unchanged)
                                }
                                Err(e) => Err(Error::Update(bounded(&e))),
                            }
                            // Incremental native updates do not report rejected txs
                            // to the pool; initial candidate packing does that itself.
                        }
                    }
                },
            ));
            let outcome = match result {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(error)) => Outcome::Failed(error),
                Err(_) => Outcome::Failed(Error::Panicked),
            };
            Result {
                outcome,
                retirement: packing.map(packing::Packing::cancel),
            }
        })
        .map_err(|e| Error::Worker(e.to_string()))?;
    Ok(Active {
        cancel,
        worker: Some(worker),
        obsolete: false,
    })
}
fn bounded(value: &impl std::fmt::Debug) -> String {
    let mut text = format!("{value:?}");
    if text.len() > 4096 {
        let end = (0..=4096)
            .rev()
            .find(|n| text.is_char_boundary(*n))
            .unwrap();
        text.truncate(end);
    }
    text
}
