// HC preparation retains its staged consensus effects outside shared packing.
use crate::chain::packing as shared;
use ae_chain::repository::hyperchain::{candidate as build, inputs};
use ae_storage::{hyperchain::candidate as owned, pool::worker, snapshots};
use ae_vm_fate::maps::MapOrder;
pub use shared::{Config, Error, Progress, Stage};
pub(crate) use shared::{execution, retryable};

pub struct StartError<O> {
    pub input: owned::Prepared<O>,
    pub error: Error,
}
impl<O> std::fmt::Debug for StartError<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
pub struct FinishError<O> {
    pub pending: Packing<O>,
    pub error: Error,
}
impl<O> std::fmt::Debug for FinishError<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FinishError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
pub struct Packing<O> {
    inner: shared::Packing<O>,
    effects: inputs::Effects,
}
impl<O: MapOrder + Clone> Packing<O> {
    pub fn start(
        input: owned::Prepared<O>,
        pool: worker::Client,
        config: Config,
    ) -> Result<Self, Box<StartError<O>>> {
        let Ok(owned::Candidate::Micro(p)) = &input.outcome else {
            return Err(Box::new(StartError {
                input,
                error: Error::Sequence,
            }));
        };
        if p.reader.selected.repository().identity().consensus != ae_protocol::ConsensusType::Pos {
            return Err(Box::new(StartError {
                input,
                error: Error::Binding,
            }));
        }
        let prepared = shared::Prepared {
            candidate: p.candidate.clone(),
            reader: p.reader.clone(),
            status: p.status,
            source_id: p.source_id,
            metadata: input.metadata.clone(),
        };
        match shared::Packing::start(prepared, pool, config) {
            Ok(inner) => Ok(Self {
                inner,
                effects: input.effects,
            }),
            Err(e) => Err(Box::new(StartError {
                input,
                error: e.error,
            })),
        }
    }
    pub fn metadata(&self) -> &snapshots::Metadata {
        self.inner.metadata()
    }
    pub fn effects(&self) -> &inputs::Effects {
        &self.effects
    }
    pub fn failure(&self) -> Option<&Error> {
        self.inner.failure()
    }
    pub fn in_flight(&self) -> bool {
        self.inner.in_flight()
    }
    pub fn expired(&self) -> bool {
        self.inner.expired()
    }
    pub fn ready(&self) -> bool {
        self.inner.ready()
    }
    pub fn retry(&mut self) -> Result<(), Error> {
        self.inner.retry()
    }
    pub fn step(&mut self) -> Result<Progress, Error> {
        self.inner.step()
    }
    pub(crate) fn cancel(mut self) -> Cancelled {
        Cancelled {
            inner: self.inner.cancel(),
            effects: self.effects.drain(),
        }
    }
    pub fn finish(self) -> Result<owned::Prepared<O>, Box<FinishError<O>>> {
        match self.inner.finish() {
            Ok(p) => Ok(owned::Prepared {
                outcome: Ok(owned::Candidate::Micro(build::Prepared {
                    candidate: p.candidate,
                    reader: p.reader,
                    status: p.status,
                    source_id: p.source_id,
                })),
                metadata: p.metadata,
                effects: self.effects,
            }),
            Err(e) => Err(Box::new(FinishError {
                pending: Self {
                    inner: e.pending,
                    effects: self.effects,
                },
                error: e.error,
            })),
        }
    }
}
#[derive(Debug, Default)]
pub struct CancelReport {
    pub selection: Option<Result<usize, worker::Error>>,
    pub failure_reports: Option<Result<Vec<bool>, worker::Error>>,
    pub discarded_failures: usize,
    pub discarded_effects: Vec<inputs::Effect>,
}
pub(crate) struct Cancelled {
    inner: shared::Cancelled,
    effects: Vec<inputs::Effect>,
}
impl Cancelled {
    pub fn retire_pool(&mut self, old: &worker::Client) -> Result<bool, Error> {
        self.inner.retire_pool(old)
    }
    pub fn step(&mut self) -> Result<bool, Error> {
        self.inner.step()
    }
    pub fn finish(self) -> CancelReport {
        let r = self.inner.finish();
        CancelReport {
            selection: r.selection,
            failure_reports: r.failure_reports,
            discarded_failures: r.discarded_failures,
            discarded_effects: self.effects,
        }
    }
}
