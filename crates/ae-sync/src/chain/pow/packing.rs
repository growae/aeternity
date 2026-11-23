// PoW uses the full block gas allowance and no staged HC consensus effects.
use crate::chain::packing as shared;
use ae_chain::repository::pow as build;
use ae_storage::{pool::worker, pow as owned, snapshots};
use ae_vm_fate::maps::MapOrder;
pub use shared::{CancelReport, Cancelled, Error, Progress, Stage};

#[derive(Debug, Clone)]
pub struct Config {
    pub timeout: ae_chain::hyperchain::candidate::Timeout,
    pub window_ms: i64,
}
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
pub struct Packing<O>(shared::Packing<O>);
impl<O: MapOrder + Clone> Packing<O> {
    pub fn start(
        input: owned::Prepared<O>,
        pool: worker::Client,
        config: Config,
    ) -> Result<Self, Box<StartError<O>>> {
        let p = &input.value;
        if p.reader.selected.repository().identity().consensus != ae_protocol::ConsensusType::Pow {
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
        let config = shared::Config {
            reserved_gas: 0.into(),
            timeout: config.timeout,
            window_ms: config.window_ms,
        };
        shared::Packing::start(prepared, pool, config)
            .map(Self)
            .map_err(|e| {
                Box::new(StartError {
                    input,
                    error: e.error,
                })
            })
    }
    pub fn metadata(&self) -> &snapshots::Metadata {
        self.0.metadata()
    }
    pub fn failure(&self) -> Option<&Error> {
        self.0.failure()
    }
    pub fn in_flight(&self) -> bool {
        self.0.in_flight()
    }
    pub fn expired(&self) -> bool {
        self.0.expired()
    }
    pub fn ready(&self) -> bool {
        self.0.ready()
    }
    pub fn retry(&mut self) -> Result<(), Error> {
        self.0.retry()
    }
    pub fn step(&mut self) -> Result<Progress, Error> {
        self.0.step()
    }
    pub fn cancel(self) -> Cancelled {
        self.0.cancel()
    }
    pub fn finish(self) -> Result<owned::Prepared<O>, Box<FinishError<O>>> {
        match self.0.finish() {
            Ok(p) => Ok(owned::Prepared {
                value: build::Prepared {
                    candidate: p.candidate,
                    reader: p.reader,
                    status: p.status,
                    source_id: p.source_id,
                },
                metadata: p.metadata,
            }),
            Err(e) => Err(Box::new(FinishError {
                pending: Self(e.pending),
                error: e.error,
            })),
        }
    }
}
