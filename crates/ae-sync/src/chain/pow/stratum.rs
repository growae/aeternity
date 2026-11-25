// Canonical work preparation and ordinary Created admission for Stratum.
// This boundary does not drain durable pool receipts or mutate accounting.
use super::preparation;
use crate::chain::{self, Importer};
use ae_storage::snapshots;
use ae_stratum::candidate::{Candidate, Sealed};
use ae_types::Hash256;
use ae_vm_fate::maps::MapOrder;
use std::sync::Arc;
pub mod execution;

#[derive(Debug)]
pub enum Error {
    Preparation(preparation::Error),
    Binding,
    Candidate(ae_stratum::candidate::Error),
}
#[derive(Debug)]
pub struct Prepared {
    pub id: preparation::Id,
    pub request: preparation::Request,
    pub calculation_time: u64,
    pub final_time: u64,
    pub source: snapshots::Metadata,
    candidate: Arc<Candidate>,
}
impl Prepared {
    pub fn from_completion(completion: preparation::Completion) -> Result<Self, Error> {
        let preparation::Completion {
            id,
            request,
            calculation_time,
            final_time,
            result,
        } = completion;
        let prepared = result.map_err(Error::Preparation)?;
        let calculation_time = calculation_time.ok_or(Error::Binding)?;
        let final_time = final_time.ok_or(Error::Binding)?;
        if prepared.state.value.view.identity().algorithm != ae_protocol::ConsensusAlgorithm::Cuckoo
            || prepared.header.common.previous_hash != request.parent.0
            || prepared.header.miner != request.miner.0
            || prepared.header.beneficiary != request.beneficiary.0
            || prepared.state.value.candidate.temporary().time != calculation_time
            || prepared.state.value.candidate.temporary().info
                != (prepared.header.common.version >= 2)
                    .then_some(request.release_info.to_be_bytes())
            || prepared.state.metadata.status != prepared.state.value.status
            || prepared.state.metadata.source_id != prepared.state.value.source_id
            || prepared
                .state
                .value
                .finish(final_time)
                .map_err(|_| Error::Binding)?
                != prepared.header
        {
            return Err(Error::Binding);
        }
        let candidate = Arc::new(Candidate::new(prepared.header).map_err(Error::Candidate)?);
        // A work record needs frozen bytes, not a live trie/snapshot lease.
        Ok(Self {
            id,
            request,
            calculation_time,
            final_time,
            source: prepared.state.metadata,
            candidate,
        })
    }
    pub fn candidate(&self) -> &Arc<Candidate> {
        &self.candidate
    }
}

// The exact submission and final outcome remain together. Empty polling is
// neither a timeout nor permission to repeat an accepted submission. The
// caller retains this handle until its outcome has been durably reconciled.
pub struct Admission {
    solution: Sealed,
    ticket: chain::Ticket,
    result: Option<Result<chain::Report, chain::Failure>>,
}
impl Admission {
    pub fn solution(&self) -> &Sealed {
        &self.solution
    }
    pub fn result(&self) -> Option<&Result<chain::Report, chain::Failure>> {
        self.result.as_ref()
    }
    pub fn poll(&mut self) -> Option<&Result<chain::Report, chain::Failure>> {
        if self.result.is_none() {
            self.result = self.ticket.try_result();
        }
        self.result()
    }
    pub fn cancel(&self) {
        self.ticket.cancel();
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Importer<O> {
    pub fn stratum_builder(
        &self,
        snapshots: snapshots::Owner,
        limits: ae_chain::repository::pow::key::Limits,
    ) -> Result<preparation::Builder<O>, super::producer::mining::Error> {
        if !snapshots.uses_database(self.database()) {
            return Err(super::producer::mining::Error::Binding);
        }
        self.pow_preparation(snapshots, limits)
    }
    pub fn import_stratum(
        &self,
        solution: &Sealed,
        now_ms: u64,
        strictly_follow_top: bool,
    ) -> Result<Admission, chain::Error> {
        if self.algorithm() != ae_protocol::ConsensusAlgorithm::Cuckoo {
            return Err(chain::Error::WrongConsensus);
        }
        let ticket = if strictly_follow_top {
            self.import_created_block_at(solution.bytes().to_vec(), now_ms, solution.parent())?
        } else {
            self.import_created_block(solution.bytes().to_vec(), now_ms)?
        };
        Ok(Admission {
            solution: solution.clone(),
            ticket,
            result: None,
        })
    }
}

impl Prepared {
    pub fn parent(&self) -> Hash256 {
        self.candidate.parent()
    }
}

// A narrow borrowed handle for the accounting actor. The root importer owns
// shutdown; this handle keeps the same database, policy and execution profile.
type Submit = dyn Fn(&Sealed, u64, bool) -> Result<Admission, chain::Error> + Send + Sync;
type Build = dyn Fn(
        snapshots::Owner,
        ae_chain::repository::pow::key::Limits,
    ) -> Result<Preparer, super::producer::mining::Error>
    + Send
    + Sync;
type Start = dyn Fn(
        preparation::Request,
        Arc<dyn super::generator::Clock>,
    ) -> Result<preparation::Task, preparation::Error>
    + Send
    + Sync;
#[derive(Clone)]
pub struct Preparer {
    start: Arc<Start>,
}
impl Preparer {
    pub fn start(
        &self,
        request: preparation::Request,
        clock: Arc<dyn super::generator::Clock>,
    ) -> Result<preparation::Task, preparation::Error> {
        (self.start)(request, clock)
    }
}
#[derive(Clone)]
pub struct Admitter {
    database: Arc<ae_storage::Database>,
    submit: Arc<Submit>,
    build: Arc<Build>,
    execution: Arc<execution::Build>,
}
impl Admitter {
    pub fn execution(
        &self,
        snapshots: snapshots::Owner,
        limits: ae_chain::repository::pow::key::Limits,
    ) -> Result<execution::Source, execution::Error> {
        (self.execution)(snapshots, limits)
    }
    pub fn preparer(
        &self,
        snapshots: snapshots::Owner,
        limits: ae_chain::repository::pow::key::Limits,
    ) -> Result<Preparer, super::producer::mining::Error> {
        (self.build)(snapshots, limits)
    }
    pub fn uses_database(&self, database: &Arc<ae_storage::Database>) -> bool {
        Arc::ptr_eq(&self.database, database)
    }
    pub fn submit(
        &self,
        solution: &Sealed,
        now_ms: u64,
        strictly_follow_top: bool,
    ) -> Result<Admission, chain::Error> {
        (self.submit)(solution, now_ms, strictly_follow_top)
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Importer<O> {
    pub fn stratum_admitter(&self) -> Admitter {
        let importer = self.borrowed();
        let preparation = self.borrowed();
        let execution = self.borrowed();
        Admitter {
            database: self.database().clone(),
            execution: Arc::new(move |snapshots, limits| {
                execution::Source::new(&execution, snapshots, limits)
            }),
            build: Arc::new(move |snapshots, limits| {
                let builder = preparation.stratum_builder(snapshots, limits)?;
                Ok(Preparer {
                    start: Arc::new(move |request, clock| builder.start(request, clock)),
                })
            }),
            submit: Arc::new(move |solution, now, strict| {
                importer.import_stratum(solution, now, strict)
            }),
        }
    }
}
