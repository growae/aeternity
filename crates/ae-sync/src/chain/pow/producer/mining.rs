use super::*;
use crate::chain::pow::{miner, preparation};
use ae_codec::headers::KeyHeader;
use ae_consensus::cuckoo::{self, solver::Nonces};
use ae_storage::snapshots;

mod state;
pub(super) use state::Mining;

#[derive(Debug, Clone)]
pub struct Instance {
    pub worker: miner::Config,
    pub external: Option<miner::external::Config>,
    pub repeats: u64,
}
impl Default for Instance {
    fn default() -> Self {
        Self {
            worker: miner::Config::default(),
            external: None,
            repeats: 1,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Config {
    pub beneficiary: AccountPublicKey,
    pub release_info: u32,
    pub instances: Vec<Instance>,
    pub preparation: repo::pow::key::Limits,
    // Operational failures back off without being reported as no solution.
    pub retry: Duration,
    // Native mining_attempt_timeout defaults to one hour; None is infinity.
    // Expiry requests cooperative cancellation and retains the original job.
    pub attempt_timeout: Option<Duration>,
}
impl Config {
    pub fn new(beneficiary: AccountPublicKey) -> Self {
        Self {
            beneficiary,
            release_info: 0,
            instances: vec![Instance::default()],
            preparation: Default::default(),
            retry: Duration::from_millis(100),
            attempt_timeout: Some(Duration::from_secs(3600)),
        }
    }
}
pub struct Inputs {
    pub snapshots: snapshots::Owner,
    pub config: Config,
    pub clock: Arc<dyn generator::Clock>,
}
pub(super) struct Context {
    pub top: Hash256,
    pub miner: AccountPublicKey,
    pub enabled: bool,
    pub strict: bool,
    pub wall: u64,
    pub now: Instant,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Binding,
    Sequence,
    Time,
    Repository(repo::Error),
    Worker(miner::Error),
    Admission(chain::Error),
    Block(ae_chain::block::Error),
}
#[derive(Debug)]
pub struct Candidate {
    pub id: preparation::Id,
    pub request: preparation::Request,
    pub header: KeyHeader,
    pub source: snapshots::Metadata,
}
#[derive(Debug)]
pub enum PreparationError {
    Build(preparation::Error),
    Binding,
}
#[derive(Debug)]
pub enum Completion {
    Prepared {
        id: preparation::Id,
        request: preparation::Request,
        calculation_time: Option<u64>,
        final_time: Option<u64>,
        result: Result<Arc<Candidate>, PreparationError>,
        used: bool,
    },
    Searched {
        candidate: Arc<Candidate>,
        result: Arc<miner::Completion>,
        selected: bool,
        // Timeout won before result publication. The exact worker receipt is
        // still retained, including a result which finished after cancellation.
        timed_out: bool,
    },
    Discarded {
        candidate: Arc<Candidate>,
        header: Box<KeyHeader>,
    },
    Admitted {
        candidate: Arc<Candidate>,
        hash: Hash256,
        result: Result<chain::Report, chain::Failure>,
    },
    Joined(Box<miner::Status>),
}
#[derive(Debug, Clone)]
pub struct Status {
    pub preparing: Option<preparation::Id>,
    pub candidate: Option<Arc<Candidate>>,
    pub candidates: usize,
    pub active: usize,
    pub timeouts: u64,
    pub timeouts_pending: usize,
    pub pending_admission: bool,
    pub workers: Vec<miner::Status>,
}
struct Preparing {
    task: preparation::Task,
    epoch: u64,
}
struct Job {
    candidate: Arc<Candidate>,
    ticket: miner::Ticket,
    deadline: Option<Instant>,
    timed_out: bool,
}
struct Cpu {
    config: Instance,
    worker: Option<miner::Worker>,
    job: Option<Job>,
}
struct MiningCandidate {
    value: Arc<Candidate>,
    nonce: u128,
}
struct Solution {
    candidate: Arc<Candidate>,
    header: Box<KeyHeader>,
    obsolete: bool,
}
struct Admission {
    candidate: Arc<Candidate>,
    hash: Hash256,
    ticket: chain::Ticket,
}

fn prepared(done: preparation::Completion) -> Completion {
    let preparation::Completion {
        id,
        request,
        calculation_time,
        final_time,
        result,
    } = done;
    let result = result.map_err(PreparationError::Build).and_then(|p| {
        if p.header.common.previous_hash != request.parent.0
            || p.header.miner != request.miner.0
            || p.header.beneficiary != request.beneficiary.0
            || p.header.seal != [0; 42]
            || p.header.nonce != 0
            || Some(p.header.time) != final_time
            || Some(p.state.value.candidate.temporary().time) != calculation_time
        {
            return Err(PreparationError::Binding);
        }
        // CPU work needs only frozen header bytes. Release the trie snapshot
        // here; ordinary admission will capture and validate state again.
        Ok(Arc::new(Candidate {
            id,
            request,
            header: p.header,
            source: p.state.metadata,
        }))
    });
    Completion::Prepared {
        id,
        request,
        calculation_time,
        final_time,
        result,
        used: false,
    }
}
fn seal(job: &Job, result: &miner::Completion) -> Result<Option<Box<KeyHeader>>, Error> {
    if result.id != job.ticket.id()
        || result.work != job.ticket.work()
        || miner::Work::key(&job.candidate.header, result.work.nonces).map_err(Error::Worker)?
            != result.work
    {
        return Err(Error::Binding);
    }
    let Ok(miner::Outcome::Found { nonce, proof }) = &result.result else {
        return Ok(None);
    };
    let index = nonce
        .checked_sub(result.work.nonces.start())
        .ok_or(Error::Binding)?;
    if result.work.nonces.at(index) != Some(*nonce) {
        return Err(Error::Binding);
    }
    let mut header = Box::new(job.candidate.header.clone());
    header.nonce = *nonce;
    for (word, edge) in header.seal.iter_mut().zip(proof.iter()) {
        *word = u32::try_from(*edge).map_err(|_| Error::Binding)?;
    }
    if !cuckoo::verify_key_seal(&header).map_err(|_| Error::Binding)? {
        return Err(Error::Binding);
    }
    Ok(Some(header))
}

// Retiring accepted work is never redirected to replacement services.
// Poll joins only threads observed finished; Drop still owns final joining.
pub struct Retirement {
    preparing: Option<Preparing>,
    cpus: Vec<Cpu>,
    solution: Option<Solution>,
    admission: Option<Admission>,
    output: Option<Completion>,
}
impl Retirement {
    pub fn poll(&mut self, outputs: &mut Vec<Completion>) -> bool {
        outputs.extend(self.output.take());
        if let Some(p) = &mut self.preparing
            && let Some(done) = p.task.try_result()
        {
            outputs.push(prepared(done));
            self.preparing = None;
        }
        if let Some(s) = self.solution.take() {
            outputs.push(Completion::Discarded {
                candidate: s.candidate,
                header: s.header,
            });
        }
        if let Some(a) = &mut self.admission
            && let Some(result) = a.ticket.try_result()
        {
            let a = self.admission.take().expect("observed admission");
            outputs.push(Completion::Admitted {
                candidate: a.candidate,
                hash: a.hash,
                result,
            });
        }
        for cpu in &mut self.cpus {
            if let Some(job) = &cpu.job
                && let Some(result) = job.ticket.try_result()
            {
                let job = cpu.job.take().expect("observed search");
                outputs.push(Completion::Searched {
                    candidate: job.candidate,
                    result,
                    selected: false,
                    timed_out: job.timed_out,
                });
            }
            if cpu.worker.as_ref().is_some_and(miner::Worker::is_finished) {
                let worker = cpu.worker.take().expect("finished miner");
                outputs.push(Completion::Joined(Box::new(worker.shutdown())));
            }
        }
        self.is_settled()
    }
    pub fn is_settled(&self) -> bool {
        self.preparing.is_none()
            && self.solution.is_none()
            && self.admission.is_none()
            && self.output.is_none()
            && self
                .cpus
                .iter()
                .all(|c| c.worker.is_none() && c.job.is_none())
    }
}
