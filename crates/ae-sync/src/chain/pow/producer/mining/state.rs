use super::*;

pub(in crate::chain::pow::producer) struct Mining<O> {
    algorithm: ae_protocol::ConsensusAlgorithm,
    builder: preparation::Builder<O>,
    instrumentation: ae_chain::instrumentation::Publisher,
    config: Config,
    clock: Arc<dyn generator::Clock>,
    epoch: u64,
    wanted: bool,
    retry_at: Option<Instant>,
    candidates: Vec<MiningCandidate>,
    preparing: Option<Preparing>,
    cpus: Vec<Cpu>,
    next_cpu: usize,
    timeouts: u64,
    solution: Option<Solution>,
    admission: Option<Admission>,
    output: Option<Completion>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Mining<O> {
    pub fn new(importer: &chain::Importer<O>, inputs: Inputs) -> Result<Self, Error> {
        let Inputs {
            snapshots,
            config,
            clock,
        } = inputs;
        let algorithm = importer.algorithm();
        if !matches!(
            algorithm,
            ae_protocol::ConsensusAlgorithm::Cuckoo | ae_protocol::ConsensusAlgorithm::OnDemand
        ) {
            return Err(Error::Binding);
        }
        if !snapshots.uses_database(importer.database())
            || config.instances.len() > 64
            || config.retry > Duration::from_secs(60)
            || config
                .attempt_timeout
                .is_some_and(|timeout| timeout.is_zero())
            || config
                .instances
                .iter()
                .any(|i| i.repeats == 0 || i.repeats > i.worker.maximum_attempts)
        {
            return Err(Error::Configuration);
        }
        let builder = importer.pow_preparation(snapshots, config.preparation)?;
        if algorithm == ae_protocol::ConsensusAlgorithm::Cuckoo
            && builder.edge_bits() != config.edge_bits
        {
            return Err(Error::Binding);
        }
        let mut cpus = Vec::new();
        for instance in &config.instances {
            let worker = if algorithm == ae_protocol::ConsensusAlgorithm::OnDemand {
                miner::Worker::spawn_on_demand(instance.worker)
            } else {
                match &instance.external {
                    Some(config) => miner::Worker::spawn_external(instance.worker, config.clone()),
                    None => miner::Worker::spawn(instance.worker),
                }
            }
            .map_err(Error::Worker)?;
            cpus.push(Cpu {
                config: instance.clone(),
                worker: Some(worker),
                job: None,
            });
        }
        Ok(Self {
            algorithm,
            builder,
            instrumentation: importer.database().instrumentation().clone(),
            config,
            clock,
            cpus,
            next_cpu: 0,
            timeouts: 0,
            epoch: 0,
            wanted: true,
            retry_at: None,
            candidates: vec![],
            preparing: None,
            solution: None,
            admission: None,
            output: None,
        })
    }
    pub fn status(&self) -> Status {
        Status {
            preparing: self.preparing.as_ref().map(|p| p.task.id()),
            candidate: self.candidates.first().map(|c| c.value.clone()),
            candidates: self.candidates.len(),
            active: self.cpus.iter().filter(|c| c.job.is_some()).count(),
            timeouts: self.timeouts,
            timeouts_pending: self
                .cpus
                .iter()
                .filter(|c| c.job.as_ref().is_some_and(|j| j.timed_out))
                .count(),
            pending_admission: self.admission.is_some(),
            workers: self
                .cpus
                .iter()
                .filter_map(|c| c.worker.as_ref().map(|w| w.client().status()))
                .collect(),
        }
    }
    // A same-generation micro top change deliberately does not call this.
    pub fn invalidate(&mut self) -> Result<(), Error> {
        self.epoch = self.epoch.checked_add(1).ok_or(Error::Sequence)?;
        self.candidates.clear();
        self.wanted = true;
        self.retry_at = None;
        if let Some(p) = &self.preparing {
            p.task.request_cancel();
        }
        for cpu in &mut self.cpus {
            if let Some(job) = &mut cpu.job {
                job.deadline = None;
                job.ticket.request_cancel();
            }
        }
        if let Some(s) = &mut self.solution {
            s.obsolete = true;
        }
        // An accepted importer receipt is retained even after invalidation.
        Ok(())
    }
    pub fn enable(&mut self) {
        self.wanted = true;
        self.retry_at = None;
    }
    pub fn take_output(&mut self) -> Option<Completion> {
        self.output.take()
    }
    fn collect(&mut self) {
        let current = self.candidates.first().map(|c| c.value.id);
        self.candidates.retain(|c| {
            Some(c.value.id) == current
                || self.cpus.iter().any(|cpu| {
                    cpu.job
                        .as_ref()
                        .is_some_and(|j| j.candidate.id == c.value.id)
                })
        });
    }
    pub fn poll(
        &mut self,
        top: Hash256,
        miner: AccountPublicKey,
        now: Instant,
    ) -> Result<(), Error> {
        // Deadline observation is independent of publication capacity. Every
        // expired job keeps its original receipt and instance until observed.
        for cpu in &mut self.cpus {
            if let Some(job) = &mut cpu.job
                && job.deadline.is_some_and(|at| now >= at)
            {
                job.deadline = None;
                if job.ticket.cancel_if_pending() {
                    job.timed_out = true;
                    self.timeouts = self.timeouts.saturating_add(1);
                }
            }
        }
        if self.output.is_some() {
            return Ok(());
        }
        if let Some(a) = &mut self.admission
            && let Some(result) = a.ticket.try_result()
        {
            let a = self.admission.take().expect("observed admission");
            self.output = Some(Completion::Admitted {
                candidate: a.candidate,
                hash: a.hash,
                result,
            });
            return Ok(());
        }
        if self.solution.as_ref().is_some_and(|s| s.obsolete) {
            let s = self.solution.take().expect("obsolete solution");
            self.output = Some(Completion::Discarded {
                candidate: s.candidate,
                header: s.header,
            });
            return Ok(());
        }
        if let Some(p) = &mut self.preparing
            && let Some(done) = p.task.try_result()
        {
            let epoch = p.epoch;
            let mut out = prepared(done, p.edge_bits, p.algorithm);
            self.preparing = None;
            self.wanted = true;
            if let Completion::Prepared { result, used, .. } = &mut out {
                match result {
                    Ok(candidate)
                        if epoch == self.epoch
                            && candidate.request.parent == top
                            && candidate.request.miner == miner =>
                    {
                        self.candidates.insert(
                            0,
                            MiningCandidate {
                                value: candidate.clone(),
                                nonce: rand::random::<u64>().into(),
                            },
                        );
                        *used = true;
                        self.retry_at = None;
                        self.collect();
                    }
                    Ok(_) => (),
                    Err(_) if epoch == self.epoch => self.retry_at = Some(now + self.config.retry),
                    Err(_) => (),
                }
            }
            self.output = Some(out);
            return Ok(());
        }
        // Retain each accepted CPU receipt until there is room to observe it.
        // At most one result is consumed per producer step.
        for offset in 0..self.cpus.len() {
            let index = (self.next_cpu + offset) % self.cpus.len();
            let Some(job) = &self.cpus[index].job else {
                continue;
            };
            let Some(result) = job.ticket.try_result() else {
                continue;
            };
            return self.searched(index, result, now);
        }
        for cpu in &self.cpus {
            let status = cpu.worker.as_ref().expect("live worker").client().status();
            if !status.running || !status.accepting {
                return Err(Error::Worker(
                    status.failure.unwrap_or(miner::Error::Closed),
                ));
            }
        }
        Ok(())
    }
    fn searched(
        &mut self,
        index: usize,
        result: Arc<miner::Completion>,
        now: Instant,
    ) -> Result<(), Error> {
        let job = self.cpus[index].job.as_ref().expect("accepted job");
        let sealed = seal(job, &result);
        let valid = self
            .candidates
            .iter()
            .any(|c| c.value.id == job.candidate.id);
        let selected = valid
            && !job.timed_out
            && matches!(sealed, Ok(Some(_)))
            && self.solution.is_none()
            && self.admission.is_none();
        let job = self.cpus[index].job.take().expect("observed job");
        self.next_cpu = (index + 1) % self.cpus.len();
        self.output = Some(Completion::Searched {
            candidate: job.candidate.clone(),
            result: result.clone(),
            selected,
            timed_out: job.timed_out,
        });
        // Observe a live reply once, before any admission or strict-parent
        // rejection. Resource bounds, cancellation and retirement are not
        // the native no_solution/runtime retry result.
        if sealed.is_ok() && !job.timed_out && !self.candidates.is_empty() {
            use ae_chain::instrumentation::{Counter, Event};
            let counter = if selected {
                Some(Counter::BlocksMined)
            } else if retry_reply(&result.result) {
                Some(Counter::MiningRetries)
            } else {
                None
            };
            if let Some(counter) = counter {
                self.instrumentation.event(Event::Counter(counter, 1));
            }
        }
        if let Some(header) = sealed?
            && selected
        {
            self.candidates.clear();
            self.solution = Some(Solution {
                candidate: job.candidate,
                header,
                obsolete: false,
            });
        }
        self.wanted = true;
        if (result.result.is_err() || job.timed_out) && valid {
            self.retry_at = Some(now + self.config.retry);
        }
        self.collect();
        Ok(())
    }
    // The producer holds its desired-state fence across this method. Snapshot
    // computation and CPU solving run in the owned background workers.
    pub fn act(&mut self, importer: &chain::Importer<O>, context: Context) -> Result<(), Error> {
        let Context {
            top,
            miner,
            enabled,
            strict,
            wall,
            now,
        } = context;
        if self.output.is_some() || self.admission.is_some() {
            return Ok(());
        }
        if let Some(solution) = &self.solution {
            if !enabled || solution.obsolete {
                return Ok(());
            }
            let block = Block::Key(solution.header.as_ref().clone());
            let bytes = block.encode().map_err(Error::Block)?;
            let hash = block.hash().map_err(|_| Error::Binding)?;
            let result = if strict {
                importer.import_created_block_at(bytes, wall, solution.candidate.request.parent)
            } else {
                importer.import_created_block(bytes, wall)
            };
            match result {
                Ok(ticket) => {
                    let solution = self.solution.take().expect("accepted solution");
                    self.admission = Some(Admission {
                        candidate: solution.candidate,
                        hash,
                        ticket,
                    });
                }
                Err(chain::Error::Busy) => (),
                Err(e) => return Err(Error::Admission(e)),
            }
            return Ok(());
        }
        if !self.wanted || self.retry_at.is_some_and(|at| now < at) {
            return Ok(());
        }
        if self
            .candidates
            .first()
            .is_none_or(|c| c.value.request.parent != top)
        {
            if self.preparing.is_none() {
                let task = self
                    .builder
                    .start(
                        preparation::Request {
                            parent: top,
                            miner,
                            beneficiary: self.config.beneficiary,
                            release_info: self.config.release_info,
                        },
                        self.clock.clone(),
                    )
                    .map_err(|_| Error::Binding)?;
                self.preparing = Some(Preparing {
                    algorithm: self.algorithm,
                    edge_bits: self.config.edge_bits,
                    task,
                    epoch: self.epoch,
                });
            }
            return Ok(());
        }
        if !enabled {
            self.wanted = false;
            return Ok(());
        }
        // Compute before accepting any job, so an unrepresentable deadline
        // cannot leave a partly submitted batch without deadline ownership.
        let deadline = self
            .config
            .attempt_timeout
            .map(|timeout| now.checked_add(timeout).ok_or(Error::Time))
            .transpose()?;
        let candidate = self.candidates.first_mut().expect("current candidate");
        let mut retry = false;
        for cpu in &mut self.cpus {
            if cpu.job.is_some() {
                continue;
            }
            let work = if self.algorithm == ae_protocol::ConsensusAlgorithm::OnDemand {
                miner::Work::on_demand(&candidate.value.header)
            } else {
                let nonces = Nonces::reserve(candidate.nonce, cpu.config.repeats)
                    .map_err(|_| Error::Configuration)?;
                miner::Work::key_with_edge_bits(
                    &candidate.value.header,
                    nonces,
                    candidate.value.edge_bits,
                )
            }
            .map_err(Error::Worker)?;
            match cpu
                .worker
                .as_ref()
                .expect("live worker")
                .client()
                .submit(work)
            {
                Ok(ticket) => {
                    cpu.job = Some(Job {
                        candidate: candidate.value.clone(),
                        ticket,
                        deadline,
                        timed_out: false,
                    });
                    candidate.nonce = work.nonces.next().into();
                }
                Err(miner::Error::Busy) => retry = true,
                Err(e) => return Err(Error::Worker(e)),
            }
        }
        self.wanted = retry;
        Ok(())
    }
    pub fn retire(self) -> Retirement {
        if let Some(p) = &self.preparing {
            p.task.request_cancel();
        }
        for cpu in &self.cpus {
            if let Some(job) = &cpu.job {
                job.ticket.request_cancel();
            }
            if let Some(worker) = &cpu.worker {
                worker.request_shutdown();
            }
        }
        Retirement {
            preparing: self.preparing,
            cpus: self.cpus,
            solution: self.solution,
            admission: self.admission,
            output: self.output,
        }
    }
}

fn retry_reply(result: &Result<miner::Outcome, miner::Error>) -> bool {
    use miner::external::{Error, Operation};
    matches!(
        result,
        Ok(miner::Outcome::NoSolution | miner::Outcome::Exhausted)
            | Err(miner::Error::External(
                Error::BadNonce | Error::BadEdge | Error::BadSize
            ))
            | Err(miner::Error::External(Error::Io {
                operation: Operation::Runtime
                    | Operation::Pipe
                    | Operation::Spawn
                    | Operation::Read,
                ..
            }))
    )
}

#[cfg(test)]
mod tests;
