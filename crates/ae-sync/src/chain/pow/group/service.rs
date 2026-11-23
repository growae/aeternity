use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
type Owner<O> = supervisor::Owner<factory::Factory<O>>;
pub(super) struct Service<O: MapOrder + Clone + Send + Sync + 'static> {
    factory: Option<factory::Factory<O>>,
    owner: Option<Owner<O>>,
    shared: Arc<Shared<O>>,
    pool: pool::Client,
    clocks: Arc<dyn Clocks>,
    interval: Duration,
    lease: Option<GroupLease>,
    retiring: Option<Retirement>,
    outbox: VecDeque<Event>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Service<O> {
    pub fn new(
        factory: factory::Factory<O>,
        shared: Arc<Shared<O>>,
        pool: pool::Client,
        clocks: Arc<dyn Clocks>,
        interval: Duration,
        lease: GroupLease,
    ) -> Self {
        Self {
            factory: Some(factory),
            owner: None,
            shared,
            pool,
            clocks,
            interval,
            lease: Some(lease),
            retiring: None,
            outbox: VecDeque::new(),
        }
    }
    pub fn run(mut self) -> Report {
        let result = catch_unwind(AssertUnwindSafe(|| self.drive()));
        let failure = match result {
            Ok(Ok(())) | Ok(Err(Error::Closed)) => None,
            Ok(Err(e)) => Some(e),
            Err(_) => Some(Error::Panicked),
        };
        {
            let mut s = lock(&self.shared.state);
            s.shutdown = true;
            s.status.accepting = false;
        }
        self.withdraw();
        let mut retirements: Vec<_> = self.retiring.take().into_iter().collect();
        if let Some(owner) = self.owner.take() {
            let shutdown = owner.shutdown();
            lock(&self.shared.state).status.supervisor = shutdown.status;
            if let Some(notice) = shutdown.pending {
                match notice {
                    supervisor::Notice::Retired(r) => retirements.push(Retirement::new(r)),
                    supervisor::Notice::StartFailed(f) => {
                        self.outbox.push_back(Event::StartFailed(f))
                    }
                    supervisor::Notice::Restarted {
                        generator_generation,
                        conductor_generation,
                    } => self.outbox.push_back(Event::Ready(GenerationId {
                        generator: generator_generation,
                        producer: conductor_generation,
                    })),
                }
            }
            let r = Retirement::new(shutdown.retired);
            if !r.empty() {
                retirements.push(r);
            }
        }
        let mut s = lock(&self.shared.state);
        s.status.running = false;
        s.status.accepting = false;
        s.status.failure = failure;
        s.status.retiring = !retirements.is_empty();
        s.status.applied_revision = None;
        let mut report = Report {
            status: s.status.clone(),
            outputs: s.outputs.drain(..).collect(),
            unpublished: self.outbox.drain(..).collect(),
            retirements,
            settled: vec![],
            pool: self.pool.clone(),
            _lease: self.lease.take().expect("group lease"),
        };
        drop(s);
        if let Err(e) = report.poll() {
            report.status.failure.get_or_insert(e);
        }
        lock(&self.shared.state).status = report.status.clone();
        report
    }
    fn drive(&mut self) -> Result<(), Error> {
        if lock(&self.shared.state).shutdown {
            return Ok(());
        }
        match Owner::start(self.factory.take().expect("factory")) {
            Ok(owner) => {
                let s = owner.status();
                self.outbox.push_back(Event::Ready(GenerationId {
                    generator: s.generator_generation,
                    producer: s.conductor_generation,
                }));
                self.owner = Some(owner);
            }
            Err(failure) => {
                let failure = *failure;
                let child = failure.failure.child;
                let mut s = lock(&self.shared.state);
                s.status.supervisor.phase = supervisor::Phase::Failed;
                s.status.supervisor.generator_generation = failure
                    .retired
                    .generator
                    .as_ref()
                    .map_or(failure.failure.generation, |(n, _)| *n);
                s.status.supervisor.conductor_generation = if child == supervisor::Member::Conductor
                {
                    failure.failure.generation
                } else {
                    0
                };
                drop(s);
                self.outbox.push_back(Event::StartFailed(failure.failure));
                self.retiring = Some(Retirement::new(failure.retired));
                return Err(Error::Startup(child));
            }
        }
        loop {
            if lock(&self.shared.state).shutdown {
                return Ok(());
            }
            self.step()?;
            let mut s = lock(&self.shared.state);
            s.status.supervisor = self.owner.as_ref().expect("owner").status();
            s.status.retiring = self.retiring.is_some();
            if s.shutdown {
                return Ok(());
            }
            drop(
                self.shared
                    .wake
                    .wait_timeout(s, self.interval)
                    .unwrap_or_else(|e| e.into_inner()),
            );
        }
    }
    fn step(&mut self) -> Result<(), Error> {
        if let Some(retiring) = &mut self.retiring {
            if !retiring.poll(&self.pool)? {
                return Ok(());
            }
            // Do not release leases or start a successor while its retirement
            // observation cannot fit. External Arc clones keep their credits.
            if *lock(&self.shared.credits) >= self.shared.limit {
                return Ok(());
            }
            self.outbox.push_back(Event::Retired(Box::new(
                self.retiring.take().expect("retiring").finish(),
            )));
        }
        if !self.flush()? {
            return Ok(());
        }
        self.controls()?;
        if *lock(&self.shared.credits) < self.shared.limit {
            let current = lock(&self.shared.state).current.clone();
            if let Some(current) = current
                && let Some(output) = current.producer.front()
            {
                self.outbox.push_back(Event::Produced {
                    generation: current.id,
                    output: output.clone(),
                });
                if !current.producer.acknowledge(&output) {
                    return Err(Error::Binding);
                }
                self.flush()?;
            }
        }
        let dead = self
            .owner
            .as_ref()
            .expect("owner")
            .children()
            .is_some_and(|(g, p)| g.is_finished() || p.is_finished());
        if dead {
            self.withdraw();
        }
        let second = self.clocks.second();
        if lock(&self.shared.state).shutdown {
            return Ok(());
        }
        let owner = self.owner.as_mut().expect("owner");
        owner.poll(second).map_err(Error::Supervision)?;
        if let Some(notice) = owner.take_notice() {
            match notice {
                supervisor::Notice::Retired(r) => self.retiring = Some(Retirement::new(r)),
                supervisor::Notice::StartFailed(f) => self.outbox.push_back(Event::StartFailed(f)),
                supervisor::Notice::Restarted {
                    generator_generation,
                    conductor_generation,
                } => self.outbox.push_back(Event::Ready(GenerationId {
                    generator: generator_generation,
                    producer: conductor_generation,
                })),
            }
        }
        Ok(())
    }
    fn withdraw(&self) {
        let mut s = lock(&self.shared.state);
        if let Some(current) = s.current.take() {
            current.producer.request_shutdown();
        }
        s.status.applied_revision = None;
    }
    fn install(&self, id: GenerationId) -> Result<(), Error> {
        let (g, p) = self
            .owner
            .as_ref()
            .expect("owner")
            .children()
            .ok_or(Error::Binding)?;
        let mut s = lock(&self.shared.state);
        if s.shutdown {
            return Err(Error::Closed);
        }
        s.current = Some(Generation {
            id,
            generator: g.client(),
            producer: p.client(),
        });
        s.status.applied_revision = None;
        Ok(())
    }
    fn controls(&self) -> Result<(), Error> {
        let mut s = lock(&self.shared.state);
        if s.shutdown {
            return Err(Error::Closed);
        }
        if let Some(current) = &s.current {
            match current.producer.set_enabled(s.status.requested_enabled) {
                Ok(revision) => {
                    let p = current.producer.status();
                    if p.applied_revision == revision && p.enabled == s.status.requested_enabled {
                        s.status.applied_revision = Some(s.status.request_revision);
                    }
                }
                Err(producer::Error::Closed) => (),
                Err(e) => return Err(Error::Producer(e)),
            }
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<bool, Error> {
        while !self.outbox.is_empty() {
            let credit = {
                let mut used = lock(&self.shared.credits);
                if *used >= self.shared.limit {
                    return Ok(false);
                }
                *used += 1;
                Credit(self.shared.credits.clone())
            };
            let ready = {
                let mut s = lock(&self.shared.state);
                if s.shutdown {
                    return Err(Error::Closed);
                }
                s.status.published = s.status.published.checked_add(1).ok_or(Error::Sequence)?;
                let event = self.outbox.pop_front().expect("event");
                let ready = if let Event::Ready(id) = &event {
                    Some(*id)
                } else {
                    None
                };
                let sequence = s.status.published;
                s.outputs.push_back(Arc::new(Output {
                    sequence,
                    event,
                    _credit: credit,
                }));
                ready
            };
            if let Some(id) = ready {
                self.install(id)?;
            }
        }
        Ok(true)
    }
}
