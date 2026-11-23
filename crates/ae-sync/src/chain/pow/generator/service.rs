use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

pub(super) struct Service<O> {
    shared: Arc<Shared<O>>,
    events: events::Reader,
    chain_cursor: events::Cursor,
    pool_cursor: pool::events::Cursor,
    tip: Option<Hash256>,
    snapshots: snapshots::Owner,
    pool: pool::Client,
    config: Config<O>,
    clock: Arc<dyn Clock>,
    lease: Option<owned::GeneratorLease>,
    status: Status,
    candidate: Option<Arc<Candidate<O>>>,
    active: Option<task::Active<O>>,
    completed: Option<task::Outcome<O>>,
    retirement: Option<packing::Cancelled>,
    last_retirement: Option<packing::CancelReport>,
    desired: Option<Hash256>,
    queue: VecDeque<Arc<Signed>>,
    queued_bytes: usize,
    retry: Option<Instant>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Service<O> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shared: Arc<Shared<O>>,
        events: events::Reader,
        chain_cursor: events::Cursor,
        tip: Option<Hash256>,
        snapshots: snapshots::Owner,
        pool: pool::Client,
        config: Config<O>,
        clock: Arc<dyn Clock>,
        lease: owned::GeneratorLease,
    ) -> Self {
        let pool_cursor = pool.event_cursor();
        Self {
            shared,
            events,
            chain_cursor,
            pool_cursor,
            tip,
            snapshots,
            pool,
            config,
            clock,
            lease: Some(lease),
            status: Status::default(),
            candidate: None,
            active: None,
            completed: None,
            retirement: None,
            last_retirement: None,
            desired: None,
            queue: VecDeque::new(),
            queued_bytes: 0,
            retry: None,
        }
    }
    pub fn run(mut self) -> Report {
        let result = catch_unwind(AssertUnwindSafe(|| self.drive()));
        let failure = match result {
            Ok(Ok(())) | Ok(Err(Error::Stopped)) => None,
            Ok(Err(e)) => Some(e),
            Err(_) => Some(Error::Panicked),
        };
        self.status.failure = failure;
        {
            let mut state = lock(&self.shared.state);
            state.status.accepting = false;
            state.shutdown = true;
            state.controls.clear();
        }
        self.status.generating = false;
        self.retry = None;
        if let Some(mut active) = self.active.take() {
            active.cancel();
            let result = active.join();
            self.retirement = result.retirement;
            self.status.retired = self.status.retired.saturating_add(1);
        }
        // Join first, then drain the exact accepted receipt. A non-settleable
        // result is returned intact, retaining the exclusive generator lease.
        while self.retirement.is_some() {
            match self.settle() {
                Ok(true) => break,
                Ok(false) => thread::sleep(self.config.poll_interval),
                Err(e) => {
                    self.status.failure.get_or_insert(e);
                    break;
                }
            }
        }
        self.sync(false);
        Report {
            status: self.status,
            retirement: self.retirement,
            last_retirement: self.last_retirement,
            _lease: self.lease.take().expect("generator lease"),
        }
    }
    fn drive(&mut self) -> Result<(), Error> {
        loop {
            if lock(&self.shared.state).shutdown {
                return Ok(());
            }
            self.control()?;
            let caught_up = self.feeds()?;
            self.collect();
            let settled = self.settle()?;
            if caught_up && settled {
                self.complete()?;
                self.launch()?;
            }
            self.sync(true);
            let state = lock(&self.shared.state);
            if state.shutdown {
                return Ok(());
            }
            if state.controls.is_empty() {
                drop(
                    self.shared
                        .wake
                        .wait_timeout(state, self.config.poll_interval)
                        .unwrap_or_else(|e| e.into_inner()),
                );
            }
        }
    }
    fn sync(&mut self, running: bool) {
        let mut state = lock(&self.shared.state);
        self.status.running = running;
        self.status.accepting = running && !state.shutdown;
        self.status.worker = self.active.is_some();
        self.status.retiring =
            self.retirement.is_some() || self.active.as_ref().is_some_and(|a| a.obsolete);
        self.status.queued_transactions = self.queue.len();
        self.status.queued_bytes = self.queued_bytes;
        self.status.retry_pending = self.retry.is_some();
        state.status = self.status.clone();
        state.candidate = self.candidate.clone();
    }
    fn control(&mut self) -> Result<(), Error> {
        let command = lock(&self.shared.state).controls.pop_front();
        let Some(command) = command else {
            return Ok(());
        };
        let result = match command.command {
            Command::Start if self.status.generating => {
                if let Some(candidate) = self.candidate.clone() {
                    if !self.publish(candidate)? {
                        Err(RequestError::OutputFull)
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                }
            }
            Command::Start => {
                self.status.generating = true;
                if let Some(tip) = self.tip {
                    self.rebuild(tip)?;
                }
                Ok(())
            }
            Command::Stop => {
                if self.status.generating {
                    self.cancel();
                    self.status.generating = false;
                    self.clear_retry();
                }
                Ok(())
            }
        };
        self.sync(true);
        command.reply.complete(result);
        Ok(())
    }
    fn cancel(&mut self) {
        if let Some(active) = &mut self.active {
            active.cancel();
        }
        self.desired = None;
        self.completed = None;
    }
    fn clear_retry(&mut self) {
        self.retry = None;
        self.status.retries = 0;
    }
    fn rebuild(&mut self, parent: Hash256) -> Result<(), Error> {
        self.cancel();
        self.status.epoch = self.status.epoch.checked_add(1).ok_or(Error::Sequence)?;
        self.candidate = None;
        self.queue.clear();
        self.queued_bytes = 0;
        self.desired = Some(parent);
        Ok(())
    }
    fn feeds(&mut self) -> Result<bool, Error> {
        let batch = self
            .events
            .after(&self.chain_cursor, self.config.feed_batch)
            .map_err(Error::Chain)?;
        if batch.closed {
            return Err(Error::ChainClosed);
        }
        let chain_caught_up = batch.records.len() < self.config.feed_batch;
        for record in batch.records {
            let change = record.change();
            if self.tip != Some(change.new_tip) {
                self.tip = Some(change.new_tip);
                if self.status.generating {
                    self.status.preemptions = self.status.preemptions.saturating_add(1);
                    self.clear_retry();
                    self.rebuild(change.new_tip)?;
                }
            }
        }
        self.chain_cursor = batch.cursor;
        let batch = match self
            .pool
            .events_after(&self.pool_cursor, self.config.feed_batch)
        {
            Ok(batch) => batch,
            Err(pool::events::ReadError::Lagged { .. }) if !self.status.generating => {
                self.status.stopped_feed_gaps = self.status.stopped_feed_gaps.saturating_add(1);
                self.pool_cursor = self.pool.event_cursor();
                return Ok(chain_caught_up);
            }
            Err(e) => return Err(Error::PoolFeed(e)),
        };
        if batch.closed {
            return Err(Error::PoolClosed);
        }
        let caught_up = chain_caught_up && batch.events.len() < self.config.feed_batch;
        if self.status.generating {
            for event in batch.events {
                self.transaction(event.transaction)?;
            }
        }
        self.pool_cursor = batch.cursor;
        Ok(caught_up)
    }
    fn transaction(&mut self, tx: Arc<Signed>) -> Result<(), Error> {
        if self.active.is_none()
            && self.completed.is_none()
            && self.retirement.is_none()
            && self.desired.is_none()
        {
            if let Some(base) = self.candidate.clone() {
                self.spawn(task::Work::Update(base, vec![tx]));
            }
            return Ok(());
        }
        let bytes = tx.encode().len();
        if self.queue.len() >= self.config.queued_transactions
            || bytes > self.config.queued_bytes.saturating_sub(self.queued_bytes)
        {
            return Err(Error::QueueLimit);
        }
        self.queued_bytes += bytes;
        self.queue.push_front(tx);
        Ok(())
    }
    fn spawn(&mut self, work: task::Work<O>) {
        match &work {
            task::Work::Create(_) => self.status.attempts = self.status.attempts.saturating_add(1),
            task::Work::Update(_, _) => self.status.updates = self.status.updates.saturating_add(1),
        }
        match task::spawn(
            work,
            self.snapshots.clone(),
            self.pool.clone(),
            self.config.clone(),
            self.clock.clone(),
        ) {
            Ok(active) => self.active = Some(active),
            Err(e) => self.completed = Some(task::Outcome::Failed(e)),
        }
    }
    fn collect(&mut self) {
        if self.active.as_ref().is_some_and(task::Active::finished) {
            let active = self.active.take().expect("finished task");
            let obsolete = active.obsolete;
            let result = active.join();
            self.retirement = result.retirement;
            if obsolete {
                self.status.retired = self.status.retired.saturating_add(1);
            } else {
                self.completed = Some(result.outcome);
            }
        }
    }
    fn settle(&mut self) -> Result<bool, Error> {
        let Some(retirement) = &mut self.retirement else {
            return Ok(true);
        };
        if self.pool.info().joined {
            retirement.retire_pool(&self.pool).map_err(Error::Packing)?;
        }
        if !retirement.step().map_err(Error::Packing)? {
            return Ok(false);
        }
        let report = self.retirement.take().expect("settled retirement").finish();
        self.status.settled_selections = self
            .status
            .settled_selections
            .saturating_add(u64::from(report.selection.is_some()));
        self.status.settled_failure_reports = self
            .status
            .settled_failure_reports
            .saturating_add(u64::from(report.failure_reports.is_some()));
        self.last_retirement = Some(report);
        Ok(true)
    }
    fn publish(&mut self, candidate: Arc<Candidate<O>>) -> Result<bool, Error> {
        if candidate
            .prepared()
            .value
            .candidate
            .block()
            .transactions
            .is_empty()
        {
            return Ok(true);
        }
        let Some(credit) =
            receipts::Credit::acquire(&self.shared.publications, self.shared.limits.1)
        else {
            return Ok(false);
        };
        let mut state = lock(&self.shared.state);
        if state.shutdown {
            return Err(Error::Stopped);
        }
        self.status.published = self
            .status
            .published
            .checked_add(1)
            .ok_or(Error::Sequence)?;
        state.publications.push_back(Arc::new(Publication {
            sequence: self.status.published,
            epoch: self.status.epoch,
            candidate,
            _credit: credit,
        }));
        Ok(true)
    }
    fn complete(&mut self) -> Result<(), Error> {
        let Some(outcome) = self.completed.take() else {
            return Ok(());
        };
        match outcome {
            task::Outcome::Candidate(candidate) => {
                // Current and externally held candidates own their capacity.
                // Two extra slots allow current + in-flight work alongside the
                // configured publication budget; clones never release credit.
                let credit =
                    receipts::Credit::acquire(&self.shared.candidates, self.shared.limits.1 + 2);
                let output_full = !candidate.value.candidate.block().transactions.is_empty()
                    && *lock(&self.shared.publications) >= self.shared.limits.1;
                if credit.is_none() || output_full {
                    self.completed = Some(task::Outcome::Candidate(candidate));
                    return Ok(());
                }
                let candidate = Arc::new(Candidate {
                    prepared: *candidate,
                    _credit: credit.expect("reserved candidate"),
                });
                assert!(self.publish(candidate.clone())?);
                self.candidate = Some(candidate);
                self.clear_retry();
            }
            task::Outcome::Unchanged => {
                self.status.unchanged = self.status.unchanged.saturating_add(1);
                self.clear_retry();
            }
            task::Outcome::Failed(e) => {
                self.status.last_error = Some(e);
                self.status.retries = self.status.retries.saturating_add(1);
                if self.status.retries == 1 {
                    self.retry_now()?;
                } else {
                    self.retry = Some(Instant::now() + retry_delay(self.status.retries));
                }
            }
            task::Outcome::Cancelled => {}
        }
        Ok(())
    }
    fn retry_now(&mut self) -> Result<(), Error> {
        self.retry = None;
        if let Some(tip) = self.tip {
            self.rebuild(tip)?;
        } else {
            self.status.retries = self.status.retries.saturating_add(1);
            self.retry = Some(Instant::now() + retry_delay(self.status.retries));
        }
        Ok(())
    }
    fn launch(&mut self) -> Result<(), Error> {
        if !self.status.generating
            || self.active.is_some()
            || self.completed.is_some()
            || self.retirement.is_some()
        {
            return Ok(());
        }
        if self.retry.is_some_and(|at| at <= Instant::now()) {
            self.retry_now()?;
        }
        if let Some(parent) = self.desired.take() {
            self.spawn(task::Work::Create(parent));
        } else if self.retry.is_none()
            && !self.queue.is_empty()
            && let Some(base) = self.candidate.clone()
        {
            let txs = self.queue.drain(..).collect();
            self.queued_bytes = 0;
            self.spawn(task::Work::Update(base, txs));
        }
        Ok(())
    }
}
