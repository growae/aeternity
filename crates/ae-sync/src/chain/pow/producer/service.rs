use super::*;
use retirement::{Pending, Signed, Signing};
use std::panic::{AssertUnwindSafe, catch_unwind};

pub(super) struct Service<O> {
    importer: chain::Importer<O>,
    generator: generator::Client<O>,
    keys: keys::Client,
    config: Config,
    clock: Box<dyn Clock>,
    shared: Arc<Shared>,
    cursor: events::Cursor,
    selected: Selected,
    status: Status,
    lease: Option<ProducerLease>,
    epoch: u64,
    last_time: Option<Instant>,
    sleep: Option<Instant>,
    generation_wanted: bool,
    initialized: bool,
    leadership_allowed: bool,
    control_after: u64,
    generator_pending: Option<(bool, generator::Ticket)>,
    promote: Option<AccountPublicKey>,
    ready: Option<Arc<generator::Candidate<O>>>,
    pending: Option<Pending>,
    signed: Option<Signed>,
    completion: Option<Completion>,
    mining: Option<mining::Mining<O>>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Service<O> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        importer: chain::Importer<O>,
        generator: generator::Client<O>,
        keys: keys::Client,
        config: Config,
        clock: Box<dyn Clock>,
        shared: Arc<Shared>,
        cursor: events::Cursor,
        selected: Selected,
        status: Status,
        lease: ProducerLease,
        mining: Option<mining::Mining<O>>,
    ) -> Self {
        Self {
            importer,
            generator,
            keys,
            config,
            clock,
            shared,
            cursor,
            selected,
            status,
            lease: Some(lease),
            epoch: 0,
            last_time: None,
            sleep: None,
            generation_wanted: false,
            initialized: false,
            leadership_allowed: false,
            control_after: 0,
            generator_pending: None,
            promote: None,
            ready: None,
            pending: None,
            signed: None,
            completion: None,
            mining,
        }
    }
    pub fn run(mut self) -> Report {
        let result = catch_unwind(AssertUnwindSafe(|| self.drive()));
        self.status.failure = match result {
            Ok(Ok(())) | Ok(Err(Error::Closed)) => None,
            Ok(Err(e)) => Some(e),
            Err(_) => Some(Error::Panicked),
        };
        {
            let mut state = lock(&self.shared.state);
            state.shutdown = true;
            state.status.accepting = false;
        }
        if let Some(commands) = lock(&self.shared.commands).clone() {
            commands.request_shutdown();
        }
        self.status.enabled = false;
        let mut controls: Vec<_> = self
            .generator_pending
            .take()
            .map(|(_, ticket)| ticket)
            .into_iter()
            .collect();
        let stop_rejected = match self.generator.stop() {
            Ok(ticket) => {
                controls.push(ticket);
                None
            }
            Err(e) => Some(e),
        };
        let mut completions: Vec<_> = self.completion.take().into_iter().collect();
        if let Some(signed) = self.signed.take() {
            completions.push(signed.discard());
        }
        self.sync(false);
        let mining = self.mining.take().map(mining::Mining::retire);
        let outputs = lock(&self.shared.state).outputs.drain(..).collect();
        let mut report = Report {
            commands: None,
            status: self.status,
            outputs,
            completions,
            generator_results: vec![],
            generator_stop_rejected: stop_rejected,
            pending: self.pending,
            generator: controls,
            mining,
            _lease: self.lease.take().expect("producer lease"),
        };
        report.poll();
        report
    }
    fn drive(&mut self) -> Result<(), Error> {
        loop {
            let state = lock(&self.shared.state);
            if state.shutdown {
                return Ok(());
            }
            if !state.status.activated {
                drop(
                    self.shared
                        .wake
                        .wait_timeout(state, self.config.poll)
                        .unwrap_or_else(|e| e.into_inner()),
                );
                continue;
            }
            drop(state);
            let (wall, now) = self.clock.now()?;
            if self.last_time.is_some_and(|last| now < last) {
                return Err(Error::Time);
            }
            self.last_time = Some(now);
            // Clock evaluation can block. Recheck the independent fence before
            // consuming events or accepting any service operation afterward.
            self.controls()?;
            let caught_up = self.events(now)?;
            let keys = self.keys.info();
            if !keys.running || !keys.accepting {
                return Err(Error::Keys(keys::Error::Stopped));
            }
            self.status.leader = policy::leader(
                self.leadership_allowed && keys.running && keys.accepting,
                self.selected.miner,
                keys.keys.signing,
            );
            if self.completion.is_none() {
                self.poll_pending()?;
            }
            if self.completion.is_none()
                && self
                    .signed
                    .as_ref()
                    .is_some_and(|s| !self.current(s.parent, s.epoch, s.signer))
            {
                self.completion = self.signed.take().map(Signed::discard);
                self.status.discarded = self.status.discarded.saturating_add(1);
            }
            if let Some(mining) = &mut self.mining {
                mining
                    .poll(self.selected.top, keys.keys.candidate, now)
                    .map_err(Error::Mining)?;
                if self.completion.is_none() {
                    self.completion = mining
                        .take_output()
                        .map(|c| Completion::Mining(Box::new(c)));
                }
            }
            self.generation()?;
            if caught_up {
                self.notifications()?;
            }
            self.emit()?;
            if caught_up
                && self.completion.is_none()
                && self.pending.is_none()
                && *lock(&self.shared.credits) < self.shared.limit
            {
                self.act(wall, now, keys.keys)?;
            }
            if caught_up
                && self.completion.is_none()
                && *lock(&self.shared.credits) < self.shared.limit
                && self.promote.is_none()
                && !matches!(self.pending, Some(Pending::Promote { .. }))
                && let Some(mining) = &mut self.mining
            {
                let shared = self.shared.clone();
                let s = lock(&shared.state);
                if !s.shutdown && s.status.request_revision == self.status.applied_revision {
                    // Promotion can finish during this step. Sample the key
                    // owner again before capturing a fresh candidate identity.
                    let keys = self.keys.info();
                    if keys.running && keys.accepting {
                        mining
                            .act(
                                &self.importer,
                                mining::Context {
                                    top: self.selected.top,
                                    miner: keys.keys.candidate,
                                    enabled: self.status.enabled && s.status.requested_enabled,
                                    strict: self.config.strictly_follow_top,
                                    wall,
                                    now,
                                },
                            )
                            .map_err(Error::Mining)?;
                    }
                }
            }
            self.sync(true);
            let s = lock(&self.shared.state);
            if s.shutdown {
                return Ok(());
            }
            drop(
                self.shared
                    .wake
                    .wait_timeout(s, self.config.poll)
                    .unwrap_or_else(|e| e.into_inner()),
            );
        }
    }
    fn controls(&mut self) -> Result<(), Error> {
        let (revision, enabled, resume, after) = {
            let s = lock(&self.shared.state);
            if s.shutdown {
                return Err(Error::Closed);
            }
            (
                s.status.request_revision,
                s.status.requested_enabled,
                s.resume,
                s.control_after,
            )
        };
        if !self.initialized || revision != self.status.applied_revision {
            self.initialized = true;
            self.status.activated = true;
            self.status.applied_revision = revision;
            self.status.enabled = enabled;
            self.control_after = after;
            self.leadership_allowed = resume;
            self.epoch = self.epoch.checked_add(1).ok_or(Error::Sequence)?;
            self.ready = None;
            self.sleep = None;
            self.generation_wanted = enabled && resume;
            if let Some(mining) = &mut self.mining {
                if enabled {
                    mining.enable();
                } else {
                    mining.invalidate().map_err(Error::Mining)?;
                }
            }
            self.promote = resume.then_some(self.selected.miner);
        }
        Ok(())
    }
    fn sync(&mut self, running: bool) {
        // Promotion may have completed after the drive loop's initial sample.
        // Publish the leader together with the completed promotion barrier.
        let keys = self.keys.info();
        self.status.leader = policy::leader(
            self.leadership_allowed && keys.running && keys.accepting,
            self.selected.miner,
            keys.keys.signing,
        );
        let mut s = lock(&self.shared.state);
        self.status.running = running;
        self.status.accepting = running && !s.shutdown;
        self.status.requested_enabled = s.status.requested_enabled;
        self.status.request_revision = s.status.request_revision;
        self.status.observed_revision = self.cursor.revision();
        self.status.top = self.selected.top;
        self.status.key = self.selected.key;
        self.status.miner = self.selected.miner;
        self.status.pending = self.pending.is_some();
        self.status.promotion_pending =
            self.promote.is_some() || matches!(self.pending, Some(Pending::Promote { .. }));
        self.status.generator_pending = self.generator_pending.is_some();
        self.status.mining = self.mining.as_ref().map(mining::Mining::status);
        self.status.sleeping = self
            .sleep
            .is_some_and(|at| self.last_time.is_some_and(|now| now < at));
        s.status = self.status.clone();
    }
    fn events(&mut self, now: Instant) -> Result<bool, Error> {
        let source = self.importer.database().events();
        let batch = source
            .after(&self.cursor, self.config.events_per_step)
            .map_err(Error::Events)?;
        if batch.closed {
            return Err(Error::SourceClosed);
        }
        let caught_up = batch.records.len() < self.config.events_per_step;
        for event in batch.records {
            let change = event.change();
            if change.old_tip == Some(change.new_tip) {
                continue;
            }
            let next = selected(
                &self.importer.database().view().map_err(Error::Repository)?,
                change.new_tip,
            )?;
            if (next.key != self.selected.key
                || event.context().origin == publication::Origin::Rollback)
                && let Some(mining) = &mut self.mining
            {
                mining.invalidate().map_err(Error::Mining)?;
            }
            if next.key != self.selected.key {
                self.sleep = None;
            }
            self.selected = next;
            self.epoch = self.epoch.checked_add(1).ok_or(Error::Sequence)?;
            self.ready = None;
            let context = event.context();
            if context.origin == publication::Origin::Rollback {
                self.leadership_allowed = false;
                self.generation_wanted = false;
                self.promote = None;
                self.sleep = None;
                continue;
            }
            let Some(block) = context.block else {
                return Err(Error::Binding);
            };
            let keys = self.keys.info().keys;
            if block.kind == publication::Kind::Key {
                self.promote = Some(next.miner);
            }
            if change.revision <= self.control_after {
                // This event was already published when the control was
                // accepted. Catch up the selected tip and key authority, but
                // do not restore leadership or generation cleared by a stop.
                continue;
            }
            self.leadership_allowed = true;
            let leader = keys.signing == next.miner
                || (block.kind == publication::Kind::Key && keys.candidate == next.miner);
            match policy::origin(leader, context.origin, block.kind) {
                policy::Action::Start => self.generation_wanted = true,
                policy::Action::Stop => self.generation_wanted = false,
                policy::Action::Sleep => {
                    self.sleep = Some(now + Duration::from_millis(self.config.micro_cycle_ms))
                }
                policy::Action::None => (),
            }
        }
        self.cursor = batch.cursor;
        Ok(caught_up)
    }
    fn current(&self, parent: Hash256, epoch: u64, signer: AccountPublicKey) -> bool {
        self.status.leader
            && self.selected.top == parent
            && self.epoch == epoch
            && self.selected.miner == signer
    }
    fn generation(&mut self) -> Result<(), Error> {
        if let Some((target, ticket)) = &self.generator_pending
            && let Some(result) = ticket.try_result()
        {
            if let Err(error) = result {
                // A repeated native start can find its already-running
                // generator's publication queue full. Its state is still on.
                if error != generator::RequestError::OutputFull
                    || !*target
                    || !self.generator.status().generating
                {
                    return Err(Error::Generator(error));
                }
            }
            self.generator_pending = None;
        }
        let info = self.generator.status();
        if !info.running || !info.accepting {
            return Err(Error::GeneratorClosed);
        }
        let target = self.status.leader && self.generation_wanted;
        if self.generator_pending.is_none() && info.generating != target {
            let reply = if target {
                self.generator.start()
            } else {
                self.generator.stop()
            };
            match reply {
                Ok(ticket) => self.generator_pending = Some((target, ticket)),
                Err(generator::RequestError::Busy) => (),
                Err(e) => return Err(Error::Generator(e)),
            }
        }
        Ok(())
    }
    fn notifications(&mut self) -> Result<(), Error> {
        for _ in 0..self.config.events_per_step {
            let Some(event) = self.generator.front() else {
                break;
            };
            if self.status.leader {
                // Native events fetch the current candidate, not the payload
                // of an older notification still waiting in a bounded queue.
                let candidate = self
                    .generator
                    .candidate()
                    .filter(|c| !c.prepared().value.candidate.block().transactions.is_empty());
                // The generator may observe a commit after our event batch.
                // Leave its notification queued until we consume that commit,
                // rather than clearing the new candidate as apparently stale.
                if self
                    .importer
                    .database()
                    .view()
                    .map_err(Error::Repository)?
                    .status()
                    .revision
                    != self.cursor.revision()
                {
                    break;
                }
                self.ready = candidate;
            }
            if !self.generator.acknowledge(&event) {
                return Err(Error::Binding);
            }
        }
        Ok(())
    }
    fn poll_pending(&mut self) -> Result<(), Error> {
        let Some(pending) = &mut self.pending else {
            return Ok(());
        };
        match pending {
            Pending::Promote { ticket, .. } => {
                let Some(result) = ticket.try_result() else {
                    return Ok(());
                };
                let Some(Pending::Promote { expected, .. }) = self.pending.take() else {
                    unreachable!()
                };
                if result.is_ok() {
                    self.status.promotions = self.status.promotions.saturating_add(1);
                }
                self.completion = Some(Completion::Promoted { expected, result });
            }
            Pending::Sign(signing) => {
                let Some(result) = signing.ticket.try_result() else {
                    return Ok(());
                };
                let Some(Pending::Sign(mut signing)) = self.pending.take() else {
                    unreachable!()
                };
                match result {
                    Ok(signed) => {
                        signing.block.header.signature = signed.header.signature;
                        if signing.block.header != signed.header || signed.signer != signing.signer
                        {
                            self.completion = Some(Completion::Discarded {
                                parent: signing.parent,
                                result: Ok(signed),
                            });
                            return Err(Error::KeyReply);
                        }
                        self.signed = Some(Signed {
                            block: signing.block,
                            signer: signed.signer,
                            parent: signing.parent,
                            epoch: signing.epoch,
                        });
                    }
                    Err(error) => {
                        self.status.discarded = self.status.discarded.saturating_add(1);
                        self.completion = Some(Completion::Discarded {
                            parent: signing.parent,
                            result: Err(error),
                        });
                    }
                }
            }
            Pending::Admit { ticket, .. } => {
                let Some(result) = ticket.try_result() else {
                    return Ok(());
                };
                let Some(Pending::Admit { parent, hash, .. }) = self.pending.take() else {
                    unreachable!()
                };
                self.completion = Some(Completion::Admitted {
                    parent,
                    hash,
                    result,
                });
            }
        }
        Ok(())
    }
    fn authorized(&self, s: &State) -> bool {
        !s.shutdown
            && s.status.activated
            && s.status.request_revision == self.status.applied_revision
    }
    fn act(&mut self, wall: u64, now: Instant, keys: keys::PublicKeys) -> Result<(), Error> {
        if let Some(expected) = self.promote {
            if expected == self.selected.miner && keys.candidate == expected {
                let shared = self.shared.clone();
                let s = lock(&shared.state);
                // Native key promotion precedes preparation even while PoW
                // production is stopped. It remains owned by this producer.
                if !self.authorized(&s) {
                    return Ok(());
                }
                match self.keys.promote_candidate(expected) {
                    Ok(ticket) => {
                        self.promote = None;
                        self.pending = Some(Pending::Promote { expected, ticket });
                    }
                    Err(keys::Error::Busy) => (),
                    Err(e) => return Err(Error::Keys(e)),
                }
                return Ok(());
            }
            self.promote = None;
        }
        if !self.status.leader || self.generator_pending.is_some() {
            return Ok(());
        }
        let sleeping = self.sleep.is_some_and(|at| now < at);
        if let Some(signed) = &self.signed {
            if !self.current(signed.parent, signed.epoch, signed.signer) {
                return Ok(());
            }
            if sleeping {
                return Ok(());
            }
            let block = Block::Micro(signed.block.as_ref().clone());
            let bytes = block.encode().map_err(Error::Block)?;
            let hash = block.hash().map_err(|_| Error::Binding)?;
            let parent = signed.parent;
            let shared = self.shared.clone();
            let s = lock(&shared.state);
            if !self.authorized(&s) {
                return Ok(());
            }
            let result = if self.config.strictly_follow_top {
                self.importer.import_created_block_at(bytes, wall, parent)
            } else {
                self.importer.import_created_block(bytes, wall)
            };
            match result {
                Ok(ticket) => {
                    self.signed = None;
                    self.pending = Some(Pending::Admit {
                        parent,
                        hash,
                        ticket,
                    });
                    self.status.admissions = self.status.admissions.saturating_add(1);
                }
                Err(chain::Error::Busy) => (),
                Err(e) => return Err(Error::Admission(e)),
            }
            return Ok(());
        }
        let present = self.ready.is_some();
        let fresh = self.ready.as_ref().is_some_and(|c| {
            c.prepared()
                .value
                .candidate
                .block()
                .header
                .common
                .previous_hash
                == self.selected.top.0
        });
        match policy::signing(self.status.leader, present, fresh, sleeping) {
            policy::Decision::Clear => self.ready = None,
            policy::Decision::Ignore | policy::Decision::Wait => (),
            policy::Decision::Sign => {
                let block = Box::new(
                    self.ready
                        .as_ref()
                        .expect("candidate")
                        .prepared()
                        .value
                        .candidate
                        .block()
                        .clone(),
                );
                let shared = self.shared.clone();
                let s = lock(&shared.state);
                if !self.authorized(&s) {
                    return Ok(());
                }
                match self
                    .keys
                    .sign_micro_for(self.selected.miner, block.header.clone())
                {
                    Ok(ticket) => {
                        self.pending = Some(Pending::Sign(Signing {
                            block,
                            signer: self.selected.miner,
                            parent: self.selected.top,
                            epoch: self.epoch,
                            ticket,
                        }));
                        self.ready = None;
                        self.status.signatures = self.status.signatures.saturating_add(1);
                    }
                    Err(keys::Error::Busy) => (),
                    Err(e) => return Err(Error::Keys(e)),
                }
            }
        }
        Ok(())
    }
    fn emit(&mut self) -> Result<(), Error> {
        if self.completion.is_none() {
            return Ok(());
        }
        let credit = {
            let mut used = lock(&self.shared.credits);
            if *used >= self.shared.limit {
                return Ok(());
            }
            *used += 1;
            Credit(self.shared.credits.clone())
        };
        let mut s = lock(&self.shared.state);
        if s.shutdown {
            return Err(Error::Closed);
        }
        self.status.published = self
            .status
            .published
            .checked_add(1)
            .ok_or(Error::Sequence)?;
        s.outputs.push_back(Arc::new(Output {
            sequence: self.status.published,
            completion: self.completion.take().expect("completion"),
            _credit: credit,
        }));
        Ok(())
    }
}
