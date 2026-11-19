// aec_eoe_vote supervisor and gen_statem delivery, aeternity fa2a6d7.
// Copyright (c) 2024, Aeternity. ISC notice in repository LICENSE.
use super::{
    local_host::{self, Finalized, LocalHost, Output, SelectedTip},
    *,
};
use ae_mempool::hc_votes::{self, PushOutcome};
use ae_state::Trees;
use ae_trie::{MemoryStore, NodeReader};
use ae_types::Hash256;
use ae_vm_fate::maps::MapOrder;
use std::{
    collections::VecDeque,
    sync::{Arc, atomic::AtomicBool},
};

pub const RESTART_PERIOD_SECONDS: u64 = 30;
pub const MAXIMUM_RESTARTS: usize = 3;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub maximum_queued_events: usize,
    pub maximum_queued_bytes: usize,
    pub maximum_outputs: usize,
    pub maximum_output_bytes: usize,
    pub maximum_requests: usize,
    pub machine: Limits,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            maximum_queued_events: 4096,
            maximum_queued_bytes: 64 * 1024 * 1024,
            maximum_outputs: 4096,
            maximum_output_bytes: 64 * 1024 * 1024,
            maximum_requests: 1024,
            machine: Limits::default(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ResourceLimit,
    TimeWentBackwards,
    TimerRange,
    DuplicateRequest,
    Stopped,
    Host(local_host::Error),
    Step(StepError<local_host::Error>),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallFailure {
    Cancelled,
    WorkerExited(Mode),
    SupervisorStopped,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceOutput {
    Publication(Output),
    Finalized {
        request: u64,
        replies: Box<[Reply<Finalized>; 2]>,
    },
    CallFailed {
        request: u64,
        failure: CallFailure,
    },
    Restarted {
        mode: Mode,
        reason: StepError<local_host::Error>,
    },
    Stopped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Timer {
    due: u64,
    kind: Timeout,
}
#[derive(Clone)]
struct Pending<A, C> {
    request: u64,
    token: u64,
    view: Arc<Trees<A, C>>,
    first: Option<Reply<Finalized>>,
    mode: Mode,
}
#[derive(Clone)]
struct Queued<A, C> {
    mode: Mode,
    generation: u64,
    timer: Option<u64>,
    event: Event<Arc<Trees<A, C>>>,
    bytes: usize,
}

// The supplied clock is monotonic milliseconds. Each queued callback is one
// atomic retry boundary; earlier successful callbacks stay committed if later
// work exhausts a local budget. Cross-worker delivery is deterministic, while
// retaining per-worker FIFO order and the reference supervisor's length-first
// broadcast order. It is not a claim that Erlang processes always interleave
// in this particular order.
#[derive(Clone)]
pub struct Coordinator<A = MemoryStore, C = MemoryStore> {
    config: Config,
    host: LocalHost<A, C>,
    children: [Machine; 2],
    generations: [u64; 2],
    timers: [Option<Timer>; 2],
    timer_generations: [u64; 2],
    block_time: Unsigned,
    pending: BTreeMap<u64, Pending<A, C>>,
    token: u64,
    queue: VecDeque<Queued<A, C>>,
    queued_bytes: usize,
    outputs: VecDeque<(ServiceOutput, usize)>,
    output_bytes: usize,
    restarts: VecDeque<u64>,
    now: u64,
    stopped: bool,
}
fn index(mode: Mode) -> usize {
    match mode {
        Mode::Fork => 0,
        Mode::Length => 1,
    }
}
impl<A: NodeReader + Clone, C: NodeReader + Clone> Coordinator<A, C> {
    pub fn new(host: LocalHost<A, C>, block_time: Unsigned, config: Config) -> Self {
        let stakers = host.stakers();
        Self {
            config,
            host,
            children: [
                Machine::new(Mode::Fork, stakers.clone(), block_time.clone()),
                Machine::new(Mode::Length, stakers, block_time.clone()),
            ],
            generations: [0; 2],
            timers: [None; 2],
            timer_generations: [0; 2],
            block_time,
            pending: BTreeMap::new(),
            token: 0,
            queue: VecDeque::new(),
            queued_bytes: 0,
            outputs: VecDeque::new(),
            output_bytes: 0,
            restarts: VecDeque::new(),
            now: 0,
            stopped: false,
        }
    }
    pub fn machine(&self, mode: Mode) -> &Machine {
        &self.children[index(mode)]
    }
    pub fn host(&self) -> &LocalHost<A, C> {
        &self.host
    }
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }
    pub fn queued_events(&self) -> usize {
        self.queue.len()
    }
    pub fn pending_requests(&self) -> usize {
        self.pending.len()
    }
    pub fn now(&self) -> u64 {
        self.now
    }
    pub fn config(&self) -> Config {
        self.config
    }
    pub fn set_config(&mut self, config: Config) {
        self.config = config;
    }
    pub fn select_tip(&mut self, tip: SelectedTip<A, C>) {
        self.host.select_tip(tip);
    }
    pub fn set_gossip_cache(&mut self, hashes: BTreeSet<Hash256>) -> Result<(), Error> {
        self.host.set_gossip_cache(hashes).map_err(Error::Host)
    }
    pub fn insert_parent(&mut self, block: ParentBlock) -> Result<(), Error> {
        self.host.insert_parent(block).map_err(Error::Host)
    }
    pub fn remove_parent(&mut self, height: &Unsigned) -> Option<ParentBlock> {
        self.host.remove_parent(height)
    }
    pub fn set_parent_cache(&mut self, client: Option<crate::parent_cache::runtime::Client>) {
        self.host.set_parent_cache(client);
    }
    pub(super) fn set_shutdown(&mut self, shutdown: Arc<AtomicBool>) {
        self.host.set_shutdown(shutdown);
    }
    pub fn entropy_hash(&mut self, epoch: &Unsigned) -> Result<Option<Vec<u8>>, Error> {
        self.transact(|next| {
            let result = next.host.entropy_hash(epoch).map_err(Error::Host)?;
            next.route_outputs()?;
            Ok(result)
        })
    }
    pub fn next_deadline(&self) -> Option<u64> {
        self.timers.iter().flatten().map(|t| t.due).min()
    }
    pub fn pop_output(&mut self) -> Option<ServiceOutput> {
        let (output, bytes) = self.outputs.pop_front()?;
        self.output_bytes -= bytes;
        Some(output)
    }
    pub fn peek_output(&self) -> Option<&ServiceOutput> {
        self.outputs.front().map(|(o, _)| o)
    }
    fn transact<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        let mut next = self.clone();
        let value = operation(&mut next)?;
        *self = next;
        Ok(value)
    }
    fn clock(&mut self, now: u64) -> Result<(), Error> {
        if now < self.now {
            Err(Error::TimeWentBackwards)
        } else {
            self.now = now;
            Ok(())
        }
    }
    pub fn negotiate(&mut self, n: Negotiation) -> Result<(), Error> {
        self.transact(|next| next.broadcast(Event::Negotiate(n)))
    }
    pub fn negotiate_child(&mut self, mode: Mode, n: Negotiation) -> Result<(), Error> {
        self.transact(|next| next.enqueue(mode, Event::Negotiate(n)))
    }
    pub fn notify_parent(&mut self, epoch: Unsigned, block: ParentBlock) -> Result<(), Error> {
        self.transact(|next| next.broadcast(Event::ParentBlock { epoch, block }))
    }
    pub fn new_epoch(&mut self, epoch: Unsigned) -> Result<(), Error> {
        self.transact(|next| {
            next.host.update_epoch(epoch);
            next.broadcast(Event::NewEpoch)
        })
    }
    pub fn admit(
        &mut self,
        signed: SignedTransaction<Transaction>,
        event: hc_votes::Event,
    ) -> Result<PushOutcome, Error> {
        self.transact(|next| {
            let result = next.host.admit(signed, event).map_err(Error::Host)?;
            next.route_outputs()?;
            Ok(result)
        })
    }
    // Fork is requested first. Its successful signed result reserves nonce 1;
    // only then is length called with offset 2 (otherwise offset 1). The view
    // passed to both public calls stays the original caller's selected view.
    pub fn request_finalize(
        &mut self,
        request: u64,
        view: Arc<Trees<A, C>>,
        now: u64,
    ) -> Result<(), Error> {
        self.transact(|next| {
            next.clock(now)?;
            if next.pending.contains_key(&request) {
                return Err(Error::DuplicateRequest);
            }
            if next.pending.len() >= next.config.maximum_requests {
                return Err(Error::ResourceLimit);
            }
            let token = next.new_token()?;
            next.pending.insert(
                request,
                Pending {
                    request,
                    token,
                    view: view.clone(),
                    first: None,
                    mode: Mode::Fork,
                },
            );
            next.enqueue(
                Mode::Fork,
                Event::GetFinalize {
                    request: token,
                    view,
                    nonce_offset: 1.into(),
                },
            )
        })
    }
    // gen_statem:call/2 has an infinite default timeout. Cancellation is an
    // explicit local caller action; it does not rewrite the worker's saved
    // request or nonce, and a late reply cannot complete a reused request ID.
    pub fn cancel_request(&mut self, request: u64) -> Result<(), Error> {
        self.transact(|next| {
            if next.pending.remove(&request).is_some() {
                next.output(ServiceOutput::CallFailed {
                    request,
                    failure: CallFailure::Cancelled,
                })?;
            }
            Ok(())
        })
    }
    fn new_token(&mut self) -> Result<u64, Error> {
        self.token = self.token.checked_add(1).ok_or(Error::ResourceLimit)?;
        Ok(self.token)
    }
    fn broadcast(&mut self, event: Event<Arc<Trees<A, C>>>) -> Result<(), Error> {
        self.enqueue(Mode::Length, event.clone())?;
        self.enqueue(Mode::Fork, event)
    }
    fn enqueue(&mut self, mode: Mode, event: Event<Arc<Trees<A, C>>>) -> Result<(), Error> {
        let bytes =
            event_size(&event, self.config.machine).map_err(|e| Error::Step(StepError::Core(e)))?;
        let total = self
            .queued_bytes
            .checked_add(bytes)
            .ok_or(Error::ResourceLimit)?;
        if self.queue.len() >= self.config.maximum_queued_events
            || total > self.config.maximum_queued_bytes
        {
            return Err(Error::ResourceLimit);
        }
        self.queue.push_back(Queued {
            mode,
            generation: self.generations[index(mode)],
            timer: None,
            event,
            bytes,
        });
        self.queued_bytes = total;
        Ok(())
    }
    fn output(&mut self, output: ServiceOutput) -> Result<(), Error> {
        let bytes = output_size(&output);
        let total = self
            .output_bytes
            .checked_add(bytes)
            .ok_or(Error::ResourceLimit)?;
        if self.outputs.len() >= self.config.maximum_outputs
            || total > self.config.maximum_output_bytes
        {
            return Err(Error::ResourceLimit);
        }
        self.outputs.push_back((output, bytes));
        self.output_bytes = total;
        Ok(())
    }
    fn route_outputs(&mut self) -> Result<(), Error> {
        for output in self.host.drain_outbox() {
            match &output {
                Output::Vote { event, signed } => self.broadcast(Event::Transaction {
                    received: *event == hc_votes::Event::Received,
                    signed: signed.clone(),
                })?,
                Output::ParentBlock { epoch, block } => self.broadcast(Event::ParentBlock {
                    epoch: epoch.clone(),
                    block: block.clone(),
                })?,
            }
            self.output(ServiceOutput::Publication(output))?;
        }
        Ok(())
    }
    // Timer expiry enters the same FIFO as incoming messages. A replaced or
    // consumed timer is not re-enqueued. Native restarts discard the old
    // worker mailbox using generations; the sibling's queued work survives.
    pub fn advance(&mut self, now: u64) -> Result<(), Error> {
        self.transact(|next| {
            next.clock(now)?;
            for mode in [Mode::Length, Mode::Fork] {
                let i = index(mode);
                if let Some(timer) = next.timers[i]
                    && timer.due <= now
                {
                    next.enqueue(mode, Event::Timeout(timer.kind))?;
                    next.queue.back_mut().unwrap().timer = Some(next.timer_generations[i]);
                    next.timers[i] = None;
                }
            }
            Ok(())
        })
    }
    // false means idle. Operational failure leaves this exact callback at the
    // queue head, including all state, pool and effects, for an explicit retry.
    pub fn step(&mut self, order: &impl MapOrder) -> Result<bool, Error> {
        let now = self.now;
        self.timed_step(order, || now)
    }
    // OTP starts relative timers when applying returned callback actions.
    // Signing and selected-state reads must not shorten the new interval.
    pub(super) fn timed_step(
        &mut self,
        order: &impl MapOrder,
        action_time: impl FnOnce() -> u64,
    ) -> Result<bool, Error> {
        if self.queue.is_empty() {
            return Ok(false);
        }
        self.transact(|next| {
            let queued = next.queue.pop_front().unwrap();
            next.queued_bytes -= queued.bytes;
            let i = index(queued.mode);
            if queued.generation != next.generations[i] {
                return Ok(true);
            }
            if queued.timer.is_some_and(|t| t != next.timer_generations[i]) {
                return Ok(true);
            }
            let old_state = next.children[i].state();
            let result =
                next.children[i].step(queued.event, &mut next.host, order, next.config.machine);
            next.clock(action_time())?;
            match result {
                Ok(t) => {
                    let mut zero_timeout = None;
                    if next.children[i].state() != old_state {
                        next.cancel_timer(i)?;
                    }
                    if let Some((duration, kind)) = t.timeout {
                        next.cancel_timer(i)?;
                        let duration = duration.to_u64().map_err(|_| Error::TimerRange)?;
                        next.timers[i] = Some(Timer {
                            due: next.now.checked_add(duration).ok_or(Error::TimerRange)?,
                            kind,
                        });
                        if duration == 0 {
                            zero_timeout = Some(kind);
                        }
                    }
                    for (token, reply) in t.replies {
                        next.reply(queued.mode, token, reply)?;
                    }
                    next.route_outputs()?;
                    if let Some(kind) = zero_timeout {
                        // OTP zero state timeouts run before the worker reads
                        // its external mailbox, including its own vote events.
                        next.enqueue(queued.mode, Event::Timeout(kind))?;
                        let mut timer = next.queue.pop_back().unwrap();
                        timer.timer = Some(next.timer_generations[i]);
                        next.queue.push_front(timer);
                        next.timers[i] = None;
                    }
                }
                Err(error) => {
                    let native = match &error {
                        StepError::Core(Failure::Native(_)) => true,
                        StepError::Host(e) => !e.is_operational(),
                        _ => false,
                    };
                    if !native {
                        return Err(Error::Step(error));
                    }
                    // Publish preserved pre-exception effects while their
                    // destinations still refer to the old worker generation.
                    next.route_outputs()?;
                    next.restart(queued.mode, error)?;
                }
            }
            Ok(true)
        })
    }
    fn cancel_timer(&mut self, i: usize) -> Result<(), Error> {
        self.timers[i] = None;
        self.timer_generations[i] = self.timer_generations[i]
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    pub fn run(&mut self, order: &impl MapOrder, maximum_steps: usize) -> Result<usize, Error> {
        let mut steps = 0;
        while steps < maximum_steps && !self.queue.is_empty() && !self.stopped {
            self.step(order)?;
            steps += 1;
        }
        Ok(steps)
    }
    fn reply(&mut self, mode: Mode, token: u64, reply: Reply<Finalized>) -> Result<(), Error> {
        let id = self
            .pending
            .iter()
            .find(|(_, p)| p.token == token && p.mode == mode)
            .map(|(id, _)| *id);
        let Some(id) = id else { return Ok(()) }; // canceled caller or superseded reply
        let mut pending = self.pending.remove(&id).unwrap();
        if mode == Mode::Fork {
            let success = matches!(&reply, Reply::Finalized(Finalized::Signed(_)));
            let token = self.new_token()?;
            pending.token = token;
            pending.mode = Mode::Length;
            pending.first = Some(reply);
            self.enqueue(
                Mode::Length,
                Event::GetFinalize {
                    request: token,
                    view: pending.view.clone(),
                    nonce_offset: finalization::length_nonce_offset(success),
                },
            )?;
            self.pending.insert(id, pending);
        } else {
            self.output(ServiceOutput::Finalized {
                request: pending.request,
                replies: Box::new([pending.first.expect("fork replied first"), reply]),
            })?;
        }
        Ok(())
    }
    fn restart(&mut self, mode: Mode, reason: StepError<local_host::Error>) -> Result<(), Error> {
        let i = index(mode);
        self.generations[i] = self.generations[i]
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        self.cancel_timer(i)?;
        self.children[i] = Machine::new(mode, self.host.stakers(), self.block_time.clone());
        let failed: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| p.mode == mode)
            .map(|(id, _)| *id)
            .collect();
        for id in failed {
            self.pending.remove(&id);
            self.output(ServiceOutput::CallFailed {
                request: id,
                failure: CallFailure::WorkerExited(mode),
            })?;
        }
        let now = self.now / 1000;
        self.restarts.retain(|&t| now - t <= RESTART_PERIOD_SECONDS);
        self.restarts.push_back(now);
        if self.restarts.len() > MAXIMUM_RESTARTS {
            self.stopped = true;
            self.timers = [None; 2];
            self.queue.clear();
            self.queued_bytes = 0;
            let pending = std::mem::take(&mut self.pending);
            for id in pending.into_keys() {
                self.output(ServiceOutput::CallFailed {
                    request: id,
                    failure: CallFailure::SupervisorStopped,
                })?;
            }
            self.output(ServiceOutput::Stopped)?;
        } else {
            self.output(ServiceOutput::Restarted { mode, reason })?;
        }
        Ok(())
    }
}
fn event_size<V>(event: &Event<V>, limits: Limits) -> Result<usize, Failure> {
    let mut budget = Budget::new(limits);
    budget.add(std::mem::size_of::<Event<V>>())?;
    match event {
        Event::Negotiate(n) => {
            if n.validators.len() > limits.maximum_validators {
                return Err(Failure::ResourceLimit);
            }
            for n in [&n.epoch, &n.height, &n.current_length] {
                budget.add(n.as_bytes().len())?;
            }
            budget.add(n.hash.len())?;
            budget.add(n.seed.as_ref().map_or(0, Vec::len))?;
            for (_, stake) in &n.validators {
                budget.add(32)?;
                budget.add(stake.as_bytes().len())?;
            }
        }
        Event::Transaction { signed, .. } => budget.signed(signed)?,
        Event::ParentBlock { epoch, block } => {
            budget.add(epoch.as_bytes().len())?;
            budget.parent(block)?;
        }
        Event::GetFinalize { nonce_offset, .. } => budget.add(nonce_offset.as_bytes().len())?,
        _ => {}
    }
    Ok(budget.bytes)
}
fn output_size(output: &ServiceOutput) -> usize {
    let base = std::mem::size_of::<ServiceOutput>();
    let extra = match output {
        ServiceOutput::Publication(Output::Vote { signed, .. }) => signed.encode().len(),
        ServiceOutput::Publication(Output::ParentBlock { epoch, block }) => [
            epoch.as_bytes().len(),
            block.hash.len(),
            block.previous_hash.len(),
            block.height.as_bytes().len(),
            block.time.as_bytes().len(),
        ]
        .into_iter()
        .fold(0usize, usize::saturating_add),
        ServiceOutput::Finalized { replies, .. } => replies.iter().fold(0usize, |n, r| {
            n.saturating_add(match r {
                Reply::Finalized(Finalized::Signed(s)) => s.encode().len(),
                _ => 0,
            })
        }),
        ServiceOutput::Restarted { reason, .. } => format!("{reason:?}").len(),
        _ => 0,
    };
    base.saturating_add(extra)
}
