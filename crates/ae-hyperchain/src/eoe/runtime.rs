// Bounded local event loop for the EOE coordinator. FSM computations stay in
// Machine; this adapter supplies monotonic timer time and cache cancellation.
use super::{
    coordinator::{self, Coordinator, ServiceOutput},
    local_host::SelectedTip,
    *,
};
use ae_mempool::hc_votes::{self, PushOutcome};
use ae_state::Trees;
use ae_trie::{MemoryStore, NodeReader};
use ae_types::Hash256;
use ae_vm_fate::maps::MapOrder;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

mod client;
pub use client::{Call, CallError, Client};
mod lifecycle;
pub use lifecycle::{Health, Info, Retired, Submission};
mod route;
use lifecycle::{Pending, WorkerExit};
pub use route::Route;
mod votes;
pub use votes::{VoteQuery, VoteSnapshot};

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub command_capacity: usize,
    pub output_capacity: usize,
    pub batch_steps: usize,
    pub maximum_command_bytes: usize,
    pub maximum_vote_reply_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            command_capacity: 64,
            output_capacity: 64,
            batch_steps: 64,
            maximum_command_bytes: 16 * 1024 * 1024,
            maximum_vote_reply_bytes: 64 * 1024 * 1024,
        }
    }
}
pub enum Command<A = MemoryStore, C = MemoryStore> {
    Negotiate(Negotiation),
    Admit {
        signed: Box<SignedTransaction<Transaction>>,
        event: hc_votes::Event,
    },
    NotifyParent {
        epoch: Unsigned,
        block: ParentBlock,
    },
    CacheParent(ParentBlock),
    ParentCache(Option<crate::parent_cache::runtime::Client>),
    Entropy(Unsigned),
    ForgetParent(Unsigned),
    NewEpoch(Unsigned),
    Votes(VoteQuery),
    SelectTip(SelectedTip<A, C>),
    GossipCache(BTreeSet<Hash256>),
    LiveGossipCache(Option<Arc<dyn ae_mempool::gossip::Transactional>>),
    Finalize {
        request: u64,
        view: Arc<Trees<A, C>>,
    },
    CancelRequest(u64),
    Configure(coordinator::Config),
    Retry,
    Stop,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied {
    Unit,
    Admission(PushOutcome),
    Entropy(Option<Vec<u8>>),
    Votes(VoteSnapshot),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    Applied {
        sequence: u64,
        result: Result<Applied, coordinator::Error>,
    },
    Service(ServiceOutput),
    // An operational callback failed. Inputs remain queued. Resolve the
    // underlying condition and issue Retry; no automatic busy retry loop.
    WorkStopped(coordinator::Error),
}
pub enum SubmitError<A = MemoryStore, C = MemoryStore> {
    Full(Box<Command<A, C>>),
    Disconnected(Box<Command<A, C>>),
    ResourceLimit(Box<Command<A, C>>),
}
enum ReplyTo {
    Stream(u64),
    Caller {
        reply: SyncSender<Result<Applied, coordinator::Error>>,
        _credit: Arc<client::Credit>,
    },
}
struct Request<A, C> {
    reply: ReplyTo,
    command: Command<A, C>,
}
pub struct Runtime<A = MemoryStore, C = MemoryStore> {
    sender: Option<SyncSender<Request<A, C>>>,
    receiver: Option<Receiver<Output>>,
    worker: Option<JoinHandle<WorkerExit>>,
    maximum_command_bytes: usize,
    stop: Arc<AtomicBool>,
    control: Arc<client::Control>,
}
impl<A: NodeReader + Clone + Send + Sync + 'static, C: NodeReader + Clone + Send + Sync + 'static>
    Runtime<A, C>
{
    pub fn spawn(
        mut coordinator: Coordinator<A, C>,
        order: impl MapOrder + Send + 'static,
        config: Config,
    ) -> io::Result<Self> {
        if config.command_capacity == 0
            || config.output_capacity == 0
            || config.batch_steps == 0
            || config.maximum_command_bytes == 0
            || config.maximum_vote_reply_bytes == 0
            || config.maximum_vote_reply_bytes > 1 << 30
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "EOE runtime limits must be positive",
            ));
        }
        let (sender, commands) = mpsc::sync_channel(config.command_capacity);
        let (outputs, receiver) = mpsc::sync_channel(config.output_capacity);
        let stop = Arc::new(AtomicBool::new(false));
        coordinator.set_shutdown(stop.clone());
        let worker_stop = stop.clone();
        let control = Arc::new(client::Control::new(
            stop.clone(),
            config,
            coordinator.host().network().clone(),
        ));
        let mut inbox = coordinator
            .subscribe(control.wake.clone())
            .map_err(|e| io::Error::other(format!("EOE subscription: {e:?}")))?;
        let worker_control = control.clone();
        let worker = thread::Builder::new()
            .name("ae-hc-eoe".into())
            .spawn(move || {
                struct End(Arc<client::Control>);
                impl Drop for End {
                    fn drop(&mut self) {
                        self.0.alive.store(false, Ordering::Release);
                    }
                }
                let replies = worker_control.vote_replies.clone();
                let _end = End(worker_control.clone());
                let mut pending = Pending::default();
                let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(
                        &mut coordinator,
                        order,
                        config,
                        Driver {
                            commands: &commands,
                            outputs,
                            stop: worker_stop,
                            replies,
                            control: &worker_control,
                            pending: &mut pending,
                            inbox: &mut inbox,
                        },
                    )
                }))
                .err();
                let _gate = worker_control.admission.lock().expect("EOE admission");
                worker_control.stop.store(true, Ordering::Release);
                worker_control.alive.store(false, Ordering::Release);
                let queued = commands.try_iter().map(|r| r.reply.submission()).collect();
                let mut retained = pending.output.into_iter().collect::<Vec<_>>();
                while let Some(output) = coordinator.pop_output() {
                    retained.push(Output::Service(output));
                }
                let mut health = worker_control.health.lock().expect("EOE health");
                if !matches!(*health, Health::WorkStopped(_)) {
                    *health = Health::Stopped;
                }
                WorkerExit {
                    state: coordinator.snapshot(),
                    inputs: inbox.take().map(pool::Inbox::retire),
                    outputs: retained,
                    queued,
                    interrupted: pending.command,
                    panic,
                }
            })?;
        Ok(Self {
            sender: Some(sender),
            receiver: Some(receiver),
            worker: Some(worker),
            maximum_command_bytes: config.maximum_command_bytes,
            stop,
            control,
        })
    }
}
impl<A, C> Runtime<A, C> {
    pub fn client(&self) -> Client<A, C> {
        Client {
            sender: self.sender.as_ref().expect("live runtime").clone(),
            control: self.control.clone(),
        }
    }
    // Nonblocking submission returns ownership on pressure or disconnection.
    pub fn try_submit(
        &self,
        sequence: u64,
        command: Command<A, C>,
    ) -> Result<(), SubmitError<A, C>> {
        let _gate = self.control.admission.lock().expect("EOE admission");
        if !self.control.alive.load(Ordering::Acquire) || self.stop.load(Ordering::Acquire) {
            return Err(SubmitError::Disconnected(Box::new(command)));
        }
        if command_size(&command, self.maximum_command_bytes).is_err() {
            return Err(SubmitError::ResourceLimit(Box::new(command)));
        }
        let Some(sender) = &self.sender else {
            return Err(SubmitError::Disconnected(Box::new(command)));
        };
        sender
            .try_send(Request {
                reply: ReplyTo::Stream(sequence),
                command,
            })
            .map_err(|e| match e {
                TrySendError::Full(r) => SubmitError::Full(Box::new(r.command)),
                TrySendError::Disconnected(r) => SubmitError::Disconnected(Box::new(r.command)),
            })?;
        self.control.wake.notify();
        Ok(())
    }
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Output, RecvTimeoutError> {
        self.receiver
            .as_ref()
            .ok_or(RecvTimeoutError::Disconnected)?
            .recv_timeout(timeout)
    }
    pub fn shutdown(mut self) -> thread::Result<()> {
        self.client().request_stop();
        // Dropping the receive side releases a worker blocked by output
        // backpressure. Dropping the sender releases its idle receive wait.
        self.receiver.take();
        self.sender.take();
        let exit = self.worker.take().expect("live worker").join()?;
        match exit.panic {
            Some(panic) => Err(panic),
            None => Ok(()),
        }
    }
}
impl<A, C> Drop for Runtime<A, C> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.control.wake.notify();
        self.receiver.take();
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
fn command_size<A, C>(command: &Command<A, C>, maximum: usize) -> Result<(), Failure> {
    let mut budget = Budget::new(Limits {
        maximum_bytes: maximum,
        ..Default::default()
    });
    budget.add(std::mem::size_of::<Command<A, C>>())?;
    match command {
        Command::Negotiate(n) => {
            for v in [&n.epoch, &n.height, &n.current_length] {
                budget.add(v.as_bytes().len())?;
            }
            budget.add(n.hash.len())?;
            budget.add(n.seed.as_ref().map_or(0, Vec::len))?;
            for (_, stake) in &n.validators {
                budget.add(32)?;
                budget.add(stake.as_bytes().len())?;
            }
        }
        Command::Admit { signed, .. } => budget.signed(signed)?,
        Command::NotifyParent { epoch, block } => {
            budget.add(epoch.as_bytes().len())?;
            budget.parent(block)?;
        }
        Command::CacheParent(block) => budget.parent(block)?,
        Command::ForgetParent(n) | Command::NewEpoch(n) | Command::Entropy(n) => {
            budget.add(n.as_bytes().len())?
        }
        Command::GossipCache(hashes) => budget.add(hashes.len().saturating_mul(32))?,
        Command::Votes(query) => {
            budget.add(query.epoch.as_bytes().len())?;
            budget.add(query.kind.as_ref().map_or(0, |n| n.as_bytes().len()))?;
        }
        _ => {}
    };
    Ok(())
}
struct Driver<'a, A, C> {
    commands: &'a Receiver<Request<A, C>>,
    outputs: SyncSender<Output>,
    stop: Arc<AtomicBool>,
    replies: Arc<votes::Budget>,
    control: &'a client::Control,
    pending: &'a mut Pending,
    inbox: &'a mut Option<pool::Inbox>,
}
fn run<A: NodeReader + Clone, C: NodeReader + Clone>(
    service: &mut Coordinator<A, C>,
    order: impl MapOrder,
    config: Config,
    driver: Driver<'_, A, C>,
) {
    let Driver {
        commands,
        outputs,
        stop,
        replies,
        control,
        pending,
        inbox,
    } = driver;
    let started = Instant::now();
    let start_ms = service.now();
    let mut blocked = false;
    loop {
        if stop.load(Ordering::Acquire) {
            return;
        }
        while let Some(output) = service.pop_output() {
            if !pending.send(Output::Service(output), &outputs, &stop) {
                return;
            }
        }
        if service.is_stopped() {
            return;
        }
        let now = start_ms
            .saturating_add(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
        if let Err(error) = service.advance(now) {
            *control.health.lock().expect("EOE health") = Health::WorkStopped(error.clone());
            if !blocked && !pending.send(Output::WorkStopped(error), &outputs, &stop) {
                return;
            }
            blocked = true;
        }
        while let Some(output) = service.pop_output() {
            if !pending.send(Output::Service(output), &outputs, &stop) {
                return;
            }
        }
        let wait = if blocked {
            Duration::from_millis(1000)
        } else if service.queued_events() > 0 {
            Duration::ZERO
        } else {
            Duration::from_millis(
                service
                    .next_deadline()
                    .map_or(1000, |due| due.saturating_sub(now).min(1000)),
            )
        };
        match commands.try_recv() {
            Ok(Request { reply, command }) => {
                pending.command = Some(reply.submission());
                let now = start_ms.saturating_add(
                    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                );
                let command_stop = matches!(command, Command::Stop);
                let retry = matches!(command, Command::Retry | Command::Configure(_));
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    service
                        .advance(now)
                        .and_then(|()| apply(service, command, now, &replies))
                }))
                .unwrap_or_else(|panic| {
                    // Publish closure before unwinding drops the private
                    // receipt. Its caller can then classify a lost reply
                    // against this exact generation without a race.
                    control.stop.store(true, Ordering::Release);
                    std::panic::resume_unwind(panic)
                });
                if retry && result.is_ok() {
                    blocked = false;
                    *control.health.lock().expect("EOE health") = Health::Running;
                }
                pending.command = None;
                let disconnected = match reply {
                    ReplyTo::Stream(sequence) => {
                        !pending.send(Output::Applied { sequence, result }, &outputs, &stop)
                    }
                    ReplyTo::Caller { reply, .. } => {
                        // The one-slot private reply cannot block on an unread
                        // completion. Losing a caller does not stop the owner.
                        let _ = reply.try_send(result);
                        false
                    }
                };
                if disconnected || command_stop {
                    return;
                }
            }
            Err(TryRecvError::Disconnected) => return,
            Err(TryRecvError::Empty) => {
                let input_ready = !blocked && inbox.as_ref().is_some_and(|i| i.peek().is_some());
                if !input_ready && !wait.is_zero() {
                    control.wake.wait(wait);
                    continue;
                }
            }
        }
        if !blocked
            && let Some(inbox) = inbox.as_mut()
            && let Some(input) = inbox.peek()
        {
            match service.receive(&input) {
                Ok(()) => {
                    inbox.acknowledge();
                }
                Err(error) => {
                    blocked = true;
                    *control.health.lock().expect("EOE health") =
                        Health::WorkStopped(error.clone());
                    if !pending.send(Output::WorkStopped(error), &outputs, &stop) {
                        return;
                    }
                }
            }
        }
        if !blocked {
            for _ in 0..config.batch_steps {
                if service.queued_events() == 0
                    || service.is_stopped()
                    || stop.load(Ordering::Acquire)
                {
                    break;
                }
                let clock = || {
                    start_ms.saturating_add(
                        u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    )
                };
                let result = service
                    .advance(clock())
                    .and_then(|()| service.timed_step(&order, clock));
                if let Err(error) = result {
                    blocked = true;
                    *control.health.lock().expect("EOE health") =
                        Health::WorkStopped(error.clone());
                    if !pending.send(Output::WorkStopped(error), &outputs, &stop) {
                        return;
                    }
                    break;
                }
            }
        }
    }
}
fn apply<A: NodeReader + Clone, C: NodeReader + Clone>(
    service: &mut Coordinator<A, C>,
    command: Command<A, C>,
    now: u64,
    replies: &Arc<votes::Budget>,
) -> Result<Applied, coordinator::Error> {
    match command {
        Command::Negotiate(n) => service.negotiate(n)?,
        Command::Admit { signed, event } => {
            return service.admit(*signed, event).map(Applied::Admission);
        }
        Command::NotifyParent { epoch, block } => service.notify_parent(epoch, block)?,
        Command::CacheParent(block) => service.insert_parent(block)?,
        Command::ParentCache(client) => service.set_parent_cache(client),
        Command::Entropy(epoch) => return service.entropy_hash(&epoch).map(Applied::Entropy),
        Command::ForgetParent(height) => {
            service.remove_parent(&height);
        }
        Command::NewEpoch(epoch) => service.new_epoch(epoch)?,
        Command::Votes(query) => {
            return service
                .host()
                .inspect_pool(|pool| replies.capture(pool, query))
                .map_err(coordinator::Error::Host)?
                .map(Applied::Votes);
        }
        Command::SelectTip(tip) => service.select_tip(tip),
        Command::GossipCache(hashes) => service.set_gossip_cache(hashes)?,
        Command::LiveGossipCache(cache) => service.set_live_gossip_cache(cache),
        Command::Finalize { request, view } => service.request_finalize(request, view, now)?,
        Command::CancelRequest(request) => service.cancel_request(request)?,
        Command::Configure(config) => service.set_config(config),
        Command::Retry | Command::Stop => {}
    };
    Ok(Applied::Unit)
}
