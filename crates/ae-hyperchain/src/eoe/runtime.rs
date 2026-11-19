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
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub command_capacity: usize,
    pub output_capacity: usize,
    pub batch_steps: usize,
    pub maximum_command_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            command_capacity: 64,
            output_capacity: 64,
            batch_steps: 64,
            maximum_command_bytes: 16 * 1024 * 1024,
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
    SelectTip(SelectedTip<A, C>),
    GossipCache(BTreeSet<Hash256>),
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
struct Request<A, C> {
    sequence: u64,
    command: Command<A, C>,
}
pub struct Runtime<A = MemoryStore, C = MemoryStore> {
    sender: Option<SyncSender<Request<A, C>>>,
    receiver: Option<Receiver<Output>>,
    worker: Option<JoinHandle<()>>,
    maximum_command_bytes: usize,
    stop: Arc<AtomicBool>,
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
        let worker = thread::Builder::new()
            .name("ae-hc-eoe".into())
            .spawn(move || run(coordinator, order, config, commands, outputs, worker_stop))?;
        Ok(Self {
            sender: Some(sender),
            receiver: Some(receiver),
            worker: Some(worker),
            maximum_command_bytes: config.maximum_command_bytes,
            stop,
        })
    }
}
impl<A, C> Runtime<A, C> {
    // Nonblocking submission returns ownership on pressure or disconnection.
    pub fn try_submit(
        &self,
        sequence: u64,
        command: Command<A, C>,
    ) -> Result<(), SubmitError<A, C>> {
        if command_size(&command, self.maximum_command_bytes).is_err() {
            return Err(SubmitError::ResourceLimit(Box::new(command)));
        }
        let Some(sender) = &self.sender else {
            return Err(SubmitError::Disconnected(Box::new(command)));
        };
        sender
            .try_send(Request { sequence, command })
            .map_err(|e| match e {
                TrySendError::Full(r) => SubmitError::Full(Box::new(r.command)),
                TrySendError::Disconnected(r) => SubmitError::Disconnected(Box::new(r.command)),
            })
    }
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Output, RecvTimeoutError> {
        self.receiver
            .as_ref()
            .ok_or(RecvTimeoutError::Disconnected)?
            .recv_timeout(timeout)
    }
    pub fn shutdown(mut self) -> thread::Result<()> {
        self.stop.store(true, Ordering::Release);
        // Dropping the receive side releases a worker blocked by output
        // backpressure. Dropping the sender releases its idle receive wait.
        self.receiver.take();
        self.sender.take();
        self.worker.take().expect("live worker").join()
    }
}
impl<A, C> Drop for Runtime<A, C> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
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
        _ => {}
    };
    Ok(())
}
fn run<A: NodeReader + Clone, C: NodeReader + Clone>(
    mut service: Coordinator<A, C>,
    order: impl MapOrder,
    config: Config,
    commands: Receiver<Request<A, C>>,
    outputs: SyncSender<Output>,
    stop: Arc<AtomicBool>,
) {
    let started = Instant::now();
    let start_ms = service.now();
    let mut blocked = false;
    loop {
        if stop.load(Ordering::Acquire) {
            return;
        }
        while let Some(output) = service.pop_output() {
            if outputs.send(Output::Service(output)).is_err() {
                return;
            }
        }
        if service.is_stopped() {
            return;
        }
        let now = start_ms
            .saturating_add(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
        if let Err(error) = service.advance(now) {
            if !blocked && outputs.send(Output::WorkStopped(error)).is_err() {
                return;
            }
            blocked = true;
        }
        while let Some(output) = service.pop_output() {
            if outputs.send(Output::Service(output)).is_err() {
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
        match commands.recv_timeout(wait) {
            Ok(Request { sequence, command }) => {
                let now = start_ms.saturating_add(
                    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                );
                let stop = matches!(command, Command::Stop);
                let retry = matches!(command, Command::Retry | Command::Configure(_));
                let result = service
                    .advance(now)
                    .and_then(|()| apply(&mut service, command, now));
                if retry && result.is_ok() {
                    blocked = false;
                }
                if outputs.send(Output::Applied { sequence, result }).is_err() || stop {
                    return;
                }
            }
            Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {}
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
                    if outputs.send(Output::WorkStopped(error)).is_err() {
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
        Command::SelectTip(tip) => service.select_tip(tip),
        Command::GossipCache(hashes) => service.set_gossip_cache(hashes)?,
        Command::Finalize { request, view } => service.request_finalize(request, view, now)?,
        Command::CancelRequest(request) => service.cancel_request(request)?,
        Command::Configure(config) => service.set_config(config),
        Command::Retry | Command::Stop => {}
    };
    Ok(Applied::Unit)
}
