use super::*;
use std::sync::{atomic::AtomicUsize, mpsc::TryRecvError};

pub(super) struct Control {
    pub admission: std::sync::Mutex<()>,
    pub health: std::sync::Mutex<Health>,
    pub alive: AtomicBool,
    pub stop: Arc<AtomicBool>,
    calls: AtomicUsize,
    maximum_calls: usize,
    maximum_command_bytes: usize,
    pub vote_replies: Arc<votes::Budget>,
    network: ae_protocol::NetworkProfile,
}
impl Control {
    pub fn new(
        stop: Arc<AtomicBool>,
        config: Config,
        network: ae_protocol::NetworkProfile,
    ) -> Self {
        Self {
            admission: std::sync::Mutex::new(()),
            health: std::sync::Mutex::new(Health::Running),
            alive: AtomicBool::new(true),
            stop,
            calls: AtomicUsize::new(0),
            maximum_calls: config.command_capacity,
            maximum_command_bytes: config.maximum_command_bytes,
            vote_replies: Arc::new(votes::Budget::new(config.maximum_vote_reply_bytes)),
            network,
        }
    }
}
pub(super) struct Credit(Arc<Control>);
impl Drop for Credit {
    fn drop(&mut self) {
        self.0.calls.fetch_sub(1, Ordering::AcqRel);
    }
}
// Fixed service incarnation. Clones neither restart nor rebind the worker.
// Every unresolved or unread call retains a slot, even after its command has
// left the mailbox. Dropping a caller does not cancel an accepted command.
pub struct Client<A = MemoryStore, C = MemoryStore> {
    pub(super) sender: SyncSender<Request<A, C>>,
    pub(super) control: Arc<Control>,
}
impl<A, C> Clone for Client<A, C> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            control: self.control.clone(),
        }
    }
}
impl<A, C> Client<A, C> {
    pub fn same_runtime(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.control, &other.control)
    }
    pub fn network(&self) -> &ae_protocol::NetworkProfile {
        &self.control.network
    }
    pub fn retained_vote_reply_bytes(&self) -> usize {
        self.control.vote_replies.used()
    }
    pub fn is_closed(&self) -> bool {
        !self.control.alive.load(Ordering::Acquire) || self.control.stop.load(Ordering::Acquire)
    }
    // Fail closed when an authoritative input source is lost. The output owner
    // still owns draining/abandoning outputs and joining Runtime::shutdown.
    // Accepted calls without replies remain unknown; never rebind or retry them.
    pub fn request_stop(&self) {
        let _gate = self.control.admission.lock().expect("EOE admission");
        self.control.stop.store(true, Ordering::Release);
    }
    pub fn outstanding(&self) -> usize {
        self.control.calls.load(Ordering::Acquire)
    }
    pub fn call(&self, command: Command<A, C>) -> Result<Call, SubmitError<A, C>> {
        let _gate = self.control.admission.lock().expect("EOE admission");
        if self.is_closed() {
            return Err(SubmitError::Disconnected(Box::new(command)));
        }
        if command_size(&command, self.control.maximum_command_bytes).is_err() {
            return Err(SubmitError::ResourceLimit(Box::new(command)));
        }
        if self
            .control
            .calls
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(1).filter(|_| n < self.control.maximum_calls)
            })
            .is_err()
        {
            return Err(SubmitError::Full(Box::new(command)));
        }
        let credit = Arc::new(Credit(self.control.clone()));
        let (reply, receiver) = mpsc::sync_channel(1);
        self.sender
            .try_send(Request {
                command,
                reply: ReplyTo::Caller {
                    reply,
                    _credit: credit.clone(),
                },
            })
            .map_err(|e| match e {
                TrySendError::Full(r) => SubmitError::Full(Box::new(r.command)),
                TrySendError::Disconnected(r) => SubmitError::Disconnected(Box::new(r.command)),
            })?;
        Ok(Call {
            receiver,
            _credit: credit,
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallError {
    Closed,
    Timeout,
}
pub struct Call {
    receiver: Receiver<Result<Applied, coordinator::Error>>,
    _credit: Arc<Credit>,
}
impl Call {
    // The accepted command remains bound to this generation. A lost receipt
    // from a replaceable worker must still be reported as unknown, never
    // resubmitted automatically to its replacement.
    pub fn replacement_pending(&self) -> bool {
        let control = &self._credit.0;
        (control.stop.load(Ordering::Acquire) || !control.alive.load(Ordering::Acquire))
            && !matches!(
                *control.health.lock().expect("EOE health"),
                Health::WorkStopped(_)
            )
    }
    // Read at most one completion. An outer error has an unknown command
    // outcome; an inner error is the worker's acknowledged rejection.
    pub fn try_reply(&mut self) -> Result<Option<Result<Applied, coordinator::Error>>, CallError> {
        match self.receiver.try_recv() {
            Ok(reply) => Ok(Some(reply)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(CallError::Closed),
        }
    }
    pub fn wait(self, timeout: Duration) -> Result<Result<Applied, coordinator::Error>, CallError> {
        self.receiver.recv_timeout(timeout).map_err(|e| match e {
            RecvTimeoutError::Timeout => CallError::Timeout,
            RecvTimeoutError::Disconnected => CallError::Closed,
        })
    }
}
