use super::*;

pub(super) struct Reply<T> {
    result: Mutex<Option<Result<T, Error>>>,
    wake: Condvar,
    owner: Weak<Shared>,
    capacity: Capacity,
    bytes: usize,
}
impl<T> Reply<T> {
    pub(super) fn new(owner: Weak<Shared>, capacity: Capacity, bytes: usize) -> Self {
        Self {
            result: Mutex::new(None),
            wake: Condvar::new(),
            owner,
            capacity,
            bytes,
        }
    }
    fn complete(&self, result: Result<T, Error>) {
        let mut slot = lock(&self.result);
        if slot.is_none() {
            *slot = Some(result);
            self.wake.notify_all();
        }
    }
    fn abandon(&self, executing: bool) {
        let mut slot = lock(&self.result);
        if slot.is_none() {
            if executing && let Some(owner) = self.owner.upgrade() {
                // A caller may observe this receipt before the worker's outer
                // unwind handler runs. Publish departure before the unknown
                // result so it cannot be mistaken for a live-worker failure.
                lock(&owner.queue).accepting = false;
                owner.wake.notify_all();
            }
            *slot = Some(Err(if executing {
                Error::Disconnected
            } else {
                Error::Stopped
            }));
            self.wake.notify_all();
        }
    }
}
impl<T> Drop for Reply<T> {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            let mut q = lock(&owner.queue);
            q.requests -= 1;
            q.bytes -= self.bytes;
        }
        let mut used = lock(&self.capacity.0.used);
        used.requests -= 1;
        used.bytes -= self.bytes;
    }
}

#[derive(Clone)]
pub struct Ticket<T>(pub(super) Arc<Reply<T>>);
// An application consumes a terminal ticket into an owned result before
// retaining a report. Historical reports must not hold live worker credits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observed<T> {
    Pending(Ticket<T>),
    Known(Result<T, Error>),
}
impl<T: Clone> From<Ticket<T>> for Observed<T> {
    fn from(ticket: Ticket<T>) -> Self {
        let mut receipt = Self::Pending(ticket);
        receipt.poll();
        receipt
    }
}
impl<T: Clone> Observed<T> {
    pub fn poll(&mut self) {
        if let Self::Pending(ticket) = self
            && let Some(result) = ticket.try_result()
        {
            *self = Self::Known(result);
        }
    }
    pub fn try_result(&self) -> Option<Result<T, Error>> {
        match self {
            Self::Pending(ticket) => ticket.try_result(),
            Self::Known(result) => Some(result.clone()),
        }
    }
}
impl<T> PartialEq for Ticket<T> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl<T> Eq for Ticket<T> {}
impl<T> std::fmt::Debug for Ticket<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Ticket")
    }
}
impl<T: Clone> Ticket<T> {
    pub fn try_result(&self) -> Option<Result<T, Error>> {
        lock(&self.0.result).clone()
    }
    // Timing out neither cancels nor resubmits an accepted request.
    pub fn wait_timeout(&self, timeout: Duration) -> Option<Result<T, Error>> {
        let (result, _) = self
            .0
            .wake
            .wait_timeout_while(lock(&self.0.result), timeout, |v| v.is_none())
            .unwrap_or_else(|e| e.into_inner());
        result.clone()
    }
}

pub(super) trait Job: Send {
    fn run(self: Box<Self>, state: &mut State);
}
pub(super) struct Pending<T, F> {
    reply: Arc<Reply<T>>,
    work: Option<F>,
    executing: bool,
}
impl<T, F> Pending<T, F> {
    pub(super) fn new(reply: Arc<Reply<T>>, work: F) -> Self {
        Self {
            reply,
            work: Some(work),
            executing: false,
        }
    }
}
impl<T: Send, F: FnOnce(&mut State) -> Result<T, Error> + Send> Job for Pending<T, F> {
    fn run(mut self: Box<Self>, state: &mut State) {
        self.executing = true;
        let result = self.work.take().unwrap()(state);
        self.reply.complete(result);
    }
}
impl<T, F> Drop for Pending<T, F> {
    fn drop(&mut self) {
        self.reply.abandon(self.executing);
    }
}
