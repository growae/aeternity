use super::*;

pub(super) struct Reply<T> {
    result: Mutex<Option<Result<T, Error>>>,
    wake: Condvar,
    owner: Weak<Shared>,
    bytes: usize,
}
impl<T> Reply<T> {
    pub(super) fn new(owner: Weak<Shared>, bytes: usize) -> Self {
        Self {
            result: Mutex::new(None),
            wake: Condvar::new(),
            owner,
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
}
impl<T> Drop for Reply<T> {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            let mut q = lock(&owner.queue);
            q.requests -= 1;
            q.bytes -= self.bytes;
        }
    }
}

#[derive(Clone)]
pub struct Ticket<T>(pub(super) Arc<Reply<T>>);
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
        self.reply.complete(Err(if self.executing {
            Error::Disconnected
        } else {
            Error::Stopped
        }));
    }
}
