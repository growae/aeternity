use super::*;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestError {
    Busy,
    Closed,
    OutputFull,
}
pub(super) struct Credit(Arc<Mutex<usize>>);
impl Credit {
    pub fn acquire(count: &Arc<Mutex<usize>>, maximum: usize) -> Option<Self> {
        let mut n = lock(count);
        if *n >= maximum {
            return None;
        }
        *n += 1;
        Some(Self(count.clone()))
    }
}
impl Drop for Credit {
    fn drop(&mut self) {
        *lock(&self.0) -= 1;
    }
}
pub(super) struct Slot {
    result: Mutex<Option<Result<(), RequestError>>>,
    ready: Condvar,
    _credit: Credit,
}
impl Slot {
    pub fn new(credit: Credit) -> Self {
        Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
            _credit: credit,
        }
    }
    pub fn complete(&self, value: Result<(), RequestError>) {
        let mut result = lock(&self.result);
        if result.is_none() {
            *result = Some(value);
            self.ready.notify_all();
        }
    }
}
#[derive(Clone)]
pub struct Ticket(pub(super) Arc<Slot>);
impl Ticket {
    pub fn try_result(&self) -> Option<Result<(), RequestError>> {
        *lock(&self.0.result)
    }
    // Observation is repeatable; timeout and drop never cancel accepted work.
    pub fn wait_timeout(&self, timeout: Duration) -> Option<Result<(), RequestError>> {
        let (result, _) = self
            .0
            .ready
            .wait_timeout_while(lock(&self.0.result), timeout, |r| r.is_none())
            .unwrap_or_else(|e| e.into_inner());
        *result
    }
}
