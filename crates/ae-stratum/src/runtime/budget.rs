use super::Error;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

pub(super) struct Budget {
    used: Mutex<(usize, usize)>,
    maximum: (usize, usize),
    pub changed: watch::Sender<()>,
}
impl Budget {
    pub fn new(records: usize, bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            used: Mutex::new((0, 0)),
            maximum: (records, bytes),
            changed: watch::channel(()).0,
        })
    }
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Credit, Error> {
        if bytes > self.maximum.1 {
            return Err(Error::Limit);
        }
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        if used.0 >= self.maximum.0 || bytes > self.maximum.1.saturating_sub(used.1) {
            return Err(Error::Busy);
        }
        used.0 += 1;
        used.1 += bytes;
        Ok(Credit {
            budget: self.clone(),
            bytes,
        })
    }
    pub fn usage(&self) -> (usize, usize) {
        *self.used.lock().unwrap_or_else(|e| e.into_inner())
    }
}
pub(super) struct Credit {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Drop for Credit {
    fn drop(&mut self) {
        let mut used = self.budget.used.lock().unwrap_or_else(|e| e.into_inner());
        used.0 -= 1;
        used.1 -= self.bytes;
        drop(used);
        self.budget.changed.send_modify(|_| ());
    }
}
struct Retained<T> {
    value: T,
    _credit: Credit,
}
// Queue removal does not refund credit: consumer-held clones remain charged.
pub struct Lease<T>(Arc<Retained<T>>);
impl<T> Lease<T> {
    pub(super) fn new(value: T, credit: Credit) -> Self {
        Self(Arc::new(Retained {
            value,
            _credit: credit,
        }))
    }
}
impl<T> Clone for Lease<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> std::ops::Deref for Lease<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0.value
    }
}
impl<T: std::fmt::Debug> std::fmt::Debug for Lease<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.value.fmt(f)
    }
}
