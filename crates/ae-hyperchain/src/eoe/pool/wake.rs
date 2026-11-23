use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

#[derive(Clone, Default)]
pub struct Wake(Arc<(Mutex<bool>, Condvar)>);
impl Wake {
    pub fn notify(&self) {
        let mut notified = self
            .0
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *notified = true;
        self.0.1.notify_one();
    }
    pub fn wait(&self, timeout: Duration) {
        let notified = self
            .0
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut notified, _) = self
            .0
            .1
            .wait_timeout_while(notified, timeout, |n| !*n)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *notified = false;
    }
}
