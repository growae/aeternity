use super::Budget;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::sync::{Mutex, OwnedMutexGuard};

// aec_worker_sup owns one restart history, including HC votes. A permit spans
// the whole replacement, even when separate Rust owners drive its children.
#[derive(Clone)]
pub(crate) struct Group(Arc<Shared>);
struct Shared {
    budget: Arc<Mutex<Budget>>,
    clock: Box<dyn Fn() -> u64 + Send + Sync>,
    attempts: AtomicU64,
    exhausted: AtomicBool,
}
pub(crate) struct Permit {
    budget: OwnedMutexGuard<Budget>,
    group: Group,
}
impl Default for Group {
    fn default() -> Self {
        let origin = std::time::Instant::now();
        Self::with_clock(move || origin.elapsed().as_secs())
    }
}
impl Group {
    fn with_clock(clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        Self(Arc::new(Shared {
            budget: Arc::new(Mutex::new(Budget::default())),
            clock: Box::new(clock),
            attempts: AtomicU64::new(0),
            exhausted: AtomicBool::new(false),
        }))
    }
    pub fn attempts(&self) -> u64 {
        self.0.attempts.load(Ordering::Acquire)
    }
    pub fn exhausted(&self) -> bool {
        self.0.exhausted.load(Ordering::Acquire)
    }
    pub fn try_acquire(&self) -> Option<Permit> {
        Some(Permit {
            budget: self.0.budget.clone().try_lock_owned().ok()?,
            group: self.clone(),
        })
    }
    pub async fn acquire(&self) -> Permit {
        Permit {
            budget: self.0.budget.clone().lock_owned().await,
            group: self.clone(),
        }
    }
    #[cfg(test)]
    pub(crate) fn controlled() -> (Self, Arc<AtomicU64>) {
        let time = Arc::new(AtomicU64::new(0));
        let clock = time.clone();
        (
            Self::with_clock(move || clock.load(Ordering::Acquire)),
            time,
        )
    }
}
impl Permit {
    pub fn record(&mut self) -> bool {
        if self.group.exhausted() {
            return false;
        }
        // Clock sampling and recording have the same serialized boundary.
        let now = (self.group.0.clock)();
        let next = self.group.attempts().checked_add(1);
        if let Some(next) = next {
            self.group.0.attempts.store(next, Ordering::Release);
            if self.budget.record(now) {
                return true;
            }
        }
        self.group.0.exhausted.store(true, Ordering::Release);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mixed_owners_serialize_replacements_and_keep_exhaustion_after_the_window() {
        let (votes, clock) = Group::controlled();
        let optional = votes.clone();
        for index in 0..5 {
            let mut permit = if index % 2 == 0 {
                votes.try_acquire().unwrap()
            } else {
                optional.acquire().await
            };
            assert!(permit.record());
            assert!(votes.try_acquire().is_none());
            assert!(optional.try_acquire().is_none());
        }
        clock.store(10, Ordering::Release);
        assert!(!optional.acquire().await.record());
        assert_eq!(votes.attempts(), 6);
        assert!(votes.exhausted() && optional.exhausted());
        clock.store(100, Ordering::Release);
        assert!(!votes.try_acquire().unwrap().record());
        assert_eq!(optional.attempts(), 6);
    }

    #[tokio::test]
    async fn the_shared_clock_is_sampled_after_waiting_and_cancelled_waiters_spend_nothing() {
        let (votes, clock) = Group::controlled();
        let mut held = votes.try_acquire().unwrap();
        for _ in 0..5 {
            assert!(held.record());
        }
        let optional = votes.clone();
        let waiting = tokio::spawn(async move { optional.acquire().await.record() });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert_eq!(votes.attempts(), 5);
        clock.store(11, Ordering::Release);
        let optional = votes.clone();
        let waiting = tokio::spawn(async move { optional.acquire().await.record() });
        drop(held);
        assert!(waiting.await.unwrap());
        assert!(!votes.exhausted());
        assert_eq!(votes.attempts(), 6);
    }
}
