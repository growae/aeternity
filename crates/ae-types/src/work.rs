use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interrupted;

// Explicit operational control. It does not own a clock, read configuration,
// charge consensus gas or turn interruption into a transaction rejection.
#[derive(Clone)]
pub struct Control(Arc<State>);
struct State {
    stopped: AtomicBool,
    poll: Box<dyn Fn() -> bool + Send + Sync>,
}
impl Control {
    pub fn combine(first: Option<&Self>, second: Option<&Self>) -> Option<Self> {
        match (first, second) {
            (Some(a), Some(b)) if !Arc::ptr_eq(&a.0, &b.0) => {
                let (a, b) = (a.clone(), b.clone());
                Some(Self::new(move || a.check().is_err() || b.check().is_err()))
            }
            (Some(a), _) | (_, Some(a)) => Some(a.clone()),
            (None, None) => None,
        }
    }
    pub fn new(poll: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        Self(Arc::new(State {
            stopped: AtomicBool::new(false),
            poll: Box::new(poll),
        }))
    }
    pub fn cancel(&self) {
        self.0.stopped.store(true, Ordering::Release);
    }
    pub fn check(&self) -> Result<(), Interrupted> {
        if self.0.stopped.load(Ordering::Acquire) {
            return Err(Interrupted);
        }
        if (self.0.poll)() {
            self.cancel();
        }
        if self.0.stopped.load(Ordering::Acquire) {
            Err(Interrupted)
        } else {
            Ok(())
        }
    }
}
impl fmt::Debug for Control {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Control")
            .field("interrupted", &self.0.stopped.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn observed_interruption_is_sticky_across_clones_and_composed_controls() {
        for cancel_first in [false, true] {
            let signal = Arc::new(AtomicBool::new(false));
            let observed = signal.clone();
            let first = Control::new(move || observed.load(Ordering::Acquire));
            let second = Control::new(|| false);
            let joined = Control::combine(Some(&first), Some(&second)).unwrap();
            assert_eq!(joined.check(), Ok(()));
            if cancel_first {
                signal.store(true, Ordering::Release);
            } else {
                second.clone().cancel();
            }
            assert_eq!(joined.check(), Err(Interrupted));
            signal.store(false, Ordering::Release);
            assert_eq!(joined.clone().check(), Err(Interrupted));
            if cancel_first {
                assert_eq!(first.check(), Err(Interrupted));
            }
        }
    }
}
