use crate::runtime::restart::Window;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::Instant,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Child {
    Admin,
    Cache,
    Report,
    LoggerSupervisor,
    Alias,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Info {
    pub restart_attempts: u64,
    pub by_child: BTreeMap<Child, u64>,
    pub exhausted: bool,
}
impl Info {
    pub fn attempts_for(&self, child: Child) -> u64 {
        self.by_child.get(&child).copied().unwrap_or(0)
    }
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Group {
    Registry,
    Report,
    Logger,
}
pub(crate) trait Halt: Send + Sync {
    fn halt(&self);
}
pub(crate) enum BindError {
    Exists,
    Exhausted,
}
struct State {
    info: Info,
    window: Window<5, 10>,
    groups: BTreeMap<Group, Weak<dyn Halt>>,
}
struct Shared {
    state: Mutex<State>,
    origin: Instant,
}
#[derive(Clone)]
pub struct Budget(Arc<Shared>);
impl Default for Budget {
    fn default() -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                info: Info::default(),
                window: Window::default(),
                groups: BTreeMap::new(),
            }),
            origin: Instant::now(),
        }))
    }
}
impl Budget {
    pub fn info(&self) -> Info {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .info
            .clone()
    }
    pub(crate) fn bind(&self, group: Group, owner: Arc<dyn Halt>) -> Result<(), BindError> {
        let mut s = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if s.info.exhausted {
            return Err(BindError::Exhausted);
        }
        if s.groups.get(&group).is_some_and(|g| g.strong_count() != 0) {
            return Err(BindError::Exists);
        }
        s.groups.insert(group, Arc::downgrade(&owner));
        Ok(())
    }
    pub(crate) fn record(&self, child: Child) -> bool {
        self.record_at(child, self.0.origin.elapsed().as_secs())
    }
    fn record_at(&self, child: Child, now: u64) -> bool {
        let halt = {
            let mut s = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            if s.info.exhausted {
                return false;
            }
            s.info.restart_attempts = s.info.restart_attempts.saturating_add(1);
            let n = s.info.by_child.entry(child).or_default();
            *n = n.saturating_add(1);
            if s.window.record(now) {
                return true;
            }
            s.info.exhausted = true;
            s.groups
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        // Never hold the budget lock while closing child admission. Owners may
        // concurrently publish a status which includes this same budget.
        for group in halt {
            group.halt();
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Observer(Budget, AtomicUsize);
    impl Halt for Observer {
        fn halt(&self) {
            assert!(self.0.info().exhausted);
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }
    #[test]
    fn shared_window_includes_the_tenth_second_and_closes_each_group_once() {
        for last in [10, 11] {
            let budget = Budget::default();
            let owners = [Group::Registry, Group::Report, Group::Logger].map(|group| {
                let owner = Arc::new(Observer(budget.clone(), AtomicUsize::new(0)));
                assert!(budget.bind(group, owner.clone()).is_ok());
                assert!(matches!(
                    budget.bind(group, owner.clone()),
                    Err(BindError::Exists)
                ));
                owner
            });
            for child in [
                Child::Admin,
                Child::Cache,
                Child::Alias,
                Child::Report,
                Child::LoggerSupervisor,
            ] {
                assert!(budget.record_at(child, 0));
            }
            assert_eq!(budget.record_at(Child::Admin, last), last == 11);
            assert_eq!(budget.info().restart_attempts, 6);
            assert_eq!(budget.info().attempts_for(Child::Admin), 2);
            for owner in &owners {
                assert_eq!(owner.1.load(Ordering::Relaxed), usize::from(last == 10));
            }
            if last == 10 {
                assert!(!budget.record_at(Child::Report, 100));
                assert!(matches!(
                    budget.bind(Group::Report, owners[1].clone()),
                    Err(BindError::Exhausted)
                ));
                assert_eq!(budget.info().restart_attempts, 6);
                assert!(owners.iter().all(|o| o.1.load(Ordering::Relaxed) == 1));
            }
        }
    }
}
