use super::*;
use std::sync::{Weak, atomic::AtomicBool};

// The parent owns this lease. Workers borrow a single active incarnation;
// clients retain weak descriptors and cannot keep the parent alive.
pub(super) struct Inner {
    closed: AtomicBool,
    worker: AtomicBool,
    pub(super) ready: AtomicBool,
}
pub struct Owner {
    db: Arc<Database>,
    inner: Arc<Inner>,
}
#[derive(Clone)]
pub struct Store {
    db: Weak<Database>,
    inner: Weak<Inner>,
}
pub struct Lease {
    db: Arc<Database>,
    inner: Arc<Inner>,
}
// Importers bind the parent lifetime, not a child incarnation. An old importer
// must stay closed even if a replacement parent initializes the same database.
pub struct Scope {
    db: Weak<Database>,
    inner: Option<Weak<Inner>>,
}
#[derive(Debug)]
pub enum CommitError<E> {
    Repository(Error),
    Guard(E),
}
impl Scope {
    pub fn is_closed(&self) -> bool {
        match &self.inner {
            Some(inner) => inner
                .upgrade()
                .is_none_or(|s| s.closed.load(Ordering::Acquire)),
            None => self
                .db
                .upgrade()
                .is_none_or(|db| db.policy_managed.load(Ordering::Acquire)),
        }
    }
    fn check(&self, db: &Database) -> Result<(), Error> {
        if !std::ptr::eq(self.db.as_ptr(), db) {
            return Err(Error::Identity);
        }
        if self.is_closed() {
            Err(Error::PolicyClosed)
        } else {
            Ok(())
        }
    }
}
impl Owner {
    pub fn new(db: Arc<Database>) -> Result<Self, Error> {
        let mut writer = db.writer.lock().map_err(|_| backend_lock())?;
        db.ensure_policy_recovered()?;
        let mut owner = db.policy_owner.lock().map_err(|_| backend_lock())?;
        if owner.upgrade().is_some() {
            return Err(Error::PolicyOwned);
        }
        db.reset_policy(&mut writer)?;
        let inner = Arc::new(Inner {
            closed: AtomicBool::new(false),
            worker: AtomicBool::new(false),
            ready: AtomicBool::new(false),
        });
        *owner = Arc::downgrade(&inner);
        db.policy_managed.store(true, Ordering::Release);
        drop(owner);
        drop(writer);
        Ok(Self { db, inner })
    }
    pub fn store(&self) -> Store {
        Store {
            db: Arc::downgrade(&self.db),
            inner: Arc::downgrade(&self.inner),
        }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let mut writer = self.db.writer.lock().unwrap_or_else(|e| e.into_inner());
        self.inner.closed.store(true, Ordering::Release);
        if self.db.reset_policy(&mut writer).is_err() {
            self.db.fail_policy();
        }
    }
}
impl Store {
    pub fn uses_database(&self, db: &Arc<Database>) -> bool {
        self.db.ptr_eq(&Arc::downgrade(db))
    }
    pub fn is_closed(&self) -> bool {
        self.inner
            .upgrade()
            .is_none_or(|s| s.closed.load(Ordering::Acquire))
    }
    pub fn snapshot(&self) -> Result<resilience::Snapshot, Error> {
        let db = self.db.upgrade().ok_or(Error::PolicyClosed)?;
        let inner = self.inner.upgrade().ok_or(Error::PolicyClosed)?;
        let guard = db.writer.lock().map_err(|_| backend_lock())?;
        db.authorize_policy(Some(&inner))?;
        db.ensure_policy_recovered()?;
        Ok(*guard)
    }
    pub fn recovery_required(&self) -> bool {
        self.db
            .upgrade()
            .is_none_or(|db| db.policy_recovery_required())
    }
    pub fn acquire(&self) -> Result<Lease, Error> {
        let db = self.db.upgrade().ok_or(Error::PolicyClosed)?;
        let inner = self.inner.upgrade().ok_or(Error::PolicyClosed)?;
        let guard = db.writer.lock().map_err(|_| backend_lock())?;
        db.authorize_policy(Some(&inner))?;
        db.ensure_policy_recovered()?;
        inner
            .worker
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::PolicyOwned)?;
        drop(guard);
        Ok(Lease { db, inner })
    }
}
impl Lease {
    pub fn update(
        &self,
        event: resilience::Event,
        config: resilience::Config,
    ) -> Result<resilience::Snapshot, Error> {
        self.db
            .update_resilience_owned(event, config, Some(&self.inner), || Ok(()))
    }
    pub fn clear_finalized_height(&self) -> Result<(), Error> {
        self.db.clear_finalized_owned(Some(&self.inner))
    }
    pub fn snapshot(&self) -> Result<resilience::Snapshot, Error> {
        let guard = self.db.writer.lock().map_err(|_| backend_lock())?;
        self.db.authorize_policy(Some(&self.inner))?;
        self.db.ensure_policy_recovered()?;
        Ok(*guard)
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.inner.worker.store(false, Ordering::Release);
    }
}
impl Database {
    pub fn policy_scope(self: &Arc<Self>) -> Result<Scope, Error> {
        let _guard = self.writer.lock().map_err(|_| backend_lock())?;
        self.ensure_policy_recovered()?;
        let inner = if self.policy_managed.load(Ordering::Acquire) {
            let owner = self
                .policy_owner
                .lock()
                .map_err(|_| backend_lock())?
                .upgrade()
                .ok_or(Error::PolicyClosed)?;
            if owner.closed.load(Ordering::Acquire) {
                return Err(Error::PolicyClosed);
            }
            Some(Arc::downgrade(&owner))
        } else {
            None
        };
        Ok(Scope {
            db: Arc::downgrade(self),
            inner,
        })
    }
    pub fn commit_in_policy_scope(
        &self,
        update: Update,
        scope: &Scope,
    ) -> Result<Committed, Error> {
        self.commit_checked(update, || scope.check(self))
    }
    // Check the worker's cancellation/client lifetime as well as the parent
    // lease while holding the actual writer, including immediately before WAL.
    // The guard must not reenter storage or wait for another writer.
    pub fn commit_guarded<E>(
        &self,
        update: Update,
        scope: &Scope,
        mut check: impl FnMut() -> Result<(), E>,
    ) -> Result<Committed, CommitError<E>> {
        let mut rejected = None;
        let result = self.commit_checked(update, || {
            scope.check(self)?;
            check().map_err(|error| {
                rejected = Some(error);
                // Internal bridge to the existing writer. This sentinel never
                // escapes as a conflict and cannot trigger a retry.
                Error::Conflict
            })
        });
        match rejected {
            Some(error) => Err(CommitError::Guard(error)),
            None => result.map_err(CommitError::Repository),
        }
    }
    // Callers hold writer before checking ownership; reset, updates, and block
    // commit all share that same serialization point.
    pub(super) fn authorize_policy(&self, token: Option<&Arc<Inner>>) -> Result<(), Error> {
        let owner = self
            .policy_owner
            .lock()
            .map_err(|_| backend_lock())?
            .upgrade();
        match (owner, token) {
            (None, None) if !self.policy_managed.load(Ordering::Acquire) => Ok(()),
            (None, None) => Err(Error::PolicyClosed),
            (Some(owner), Some(token))
                if Arc::ptr_eq(&owner, token) && !owner.closed.load(Ordering::Acquire) =>
            {
                Ok(())
            }
            (_, Some(_)) => Err(Error::PolicyClosed),
            (Some(_), None) => Err(Error::PolicyOwned),
        }
    }
    pub fn policy_ready(&self) -> bool {
        if !self.policy_managed.load(Ordering::Acquire) {
            return true;
        }
        self.policy_owner
            .lock()
            .ok()
            .and_then(|s| s.upgrade())
            .is_some_and(|s| !s.closed.load(Ordering::Acquire) && s.ready.load(Ordering::Acquire))
    }
    pub fn policy_recovery_required(&self) -> bool {
        self.policy_recovery.load(Ordering::Acquire)
    }
    pub(super) fn ensure_policy_recovered(&self) -> Result<(), Error> {
        if self.policy_recovery_required() {
            Err(Error::RecoveryRequired)
        } else {
            Ok(())
        }
    }
    pub(super) fn fail_policy(&self) {
        self.policy_recovery.store(true, Ordering::Release);
        self.events.fail(events::Failure::UncertainWrite);
        self.gc_events.fail(gc::events::Failure::UncertainWrite);
        self.wake_pool();
        self.wake_gc();
    }
    fn reset_policy(&self, writer: &mut resilience::Snapshot) -> Result<(), Error> {
        // Invalidate old plans even when the departing cache was empty.
        let generation = writer
            .generation
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        *writer = resilience::Snapshot {
            generation,
            state: Default::default(),
        };
        Ok(())
    }
    pub(super) fn policy_operation<T>(
        &self,
        action: impl FnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)) {
            Ok(result) => result,
            Err(_) => {
                self.fail_policy();
                Err(Error::Backend("fork policy worker panicked".into()))
            }
        }
    }
    pub(super) fn write_policy(&self, batch: WriteBatch) -> Result<(), Error> {
        #[cfg(test)]
        let failure = self.policy_write_failure.swap(0, Ordering::Relaxed);
        #[cfg(test)]
        if failure == 1 {
            self.fail_policy();
            return Err(Error::Backend("injected policy write failure".into()));
        }
        if let Err(error) = self.db.write_opt(batch, &write_options()) {
            self.fail_policy();
            return Err(backend(error));
        }
        #[cfg(test)]
        if failure == 2 {
            self.fail_policy();
            return Err(Error::Backend("injected policy acknowledgment loss".into()));
        }
        #[cfg(test)]
        assert_ne!(failure, 3, "injected policy write panic");
        Ok(())
    }
}

#[cfg(test)]
mod tests;
