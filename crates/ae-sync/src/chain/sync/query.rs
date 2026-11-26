// Live aec_sync:sync_progress / aec_sync_stats:get_estimate queries.
// aeternity fa2a6d7. Copyright (c) Aeternity developers. ISC: LICENSE.
use super::*;

pub struct Ticket<T> {
    pending: blocking::Pending<Result<T, Error>>,
    source: Weak<dyn Source>,
    timeout: Duration,
}
impl<T> Ticket<T> {
    pub async fn wait(self) -> Result<T, Error> {
        let result = tokio::time::timeout(self.timeout, self.pending)
            .await
            .map_err(|_| Error::Timeout)??;
        // Even an already completed, unread response cannot outlive its owner.
        self.source.upgrade().ok_or(Error::Closed)?.check()?;
        result
    }
}
pub(super) trait Source: Send + Sync {
    fn check(&self) -> Result<(), Error>;
    fn progress(self: Arc<Self>) -> Result<Ticket<lifecycle::Progress>, Error>;
    fn height(self: Arc<Self>) -> Result<Ticket<u64>, Error>;
    fn estimate(&self, task: u64) -> Result<Option<f64>, Error>;
}
impl Handle {
    pub fn same_owner(&self, other: &Self) -> bool {
        self.queries.ptr_eq(&other.queries)
    }
    pub fn progress(&self) -> Result<Ticket<lifecycle::Progress>, Error> {
        self.queries.upgrade().ok_or(Error::Closed)?.progress()
    }
    pub fn selected_height(&self) -> Result<Ticket<u64>, Error> {
        self.queries.upgrade().ok_or(Error::Closed)?.height()
    }
    pub fn estimate(&self, task: u64) -> Result<Option<f64>, Error> {
        self.queries.upgrade().ok_or(Error::Closed)?.estimate(task)
    }
}
fn selected(db: &Database) -> Result<u64, Error> {
    let view = db.view()?;
    let top = view.status().tip.ok_or(repository::Error::Uninitialized)?;
    Ok(view.require_block(top)?.header().common().height)
}
impl<O: MapOrder + Send + Sync + 'static> Context<O> {
    fn query<T: Send + 'static>(
        self: Arc<Self>,
        read: impl FnOnce(&Self) -> Result<T, Error> + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        self.check()?;
        let reserved = self
            .reads
            .reserve(self.config.messages.body_bytes + self.config.messages.work_bytes)?;
        let source: Arc<dyn Source> = self.clone();
        let timeout = self.config.timeout;
        let pending = reserved.submit(move |cancel| {
            self.check()?;
            if cancel.cancelled() {
                return Err(Error::Closed);
            }
            let value = read(&self)?;
            self.check()?;
            if cancel.cancelled() {
                return Err(Error::Closed);
            }
            Ok(value)
        })?;
        Ok(Ticket {
            pending,
            source: Arc::downgrade(&source),
            timeout,
        })
    }
}
impl<O: MapOrder + Send + Sync + 'static> Source for Context<O> {
    fn check(&self) -> Result<(), Error> {
        if self.stopped.load(Ordering::Acquire) || *self.requested.borrow() || self.reads.stopped()
        {
            return Err(Error::Closed);
        }
        self.importer.check_open().map_err(Error::Policy)
    }
    fn progress(self: Arc<Self>) -> Result<Ticket<lifecycle::Progress>, Error> {
        self.query(|context| {
            let (tasks, target, _) = context
                .shared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .lifecycle();
            // The native inactive branch never reads chain storage.
            let height = if tasks.iter().any(|t| !t.suspect) {
                selected(&context.importer.context.db)?
            } else {
                0
            };
            lifecycle::progress(&tasks, target, height).map_err(Error::Progress)
        })
    }
    fn height(self: Arc<Self>) -> Result<Ticket<u64>, Error> {
        self.query(|context| selected(&context.importer.context.db))
    }
    fn estimate(&self, task: u64) -> Result<Option<f64>, Error> {
        self.check()?;
        let estimate = self
            .shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .estimate(task);
        self.check()?;
        match estimate {
            Ok(speed) => Ok(Some(speed)),
            Err(statistics::Error::UnknownTask) => Ok(None),
            Err(error) => Err(Error::Statistics(error)),
        }
    }
}

#[cfg(test)]
mod tests;
