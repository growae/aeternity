use super::*;
use ae_storage::events::{Cursor, Reader};

pub(super) struct Height {
    reader: Reader,
    records: usize,
    state: Mutex<(Cursor, Option<u64>)>,
}
impl Height {
    pub fn new(db: &Database) -> Result<Self, Error> {
        let (view, cursor) = db.event_snapshot()?;
        let height = view
            .require_block(view.status().tip.ok_or(repository::Error::Uninitialized)?)?
            .header()
            .common()
            .height;
        let reader = db.events();
        let records = reader.info().limits.records;
        Ok(Self {
            reader,
            records,
            state: Mutex::new((cursor, Some(height))),
        })
    }
    fn seed(&self, cursor: Cursor, height: u64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // A delayed request cannot move the event consumer back in time.
        if cursor.revision() >= state.0.revision() {
            *state = (cursor, Some(height));
        }
    }
    fn get(&self) -> Option<u64> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Catch up to the retained source snapshot in one bounded batch. An
        // arbitrary smaller batch would report a stale marker on a quiet chain
        // after a burst, even though every required event remains available.
        match self.reader.after(&state.0, self.records) {
            Ok(batch) if !batch.closed => {
                for payload in batch.records {
                    let context = payload.context();
                    if context.origin == ae_chain::publication::Origin::Rollback {
                        state.1 = None;
                    } else if matches!(
                        context.origin,
                        ae_chain::publication::Origin::Created
                            | ae_chain::publication::Origin::Received
                            | ae_chain::publication::Origin::Synced
                    ) && payload.change().old_tip != Some(payload.change().new_tip)
                    {
                        state.1 = context.block.map(|block| block.height);
                    }
                }
                state.0 = batch.cursor;
            }
            Err(ae_storage::events::Error::Gap { resume, .. }) => {
                *state = (resume, None);
            }
            _ => state.1 = None,
        }
        state.1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Current {
    pub revision: u64,
    pub top: Hash256,
    pub key: Hash256,
    pub height: u64,
}

pub struct Ticket {
    pending: blocking::Pending<Result<Current, Error>>,
    timeout: Duration,
}
impl Ticket {
    pub async fn wait(self) -> Result<Current, Error> {
        tokio::time::timeout(self.timeout, self.pending)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::from)?
    }
}
impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    // Seeded at owner creation, then advanced only by acknowledged selected
    // top events. A missing/gapped/failed source is unknown, never height zero.
    // This hot path uses no storage-work slot, including under HTTP overload.
    pub fn cached_height(&self) -> Option<u64> {
        self.check_open().ok()?;
        self.context.height.get()
    }
    // The accepted read belongs to this exact importer. It shares its bounded,
    // joined worker, cancellation and recovery checks with ledger operations.
    // No snapshot or database job is run on the async executor.
    pub fn current(&self) -> Result<Ticket, Error> {
        let context = self.context.clone();
        let pending = self.reserve()?.submit(move |cancel| {
            context.check(&cancel, None)?;
            let (view, cursor) = context.db.event_snapshot()?;
            let status = view.status();
            let top = status.tip.ok_or(repository::Error::Uninitialized)?;
            let block = view.require_block(top)?;
            let height = block.header().common().height;
            let key = match block {
                Block::Key(_) => top,
                Block::Micro(micro) => Hash256(micro.header.common.previous_key_hash),
            };
            context.check(&cancel, None)?;
            context.height.seed(cursor, height);
            Ok(Current {
                revision: status.revision,
                top,
                key,
                height,
            })
        })?;
        Ok(Ticket {
            pending,
            timeout: self.context.config.timeout,
        })
    }
}
