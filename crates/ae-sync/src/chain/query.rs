use super::*;
use ae_codec::headers::Header;
use ae_storage::events::{Cursor, Reader};

pub mod accounts;
pub mod blocks;
pub mod contracts;
pub mod oracles;
pub mod transactions;

pub(super) struct Height {
    reader: Reader,
    records: usize,
    state: Mutex<(Cursor, Option<u64>)>,
}
impl Height {
    pub fn new(db: &Database) -> Result<Self, Error> {
        let (view, cursor) = db.event_snapshot()?;
        let height = view
            .require_header(view.status().tip.ok_or(repository::Error::Uninitialized)?)?
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Key {
    pub selected: Current,
    pub header: ae_codec::headers::KeyHeader,
    pub algorithm: ae_protocol::ConsensusAlgorithm,
    pub genesis: Hash256,
    pub network: ae_protocol::NetworkProfile,
}

pub struct Ticket<T = Current> {
    pending: blocking::Pending<Result<T, Error>>,
    timeout: Duration,
}
// Retain the read job's capacity through serialization and socket ownership.
// Dropping the guard returns both its job slot and byte reservation.
pub struct ReadCredits {
    _credits: blocking::Credits,
}
pub struct Retained<T> {
    pub value: T,
    pub credits: ReadCredits,
}
// Reserve the header reader before requesting candidate construction. The
// reservation and resulting receipt remain bound to this exact importer.
pub struct PendingHeader<O> {
    context: Arc<Context<O>>,
    reservation: blocking::Reservation,
    timeout: Duration,
}
impl<O: MapOrder + Send + Sync + 'static> PendingHeader<O> {
    pub fn submit(
        self,
        header: Arc<ae_codec::headers::KeyHeader>,
    ) -> Result<Ticket<Option<blocks::ClientHeader>>, Error> {
        let context = self.context;
        let pending = self.reservation.submit(move |cancel| {
            context.check(&cancel, None)?;
            let view = context.db.view()?;
            let Some(previous) = view.header(Hash256(header.common.previous_hash))? else {
                return Ok(None);
            };
            let previous = match previous {
                Header::Key(_) => blocks::Kind::Key,
                Header::Micro(_) => blocks::Kind::Micro,
            };
            context.check(&cancel, None)?;
            Ok(Some(blocks::ClientHeader {
                header: Header::Key(header.as_ref().clone()),
                previous,
            }))
        })?;
        Ok(Ticket {
            pending,
            timeout: self.timeout,
        })
    }
}
impl<T> Ticket<T> {
    pub async fn wait_retained(self) -> Result<Retained<T>, Error> {
        let held = tokio::time::timeout(self.timeout, self.pending.retained())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::from)?;
        Ok(Retained {
            value: held.value?,
            credits: ReadCredits {
                _credits: held.credits,
            },
        })
    }
    pub async fn wait(self) -> Result<T, Error> {
        tokio::time::timeout(self.timeout, self.pending)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::from)?
    }
}
impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    pub fn pending_header_query(&self) -> Result<PendingHeader<O>, Error> {
        Ok(PendingHeader {
            context: self.context.clone(),
            reservation: self.reserve_header()?,
            timeout: self.context.config.timeout,
        })
    }
    fn reserve_header(&self) -> Result<blocking::Reservation, Error> {
        self.check_open()?;
        let weight = self.context.db.read_config().header_read_weight()?;
        self.jobs.reserve(weight).map_err(Into::into)
    }
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
        let pending = self.reserve_header()?.submit(move |cancel| {
            context.check(&cancel, None)?;
            let (view, cursor) = context.db.event_snapshot()?;
            let status = view.status();
            let top = status.tip.ok_or(repository::Error::Uninitialized)?;
            let block = view.require_header(top)?;
            let height = block.common().height;
            let key = match block {
                Header::Key(_) => top,
                Header::Micro(micro) => Hash256(micro.common.previous_key_hash),
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
    // The selected top can be a microblock. Resolve its key header in the
    // same bounded storage job and snapshot, retaining this importer's fixed
    // network/consensus identity. This does not sample other node services.
    pub fn top_key(&self) -> Result<Ticket<Key>, Error> {
        let context = self.context.clone();
        let algorithm = self.algorithm();
        let pending = self.reserve_header()?.submit(move |cancel| {
            context.check(&cancel, None)?;
            let (view, cursor) = context.db.event_snapshot()?;
            let status = view.status();
            let top = status.tip.ok_or(repository::Error::Uninitialized)?;
            let block = view.require_header(top)?;
            let height = block.common().height;
            let (hash, header) = match block {
                Header::Key(header) => (top, header),
                Header::Micro(micro) => {
                    let hash = Hash256(micro.common.previous_key_hash);
                    let Header::Key(header) = view.require_header(hash)? else {
                        return Err(
                            repository::Error::Corrupt("selected key is not a key block").into(),
                        );
                    };
                    (hash, header)
                }
            };
            context.check(&cancel, None)?;
            context.height.seed(cursor, height);
            Ok(Key {
                selected: Current {
                    revision: status.revision,
                    top,
                    key: hash,
                    height,
                },
                header,
                algorithm,
                genesis: context.strategy.genesis(),
                network: context.strategy.network().clone(),
            })
        })?;
        Ok(Ticket {
            pending,
            timeout: self.context.config.timeout,
        })
    }
}
