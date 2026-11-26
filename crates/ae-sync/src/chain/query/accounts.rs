use super::*;
use ae_chain::account::{self, Selection};
use ae_mempool::pool::Signed;
use ae_mempool::pool::accounts::{Limits as PoolLimits, Strategy};
use ae_storage::pool::worker;
use ae_types::{AccountPublicKey, Unsigned};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    State(Selection),
    Pending,
    Nonce(Strategy),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request {
    pub account: AccountPublicKey,
    pub kind: Kind,
}
pub enum Reply {
    Account(account::Info),
    Pending(Vec<Arc<Signed>>),
    Nonce(Unsigned),
    StateError(account::Error),
    PoolError(worker::Error),
}
pub struct Rendered<T> {
    pub value: T,
    pub pool_credits: Option<worker::accounts::Credits>,
}
pub struct Permit<'a, O> {
    importer: &'a Importer<O>,
    reservation: blocking::Reservation,
    limits: account::Limits,
}
pub fn limits() -> account::Limits {
    account::Limits {
        reads: ae_chain::repository::point::Limits {
            read_bytes: 16 << 20,
            ..Default::default()
        },
        fate: ae_vm_fate::Limits {
            max_bytes: 4 << 20,
            max_nodes: 65_536,
            max_depth: 64,
        },
    }
}
impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    pub fn account_query(&self, rendering_bytes: usize) -> Result<Permit<'_, O>, Error> {
        self.check_open()?;
        let limits = limits();
        // Point-read bytes plus bounded RLP/FATE value metadata and adapter
        // rendering work. This is operational credit, not a consensus limit.
        let weight = limits
            .reads
            .read_bytes
            .checked_add(limits.fate.max_bytes)
            .and_then(|n| n.checked_add(limits.reads.rlp.max_items.checked_mul(128)?))
            .and_then(|n| n.checked_add(limits.fate.max_nodes.checked_mul(128)?))
            .and_then(|n| n.checked_add(rendering_bytes))
            .ok_or(Error::Busy)?;
        Ok(Permit {
            importer: self,
            reservation: self.jobs.reserve(weight)?,
            limits,
        })
    }
}
impl<O: MapOrder + Send + Sync + 'static> Permit<'_, O> {
    pub fn submit<T: Send + 'static>(
        self,
        request: Request,
        gc_enabled: bool,
        pool: Option<worker::Client>,
        pool_timeout: Duration,
        render: impl FnOnce(Reply) -> T + Send + 'static,
    ) -> Result<Ticket<Rendered<T>>, Error> {
        self.importer.check_open()?;
        let context = self.importer.context.clone();
        let limits = self.limits;
        let pending = self.reservation.submit(move |cancel| {
            context.check(&cancel, None)?;
            let view = context.db.view()?;
            let result =
                account::repository::Reader::new(&view, limits, gc_enabled).and_then(|reader| {
                    let selection = match request.kind {
                        Kind::State(s) => s,
                        _ => Selection::Current,
                    };
                    let account = reader.account(request.account, selection)?;
                    if matches!(request.kind, Kind::State(_)) {
                        reader.describe(account).map(|info| (None, Some(info)))
                    } else {
                        Ok((Some(account), None))
                    }
                });
            // Release the database snapshot before waiting on the independent
            // pool actor, which may commit earlier accepted mutations.
            drop(view);
            context.check(&cancel, None)?;
            let mut credits = None;
            let reply = match result {
                Err(e) => Reply::StateError(e),
                Ok((_, Some(info))) => Reply::Account(info),
                Ok((Some(account), None))
                    if matches!(request.kind, Kind::Nonce(_))
                        && account.authorization().is_some() =>
                {
                    Reply::Nonce(0.into())
                }
                Ok((Some(account), None)) => {
                    let pool = pool.filter(|p| p.uses_database(&context.db));
                    match pool {
                        None => Reply::PoolError(worker::Error::Stopped),
                        Some(pool) => match request.kind {
                            Kind::Pending => match pool
                                .pending_account(request.account, PoolLimits::default())
                                .and_then(|t| t.wait_timeout(pool_timeout))
                            {
                                Ok(held) => {
                                    credits = Some(held.credits);
                                    Reply::Pending(held.value)
                                }
                                Err(e) => Reply::PoolError(e),
                            },
                            Kind::Nonce(strategy) => match pool
                                .account_nonce(
                                    request.account,
                                    account.nonce().clone(),
                                    strategy,
                                    PoolLimits::default(),
                                )
                                .and_then(|t| t.wait_timeout(pool_timeout))
                            {
                                Ok(held) => {
                                    credits = Some(held.credits);
                                    Reply::Nonce(held.value)
                                }
                                Err(e) => Reply::PoolError(e),
                            },
                            Kind::State(_) => unreachable!(),
                        },
                    }
                }
                _ => unreachable!(),
            };
            context.check(&cancel, None)?;
            let value = render(reply);
            context.check(&cancel, None)?;
            Ok(Rendered {
                value,
                pool_credits: credits,
            })
        })?;
        Ok(Ticket {
            pending,
            timeout: self.importer.context.config.timeout,
        })
    }
}
