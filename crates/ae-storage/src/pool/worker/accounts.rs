use super::*;
use ae_mempool::pool::accounts::{Limits, Strategy};
use ae_types::{AccountPublicKey, Unsigned};

pub(super) struct Budget {
    used: Mutex<(usize, usize)>,
    settings: Settings,
}
impl Budget {
    pub(super) fn new(settings: Settings) -> Arc<Self> {
        Arc::new(Self {
            used: Mutex::new((0, 0)),
            settings,
        })
    }
    pub(super) fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Credits, Error> {
        if bytes > self.settings.single_request_bytes {
            return Err(Error::RequestTooLarge);
        }
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        let total = used.1.checked_add(bytes).ok_or(Error::Saturated)?;
        if used.0 >= self.settings.requests || total > self.settings.request_bytes {
            return Err(Error::Saturated);
        }
        used.0 += 1;
        used.1 = total;
        Ok(Credits {
            _lease: Arc::new(Lease {
                budget: self.clone(),
                bytes,
            }),
        })
    }
}
struct Lease {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut used = self.budget.used.lock().unwrap_or_else(|e| e.into_inner());
        used.0 -= 1;
        used.1 -= self.bytes;
    }
}
// Credit survives queueing, execution, reply delivery and caller ownership.
// Its budget survives worker retirement; a replacement has a distinct budget.
pub struct Credits {
    _lease: Arc<Lease>,
}
pub struct Retained<T> {
    pub value: T,
    pub credits: Credits,
}
impl Client {
    // Includes policy-settings reads sharing the same retained reply budget.
    pub fn account_query_usage(&self) -> (usize, usize) {
        *self
            .shared
            .accounts
            .used
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
    pub fn pending_account(
        &self,
        owner: AccountPublicKey,
        limits: Limits,
    ) -> Result<Ticket<Retained<Vec<Arc<crate::pool::Signed>>>>, Error> {
        if limits.items > self.shared.settings.max_peek {
            return Err(Error::RequestTooLarge);
        }
        let bytes = BASE_WEIGHT
            .checked_add(limits.bytes)
            .ok_or(Error::RequestTooLarge)?;
        let credits = self.shared.accounts.reserve(bytes)?;
        self.submit(BASE_WEIGHT, move |service| {
            let value = service.pending_account(owner, limits)?;
            Ok(Retained { value, credits })
        })
    }
    pub fn account_nonce(
        &self,
        owner: AccountPublicKey,
        state: Unsigned,
        strategy: Strategy,
        limits: Limits,
    ) -> Result<Ticket<Retained<Unsigned>>, Error> {
        if limits.items > self.shared.settings.max_peek || state.as_bytes().len() >= limits.bytes {
            return Err(Error::RequestTooLarge);
        }
        let input = BASE_WEIGHT
            .checked_add(state.as_bytes().len())
            .ok_or(Error::RequestTooLarge)?;
        let bytes = input
            .checked_add(limits.bytes)
            .ok_or(Error::RequestTooLarge)?;
        let credits = self.shared.accounts.reserve(bytes)?;
        self.submit(input, move |service| {
            let value = service.account_nonce(owner, &state, strategy, limits)?;
            Ok(Retained { value, credits })
        })
    }
}
