// aec_next_nonce and aec_tx_pool account-index queries, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    Max,
    Continuity,
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub items: usize,
    pub bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            items: 100_000,
            bytes: 8 << 20,
        }
    }
}

// The first missing successor is unchanged by duplicate or stale nonces.
// Consume sorted index keys, preserving arbitrary-precision arithmetic.
fn continuity<'a>(state: &Unsigned, nonces: impl IntoIterator<Item = &'a Unsigned>) -> Unsigned {
    let mut next = state.add(&1.into());
    for nonce in nonces {
        match nonce.cmp(&next) {
            std::cmp::Ordering::Less => (),
            std::cmp::Ordering::Equal => next = next.add(&1.into()),
            std::cmp::Ordering::Greater => break,
        }
    }
    next
}
impl Pool {
    pub fn pending_account(
        &self,
        owner: AccountPublicKey,
        limits: Limits,
    ) -> Result<Vec<Arc<Signed>>, Error> {
        let mut bytes = 0usize;
        let mut result = Vec::new();
        if let Some(nonces) = self.nonces.get(&Some(owner)) {
            for key in nonces.values().flatten() {
                let Some((_, entry)) = self.entry(key) else {
                    continue;
                };
                bytes = bytes
                    .checked_add(entry.bytes)
                    .and_then(|n| n.checked_add(std::mem::size_of::<Arc<Signed>>()))
                    .ok_or(Error::ResourceLimit)?;
                if result.len() == limits.items || bytes > limits.bytes {
                    return Err(Error::ResourceLimit);
                }
                result.push(entry.tx.clone());
            }
        }
        Ok(result)
    }
    pub fn account_nonce(
        &self,
        owner: AccountPublicKey,
        state: &Unsigned,
        strategy: Strategy,
        limits: Limits,
    ) -> Result<Unsigned, Error> {
        let check = |value: Unsigned| {
            if value.as_bytes().len() > limits.bytes {
                Err(Error::ResourceLimit)
            } else {
                Ok(value)
            }
        };
        if state.as_bytes().len() >= limits.bytes {
            return Err(Error::ResourceLimit);
        }
        if strategy == Strategy::Max {
            let maximum = self.max_nonce(owner).unwrap_or(state).max(state);
            if maximum.as_bytes().len() >= limits.bytes {
                return Err(Error::ResourceLimit);
            }
            return check(maximum.add(&1.into()));
        }
        let nonces = self.nonces.get(&Some(owner));
        // The native list includes every transaction. Enforce the complete
        // list's item/nonce-byte budget, even when its first gap appears early.
        let mut count = 0usize;
        let mut bytes = state.as_bytes().len();
        if let Some(nonces) = nonces {
            for (nonce, keys) in nonces {
                count = count.checked_add(keys.len()).ok_or(Error::ResourceLimit)?;
                bytes = nonce
                    .as_bytes()
                    .len()
                    .checked_add(std::mem::size_of::<Unsigned>())
                    .and_then(|n| n.checked_mul(keys.len()))
                    .and_then(|n| n.checked_add(bytes))
                    .ok_or(Error::ResourceLimit)?;
                if count > limits.items || bytes > limits.bytes {
                    return Err(Error::ResourceLimit);
                }
            }
        }
        check(continuity(state, nonces.into_iter().flat_map(|n| n.keys())))
    }
}
