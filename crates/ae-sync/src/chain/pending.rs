// aec_conductor:get_key_block_candidate/get_pending_key_block, fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use ae_codec::headers::KeyHeader;
use ae_types::{AccountPublicKey, Hash256};
use std::sync::Arc;

pub mod service;

// A single owner serializes calls. The host performs each lookup once, with
// no retry or implicit switch to a replacement key worker after acceptance.
pub trait Host {
    type Error;
    fn top(&mut self) -> Result<Option<Hash256>, Self::Error>;
    fn beneficiary(&mut self) -> Result<AccountPublicKey, Self::Error>;
    fn candidate_key(&mut self) -> Result<AccountPublicKey, Self::Error>;
    // None is the native candidate-construction rejection (not_found).
    // Err retains an operational/provider failure rather than hiding it.
    fn create(
        &mut self,
        parent: Hash256,
        beneficiary: AccountPublicKey,
        miner: AccountPublicKey,
    ) -> Result<Option<KeyHeader>, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error<E> {
    NotFound,
    BeneficiaryNotConfigured,
    Host(E),
}

#[derive(Default)]
pub struct Cache {
    candidate: Option<Arc<KeyHeader>>,
}
impl Cache {
    pub fn cached(&self) -> Option<&Arc<KeyHeader>> {
        self.candidate.as_ref()
    }
    pub fn get<H: Host>(
        &mut self,
        host: &mut H,
        has_beneficiary: bool,
    ) -> Result<Arc<KeyHeader>, Error<H::Error>> {
        let top = host.top().map_err(Error::Host)?;
        if let Some(candidate) = &self.candidate
            && top == Some(Hash256(candidate.common.previous_hash))
        {
            return Ok(candidate.clone());
        }
        let top = top.ok_or(Error::NotFound)?;
        if !has_beneficiary {
            return Err(Error::BeneficiaryNotConfigured);
        }
        let beneficiary = host.beneficiary().map_err(Error::Host)?;
        let miner = host.candidate_key().map_err(Error::Host)?;
        let candidate = host
            .create(top, beneficiary, miner)
            .map_err(Error::Host)?
            .ok_or(Error::NotFound)?;
        let candidate = Arc::new(candidate);
        // A failed rebuild keeps the old cache. A later return to that parent
        // can reuse it, regardless of mining state or intervening key changes.
        self.candidate = Some(candidate.clone());
        Ok(candidate)
    }
}

#[cfg(test)]
mod tests;
