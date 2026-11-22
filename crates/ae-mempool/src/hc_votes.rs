// aec_hc_vote_pool, aeternity fa2a6d7.
// Copyright (c) 2024, Aeternity Foundation. ISC notice in repository LICENSE.
use ae_execution::transactions::{self, SignatureError, SignatureLimits};
use ae_protocol::{ConsensusType, NetworkProfile, Protocol};
use ae_tx::{HcVote, SignedTransaction, Transaction, TransactionKind, TransactionLeaf};
use ae_types::{Hash256, Unsigned};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Created,
    Received,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    // A gossip-cache hit bypasses validation, insertion and publication.
    Cached,
    // Publish the caller's signed transaction even when inserted is false.
    Accepted {
        hash: Hash256,
        inserted: bool,
        event: Event,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    OnlyHcVoteAllowed,
    InvalidAtProtocol,
    Signature(SignatureError),
    // Local policy, never a consensus rejection. A retry must remain possible.
    ResourceLimit,
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub maximum_entries: usize,
    pub maximum_retained_wire_bytes: usize,
    pub maximum_encoded_bytes: usize,
    pub maximum_encoding_work_bytes: usize,
    pub signatures: SignatureLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum_entries: 100_000,
            maximum_retained_wire_bytes: 256 * 1024 * 1024,
            maximum_encoded_bytes: 16 * 1024 * 1024,
            maximum_encoding_work_bytes: 256 * 1024 * 1024,
            signatures: SignatureLimits::default(),
        }
    }
}

// Capture these from one selected chain tip (or genesis when no tip exists).
// No account lookup is needed: a plain HC vote's sole signer is its voter.
pub struct Admission<'a> {
    pub network: &'a NetworkProfile,
    pub protocol: Protocol,
    pub consensus: ConsensusType,
    pub limits: Limits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VotePool {
    epoch: Unsigned,
    votes: BTreeMap<Hash256, HcVote>,
    epochs: BTreeMap<Unsigned, Vec<Hash256>>,
    types: BTreeMap<(Unsigned, Unsigned), Vec<Hash256>>,
    retained_wire_bytes: usize,
}
impl Default for VotePool {
    fn default() -> Self {
        Self {
            epoch: 0.into(),
            votes: BTreeMap::new(),
            epochs: BTreeMap::new(),
            types: BTreeMap::new(),
            retained_wire_bytes: 0,
        }
    }
}
impl VotePool {
    pub fn epoch(&self) -> &Unsigned {
        &self.epoch
    }
    pub fn len(&self) -> usize {
        self.votes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.votes.is_empty()
    }
    pub fn retained_wire_bytes(&self) -> usize {
        self.retained_wire_bytes
    }
    pub fn peek<'a>(
        &'a self,
        epoch: &Unsigned,
        vote_type: Option<&Unsigned>,
    ) -> impl Iterator<Item = &'a HcVote> {
        let hashes = match vote_type {
            Some(kind) => self.types.get(&(epoch.clone(), kind.clone())),
            None => self.epochs.get(epoch),
        };
        hashes
            .into_iter()
            .flatten()
            .rev()
            .map(|hash| &self.votes[hash])
    }
    pub fn update_epoch(&mut self, epoch: Unsigned) {
        // The first epoch and backward/equal notifications keep all votes.
        // A later forward notification clears ALL epochs, including future votes.
        if self.epoch != 0.into() && epoch > self.epoch {
            self.votes.clear();
            self.epochs.clear();
            self.types.clear();
            self.retained_wire_bytes = 0;
        }
        self.epoch = epoch;
    }
    pub fn push(
        &mut self,
        signed: &SignedTransaction<Transaction>,
        event: Event,
        admission: &Admission<'_>,
        gossip_contains: impl FnOnce(Hash256) -> bool,
    ) -> Result<PushOutcome, Error> {
        let tx = signed.transaction().view();
        let limits = admission.limits;
        let (hash, encoded_bytes) = encoded(signed, limits)?;
        if gossip_contains(hash) {
            return Ok(PushOutcome::Cached);
        }
        if tx.kind() != TransactionKind::HcVote {
            return Err(Error::OnlyHcVoteAllowed);
        }
        let TransactionLeaf::HcVote(vote) = tx.leaf() else {
            unreachable!("plain vote kind")
        };
        if !vote.valid_at_consensus(admission.consensus) {
            return Err(Error::InvalidAtProtocol);
        }
        transactions::verify(
            tx,
            signed.signatures(),
            admission.network,
            admission.protocol,
            false,
            limits.signatures,
        )
        .map_err(Error::Signature)?;
        let inserted = !self.votes.contains_key(&hash);
        if inserted {
            let bytes = self
                .retained_wire_bytes
                .checked_add(encoded_bytes)
                .ok_or(Error::ResourceLimit)?;
            if self.votes.len() >= limits.maximum_entries
                || bytes > limits.maximum_retained_wire_bytes
            {
                return Err(Error::ResourceLimit);
            }
            self.votes.insert(hash, vote.clone());
            self.epochs
                .entry(vote.epoch().clone())
                .or_default()
                .push(hash);
            self.types
                .entry((vote.epoch().clone(), vote.vote_type().clone()))
                .or_default()
                .push(hash);
            self.retained_wire_bytes = bytes;
        }
        Ok(PushOutcome::Accepted {
            hash,
            inserted,
            event,
        })
    }
}

// Performs the same bounded hashing used by push, without reading selected
// chain state. Runtime owners use this for the native cache-before-validation
// fast path; a cache miss still goes through the complete push checks.
pub fn admission_hash(
    signed: &SignedTransaction<Transaction>,
    limits: Limits,
) -> Result<Hash256, Error> {
    encoded(signed, limits).map(|(hash, _)| hash)
}
fn encoded(
    signed: &SignedTransaction<Transaction>,
    limits: Limits,
) -> Result<(Hash256, usize), Error> {
    let tx = signed.transaction().view();
    // Nested envelopes repeatedly encode their inner bytes. Bound their
    // total work before hashing, including the gossip-cache fast path.
    let encoding_work = tx
        .layers()
        .iter()
        .try_fold(tx.leaf().wire_size(), |n, layer| {
            n.checked_add(layer.wire_size())
        })
        .and_then(|n| n.checked_add(signed.signatures().len().saturating_mul(64)));
    if tx.layers().len() > limits.signatures.max_layers
        || encoding_work.is_none_or(|n| n > limits.maximum_encoding_work_bytes)
    {
        return Err(Error::ResourceLimit);
    }
    // Bound large signatures/transactions before constructing hash input.
    if tx.wire_size() > limits.maximum_encoded_bytes
        || signed.signatures().len().saturating_mul(64) > limits.maximum_encoded_bytes
    {
        return Err(Error::ResourceLimit);
    }
    let encoded = signed.encode();
    if encoded.len() > limits.maximum_encoded_bytes {
        return Err(Error::ResourceLimit);
    }
    Ok((ae_crypto::blake2b_256(&encoded), encoded.len()))
}
