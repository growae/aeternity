// aec_eoe_gen_vote/aec_eoe_fork_vote/aec_eoe_length_vote, aeternity fa2a6d7.
// Copyright (c) 2024, Aeternity. ISC notice in repository LICENSE.
use crate::parent_block::ParentBlock;
use ae_tx::{SignedTransaction, Transaction};
use ae_types::{AccountPublicKey, Unsigned};
use num_bigint::{BigInt, Sign};
use std::collections::{BTreeMap, BTreeSet};

mod callbacks;
pub mod finalization;
pub mod local_host;
mod machine;
pub mod payload;
pub use callbacks::{ClientState, FinalizeInput, calculate_delta, finalize_calldata};
pub use machine::{
    Event, Host, Machine, Negotiation, Reply, ResultState, State, StepError, Timeout, Transition,
    TransitionKind, majority,
};
pub use payload::{Field, Payload};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Fork,
    Length,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteType {
    Proposal,
    Vote,
    Commit,
}
impl Mode {
    pub fn type_number(self, kind: VoteType) -> Unsigned {
        let base = match self {
            Self::Fork => 0,
            Self::Length => 3,
        };
        (base
            + match kind {
                VoteType::Proposal => 1,
                VoteType::Vote => 2,
                VoteType::Commit => 3,
            })
        .into()
    }
    pub fn vote_type(self, n: &Unsigned) -> Option<VoteType> {
        [VoteType::Proposal, VoteType::Vote, VoteType::Commit]
            .into_iter()
            .find(|k| &self.type_number(*k) == n)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Native {
    Badarg,
    Badarith,
    FunctionClause,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Native(Native),
    ResourceLimit,
    Fate(ae_vm_fate::Error),
    InvalidOrder,
    MissingKey(AccountPublicKey),
}
impl From<ae_vm_fate::Error> for Failure {
    fn from(e: ae_vm_fate::Error) -> Self {
        Self::Fate(e)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub maximum_bytes: usize,
    pub maximum_fields: usize,
    pub maximum_validators: usize,
    pub maximum_buffered_votes: usize,
    pub maximum_parent_blocks: usize,
    pub fate: ae_vm_fate::Limits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum_bytes: 16 * 1024 * 1024,
            maximum_fields: 10_000,
            maximum_validators: 100_000,
            maximum_buffered_votes: 100_000,
            maximum_parent_blocks: 100_000,
            fate: Default::default(),
        }
    }
}
fn big(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}

struct Budget {
    bytes: usize,
    limits: Limits,
}
impl Budget {
    fn new(limits: Limits) -> Self {
        Self { bytes: 0, limits }
    }
    fn add(&mut self, bytes: usize) -> Result<(), Failure> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(Failure::ResourceLimit)?;
        if self.bytes > self.limits.maximum_bytes {
            Err(Failure::ResourceLimit)
        } else {
            Ok(())
        }
    }
    fn integer(&mut self, value: &BigInt) -> Result<(), Failure> {
        // A lower bound on decimal length avoids formatting an unbounded int.
        if value.bits().saturating_sub(1) / 4 > self.limits.maximum_bytes as u64 {
            return Err(Failure::ResourceLimit);
        }
        self.add(value.to_string().len())
    }
    fn payload(&mut self, fields: &Payload) -> Result<(), Failure> {
        if fields.len() > self.limits.maximum_fields {
            return Err(Failure::ResourceLimit);
        }
        for (key, value) in fields {
            self.add(key.len())?;
            match value {
                Field::Binary(b) => self.add(b.len())?,
                Field::Integer(n) => self.integer(n)?,
            }
        }
        Ok(())
    }
    fn signed(&mut self, signed: &SignedTransaction<Transaction>) -> Result<(), Failure> {
        let view = signed.transaction().view();
        self.add(
            signed
                .signatures()
                .len()
                .checked_mul(64)
                .ok_or(Failure::ResourceLimit)?,
        )?;
        self.add(view.leaf().wire_size())?;
        for layer in view.layers() {
            self.add(layer.wire_size())?;
        }
        Ok(())
    }
    fn parent(&mut self, block: &ParentBlock) -> Result<(), Failure> {
        for bytes in [
            &block.hash[..],
            &block.previous_hash,
            block.height.as_bytes(),
            block.time.as_bytes(),
        ] {
            self.add(bytes.len())?;
        }
        Ok(())
    }
}
