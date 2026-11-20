// aec_blocks body admission, aeternity fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{
    block::{Block, MicroBlock},
    fraud,
};
use ae_codec::headers::Header;
use ae_protocol::{NetworkProfile, Protocol};
use num_bigint::{BigInt, Sign};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BodyError {
    Protocol(ae_protocol::ProfileError),
    Tree(ae_trie::Error),
    Fraud(fraud::Error),
    TransactionsHash,
    GasLimit,
    MinimumFee,
    ProofHash,
    ResourceLimit,
}
impl BodyError {
    pub fn is_operational(&self) -> bool {
        matches!(
            self,
            Self::ResourceLimit | Self::Tree(_) | Self::Protocol(_)
        )
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_transactions: usize,
    pub max_signed_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_transactions: 100_000,
            max_signed_bytes: 16 * 1024 * 1024,
        }
    }
}
fn protocol(b: &MicroBlock) -> Result<Protocol, BodyError> {
    Protocol::try_from(b.header.common.version).map_err(BodyError::Protocol)
}
pub fn gas(b: &MicroBlock) -> Result<BigInt, BodyError> {
    let p = protocol(b)?;
    Ok(b.transactions
        .iter()
        .map(|s| {
            let pricing = s
                .transaction()
                .view()
                .pricing(&b.header.common.height.into(), p);
            if p <= Protocol::Iris {
                pricing.gas_limit
            } else {
                pricing.minimum_block_gas
            }
        })
        .sum())
}
pub fn validate_body(b: &MicroBlock, gas_limit: &BigInt, limits: Limits) -> Result<(), BodyError> {
    if b.transactions.len() > limits.max_transactions {
        return Err(BodyError::ResourceLimit);
    }
    let mut remaining = limits.max_signed_bytes;
    let mut txs = Vec::with_capacity(b.transactions.len());
    for s in &b.transactions {
        if s.transaction().view().wire_size() > remaining {
            return Err(BodyError::ResourceLimit);
        }
        let bytes = s.encode();
        remaining = remaining
            .checked_sub(bytes.len())
            .ok_or(BodyError::ResourceLimit)?;
        txs.push(bytes);
    }
    let root = ae_trie::transaction_root(txs.iter().map(Vec::as_slice)).map_err(BodyError::Tree)?;
    if root.0 != b.header.transactions_hash {
        return Err(BodyError::TransactionsHash);
    }
    if &gas(b)? > gas_limit {
        return Err(BodyError::GasLimit);
    }
    let p = protocol(b)?;
    for s in &b.transactions {
        let tx = s.transaction().view();
        let fee = BigInt::from_bytes_be(Sign::Plus, tx.fee().as_bytes());
        if fee < tx.pricing(&b.header.common.height.into(), p).minimum_fee {
            return Err(BodyError::MinimumFee);
        }
    }
    if let Some(proof) = &b.proof {
        let hash = proof.hash().map_err(BodyError::Fraud)?;
        if b.header.proof_of_fraud != Some(hash.0) {
            return Err(BodyError::ProofHash);
        }
        proof.validate().map_err(BodyError::Fraud)?;
    }
    Ok(())
}
#[derive(Debug)]
pub enum PowError {
    Header(ae_consensus::headers::Error),
    Body(BodyError),
}
#[derive(Debug, Clone)]
pub struct PowRules {
    pub now_ms: u64,
    pub micro_cycle_ms: u64,
    pub configured_gas_limit: BigInt,
    pub body_limits: Limits,
}
// Standalone admission includes the original opportunistic timestamp check.
// The caller supplies a previous header only when its hash matches the lookup.
pub fn validate_pow(
    b: &Block,
    network: &NetworkProfile,
    known_previous: Option<&Header>,
    rules: &PowRules,
) -> Result<(), PowError> {
    let header = b.header();
    let micro = match b {
        Block::Micro(m) => Some(m),
        _ => None,
    };
    validate_pow_parts(&header, micro, network, known_previous, rules)
}
pub fn validate_pow_micro(
    b: &MicroBlock,
    network: &NetworkProfile,
    known_previous: Option<&Header>,
    rules: &PowRules,
) -> Result<(), PowError> {
    validate_pow_parts(
        &Header::Micro(b.header.clone()),
        Some(b),
        network,
        known_previous,
        rules,
    )
}
fn validate_pow_parts(
    header: &Header,
    micro: Option<&MicroBlock>,
    network: &NetworkProfile,
    known_previous: Option<&Header>,
    rules: &PowRules,
) -> Result<(), PowError> {
    network
        .validate_version(header.common().height, header.common().version)
        .map_err(|e| PowError::Header(e.into()))?;
    if let (Header::Micro(m), Some(previous)) = (header, known_previous) {
        let interval = if matches!(previous, Header::Micro(_)) {
            rules.micro_cycle_ms
        } else {
            1
        };
        if u128::from(m.time)
            < u128::from(ae_consensus::headers::timestamp(previous)) + u128::from(interval)
        {
            return Err(PowError::Header(
                ae_consensus::headers::Error::MicroInterval,
            ));
        }
    }
    ae_consensus::headers::validate_standalone(header, network, rules.now_ms)
        .map_err(PowError::Header)?;
    if let Some(m) = micro {
        let gas_limit = network
            .fixed_block_gas_limit()
            .map_or_else(|| rules.configured_gas_limit.clone(), BigInt::from);
        validate_body(m, &gas_limit, rules.body_limits).map_err(PowError::Body)?;
    }
    Ok(())
}
