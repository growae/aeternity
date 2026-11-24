#![forbid(unsafe_code)]
// Aeternity fa2a6d7 and aestratum_lib 17b56c5: pool work and session services.
// Transport, persistence and node admission are separate.
pub mod accounting;
pub mod candidate;
pub mod ingress;
pub mod job;
pub mod nonce;
pub mod payout;
pub mod proof;
pub mod queue;
pub mod registry;
pub mod rewards;
pub mod runtime;
pub mod services;
pub mod session;
pub mod share;
pub mod target;
pub mod wire;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Nonce,
    Hex,
    Integer,
    Arithmetic,
    Float,
    Proof,
    // Original Cuckoo numeric fold raises badarith. This is terminal for the
    // submitting session, not an invalid_solution response or retained error.
    ProofArithmetic,
    Version,
    Hash,
    Configuration,
    Entropy,
    NotEnoughJobs,
    Limit(&'static str),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

pub(crate) const MAX_INTEGER_BITS: u64 = 16384;
pub(crate) fn integer(value: &num_bigint::BigInt) -> Result<(), Error> {
    if value.bits() > MAX_INTEGER_BITS {
        Err(Error::Limit("integer"))
    } else {
        Ok(())
    }
}
