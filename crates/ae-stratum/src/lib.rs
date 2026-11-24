#![forbid(unsafe_code)]
// Aeternity fa2a6d7 and aestratum_lib 17b56c5: deterministic pool work state.
// Transport, registry ownership, persistence and node admission are separate.
pub mod job;
pub mod nonce;
pub mod queue;
pub mod share;
pub mod target;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Nonce,
    Hex,
    Integer,
    Arithmetic,
    Float,
    Version,
    Hash,
    Configuration,
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
