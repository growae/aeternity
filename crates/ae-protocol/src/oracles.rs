// Aeternity fa2a6d7, aeo_utils, aec_governance and aect_contracts.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use crate::Protocol;
use ae_types::Unsigned;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TtlError;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ttl {
    Delta(Unsigned),
    Block(Unsigned),
}
impl Ttl {
    pub fn delta(&self, height: &Unsigned) -> Result<Unsigned, TtlError> {
        match self {
            Self::Delta(d) => Ok(d.clone()),
            Self::Block(h) if h > height => h.checked_sub(height).map_err(|_| TtlError),
            Self::Block(_) => Err(TtlError),
        }
    }
    pub fn expiry(&self, height: &Unsigned) -> Result<Unsigned, TtlError> {
        Ok(height.add(&self.delta(height)?))
    }
}
pub const STATE_GAS_PART: u64 = 32_000;
pub const STATE_GAS_WHOLE: u64 = 175_200;
pub fn state_gas(delta: &Unsigned) -> Unsigned {
    delta
        .mul_u64(STATE_GAS_PART)
        .add(&(STATE_GAS_WHOLE - 1).into())
        .div_rem_u64(STATE_GAS_WHOLE)
        .expect("nonzero denominator")
        .0
}
pub fn abi_available(abi: &Unsigned, protocol: Protocol) -> bool {
    match abi.to_u64() {
        Ok(0) => true,
        Ok(1) => protocol <= Protocol::Lima,
        Ok(3) => protocol >= Protocol::Lima,
        _ => false,
    }
}
