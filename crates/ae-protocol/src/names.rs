// Public-network governance at aeternity fa2a6d7, aec_governance.erl.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use crate::Protocol;
use ae_types::Unsigned;

pub const PRECLAIM_EXPIRATION: u64 = 300;
pub const PRECLAIM_DELAY: u64 = 1;
pub const PROTECTION_PERIOD: u64 = 2016;
pub const LEGACY_LOCKED_FEE: u64 = 3;
pub const BID_INCREMENT_PERCENT: u64 = 5;

pub fn max_expiration(protocol: Protocol) -> u64 {
    if protocol < Protocol::Iris {
        50_000
    } else {
        180_000
    }
}
// Callers supply the IDNA-normalized label byte length, excluding registrar.
// A development override must be resolved by its explicit network profile.
pub fn bid_timeout(size: usize, protocol: Protocol, development_override: Option<u64>) -> u64 {
    if protocol < Protocol::Lima {
        return 0;
    }
    if let Some(value) = development_override {
        return value;
    }
    match (size, protocol < Protocol::Ceres) {
        (0..=4, true) => 29_760,
        (0..=4, false) => 2_400,
        (5..=8, true) => 14_880,
        (5..=8, false) => 960,
        (9..=12, _) => 480,
        _ => 0,
    }
}
pub fn bid_extension(size: usize, protocol: Protocol, development_override: Option<u64>) -> u64 {
    if protocol < Protocol::Ceres {
        bid_timeout(size, protocol, development_override)
    } else if size <= 12 {
        240
    } else {
        0
    }
}
pub fn claim_fee(size: std::num::NonZeroUsize, protocol: Protocol) -> Unsigned {
    if protocol < Protocol::Lima {
        return 0.into();
    }
    let (mut a, mut b) = (2u64, 3u64);
    for _ in size.get().min(31)..31 {
        (a, b) = (b, a + b);
    }
    Unsigned::from(b).mul_u64(100_000_000_000_000)
}
pub fn valid_overbid(new: &Unsigned, old: &Unsigned) -> bool {
    new.checked_sub(old)
        .is_ok_and(|difference| difference.mul_u64(100) >= old.mul_u64(BID_INCREMENT_PERCENT))
}
