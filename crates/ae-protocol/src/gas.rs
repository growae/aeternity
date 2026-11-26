// aec_governance gas rules, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use crate::Protocol;

pub const TRANSACTION: u64 = 15_000;
pub const TRANSACTION_BYTE: u64 = 20;
pub const STORE_BYTE: u64 = 5;
pub const PAYING_FOR: u64 = TRANSACTION / 5;

pub fn channel_force_progress(protocol: Protocol) -> u64 {
    if protocol >= Protocol::Fortuna {
        30 * TRANSACTION
    } else {
        TRANSACTION
    }
}
