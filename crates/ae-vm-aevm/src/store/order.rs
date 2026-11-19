// SPDX-License-Identifier: Apache-2.0
// Adapted from OTP-26.2.5.21 erl_term_hashing.c and erl_map.c.
// Copyright Ericsson AB 2022-2023; 2014-2026. All Rights Reserved.
// Modified: unsigned/binary keys, explicit 64-bit mixing profile and Rust budgets.
// License text: licenses/Apache-2.0.txt at the repository root.
use super::{Budget, Error};
use num_bigint::BigUint;
use num_traits::ToPrimitive;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderProfile {
    Otp26Word64Jenkins,
    Otp26Word64Crc32c,
}
const H: u32 = 0x9e3779b9;
fn constant(n: u32) -> u32 {
    H.wrapping_mul(n)
}
pub(super) fn nibble_key(hash: u32) -> u32 {
    let mut n = hash;
    let mut r = 0;
    for _ in 0..8 {
        r = (r << 4) | (n & 15);
        n >>= 4;
    }
    r
}
fn jenkins(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    a = a.wrapping_sub(b).wrapping_sub(c);
    a ^= c >> 13;
    b = b.wrapping_sub(c).wrapping_sub(a);
    b ^= a << 8;
    c = c.wrapping_sub(a).wrapping_sub(b);
    c ^= b >> 13;
    a = a.wrapping_sub(b).wrapping_sub(c);
    a ^= c >> 12;
    b = b.wrapping_sub(c).wrapping_sub(a);
    b ^= a << 16;
    c = c.wrapping_sub(a).wrapping_sub(b);
    c ^= b >> 5;
    a = a.wrapping_sub(b).wrapping_sub(c);
    a ^= c >> 3;
    b = b.wrapping_sub(c).wrapping_sub(a);
    b ^= a << 10;
    c = c.wrapping_sub(a).wrapping_sub(b);
    c ^= b >> 15;
    (a, b, c)
}
fn crc32c(mut crc: u32, word: u32) -> u32 {
    crc ^= word;
    for _ in 0..32 {
        crc = (crc >> 1) ^ if crc & 1 != 0 { 0x82f63b78 } else { 0 };
    }
    crc
}
fn binary(bytes: &[u8], init: u32) -> u32 {
    let (mut a, mut b, mut c) = (H, H, init);
    let mut chunks = bytes.chunks_exact(12);
    for v in chunks.by_ref() {
        a = a.wrapping_add(u32::from_le_bytes(v[..4].try_into().unwrap()));
        b = b.wrapping_add(u32::from_le_bytes(v[4..8].try_into().unwrap()));
        c = c.wrapping_add(u32::from_le_bytes(v[8..12].try_into().unwrap()));
        (a, b, c) = jenkins(a, b, c);
    }
    c = c.wrapping_add(bytes.len() as u32);
    for (i, &n) in chunks.remainder().iter().enumerate() {
        match i {
            0..=3 => a = a.wrapping_add((n as u32) << (8 * i)),
            4..=7 => b = b.wrapping_add((n as u32) << (8 * (i - 4))),
            _ => c = c.wrapping_add((n as u32) << (8 * (i - 7))),
        }
    }
    jenkins(a, b, c).2
}
fn mix(profile: OrderProfile, hash: &mut u32, x: u32, y: u32, c: u32) {
    let a = c.wrapping_add(x);
    let b = c.wrapping_add(y);
    *hash = match profile {
        OrderProfile::Otp26Word64Jenkins => jenkins(a, b, *hash).2,
        OrderProfile::Otp26Word64Crc32c => crc32c(crc32c(*hash, a).wrapping_add(*hash), b),
    };
}
fn integer(profile: OrderProfile, n: &BigUint, b: &mut Budget) -> Result<u32, Error> {
    let mut hash = 3432918353;
    if let Some(n) = n.to_u64().filter(|n| *n < (1 << 59)) {
        let word = (n << 4) | 15;
        mix(profile, &mut hash, word as u32, (word >> 32) as u32, H);
    } else {
        for digit in n.iter_u64_digits() {
            b.work(1)?;
            mix(
                profile,
                &mut hash,
                digit as u32,
                (digit >> 32) as u32,
                constant(11),
            );
        }
    }
    Ok(hash)
}
fn indices(hashes: &[u32], reverse: bool, b: &mut Budget) -> Result<Vec<usize>, Error> {
    b.bytes(hashes.len().checked_mul(16).ok_or(Error::ResourceLimit)?)?;
    b.work(
        hashes
            .len()
            .checked_mul(hashes.len().max(1).ilog2() as usize + 1)
            .ok_or(Error::ResourceLimit)?,
    )?;
    let mut order: Vec<_> = (0..hashes.len()).collect();
    if hashes.len() > 32 {
        order.sort_by_key(|&i| nibble_key(hashes[i]));
        if reverse {
            order.reverse();
        }
    }
    Ok(order)
}
// Inputs are already in ascending Erlang term order. Full hash collisions retain
// that order before the ToList/keys/values reversal.
pub(super) fn integers(
    keys: &[BigUint],
    profile: OrderProfile,
    fold: bool,
    b: &mut Budget,
) -> Result<Vec<usize>, Error> {
    b.work(keys.len())?;
    b.bytes(keys.len().checked_mul(4).ok_or(Error::ResourceLimit)?)?;
    let hashes = keys
        .iter()
        .map(|n| integer(profile, n, b))
        .collect::<Result<Vec<_>, _>>()?;
    indices(&hashes, !fold, b)
}
pub(super) fn binaries(keys: &[&[u8]], b: &mut Budget) -> Result<Vec<usize>, Error> {
    binary_indices(keys, false, b)
}
pub(crate) fn binary_indices(
    keys: &[&[u8]],
    fold: bool,
    b: &mut Budget,
) -> Result<Vec<usize>, Error> {
    b.work(keys.len())?;
    b.bytes(keys.len().checked_mul(4).ok_or(Error::ResourceLimit)?)?;
    let mut hashes = Vec::new();
    for key in keys {
        if key.len() > u32::MAX as usize {
            return Err(Error::ResourceLimit);
        }
        b.work(key.len().div_ceil(12))?;
        let con = 3432918353u32.wrapping_add(constant(13));
        hashes.push(if key.is_empty() {
            con
        } else {
            binary(key, con)
        });
    }
    indices(&hashes, !fold, b)
}
