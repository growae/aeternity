// SPDX-License-Identifier: Apache-2.0
// Adapted from OTP-26.2.5.21 erl_term_hashing.c and erl_map.c.
// Copyright Ericsson AB 2022-2023; 2014-2026. All Rights Reserved.
// Modified: explicit word/atom/mixing profiles, safe Rust and work budgets.
// License text: licenses/Apache-2.0.txt at the repository root.
use super::{Mixing, Otp26, Term, WordSize};
use crate::{Budget, Error};
use num_bigint::Sign;
use num_traits::ToPrimitive;
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
fn mix(profile: &Otp26, hash: &mut u32, x: u32, y: u32, c: u32) {
    let a = c.wrapping_add(x);
    let b = c.wrapping_add(y);
    *hash = match profile.mixing {
        Mixing::Jenkins => jenkins(a, b, *hash).2,
        Mixing::Crc32c => crc32c(crc32c(*hash, a).wrapping_add(*hash), b),
    };
}
fn immediate(profile: &Otp26, hash: &mut u32, word: u64) {
    mix(
        profile,
        hash,
        word as u32,
        if profile.word_size == WordSize::Bits64 {
            (word >> 32) as u32
        } else {
            0
        },
        H,
    );
}
fn const_hash(hash: &mut u32, c: u32) {
    *hash = (*hash ^ c).rotate_left(17);
}
// block_hash is compiled before OTP redefines MIX for internal CRC32C: binary
// blocks always use the original Jenkins function, independently of the profile.
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
pub(super) fn term(
    profile: &Otp26,
    term: &Term<'_>,
    budget: &mut Budget,
    depth: usize,
) -> Result<u32, Error> {
    let mut hash = 3432918353;
    walk(profile, term, &mut hash, budget, depth)?;
    Ok(hash)
}
fn walk(
    profile: &Otp26,
    value: &Term<'_>,
    hash: &mut u32,
    budget: &mut Budget,
    depth: usize,
) -> Result<(), Error> {
    budget.visit(depth)?;
    match value {
        Term::Integer(n) => {
            let bits = if profile.word_size == WordSize::Bits64 {
                59
            } else {
                27
            };
            if let Some(n) = n
                .to_i64()
                .filter(|&n| n >= -(1i64 << bits) && n < (1i64 << bits))
            {
                immediate(profile, hash, ((n as u64) << 4) | 15);
            } else {
                let con = constant(if n.sign() == Sign::Minus { 10 } else { 11 });
                for digit in n.magnitude().iter_u64_digits() {
                    budget.visit(depth)?;
                    mix(profile, hash, digit as u32, (digit >> 32) as u32, con);
                }
            }
        }
        Term::Atom(atom) => immediate(
            profile,
            hash,
            ((*profile.atoms.get(atom).ok_or(Error::InvalidOrderProfile)? as u64) << 6) | 11,
        ),
        Term::Binary(bytes) => {
            if bytes.len() > u32::MAX as usize {
                return Err(Error::ResourceLimit);
            }
            let con = hash.wrapping_add(constant(13));
            *hash = if bytes.is_empty() {
                con
            } else {
                binary(bytes, con)
            };
        }
        Term::Tuple(vs) => {
            mix(profile, hash, vs.len() as u32, 0, constant(9));
            for v in vs {
                walk(profile, v, hash, budget, depth + 1)?;
            }
        }
        Term::List(vs) => {
            if vs.is_empty() {
                immediate(profile, hash, 59)
            } else {
                list(
                    profile,
                    &vs.iter().collect::<Vec<_>>(),
                    None,
                    hash,
                    budget,
                    depth + 1,
                )?;
            }
        }
        Term::Map(rows) => {
            mix(profile, hash, rows.len() as u32, 0, constant(16));
            if rows.len() <= 32 {
                for (k, v) in rows {
                    walk(profile, k, hash, budget, depth + 1)?;
                    walk(profile, v, hash, budget, depth + 1)?;
                }
            } else {
                let hashes: Vec<_> = rows
                    .iter()
                    .map(|(k, _)| term(profile, k, budget, depth + 1))
                    .collect::<Result<_, _>>()?;
                let mut indices: Vec<_> = (0..rows.len()).collect();
                indices.sort_by_key(|&i| nibble_key(hashes[i]));
                hamt(profile, rows, &indices, &hashes, 0, hash, budget, depth + 1)?;
            }
        }
    }
    Ok(())
}
fn byte(v: &Term<'_>) -> Option<u8> {
    if let Term::Integer(n) = v {
        n.to_u8()
    } else {
        None
    }
}
fn list(
    profile: &Otp26,
    vs: &[&Term<'_>],
    tail: Option<&Term<'_>>,
    hash: &mut u32,
    budget: &mut Budget,
    depth: usize,
) -> Result<(), Error> {
    let mut i = 0;
    while i < vs.len() {
        let mut n = 0;
        let mut packed = 0u32;
        while i < vs.len() {
            let Some(b) = byte(vs[i]) else { break };
            budget.visit(depth)?;
            packed = (packed << 8).wrapping_add(b as u32);
            i += 1;
            n += 1;
            if n == 4 {
                mix(profile, hash, packed, 0, constant(4));
                n = 0;
                packed = 0;
            }
        }
        if n > 0 {
            mix(profile, hash, packed, n, constant(22));
        }
        if i == vs.len() {
            break;
        }
        const_hash(hash, constant(17));
        walk(profile, vs[i], hash, budget, depth + 1)?;
        i += 1;
        if i == vs.len() && !matches!(tail,Some(Term::List(v))if !v.is_empty()) {
            const_hash(hash, constant(18));
        }
    }
    match tail {
        Some(v) => walk(profile, v, hash, budget, depth + 1)?,
        None => immediate(profile, hash, 59),
    }
    Ok(())
}
#[allow(clippy::too_many_arguments)]
fn hamt(
    profile: &Otp26,
    rows: &[(Term<'_>, Term<'_>)],
    indices: &[usize],
    hashes: &[u32],
    level: u32,
    hash: &mut u32,
    budget: &mut Budget,
    depth: usize,
) -> Result<(), Error> {
    budget.visit(depth)?;
    if level == 8 {
        // Collision nodes are real tuples of improper [key | value] cells.
        mix(profile, hash, indices.len() as u32, 0, constant(9));
        for &i in indices {
            let (k, v) = &rows[i];
            if let Term::List(tail) = v {
                let cells: Vec<_> = std::iter::once(k).chain(tail).collect();
                list(profile, &cells, None, hash, budget, depth + 1)?;
            } else {
                list(profile, &[k], Some(v), hash, budget, depth + 1)?;
            }
        }
        return Ok(());
    }
    let mut end = indices.len();
    while end > 0 {
        let nibble = (hashes[indices[end - 1]] >> (level * 4)) & 15;
        let mut start = end - 1;
        while start > 0 && (hashes[indices[start - 1]] >> (level * 4)) & 15 == nibble {
            start -= 1;
        }
        if end - start == 1 {
            let (k, v) = &rows[indices[start]];
            walk(profile, k, hash, budget, depth + 1)?;
            walk(profile, v, hash, budget, depth + 1)?;
        } else {
            hamt(
                profile,
                rows,
                &indices[start..end],
                hashes,
                level + 1,
                hash,
                budget,
                depth + 1,
            )?;
        }
        end = start;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Limits, maps::order::Atom, value::Value};
    use serde_json::Value as Json;

    #[test]
    fn original_c_mix_macros_and_numeric_word_boundaries_match() {
        let fixture: Json = serde_json::from_str(include_str!(
            "../../../tests/fixtures/otp26_hash_kernels.json"
        ))
        .unwrap();
        let profile = |word_size, mixing| {
            Otp26::new(
                word_size,
                mixing,
                Atom::ALL
                    .into_iter()
                    .enumerate()
                    .map(|(i, a)| (a, i as u32))
                    .collect(),
            )
            .unwrap()
        };
        for (mixing, column) in [(Mixing::Jenkins, 4), (Mixing::Crc32c, 5)] {
            let p = profile(WordSize::Bits64, mixing);
            for row in fixture["mixes"].as_array().unwrap() {
                let u = |i: usize| row[i].as_u64().unwrap() as u32;
                let mut hash = u(0);
                mix(&p, &mut hash, u(1), u(2), u(3));
                assert_eq!(hash, u(column), "{mixing:?}: {row}");
            }
            for row in fixture["integers"].as_array().unwrap() {
                let word_size = if row["word_size"] == 32 {
                    WordSize::Bits32
                } else {
                    WordSize::Bits64
                };
                let p = profile(word_size, mixing);
                let value = Value::Integer(row["integer"].as_str().unwrap().parse().unwrap());
                let expected = fixture["mixes"][row["index"].as_u64().unwrap() as usize][column]
                    .as_u64()
                    .unwrap() as u32;
                assert_eq!(
                    p.hash(&value, Limits::default()).unwrap(),
                    expected,
                    "{word_size:?} {mixing:?}: {value:?}"
                );
            }
        }
    }
}
