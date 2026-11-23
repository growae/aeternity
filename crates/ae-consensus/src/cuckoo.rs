// Behavioral reference: aeminer c6b6a6d, aeminer_pow_cuckoo.erl and aeminer_siphash24.erl.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::target::Target;
use ae_codec::headers::{Header, HeaderError, KeyHeader};
use ae_crypto::blake2b_256;
use ae_types::Hash256;
use base64::{Engine, engine::general_purpose::STANDARD};
use std::collections::BTreeMap;

pub mod solver;

pub const MAINNET_EDGE_BITS: u8 = 29;

pub fn sealing_bytes(header: &KeyHeader) -> Result<Vec<u8>, HeaderError> {
    let mut blank = header.clone();
    blank.nonce = 0;
    blank.seal = [0; 42];
    Header::Key(blank).encode()
}
pub fn verify_key_seal(header: &KeyHeader) -> Result<bool, HeaderError> {
    let solution: Vec<u64> = header.seal.iter().map(|x| u64::from(*x)).collect();
    Ok(verify(
        &sealing_bytes(header)?,
        header.nonce,
        &solution,
        Target(header.target),
        MAINNET_EDGE_BITS,
    ))
}
pub fn solution_hash(solution: &[u64], edge_bits: u8) -> Option<Hash256> {
    if !(1..=63).contains(&edge_bits) {
        return None;
    }
    let mut sorted = solution.to_vec();
    sorted.sort();
    let mut bytes = Vec::with_capacity(sorted.len() * if edge_bits > 31 { 8 } else { 4 });
    for edge in sorted {
        if edge_bits > 31 {
            bytes.extend_from_slice(&edge.to_be_bytes());
        } else {
            bytes.extend_from_slice(&(edge as u32).to_be_bytes());
        }
    }
    Some(blake2b_256(&bytes))
}
pub fn verify(data: &[u8], nonce: u64, solution: &[u64], target: Target, edge_bits: u8) -> bool {
    if solution.len() != 42
        || !solution_hash(solution, edge_bits).is_some_and(|hash| target.accepts(&hash))
    {
        return false;
    }
    verify_proof_from_hash(blake2b_256(data), nonce, solution, edge_bits)
}
pub fn verify_proof_from_hash(hash: Hash256, nonce: u64, solution: &[u64], edge_bits: u8) -> bool {
    if solution.len() != 42 || !(1..=63).contains(&edge_bits) {
        return false;
    }
    let mask = (1u64 << edge_bits) - 1;
    if solution.iter().any(|x| *x > mask) || solution.windows(2).any(|p| p[0] >= p[1]) {
        return false;
    }
    let keys = keys(hash, nonce);
    let edges: Vec<(u64, u64)> = solution
        .iter()
        .map(|n| {
            (
                (siphash(keys, 2 * n) & mask) << 1,
                ((siphash(keys, 2 * n + 1) & mask) << 1) | 1,
            )
        })
        .collect();
    is_single_cycle(&edges)
}
fn keys(hash: Hash256, nonce: u64) -> [u64; 4] {
    let mut header = STANDARD.encode(hash.0).into_bytes();
    header.extend_from_slice(STANDARD.encode(nonce.to_le_bytes()).as_bytes());
    header.resize(80, 0);
    let seed = blake2b_256(&header);
    std::array::from_fn(|i| {
        u64::from_le_bytes(
            seed.0[8 * i..8 * i + 8]
                .try_into()
                .expect("fixed key slice"),
        )
    })
}
fn is_single_cycle(edges: &[(u64, u64)]) -> bool {
    let mut adjacent: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    for (index, (a, b)) in edges.iter().enumerate() {
        adjacent.entry(*a).or_default().push(index);
        adjacent.entry(*b).or_default().push(index);
    }
    if adjacent.len() != 42 || adjacent.values().any(|v| v.len() != 2) {
        return false;
    }
    let mut visited = [false; 42];
    let mut edge = 0;
    let mut vertex = edges[0].0;
    for _ in 0..42 {
        if visited[edge] {
            return false;
        }
        visited[edge] = true;
        let (a, b) = edges[edge];
        vertex = if vertex == a { b } else { a };
        let choices = &adjacent[&vertex];
        edge = if choices[0] == edge {
            choices[1]
        } else {
            choices[0]
        };
    }
    edge == 0 && vertex == edges[0].0
}

// Aeternity's Cuckoo variant uses the four hash words directly as its state,
// omits the standard SipHash IV XOR, and rotates the final result by 17 bits.
pub fn siphash(mut v: [u64; 4], nonce: u64) -> u64 {
    v[3] ^= nonce;
    for _ in 0..2 {
        round(&mut v);
    }
    v[0] ^= nonce;
    v[2] ^= 0xff;
    for _ in 0..4 {
        round(&mut v);
    }
    (v[0] ^ v[1] ^ v[2] ^ v[3]).rotate_left(17)
}
fn round(v: &mut [u64; 4]) {
    v[0] = v[0].wrapping_add(v[1]);
    v[2] = v[2].wrapping_add(v[3]);
    v[1] = v[1].rotate_left(13);
    v[3] = v[3].rotate_left(16);
    v[1] ^= v[0];
    v[3] ^= v[2];
    v[0] = v[0].rotate_left(32);
    v[2] = v[2].wrapping_add(v[1]);
    v[0] = v[0].wrapping_add(v[3]);
    v[1] = v[1].rotate_left(17);
    v[3] = v[3].rotate_left(21);
    v[1] ^= v[2];
    v[3] ^= v[0];
    v[2] = v[2].rotate_left(32);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cycles_cannot_branch_or_split_into_multiple_components() {
        let edges: Vec<_> = (0..21)
            .flat_map(|n| [(2 * n, 2 * n + 1), (2 * ((n + 1) % 21), 2 * n + 1)])
            .collect();
        assert!(is_single_cycle(&edges));
        let mut branches = edges.clone();
        branches[1] = branches[0];
        assert!(!is_single_cycle(&branches));
        let mut split: Vec<_> = (0..10)
            .flat_map(|n| [(2 * n, 2 * n + 1), (2 * ((n + 1) % 10), 2 * n + 1)])
            .collect();
        split.extend((0..11).flat_map(|n| {
            [
                (20 + 2 * n, 21 + 2 * n),
                (20 + 2 * ((n + 1) % 11), 21 + 2 * n),
            ]
        }));
        assert!(!is_single_cycle(&split));
    }
}
