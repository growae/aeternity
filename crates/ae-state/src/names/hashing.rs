// Aeternity fa2a6d7, aens_hash. Inputs here are already ASCII-normalized.
// Copyright (c) 2018, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::Error;
use ae_crypto::blake2b_256;
use ae_types::{Hash256, Unsigned};

fn chain_domain(name: &[u8]) -> bool {
    // binary:split(..., [global, trim]) removes trailing empty components.
    name.split(|b| *b == b'.').rfind(|part| !part.is_empty()) == Some(b"chain".as_slice())
}
fn legacy_hash(name: &[u8]) -> Hash256 {
    let mut root = Hash256::ZERO;
    // The reference reverses labels then recursively hashes the tail first.
    for label in name.split(|b| *b == b'.') {
        root = blake2b_256(&[root.as_ref(), blake2b_256(label).as_ref()].concat());
    }
    root
}
pub fn name_hash(ascii: &[u8]) -> Hash256 {
    if chain_domain(ascii) {
        blake2b_256(ascii)
    } else {
        legacy_hash(ascii)
    }
}
pub fn commitment_hash(ascii: &[u8], salt: &Unsigned) -> Result<Hash256, Error> {
    if chain_domain(ascii) && salt == &0.into() {
        return Err(Error::IllegalSalt);
    }
    // Erlang's 256-bit bit syntax retains the low 256 bits of large integers.
    let raw = salt.as_bytes();
    let size = raw.len().min(32);
    let mut salt_bytes = [0u8; 32];
    salt_bytes[32 - size..].copy_from_slice(&raw[raw.len() - size..]);
    let prefix = if chain_domain(ascii) {
        ascii.to_vec()
    } else {
        legacy_hash(ascii).0.to_vec()
    };
    Ok(blake2b_256(&[prefix.as_slice(), &salt_bytes].concat()))
}
pub fn auction_hash(name: [u8; 32]) -> [u8; 33] {
    let mut bytes = [0; 33];
    bytes[..32].copy_from_slice(&name);
    bytes
}
