#![forbid(unsafe_code)]

use ae_types::Hash256;
use blake2::{Blake2b, Digest, digest::consts::U32};
use sha2::Sha256;

pub mod bls;
mod bls_field;
mod poseidon;
mod secp;
mod signature;
pub use bls_field::BlsField;
pub use poseidon::poseidon_bls12_381;
pub use secp::{InvalidSecp256k1PublicKey, recover_secp256k1_address, verify_secp256k1};
pub use signature::{Ed25519SecretKey, verify_ed25519};

pub fn keccak256(bytes: &[u8]) -> Hash256 {
    use tiny_keccak::{Hasher, Keccak};
    let mut hash = [0; 32];
    let mut state = Keccak::v256();
    state.update(bytes);
    state.finalize(&mut hash);
    Hash256(hash)
}

pub fn blake2b_256(bytes: &[u8]) -> Hash256 {
    // Output size is part of BLAKE2's parameters, not truncation of BLAKE2b-512.
    Hash256(Blake2b::<U32>::digest(bytes).into())
}

pub fn sha256(bytes: &[u8]) -> Hash256 {
    Hash256(Sha256::digest(bytes).into())
}

pub fn api_checksum(bytes: &[u8]) -> [u8; 4] {
    let hash = sha256(sha256(bytes).as_ref());
    [hash.0[0], hash.0[1], hash.0[2], hash.0[3]]
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_hash_vectors() {
        assert_eq!(
            hex::encode(blake2b_256(b"").0),
            "0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8"
        );
        assert_eq!(
            hex::encode(blake2b_256(b"abc").0),
            "bddd813c634239723171ef3fee98579b94964e3bb1cb3e427262c8c068d52319"
        );
        assert_eq!(
            hex::encode(sha256(b"abc").0),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(api_checksum(b""), [0x5d, 0xf6, 0xe0, 0xe2]);
    }
}
