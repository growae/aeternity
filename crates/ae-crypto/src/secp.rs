use secp256k1::{Message, PublicKey, Secp256k1, VerifyOnly, ecdsa};
use std::sync::LazyLock;

static CONTEXT: LazyLock<Secp256k1<VerifyOnly>> = LazyLock::new(Secp256k1::verification_only);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSecp256k1PublicKey;

// aeu_crypto passes this precomputed digest to OpenSSL. Accept high-S as it
// does, but preserve invalid public keys as errors rather than false results.
pub fn verify_secp256k1(
    public_key: &[u8; 64],
    digest: &[u8; 32],
    signature: &[u8; 64],
) -> Result<bool, InvalidSecp256k1PublicKey> {
    let mut encoded = [4; 65];
    encoded[1..].copy_from_slice(public_key);
    let key = PublicKey::from_slice(&encoded).map_err(|_| InvalidSecp256k1PublicKey)?;
    let Ok(mut signature) = ecdsa::Signature::from_compact(signature) else {
        return Ok(false);
    };
    signature.normalize_s();
    Ok(CONTEXT
        .verify_ecdsa(&Message::from_digest(*digest), &signature, &key)
        .is_ok())
}

// Historical recovery accepts only 27/28 in the first byte, followed by R/S.
pub fn recover_secp256k1_address(digest: &[u8; 32], signature: &[u8; 65]) -> Option<[u8; 20]> {
    if !matches!(signature[0], 27 | 28) {
        return None;
    }
    let id = ecdsa::RecoveryId::from_i32(i32::from(signature[0] - 27)).ok()?;
    let signature = ecdsa::RecoverableSignature::from_compact(&signature[1..], id).ok()?;
    let key = CONTEXT
        .recover_ecdsa(&Message::from_digest(*digest), &signature)
        .ok()?;
    let hash = crate::keccak256(&key.serialize_uncompressed()[1..]);
    let address: [u8; 20] = hash.0[12..].try_into().unwrap();
    (address != [0; 20]).then_some(address)
}
