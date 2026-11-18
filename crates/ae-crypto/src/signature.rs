use ae_types::{AccountPublicKey, Signature};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};

// Match libsodium's non-ED25519_COMPAT detached verification: canonical S and
// public-key y coordinate, no small-order A or R, and the uncofactored equation.
// Dalek's strict verifier checks S/order/equation; its key parser also accepts
// noncanonical field encodings, so check the encoded y before parsing it.
pub fn verify_ed25519(
    public_key: &AccountPublicKey,
    message: &[u8],
    signature: &Signature,
) -> bool {
    let mut y = public_key.0;
    y[31] &= 0x7f;
    let mut prime = [0xff; 32];
    prime[0] = 0xed;
    prime[31] = 0x7f;
    if y.iter().rev().cmp(prime.iter().rev()) != std::cmp::Ordering::Less {
        return false;
    }
    let Ok(key) = VerifyingKey::from_bytes(&public_key.0) else {
        return false;
    };
    key.verify_strict(message, &ed25519_dalek::Signature::from_bytes(&signature.0))
        .is_ok()
}

// Deliberately has no Debug/Display/serialization implementation. SigningKey
// zeroizes its secret scalar and seed on drop through Dalek's default feature.
pub struct Ed25519SecretKey(SigningKey);

impl Ed25519SecretKey {
    pub fn from_seed(seed: &[u8; 32]) -> Self {
        Self(SigningKey::from_bytes(seed))
    }
    pub fn public_key(&self) -> AccountPublicKey {
        AccountPublicKey(self.0.verifying_key().to_bytes())
    }
    pub fn sign(&self, message: &[u8]) -> Signature {
        Signature(self.0.sign(message).to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc8032_empty_message_and_modified_signature() {
        let seed = hex::decode("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
            .unwrap();
        let key = Ed25519SecretKey::from_seed(seed.as_slice().try_into().unwrap());
        assert_eq!(
            hex::encode(key.public_key().0),
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
        );
        let signature = key.sign(b"");
        assert_eq!(
            hex::encode(signature.0),
            "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
        );
        assert!(verify_ed25519(&key.public_key(), b"", &signature));
        assert!(!verify_ed25519(&key.public_key(), b"x", &signature));
        let mut changed = signature;
        changed.0[63] |= 0x80;
        assert!(!verify_ed25519(&key.public_key(), b"", &changed));
    }
}
