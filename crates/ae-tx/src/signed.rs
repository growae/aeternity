// Behavioral reference: aeternity fa2a6d7, aetx_sign.erl.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use crate::{CanonicalTransaction, Error, Spend};
use ae_codec::object::{self, Field, FieldType, ObjectTag, Value};
use ae_crypto::{blake2b_256, verify_ed25519};
use ae_protocol::{NetworkProfile, Protocol};
use ae_types::{AccountPublicKey, Hash256, Signature};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedTransaction<T> {
    transaction: T,
    signatures: Vec<Signature>,
}
fn schema() -> Vec<Field> {
    vec![
        Field {
            name: "signatures".into(),
            kind: FieldType::List(Box::new(FieldType::Binary)),
        },
        Field {
            name: "transaction".into(),
            kind: FieldType::Binary,
        },
    ]
}
impl<T: CanonicalTransaction> SignedTransaction<T> {
    pub fn new(transaction: T, mut signatures: Vec<Signature>) -> Self {
        signatures.sort();
        signatures.dedup();
        Self {
            transaction,
            signatures,
        }
    }
    pub fn transaction(&self) -> &T {
        &self.transaction
    }
    pub fn signatures(&self) -> &[Signature] {
        &self.signatures
    }
    pub fn add_signatures(&mut self, signatures: impl IntoIterator<Item = Signature>) {
        self.signatures.extend(signatures);
        self.signatures.sort();
        self.signatures.dedup();
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let fields = object::deserialize(bytes, ObjectTag::SignedTx, &1.into(), &schema())?;
        let [(_, Value::List(values)), (_, Value::Binary(bytes))] = fields.as_slice() else {
            unreachable!("validated schema")
        };
        let signatures = values
            .iter()
            .map(|v| {
                let Value::Binary(bytes) = v else {
                    unreachable!("validated signature type")
                };
                Signature::try_from(bytes.as_slice())
            })
            .collect::<Result<_, _>>()?;
        // Decoding preserves order AND duplicates; constructors deduplicate.
        Ok(Self {
            transaction: T::decode(bytes)?,
            signatures,
        })
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut sorted = self.signatures.clone();
        sorted.sort(); // Do not deduplicate a decoded signature list.
        object::serialize(
            ObjectTag::SignedTx,
            &1.into(),
            &schema(),
            &[
                (
                    "signatures".into(),
                    Value::List(sorted.iter().map(|s| Value::Binary(s.0.to_vec())).collect()),
                ),
                (
                    "transaction".into(),
                    Value::Binary(self.transaction.encode()),
                ),
            ],
        )
        .expect("typed signed transaction")
    }
    pub fn hash(&self) -> Hash256 {
        blake2b_256(&self.encode())
    }

    // Callers must derive signers from transaction semantics/state, removing
    // identities authenticated by GA execution. Every remaining signature must
    // match exactly one remaining signer; no unused signatures are accepted.
    pub fn verify_signers(
        &self,
        signers: &[AccountPublicKey],
        network: &NetworkProfile,
        protocol: Protocol,
        sponsored_inner: bool,
    ) -> Result<(), Error> {
        if signers.len() != self.signatures.len() {
            return Err(Error::SignatureVerificationFailed);
        }
        if signers.is_empty() {
            return Ok(());
        }
        let (plain, hashed) = self.payloads(network, protocol, sponsored_inner);
        let mut remaining = self.signatures.clone();
        for signer in signers {
            let Some(index) = remaining.iter().position(|s| {
                verify_ed25519(signer, &plain, s)
                    || hashed
                        .as_ref()
                        .is_some_and(|p| verify_ed25519(signer, p, s))
            }) else {
                return Err(Error::SignatureVerificationFailed);
            };
            remaining.remove(index);
            // The Erlang search accumulates earlier failures in reverse order.
            remaining[..index].reverse();
        }
        Ok(())
    }
    pub fn verify_one(
        &self,
        signer: &AccountPublicKey,
        network: &NetworkProfile,
        protocol: Protocol,
    ) -> Result<(), Error> {
        let (plain, hashed) = self.payloads(network, protocol, false);
        if self.signatures.iter().any(|s| {
            verify_ed25519(signer, &plain, s)
                || hashed
                    .as_ref()
                    .is_some_and(|p| verify_ed25519(signer, p, s))
        }) {
            Ok(())
        } else {
            Err(Error::SignatureVerificationFailed)
        }
    }
    fn payloads(
        &self,
        network: &NetworkProfile,
        protocol: Protocol,
        sponsored_inner: bool,
    ) -> (Vec<u8>, Option<Vec<u8>>) {
        let tx_bytes = self.transaction.encode();
        let prefix = network.signing_prefix(sponsored_inner);
        let mut plain = prefix.clone();
        plain.extend_from_slice(&tx_bytes);
        let hashed = (protocol >= Protocol::Lima).then(|| {
            let mut payload = prefix;
            payload.extend_from_slice(blake2b_256(&tx_bytes).as_ref());
            payload
        });
        (plain, hashed)
    }
}
impl SignedTransaction<Spend> {
    pub fn verify(
        &self,
        network: &NetworkProfile,
        protocol: Protocol,
        sponsored_inner: bool,
    ) -> Result<(), Error> {
        self.verify_signers(
            &[self.transaction.sender()],
            network,
            protocol,
            sponsored_inner,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ae_crypto::Ed25519SecretKey;
    use ae_types::{Id, IdTag};

    fn fixture() -> (Ed25519SecretKey, Spend, NetworkProfile) {
        let key = Ed25519SecretKey::from_seed(&[7; 32]);
        let mut spend = Spend::new(key.public_key(), Id::new(IdTag::Account, [8; 32])).unwrap();
        spend.set_amount(100.into());
        spend.set_nonce(1.into());
        spend.set_fee(20_000.into());
        (key, spend, NetworkProfile::mainnet())
    }
    #[test]
    fn protocol_and_sponsorship_signature_domains() {
        let (key, tx, network) = fixture();
        for hashed in [false, true] {
            for inner in [false, true] {
                let bytes = tx.encode();
                let mut payload = network.signing_prefix(inner);
                if hashed {
                    payload.extend_from_slice(blake2b_256(&bytes).as_ref());
                } else {
                    payload.extend_from_slice(&bytes);
                }
                let signed = SignedTransaction::new(tx.clone(), vec![key.sign(&payload)]);
                assert!(signed.verify(&network, Protocol::Lima, inner).is_ok());
                assert_eq!(
                    signed.verify(&network, Protocol::Fortuna, inner).is_ok(),
                    !hashed
                );
                assert!(signed.verify(&network, Protocol::Ceres, !inner).is_err());
                assert!(
                    signed
                        .verify(&NetworkProfile::uat(), Protocol::Ceres, inner)
                        .is_err()
                );
                assert_eq!(
                    SignedTransaction::<Spend>::decode(&signed.encode()).unwrap(),
                    signed
                );
            }
        }
    }
    #[test]
    fn exact_signers_and_constructor_vs_decoder_duplicates() {
        let (key, tx, network) = fixture();
        let mut payload = network.signing_prefix(false);
        payload.extend_from_slice(&tx.encode());
        let signature = key.sign(&payload);
        let mut signed = SignedTransaction::new(tx, vec![signature, signature]);
        assert_eq!(signed.signatures.len(), 1);
        assert!(signed.verify(&network, Protocol::Roma, false).is_ok());
        signed.signatures.push(signature);
        let decoded = SignedTransaction::<Spend>::decode(&signed.encode()).unwrap();
        assert_eq!(decoded.signatures.len(), 2);
        assert!(decoded.verify(&network, Protocol::Roma, false).is_err());
        assert!(
            decoded
                .verify_one(&key.public_key(), &network, Protocol::Roma)
                .is_ok()
        );
        assert!(
            decoded
                .verify_signers(
                    &[key.public_key(), key.public_key()],
                    &network,
                    Protocol::Roma,
                    false
                )
                .is_ok()
        );
        assert!(
            decoded
                .verify_signers(&[], &network, Protocol::Roma, false)
                .is_err()
        );
        let unsigned = SignedTransaction::new(decoded.transaction, vec![]);
        assert!(
            unsigned
                .verify_signers(&[], &network, Protocol::Roma, false)
                .is_ok()
        );
    }
}
