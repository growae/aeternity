// aec_preset_keys and HC key-header seals, aeternity fa2a6d7.
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use ae_codec::headers::{Header, HeaderError, KeyHeader, MicroHeader};
use ae_crypto::{Ed25519SecretKey, verify_ed25519};
use ae_types::{AccountPublicKey, Signature};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    MissingKey,
    NoActive,
    Encoding(HeaderError),
    Padding([u32; 26]),
    Signature,
}
// Secret keys never implement Debug or serialization. Clones share immutable
// key material while candidate and active-key selection remain staged locally.
#[derive(Clone, Default)]
pub struct Keys {
    keys: Arc<BTreeMap<AccountPublicKey, Ed25519SecretKey>>,
    active: Option<AccountPublicKey>,
    candidate: Option<AccountPublicKey>,
}
impl Keys {
    pub(crate) fn from_shared(keys: Arc<BTreeMap<AccountPublicKey, Ed25519SecretKey>>) -> Self {
        Self {
            keys,
            active: None,
            candidate: None,
        }
    }
    pub fn new(keys: impl IntoIterator<Item = Ed25519SecretKey>) -> Self {
        Self {
            keys: Arc::new(keys.into_iter().map(|k| (k.public_key(), k)).collect()),
            active: None,
            candidate: None,
        }
    }
    pub fn active(&self) -> Option<AccountPublicKey> {
        self.active
    }
    pub fn candidate(&self) -> Option<AccountPublicKey> {
        self.candidate
    }
    pub fn contains(&self, key: AccountPublicKey) -> bool {
        self.keys.contains_key(&key)
    }
    pub fn set_candidate(&mut self, key: AccountPublicKey) -> Result<(), Error> {
        if !self.contains(key) {
            return Err(Error::MissingKey);
        }
        self.candidate = Some(key);
        Ok(())
    }
    pub fn promote(&mut self, key: AccountPublicKey) -> Result<(), Error> {
        self.candidate = None;
        self.active = Some(key).filter(|k| self.contains(*k));
        self.active.map(|_| ()).ok_or(Error::MissingKey)
    }
    pub fn sign(&self, key: AccountPublicKey, message: &[u8]) -> Result<Signature, Error> {
        self.keys
            .get(&key)
            .map(|k| k.sign(message))
            .ok_or(Error::MissingKey)
    }
    pub fn sign_micro(
        &self,
        header: &MicroHeader,
        by: Option<AccountPublicKey>,
    ) -> Result<MicroHeader, Error> {
        let by = by.or(self.active).ok_or(Error::NoActive)?;
        let bytes = Header::Micro(header.clone())
            .signature_bytes()
            .map_err(Error::Encoding)?;
        let mut signed = header.clone();
        signed.signature = self.sign(by, &bytes)?.0;
        Ok(signed)
    }
    // Missing candidate keys are a retry with the reference's 1000 ms delay.
    // The caller performs sleeping; the candidate state is unchanged on miss.
    pub fn seal(&mut self, header: &KeyHeader) -> Result<Seal, Error> {
        let by = AccountPublicKey(header.beneficiary);
        if self.set_candidate(by).is_err() {
            return Ok(Seal::NoSolution { retry_ms: 1000 });
        }
        let bytes = Header::Key(header.clone())
            .signature_bytes()
            .map_err(Error::Encoding)?;
        let signature = self.sign(by, &bytes)?;
        let mut seal = [0; 42];
        for (word, bytes) in seal[..16].iter_mut().zip(signature.0.chunks_exact(4)) {
            *word = u32::from_be_bytes(bytes.try_into().unwrap());
        }
        Ok(Seal::Found(seal))
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seal {
    Found([u32; 42]),
    NoSolution { retry_ms: u64 },
}
pub fn verify_key(header: &KeyHeader) -> Result<(), Error> {
    let padding: [u32; 26] = header.seal[16..].try_into().unwrap();
    if padding[0] > 1 || padding[1..].iter().any(|w| *w != 0) {
        return Err(Error::Padding(padding));
    }
    let mut signature = [0; 64];
    for (bytes, word) in signature.chunks_exact_mut(4).zip(header.seal[..16].iter()) {
        bytes.copy_from_slice(&word.to_be_bytes());
    }
    let bytes = Header::Key(header.clone())
        .signature_bytes()
        .map_err(Error::Encoding)?;
    if verify_ed25519(
        &AccountPublicKey(header.miner),
        &bytes,
        &Signature(signature),
    ) || header.target == 0
    {
        Ok(())
    } else {
        Err(Error::Signature)
    }
}
pub fn verify_micro(header: &MicroHeader, leader: AccountPublicKey) -> Result<(), Error> {
    let bytes = Header::Micro(header.clone())
        .signature_bytes()
        .map_err(Error::Encoding)?;
    if verify_ed25519(&leader, &bytes, &Signature(header.signature)) {
        Ok(())
    } else {
        Err(Error::Signature)
    }
}

#[derive(Debug)]
pub enum MicroError<E, A = ae_trie::MemoryStore, C = ae_trie::MemoryStore> {
    TimeTooLow,
    Signing(Error),
    Leader(crate::block::Error<E, A, C>),
}
pub fn validate_micro<
    A: ae_trie::NodeWriter + Clone,
    C: ae_trie::NodeWriter + Clone,
    O: ae_vm_fate::maps::MapOrder,
    H: crate::block::Host,
>(
    state: &mut crate::block::State,
    reader: &crate::chain::Reader<A, C, O>,
    header: &MicroHeader,
    previous_time: u64,
    host: &mut H,
    limits: crate::leader::Limits,
) -> Result<(), MicroError<H::Error, A, C>> {
    if header.time <= previous_time {
        return Err(MicroError::TimeTooLow);
    }
    let env = crate::chain::RunEnv::Hash(header.common.previous_hash.to_vec());
    let leader =
        match state.leader_for_timeslot(reader, &env, host, &header.common.height.into(), limits) {
            Ok(p) => p,
            Err(crate::block::Error::Leader(
                crate::leader::Error::NotInCache | crate::leader::Error::EpochDidNotEnd,
            )) => return Err(MicroError::Signing(Error::Signature)),
            Err(e) => return Err(MicroError::Leader(e)),
        };
    let ae_vm_fate::value::ObjectPayload::Bytes(bytes) = &leader else {
        return Err(MicroError::Leader(crate::block::Error::Native(
            crate::block::Native::InvalidLeader(leader),
        )));
    };
    let Ok(key) = bytes.as_slice().try_into() else {
        return Err(MicroError::Leader(crate::block::Error::Native(
            crate::block::Native::InvalidLeader(leader),
        )));
    };
    verify_micro(header, AccountPublicKey(key)).map_err(MicroError::Signing)
}
