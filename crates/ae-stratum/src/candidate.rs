// aec_consensus_bitcoin_ng:key_header_for_sealing and aestratum candidate work.
// A cached candidate is data, not authorization to admit a block. Active work
// is tracked separately, and every sealed header still needs chain validation.
use crate::{ingress, job::Work};
use ae_codec::headers::{Header, HeaderError, HeaderMode, KeyHeader};
use ae_consensus::target::Target;
use ae_crypto::blake2b_256;
use ae_types::Hash256;
use num_bigint::{BigInt, Sign};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Header(HeaderError),
    Binding,
    Sealed,
    ProofWidth,
    Capacity,
    Work(crate::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    header: KeyHeader,
    bytes: Vec<u8>,
    hash: Hash256,
}
impl Candidate {
    pub fn new(header: KeyHeader) -> Result<Self, Error> {
        if header.nonce != 0 || header.seal != [0; 42] {
            return Err(Error::Sealed);
        }
        let encoded = Header::Key(header.clone())
            .encode()
            .map_err(Error::Header)?;
        // Encoding alone permits HC flags. Stratum work here is canonical PoW.
        if Header::decode(&encoded, HeaderMode::ProofOfWork).map_err(Error::Header)?
            != Header::Key(header.clone())
        {
            return Err(Error::Binding);
        }
        Ok(Self {
            hash: blake2b_256(&encoded),
            header,
            bytes: encoded,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let Header::Key(header) =
            Header::decode(bytes, HeaderMode::ProofOfWork).map_err(Error::Header)?
        else {
            return Err(Error::Binding);
        };
        let candidate = Self::new(header)?;
        if candidate.bytes != bytes {
            return Err(Error::Binding);
        }
        Ok(candidate)
    }
    pub fn header(&self) -> &KeyHeader {
        &self.header
    }
    pub fn sealing_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn hash(&self) -> Hash256 {
        self.hash
    }
    pub fn parent(&self) -> Hash256 {
        Hash256(self.header.common.previous_hash)
    }
    pub fn work(&self) -> Result<Work, Error> {
        Work::new(
            hex::encode(self.hash.0),
            self.header.common.version,
            BigInt::from_bytes_be(Sign::Plus, Target(self.header.target).value().as_bytes()),
        )
        .map_err(Error::Work)
    }
    pub fn solution(&self, input: &ingress::Input) -> Result<Sealed, Error> {
        input.validate().map_err(Error::Work)?;
        if input.hash != self.hash {
            return Err(Error::Binding);
        }
        self.seal(input.solution.as_ref().ok_or(Error::Binding)?)
    }
    pub fn seal(&self, solution: &ingress::Solution) -> Result<Sealed, Error> {
        let mut header = self.header.clone();
        header.nonce = solution.nonce.0;
        for (dst, src) in header.seal.iter_mut().zip(solution.proof.iter()) {
            *dst = u32::try_from(*src).map_err(|_| Error::ProofWidth)?;
        }
        let header_value = Header::Key(header.clone());
        Ok(Sealed {
            bytes: header_value.encode().map_err(Error::Header)?,
            hash: header_value.hash().map_err(Error::Header)?,
            header,
            work_hash: self.hash,
        })
    }
}

// Sealing reconstructs bytes, including invalid proofs. The native pool marks
// accounting before conductor admission; a pool solution is not a validity
// certificate. In particular no configurable small graph replaces PoW29.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sealed {
    header: KeyHeader,
    bytes: Vec<u8>,
    hash: Hash256,
    work_hash: Hash256,
}
impl Sealed {
    pub fn header(&self) -> &KeyHeader {
        &self.header
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn hash(&self) -> Hash256 {
        self.hash
    }
    pub fn work_hash(&self) -> Hash256 {
        self.work_hash
    }
    pub fn parent(&self) -> Hash256 {
        Hash256(self.header.common.previous_hash)
    }
}

// Conductor prepends new candidates without invalidating older work. An exact
// matching reply consumes the entire active set before insertion, even if
// insertion fails. A missing reply leaves the set alone. Capacity is a Rust
// operational bound: pressure rejects publication rather than evicting work.
pub struct Active {
    capacity: usize,
    candidates: Vec<Arc<Candidate>>,
}
impl Active {
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if !(1..=4096).contains(&capacity) {
            return Err(Error::Capacity);
        }
        Ok(Self {
            capacity,
            candidates: Vec::new(),
        })
    }
    pub fn candidates(&self) -> &[Arc<Candidate>] {
        &self.candidates
    }
    // False is a completed preparation for an obsolete top, not publication.
    pub fn publish(&mut self, candidate: Arc<Candidate>, top: Hash256) -> Result<bool, Error> {
        if candidate.parent() != top {
            return Ok(false);
        }
        if self.candidates.len() == self.capacity {
            return Err(Error::Capacity);
        }
        self.candidates.insert(0, candidate);
        Ok(true)
    }
    pub fn claim(&mut self, sealing_bytes: &[u8]) -> Option<Arc<Candidate>> {
        let candidate = self
            .candidates
            .iter()
            .find(|c| c.sealing_bytes() == sealing_bytes)?
            .clone();
        self.clear();
        Some(candidate)
    }
    pub fn clear(&mut self) {
        self.candidates.clear();
    }
}
