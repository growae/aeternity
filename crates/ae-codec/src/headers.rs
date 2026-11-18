// Layout reference: aeternity fa2a6d7, aec_headers.erl and blocks.hrl.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use ae_crypto::blake2b_256;
use ae_types::Hash256;
use std::fmt;

const KEY: u32 = 1 << 31;
const OPTIONAL: u32 = 1 << 30;
const HOLE: u32 = 1 << 29;
const END_OF_EPOCH: u32 = 1 << 28;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderMode {
    ProofOfWork,
    Hyperchain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommonHeader {
    pub version: u32,
    pub height: u64,
    pub previous_hash: [u8; 32],
    pub previous_key_hash: [u8; 32],
    pub state_hash: [u8; 32],
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyHeader {
    pub common: CommonHeader,
    pub miner: [u8; 32],
    pub beneficiary: [u8; 32],
    pub target: u32,
    pub seal: [u32; 42],
    pub nonce: u64,
    pub time: u64,
    pub info: Option<[u8; 4]>,
    pub hole: bool,
    pub end_of_epoch: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicroHeader {
    pub common: CommonHeader,
    pub transactions_hash: [u8; 32],
    pub time: u64,
    pub proof_of_fraud: Option<[u8; 32]>,
    pub signature: [u8; 64],
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Header {
    Key(KeyHeader),
    Micro(MicroHeader),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderError {
    Malformed,
    ReservedFlags,
    HyperchainFlags,
    VersionInfo,
    TrailingBytes,
}
impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for HeaderError {}

struct Reader<'a> {
    rest: &'a [u8],
}
impl Reader<'_> {
    fn bytes<const N: usize>(&mut self) -> Result<[u8; N], HeaderError> {
        let (bytes, tail) = self
            .rest
            .split_at_checked(N)
            .ok_or(HeaderError::Malformed)?;
        self.rest = tail;
        bytes.try_into().map_err(|_| HeaderError::Malformed)
    }
    fn u32(&mut self) -> Result<u32, HeaderError> {
        Ok(u32::from_be_bytes(self.bytes()?))
    }
    fn u64(&mut self) -> Result<u64, HeaderError> {
        Ok(u64::from_be_bytes(self.bytes()?))
    }
}

impl Header {
    pub fn decode(bytes: &[u8], mode: HeaderMode) -> Result<Self, HeaderError> {
        let (header, tail) = Self::decode_partial(bytes, mode)?;
        if !tail.is_empty() {
            return Err(HeaderError::TrailingBytes);
        }
        Ok(header)
    }

    pub fn decode_partial(bytes: &[u8], mode: HeaderMode) -> Result<(Self, &[u8]), HeaderError> {
        let mut r = Reader { rest: bytes };
        let version = r.u32()?;
        let flags = r.u32()?;
        let is_key = flags & KEY != 0;
        let optional = flags & OPTIONAL != 0;
        if is_key {
            if flags & 0x0fff_ffff != 0 {
                return Err(HeaderError::ReservedFlags);
            }
            if mode == HeaderMode::ProofOfWork && flags & (HOLE | END_OF_EPOCH) != 0 {
                return Err(HeaderError::HyperchainFlags);
            }
            if version == 0 || (version == 1 && optional) {
                return Err(HeaderError::VersionInfo);
            }
            if bytes.len() != 364 + usize::from(optional) * 4 {
                return Err(HeaderError::Malformed);
            }
        } else if flags & 0x3fff_ffff != 0 {
            return Err(HeaderError::ReservedFlags);
        }
        let common = CommonHeader {
            version,
            height: r.u64()?,
            previous_hash: r.bytes()?,
            previous_key_hash: r.bytes()?,
            state_hash: r.bytes()?,
        };
        let header = if is_key {
            let miner = r.bytes()?;
            let beneficiary = r.bytes()?;
            let target = r.u32()?;
            let mut seal = [0; 42];
            for word in &mut seal {
                *word = r.u32()?;
            }
            let nonce = r.u64()?;
            let time = r.u64()?;
            let info = if optional { Some(r.bytes()?) } else { None };
            Self::Key(KeyHeader {
                common,
                miner,
                beneficiary,
                target,
                seal,
                nonce,
                time,
                info,
                hole: flags & HOLE != 0,
                end_of_epoch: flags & END_OF_EPOCH != 0,
            })
        } else {
            let transactions_hash = r.bytes()?;
            let time = r.u64()?;
            let proof_of_fraud = if optional { Some(r.bytes()?) } else { None };
            Self::Micro(MicroHeader {
                common,
                transactions_hash,
                time,
                proof_of_fraud,
                signature: r.bytes()?,
            })
        };
        Ok((header, r.rest))
    }

    pub fn common(&self) -> &CommonHeader {
        match self {
            Self::Key(k) => &k.common,
            Self::Micro(m) => &m.common,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, HeaderError> {
        let flags = match self {
            Self::Key(k) => {
                if k.common.version == 0 || (k.common.version == 1 && k.info.is_some()) {
                    return Err(HeaderError::VersionInfo);
                }
                KEY | if k.info.is_some() { OPTIONAL } else { 0 }
                    | if k.hole { HOLE } else { 0 }
                    | if k.end_of_epoch { END_OF_EPOCH } else { 0 }
            }
            Self::Micro(m) => {
                if m.proof_of_fraud.is_some() {
                    OPTIONAL
                } else {
                    0
                }
            }
        };
        let mut bytes = Vec::with_capacity(368);
        let common = self.common();
        bytes.extend_from_slice(&common.version.to_be_bytes());
        bytes.extend_from_slice(&flags.to_be_bytes());
        bytes.extend_from_slice(&common.height.to_be_bytes());
        bytes.extend_from_slice(&common.previous_hash);
        bytes.extend_from_slice(&common.previous_key_hash);
        bytes.extend_from_slice(&common.state_hash);
        match self {
            Self::Key(k) => {
                bytes.extend_from_slice(&k.miner);
                bytes.extend_from_slice(&k.beneficiary);
                bytes.extend_from_slice(&k.target.to_be_bytes());
                for word in k.seal {
                    bytes.extend_from_slice(&word.to_be_bytes());
                }
                bytes.extend_from_slice(&k.nonce.to_be_bytes());
                bytes.extend_from_slice(&k.time.to_be_bytes());
                if let Some(info) = k.info {
                    bytes.extend_from_slice(&info);
                }
            }
            Self::Micro(m) => {
                bytes.extend_from_slice(&m.transactions_hash);
                bytes.extend_from_slice(&m.time.to_be_bytes());
                if let Some(pof) = m.proof_of_fraud {
                    bytes.extend_from_slice(&pof);
                }
                bytes.extend_from_slice(&m.signature);
            }
        }
        Ok(bytes)
    }

    pub fn signature_bytes(&self) -> Result<Vec<u8>, HeaderError> {
        let mut blank = self.clone();
        match &mut blank {
            Self::Key(k) => k.seal = [0; 42],
            Self::Micro(m) => m.signature = [0; 64],
        }
        blank.encode()
    }

    pub fn hash(&self) -> Result<Hash256, HeaderError> {
        Ok(blake2b_256(&self.encode()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn micro(pof: bool) -> Header {
        Header::Micro(MicroHeader {
            common: CommonHeader {
                version: 6,
                height: 42,
                previous_hash: [1; 32],
                previous_key_hash: [2; 32],
                state_hash: [3; 32],
            },
            transactions_hash: [4; 32],
            time: 123,
            proof_of_fraud: if pof { Some([5; 32]) } else { None },
            signature: [6; 64],
        })
    }
    #[test]
    fn micro_layout_signing_and_partial_decode() {
        for pof in [false, true] {
            let header = micro(pof);
            let mut raw = header.encode().unwrap();
            assert_eq!(raw.len(), if pof { 248 } else { 216 });
            assert_eq!(
                Header::decode(&raw, HeaderMode::ProofOfWork),
                Ok(header.clone())
            );
            let signing = header.signature_bytes().unwrap();
            assert_eq!(&signing[..signing.len() - 64], &raw[..raw.len() - 64]);
            assert_eq!(&signing[signing.len() - 64..], &[0; 64]);
            raw.push(0xc0);
            assert_eq!(
                Header::decode_partial(&raw, HeaderMode::ProofOfWork),
                Ok((header, &[0xc0][..]))
            );
            assert_eq!(
                Header::decode(&raw, HeaderMode::ProofOfWork),
                Err(HeaderError::TrailingBytes)
            );
        }
    }
    #[test]
    fn reserved_flags_and_truncation_are_rejected() {
        let raw = micro(false).encode().unwrap();
        for len in 0..raw.len() {
            assert!(Header::decode(&raw[..len], HeaderMode::ProofOfWork).is_err());
        }
        for bit in 0..30 {
            let mut bad = raw.clone();
            bad[4..8].copy_from_slice(&(1u32 << bit).to_be_bytes());
            assert_eq!(
                Header::decode(&bad, HeaderMode::ProofOfWork),
                Err(HeaderError::ReservedFlags)
            );
        }
    }
}
