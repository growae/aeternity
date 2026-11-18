// Behavioral reference: aeternity fa2a6d7, aec_headers.erl,
// aec_block_insertion.erl and aec_consensus_bitcoin_ng.erl.
// Copyright (c) 2020, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{
    cuckoo,
    target::{Target, TargetError, WorkSample, retarget},
};
use ae_codec::headers::{Header, HeaderError, KeyHeader};
use ae_crypto::verify_ed25519;
use ae_protocol::{NetworkProfile, ProfileError};
use ae_types::{AccountPublicKey, Hash256, Signature};
use std::{collections::BTreeMap, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Encoding(HeaderError),
    Protocol(ProfileError),
    Target(TargetError),
    GenesisRequiresAnchor,
    FutureTimestamp,
    IncorrectProofOfWork,
    PreviousHash,
    PreviousKeyHash,
    Height,
    InvalidHistory,
    MissingHistory { required: usize, actual: usize },
    KeyFromPast,
    MicroInterval,
    MicroSignature,
    WrongTarget { actual: Target, expected: Target },
    Checkpoint,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<HeaderError> for Error {
    fn from(e: HeaderError) -> Self {
        Self::Encoding(e)
    }
}
impl From<ProfileError> for Error {
    fn from(e: ProfileError) -> Self {
        Self::Protocol(e)
    }
}
impl From<TargetError> for Error {
    fn from(e: TargetError) -> Self {
        Self::Target(e)
    }
}

pub type Checkpoints = BTreeMap<u64, Hash256>;

// The chain owner supplies already validated parents and any requested recent
// key history, oldest first. Missing history is local unavailability, not a
// judgment that the peer's block is invalid. Genesis is installed separately by
// the network's trusted genesis hash and reconstructed state root.
pub struct Context<'a> {
    pub previous: &'a Header,
    pub previous_key: &'a KeyHeader,
    pub recent_keys: &'a [KeyHeader],
}
pub fn timestamp(header: &Header) -> u64 {
    match header {
        Header::Key(h) => h.time,
        Header::Micro(h) => h.time,
    }
}

// Only header validity is established here. The body, fraud proof, transaction
// execution and resulting state commitment remain block-validation obligations.
pub fn validate_header(
    header: &Header,
    network: &NetworkProfile,
    now_ms: u64,
    ctx: &Context<'_>,
    checkpoints: &Checkpoints,
) -> Result<(), Error> {
    validate_standalone(header, network, now_ms)?;
    validate_context(header, ctx, checkpoints)
}
pub fn validate_standalone(
    header: &Header,
    network: &NetworkProfile,
    now_ms: u64,
) -> Result<(), Error> {
    // Encoding also validates typed headers constructed locally.
    header.encode()?;
    if matches!(header,Header::Key(k) if k.hole || k.end_of_epoch) {
        return Err(HeaderError::HyperchainFlags.into());
    }
    if matches!(header,Header::Key(k) if k.common.height==0) {
        return Err(Error::GenesisRequiresAnchor);
    }
    network.validate_version(header.common().height, header.common().version)?;
    if let Header::Key(k) = header
        && !cuckoo::verify_key_seal(k)?
    {
        return Err(Error::IncorrectProofOfWork);
    }
    if u128::from(timestamp(header)) >= u128::from(now_ms) + 540_000 {
        return Err(Error::FutureTimestamp);
    }
    Ok(())
}
pub fn validate_context(
    header: &Header,
    ctx: &Context<'_>,
    checkpoints: &Checkpoints,
) -> Result<(), Error> {
    let common = header.common();
    if common.previous_hash != ctx.previous.hash()?.0 {
        return Err(Error::PreviousHash);
    }
    let key_hash = Header::Key(ctx.previous_key.clone()).hash()?;
    if common.previous_key_hash != key_hash.0 {
        return Err(Error::PreviousKeyHash);
    }
    match ctx.previous {
        Header::Key(_) if common.previous_hash != key_hash.0 => return Err(Error::PreviousKeyHash),
        Header::Micro(m)
            if m.common.previous_key_hash != key_hash.0
                || m.common.height != ctx.previous_key.common.height =>
        {
            return Err(Error::PreviousKeyHash);
        }
        _ => {}
    }
    let expected_height = match header {
        Header::Key(_) => ctx
            .previous
            .common()
            .height
            .checked_add(1)
            .ok_or(Error::Height)?,
        Header::Micro(_) => ctx.previous.common().height,
    };
    if common.height != expected_height {
        return Err(Error::Height);
    }
    match header {
        Header::Micro(m) => {
            let interval = if matches!(ctx.previous, Header::Micro(_)) {
                3000
            } else {
                1
            };
            if u128::from(m.time) < u128::from(timestamp(ctx.previous)) + interval {
                return Err(Error::MicroInterval);
            }
            if !verify_ed25519(
                &AccountPublicKey(ctx.previous_key.miner),
                &header.signature_bytes()?,
                &Signature(m.signature),
            ) {
                return Err(Error::MicroSignature);
            }
        }
        Header::Key(k) => {
            if let Some(hash) = checkpoints.get(&common.height)
                && header.hash()? != *hash
            {
                return Err(Error::Checkpoint);
            }
            let needed = if common.height > 18 {
                18
            } else if common.height > 11 {
                11
            } else {
                0
            };
            let history = history(ctx, needed)?;
            let median = if needed == 0 {
                0
            } else {
                let mut times: Vec<_> = history[history.len() - 11..]
                    .iter()
                    .map(|k| k.time)
                    .collect();
                times.sort_unstable();
                times[5]
            };
            if k.time <= median {
                return Err(Error::KeyFromPast);
            }
            let expected = if common.height <= 18 {
                Target(ctx.previous_key.target)
            } else {
                let samples: Vec<_> = history
                    .iter()
                    .map(|k| WorkSample {
                        height: k.common.height,
                        timestamp: k.time,
                        target: Target(k.target),
                    })
                    .collect();
                retarget(&samples)?
            };
            if Target(k.target) != expected {
                return Err(Error::WrongTarget {
                    actual: Target(k.target),
                    expected,
                });
            }
        }
    }
    Ok(())
}
fn history<'a>(ctx: &Context<'a>, needed: usize) -> Result<&'a [KeyHeader], Error> {
    if ctx.recent_keys.len() < needed {
        return Err(Error::MissingHistory {
            required: needed,
            actual: ctx.recent_keys.len(),
        });
    }
    let history = &ctx.recent_keys[ctx.recent_keys.len() - needed..];
    if needed == 0 {
        return Ok(history);
    }
    if history.last() != Some(ctx.previous_key) {
        return Err(Error::InvalidHistory);
    }
    for pair in history.windows(2) {
        if pair[0].common.height.checked_add(1) != Some(pair[1].common.height)
            || pair[1].common.previous_key_hash != Header::Key(pair[0].clone()).hash()?.0
        {
            return Err(Error::InvalidHistory);
        }
    }
    Ok(history)
}
