// aec_dry_run:tx_env_and_trees and aetx_env hash selection, fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use ae_codec::headers::{Header, HeaderError, KeyHeader};
use ae_types::Hash256;
use num_bigint::BigInt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    Top,
    Height(BigInt),
    Hash(Hash256),
    InMicro(Hash256),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Returned {
    NoTop,
    ChainTooShort,
    NotMicro,
    BlockNotFound,
    StateGarbageCollected,
}
impl Returned {
    pub fn reason(self) -> &'static str {
        match self {
            Self::NoTop => "No top block hash",
            Self::ChainTooShort => "Chain too short",
            Self::NotMicro => "dry_run 'in' only applicable to Micro Block (hash)",
            Self::BlockNotFound => "Block not found",
            Self::StateGarbageCollected => "state garbage collected",
        }
    }
}
#[derive(Debug)]
pub enum Error<E> {
    // The original setup destructures these returned tuples as environment and
    // trees, then raises badrecord. They are not friendly final HTTP errors.
    Returned(Returned),
    InvalidHash,
    StateGarbageCollected,
    InvalidHeight,
    MissingLeader(Hash256),
    ExpectedKey,
    Header(HeaderError),
    Source(E),
}
#[derive(Debug)]
pub enum StateError<E> {
    GarbageCollected,
    Source(E),
}
// All methods must read the same immutable captured revision. Selection never
// writes or captures a second top while resolving a historical environment.
pub trait Source {
    type State;
    type Error;
    fn top(&mut self) -> Result<Option<Hash256>, Self::Error>;
    fn key_header(&mut self, height: &BigInt) -> Result<Option<KeyHeader>, Self::Error>;
    fn header(&mut self, hash: Hash256) -> Result<Option<Header>, Self::Error>;
    fn state(&mut self, hash: Hash256) -> Result<Self::State, StateError<Self::Error>>;
}
#[derive(Debug)]
pub struct Selected<T> {
    pub state: T,
    pub state_hash: Hash256,
    pub key: KeyHeader,
    pub key_hash: Hash256,
    pub timestamp: BigInt,
}
fn state<S: Source>(
    source: &mut S,
    hash: Hash256,
    returned: bool,
) -> Result<S::State, Error<S::Error>> {
    source.state(hash).map_err(|e| match e {
        StateError::GarbageCollected if returned => {
            Error::Returned(Returned::StateGarbageCollected)
        }
        StateError::GarbageCollected => Error::StateGarbageCollected,
        StateError::Source(e) => Error::Source(e),
    })
}
fn key<E>(header: Header) -> Result<KeyHeader, Error<E>> {
    match header {
        Header::Key(k) => Ok(k),
        Header::Micro(_) => Err(Error::ExpectedKey),
    }
}
pub fn select<S: Source>(
    source: &mut S,
    selector: &Selector,
) -> Result<Selected<S::State>, Error<S::Error>> {
    let hash = match selector {
        Selector::Top => source
            .top()
            .map_err(Error::Source)?
            .ok_or(Error::Returned(Returned::NoTop))?,
        Selector::Hash(hash) => *hash,
        Selector::Height(height) => {
            if height < &BigInt::from(0) {
                return Err(Error::InvalidHeight);
            }
            let key = source
                .key_header(height)
                .map_err(Error::Source)?
                .ok_or(Error::Returned(Returned::ChainTooShort))?;
            let hash = Header::Key(key.clone()).hash().map_err(Error::Header)?;
            let state = state(source, hash, true)?;
            let timestamp = key.time.into();
            return Ok(Selected {
                state,
                state_hash: hash,
                key,
                key_hash: hash,
                timestamp,
            });
        }
        Selector::InMicro(hash) => {
            let header = source
                .header(*hash)
                .map_err(Error::Source)?
                .ok_or(Error::Returned(Returned::BlockNotFound))?;
            let Header::Micro(micro) = header else {
                return Err(Error::Returned(Returned::NotMicro));
            };
            let leader = Hash256(micro.common.previous_key_hash);
            let header = source
                .header(leader)
                .map_err(Error::Source)?
                .ok_or(Error::MissingLeader(leader))?;
            let previous = Hash256(micro.common.previous_hash);
            let state = state(source, previous, true)?;
            let key_hash = header.hash().map_err(Error::Header)?;
            let key = key(header)?;
            return Ok(Selected {
                state,
                state_hash: previous,
                key,
                key_hash,
                timestamp: micro.time.into(),
            });
        }
    };
    let header = source
        .header(hash)
        .map_err(Error::Source)?
        .ok_or(Error::InvalidHash)?;
    // Hash selection reads state before the micro leader. In-micro selection
    // above reads the leader first. Preserve failures when both are missing.
    let state = state(source, hash, false)?;
    let (key, key_hash, timestamp) = match header {
        Header::Key(key) => {
            let timestamp = BigInt::from(key.time) + 1;
            (key, hash, timestamp)
        }
        Header::Micro(micro) => {
            let hash = Hash256(micro.common.previous_key_hash);
            let header = source
                .header(hash)
                .map_err(Error::Source)?
                .ok_or(Error::MissingLeader(hash))?;
            (key(header)?, hash, micro.time.into())
        }
    };
    Ok(Selected {
        state,
        state_hash: hash,
        key,
        key_hash,
        timestamp,
    })
}
