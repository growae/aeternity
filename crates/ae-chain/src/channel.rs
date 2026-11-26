// aec_chain:get_channel and aesc_channels decoding, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use crate::repository::{self as repo, Read, Record, View, point};
use ae_codec::rlp::{self, Rlp};
pub use ae_state::channels::Channel;
use ae_trie::{Nibbles, Trie, Value};
use ae_types::Id;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    NotPresent,
    NoStateTrees,
    InvalidObject,
    ResourceLimit,
    Backend(repo::Error),
}
impl From<repo::Error> for Error {
    fn from(e: repo::Error) -> Self {
        match e {
            repo::Error::ResourceLimit
            | repo::Error::Trie(ae_trie::Error::ResourceLimit)
            | repo::Error::Trie(ae_trie::Error::Decode(rlp::DecodeError::ResourceLimit)) => {
                Self::ResourceLimit
            }
            e => Self::Backend(e),
        }
    }
}
impl From<ae_state::channels::Error> for Error {
    fn from(e: ae_state::channels::Error) -> Self {
        use ae_state::channels::Error as E;
        match e {
            E::Tree(e) => repo::Error::Trie(e).into(),
            E::Object(ae_codec::object::ObjectError::Rlp(rlp::DecodeError::ResourceLimit)) => {
                Self::ResourceLimit
            }
            _ => Self::InvalidObject,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub reads: point::Limits,
    pub delegates: usize,
    pub retained_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            reads: point::Limits {
                read_bytes: 16 << 20,
                ..Default::default()
            },
            delegates: 4096,
            retained_bytes: 8 << 20,
        }
    }
}
pub struct Reader<S> {
    view: View<point::Meter<S>>,
    limits: Limits,
}
impl<S: Read> Reader<S> {
    pub fn new(view: &View<S>, limits: Limits) -> Result<Self, Error> {
        Ok(Self {
            view: point::view(view, limits.reads)?,
            limits,
        })
    }
    fn value<'a>(&self, bytes: &'a [u8]) -> Result<Rlp<'a>, Error> {
        rlp::decode_with_limits(
            bytes,
            rlp::DecodeLimits {
                max_input_bytes: self
                    .limits
                    .reads
                    .value_bytes
                    .min(self.limits.reads.rlp.max_input_bytes),
                ..self.limits.reads.rlp
            },
        )
        .map_err(|e| Error::from(repo::Error::Trie(e.into())))
    }
    pub fn query(&self, key: [u8; 32]) -> Result<Channel, Error> {
        let top = self.view.status().tip.ok_or(Error::NoStateTrees)?;
        let raw = self
            .view
            .source()
            .get(&repo::key(repo::RECORDS, top.0))?
            .ok_or(Error::NoStateTrees)?;
        self.value(&raw)?;
        let record = Record::decode(&raw)?;
        let header = self.view.require_header(top)?;
        if record.roots.state.hash().0 != header.common().state_hash {
            return Err(repo::Error::Corrupt("channel state binding").into());
        }
        let tree = Trie::from_root(record.roots.state.channels, self.view.source().clone())
            .map_err(repo::Error::from)?;
        let value = tree
            .get(&Nibbles::from_bytes(&key))
            .map_err(repo::Error::from)?
            .ok_or(Error::NotPresent)?;
        let Value::Bytes(bytes) = value else {
            return Err(Error::InvalidObject);
        };
        let Rlp::List(fields) = self.value(&bytes)? else {
            return Err(Error::InvalidObject);
        };
        let indices: &[usize] = if matches!(fields.get(1), Some(Rlp::Bytes([3]))) {
            &[8, 9]
        } else {
            &[8]
        };
        let mut delegates = 0usize;
        for &index in indices {
            let Some(Rlp::List(ids)) = fields.get(index) else {
                return Err(Error::InvalidObject);
            };
            delegates = delegates
                .checked_add(ids.len())
                .filter(|n| *n <= self.limits.delegates)
                .ok_or(Error::ResourceLimit)?;
        }
        delegates
            .checked_mul(std::mem::size_of::<Id>())
            .and_then(|n| n.checked_add(std::mem::size_of::<Channel>()))
            .and_then(|n| n.checked_add(bytes.len()))
            .filter(|n| *n <= self.limits.retained_bytes)
            .ok_or(Error::ResourceLimit)?;
        // Native v3 decoding preserves non-account delegate IDs. Do not impose
        // current constructor policy on historical stored records, or filter by
        // lock height/activity. Only the channel trie is read.
        Channel::deserialize(key, &bytes).map_err(Into::into)
    }
}
#[cfg(test)]
mod tests;
