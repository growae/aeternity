// aec_chain oracle queries and aeo_state_tree range semantics, fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use crate::repository::{self as repo, Read, Record, View, point};
use ae_codec::rlp;
use ae_state::oracles::{Oracle, Query, QueryFilter};
use ae_trie::{IteratorOptions, Nibbles, Trie, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Metadata,
    Query([u8; 32]),
    Queries {
        after: Option<[u8; 32]>,
        filter: QueryFilter,
        limit: usize,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Metadata(Oracle),
    Query(Query),
    Queries(Vec<Query>),
}
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
impl From<ae_state::oracles::Error> for Error {
    fn from(e: ae_state::oracles::Error) -> Self {
        use ae_state::oracles::Error as E;
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
    pub scanned_items: usize,
    pub returned_items: usize,
    pub retained_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            reads: point::Limits {
                read_bytes: 16 << 20,
                ..Default::default()
            },
            scanned_items: 4096,
            returned_items: 1000,
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
    fn check_value(&self, bytes: &[u8]) -> Result<(), Error> {
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
        .map_err(|e| Error::from(repo::Error::Trie(e.into())))?;
        Ok(())
    }
    fn bytes(&self, value: Value) -> Result<Vec<u8>, Error> {
        let Value::Bytes(bytes) = value else {
            return Err(Error::InvalidObject);
        };
        self.check_value(&bytes)?;
        Ok(bytes)
    }
    pub fn query(&self, key: [u8; 32], kind: Kind) -> Result<Reply, Error> {
        if matches!(kind, Kind::Queries { limit, .. } if limit > self.limits.returned_items) {
            return Err(Error::ResourceLimit);
        }
        let top = self.view.status().tip.ok_or(Error::NoStateTrees)?;
        let raw = self
            .view
            .source()
            .get(&repo::key(repo::RECORDS, top.0))?
            .ok_or(Error::NoStateTrees)?;
        self.check_value(&raw)?;
        let record = Record::decode(&raw)?;
        let header = self.view.require_header(top)?;
        if record.roots.state.hash().0 != header.common().state_hash {
            return Err(repo::Error::Corrupt("oracle state binding").into());
        }
        // The expiry index is unrelated to client reads. TTL filtering belongs
        // to block execution; a read exposes exactly the authenticated state.
        let tree = Trie::from_root(record.roots.state.oracles, self.view.source().clone())
            .map_err(repo::Error::from)?;
        let mut remaining = self.limits.retained_bytes;
        match kind {
            Kind::Metadata => {
                let value = tree
                    .get(&Nibbles::from_bytes(&key))
                    .map_err(repo::Error::from)?
                    .ok_or(Error::NotPresent)?;
                let bytes = self.bytes(value)?;
                retain::<Oracle>(&mut remaining, bytes.len())?;
                Ok(Reply::Metadata(Oracle::deserialize(key, &bytes)?))
            }
            Kind::Query(id) => {
                let value = tree
                    .get(&Nibbles::from_bytes(&query_key(key, id)))
                    .map_err(repo::Error::from)?
                    .ok_or(Error::NotPresent)?;
                let bytes = self.bytes(value)?;
                retain::<Query>(&mut remaining, bytes.len())?;
                Ok(Reply::Query(Query::deserialize(id, &bytes)?))
            }
            Kind::Queries {
                after,
                filter,
                limit,
            } => {
                let mut out = Vec::new();
                if limit == 0 {
                    return Ok(Reply::Queries(out));
                }
                let after = after.map_or_else(|| key.to_vec(), |id| query_key(key, id).to_vec());
                for (visited, item) in tree
                    .iter_with_options(IteratorOptions {
                        after: Some(Nibbles::from_bytes(&after)),
                        ..Default::default()
                    })
                    .enumerate()
                {
                    let (key, value) = item.map_err(repo::Error::from)?;
                    // Native iteration stops at the next oracle record and
                    // deliberately does not constrain the oracle prefix.
                    if key.as_slice().len() <= 64 {
                        break;
                    }
                    if visited >= self.limits.scanned_items {
                        return Err(Error::ResourceLimit);
                    }
                    if key.as_slice().len() != 128 {
                        return Err(Error::InvalidObject);
                    }
                    let mut id = [0; 32];
                    for (byte, pair) in id.iter_mut().zip(key.as_slice()[64..].chunks_exact(2)) {
                        *byte = (pair[0] << 4) | pair[1];
                    }
                    let bytes = self.bytes(value)?;
                    let query = Query::deserialize(id, &bytes)?;
                    if filter == QueryFilter::All
                        || query.is_open() == (filter == QueryFilter::Open)
                    {
                        retain::<Query>(&mut remaining, bytes.len())?;
                        out.try_reserve_exact(1).map_err(|_| Error::ResourceLimit)?;
                        out.push(query);
                        if out.len() == limit {
                            break;
                        }
                    }
                }
                Ok(Reply::Queries(out))
            }
        }
    }
}
fn query_key(oracle: [u8; 32], id: [u8; 32]) -> [u8; 64] {
    let mut key = [0; 64];
    key[..32].copy_from_slice(&oracle);
    key[32..].copy_from_slice(&id);
    key
}
fn retain<T>(remaining: &mut usize, serialized: usize) -> Result<(), Error> {
    // Serialized fields bound owned byte buffers; account for the containing
    // object separately. Reject the complete result on any budget failure.
    *remaining = serialized
        .checked_add(std::mem::size_of::<T>())
        .and_then(|n| remaining.checked_sub(n))
        .ok_or(Error::ResourceLimit)?;
    Ok(())
}

#[cfg(test)]
mod tests;
