// aec_chain name/auction queries and aens lookup, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use crate::repository::{self as repo, Read, Record, View, point};
use ae_codec::{
    idna,
    rlp::{self, Rlp},
};
use ae_state::names::{Auction, Name, Pointer, hashing};
use ae_trie::{Nibbles, Trie, Value};
use ae_types::IdTag;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Name,
    Hash,
    Auction,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Name(Name),
    Auction(Auction),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    NotPresent,
    Revoked,
    NoStateTrees,
    Normalization(idna::Error),
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
impl From<ae_state::names::Error> for Error {
    fn from(e: ae_state::names::Error) -> Self {
        use ae_state::names::Error as E;
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
    pub input_bytes: usize,
    pub pointers: usize,
    pub retained_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            reads: point::Limits {
                read_bytes: 16 << 20,
                ..Default::default()
            },
            input_bytes: 4096,
            pointers: 4096,
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
    fn retained(&self, serialized: usize, overhead: usize) -> Result<(), Error> {
        serialized
            .checked_add(overhead)
            .filter(|n| *n <= self.limits.retained_bytes)
            .ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    pub fn query(&self, input: &[u8], kind: Kind) -> Result<Reply, Error> {
        // Bound normalization and key allocation independently of consensus.
        if input.len() > self.limits.input_bytes {
            return Err(Error::ResourceLimit);
        }
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
            return Err(repo::Error::Corrupt("name state binding").into());
        }
        // Native current-state absence is observed before text normalization.
        // Hash input is already API-decoded by the caller and may have any width.
        let key = match kind {
            Kind::Hash => input.to_vec(),
            Kind::Name | Kind::Auction => {
                let ascii = idna::to_ascii(input).map_err(Error::Normalization)?;
                let hash = hashing::name_hash(&ascii).0;
                if kind == Kind::Auction {
                    hashing::auction_hash(hash).to_vec()
                } else {
                    hash.to_vec()
                }
            }
        };
        // Do not open the expiry cache or apply a read-time TTL filter.
        let tree = Trie::from_root(record.roots.state.names, self.view.source().clone())
            .map_err(repo::Error::from)?;
        let value = tree
            .get(&Nibbles::from_bytes(&key))
            .map_err(repo::Error::from)?
            .ok_or(Error::NotPresent)?;
        let Value::Bytes(bytes) = value else {
            return Err(Error::InvalidObject);
        };
        let decoded = self.value(&bytes)?;
        if kind == Kind::Auction {
            self.retained(bytes.len(), std::mem::size_of::<Auction>())?;
            let auction = Auction::deserialize(
                key.as_slice()
                    .try_into()
                    .map_err(|_| Error::InvalidObject)?,
                &bytes,
            )?;
            if auction.bidder.tag != IdTag::Account {
                return Err(Error::InvalidObject);
            }
            return Ok(Reply::Auction(auction));
        }
        // Preflight pointer counts and retained storage before the object
        // decoder allocates pointer containers and owned keys/data.
        let Rlp::List(fields) = decoded else {
            return Err(Error::InvalidObject);
        };
        let Some(Rlp::List(pointers)) = fields.get(6) else {
            return Err(Error::InvalidObject);
        };
        if pointers.len() > self.limits.pointers {
            return Err(Error::ResourceLimit);
        }
        let overhead = pointers
            .len()
            .checked_mul(std::mem::size_of::<Pointer>())
            .and_then(|n| n.checked_add(std::mem::size_of::<Name>()))
            .ok_or(Error::ResourceLimit)?;
        self.retained(bytes.len(), overhead)?;
        let name = Name::deserialize(
            key.as_slice()
                .try_into()
                .map_err(|_| Error::InvalidObject)?,
            &bytes,
        )?;
        match name.status.as_slice() {
            b"revoked" => return Err(Error::Revoked),
            b"claimed" => (),
            _ => return Err(Error::InvalidObject),
        };
        if name.owner.tag != IdTag::Account {
            return Err(Error::InvalidObject);
        }
        Ok(Reply::Name(name))
    }
}
#[cfg(test)]
mod tests;
