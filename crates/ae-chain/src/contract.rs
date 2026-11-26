// aec_chain contract queries and aehttp_helpers:get_poi, fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use crate::repository::{self as repo, Read, Record, View, point};
use ae_codec::rlp;
use ae_state::{
    accounts::Accounts,
    contracts::{Code, Contract, Contracts},
    transport::{self, Proofs},
};
use ae_trie::{Nibbles, Value};
use ae_types::{AccountPublicKey, IdTag};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Metadata,
    Code,
    Proof,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Metadata(Contract),
    Code(Vec<u8>),
    Proof(Vec<u8>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    NotPresent,
    AccountNotPresent,
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
impl From<ae_state::contracts::Error> for Error {
    fn from(e: ae_state::contracts::Error) -> Self {
        use ae_state::contracts::Error as E;
        match e {
            E::Tree(e) => repo::Error::Trie(e).into(),
            E::ResourceLimit
            | E::Object(ae_codec::object::ObjectError::Rlp(rlp::DecodeError::ResourceLimit)) => {
                Self::ResourceLimit
            }
            _ => Self::InvalidObject,
        }
    }
}
impl From<transport::Error> for Error {
    fn from(e: transport::Error) -> Self {
        match e {
            transport::Error::ResourceLimit => Self::ResourceLimit,
            transport::Error::Contract(e) => e.into(),
            transport::Error::Tree(e) => repo::Error::Trie(e).into(),
            _ => Self::InvalidObject,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub reads: point::Limits,
    pub proof: transport::Limits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            reads: point::Limits {
                read_bytes: 16 << 20,
                ..Default::default()
            },
            proof: transport::Limits {
                max_input_bytes: 8 << 20,
                max_items: 65_536,
                max_depth: 64,
                max_entries: 65_536,
                max_key_bytes: 256,
                max_work_bytes: 32 << 20,
            },
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
    fn contract(
        &self,
        tree: &Contracts<point::Meter<S>>,
        key: [u8; 32],
    ) -> Result<Contract, Error> {
        let bytes = match tree
            .tree()
            .get(&Nibbles::from_bytes(&key))
            .map_err(repo::Error::from)?
        {
            Some(Value::Bytes(bytes)) => bytes,
            None => return Err(Error::NotPresent),
            _ => return Err(Error::InvalidObject),
        };
        self.check_value(&bytes)?;
        let contract = Contract::deserialize(key, &bytes)?;
        // Native get_contract opens a lazy store even for metadata/code.
        // A missing store is not the same as a missing contract record.
        let store = tree.read_store(key)?;
        contract.validate_writes(store.write_cache())?;
        Ok(contract)
    }
    pub fn query(&self, key: [u8; 32], kind: Kind) -> Result<Reply, Error> {
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
            return Err(repo::Error::Corrupt("contract state binding").into());
        }
        let tree = Contracts::from_root(record.roots.state.contracts, self.view.source().clone())?;
        if kind == Kind::Proof {
            let mut proofs = Proofs::new(&record.roots.state);
            // Contract proof includes every store path, then the account path.
            // It does not resolve code references or require a store marker.
            proofs
                .add_contract(key, &tree, self.limits.proof)
                .map_err(|e| match e {
                    transport::Error::NotPresent => Error::NotPresent,
                    e => e.into(),
                })?;
            let accounts =
                Accounts::from_root(record.roots.state.accounts, self.view.source().clone())
                    .map_err(|e| Error::from(transport::Error::from(e)))?;
            proofs
                .add_account(AccountPublicKey(key), &accounts, self.limits.proof)
                .map_err(|e| match e {
                    transport::Error::NotPresent => Error::AccountNotPresent,
                    e => e.into(),
                })?;
            return Ok(Reply::Proof(proofs.serialize(self.limits.proof)?));
        }
        let contract = self.contract(&tree, key)?;
        if kind == Kind::Metadata {
            return Ok(Reply::Metadata(contract));
        }
        let code = match contract.code {
            Code::Inline(code) => code,
            Code::Reference(reference) if reference.tag == IdTag::Contract => {
                let reference = self.contract(&tree, reference.value).map_err(|e| match e {
                    Error::NotPresent => Error::InvalidObject,
                    e => e,
                })?;
                match reference.code {
                    Code::Inline(code) => code,
                    Code::Reference(_) => return Err(Error::InvalidObject),
                }
            }
            _ => return Err(Error::InvalidObject),
        };
        Ok(Reply::Code(code))
    }
}

#[cfg(test)]
mod tests;
