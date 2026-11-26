// aec_chain:get_contract_call/get_ga_call, aeternity fa2a6d7.
// Copyright (c) 2017-2026, Aeternity developers. ISC: repository LICENSE.
use super::{Error, Ga, Lookup};
use crate::repository::{self as repo, Read, Record, View};
use ae_codec::rlp::{self, DecodeLimits};
use ae_state::{
    accounts::Account,
    calls::{Call, ga_call_id},
};
use ae_trie::{Nibbles, NodeReader, Trie, Value};
use ae_types::{AccountPublicKey, Hash256, IdTag};
use std::{cell::Cell, rc::Rc};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub read_bytes: usize,
    pub reads: usize,
    pub node_bytes: usize,
    pub value_bytes: usize,
    pub rlp: DecodeLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            read_bytes: 64 << 20,
            reads: 4096,
            node_bytes: 8 << 20,
            value_bytes: 4 << 20,
            rlp: DecodeLimits {
                max_input_bytes: 8 << 20,
                max_items: 65_536,
                max_depth: 64,
            },
        }
    }
}
#[derive(Clone)]
struct Meter<S> {
    source: S,
    bytes: Rc<Cell<usize>>,
    reads: Rc<Cell<usize>>,
    limits: Limits,
}
impl<S: Read> Meter<S> {
    fn read(&self, key: &[u8], limit: usize) -> Result<Option<Vec<u8>>, repo::Error> {
        self.reads.set(
            self.reads
                .get()
                .checked_sub(1)
                .ok_or(repo::Error::ResourceLimit)?,
        );
        // Pinned storage checks the remaining shared credit before copying.
        let bytes = self.source.get_bounded(key, limit.min(self.bytes.get()))?;
        if let Some(bytes) = &bytes {
            self.bytes.set(
                self.bytes
                    .get()
                    .checked_sub(bytes.len())
                    .ok_or(repo::Error::ResourceLimit)?,
            );
        }
        Ok(bytes)
    }
}
impl<S: Read> NodeReader for Meter<S> {
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.read(&repo::key(repo::NODES, hash.0), self.limits.node_bytes)
            .map_err(|e| match e {
                repo::Error::ResourceLimit => ae_trie::Error::ResourceLimit,
                repo::Error::Trie(e) => e,
                e => ae_trie::Error::Backend(e.to_string()),
            })
    }
    fn decode_limits(&self) -> DecodeLimits {
        self.limits.rlp
    }
}
impl<S: Read> Read for Meter<S> {
    fn source_id(&self) -> u64 {
        self.source.source_id()
    }
    fn resilience(&self) -> crate::resilience::Snapshot {
        self.source.resilience()
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, repo::Error> {
        self.read(key, self.limits.value_bytes)
    }
    fn get_bounded(&self, key: &[u8], bytes: usize) -> Result<Option<Vec<u8>>, repo::Error> {
        self.read(key, bytes)
    }
    // This adapter admits point reads only; no unmetered scan fallback.
    fn prefix(&self, _: &[u8], _: usize) -> Result<repo::Entries, repo::Error> {
        Err(repo::Error::ResourceLimit)
    }
    fn range(&self, _: &[u8], _: &[u8], _: usize) -> Result<repo::Entries, repo::Error> {
        Err(repo::Error::ResourceLimit)
    }
    fn visit_range(
        &self,
        _: &[u8],
        _: &[u8],
        _: usize,
        _: &mut repo::Visitor<'_>,
    ) -> Result<(), repo::Error> {
        Err(repo::Error::ResourceLimit)
    }
}
pub struct Reader<S> {
    view: View<Meter<S>>,
    limits: Limits,
}
impl<S: Read> Reader<S> {
    pub fn new(view: &View<S>, limits: Limits) -> Result<Self, Error> {
        let source = Meter {
            source: view.source().clone(),
            bytes: Rc::new(Cell::new(limits.read_bytes)),
            reads: Rc::new(Cell::new(limits.reads)),
            limits,
        };
        let view = View::load_with_config(source, view.config().clone())?;
        Ok(Self { view, limits })
    }
    fn record(&self, block: Hash256) -> Result<Record, Error> {
        let bytes = self
            .view
            .source()
            .get(&repo::key(repo::RECORDS, block.0))?
            .ok_or(Error::NoStateTrees)?;
        self.check_value(&bytes)?;
        Record::decode(&bytes).map_err(Into::into)
    }
    fn check_value(&self, bytes: &[u8]) -> Result<(), Error> {
        rlp::decode_with_limits(
            bytes,
            DecodeLimits {
                max_input_bytes: self.limits.value_bytes.min(self.limits.rlp.max_input_bytes),
                ..self.limits.rlp
            },
        )
        .map_err(|e| Error::Backend(repo::Error::Trie(e.into())))?;
        Ok(())
    }
    fn value(&self, root: Option<Hash256>, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let trie = Trie::from_root(root, self.view.source().clone()).map_err(repo::Error::from)?;
        match trie
            .get(&Nibbles::from_bytes(key))
            .map_err(repo::Error::from)?
        {
            Some(Value::Bytes(bytes)) => {
                self.check_value(&bytes)?;
                Ok(Some(bytes))
            }
            None => Ok(None),
            _ => Err(Error::InvalidObject),
        }
    }
    fn call(&self, record: Record, namespace: [u8; 32], id: Hash256) -> Result<Call, Error> {
        let key = [namespace.as_slice(), id.as_ref()].concat();
        let bytes = self
            .value(record.roots.state.calls, &key)?
            .ok_or(Error::CallNotFound)?;
        Call::deserialize(id.0, &bytes).map_err(|_| Error::InvalidObject)
    }
}
impl<S: Read> Lookup for Reader<S> {
    fn contract(&mut self, contract: [u8; 32], id: Hash256, block: Hash256) -> Result<Call, Error> {
        let header = self.view.require_header(block)?;
        match self.view.require_retained(header.common().height) {
            Err(repo::Error::Pruned { .. }) => return Err(Error::GarbageCollected),
            r => r?,
        }
        let record = self.record(block)?;
        if record.roots.state.hash().0 != header.common().state_hash {
            return Err(repo::Error::Corrupt("call state binding").into());
        }
        self.call(record, contract, id)
    }
    fn ga(&mut self, owner: AccountPublicKey, auth: Hash256, block: Hash256) -> Result<Ga, Error> {
        let top = self.view.status().tip.ok_or(Error::InvalidObject)?;
        // Original GA lookup uses current-top authorization, not the historical
        // account. All selected records still belong to this immutable view.
        let current = self.record(top).map_err(|e| {
            if e == Error::NoStateTrees {
                Error::InvalidObject
            } else {
                e
            }
        })?;
        let bytes = self
            .value(current.roots.state.accounts, &owner.0)?
            .ok_or(Error::InvalidObject)?;
        let account = Account::deserialize(owner, &bytes).map_err(|_| Error::InvalidObject)?;
        let authorization = account.authorization().ok_or(Error::InvalidObject)?;
        if authorization.contract.tag != IdTag::Contract {
            return Err(Error::InvalidObject);
        }
        let id = ga_call_id(auth.0, authorization.contract.value);
        // There is no native header/GC gate here. The durable state record
        // selects the call root, and each fetched MPT node verifies its hash.
        let record = self.record(block)?;
        Ga::from_call(self.call(record, owner.0, id)?)
    }
}
