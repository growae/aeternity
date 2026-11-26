// aec_chain:get_contract_call/get_ga_call, aeternity fa2a6d7.
// Copyright (c) 2017-2026, Aeternity developers. ISC: repository LICENSE.
use super::{Error, Ga, Lookup};
pub use crate::repository::point::Limits;
use crate::repository::point::{self, Meter};
use crate::repository::{self as repo, Read, Record, View};
use ae_codec::rlp::{self, DecodeLimits};
use ae_state::{
    accounts::Account,
    calls::{Call, ga_call_id},
};
use ae_trie::{Nibbles, Trie, Value};
use ae_types::{AccountPublicKey, Hash256, IdTag};

#[cfg(test)]
mod tests;

pub struct Reader<S> {
    view: View<Meter<S>>,
    limits: Limits,
}
impl<S: Read> Reader<S> {
    pub fn new(view: &View<S>, limits: Limits) -> Result<Self, Error> {
        let view = point::view(view, limits)?;
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
