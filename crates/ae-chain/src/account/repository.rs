use super::{Error, Info, Limits, Selection, authentication_name};
use crate::repository::{
    self as repo, Read, Record, View,
    point::{self, Meter},
};
use ae_codec::rlp::{self, DecodeLimits};
use ae_state::{
    accounts::Account,
    contracts::{Code, Contract},
};
use ae_trie::{Nibbles, Trie, Value};
use ae_types::{AccountPublicKey, Hash256, IdTag};

#[cfg(test)]
mod tests;

pub struct Reader<S> {
    view: View<Meter<S>>,
    limits: Limits,
    gc_enabled: bool,
}
impl<S: Read> Reader<S> {
    pub fn new(view: &View<S>, limits: Limits, gc_enabled: bool) -> Result<Self, Error> {
        Ok(Self {
            view: point::view(view, limits.reads)?,
            limits,
            gc_enabled,
        })
    }
    pub fn account(&self, key: AccountPublicKey, selection: Selection) -> Result<Account, Error> {
        let hash = match selection {
            Selection::Current => self.view.status().tip.ok_or(Error::NoStateTrees)?,
            Selection::Hash(hash) => hash,
            Selection::Height(height) => {
                // Native reachability precedes canonical-height lookup, and is
                // disabled when GC is disabled even if a previous floor exists.
                if self.gc_enabled && height < u128::from(self.view.retention()?.height) {
                    return Err(Error::GarbageCollected);
                }
                let height = u64::try_from(height).map_err(|_| Error::HeightNotAvailable)?;
                self.view
                    .canonical_key(height)?
                    .ok_or(Error::HeightNotAvailable)?
            }
        };
        let record = self.record(hash)?;
        let bytes = self
            .value(record.roots.state.accounts, &key.0)?
            .ok_or(Error::AccountNotFound)?;
        Account::deserialize(key, &bytes).map_err(|_| Error::InvalidObject)
    }
    // Kept separate from account lookup: pending and nonce calls must not
    // resolve authorization code (GA next_nonce returns zero before pool reads).
    pub fn describe(&self, account: Account) -> Result<Info, Error> {
        let auth_fun = if let Some(auth) = account.authorization() {
            if auth.contract.tag != IdTag::Contract {
                return Err(Error::InvalidObject);
            }
            let top = self.view.status().tip.ok_or(Error::InvalidObject)?;
            // Missing current code state is a serialization failure, never a
            // "hash not available" response for the historical account query.
            let record = self.record(top).map_err(|e| match e {
                Error::NoStateTrees => Error::InvalidObject,
                e => e,
            })?;
            let contract = self.contract(record.roots.state.contracts, auth.contract.value)?;
            let code = match contract.code {
                Code::Inline(code) => code,
                Code::Reference(reference) if reference.tag == IdTag::Contract => {
                    // Original get_contract_with_code follows exactly one ref.
                    match self
                        .contract(record.roots.state.contracts, reference.value)?
                        .code
                    {
                        Code::Inline(code) => code,
                        Code::Reference(_) => return Err(Error::InvalidObject),
                    }
                }
                _ => return Err(Error::InvalidObject),
            };
            Some(authentication_name(
                contract.version.abi,
                &auth.function,
                &code,
                self.limits,
            )?)
        } else {
            None
        };
        Ok(Info { account, auth_fun })
    }
    fn contract(&self, root: Option<Hash256>, key: [u8; 32]) -> Result<Contract, Error> {
        let bytes = self.value(root, &key)?.ok_or(Error::InvalidObject)?;
        Contract::deserialize(key, &bytes).map_err(|_| Error::InvalidObject)
    }
    fn record(&self, hash: Hash256) -> Result<Record, Error> {
        let bytes = self
            .view
            .source()
            .get(&repo::key(repo::RECORDS, hash.0))?
            .ok_or(Error::NoStateTrees)?;
        self.check_value(&bytes)?;
        let record = Record::decode(&bytes)?;
        // No blanket GC gate or transaction reads for hash/current queries.
        // State is still bound to the authenticated header in this repository.
        let header = self.view.require_header(hash)?;
        if record.roots.state.hash().0 != header.common().state_hash {
            return Err(repo::Error::Corrupt("account state binding").into());
        }
        Ok(record)
    }
    fn check_value(&self, bytes: &[u8]) -> Result<(), Error> {
        rlp::decode_with_limits(
            bytes,
            DecodeLimits {
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
}
