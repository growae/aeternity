use super::*;
use crate::{
    StateRoots, Trees,
    accounts::{Account, Accounts},
    contracts::{Contract, Contracts, store::Contents},
};
use ae_trie::{MemoryStore, Value as TreeValue};
use ae_types::{AccountPublicKey, Hash256};
use std::collections::BTreeMap;

mod encoding;
mod walk;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proof {
    root: Hash256,
    // Decoding stores the original list-of-binaries node representation.
    // Arbitrary keys and malformed/unrelated nodes are not rejected eagerly.
    nodes: BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
}
impl Proof {
    pub fn new(root: Hash256) -> Self {
        Self {
            root,
            nodes: BTreeMap::new(),
        }
    }
    pub fn root_hash(&self) -> Hash256 {
        self.root
    }
    pub fn nodes(&self) -> &BTreeMap<Vec<u8>, Vec<Vec<u8>>> {
        &self.nodes
    }
    pub fn from_parts(
        root: Hash256,
        nodes: impl IntoIterator<Item = (Vec<u8>, Vec<Vec<u8>>)>,
    ) -> Self {
        Self {
            root,
            nodes: nodes.into_iter().collect(),
        }
    }
    pub fn lookup(&self, key: &[u8], limits: Limits) -> Result<Option<TreeValue>, Error> {
        self.lookup_with_budget(key, &Budget::new(limits))
    }
    fn lookup_with_budget(&self, key: &[u8], b: &Budget) -> Result<Option<TreeValue>, Error> {
        b.entry(key)?;
        match self.walk(key, None, b) {
            Err(Error::BadValue(v)) if !v.is_empty() => Ok(Some(v)),
            Err(e @ (Error::ResourceLimit | Error::NativeBadarg)) => Err(e),
            _ => Ok(None),
        }
    }
    pub fn verify(&self, key: &[u8], expected: &TreeValue, limits: Limits) -> Result<(), Error> {
        self.verify_with_budget(key, expected, &Budget::new(limits))
    }
    fn verify_with_budget(
        &self,
        key: &[u8],
        expected: &TreeValue,
        b: &Budget,
    ) -> Result<(), Error> {
        b.entry(key)?;
        self.walk(key, Some(expected), b)
    }
    fn merge(&mut self, nodes: &MemoryStore, b: &Budget) -> Result<(), Error> {
        let mut additional = BTreeMap::new();
        for (key, bytes) in nodes.nodes() {
            b.entry(&key.0)?;
            b.charge(bytes.len())?;
            let value = rlp::decode_with_limits(
                bytes,
                rlp::DecodeLimits {
                    max_input_bytes: b.limits.max_input_bytes,
                    max_items: b.limits.max_items,
                    max_depth: b.limits.max_depth,
                },
            )
            .map_err(object::ObjectError::from)?;
            let rlp::Rlp::List(values) = value else {
                return Err(Error::InvalidValue);
            };
            let values = values
                .into_iter()
                .map(|v| match v {
                    rlp::Rlp::Bytes(b) => Ok(b.to_vec()),
                    _ => Err(Error::InvalidValue),
                })
                .collect::<Result<Vec<_>, _>>()?;
            additional.insert(key.0.to_vec(), values);
        }
        self.nodes.extend(additional);
        Ok(())
    }
}
struct Reader<'a> {
    proof: &'a Proof,
    budget: &'a Budget,
}
impl NodeReader for Reader<'_> {
    fn verify_node_hashes(&self) -> bool {
        false
    }
    fn decode_limits(&self) -> rlp::DecodeLimits {
        self.budget.decode_limits()
    }

    fn node(&self, key: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.budget
            .charge(1)
            .map_err(|_| ae_trie::Error::ResourceLimit)?;
        let Some(values) = self.proof.nodes.get(key.0.as_slice()) else {
            return Ok(None);
        };
        for value in values {
            self.budget
                .charge(value.len().saturating_add(1))
                .map_err(|_| ae_trie::Error::ResourceLimit)?;
        }
        let bytes = rlp::encode(&rlp::Rlp::List(
            values.iter().map(|v| rlp::Rlp::Bytes(v)).collect(),
        ));
        self.budget
            .charge(bytes.len())
            .map_err(|_| ae_trie::Error::ResourceLimit)?;
        Ok(Some(bytes))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Proofs {
    pub accounts: Option<Proof>,
    pub calls: Option<Proof>,
    pub channels: Option<Proof>,
    pub contracts: Option<Proof>,
    pub names: Option<Proof>,
    pub oracles: Option<Proof>,
}
impl Proofs {
    pub fn new(roots: &StateRoots) -> Self {
        let part = |root: Option<Hash256>| root.filter(|r| *r != Hash256::ZERO).map(Proof::new);
        Self {
            accounts: part(roots.accounts),
            calls: part(roots.calls),
            channels: part(roots.channels),
            contracts: part(roots.contracts),
            names: part(roots.names),
            oracles: part(roots.oracles),
        }
    }
    pub fn from_trees<A: NodeReader, S: NodeReader>(trees: &Trees<A, S>) -> Self {
        Self::new(&trees.roots())
    }
    pub fn roots(&self) -> StateRoots {
        StateRoots {
            accounts: self.accounts.as_ref().map(Proof::root_hash),
            calls: self.calls.as_ref().map(Proof::root_hash),
            channels: self.channels.as_ref().map(Proof::root_hash),
            contracts: self.contracts.as_ref().map(Proof::root_hash),
            names: self.names.as_ref().map(Proof::root_hash),
            oracles: self.oracles.as_ref().map(Proof::root_hash),
        }
    }
    pub fn hash(&self) -> Hash256 {
        self.roots().hash()
    }
    pub fn add_account<S: NodeReader>(
        &mut self,
        key: AccountPublicKey,
        accounts: &Accounts<S>,
        limits: Limits,
    ) -> Result<(), Error> {
        let p = self.accounts.as_mut().ok_or(Error::NotPresent)?;
        if accounts.root_hash() != Some(p.root) {
            return Err(Error::WrongRootHash);
        }
        let b = Budget::new(limits);
        let tree = Trie::from_root(
            accounts.root_hash(),
            ReadBudget {
                store: accounts.tree().store(),
                budget: &b,
            },
        )?;
        let (value, nodes) = tree.lookup_with_proof(&Nibbles::from_bytes(&key.0))?;
        if value.is_none() {
            return Err(Error::NotPresent);
        }
        p.merge(&nodes, &b)
    }
    pub fn add_contract<S: NodeReader>(
        &mut self,
        key: [u8; 32],
        contracts: &Contracts<S>,
        limits: Limits,
    ) -> Result<(), Error> {
        let p = self.contracts.as_mut().ok_or(Error::NotPresent)?;
        if contracts.root_hash() != Some(p.root) {
            return Err(Error::WrongRootHash);
        }
        let b = Budget::new(limits);
        let tree = Contracts::from_root(
            contracts.root_hash(),
            ReadBudget {
                store: contracts.tree().store(),
                budget: &b,
            },
        )
        .map_err(Error::from)?;
        let proof = tree.proof(key).map_err(|e| match e {
            crate::contracts::Error::NotPresent => Error::NotPresent,
            e => Error::from(e),
        })?;
        p.merge(proof.nodes(), &b)
    }
    pub fn lookup_account(
        &self,
        key: AccountPublicKey,
        limits: Limits,
    ) -> Result<Option<Account>, Error> {
        let Some(p) = &self.accounts else {
            return Ok(None);
        };
        let b = Budget::new(limits);
        match p.lookup_with_budget(&key.0, &b)? {
            Some(TreeValue::Bytes(bytes)) => {
                b.envelope(&bytes)?;
                Ok(Some(
                    Account::deserialize(key, &bytes).map_err(Error::from)?,
                ))
            }
            None => Ok(None),
            _ => Err(Error::InvalidValue),
        }
    }
    pub fn verify_account(
        &self,
        key: AccountPublicKey,
        account: &Account,
        limits: Limits,
    ) -> Result<(), Error> {
        let p = self.accounts.as_ref().ok_or(Error::EmptyAccountsProof)?;
        if key != account.public_key() {
            return Err(Error::InvalidValue);
        }
        p.verify(
            &key.0,
            &TreeValue::Bytes(account.serialize().map_err(Error::from)?),
            limits,
        )
    }
    pub fn lookup_contract(
        &self,
        key: [u8; 32],
        limits: Limits,
    ) -> Result<Option<(Contract, Contents)>, Error> {
        let Some(p) = &self.contracts else {
            return Ok(None);
        };
        let b = Budget::new(limits);
        Self::contract(p, key, &b)
    }
    fn contract(
        p: &Proof,
        key: [u8; 32],
        b: &Budget,
    ) -> Result<Option<(Contract, Contents)>, Error> {
        let Some(TreeValue::Bytes(raw)) = p.lookup_with_budget(&key, b)? else {
            return Ok(None);
        };
        b.envelope(&raw)?;
        let contract = Contract::deserialize(key, &raw).map_err(Error::from)?;
        let tree = Contracts::from_root(
            Some(p.root),
            Reader {
                proof: p,
                budget: b,
            },
        )
        .map_err(store_native_error)?;
        let store = tree.read_store(key).map_err(store_native_error)?;
        let contents = store.contents().map_err(store_native_error)?;
        charge_contents(&contents, b)?;
        Ok(Some((contract, contents)))
    }
    pub fn verify_contract(
        &self,
        key: [u8; 32],
        contract: &Contract,
        contents: &Contents,
        limits: Limits,
    ) -> Result<(), Error> {
        let p = self.contracts.as_ref().ok_or(Error::EmptyContractsProof)?;
        if key != contract.public_key {
            return Err(Error::InvalidValue);
        }
        let b = Budget::new(limits);
        p.verify_with_budget(
            &key,
            &TreeValue::Bytes(contract.serialize().map_err(Error::from)?),
            &b,
        )?;
        // Only lookup_store_poi is inside the original try expression. Errors
        // while materializing Store1 in its `of` body remain native failures.
        let tree = Contracts::from_root(
            Some(p.root),
            Reader {
                proof: p,
                budget: &b,
            },
        )
        .map_err(store_verify_error)?;
        let store = tree.read_store(key).map_err(store_verify_error)?;
        let actual = store.contents().map_err(store_native_error)?;
        charge_contents(&actual, &b)?;
        charge_contents(contents, &b)?;
        if actual == *contents && contract.validate_writes(contents).is_ok() {
            Ok(())
        } else {
            Err(Error::BadProof)
        }
    }
}
fn store_native_error(e: crate::contracts::Error) -> Error {
    match e {
        crate::contracts::Error::Tree(
            ae_trie::Error::MissingNode(_)
            | ae_trie::Error::InvalidNode
            | ae_trie::Error::InvalidPath
            | ae_trie::Error::InvalidReference,
        ) => Error::NativeFunctionClause,
        e => Error::from(e),
    }
}
fn store_verify_error(e: crate::contracts::Error) -> Error {
    match Error::from(e) {
        Error::ResourceLimit => Error::ResourceLimit,
        _ => Error::BadProof,
    }
}

fn charge_contents(contents: &Contents, b: &Budget) -> Result<(), Error> {
    for (key, value) in contents {
        b.entry(key)?;
        b.charge(value.len())?;
    }
    Ok(())
}
