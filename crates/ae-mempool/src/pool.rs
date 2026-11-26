// aec_tx_pool / aec_tx_pool_gc, aeternity fa2a6d7.
// Copyright (c) 2018-2019, Aeternity Anstalt. ISC: repository LICENSE.
use ae_execution::transactions::{SignatureError, SignatureLimits};
use ae_protocol::{ConsensusType, NetworkProfile, Protocol};
use ae_state::{Trees, accounts::Account};
use ae_trie::NodeReader;
use ae_tx::{SignedTransaction, Transaction, TransactionLeaf, TransactionRef, Wrapper};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use num_bigint::{BigInt, Sign};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
pub mod accounts;
mod admission;
mod maintenance;
mod selection;
#[cfg(test)]
mod tests;
pub use maintenance::{FailureSettings, Reconcile};
pub use selection::{Candidate, CandidateRequest, MIN_TRANSACTION_GAS, SelectOptions};
pub type Signed = SignedTransaction<Transaction>;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Created,
    Received,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Location {
    Unknown,
    Deleted,
    Pool,
    Block(Hash256),
}
#[derive(Debug, Clone, Copy)]
pub struct PushOptions {
    pub event: Event,
    pub location: Location,
    pub gossip_cached: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    AlreadyKnown,
    AlreadyAccepted,
    NotFound,
    NoOrigin,
    NonceTooLow,
    NonceTooHigh,
    GeneralizedAccount,
    TooMuchAuthGas,
    InvalidAtProtocol,
    Signature(SignatureError),
    TooLowFee,
    TooLowGasPrice,
    TooLowMinerGasPrice,
    NotPayable,
    UnresolvedRecipient,
    TtlExpired,
    ResourceLimit,
    Transaction(ae_tx::Error),
    Record(ae_tx::storage::Error),
    Account(ae_state::accounts::AccountError),
    Name(ae_state::names::Error),
}
impl Error {
    pub fn is_operational(&self) -> bool {
        match self {
            Self::ResourceLimit | Self::Record(_) | Self::Account(_) | Self::Name(_) => true,
            Self::Transaction(e) => e.is_operational(),
            Self::Signature(e) => e.is_operational(),
            _ => false,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Config {
    pub nonce_baseline: Unsigned,
    pub nonce_offset: Unsigned,
    pub maximum_auth_gas: Unsigned,
    pub minimum_miner_gas_price: Unsigned,
    pub tx_ttl: u64,
    pub future_nonce_ttl: u64,
    pub invalid_tx_ttl: u64,
    pub allow_reentry: bool,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            nonce_baseline: 1.into(),
            nonce_offset: 5.into(),
            maximum_auth_gas: 50_000.into(),
            minimum_miner_gas_price: 1_000_000_000u64.into(),
            tx_ttl: 6720,
            future_nonce_ttl: 480,
            invalid_tx_ttl: 5,
            allow_reentry: false,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub entries: usize,
    pub retained_record_bytes: usize,
    pub record_bytes: usize,
    pub work: usize,
    pub selected: usize,
    pub name_hops: usize,
    pub signatures: SignatureLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            entries: 100_000,
            retained_record_bytes: 256 * 1024 * 1024,
            record_bytes: 16 * 1024 * 1024,
            work: 1_000_000,
            selected: 100_000,
            name_hops: 128,
            signatures: Default::default(),
        }
    }
}
pub struct Admission<'a, A, C> {
    pub trees: &'a Trees<A, C>,
    pub network: &'a NetworkProfile,
    pub protocol: Protocol,
    pub consensus: ConsensusType,
    pub height: u64,
    pub record: ae_tx::storage::Config<'a>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountInfo {
    pub nonce: Unsigned,
    pub generalized: bool,
}
impl From<Account> for AccountInfo {
    fn from(a: Account) -> Self {
        Self {
            nonce: a.nonce().clone(),
            generalized: a.authorization().is_some(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub fee: Reverse<Unsigned>,
    pub gas_price: Reverse<Unsigned>,
    pub origin: Option<AccountPublicKey>,
    pub nonce: Unsigned,
    pub hash: Hash256,
}
impl Key {
    pub fn of(tx: &Signed) -> Result<Self, Error> {
        let v = tx.transaction().view();
        let mut fee = v.leaf().fee().clone();
        for l in v.layers() {
            fee = fee.add(match l.wrapper() {
                Wrapper::GaMeta(m) => &m.fee,
                Wrapper::PayingFor(p) => &p.fee,
            });
        }
        Ok(Self {
            fee: Reverse(fee),
            gas_price: Reverse(v.gas_price().cloned().unwrap_or_else(|| 0.into())),
            origin: Some(v.origin().map_err(|_| Error::NoOrigin)?),
            nonce: v.nonce(),
            hash: tx.hash(),
        })
    }
}
#[derive(Debug, Clone)]
struct Entry {
    tx: Arc<Signed>,
    bytes: usize,
    failures: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Inspection {
    pub failures: u64,
    pub visited: bool,
    pub expires: i128,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushOutcome {
    Cached,
    Accepted {
        hash: Hash256,
        inserted: bool,
        event: Event,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionEvent {
    Submit,
    Ignore,
    Reject,
}
#[derive(Debug, Clone)]
pub struct Pool {
    active: BTreeMap<Key, Entry>,
    visited: BTreeMap<Key, Entry>,
    nonces: BTreeMap<Option<AccountPublicKey>, BTreeMap<Unsigned, BTreeSet<Key>>>,
    gc: BTreeMap<Hash256, (i128, Key)>,
    origins: BTreeMap<Option<AccountPublicKey>, Unsigned>,
    bytes: usize,
    gc_height: u64,
    config: Config,
    limits: Limits,
}
fn number(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
fn innermost(mut tx: TransactionRef<'_>) -> TransactionRef<'_> {
    while matches!(tx.layer().map(|l| l.wrapper()), Some(Wrapper::GaMeta(_))) {
        tx = tx.inner().expect("meta inner");
    }
    tx
}
fn nonce_check(
    account: Option<&AccountInfo>,
    nonce: &Unsigned,
    created: bool,
    cfg: &Config,
) -> Result<(), Error> {
    match account {
        None if created && nonce > &cfg.nonce_baseline => Err(Error::NonceTooHigh),
        None => Ok(()),
        Some(a) if a.generalized => {
            if nonce == &0.into() {
                Ok(())
            } else {
                Err(Error::GeneralizedAccount)
            }
        }
        Some(a) if nonce <= &a.nonce => Err(Error::NonceTooLow),
        Some(a) if created && nonce > &a.nonce.add(&cfg.nonce_offset) => Err(Error::NonceTooHigh),
        _ => Ok(()),
    }
}
fn meta_gas(mut tx: TransactionRef<'_>, cfg: &Config) -> Result<(), Error> {
    while let Some(Wrapper::GaMeta(m)) = tx.layer().map(|l| l.wrapper()) {
        if m.gas > cfg.maximum_auth_gas {
            return Err(Error::TooMuchAuthGas);
        }
        tx = tx.inner().expect("meta inner");
    }
    Ok(())
}
impl Pool {
    pub fn new(config: Config, limits: Limits, gc_height: u64) -> Self {
        Self {
            active: Default::default(),
            visited: Default::default(),
            nonces: Default::default(),
            gc: Default::default(),
            origins: Default::default(),
            bytes: 0,
            gc_height,
            config,
            limits,
        }
    }
    pub fn config(&self) -> &Config {
        &self.config
    }
    pub fn reconfigure(&mut self, config: Config) {
        // Existing entries retain their expiration heights; subsequent
        // admission and selection use the replacement service policy.
        self.config = config;
    }
    pub fn len(&self) -> usize {
        self.active.len() + self.visited.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn sizes(&self) -> (usize, usize) {
        (self.active.len(), self.visited.len())
    }
    pub fn retained_record_bytes(&self) -> usize {
        self.bytes
    }
    pub fn transaction(&self, hash: Hash256) -> Option<Arc<Signed>> {
        let (_, key) = self.gc.get(&hash)?;
        self.entry(key).map(|(_, entry)| entry.tx.clone())
    }
    pub fn gc_height(&self) -> u64 {
        self.gc_height
    }
    pub fn max_nonce(&self, account: AccountPublicKey) -> Option<&Unsigned> {
        self.nonces
            .get(&Some(account))?
            .last_key_value()
            .map(|(n, _)| n)
    }
    pub fn peek(
        &self,
        max: usize,
        account: Option<AccountPublicKey>,
        max_nonce: Option<&Unsigned>,
    ) -> Vec<Arc<Signed>> {
        if let Some(account) = account {
            return self
                .nonces
                .get(&Some(account))
                .into_iter()
                .flat_map(|n| n.iter())
                .take_while(|(n, _)| max_nonce.is_none_or(|max| *n <= max))
                .flat_map(|(_, keys)| keys)
                .filter_map(|k| self.entry(k).map(|(_, e)| e.tx.clone()))
                .take(max)
                .collect();
        }
        let mut a = self.active.iter().peekable();
        let mut b = self.visited.iter().peekable();
        let mut out = vec![];
        while out.len() < max {
            let entry = match (a.peek(), b.peek()) {
                (Some((ka, _)), Some((kb, _))) if ka <= kb => a.next(),
                (Some(_), Some(_)) => b.next(),
                (Some(_), None) => a.next(),
                (None, Some(_)) => b.next(),
                _ => break,
            };
            out.push(entry.expect("nonempty iterator").1.tx.clone());
        }
        out
    }
    pub fn inspect(&self, hash: Hash256, location: Location) -> Result<Inspection, Error> {
        if matches!(location, Location::Block(_)) {
            return Err(Error::AlreadyAccepted);
        }
        let (expires, key) = self.gc.get(&hash).ok_or(Error::NotFound)?;
        let (visited, e) = self.entry(key).ok_or(Error::NotFound)?;
        Ok(Inspection {
            failures: e.failures,
            visited,
            expires: *expires,
        })
    }
    fn entry(&self, key: &Key) -> Option<(bool, &Entry)> {
        self.active
            .get(key)
            .map(|e| (false, e))
            .or_else(|| self.visited.get(key).map(|e| (true, e)))
    }
    fn encode_size(
        &self,
        tx: &Signed,
        mut config: ae_tx::storage::Config<'_>,
    ) -> Result<usize, Error> {
        if tx.transaction().layers().len() > self.limits.signatures.max_layers {
            return Err(Error::ResourceLimit);
        }
        config.max_record_bytes = config.max_record_bytes.min(self.limits.record_bytes);
        ae_tx::storage::encode(tx, config)
            .map(|b| b.len())
            .map_err(Error::Record)
    }
    fn expiry(&self, tx: &Signed, residency: u64) -> Result<u64, Error> {
        let expiry = self
            .gc_height
            .checked_add(residency)
            .ok_or(Error::ResourceLimit)?;
        Ok(tx
            .transaction()
            .view()
            .ttl()
            .map_or(expiry, |n| n.to_u64().map_or(expiry, |n| expiry.min(n))))
    }
    fn residency(
        &self,
        tx: &Signed,
        lookup: &mut impl FnMut(AccountPublicKey) -> Result<Option<AccountInfo>, Error>,
    ) -> Result<u64, Error> {
        let tx = innermost(tx.transaction().view());
        if let Ok(key) = tx.origin()
            && let Some(a) = lookup(key)?
            && !a.generalized
            && tx.nonce() > a.nonce.add(&self.config.nonce_offset)
        {
            return Ok(self.config.future_nonce_ttl.min(self.config.tx_ttl));
        }
        Ok(self.config.tx_ttl)
    }
    fn check_capacity(&self, key: &Key, bytes: usize) -> Result<(), Error> {
        let replacing = self.active.get(key);
        if self.len() + usize::from(replacing.is_none()) > self.limits.entries
            || self
                .bytes
                .checked_sub(replacing.map_or(0, |e| e.bytes))
                .and_then(|n| n.checked_add(bytes))
                .is_none_or(|n| n > self.limits.retained_record_bytes)
        {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
    fn insert(&mut self, key: Key, tx: Arc<Signed>, bytes: usize, expiry: u64) {
        if let Some(old) = self.active.insert(
            key.clone(),
            Entry {
                tx,
                bytes,
                failures: 0,
            },
        ) {
            self.bytes -= old.bytes;
        }
        self.bytes += bytes;
        self.nonces
            .entry(key.origin)
            .or_default()
            .entry(key.nonce.clone())
            .or_default()
            .insert(key.clone());
        self.gc
            .entry(key.hash)
            .and_modify(|(h, _)| *h = (*h).min(i128::from(expiry)))
            .or_insert((i128::from(expiry), key));
    }
    fn remove(&mut self, key: &Key) {
        for map in [&mut self.active, &mut self.visited] {
            if let Some(e) = map.remove(key) {
                self.bytes -= e.bytes;
            }
        }
        if let Some(n) = self.nonces.get_mut(&key.origin) {
            if let Some(keys) = n.get_mut(&key.nonce) {
                keys.remove(key);
                if keys.is_empty() {
                    n.remove(&key.nonce);
                }
            }
            if n.is_empty() {
                self.nonces.remove(&key.origin);
            }
        }
        self.gc.remove(&key.hash);
    }
    pub fn delete(&mut self, hash: Hash256, location: Location) -> Result<(), Error> {
        if matches!(location, Location::Block(_)) {
            return Err(Error::AlreadyAccepted);
        }
        let key = self.gc.get(&hash).ok_or(Error::NotFound)?.1.clone();
        self.remove(&key);
        Ok(())
    }
    pub fn revisit(&mut self) {
        for (key, e) in std::mem::take(&mut self.visited) {
            if let std::collections::btree_map::Entry::Vacant(slot) = self.active.entry(key) {
                slot.insert(e);
            } else {
                self.bytes -= e.bytes;
            }
        }
    }
}
