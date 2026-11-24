use crate::{
    Error, integer,
    session::{Membership, Registration, WorkerRegistration},
};
use num_bigint::BigInt;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub accounts: usize,
    pub retained_bytes: usize,
    pub field_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            accounts: 4096,
            retained_bytes: 16 << 20,
            field_bytes: 4096,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), Error> {
        if self.accounts == 0
            || self.accounts > 1_000_000
            || self.retained_bytes == 0
            || self.retained_bytes > 1 << 30
            || self.field_bytes == 0
            || self.field_bytes > 1 << 20
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
    fn field(self, value: &[u8]) -> Result<(), Error> {
        if value.len() > self.field_bytes {
            Err(Error::Limit("registry field"))
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub connection: u64,
    pub workers: BTreeMap<Vec<u8>, BigInt>,
}
impl Entry {
    pub fn retained_bytes(&self) -> usize {
        128 + self
            .workers
            .iter()
            .map(|(w, t)| worker_bytes(w, t))
            .sum::<usize>()
    }
}
fn worker_bytes(worker: &[u8], time: &BigInt) -> usize {
    96 + worker.len() + time.bits().div_ceil(8) as usize
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registry {
    accounts: BTreeMap<Vec<u8>, Entry>,
    connections: BTreeMap<u64, Vec<u8>>,
    bytes: usize,
    limits: Limits,
}
impl Registry {
    pub fn new(limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        Ok(Self {
            accounts: BTreeMap::new(),
            connections: BTreeMap::new(),
            bytes: 0,
            limits,
        })
    }
    pub fn len(&self) -> usize {
        self.accounts.len()
    }
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }
    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }
    pub fn accounts(&self) -> &BTreeMap<Vec<u8>, Entry> {
        &self.accounts
    }
    pub fn connections(&self) -> impl Iterator<Item = u64> + '_ {
        self.connections.keys().copied()
    }
    pub fn member_account(&self, account: &[u8]) -> bool {
        self.accounts.contains_key(account)
    }
    pub fn member_connection(&self, connection: u64) -> bool {
        self.connections.contains_key(&connection)
    }
    pub fn membership(&self, account: &[u8], connection: u64) -> Membership {
        match self.accounts.get(account) {
            Some(e) if e.connection == connection => Membership::Both,
            Some(_) => Membership::AccountOnly,
            None => Membership::Neither,
        }
    }
    pub fn find_account(&self, account: &[u8]) -> Option<&Entry> {
        self.accounts.get(account)
    }
    pub fn find_connection(&self, connection: u64) -> Option<(&[u8], &Entry)> {
        let account = self.connections.get(&connection)?;
        Some((account, self.accounts.get(account)?))
    }
    fn capacity(&self, bytes: usize) -> Result<(), Error> {
        if bytes > self.limits.retained_bytes.saturating_sub(self.bytes) {
            Err(Error::Limit("registry bytes"))
        } else {
            Ok(())
        }
    }
    pub fn add(
        &mut self,
        account: &[u8],
        worker: &[u8],
        connection: u64,
        now: BigInt,
    ) -> Result<Registration, Error> {
        self.limits.field(account)?;
        self.limits.field(worker)?;
        if self.member_account(account) || self.member_connection(connection) {
            return Ok(Registration::AlreadyPresent);
        }
        integer(&now)?;
        if self.len() >= self.limits.accounts {
            return Err(Error::Limit("registry accounts"));
        }
        let charge = 128 + 2 * account.len() + worker_bytes(worker, &now);
        self.capacity(charge)?;
        self.connections.insert(connection, account.to_vec());
        self.accounts.insert(
            account.to_vec(),
            Entry {
                connection,
                workers: [(worker.to_vec(), now)].into(),
            },
        );
        self.bytes += charge;
        Ok(Registration::Added)
    }
    pub fn worker_status(
        &self,
        account: &[u8],
        worker: &[u8],
        maximum: usize,
    ) -> WorkerRegistration {
        match self.accounts.get(account) {
            None => WorkerRegistration::AccountNotFound,
            Some(e) if e.workers.contains_key(worker) => WorkerRegistration::AlreadyPresent,
            Some(e) if e.workers.len() >= maximum => WorkerRegistration::Exhausted,
            Some(_) => WorkerRegistration::Added,
        }
    }
    pub fn add_worker(
        &mut self,
        account: &[u8],
        worker: &[u8],
        maximum: usize,
        now: BigInt,
    ) -> Result<WorkerRegistration, Error> {
        self.limits.field(account)?;
        self.limits.field(worker)?;
        let result = self.worker_status(account, worker, maximum);
        if result != WorkerRegistration::Added {
            return Ok(result);
        }
        integer(&now)?;
        let charge = worker_bytes(worker, &now);
        self.capacity(charge)?;
        self.accounts
            .get_mut(account)
            .expect("checked account")
            .workers
            .insert(worker.to_vec(), now);
        self.bytes += charge;
        Ok(WorkerRegistration::Added)
    }
    pub fn delete_account(&mut self, account: &[u8]) -> bool {
        if let Some(e) = self.accounts.remove(account) {
            self.connections.remove(&e.connection);
            self.bytes -= e.retained_bytes() + 2 * account.len();
            true
        } else {
            false
        }
    }
    pub fn delete_connection(&mut self, connection: u64) -> bool {
        match self.connections.get(&connection).cloned() {
            Some(account) => self.delete_account(&account),
            None => false,
        }
    }
}
