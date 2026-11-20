use super::*;
use std::ops::Bound::{Included, Unbounded};
const MIN_GAS: u64 = 15_000;
#[derive(Debug, Clone, Copy, Default)]
pub struct SelectOptions {
    pub deadline: Option<i64>,
    pub ceiling: Option<i64>,
    pub packed: bool,
}
pub struct CandidateRequest<'a> {
    pub block: Hash256,
    pub height: u64,
    pub protocol: Protocol,
    pub max_gas: u64,
    pub ignore: &'a BTreeSet<Hash256>,
    pub options: SelectOptions,
}
#[derive(Debug, Clone)]
pub struct Candidate {
    pub block: Hash256,
    pub transactions: Vec<Arc<Signed>>,
    pub gas_left: u64,
    pub expired: bool,
    pub work: usize,
}
#[derive(Clone)]
struct Chosen {
    key: Key,
    visited: bool,
    tx: Arc<Signed>,
}
struct Selection<'a, F, T> {
    pool: &'a Pool,
    lookup: F,
    now: T,
    opts: SelectOptions,
    height: u64,
    protocol: Protocol,
    ignore: &'a BTreeSet<Hash256>,
    gas: u64,
    expired: bool,
    work: usize,
    accounts: BTreeMap<AccountPublicKey, Option<AccountInfo>>,
    expected: BTreeMap<Option<AccountPublicKey>, Option<Unsigned>>,
    done: BTreeSet<Option<AccountPublicKey>>,
    examined: BTreeSet<Hash256>,
    resume: BTreeMap<Option<AccountPublicKey>, (Unsigned, Key)>,
    regular: BTreeMap<(Option<AccountPublicKey>, Unsigned), Chosen>,
    meta: Vec<Chosen>,
    bad: Vec<Chosen>,
    expiry: BTreeMap<Hash256, u64>,
}
#[derive(PartialEq, Eq)]
enum Judgment {
    Selected,
    NoGas,
    Invalid,
}
enum Serve {
    Taken,
    Pending,
    Stop,
}
impl<F: FnMut(AccountPublicKey) -> Result<Option<AccountInfo>, Error>, T: FnMut() -> i64>
    Selection<'_, F, T>
{
    fn charge(&mut self) -> Result<(), Error> {
        self.work = self.work.checked_add(1).ok_or(Error::ResourceLimit)?;
        if self.work > self.pool.limits.work {
            Err(Error::ResourceLimit)
        } else {
            Ok(())
        }
    }
    fn stop(&mut self) -> bool {
        if self.expired {
            return true;
        }
        if self.opts.deadline.is_none() && self.opts.ceiling.is_none() {
            return false;
        }
        let now = (self.now)();
        self.opts.ceiling.is_some_and(|t| now >= t)
            || ((self.opts.packed || !self.regular.is_empty() || !self.meta.is_empty())
                && self.opts.deadline.is_some_and(|t| now >= t))
    }
    fn account(&mut self, key: AccountPublicKey) -> Result<Option<AccountInfo>, Error> {
        if let Some(a) = self.accounts.get(&key) {
            return Ok(a.clone());
        }
        let a = (self.lookup)(key)?;
        self.accounts.insert(key, a.clone());
        Ok(a)
    }
    fn packed(
        &mut self,
        origin: Option<AccountPublicKey>,
        nonce: &Unsigned,
    ) -> Result<bool, Error> {
        if self.ignore.is_empty() {
            return Ok(false);
        }
        let pool = self.pool;
        let Some(keys) = pool.nonces.get(&origin).and_then(|n| n.get(nonce)) else {
            return Ok(false);
        };
        let mut iter = keys.iter().peekable();
        let mut count = 0;
        while let Some(key) = iter.next() {
            self.charge()?;
            if self.ignore.contains(&key.hash) {
                return Ok(true);
            }
            count += 1;
            if count % 100 == 0 && iter.peek().is_some() && self.stop() {
                self.expired = true;
                return Ok(false);
            }
        }
        Ok(false)
    }
    fn expected(&mut self, origin: Option<AccountPublicKey>) -> Result<Option<Unsigned>, Error> {
        if let Some(n) = self.expected.get(&origin) {
            return Ok(n.clone());
        }
        let account = origin.map(|k| self.account(k)).transpose()?.flatten();
        let n = match account {
            Some(a) if !a.generalized => {
                let mut n = a.nonce.add(&1.into());
                while self.packed(origin, &n)? {
                    if self.stop() {
                        self.expired = true;
                        return Ok(None);
                    }
                    n = n.add(&1.into());
                }
                if self.expired {
                    return Ok(None);
                }
                Some(n)
            }
            _ => None,
        };
        self.expected.insert(origin, n.clone());
        Ok(n)
    }
    fn check(&mut self, key: &Key, visited: bool) -> Result<Judgment, Error> {
        self.charge()?;
        let tx = if visited {
            &self.pool.visited
        } else {
            &self.pool.active
        }
        .get(key)
        .expect("stable pool view")
        .tx
        .clone();
        let v = tx.transaction().view();
        let pricing = v.pricing(&self.height.into(), self.protocol);
        let valid = if v.ttl().is_some_and(|ttl| ttl <= &self.height.into())
            || pricing.gas_limit <= BigInt::from(0)
        {
            false
        } else {
            let inner = innermost(v);
            match inner.origin() {
                Err(_) => false,
                Ok(k) => {
                    nonce_check(
                        self.account(k)?.as_ref(),
                        &inner.nonce(),
                        false,
                        &self.pool.config,
                    )
                    .is_ok()
                        && meta_gas(v, &self.pool.config).is_ok()
                        && pricing.minimum_gas_price
                            >= number(&self.pool.config.minimum_miner_gas_price)
                }
            }
        };
        if valid && pricing.gas_limit > BigInt::from(self.gas) {
            return Ok(Judgment::NoGas);
        }
        if self.regular.len() + self.meta.len() + self.bad.len() == self.pool.limits.selected {
            return Err(Error::ResourceLimit);
        }
        let chosen = Chosen {
            key: key.clone(),
            visited,
            tx,
        };
        if valid {
            let (_, bytes) = pricing.gas_limit.to_bytes_be();
            let cost = Unsigned::from_field_bytes(&bytes)
                .map_err(|_| Error::ResourceLimit)?
                .to_u64()
                .map_err(|_| Error::ResourceLimit)?;
            self.gas -= cost;
            if key.nonce == 0.into() {
                self.meta.push(chosen);
            } else {
                self.regular.insert((key.origin, key.nonce.clone()), chosen);
            }
            Ok(Judgment::Selected)
        } else {
            let expires = self
                .height
                .checked_add(self.pool.config.invalid_tx_ttl)
                .ok_or(Error::ResourceLimit)?;
            self.expiry
                .entry(key.hash)
                .and_modify(|n| *n = (*n).min(expires))
                .or_insert(expires);
            self.bad.push(chosen);
            Ok(Judgment::Invalid)
        }
    }
    fn entry(&mut self, key: &Key, visited: bool) -> Result<(), Error> {
        if key.nonce != 0.into() && self.regular.contains_key(&(key.origin, key.nonce.clone())) {
            return Ok(());
        }
        self.check(key, visited)?;
        Ok(())
    }
    fn serve_one(
        &mut self,
        origin: Option<AccountPublicKey>,
        nonce: &Unsigned,
        price: &(Reverse<Unsigned>, Reverse<Unsigned>),
    ) -> Result<Serve, Error> {
        let pool = self.pool;
        let Some(keys) = pool.nonces.get(&origin).and_then(|n| n.get(nonce)) else {
            return Ok(Serve::Stop);
        };
        let start = self
            .resume
            .get(&origin)
            .filter(|(n, _)| n == nonce)
            .map(|(_, k)| k.clone());
        for key in keys.range((start.as_ref().map_or(Unbounded, Included), Unbounded)) {
            self.charge()?;
            if self.ignore.contains(&key.hash) {
                return Ok(Serve::Taken);
            }
            if !self.examined.contains(&key.hash) {
                if (&key.fee, &key.gas_price) > (&price.0, &price.1) {
                    self.resume.insert(origin, (nonce.clone(), key.clone()));
                    return Ok(Serve::Pending);
                }
                let Some((visited, _)) = pool.entry(key) else {
                    continue;
                };
                if self.check(key, visited)? == Judgment::Selected {
                    return Ok(Serve::Taken);
                }
                self.examined.insert(key.hash);
            }
            if self.stop() {
                self.expired = true;
                return Ok(Serve::Stop);
            }
        }
        Ok(Serve::Stop)
    }
    fn serve(
        &mut self,
        origin: Option<AccountPublicKey>,
        mut nonce: Unsigned,
        price: (Reverse<Unsigned>, Reverse<Unsigned>),
    ) -> Result<(), Error> {
        loop {
            match self.serve_one(origin, &nonce, &price)? {
                Serve::Taken => {
                    nonce = nonce.add(&1.into());
                    self.expected.insert(origin, Some(nonce.clone()));
                }
                Serve::Pending => return Ok(()),
                Serve::Stop => {
                    self.done.insert(origin);
                    return Ok(());
                }
            }
        }
    }
    fn consider(&mut self, key: &Key, visited: bool) -> Result<(), Error> {
        self.charge()?;
        if self.ignore.contains(&key.hash) {
            return Ok(());
        }
        if key.nonce == 0.into() {
            return self.entry(key, visited);
        }
        if self.done.contains(&key.origin) {
            return Ok(());
        }
        let expected = self.expected(key.origin)?;
        if self.expired {
            return Ok(());
        }
        match expected {
            None => self.entry(key, visited),
            Some(n) if key.nonce < n => self.entry(key, visited),
            Some(n) if key.nonce == n => {
                self.serve(key.origin, n, (key.fee.clone(), key.gas_price.clone()))
            }
            _ => Ok(()),
        }
    }
    fn walk(&mut self, visited: bool) -> Result<(), Error> {
        if self.expired || self.gas <= MIN_GAS {
            return Ok(());
        }
        let pool = self.pool;
        let map = if visited { &pool.visited } else { &pool.active };
        let mut iter = map.keys().peekable();
        while iter.peek().is_some() && self.gas > MIN_GAS {
            for key in iter.by_ref().take(20) {
                if self.gas <= MIN_GAS || self.expired {
                    break;
                }
                self.consider(key, visited)?;
            }
            if self.stop() {
                self.expired = true;
                break;
            }
        }
        Ok(())
    }
}
impl Pool {
    pub fn candidate(
        &mut self,
        request: CandidateRequest<'_>,
        lookup: impl FnMut(AccountPublicKey) -> Result<Option<AccountInfo>, Error>,
        now: impl FnMut() -> i64,
    ) -> Result<Candidate, Error> {
        let CandidateRequest {
            block,
            height,
            protocol,
            max_gas,
            ignore,
            options,
        } = request;
        if ignore.len() > self.limits.work {
            return Err(Error::ResourceLimit);
        }
        let mut selection = Selection {
            pool: self,
            lookup,
            now,
            opts: options,
            height,
            protocol,
            ignore,
            gas: max_gas,
            expired: false,
            work: 0,
            accounts: Default::default(),
            expected: Default::default(),
            done: Default::default(),
            examined: Default::default(),
            resume: Default::default(),
            regular: Default::default(),
            meta: vec![],
            bad: vec![],
            expiry: Default::default(),
        };
        selection.walk(false)?;
        selection.walk(true)?;
        let gas_left = selection.gas;
        let expired = selection.expired;
        let work = selection.work;
        let expiry = std::mem::take(&mut selection.expiry);
        let chosen: Vec<_> = std::mem::take(&mut selection.regular)
            .into_values()
            .chain(std::mem::take(&mut selection.meta))
            .chain(std::mem::take(&mut selection.bad))
            .collect();
        drop(selection);
        for (hash, expires) in expiry {
            if let Some((ttl, _)) = self.gc.get_mut(&hash) {
                *ttl = (*ttl).min(expires);
            }
        }
        for c in &chosen {
            if !c.visited
                && let Some(entry) = self.active.remove(&c.key)
                && let Some(old) = self.visited.insert(c.key.clone(), entry)
            {
                self.bytes -= old.bytes;
            }
        }
        Ok(Candidate {
            block,
            transactions: chosen.into_iter().map(|c| c.tx).collect(),
            gas_left,
            expired,
            work,
        })
    }
}
