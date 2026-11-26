// Independent observation cache; behavior references are aehttp_app at
// aeternity fa2a6d7 and the Apache-2.0 kache e5e10c6. No kache code is copied.
use ae_chain::gas_prices::{self as gas, Stats};
use ae_types::Hash256;
use num_bigint::BigInt;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub enum Error {
    Configuration,
    Busy,
    Closed,
    Timeout,
    FillAborted,
    Check(super::super::Error),
    Fill(Arc<gas::Error>),
}
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub entries: usize,
    pub bytes: usize,
    pub fills: usize,
    pub requests: usize,
    pub timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            entries: 1200,
            bytes: 32 << 20,
            fills: 8,
            requests: 64,
            timeout: Duration::from_secs(5),
        }
    }
}
#[derive(Debug)]
pub enum Data {
    Observed(Stats),
    MissingState,
}
#[derive(Debug)]
struct Budget {
    used: AtomicUsize,
    maximum: usize,
}
#[derive(Debug)]
struct Charge {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
#[derive(Debug)]
pub struct Value {
    data: Data,
    _charge: Charge,
}
impl Value {
    pub fn data(&self) -> &Data {
        &self.data
    }
}
type ResultValue = Result<Arc<Value>, Error>;
struct Flight {
    result: Mutex<Option<ResultValue>>,
    wake: Condvar,
    deadline: Instant,
    waiters: AtomicUsize,
}
impl Flight {
    fn finish(&self, result: ResultValue) {
        let mut output = self.result.lock().unwrap_or_else(|e| e.into_inner());
        if output.is_none() {
            *output = Some(result);
            self.wake.notify_all();
        }
    }
}
struct Entry {
    value: Arc<Value>,
    age: u64,
}
#[derive(Default)]
struct State {
    ready: BTreeMap<Hash256, Entry>,
    ages: BTreeMap<u64, Hash256>,
    pending: BTreeMap<Hash256, Arc<Flight>>,
    clock: u64,
    closed: bool,
}
impl State {
    fn age(&mut self) -> u64 {
        if self.clock == u64::MAX {
            // Preserve encounter order without allowing a wrapping generation
            // number to make a recently used entry look oldest.
            let keys: Vec<_> = self.ages.values().copied().collect();
            self.ages.clear();
            for (age, key) in keys.into_iter().enumerate() {
                self.ready.get_mut(&key).expect("indexed entry").age = age as u64;
                self.ages.insert(age as u64, key);
            }
            self.clock = self.ready.len() as u64;
        }
        let age = self.clock;
        self.clock += 1;
        age
    }
    fn hit(&mut self, key: Hash256) -> Option<Arc<Value>> {
        let entry = self.ready.remove(&key)?;
        self.ages.remove(&entry.age);
        let value = entry.value.clone();
        let age = self.age();
        self.ready.insert(
            key,
            Entry {
                value: entry.value,
                age,
            },
        );
        self.ages.insert(age, key);
        Some(value)
    }
}

// Create one cache per HTTP owner. Clones share it across listeners and reader
// replacements; another database/runtime needs a fresh cache. There is no TTL.
pub struct Cache {
    config: Config,
    state: Mutex<State>,
    budget: Arc<Budget>,
    requests: AtomicUsize,
    #[cfg(test)]
    before_evict: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Info {
    pub entries: usize,
    pub pending: usize,
    pub waiters: usize,
    pub requests: usize,
    pub retained_bytes: usize,
    pub closed: bool,
}
struct Request<'a>(&'a AtomicUsize);
impl Drop for Request<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

// Dropping a panicking/abandoned fill releases its key and wakes all waiters.
// The fill itself runs on its caller's owned blocking worker, never a detached
// thread or another job in the same pool.
struct Owner<'a> {
    cache: &'a Cache,
    key: Hash256,
    flight: Arc<Flight>,
}
impl Drop for Owner<'_> {
    fn drop(&mut self) {
        let mut state = self.cache.state.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .pending
            .get(&self.key)
            .is_some_and(|f| Arc::ptr_eq(f, &self.flight))
        {
            state.pending.remove(&self.key);
            self.flight.finish(Err(Error::FillAborted));
        }
    }
}
impl Cache {
    pub fn new(config: Config) -> Result<Self, Error> {
        if config.entries == 0
            || config.bytes < 256
            || config.fills == 0
            || config.requests < config.fills
            || config.timeout.is_zero()
            || config.entries > 1_000_000
            || config.requests > 65_536
            || Instant::now().checked_add(config.timeout).is_none()
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            config,
            state: Mutex::new(State::default()),
            budget: Arc::new(Budget {
                used: AtomicUsize::new(0),
                maximum: config.bytes,
            }),
            requests: AtomicUsize::new(0),
            #[cfg(test)]
            before_evict: Mutex::new(None),
        })
    }
    pub fn info(&self) -> Info {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Info {
            entries: state.ready.len(),
            pending: state.pending.len(),
            waiters: state
                .pending
                .values()
                .map(|f| f.waiters.load(Ordering::Acquire))
                .sum(),
            requests: self.requests.load(Ordering::Acquire),
            retained_bytes: self.budget.used.load(Ordering::Acquire),
            closed: state.closed,
        }
    }
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        for (_, flight) in std::mem::take(&mut state.pending) {
            flight.finish(Err(Error::Closed));
        }
        state.ready.clear();
        state.ages.clear();
        // Values retained by a response/reader keep their memory charge until
        // the last Arc drops, even after eviction or owner shutdown.
    }
    fn check_context(
        &self,
        check: &mut impl FnMut() -> Result<(), super::super::Error>,
    ) -> Result<(), Error> {
        check().map_err(Error::Check)?;
        if self.state.lock().unwrap_or_else(|e| e.into_inner()).closed {
            return Err(Error::Closed);
        }
        Ok(())
    }
    fn check(
        &self,
        deadline: Instant,
        check: &mut impl FnMut() -> Result<(), super::super::Error>,
    ) -> Result<(), Error> {
        self.check_context(check)?;
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        Ok(())
    }
    pub fn get_or_fill(
        &self,
        key: Hash256,
        mut check: impl FnMut() -> Result<(), super::super::Error>,
        fill: impl FnOnce(Instant) -> Result<Stats, gas::Error>,
    ) -> ResultValue {
        check().map_err(Error::Check)?;
        self.requests
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.config.requests).then_some(n + 1)
            })
            .map_err(|_| Error::Busy)?;
        let _request = Request(&self.requests);
        let (flight, owner) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.closed {
                return Err(Error::Closed);
            }
            if let Some(value) = state.hit(key) {
                return Ok(value);
            }
            if let Some(flight) = state.pending.get(&key) {
                (flight.clone(), false)
            } else {
                if state.pending.len() >= self.config.fills {
                    return Err(Error::Busy);
                }
                let flight = Arc::new(Flight {
                    result: Mutex::new(None),
                    wake: Condvar::new(),
                    deadline: Instant::now() + self.config.timeout,
                    waiters: AtomicUsize::new(0),
                });
                state.pending.insert(key, flight.clone());
                (flight, true)
            }
        };
        if !owner {
            flight.waiters.fetch_add(1, Ordering::AcqRel);
            let _waiter = Request(&flight.waiters);
            loop {
                self.check_context(&mut check)?;
                let output = flight.result.lock().unwrap_or_else(|e| e.into_inner());
                // Native receives an already queued reply even if the caller
                // resumes after its timeout. Owner checks reject late fills;
                // caller cancellation/recovery and cache close still win.
                if let Some(result) = output.as_ref() {
                    return result.clone();
                }
                let wait = flight
                    .deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10));
                if wait.is_zero() {
                    return Err(Error::Timeout);
                }
                drop(
                    flight
                        .wake
                        .wait_timeout(output, wait)
                        .unwrap_or_else(|e| e.into_inner()),
                );
            }
        }
        let guard = Owner {
            cache: self,
            key,
            flight: flight.clone(),
        };
        // Keep the key owned until this bounded fill returns, even if waiters
        // time out. Late results are rejected and cannot overwrite a retry.
        let result = (|| {
            self.check(flight.deadline, &mut check)?;
            let result = fill(flight.deadline);
            self.check(flight.deadline, &mut check)?;
            let data = match result {
                Ok(stats) => {
                    stats
                        .check(gas::Limits::default())
                        .map_err(|e| Error::Fill(Arc::new(e)))?;
                    Data::Observed(stats)
                }
                Err(gas::Error::MissingState(hash)) if hash == key => Data::MissingState,
                Err(error) => return Err(Error::Fill(Arc::new(error))),
            };
            self.insert(key, data, &flight)
        })();
        flight.finish(result.clone());
        drop(guard);
        result
    }
    fn insert(&self, key: Hash256, data: Data, flight: &Arc<Flight>) -> ResultValue {
        let bytes = weight(&data)?;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(Error::Closed);
        }
        if Instant::now() >= flight.deadline {
            return Err(Error::Timeout);
        }
        let evict = if state.ready.len() >= self.config.entries {
            state.ages.first_key_value().map(|(&age, &key)| (age, key))
        } else {
            None
        };
        let reclaim = evict
            .and_then(|(_, key)| state.ready.get(&key))
            .filter(|e| Arc::strong_count(&e.value) == 1)
            .map_or(0, |e| e.value._charge.bytes);
        if self
            .budget
            .used
            .load(Ordering::Acquire)
            .checked_sub(reclaim)
            .and_then(|n| n.checked_add(bytes))
            .is_none_or(|n| n > self.budget.maximum)
        {
            return Err(Error::Busy);
        }
        if let Some((age, key)) = evict {
            #[cfg(test)]
            if let Some(hook) = self.before_evict.lock().unwrap().take() {
                hook();
            }
            state.ages.remove(&age);
            state.ready.remove(&key);
        }
        // An external Weak may have been upgraded since the optimistic
        // reclaim check. Reserve against the actual remaining allocation,
        // never assume eviction released its last strong reference.
        self.budget
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|n| *n <= self.budget.maximum)
            })
            .map_err(|_| Error::Busy)?;
        let charge = Charge {
            budget: self.budget.clone(),
            bytes,
        };
        let data = match data {
            Data::Observed(s) => Data::Observed(Stats {
                minimum: s.minimum.map(compact),
                used: compact(s.used),
                total: compact(s.total),
            }),
            Data::MissingState => Data::MissingState,
        };
        let value = Arc::new(Value {
            data,
            _charge: charge,
        });
        let age = state.age();
        state.ready.insert(
            key,
            Entry {
                value: value.clone(),
                age,
            },
        );
        state.ages.insert(age, key);
        state.pending.remove(&key);
        Ok(value)
    }
}
fn compact(n: BigInt) -> BigInt {
    let (sign, bytes) = n.to_bytes_be();
    BigInt::from_bytes_be(sign, &bytes)
}
fn weight(data: &Data) -> Result<usize, Error> {
    let mut bytes = 256usize;
    if let Data::Observed(stats) = data {
        for n in stats.minimum.iter().chain([&stats.used, &stats.total]) {
            let n = usize::try_from(n.bits().div_ceil(8)).map_err(|_| Error::Busy)?;
            // Compact integers plus limbs, transient conversion, Arc and map
            // overhead. Charges stay with the shared allocation when evicted.
            bytes = bytes
                .checked_add(n.checked_mul(8).ok_or(Error::Busy)?)
                .ok_or(Error::Busy)?;
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests;
