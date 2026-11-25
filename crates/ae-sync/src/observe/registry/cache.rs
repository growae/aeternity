use super::*;
use reporter::Time;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheInfo {
    pub sequence: u64,
    pub timer: Option<u64>,
    pub written_microseconds: i128,
    pub ttl_ms: u64,
}
pub(super) struct Cached {
    value: Value,
    info: CacheInfo,
    due: Option<Instant>,
    bytes: usize,
}
impl State {
    pub(super) fn clear_cache_name(&mut self, name: &Name) {
        let mut removed = 0;
        self.cache.retain(|(n, _), value| {
            if n == name {
                removed += value.bytes;
                false
            } else {
                true
            }
        });
        self.bytes -= removed;
        self.cache_bytes -= removed;
    }
}
impl Store {
    pub(super) fn claim(&self) -> Result<(), Error> {
        let mut s = self.lock();
        s.open()?;
        if s.owned {
            return Err(Error::AlreadyRunning);
        }
        s.owned = true;
        Ok(())
    }
    pub(super) fn close(&self) {
        let mut s = self.lock();
        s.closed = true;
        s.enabled = false;
        s.entries.clear();
        s.aliases.clear();
        s.cache.clear();
        s.bytes = 0;
        s.cache_bytes = 0;
        s.cache_signal = None;
        for signal in &s.signals {
            signal.notify();
        }
    }
    pub(super) fn set_cache(&self, name: &Name, ttl_ms: u64) -> Result<(), Error> {
        self.scalar_name(name)?;
        ttl(ttl_ms)?;
        let mut s = self.lock();
        s.open()?;
        let entry = s.entries.get_mut(name).ok_or(Error::NotFound)?;
        if !entry.metric.entry.enabled {
            return Err(Error::Disabled);
        }
        entry.scalar.as_ref().ok_or(Error::SampledEntry)?;
        // cache=0 bypasses the cache; it does not remove retained datapoints.
        entry.cache_ms = ttl_ms;
        Ok(())
    }
    fn cache_bytes(&self, name: &Name, point: &Part, value: &Value) -> Result<usize, Error> {
        Ok(192usize
            .saturating_add(self.scalar_name(name)?)
            .saturating_add(self.limits.report.part(point).map_err(|_| Error::Limit)?)
            .saturating_add(value.bytes(self.limits.report)?))
    }
    pub(super) fn cache_write(
        &self,
        name: &Name,
        point: &Part,
        value: &Value,
        ttl_ms: u64,
    ) -> Result<u64, Error> {
        ttl(ttl_ms)?;
        let bytes = self.cache_bytes(name, point, value)?;
        let time = self.clock.now();
        let mut s = self.lock();
        s.open()?;
        let key = (name.clone(), point.clone());
        let old = s.cache.get(&key).map_or(0, |c| c.bytes);
        let total = s.bytes - old + bytes;
        if total > self.limits.retained_bytes
            || (old == 0 && s.cache.len() >= self.limits.subscriptions)
        {
            return Err(Error::Limit);
        }
        let sequence = s.cache_sequence.checked_add(1).ok_or(Error::Sequence)?;
        s.cache.insert(
            key,
            Cached {
                value: value.clone(),
                info: CacheInfo {
                    sequence,
                    timer: None,
                    written_microseconds: time.unix_microseconds,
                    ttl_ms,
                },
                due: None,
                bytes,
            },
        );
        s.bytes = total;
        s.cache_bytes = s.cache_bytes - old + bytes;
        s.cache_sequence = sequence;
        if let Some(signal) = &s.cache_signal {
            signal.notify();
        }
        Ok(sequence)
    }
    pub(super) fn cache_read(&self, name: &Name, point: &Part) -> Result<Option<Value>, Error> {
        self.scalar_name(name)?;
        self.limits.report.part(point).map_err(|_| Error::Limit)?;
        let s = self.lock();
        s.open()?;
        // Expiry is exclusively a cache-owner action. Reading a delayed timer's
        // value must not silently turn into eager deadline-based expiry.
        Ok(s.cache
            .get(&(name.clone(), point.clone()))
            .map(|c| c.value.clone()))
    }
    pub(super) fn cache_info(&self, name: &Name, point: &Part) -> Result<Option<CacheInfo>, Error> {
        self.scalar_name(name)?;
        self.limits.report.part(point).map_err(|_| Error::Limit)?;
        let s = self.lock();
        s.open()?;
        Ok(s.cache
            .get(&(name.clone(), point.clone()))
            .map(|c| c.info.clone()))
    }
    pub(super) fn cache_delete(&self, name: &Name, point: Option<&Part>) -> Result<(), Error> {
        self.scalar_name(name)?;
        let mut s = self.lock();
        s.open()?;
        if let Some(point) = point {
            self.limits.report.part(point).map_err(|_| Error::Limit)?;
            if let Some(old) = s.cache.remove(&(name.clone(), point.clone())) {
                s.bytes -= old.bytes;
                s.cache_bytes -= old.bytes;
            }
        } else {
            s.clear_cache_name(name);
        }
        Ok(())
    }
    pub(super) fn scalar_read(
        &self,
        s: &mut State,
        name: &Name,
        points: &Points,
        time: Time,
    ) -> Result<Reading, Error> {
        let entry = s.entries.get(name).ok_or(Error::NotFound)?;
        if !entry.metric.entry.enabled {
            return Ok(Reading::Disabled);
        }
        let scalar = entry.scalar.as_ref().ok_or(Error::SampledEntry)?;
        if entry.cache_ms == 0 {
            return Ok(Reading::Values(scalar.values(
                points,
                timestamp(time),
                self.limits,
            )?));
        }
        let ttl_ms = entry.cache_ms;
        let defaults;
        let requested = if *points == Points::default_points() {
            defaults = scalar::defaults();
            &defaults[..]
        } else {
            match points {
                Points::One(p) => std::slice::from_ref(p),
                Points::Many(v) => v,
            }
        };
        let missing: Vec<Part> = requested
            .iter()
            .filter(|p| !s.cache.contains_key(&(name.clone(), (*p).clone())))
            .cloned()
            .collect();
        let fresh = scalar.values(&Points::Many(missing), timestamp(time), self.limits)?;
        let mut result = Vec::with_capacity(requested.len());
        let mut response_bytes = 32usize;
        for point in requested {
            let value = s
                .cache
                .get(&(name.clone(), point.clone()))
                .map(|c| &c.value)
                .or_else(|| fresh.iter().find(|(p, _)| p == point).map(|(_, v)| v))
                .expect("requested scalar datapoint");
            response_bytes = response_bytes
                .saturating_add(self.limits.report.part(point).map_err(|_| Error::Limit)?)
                .saturating_add(value.bytes(self.limits.report)?);
            if response_bytes > self.limits.retained_bytes {
                return Err(Error::Limit);
            }
            result.push((point.clone(), value.clone()));
        }
        // Bound and validate the whole fill before retaining anything. A cache
        // quota rejection cannot partially replace an earlier cache snapshot.
        let mut inserts = BTreeMap::new();
        let mut bytes = 0usize;
        for (point, value) in fresh {
            let entry_bytes = self.cache_bytes(name, &point, &value)?;
            if inserts.insert(point, (value, entry_bytes)).is_none() {
                bytes = bytes.saturating_add(entry_bytes);
            }
        }
        if s.cache.len().saturating_add(inserts.len()) > self.limits.subscriptions
            || s.bytes.saturating_add(bytes) > self.limits.retained_bytes
        {
            return Err(Error::Limit);
        }
        let final_sequence = s
            .cache_sequence
            .checked_add(inserts.len() as u64)
            .ok_or(Error::Sequence)?;
        for (point, (value, bytes)) in inserts {
            s.cache_sequence += 1;
            s.cache.insert(
                (name.clone(), point),
                Cached {
                    value,
                    bytes,
                    due: None,
                    info: CacheInfo {
                        sequence: s.cache_sequence,
                        timer: None,
                        written_microseconds: time.unix_microseconds,
                        ttl_ms,
                    },
                },
            );
        }
        s.cache_sequence = final_sequence;
        s.bytes += bytes;
        s.cache_bytes += bytes;
        if bytes != 0
            && let Some(signal) = &s.cache_signal
        {
            signal.notify();
        }
        Ok(Reading::Values(result))
    }
    pub(super) fn sample_locked(
        &self,
        s: &mut State,
        name: &Name,
        points: &Points,
        time: Time,
    ) -> Result<Sample, StoreError> {
        let stored = s.entries.get(name).ok_or(StoreError::NotFound)?;
        if stored.scalar.is_none() {
            return selected(stored, points, timestamp(time), self.limits);
        }
        let Reading::Values(values) = self
            .scalar_read(s, name, points, time)
            .map_err(as_reporter)?
        else {
            return Ok(Sample {
                metric: name.clone(),
                values: vec![],
            });
        };
        Ok(Sample {
            metric: name.clone(),
            values: values.into_iter().map(|(p, v)| (p, v.report())).collect(),
        })
    }
    pub(super) fn cache_turn(
        &self,
        signal: Option<service::Signal>,
        restart: bool,
        time: Time,
    ) -> Result<Option<Instant>, Error> {
        let mut s = self.lock();
        s.open()?;
        if let Some(signal) = signal {
            s.cache_signal = Some(signal);
        }
        let mut plans = Vec::new();
        let mut timer = s.timer_sequence;
        for (key, cached) in &s.cache {
            if restart || cached.info.timer.is_none() {
                timer = timer.checked_add(1).ok_or(Error::Sequence)?;
                let delay = timeout(
                    cached.info.written_microseconds,
                    cached.info.ttl_ms,
                    time.unix_microseconds,
                )?;
                let due = time
                    .monotonic
                    .checked_add(Duration::from_millis(delay))
                    .ok_or(Error::Limit)?;
                plans.push((key.clone(), timer, due));
            }
        }
        for (key, timer, due) in plans {
            let entry = s.cache.get_mut(&key).expect("cache locked");
            entry.info.timer = Some(timer);
            entry.due = Some(due);
        }
        s.timer_sequence = timer;
        Ok(s.cache.values().filter_map(|c| c.due).min())
    }
    pub(super) fn expire_next(&self, time: Time) -> Result<(), Error> {
        let s = self.lock();
        s.open()?;
        let key = s
            .cache
            .iter()
            .filter_map(|(key, c)| c.due.map(|due| (key, due, c.info.timer)))
            .filter(|(_, due, _)| *due <= time.monotonic)
            .min_by_key(|(_, due, timer)| (*due, *timer))
            .map(|(key, _, timer)| (key.clone(), timer.expect("armed timer")));
        drop(s);
        if let Some(((name, point), timer)) = key {
            self.cache_timeout(&name, &point, timer)?;
        }
        Ok(())
    }
    pub(super) fn cache_timeout(&self, name: &Name, point: &Part, timer: u64) -> Result<(), Error> {
        let mut s = self.lock();
        s.open()?;
        let key = (name.clone(), point.clone());
        if s.cache
            .get(&key)
            .is_some_and(|c| c.info.timer == Some(timer))
        {
            let old = s.cache.remove(&key).expect("matched cache token");
            s.bytes -= old.bytes;
            s.cache_bytes -= old.bytes;
        }
        Ok(())
    }
}
fn timestamp(time: Time) -> i128 {
    time.unix_microseconds.div_euclid(1000) - 1_258_000_000_000
}
fn ttl(ttl: u64) -> Result<(), Error> {
    if ttl > u32::MAX as u64 {
        Err(Error::Limit)
    } else {
        Ok(())
    }
}
fn timeout(written: i128, ttl_ms: u64, now: i128) -> Result<u64, Error> {
    let age = now.saturating_sub(written) / 1000;
    let delay = i128::from(ttl_ms).saturating_sub(age).max(0);
    let delay = u64::try_from(delay).map_err(|_| Error::Limit)?;
    ttl(delay)?;
    Ok(delay)
}
