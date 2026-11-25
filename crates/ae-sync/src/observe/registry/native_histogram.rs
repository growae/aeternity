use super::*;
use histogram::{Config, probe};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramOptions {
    pub config: Config,
    pub policy: probe::Policy,
    pub enabled: bool,
    pub cache_ms: u64,
}
impl Default for HistogramOptions {
    fn default() -> Self {
        Self {
            config: Config::default(),
            policy: probe::Policy::default(),
            enabled: true,
            cache_ms: 0,
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HistogramSettings {
    pub config: Option<Config>,
    pub enabled: Option<bool>,
    pub cache_ms: Option<u64>,
}
pub(super) struct Registered {
    config: Config,
    policy: probe::Policy,
    pub(super) owner: Option<probe::Owner>,
}
impl Registered {
    pub(super) fn client(&self) -> Result<probe::Client, Error> {
        self.owner
            .as_ref()
            .map(probe::Owner::client)
            .ok_or(Error::Closed)
    }
}
impl Store {
    fn spawn_histogram(
        &self,
        config: Config,
        policy: probe::Policy,
    ) -> Result<probe::Owner, Error> {
        let signal = Arc::downgrade(&self.probe_signal);
        probe::Owner::spawn_notified(
            config,
            policy,
            self.probe_budget.clone(),
            self.clock.clone(),
            Some(Box::new(move || {
                if let Some(signal) = signal.upgrade() {
                    let signal = signal.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    if let Some(signal) = signal {
                        signal.notify();
                    }
                }
            })),
        )
    }
    pub fn create_histogram(&self, name: &Name, options: &HistogramOptions) -> Result<(), Error> {
        self.register_histogram(name, options, false)
    }
    pub fn ensure_histogram(&self, name: &Name, options: &HistogramOptions) -> Result<(), Error> {
        self.register_histogram(name, options, true)
    }
    fn register_histogram(
        &self,
        name: &Name,
        options: &HistogramOptions,
        ensure: bool,
    ) -> Result<(), Error> {
        let _control = self.control();
        let name_bytes = self.scalar_name(name)?;
        let s = self.lock();
        s.open()?;
        if !s.owned {
            return Err(Error::Closed);
        }
        if let Some(old) = s.entries.get(name) {
            return if !ensure {
                Err(Error::Exists)
            } else if old.histogram.is_none() {
                Err(Error::TypeMismatch)
            } else {
                Ok(())
            };
        }
        options.config.validate(self.limits)?;
        self.probe_budget.validate_policy(&options.policy)?;
        cache::ttl(options.cache_ms)?;
        let bytes = 512usize
            .saturating_add(name_bytes.saturating_mul(2))
            .saturating_add(options.policy.0.len().saturating_mul(64));
        if s.entries.len() >= self.limits.subscriptions
            || s.bytes.saturating_add(bytes) > self.limits.retained_bytes
        {
            return Err(Error::Limit);
        }
        drop(s);
        let owner = if options.enabled {
            Some(self.spawn_histogram(options.config, options.policy.clone())?)
        } else {
            None
        };
        let mut s = self.lock();
        s.open()?;
        // Cache traffic can consume quota while the worker starts.
        if s.bytes.saturating_add(bytes) > self.limits.retained_bytes {
            return Err(Error::Limit);
        }
        let version = s.revision.checked_add(1).ok_or(Error::Sequence)?;
        s.entries.insert(
            name.clone(),
            Stored {
                metric: Metric {
                    entry: Entry {
                        name: name.clone(),
                        kind: "histogram".into(),
                        enabled: options.enabled,
                    },
                    values: vec![],
                },
                scalar: None,
                histogram: Some(Registered {
                    config: options.config,
                    policy: options.policy.clone(),
                    owner,
                }),
                read_epoch: 0,
                version,
                bytes,
                cache_ms: options.cache_ms,
            },
        );
        s.bytes += bytes;
        s.revision = version;
        for signal in &s.signals {
            signal.notify();
        }
        // A worker can fail before its entry is published.
        if let Some(signal) = &*self.probe_signal.lock().unwrap_or_else(|e| e.into_inner()) {
            signal.notify();
        }
        Ok(())
    }
    pub fn histogram_options(&self, name: &Name) -> Result<HistogramOptions, Error> {
        self.scalar_name(name)?;
        let s = self.lock();
        s.open()?;
        let entry = s.entries.get(name).ok_or(Error::NotFound)?;
        let h = entry.histogram.as_ref().ok_or(Error::TypeMismatch)?;
        Ok(HistogramOptions {
            config: h.config,
            policy: h.policy.clone(),
            enabled: entry.metric.entry.enabled,
            cache_ms: entry.cache_ms,
        })
    }
    pub fn histogram_probe_info(&self, name: &Name) -> Result<Option<probe::Info>, Error> {
        self.scalar_name(name)?;
        let s = self.lock();
        s.open()?;
        let h = s
            .entries
            .get(name)
            .ok_or(Error::NotFound)?
            .histogram
            .as_ref()
            .ok_or(Error::TypeMismatch)?;
        Ok(h.owner.as_ref().map(|owner| owner.client().info()))
    }
    pub fn histogram_usage(&self) -> probe::Usage {
        self.probe_budget.usage()
    }
    pub fn set_histogram(&self, name: &Name, settings: HistogramSettings) -> Result<(), Error> {
        let _control = self.control();
        self.histogram_settings_locked(name, settings)
    }
    pub(super) fn histogram_settings_locked(
        &self,
        name: &Name,
        settings: HistogramSettings,
    ) -> Result<(), Error> {
        self.scalar_name(name)?;
        if let Some(config) = settings.config {
            config.validate(self.limits)?;
        }
        if let Some(ttl) = settings.cache_ms {
            cache::ttl(ttl)?;
        }
        let s = self.lock();
        s.open()?;
        let entry = s.entries.get(name).ok_or(Error::NotFound)?;
        let h = entry.histogram.as_ref().ok_or(Error::TypeMismatch)?;
        let was_enabled = entry.metric.entry.enabled;
        let enabled = settings.enabled.unwrap_or(was_enabled);
        if !was_enabled && !enabled {
            return Err(if settings.enabled.is_some() {
                Error::InvalidStatus
            } else {
                Error::Disabled
            });
        }
        let next_version = s.revision.checked_add(1).ok_or(Error::Sequence)?;
        let old_config = h.config;
        let config = settings.config.unwrap_or(h.config);
        let policy = h.policy.clone();
        drop(s);
        // Native enable starts from the previous saved options, then saves the
        // options supplied in this call for the next worker generation.
        let mut fresh = if enabled && !was_enabled {
            Some(self.spawn_histogram(old_config, policy)?)
        } else {
            None
        };
        let mut s = self.lock();
        let entry = s.entries.get_mut(name).expect("serialized registration");
        let next_epoch = entry.read_epoch.checked_add(1).ok_or(Error::Sequence)?;
        let h = entry.histogram.as_mut().expect("histogram checked");
        if enabled {
            let client = fresh
                .as_ref()
                .map(probe::Owner::client)
                .or_else(|| h.owner.as_ref().map(probe::Owner::client))
                .ok_or(Error::Closed)?;
            client.configure_next(config)?;
        }
        let retired = if !enabled { h.owner.take() } else { None };
        if fresh.is_some() {
            h.owner = fresh.take();
        }
        h.config = config;
        entry.metric.entry.enabled = enabled;
        entry.read_epoch = next_epoch;
        if let Some(ttl) = settings.cache_ms {
            entry.cache_ms = ttl;
        }
        if enabled != was_enabled {
            entry.version = next_version;
            s.revision = next_version;
            for signal in &s.signals {
                signal.notify();
            }
        }
        if !enabled {
            s.clear_cache_name(name);
        }
        drop(s);
        drop(retired);
        Ok(())
    }
    pub(super) fn histogram_turn(&self, signal: Option<service::Signal>) -> Result<(), Error> {
        let _control = self.control();
        if let Some(signal) = signal {
            *self.probe_signal.lock().unwrap_or_else(|e| e.into_inner()) = Some(signal);
        }
        let mut s = self.lock();
        s.open()?;
        let terminal: Vec<_> = s
            .entries
            .iter()
            .filter_map(|(name, entry)| {
                let owner = entry.histogram.as_ref()?.owner.as_ref()?;
                let info = owner.client().info();
                info.closing.then(|| {
                    (
                        name.clone(),
                        info.terminal_action.unwrap_or(probe::Action::Disable),
                    )
                })
            })
            .collect();
        let final_version = s
            .revision
            .checked_add(terminal.len() as u64)
            .ok_or(Error::Sequence)?;
        let mut retired = Vec::new();
        for (name, action) in terminal {
            s.revision += 1;
            s.clear_cache_name(&name);
            if action == probe::Action::Delete {
                let entry = s.entries.remove(&name).expect("entry locked");
                s.bytes -= entry.bytes;
                retired.push(entry.histogram.expect("histogram checked").owner);
            } else {
                let version = s.revision;
                let entry = s.entries.get_mut(&name).expect("entry locked");
                entry.metric.entry.enabled = false;
                entry.version = version;
                retired.push(
                    entry
                        .histogram
                        .as_mut()
                        .expect("histogram checked")
                        .owner
                        .take(),
                );
            }
        }
        s.revision = final_version;
        if !retired.is_empty() {
            for signal in &s.signals {
                signal.notify();
            }
        }
        drop(s);
        // Only this independent admin thread joins a terminal coordinator.
        drop(retired);
        Ok(())
    }
    pub(super) fn histogram_read(
        &self,
        name: &Name,
        points: &Points,
        time: reporter::Time,
    ) -> Result<Reading, Error> {
        let requested = if *points == Points::default_points() {
            histogram::datapoints()
        } else {
            match points {
                Points::One(p) => vec![p.clone()],
                Points::Many(p) => p.clone(),
            }
        };
        let mut s = self.lock();
        s.open()?;
        let entry = s.entries.get(name).ok_or(Error::NotFound)?;
        if !entry.metric.entry.enabled {
            return Ok(Reading::Disabled);
        }
        let h = entry.histogram.as_ref().ok_or(Error::TypeMismatch)?;
        let client = h.client()?;
        let version = entry.version;
        let epoch = entry.read_epoch;
        let ttl = entry.cache_ms;
        let generation = client.info().generation;
        let missing = if ttl == 0 {
            requested.clone()
        } else {
            requested
                .iter()
                .filter(|p| !s.cache.contains_key(&(name.clone(), (*p).clone())))
                .cloned()
                .collect()
        };
        if ttl != 0 && missing.is_empty() {
            return self.cache_fill(&mut s, name, &requested, vec![], ttl, time);
        }
        let ticket = client.values(&Points::Many(missing))?;
        drop(s);
        let fresh = ticket.blocking_wait()?;
        let written = self.clock.now();
        let mut s = self.lock();
        s.open()?;
        let entry = s.entries.get(name).ok_or(Error::Stale)?;
        if entry.version != version
            || entry.read_epoch != epoch
            || !entry.metric.entry.enabled
            || client.info().generation != generation
        {
            return Err(Error::Stale);
        }
        if ttl == 0 {
            return Ok(Reading::Values(fresh));
        }
        self.cache_fill(&mut s, name, &requested, fresh, ttl, written)
    }
}

#[cfg(test)]
mod tests;
