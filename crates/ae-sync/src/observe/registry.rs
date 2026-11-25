use super::{
    report,
    reporter::{
        self, Catalog, Clock, Limits, Points, Sample, Selector, Signal, Source, SystemClock,
    },
};
use crate::routing::{Name, Part};
use reporter::Error as StoreError;
use std::sync::Arc;
mod cache;
pub mod histogram;
mod native_histogram;
pub mod predefined;
mod scalar;
mod service;
use crate::routing::Entry;
pub use cache::CacheInfo;
pub use native_histogram::{HistogramOptions, HistogramSettings};
use scalar::Scalar;
pub use scalar::{Alias, Error, Info, Kind, Reading, Value};
pub use service::{Client, Manager, ManagerInfo, Owner, ServiceInfo, Ticket};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

#[derive(Debug, Clone)]
pub struct Metric {
    pub entry: Entry,
    pub values: Vec<(Part, report::Value)>,
}
struct Stored {
    metric: Metric,
    scalar: Option<Scalar>,
    histogram: Option<native_histogram::Registered>,
    read_epoch: u64,
    version: u64,
    bytes: usize,
    cache_ms: u64,
}
struct State {
    entries: BTreeMap<Name, Stored>,
    revision: u64,
    bytes: usize,
    signals: Vec<Signal>,
    enabled: bool,
    aliases: BTreeMap<Vec<u8>, scalar::Mapping>,
    cache: BTreeMap<(Name, Part), cache::Cached>,
    cache_bytes: usize,
    cache_sequence: u64,
    timer_sequence: u64,
    cache_signal: Option<service::Signal>,
    owned: bool,
    closed: bool,
}
// Stable value storage is independent of connection, logger and reporter
// generations. Publishing a coherent observer snapshot never reports it early.
pub struct Store {
    state: Mutex<State>,
    control: Mutex<()>,
    probe_budget: histogram::probe::Budget,
    probe_signal: Arc<Mutex<Option<service::Signal>>>,
    limits: Limits,
    clock: Arc<dyn Clock>,
}
impl Store {
    pub fn new(limits: Limits) -> Result<Self, StoreError> {
        Self::with_clock(limits, Arc::new(SystemClock))
    }
    pub fn with_clock(limits: Limits, clock: Arc<dyn Clock>) -> Result<Self, StoreError> {
        limits.validate()?;
        Ok(Self {
            control: Mutex::new(()),
            probe_budget: histogram::probe::Budget::new(histogram::probe::Limits {
                values: limits,
                requests: limits.requests,
                pending_bytes: limits.pending_bytes,
                retained_bytes: limits.retained_bytes,
                ..Default::default()
            })
            .map_err(as_reporter)?,
            probe_signal: Arc::new(Mutex::new(None)),
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                revision: 0,
                bytes: 0,
                signals: vec![],
                enabled: true,
                aliases: BTreeMap::new(),
                cache: BTreeMap::new(),
                cache_bytes: 0,
                cache_sequence: 0,
                timer_sequence: 0,
                cache_signal: None,
                owned: false,
                closed: false,
            }),
            limits,
            clock,
        })
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn control(&self) -> std::sync::MutexGuard<'_, ()> {
        self.control.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn size(&self, m: &Metric) -> Result<usize, StoreError> {
        let l = self.limits;
        let mut bytes = 192usize
            .saturating_add(l.atom(&m.entry.kind)?)
            .saturating_add(
                l.report
                    .name(&m.entry.name)
                    .map_err(StoreError::Report)?
                    .saturating_mul(2),
            );
        if m.values.len() > l.report.parts {
            return Err(StoreError::Limit);
        }
        let mut seen = BTreeSet::new();
        for (p, v) in &m.values {
            if !seen.insert(p) {
                return Err(StoreError::InvalidDatapoint);
            }
            bytes = bytes
                .saturating_add(l.report.part(p).map_err(StoreError::Report)?)
                .saturating_add(v.size(l.report).map_err(StoreError::Report)?);
        }
        if bytes > l.retained_bytes {
            return Err(StoreError::Limit);
        }
        Ok(bytes)
    }
    pub fn put(&self, metrics: Vec<Metric>) -> Result<(), StoreError> {
        self.put_frame(metrics, false)
    }
    fn put_frame(&self, metrics: Vec<Metric>, predefined: bool) -> Result<(), StoreError> {
        let _control = self.control();
        if metrics.len() > self.limits.subscriptions {
            return Err(StoreError::Limit);
        }
        let mut seen = BTreeSet::new();
        let mut checked = Vec::with_capacity(metrics.len());
        let mut incoming_bytes = 0usize;
        for m in metrics {
            let bytes = self.size(&m)?;
            incoming_bytes = incoming_bytes.saturating_add(bytes);
            if incoming_bytes > self.limits.retained_bytes {
                return Err(StoreError::Limit);
            }
            if !seen.insert(m.entry.name.clone()) {
                return Err(StoreError::Configuration);
            }
            // Clone after validation so caller-supplied spare capacities do not
            // become uncharged retained storage.
            checked.push((m.clone(), bytes));
        }
        let mut s = self.lock();
        s.open().map_err(as_reporter)?;
        let mut total = s.bytes;
        let mut count = s.entries.len();
        let mut gauges = BTreeMap::new();
        for (m, bytes) in &checked {
            if predefined && let Some(kind) = predefined::sampled_kind(&m.entry.name) {
                let old = s
                    .entries
                    .get(&m.entry.name)
                    .ok_or(StoreError::Configuration)?;
                let scalar = old.scalar.as_ref().ok_or(StoreError::Configuration)?;
                if scalar.kind != kind
                    || m.values.len() != 1
                    || m.values[0].0 != Part::atom("value")
                {
                    return Err(StoreError::Configuration);
                }
                if kind == Kind::Gauge && s.enabled && old.metric.entry.enabled {
                    let value = match &m.values[0].1 {
                        report::Value::Integer(n) => Value::Integer(n.clone()),
                        report::Value::Float(n) => Value::Float(*n),
                        _ => return Err(StoreError::Configuration),
                    };
                    let bytes = old.bytes
                        - scalar
                            .value
                            .bytes(self.limits.report)
                            .map_err(as_reporter)?
                        + value.bytes(self.limits.report).map_err(as_reporter)?;
                    total = total - old.bytes + bytes;
                    gauges.insert(m.entry.name.clone(), (value, bytes));
                }
                continue;
            }
            if let Some(old) = s.entries.get(&m.entry.name) {
                if old.scalar.is_some() || old.histogram.is_some() {
                    return Err(StoreError::Configuration);
                }
                total -= old.bytes;
            } else {
                count += 1;
            }
            total = total.saturating_add(*bytes);
        }
        if count > self.limits.subscriptions || total > self.limits.retained_bytes {
            return Err(StoreError::Limit);
        }
        let next = s.revision.checked_add(1).ok_or(StoreError::Sequence)?;
        let mut changed = false;
        for (metric, bytes) in checked {
            if predefined && predefined::sampled_kind(&metric.entry.name).is_some() {
                if let Some((value, bytes)) = gauges.remove(&metric.entry.name) {
                    let old = s
                        .entries
                        .get_mut(&metric.entry.name)
                        .expect("frame checked");
                    old.scalar.as_mut().expect("gauge checked").value = value;
                    old.bytes = bytes;
                }
                continue;
            }
            let version = s
                .entries
                .get(&metric.entry.name)
                .filter(|old| {
                    old.metric.entry == metric.entry
                        && old
                            .metric
                            .values
                            .iter()
                            .map(|p| &p.0)
                            .eq(metric.values.iter().map(|p| &p.0))
                })
                .map(|old| old.version)
                .unwrap_or_else(|| {
                    changed = true;
                    next
                });
            s.entries.insert(
                metric.entry.name.clone(),
                Stored {
                    metric,
                    scalar: None,
                    histogram: None,
                    read_epoch: 0,
                    cache_ms: 0,
                    version,
                    bytes,
                },
            );
        }
        s.bytes = total;
        if changed {
            s.revision = next;
            for signal in &s.signals {
                signal.notify();
            }
        }
        Ok(())
    }
    pub fn remove(&self, name: &Name) -> Result<(), StoreError> {
        let _control = self.control();
        let mut s = self.lock();
        s.open().map_err(as_reporter)?;
        let next = s.revision.checked_add(1).ok_or(StoreError::Sequence)?;
        s.clear_cache_name(name);
        let old = s.entries.remove(name).ok_or(StoreError::NotFound)?;
        s.bytes -= old.bytes;
        s.revision = next;
        for signal in &s.signals {
            signal.notify();
        }
        drop(s);
        drop(old);
        Ok(())
    }
    pub fn publish(&self, snapshot: &super::metrics::Snapshot) -> Result<(), StoreError> {
        self.publish_frame(snapshot, false)
    }
    fn publish_frame(
        &self,
        snapshot: &super::metrics::Snapshot,
        predefined: bool,
    ) -> Result<(), StoreError> {
        if snapshot.metrics.len() > self.limits.subscriptions {
            return Err(StoreError::Limit);
        }
        let mut entries: BTreeMap<Name, Metric> = BTreeMap::new();
        for metric in &snapshot.metrics {
            if metric.name.len() > self.limits.report.bytes {
                return Err(StoreError::Limit);
            }
            let (entry, key) = metric.routing();
            let value = match &metric.value {
                super::metrics::Number::Integer(n) => report::Value::Integer((*n).into()),
                super::metrics::Number::Unsigned(n) => {
                    if n.as_bytes().len() as u64 > self.limits.report.integer_bits.div_ceil(8) {
                        return Err(StoreError::Limit);
                    }
                    report::Value::Integer(num_bigint::BigInt::from_bytes_be(
                        num_bigint::Sign::Plus,
                        n.as_bytes(),
                    ))
                }
                super::metrics::Number::Float(n) => report::Value::Float(*n),
            };
            entries
                .entry(entry.name.clone())
                .or_insert(Metric {
                    entry,
                    values: vec![],
                })
                .values
                .push((key.datapoint, value));
        }
        self.put_frame(entries.into_values().collect(), predefined)
    }
}
fn selected(
    stored: &Stored,
    points: &Points,
    now: i128,
    limits: Limits,
) -> Result<Sample, StoreError> {
    let m = &stored.metric;
    let values = if let Some(scalar) = &stored.scalar {
        scalar
            .values(points, now, limits)
            .map_err(|_| StoreError::Limit)?
            .into_iter()
            .map(|(p, v)| (p, v.report()))
            .collect()
    } else if *points == Points::default_points() {
        m.values.clone()
    } else {
        let requested = match points {
            Points::One(p) => std::slice::from_ref(p),
            Points::Many(v) => v,
        };
        requested
            .iter()
            .map(|p| {
                m.values
                    .iter()
                    .find(|(dp, _)| dp == p)
                    .cloned()
                    .ok_or(StoreError::InvalidDatapoint)
            })
            .collect::<Result<_, _>>()?
    };
    Ok(Sample {
        metric: m.entry.name.clone(),
        values,
    })
}
impl Source for Store {
    fn valid(&self, metric: &Selector, points: &Points) -> Result<bool, StoreError> {
        let now = self.timestamp();
        let s = self.lock();
        s.open().map_err(as_reporter)?;
        let Selector::Metric(name) = metric else {
            return Ok(true);
        };
        self.limits.points(points)?;
        Ok(s.entries.get(name).is_some_and(|m| {
            m.metric.entry.enabled
                && if m.scalar.is_some() || m.histogram.is_some() {
                    if *points == Points::default_points() {
                        true
                    } else {
                        let requested = match points {
                            Points::One(p) => std::slice::from_ref(p),
                            Points::Many(v) => v,
                        };
                        let mut available = if m.histogram.is_some() {
                            histogram::datapoints()
                        } else {
                            scalar::defaults()
                        };
                        !requested.is_empty()
                            && requested.iter().all(|p| {
                                if let Some(i) = available.iter().position(|a| a == p) {
                                    available.remove(i);
                                    true
                                } else {
                                    false
                                }
                            })
                    }
                } else {
                    selected(m, points, now, self.limits).is_ok()
                }
        }))
    }
    fn read(&self, metric: &Selector, points: &Points) -> Result<Vec<Sample>, StoreError> {
        let time = self.clock.now();
        self.limits.points(points)?;
        let mut s = self.lock();
        s.open().map_err(as_reporter)?;
        let names: Vec<Name> = match metric {
            Selector::Metric(name) => s
                .entries
                .contains_key(name)
                .then(|| name.clone())
                .into_iter()
                .collect(),
            Selector::Find(prefix) => s
                .entries
                .keys()
                .filter(|n| n.starts_with(prefix))
                .cloned()
                .collect(),
        };
        let mut bytes = 32usize;
        let mut reserve = |sample: &Sample| -> Result<(), StoreError> {
            bytes = bytes.saturating_add(
                self.limits
                    .report
                    .name(&sample.metric)
                    .map_err(StoreError::Report)?,
            );
            for (p, v) in &sample.values {
                bytes = bytes
                    .saturating_add(self.limits.report.part(p).map_err(StoreError::Report)?)
                    .saturating_add(v.size(self.limits.report).map_err(StoreError::Report)?);
            }
            if bytes > self.limits.retained_bytes {
                Err(StoreError::Limit)
            } else {
                Ok(())
            }
        };
        let mut plan = Vec::new();
        // Capture sampled frames and scalars under one lock, preserving coherent
        // publication even if histogram work below blocks on another thread.
        for name in names {
            let entry = &s.entries[&name];
            if !entry.metric.entry.enabled {
                continue;
            }
            let sample = if entry.histogram.is_some() {
                None
            } else {
                match self.sample_locked(&mut s, &name, points, time) {
                    Ok(sample) => {
                        reserve(&sample)?;
                        Some(sample)
                    }
                    Err(StoreError::InvalidDatapoint) => continue,
                    Err(e) => return Err(e),
                }
            };
            plan.push((name, sample));
        }
        drop(s);
        let mut result = Vec::with_capacity(plan.len());
        for (name, sample) in plan {
            let sample = match sample {
                Some(sample) => sample,
                None => match self.histogram_read(&name, points, time) {
                    Ok(Reading::Values(values)) => {
                        let sample = Sample {
                            metric: name,
                            values: values.into_iter().map(|(p, v)| (p, v.report())).collect(),
                        };
                        reserve(&sample)?;
                        sample
                    }
                    Ok(Reading::Disabled) | Err(Error::NotFound) => continue,
                    Err(e) => return Err(as_reporter(e)),
                },
            };
            result.push(sample);
        }
        Ok(result)
    }
    fn catalog(&self, after: Option<u64>) -> Result<Option<Catalog>, StoreError> {
        let s = self.lock();
        s.open().map_err(as_reporter)?;
        Ok((after != Some(s.revision)).then(|| Catalog {
            revision: s.revision,
            entries: s
                .entries
                .values()
                .map(|m| (m.metric.entry.clone(), m.version))
                .collect(),
        }))
    }
    fn watch(&self, signal: Signal) -> Result<(), StoreError> {
        let mut s = self.lock();
        s.open().map_err(as_reporter)?;
        s.signals.retain(|w| !w.closed());
        if s.signals.len() >= self.limits.reporters {
            return Err(StoreError::Limit);
        }
        s.signals.push(signal);
        Ok(())
    }
}

#[cfg(test)]
mod tests;

fn as_reporter(error: Error) -> StoreError {
    match error {
        Error::Closed => StoreError::Closed,
        Error::Sequence => StoreError::Sequence,
        Error::Limit => StoreError::Limit,
        Error::NotFound => StoreError::NotFound,
        _ => StoreError::Source,
    }
}
impl State {
    fn open(&self) -> Result<(), Error> {
        if self.closed {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
}
