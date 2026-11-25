use super::{
    report,
    reporter::{
        self, Catalog, Clock, Limits, Points, Sample, Selector, Signal, Source, SystemClock,
    },
};
use crate::routing::{Name, Part};
use reporter::Error as StoreError;
use std::sync::Arc;
mod scalar;
use crate::routing::Entry;
use scalar::Scalar;
pub use scalar::{Alias, Error, Info, Kind, Reading, Value};
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
    version: u64,
    bytes: usize,
}
struct State {
    entries: BTreeMap<Name, Stored>,
    revision: u64,
    bytes: usize,
    signals: Vec<Signal>,
    enabled: bool,
    aliases: BTreeMap<Vec<u8>, scalar::Mapping>,
}
// Stable value storage is independent of connection, logger and reporter
// generations. Publishing a coherent observer snapshot never reports it early.
pub struct Store {
    state: Mutex<State>,
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
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                revision: 0,
                bytes: 0,
                signals: vec![],
                enabled: true,
                aliases: BTreeMap::new(),
            }),
            limits,
            clock,
        })
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
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
        let mut total = s.bytes;
        let mut count = s.entries.len();
        for (m, bytes) in &checked {
            if let Some(old) = s.entries.get(&m.entry.name) {
                if old.scalar.is_some() {
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
        let mut s = self.lock();
        let next = s.revision.checked_add(1).ok_or(StoreError::Sequence)?;
        let old = s.entries.remove(name).ok_or(StoreError::NotFound)?;
        s.bytes -= old.bytes;
        s.revision = next;
        for signal in &s.signals {
            signal.notify();
        }
        Ok(())
    }
    pub fn publish(&self, snapshot: &super::metrics::Snapshot) -> Result<(), StoreError> {
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
        self.put(entries.into_values().collect())
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
        let Selector::Metric(name) = metric else {
            return Ok(true);
        };
        let now = self.timestamp();
        let s = self.lock();
        self.limits.points(points)?;
        Ok(s.entries.get(name).is_some_and(|m| {
            m.metric.entry.enabled
                && if m.scalar.is_some() {
                    if *points == Points::default_points() {
                        true
                    } else {
                        let requested = match points {
                            Points::One(p) => std::slice::from_ref(p),
                            Points::Many(v) => v,
                        };
                        let mut available = vec![Part::atom("value"), Part::atom("ms_since_reset")];
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
        let now = self.timestamp();
        self.limits.points(points)?;
        let s = self.lock();
        let entries: Box<dyn Iterator<Item = &Stored> + '_> = match metric {
            Selector::Metric(name) => Box::new(s.entries.get(name).into_iter()),
            Selector::Find(prefix) => Box::new(
                s.entries
                    .iter()
                    .filter(|(n, _)| n.starts_with(prefix))
                    .map(|(_, m)| m),
            ),
        };
        let mut result = Vec::new();
        let mut bytes = 32usize;
        for m in entries.filter(|m| m.metric.entry.enabled) {
            let sample = match selected(m, points, now, self.limits) {
                Ok(sample) => sample,
                Err(StoreError::InvalidDatapoint) => continue,
                Err(e) => return Err(e),
            };
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
                return Err(StoreError::Limit);
            }
            result.push(sample);
        }
        Ok(result)
    }
    fn catalog(&self, after: Option<u64>) -> Result<Option<Catalog>, StoreError> {
        let s = self.lock();
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
