use super::*;
use crate::routing::Entry;
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
    version: u64,
    bytes: usize,
}
struct State {
    entries: BTreeMap<Name, Stored>,
    revision: u64,
    bytes: usize,
    signals: Vec<Signal>,
}
// Stable value storage is independent of connection, logger and reporter
// generations. Publishing a coherent observer snapshot never reports it early.
pub struct Store {
    state: Mutex<State>,
    limits: Limits,
}
impl Store {
    pub fn new(limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        Ok(Self {
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                revision: 0,
                bytes: 0,
                signals: vec![],
            }),
            limits,
        })
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn size(&self, m: &Metric) -> Result<usize, Error> {
        let l = self.limits;
        let mut bytes = 192usize
            .saturating_add(l.atom(&m.entry.kind)?)
            .saturating_add(l.report.name(&m.entry.name).map_err(Error::Report)?);
        if m.values.len() > l.report.parts {
            return Err(Error::Limit);
        }
        let mut seen = BTreeSet::new();
        for (p, v) in &m.values {
            if !seen.insert(p) {
                return Err(Error::InvalidDatapoint);
            }
            bytes = bytes
                .saturating_add(l.report.part(p).map_err(Error::Report)?)
                .saturating_add(v.size(l.report).map_err(Error::Report)?);
        }
        if bytes > l.retained_bytes {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
    pub fn put(&self, metrics: Vec<Metric>) -> Result<(), Error> {
        if metrics.len() > self.limits.subscriptions {
            return Err(Error::Limit);
        }
        let mut seen = BTreeSet::new();
        let mut checked = Vec::with_capacity(metrics.len());
        let mut incoming_bytes = 0usize;
        for m in metrics {
            let bytes = self.size(&m)?;
            incoming_bytes = incoming_bytes.saturating_add(bytes);
            if incoming_bytes > self.limits.retained_bytes {
                return Err(Error::Limit);
            }
            if !seen.insert(m.entry.name.clone()) {
                return Err(Error::Configuration);
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
                total -= old.bytes;
            } else {
                count += 1;
            }
            total = total.saturating_add(*bytes);
        }
        if count > self.limits.subscriptions || total > self.limits.retained_bytes {
            return Err(Error::Limit);
        }
        let next = s.revision.checked_add(1).ok_or(Error::Sequence)?;
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
    pub fn remove(&self, name: &Name) -> Result<(), Error> {
        let mut s = self.lock();
        let next = s.revision.checked_add(1).ok_or(Error::Sequence)?;
        let old = s.entries.remove(name).ok_or(Error::NotFound)?;
        s.bytes -= old.bytes;
        s.revision = next;
        for signal in &s.signals {
            signal.notify();
        }
        Ok(())
    }
    pub fn publish(&self, snapshot: &super::super::metrics::Snapshot) -> Result<(), Error> {
        if snapshot.metrics.len() > self.limits.subscriptions {
            return Err(Error::Limit);
        }
        let mut entries: BTreeMap<Name, Metric> = BTreeMap::new();
        for metric in &snapshot.metrics {
            if metric.name.len() > self.limits.report.bytes {
                return Err(Error::Limit);
            }
            let (entry, key) = metric.routing();
            let value = match &metric.value {
                super::super::metrics::Number::Integer(n) => report::Value::Integer((*n).into()),
                super::super::metrics::Number::Unsigned(n) => {
                    if n.as_bytes().len() as u64 > self.limits.report.integer_bits.div_ceil(8) {
                        return Err(Error::Limit);
                    }
                    report::Value::Integer(num_bigint::BigInt::from_bytes_be(
                        num_bigint::Sign::Plus,
                        n.as_bytes(),
                    ))
                }
                super::super::metrics::Number::Float(n) => report::Value::Float(*n),
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
fn selected(m: &Metric, points: &Points) -> Result<Sample, Error> {
    let values = if *points == Points::default_points() {
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
                    .ok_or(Error::InvalidDatapoint)
            })
            .collect::<Result<_, _>>()?
    };
    Ok(Sample {
        metric: m.entry.name.clone(),
        values,
    })
}
impl Source for Store {
    fn valid(&self, metric: &Selector, points: &Points) -> Result<bool, Error> {
        let Selector::Metric(name) = metric else {
            return Ok(true);
        };
        let s = self.lock();
        Ok(s.entries
            .get(name)
            .is_some_and(|m| m.metric.entry.enabled && selected(&m.metric, points).is_ok()))
    }
    fn read(&self, metric: &Selector, points: &Points) -> Result<Vec<Sample>, Error> {
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
        Ok(entries
            .filter(|m| m.metric.entry.enabled)
            .filter_map(|m| selected(&m.metric, points).ok())
            .collect())
    }
    fn catalog(&self, after: Option<u64>) -> Result<Option<Catalog>, Error> {
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
    fn watch(&self, signal: Signal) -> Result<(), Error> {
        let mut s = self.lock();
        s.signals.retain(|w| !w.closed());
        if s.signals.len() >= self.limits.reporters {
            return Err(Error::Limit);
        }
        s.signals.push(signal);
        Ok(())
    }
}
