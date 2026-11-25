use super::{report, statsd};
use crate::routing::{Key, Name, Part};
use std::sync::Arc;
use std::time::Duration;

mod callback;
mod order;
mod service;
mod timing;
pub use super::registry::{Metric, Store};
pub use service::{
    Client, Info, IntervalInfo, Owner, ReporterInfo, Signal, SubscriptionInfo, Ticket,
};
pub use timing::{Clock, SystemClock, Time};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Limit,
    Busy,
    Closed,
    UnknownReporter,
    NotFound,
    AlreadyRunning,
    InvalidDatapoint,
    UnsupportedKey,
    Source,
    Report(report::Error),
    Spawn,
    Panicked,
    Sequence,
    Stale,
    RestartLimit,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selector {
    Metric(Name),
    Find(Name),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Points {
    One(Part),
    Many(Vec<Part>),
}
impl Points {
    pub fn default_points() -> Self {
        Self::One(Part::atom("default"))
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Subscription {
    pub reporter: String,
    pub metric: Selector,
    pub points: Points,
    pub retry: bool,
    pub extra: report::Extra,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Interval {
    Named(String),
    Milliseconds(u64),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Period {
    Manual,
    Milliseconds(u64),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedInterval {
    pub name: String,
    pub period: Period,
    pub delay_ms: u64,
}
#[derive(Debug, Clone)]
pub struct Config {
    pub name: String,
    pub module: String,
    pub options: report::Options,
    pub intervals: Vec<NamedInterval>,
    pub enabled: bool,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            name: "aec_metrics_main".into(),
            module: "aec_metrics".into(),
            options: Default::default(),
            intervals: vec![NamedInterval {
                name: "default".into(),
                period: Period::Milliseconds(10_000),
                delay_ms: 0,
            }],
            enabled: true,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Sample {
    pub metric: Name,
    pub values: Vec<(Part, report::Value)>,
}
// Reads execute on the registry/timer owner, as in exometer_report:get_values.
// Implementations must bound their own work and return owned, stable samples.
#[derive(Debug, Clone)]
pub struct Catalog {
    pub revision: u64,
    pub entries: Vec<(crate::routing::Entry, u64)>,
}
pub trait Source: Send + Sync + 'static {
    fn watch(&self, _signal: Signal) -> Result<(), Error> {
        Ok(())
    }
    fn catalog(&self, _after: Option<u64>) -> Result<Option<Catalog>, Error> {
        Ok(None)
    }
    fn valid(&self, metric: &Selector, points: &Points) -> Result<bool, Error>;
    fn read(&self, metric: &Selector, points: &Points) -> Result<Vec<Sample>, Error>;
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub requests: usize,
    pub pending_bytes: usize,
    pub reporters: usize,
    pub subscriptions: usize,
    pub intervals: usize,
    pub retained_bytes: usize,
    pub callback_records: usize,
    pub callback_bytes: usize,
    pub report: report::Limits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            requests: 128,
            pending_bytes: 1 << 20,
            reporters: 32,
            subscriptions: 65_536,
            intervals: 256,
            retained_bytes: 16 << 20,
            callback_records: 128,
            callback_bytes: 1 << 20,
            report: Default::default(),
        }
    }
}
impl Limits {
    pub(super) fn points(self, points: &Points) -> Result<(), Error> {
        let p = match points {
            Points::One(p) => std::slice::from_ref(p),
            Points::Many(p) => p,
        };
        if p.len() > self.report.parts {
            return Err(Error::Limit);
        }
        for point in p {
            self.report.part(point).map_err(Error::Report)?;
        }
        Ok(())
    }
    pub(super) fn validate(self) -> Result<(), Error> {
        self.report.validate().map_err(Error::Report)?;
        if self.requests == 0
            || self.requests > 65_536
            || self.pending_bytes == 0
            || self.pending_bytes > 1 << 30
            || self.reporters == 0
            || self.reporters > 1024
            || self.subscriptions == 0
            || self.subscriptions > 1_000_000
            || self.intervals == 0
            || self.intervals > 4096
            || self.retained_bytes == 0
            || self.retained_bytes > 1 << 30
            || self.callback_records == 0
            || self.callback_records > 65_536
            || self.callback_bytes == 0
            || self.callback_bytes > 1 << 30
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
    pub(super) fn atom(self, s: &str) -> Result<usize, Error> {
        if s.len() > 1020 || s.chars().count() > 255 {
            return Err(Error::Limit);
        }
        Ok(32 + s.len())
    }
    fn period(self, p: Period) -> Result<(), Error> {
        if let Period::Milliseconds(ms) = p {
            // This is an operational bound on timer admission, not consensus.
            if ms > u32::MAX as u64 {
                return Err(Error::Limit);
            }
        }
        Ok(())
    }
    fn interval(self, i: &Interval) -> Result<usize, Error> {
        match i {
            Interval::Named(n) => self.atom(n),
            Interval::Milliseconds(ms) => {
                self.period(Period::Milliseconds(*ms))?;
                Ok(32)
            }
        }
    }
    fn named(self, i: &NamedInterval) -> Result<usize, Error> {
        self.period(i.period)?;
        self.period(Period::Milliseconds(i.delay_ms))?;
        Ok(64 + self.atom(&i.name)?)
    }
    fn event(self, name: Name, point: Part, extra: report::Extra) -> Result<report::Event, Error> {
        report::Options::default()
            .event(
                Key {
                    name,
                    datapoint: point,
                },
                report::Value::Integer(0.into()),
                extra,
                self.report,
            )
            .map_err(Error::Report)
    }
    fn subscription(self, s: &Subscription) -> Result<usize, Error> {
        let name = match &s.metric {
            Selector::Metric(n) | Selector::Find(n) => n,
        };
        let points = match &s.points {
            Points::One(p) => std::slice::from_ref(p),
            Points::Many(p) => p,
        };
        if points.is_empty() || points.len() > self.report.parts {
            return Err(Error::InvalidDatapoint);
        }
        let mut bytes = 128usize
            .saturating_add(self.atom(&s.reporter)?)
            .saturating_add(self.report.name(name).map_err(Error::Report)?)
            .saturating_add(s.extra.size(self.report).map_err(Error::Report)?);
        for p in points {
            bytes = bytes.saturating_add(self.report.part(p).map_err(Error::Report)?);
        }
        if bytes > self.pending_bytes {
            return Err(Error::Limit);
        }
        order::validate(s)?;
        Ok(bytes)
    }
    fn config(self, c: &Config) -> Result<usize, Error> {
        if c.intervals.len() > self.intervals {
            return Err(Error::Limit);
        }
        let mut bytes = self.atom(&c.name)? + self.atom(&c.module)?;
        let e = c
            .options
            .event(
                Key {
                    name: vec![],
                    datapoint: Part::atom("value"),
                },
                report::Value::Integer(0.into()),
                report::Extra::Undefined,
                self.report,
            )
            .map_err(Error::Report)?;
        bytes += e.retained_bytes(self.report).map_err(Error::Report)?;
        // Include options retained by the callback worker, not only the sample.
        for r in &c.options.type_map {
            bytes = bytes.saturating_add(
                self.event(
                    r.head.clone(),
                    Part::atom("value"),
                    report::Extra::Undefined,
                )?
                .retained_bytes(self.report)
                .map_err(Error::Report)?,
            );
            bytes = bytes.saturating_add(self.atom(&r.kind)?);
        }
        let mut names = std::collections::BTreeSet::new();
        for i in &c.intervals {
            if !names.insert(&i.name) {
                return Err(Error::Configuration);
            }
            bytes = bytes.saturating_add(self.named(i)?);
        }
        if bytes > self.pending_bytes {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
    fn batch(self, samples: &[Sample], extra: &report::Extra) -> Result<usize, Error> {
        if samples.len() > self.subscriptions {
            return Err(Error::Limit);
        }
        let mut bytes = 64usize.saturating_add(extra.size(self.report).map_err(Error::Report)?);
        for sample in samples {
            if sample.values.len() > self.report.parts {
                return Err(Error::Limit);
            }
            bytes = bytes.saturating_add(self.report.name(&sample.metric).map_err(Error::Report)?);
            for (dp, value) in &sample.values {
                bytes = bytes
                    .saturating_add(self.report.part(dp).map_err(Error::Report)?)
                    .saturating_add(value.size(self.report).map_err(Error::Report)?);
                if bytes > self.callback_bytes {
                    return Err(Error::Limit);
                }
            }
        }
        if bytes > self.callback_bytes {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests;
