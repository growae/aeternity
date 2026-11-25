use super::*;
use num_bigint::BigInt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Counter,
    Gauge,
}
impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
        }
    }
}

// Concrete retained gauge values. Conversion to the reporter's nonnumeric
// sentinel happens only at the output boundary, never during registry updates.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(BigInt),
    Float(f64),
    Atom(String),
    Binary(Vec<u8>),
    List(Vec<Value>),
    Tuple(Vec<Value>),
}
impl Value {
    pub(super) fn report(&self) -> report::Value {
        match self {
            Self::Integer(n) => report::Value::Integer(n.clone()),
            Self::Float(n) => report::Value::Float(*n),
            _ => report::Value::Other,
        }
    }
    pub(super) fn bytes(&self, limits: report::Limits) -> Result<usize, Error> {
        fn visit(
            v: &Value,
            l: report::Limits,
            depth: usize,
            nodes: &mut usize,
        ) -> Result<usize, Error> {
            *nodes += 1;
            if depth > 32 || *nodes > l.bytes / 32 {
                return Err(Error::Limit);
            }
            let size = match v {
                Value::Integer(n) if n.bits() <= l.integer_bits => {
                    32 + n.to_signed_bytes_le().len()
                }
                Value::Integer(_) => return Err(Error::Limit),
                Value::Float(n) if !n.is_finite() => return Err(Error::InvalidValue),
                Value::Float(_) => 32,
                Value::Atom(s) if s.chars().count() <= 255 => 32usize.saturating_add(s.len()),
                Value::Atom(_) => return Err(Error::Limit),
                Value::Binary(b) => 32usize.saturating_add(b.len()),
                Value::List(v) | Value::Tuple(v) => {
                    if v.len() > l.bytes / 32 {
                        return Err(Error::Limit);
                    }
                    v.iter().try_fold(32usize, |n, v| {
                        Ok(n.saturating_add(visit(v, l, depth + 1, nodes)?))
                    })?
                }
            };
            if size > l.bytes {
                Err(Error::Limit)
            } else {
                Ok(size)
            }
        }
        visit(self, limits, 0, &mut 0)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    NotFound,
    Exists,
    TypeMismatch,
    SampledEntry,
    Disabled,
    InvalidStatus,
    InvalidValue,
    InvalidAlias,
    Limit,
    Sequence,
    Closed,
    Busy,
    Stale,
    Spawn,
    Panicked,
    AlreadyRunning,
    RestartLimit,
    Arithmetic,
}
#[derive(Debug, Clone, PartialEq)]
pub enum Reading {
    Disabled,
    Values(Vec<(Part, Value)>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub entry: Entry,
    pub timestamp_ms: Option<i128>,
    pub cache_ms: Option<u64>,
    pub datapoints: Option<Vec<Part>>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Alias {
    Atom(String),
    Binary(Vec<u8>),
}
impl Alias {
    pub(super) fn key(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        match self {
            Self::Atom(a) => {
                limits.atom(a).map_err(|_| Error::Limit)?;
                a.chars()
                    .map(|c| u8::try_from(c as u32).map_err(|_| Error::InvalidAlias))
                    .collect()
            }
            Self::Binary(b) if b.len() <= limits.report.bytes => Ok(b.clone()),
            Self::Binary(_) => Err(Error::Limit),
        }
    }
}
pub(super) struct Mapping {
    name: Name,
    point: Part,
    bytes: usize,
}
pub(super) struct Scalar {
    pub(super) kind: Kind,
    pub(super) value: Value,
    timestamp: i128,
}
pub(super) fn defaults() -> Vec<Part> {
    vec![Part::atom("value"), Part::atom("ms_since_reset")]
}
impl Scalar {
    pub(super) fn values(
        &self,
        points: &Points,
        now: i128,
        limits: Limits,
    ) -> Result<Vec<(Part, Value)>, Error> {
        let default;
        let requested = if *points == Points::default_points() {
            default = defaults();
            &default[..]
        } else {
            match points {
                Points::One(p) => std::slice::from_ref(p),
                Points::Many(v) => v,
            }
        };
        let mut bytes = 32usize;
        requested
            .iter()
            .map(|p| {
                let v = match p {
                    Part::Atom(s) if s == "value" => self.value.clone(),
                    Part::Atom(s) if s == "ms_since_reset" => {
                        Value::Integer(BigInt::from(now) - BigInt::from(self.timestamp))
                    }
                    Part::Atom(s) if self.kind == Kind::Counter => {
                        let bits = match s.as_str() {
                            "value16" => 16,
                            "value32" => 32,
                            "value64" => 64,
                            _ => 0,
                        };
                        if bits == 0 {
                            Value::Atom("undefined".into())
                        } else {
                            let Value::Integer(n) = &self.value else {
                                unreachable!("counter accepts only integers")
                            };
                            Value::Integer(n % ((BigInt::from(1) << bits) - 1))
                        }
                    }
                    _ => Value::Atom("undefined".into()),
                };
                bytes = bytes
                    .saturating_add(limits.report.part(p).map_err(|_| Error::Limit)?)
                    .saturating_add(v.bytes(limits.report)?);
                if bytes > limits.retained_bytes {
                    return Err(Error::Limit);
                }
                Ok((p.clone(), v))
            })
            .collect()
    }
}
impl Store {
    pub(super) fn timestamp(&self) -> i128 {
        // exometer_util:timestamp/0 measures milliseconds from mega-second 1258.
        self.clock.now().unix_microseconds.div_euclid(1000) - 1_258_000_000_000
    }
    pub(super) fn scalar_name(&self, name: &Name) -> Result<usize, Error> {
        self.limits.report.name(name).map_err(|_| Error::Limit)
    }
    pub(super) fn scalar_points(&self, points: &Points) -> Result<(), Error> {
        self.limits.points(points).map_err(|_| Error::Limit)
    }
    pub fn create(&self, name: &Name, kind: Kind, enabled: bool) -> Result<(), Error> {
        self.register(name, kind, enabled, false)
    }
    pub fn ensure(&self, name: &Name, kind: Kind, enabled: bool) -> Result<(), Error> {
        self.register(name, kind, enabled, true)
    }
    fn register(&self, name: &Name, kind: Kind, enabled: bool, ensure: bool) -> Result<(), Error> {
        let _control = self.control();
        let bytes = 256usize
            .saturating_add(self.scalar_name(name)?.saturating_mul(2))
            .saturating_add(Value::Integer(0.into()).bytes(self.limits.report)?);
        let now = self.timestamp();
        let mut s = self.lock();
        s.open()?;
        if let Some(old) = s.entries.get(name) {
            return if !ensure {
                Err(Error::Exists)
            } else if old.metric.entry.kind != kind.name() {
                Err(Error::TypeMismatch)
            } else {
                Ok(())
            };
        }
        if s.entries.len() >= self.limits.subscriptions
            || s.bytes.saturating_add(bytes) > self.limits.retained_bytes
        {
            return Err(Error::Limit);
        }
        let version = s.revision.checked_add(1).ok_or(Error::Sequence)?;
        s.entries.insert(
            name.clone(),
            Stored {
                histogram: None,
                read_epoch: 0,
                metric: Metric {
                    entry: Entry {
                        name: name.clone(),
                        kind: kind.name().into(),
                        enabled,
                    },
                    values: vec![],
                },
                scalar: Some(Scalar {
                    kind,
                    value: Value::Integer(0.into()),
                    timestamp: now,
                }),
                version,
                bytes,
                cache_ms: 0,
            },
        );
        s.bytes += bytes;
        s.revision = version;
        for signal in &s.signals {
            signal.notify();
        }
        Ok(())
    }
    pub fn update(&self, name: &Name, value: &Value) -> Result<(), Error> {
        self.scalar_name(name)?;
        let mut s = self.lock();
        s.open()?;
        // The original global gate precedes lookup, even for absent entries.
        if !s.enabled {
            return Ok(());
        }
        let stored = s.entries.get(name).ok_or(Error::NotFound)?;
        if !stored.metric.entry.enabled {
            return Ok(());
        }
        if let Some(histogram) = &stored.histogram {
            drop(histogram.client()?.update(value)?);
            return Ok(());
        }
        let scalar = stored.scalar.as_ref().ok_or(Error::SampledEntry)?;
        value.bytes(self.limits.report)?;
        let value = match (&scalar.kind, &scalar.value, value) {
            (Kind::Counter, Value::Integer(old), Value::Integer(delta)) => {
                Value::Integer(old + delta)
            }
            (Kind::Counter, _, _) => return Err(Error::InvalidValue),
            (Kind::Gauge, _, v) => v.clone(),
        };
        let bytes = stored.bytes - scalar.value.bytes(self.limits.report)?
            + value.bytes(self.limits.report)?;
        let total = s.bytes - stored.bytes + bytes;
        if total > self.limits.retained_bytes {
            return Err(Error::Limit);
        }
        let stored = s.entries.get_mut(name).expect("entry locked");
        stored.scalar.as_mut().expect("scalar checked").value = value;
        stored.bytes = bytes;
        s.bytes = total;
        Ok(())
    }
    pub fn reset(&self, name: &Name) -> Result<(), Error> {
        self.scalar_name(name)?;
        let now = self.timestamp();
        let mut s = self.lock();
        s.open()?;
        if !s.enabled {
            return Ok(());
        }
        let stored = s.entries.get(name).ok_or(Error::NotFound)?;
        if !stored.metric.entry.enabled {
            return Ok(());
        }
        if let Some(histogram) = &stored.histogram {
            let client = histogram.client()?;
            let epoch = stored.read_epoch.checked_add(1).ok_or(Error::Sequence)?;
            drop(client.reset()?);
            s.entries.get_mut(name).expect("entry locked").read_epoch = epoch;
            for point in histogram::datapoints() {
                s.clear_cache_point(name, &point);
            }
            return Ok(());
        }
        let scalar = stored.scalar.as_ref().ok_or(Error::SampledEntry)?;
        let old = scalar.value.bytes(self.limits.report)?;
        let value = Value::Integer(0.into());
        let bytes = value.bytes(self.limits.report)?;
        if s.bytes - old + bytes > self.limits.retained_bytes {
            return Err(Error::Limit);
        }
        let stored = s.entries.get_mut(name).expect("entry locked");
        let scalar = stored.scalar.as_mut().expect("scalar checked");
        scalar.value = value;
        scalar.timestamp = now;
        stored.bytes = stored.bytes - old + bytes;
        s.bytes = s.bytes - old + bytes;
        Ok(())
    }
    pub fn get_value(&self, name: &Name, points: &Points) -> Result<Reading, Error> {
        self.scalar_name(name)?;
        self.scalar_points(points)?;
        let time = self.clock.now();
        let mut s = self.lock();
        s.open()?;
        if s.entries.get(name).is_some_and(|e| e.histogram.is_some()) {
            drop(s);
            return self.histogram_read(name, points, time);
        }
        self.scalar_read(&mut s, name, points, time)
    }
    pub fn info(&self, name: &Name) -> Result<Info, Error> {
        self.scalar_name(name)?;
        let s = self.lock();
        s.open()?;
        let stored = s.entries.get(name).ok_or(Error::NotFound)?;
        let (timestamp_ms, datapoints) = if let Some(scalar) = &stored.scalar {
            (Some(scalar.timestamp), defaults())
        } else if stored.histogram.is_some() {
            (None, histogram::datapoints())
        } else {
            return Err(Error::SampledEntry);
        };
        Ok(Info {
            entry: stored.metric.entry.clone(),
            timestamp_ms: timestamp_ms.filter(|_| stored.metric.entry.enabled),
            cache_ms: stored.metric.entry.enabled.then_some(stored.cache_ms),
            datapoints: stored.metric.entry.enabled.then_some(datapoints),
        })
    }
    pub fn set_enabled(&self, name: &Name, enabled: bool) -> Result<(), Error> {
        let _control = self.control();
        self.scalar_name(name)?;
        let mut s = self.lock();
        s.open()?;
        let stored = s.entries.get(name).ok_or(Error::NotFound)?;
        if stored.histogram.is_some() {
            drop(s);
            return self.histogram_settings_locked(
                name,
                HistogramSettings {
                    enabled: Some(enabled),
                    ..Default::default()
                },
            );
        }
        stored.scalar.as_ref().ok_or(Error::SampledEntry)?;
        if stored.metric.entry.enabled == enabled {
            // exometer:setopts has a case_clause for already-enabled scalars;
            // disabled scalars require an explicit transition back to enabled.
            return Err(if enabled {
                Error::InvalidStatus
            } else {
                Error::Disabled
            });
        }
        let version = s.revision.checked_add(1).ok_or(Error::Sequence)?;
        let stored = s.entries.get_mut(name).expect("entry locked");
        stored.metric.entry.enabled = enabled;
        stored.version = version;
        s.revision = version;
        for signal in &s.signals {
            signal.notify();
        }
        Ok(())
    }
    pub fn set_global_enabled(&self, enabled: bool) -> Result<bool, Error> {
        let mut s = self.lock();
        s.open()?;
        Ok(std::mem::replace(&mut s.enabled, enabled))
    }
    pub fn retained_bytes(&self) -> usize {
        self.lock().bytes
    }
    pub fn find_entries(&self, prefix: &Name) -> Result<Vec<Entry>, Error> {
        self.scalar_name(prefix)?;
        let s = self.lock();
        s.open()?;
        Ok(s.entries
            .iter()
            .filter(|(n, _)| n.starts_with(prefix))
            .map(|(_, e)| e.metric.entry.clone())
            .collect())
    }

    pub fn aggregate(&self, prefix: &Name, points: &Points) -> Result<Vec<(Part, BigInt)>, Error> {
        self.scalar_name(prefix)?;
        self.scalar_points(points)?;
        let samples = self
            .read(&Selector::Find(prefix.clone()), points)
            .map_err(|e| {
                if e == StoreError::Closed {
                    Error::Closed
                } else {
                    Error::Limit
                }
            })?;
        let mut sums = BTreeMap::<Part, BigInt>::new();
        for sample in samples {
            for (p, v) in sample.values {
                if let report::Value::Integer(n) = v {
                    let sum = sums.entry(p).or_default();
                    *sum += n;
                    if sum.bits() > self.limits.report.integer_bits {
                        return Err(Error::Limit);
                    }
                }
            }
        }
        Ok(sums.into_iter().collect())
    }
    pub fn add_alias(&self, alias: &Alias, name: &Name, point: &Part) -> Result<(), Error> {
        if !matches!(point, Part::Atom(_) | Part::Integer(_)) {
            return Err(Error::InvalidAlias);
        }
        let key = alias.key(self.limits)?;
        let bytes = 96usize
            .saturating_add(key.len())
            .saturating_add(self.scalar_name(name)?)
            .saturating_add(self.limits.report.part(point).map_err(|_| Error::Limit)?);
        let mut s = self.lock();
        s.open()?;
        if s.aliases.contains_key(&key) {
            return Err(Error::Exists);
        }
        if s.aliases.len() >= self.limits.subscriptions
            || s.bytes.saturating_add(bytes) > self.limits.retained_bytes
        {
            return Err(Error::Limit);
        }
        s.aliases.insert(
            key,
            Mapping {
                name: name.clone(),
                point: point.clone(),
                bytes,
            },
        );
        s.bytes += bytes;
        Ok(())
    }
    pub fn resolve_alias(&self, alias: &Alias) -> Result<Option<(Name, Part)>, Error> {
        let key = alias.key(self.limits)?;
        let s = self.lock();
        s.open()?;
        Ok(s.aliases
            .get(&key)
            .map(|m| (m.name.clone(), m.point.clone())))
    }

    pub fn delete_alias(&self, alias: &Alias) -> Result<(), Error> {
        let key = alias.key(self.limits)?;
        let mut s = self.lock();
        s.open()?;
        if let Some(m) = s.aliases.remove(&key) {
            s.bytes -= m.bytes;
        }
        Ok(())
    }
    pub fn alias_value(&self, alias: &Alias) -> Result<Reading, Error> {
        let (name, point) = self.resolve_alias(alias)?.ok_or(Error::NotFound)?;
        self.get_value(&name, &Points::Many(vec![point]))
    }
    pub fn update_alias(&self, alias: &Alias, value: &Value) -> Result<(), Error> {
        let (name, _) = self.resolve_alias(alias)?.ok_or(Error::NotFound)?;
        self.update(&name, value)
    }
}
