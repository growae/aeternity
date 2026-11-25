use super::metrics;
use crate::routing::{Key, Name, Part};
use num_bigint::BigInt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Limit,
    InvalidNumber,
    InvalidIolist,
    UnsupportedType,
    InvalidTag,
    InvalidExtra,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub bytes: usize,
    pub parts: usize,
    pub options: usize,
    pub tags: usize,
    pub type_rules: usize,
    pub integer_bits: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            bytes: 64 << 10,
            parts: 64,
            options: 64,
            tags: 64,
            type_rules: 256,
            integer_bits: 4096,
        }
    }
}
impl Limits {
    pub fn validate(self) -> Result<(), Error> {
        if self.bytes == 0
            || self.bytes > 1 << 20
            || self.parts == 0
            || self.parts > 256
            || self.options == 0
            || self.options > 4096
            || self.tags == 0
            || self.tags > 4096
            || self.type_rules == 0
            || self.type_rules > 4096
            || self.integer_bits == 0
            || self.integer_bits > 65_536
        {
            return Err(Error::Limit);
        }
        Ok(())
    }
    fn atom(self, s: &str) -> Result<usize, Error> {
        self.size(32usize.saturating_add(s.len()))?;
        if s.chars().count() > 255 {
            return Err(Error::Limit);
        }
        Ok(32 + s.len())
    }
    fn part(self, p: &Part) -> Result<usize, Error> {
        let n = match p {
            Part::Integer(n) if n.bits() <= self.integer_bits => 32 + n.to_signed_bytes_le().len(),
            Part::Integer(_) => return Err(Error::Limit),
            Part::Atom(s) => self.atom(s)?,
            Part::Binary(s) => 32usize.saturating_add(s.len()),
            Part::List(s) => 32usize.saturating_add(s.len().saturating_mul(4)),
        };
        self.size(n)
    }
    fn size(self, n: usize) -> Result<usize, Error> {
        if n > self.bytes {
            Err(Error::Limit)
        } else {
            Ok(n)
        }
    }
    fn name(self, name: &[Part]) -> Result<usize, Error> {
        if name.len() > self.parts {
            return Err(Error::Limit);
        }
        name.iter()
            .try_fold(32usize, |n, p| self.size(n.saturating_add(self.part(p)?)))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(BigInt),
    Float(f64),
    Other,
}
impl Value {
    fn size(&self, l: Limits) -> Result<usize, Error> {
        match self {
            Self::Integer(n) if n.bits() <= l.integer_bits => {
                l.size(32 + n.to_signed_bytes_le().len())
            }
            Self::Integer(_) => Err(Error::Limit),
            Self::Float(n) if !n.is_finite() => Err(Error::InvalidNumber),
            _ => Ok(32),
        }
    }
    fn bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(match self {
            Self::Integer(n) => n.to_string().into_bytes(),
            Self::Float(n) => metrics::Number::Float(*n)
                .text()
                .map_err(|_| Error::InvalidNumber)?
                .into_bytes(),
            // aec_metrics:value(_) returns integer zero in an iolist.
            Self::Other => vec![0],
        })
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum Tag {
    Text(Part),
    Pair(Value, Value),
    Invalid,
}
#[derive(Debug, Clone, PartialEq)]
pub enum OptionValue {
    ReportType(String),
    Tags(Vec<Tag>),
    NonListTags,
    Other,
}
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Extra {
    #[default]
    Undefined,
    List(Vec<OptionValue>),
    Other,
}
impl Extra {
    fn size(&self, l: Limits) -> Result<usize, Error> {
        let Self::List(options) = self else {
            return Ok(32);
        };
        if options.len() > l.options {
            return Err(Error::Limit);
        }
        options.iter().try_fold(32usize, |n, option| {
            let bytes = match option {
                OptionValue::ReportType(t) => l.atom(t)?,
                OptionValue::Tags(tags) => {
                    if tags.len() > l.tags {
                        return Err(Error::Limit);
                    }
                    tags.iter().try_fold(32usize, |n, tag| {
                        let bytes = match tag {
                            Tag::Text(p) => l.part(p)?,
                            Tag::Pair(a, b) => a.size(l)?.saturating_add(b.size(l)?),
                            Tag::Invalid => 32,
                        };
                        l.size(n.saturating_add(bytes))
                    })?
                }
                _ => 32,
            };
            l.size(n.saturating_add(bytes))
        })
    }
    fn report_type(&self) -> Option<&str> {
        let Self::List(options) = self else {
            return None;
        };
        options.iter().find_map(|o| {
            if let OptionValue::ReportType(t) = o {
                Some(t.as_str())
            } else {
                None
            }
        })
    }
}
#[derive(Debug, Clone)]
pub struct TypeRule {
    // An atom '_' in the head matches one element; tail matches any suffix.
    pub head: Name,
    pub tail: bool,
    pub kind: String,
}
impl TypeRule {
    fn matches(&self, key: &[Part]) -> bool {
        (self.tail || key.len() == self.head.len())
            && key.len() >= self.head.len()
            && self
                .head
                .iter()
                .zip(key)
                .all(|(p, k)| p == k || matches!(p,Part::Atom(a) if a=="_"))
    }
}
#[derive(Debug, Clone)]
pub struct Options {
    pub prefix: Part,
    pub type_map: Vec<TypeRule>,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            prefix: Part::List(vec![]),
            type_map: vec![],
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub key: Key,
    pub name: Name,
    pub value: Value,
    pub kind: String,
    pub extra: Extra,
}
impl Options {
    pub fn event(&self, key: Key, value: Value, extra: Extra, l: Limits) -> Result<Event, Error> {
        l.validate()?;
        if self.type_map.len() > l.type_rules {
            return Err(Error::Limit);
        }
        let mut bytes = l.part(&self.prefix)?;
        for r in &self.type_map {
            bytes = l.size(
                bytes
                    .saturating_add(l.name(&r.head)?)
                    .saturating_add(l.atom(&r.kind)?),
            )?;
        }
        let key_bytes = l.name(&key.name)?.saturating_add(l.part(&key.datapoint)?);
        l.size(
            bytes
                .saturating_add(key_bytes.saturating_mul(2))
                .saturating_add(value.size(l)?)
                .saturating_add(extra.size(l)?),
        )?;
        let mut full_key = key.name.clone();
        full_key.push(key.datapoint.clone());
        let kind = extra
            .report_type()
            .or_else(|| {
                self.type_map
                    .iter()
                    .find(|r| r.matches(&full_key))
                    .map(|r| r.kind.as_str())
            })
            .unwrap_or("gauge")
            .to_owned();
        let mut name = Vec::with_capacity(full_key.len() + 1);
        if !matches!(&self.prefix, Part::List(s) if s.is_empty()) {
            name.push(self.prefix.clone());
        }
        name.extend(full_key);
        let event = Event {
            key,
            name,
            value,
            kind,
            extra,
        };
        event.retained_bytes(l)?;
        Ok(event)
    }
}
impl Event {
    pub fn retained_bytes(&self, l: Limits) -> Result<usize, Error> {
        l.validate()?;
        l.size(
            128usize
                .saturating_add(l.name(&self.key.name)?)
                .saturating_add(l.part(&self.key.datapoint)?)
                .saturating_add(l.name(&self.name)?)
                .saturating_add(self.value.size(l)?)
                .saturating_add(l.atom(&self.kind)?)
                .saturating_add(self.extra.size(l)?),
        )
    }
    pub fn line(&self, l: Limits) -> Result<Vec<u8>, Error> {
        self.retained_bytes(l)?;
        let mut out = Vec::new();
        for (i, p) in self.name.iter().enumerate() {
            if i != 0 {
                out.push(b'.');
            }
            match p {
                Part::Integer(n) => out.extend(n.to_string().bytes()),
                Part::Binary(s) => out.extend(s),
                Part::Atom(s) => {
                    for c in s.chars() {
                        out.push(u8::try_from(c as u32).map_err(|_| Error::InvalidIolist)?);
                    }
                }
                Part::List(s) => {
                    for c in s {
                        out.push(u8::try_from(*c).map_err(|_| Error::InvalidIolist)?);
                    }
                }
            }
            l.size(out.len())?;
        }
        out.push(b':');
        out.extend(self.value.bytes()?);
        out.push(b'|');
        out.extend_from_slice(match self.kind.as_str() {
            "gauge" => b"g",
            "counter" => b"c",
            "timer" => b"ms",
            "histogram" => b"h",
            "meter" => b"m",
            "set" => b"s",
            _ => return Err(Error::UnsupportedType),
        });
        match &self.extra {
            Extra::Other => return Err(Error::InvalidExtra),
            Extra::Undefined => (),
            Extra::List(options) => {
                for option in options {
                    match option {
                        OptionValue::NonListTags => break,
                        OptionValue::Tags(tags) => {
                            if !tags.is_empty() {
                                out.extend_from_slice(b"|#");
                            }
                            for (i, t) in tags.iter().enumerate() {
                                if i != 0 {
                                    out.push(b',');
                                }
                                match t {
                                    Tag::Text(Part::Atom(_) | Part::Binary(_) | Part::List(_)) => {
                                        out.push(0)
                                    }
                                    Tag::Pair(a, b) => {
                                        out.extend(a.bytes()?);
                                        out.push(b':');
                                        out.extend(b.bytes()?);
                                    }
                                    _ => return Err(Error::InvalidTag),
                                }
                                l.size(out.len())?;
                            }
                            break;
                        }
                        _ => (),
                    }
                }
            }
        }
        l.size(out.len())?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests;
