// User-file configuration boundary, aeu_env.erl at fa2a6d7.
// Validation does not imply that a runtime service has applied a setting.
use serde_json::{Map, Value};
use std::{collections::BTreeMap, fmt};

pub mod auxiliary;
pub mod chain;
pub mod consensus;
pub mod discovery;
pub mod execution;
mod files;
pub mod network;
pub mod presets;
pub mod protocols;
pub mod resolution;
pub mod runtime;
mod schema;
mod yaml;
pub use files::{Discovery, Source};

pub const MAX_BYTES: usize = 16 << 20;
const MAX_DEPTH: usize = 64;
const MAX_NODES: usize = 100_000;
const MAX_NUMBER: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    Yaml,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Format,
    Encoding,
    Parse,
    Schema { path: String },
    Environment { name: String },
    Limit(&'static str),
    Io(std::io::ErrorKind),
    Unsupported(&'static str),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "configuration error: {self:?}")
    }
}
impl std::error::Error for Error {}

// No Debug dump: configuration can contain passwords and signing keys.
#[derive(Clone, PartialEq)]
pub struct Configuration {
    value: Value,
}
impl fmt::Debug for Configuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Configuration { .. }")
    }
}
impl Configuration {
    pub fn empty() -> Self {
        Self {
            value: Value::Object(Map::new()),
        }
    }
    pub fn parse(bytes: &[u8], format: Format) -> Result<Self, Error> {
        if bytes.len() > MAX_BYTES {
            return Err(Error::Limit("input bytes"));
        }
        let value = match format {
            Format::Json => json(bytes)?,
            Format::Yaml => yaml::parse(bytes)?,
        };
        Self::from_value(value)
    }
    pub fn from_value(mut value: Value) -> Result<Self, Error> {
        budget(&value)?;
        normalize_numbers(&mut value)?;
        schema::validate(&value)?;
        Ok(Self { value })
    }
    // Raw stored map. setup's application-specific variable expansion is a
    // separate runtime boundary; strings containing '$' stay literal here.
    pub fn user_map(&self) -> &Value {
        &self.value
    }
    pub fn get(&self, path: &[&str]) -> Option<&Value> {
        path.iter().try_fold(&self.value, |v, key| v.get(*key))
    }
    pub fn get_or_schema_default(&self, path: &[&str]) -> Option<&Value> {
        self.get(path).or_else(|| schema_default(path))
    }
    // Input is an explicit environment snapshot. Pure configuration operations
    // never read or modify the process environment.
    pub fn with_environment(&self, env: &BTreeMap<String, String>) -> Result<Self, Error> {
        let mut names = BTreeMap::new();
        schema::names(schema::original(), "AE", &[], &mut names);
        let mut patch = Value::Object(Map::new());
        let mut bytes = 0usize;
        for (name, (path, kind)) in names {
            let Some(text) = env.get(&name) else { continue };
            bytes = bytes
                .checked_add(text.len())
                .filter(|n| *n <= MAX_BYTES)
                .ok_or(Error::Limit("environment bytes"))?;
            let bad = || Error::Environment { name: name.clone() };
            let value = match kind.as_str() {
                "string" => Value::String(text.clone()),
                "boolean" => match text.as_str() {
                    "true" => Value::Bool(true),
                    "false" => Value::Bool(false),
                    _ => return Err(bad()),
                },
                "integer" => {
                    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
                    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
                        return Err(bad());
                    }
                    yaml::integer(text, 10)?.ok_or_else(bad)?
                }
                "object" | "array" => json(text.as_bytes()).map_err(|_| bad())?,
                // Native coerce_type has no number clause. Do not silently add
                // one: callers can use the parent object's JSON override.
                _ => return Err(bad()),
            };
            let nested = path.iter().rev().fold(value, |v, key| {
                Value::Object(Map::from_iter([(key.clone(), v)]))
            });
            merge(&mut patch, nested)?;
            budget(&patch)?;
        }
        schema::validate(&patch)?;
        let mut result = self.value.clone();
        merge(&mut result, patch)?;
        Self::from_value(result)
    }
}

pub fn schema_default(path: &[&str]) -> Option<&'static Value> {
    let mut node = schema::original();
    for key in path {
        node = node.get("properties")?.get(*key)?;
    }
    node.get("default")
}
fn merge(base: &mut Value, patch: Value) -> Result<(), Error> {
    match (base, patch) {
        (Value::Object(base), Value::Object(patch)) => {
            for (key, value) in patch {
                if let Some(old) = base.get_mut(&key) {
                    if old.is_object() {
                        merge(old, value)?;
                    } else {
                        *old = value;
                    }
                } else {
                    base.insert(key, value);
                }
            }
            Ok(())
        }
        _ => Err(Error::Schema {
            path: String::new(),
        }),
    }
}
fn json(bytes: &[u8]) -> Result<Value, Error> {
    if bytes.len() > MAX_BYTES {
        return Err(Error::Limit("input bytes"));
    }
    let mut value = serde_json::from_slice(bytes).map_err(|_| Error::Parse)?;
    budget(&value)?;
    normalize_numbers(&mut value)?;
    Ok(value)
}
fn normalize_numbers(value: &mut Value) -> Result<(), Error> {
    match value {
        Value::Number(n) => {
            let s = n.as_str();
            if s.len() > MAX_NUMBER {
                return Err(Error::Limit("number digits"));
            }
            if s.contains(['.', 'e', 'E']) {
                let f: f64 = s.parse().map_err(|_| Error::Parse)?;
                *n = serde_json::Number::from_f64(f).ok_or(Error::Parse)?;
            }
        }
        Value::Array(a) => {
            for v in a {
                normalize_numbers(v)?;
            }
        }
        Value::Object(m) => {
            for v in m.values_mut() {
                normalize_numbers(v)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn budget(value: &Value) -> Result<(usize, usize, usize), Error> {
    fn walk(
        v: &Value,
        depth: usize,
        nodes: &mut usize,
        bytes: &mut usize,
        max: &mut usize,
    ) -> Result<(), Error> {
        if depth > MAX_DEPTH {
            return Err(Error::Limit("depth"));
        }
        *max = (*max).max(depth);
        *nodes += 1;
        *bytes += match v {
            Value::String(s) => s.len(),
            Value::Number(n) => n.as_str().len(),
            _ => 8,
        };
        if *nodes > MAX_NODES || *bytes > MAX_BYTES {
            return Err(Error::Limit("expanded value"));
        }
        match v {
            Value::Array(a) => {
                for v in a {
                    walk(v, depth + 1, nodes, bytes, max)?;
                }
            }
            Value::Object(m) => {
                for (k, v) in m {
                    *bytes += k.len();
                    walk(v, depth + 1, nodes, bytes, max)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let (mut nodes, mut bytes, mut depth) = (0, 0, 0);
    walk(value, 0, &mut nodes, &mut bytes, &mut depth)?;
    Ok((nodes, bytes, depth))
}
