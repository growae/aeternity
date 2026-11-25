use num_bigint::BigInt;
use std::collections::{BTreeMap, BTreeSet};

mod rules;
mod state;
mod worker;
pub use worker::{Client, Info, Owner, Store, Ticket, Worker};
pub type Destinations = Option<Vec<String>>;
pub type Routes = Vec<Destinations>;
pub use state::Engine;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Part {
    Integer(BigInt),
    Atom(String),
    List(Vec<u32>),
    Binary(Vec<u8>),
}
impl Part {
    pub fn atom(value: impl Into<String>) -> Self {
        Self::Atom(value.into())
    }
    fn bytes(&self) -> usize {
        32 + match self {
            Self::Integer(n) => n.to_signed_bytes_be().len(),
            Self::Atom(s) => s.len(),
            Self::List(s) => s.len().saturating_mul(4),
            Self::Binary(s) => s.len(),
        }
    }
}
pub type Name = Vec<Part>;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: Name,
    pub kind: String,
    pub enabled: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub name: Vec<u8>,
    pub kind: Vec<u8>,
    pub datapoints: Vec<u8>,
    pub actions: Vec<u8>,
}
impl Default for Rule {
    fn default() -> Self {
        Self {
            name: b"*".to_vec(),
            kind: b"*".to_vec(),
            datapoints: b"default".to_vec(),
            actions: b"log,send".to_vec(),
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub requests: usize,
    pub pending_bytes: usize,
    pub metrics: usize,
    pub cells: usize,
    pub rules: usize,
    pub atoms: usize,
    pub bytes: usize,
    pub text: usize,
    pub parts: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            requests: 128,
            pending_bytes: 1 << 20,
            metrics: 4096,
            cells: 65_536,
            rules: 256,
            atoms: 16_384,
            bytes: 16 << 20,
            text: 4096,
            parts: 64,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), Error> {
        if self.requests == 0
            || self.requests > 65_536
            || self.pending_bytes == 0
            || self.pending_bytes > 1 << 30
            || self.metrics == 0
            || self.metrics > 65_536
            || self.cells == 0
            || self.cells > 1_000_000
            || self.rules == 0
            || self.rules > 4096
            || self.atoms == 0
            || self.atoms > 1_000_000
            || self.bytes == 0
            || self.bytes > 1 << 30
            || self.text == 0
            || self.text > 65_536
            || self.parts == 0
            || self.parts > 256
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
    fn name(self, name: &Name) -> Result<(), Error> {
        if name.len() > self.parts
            || name.iter().any(|p| match p {
                Part::Atom(a) => a.chars().count() > 255,
                Part::Integer(n) => n.bits() > self.text as u64 * 8,
                Part::List(s) => s.len() > self.text,
                Part::Binary(s) => s.len() > self.text,
            })
        {
            Err(Error::Limit)
        } else {
            Ok(())
        }
    }
    fn rule(self, r: &Rule) -> Result<(), Error> {
        if [&r.name, &r.kind, &r.datapoints, &r.actions]
            .iter()
            .any(|s| s.len() > self.text)
        {
            Err(Error::Limit)
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct Config {
    pub limits: Limits,
    pub rules: Vec<Rule>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Limit,
    InvalidRule,
    AlreadyRunning,
    Closed,
    Busy,
    Stopped,
    Disconnected,
    Panicked,
    Spawn,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Destination {
    Log,
    Send,
}
impl Destination {
    fn atom(self) -> String {
        match self {
            Self::Log => "log",
            Self::Send => "send",
        }
        .into()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub name: Name,
    pub datapoint: Part,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    pub entries: Vec<Entry>,
    pub datapoints: Option<Vec<Part>>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cell {
    pub key: Key,
    pub destinations: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct Setup {
    pub config: Config,
    pub entries: Vec<Entry>,
    pub atoms: BTreeSet<String>,
}
impl Default for Setup {
    fn default() -> Self {
        Self {
            config: Config::default(),
            entries: vec![],
            atoms: standard_atoms(),
        }
    }
}
pub fn standard_atoms() -> BTreeSet<String> {
    [
        "ae",
        "epoch",
        "default",
        "value",
        "log",
        "send",
        "none",
        "enabled",
        "disabled",
        "gauge",
        "counter",
        "histogram",
        "timer",
        "meter",
        "set",
        "_",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[cfg(test)]
mod tests;
