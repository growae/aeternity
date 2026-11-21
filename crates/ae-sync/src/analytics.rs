use ae_types::{PeerPublicKey, Unsigned};
use std::{collections::BTreeMap, time::Duration};

mod state;
mod worker;
pub use state::{Engine, Snapshot, Value};
pub use worker::{Client, Info, Ticket, Worker};

pub const VERSION_AGE_SECONDS: i64 = 3600;
pub const SWEEP_AGE_SECONDS: i64 = 86400;
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(86700);
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
pub const NATIVE_CONNECTION_LIMIT: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Host {
    Binary(Vec<u8>),
    List(Vec<i64>),
}
impl Host {
    fn bytes(&self) -> usize {
        match self {
            Self::Binary(b) => b.len(),
            Self::List(l) => l.len().saturating_mul(8),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub network_id: Vec<u8>,
    pub version: Vec<u8>,
    pub os: Vec<u8>,
    pub revision: Vec<u8>,
    pub vendor: Vec<u8>,
}
impl NodeInfo {
    fn fields(&self) -> [&Vec<u8>; 5] {
        [
            &self.network_id,
            &self.version,
            &self.os,
            &self.revision,
            &self.vendor,
        ]
    }
    fn bytes(&self) -> usize {
        128 + self.fields().iter().map(|b| b.len()).sum::<usize>()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PeerInfo {
    #[default]
    Unresolved,
    Unknown,
    Resolved(NodeInfo),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Announcement {
    pub peer: PeerPublicKey,
    pub host: Host,
    pub port: Unsigned,
    pub genesis: Vec<u8>,
    pub top: Vec<u8>,
    pub difficulty: Unsigned,
    pub temporary: bool,
}
impl Announcement {
    fn bytes(&self) -> usize {
        256 + self.host.bytes()
            + self.port.as_bytes().len()
            + self.genesis.len()
            + self.top.len()
            + self.difficulty.as_bytes().len()
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stat {
    pub host: Host,
    pub port: Unsigned,
    pub first_seen: i64,
    pub last_seen: i64,
    pub genesis: Vec<u8>,
    pub top: Vec<u8>,
    pub difficulty: Unsigned,
    pub info: PeerInfo,
}
impl Stat {
    fn bytes(&self) -> usize {
        256 + self.host.bytes()
            + self.port.as_bytes().len()
            + self.genesis.len()
            + self.top.len()
            + self.difficulty.as_bytes().len()
            + match &self.info {
                PeerInfo::Resolved(i) => i.bytes(),
                _ => 0,
            }
    }
    fn age(&self) -> i128 {
        i128::from(self.last_seen) - i128::from(self.first_seen)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Probe {
    pub peer: PeerPublicKey,
    id: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Recorded {
    pub probe: Option<Probe>,
    pub deferred: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeResult {
    NoConnection,
    Unknown,
    Resolved(NodeInfo),
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub peers: usize,
    pub probes: usize,
    pub requests: usize,
    pub request_bytes: usize,
    pub retained_bytes: usize,
    pub field_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            peers: 4096,
            probes: 64,
            requests: 128,
            request_bytes: 1 << 20,
            retained_bytes: 16 << 20,
            field_bytes: 4096,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), Error> {
        if self.peers == 0
            || self.peers > 1_000_000
            || self.probes == 0
            || self.probes > 4096
            || self.requests == 0
            || self.requests > 65536
            || self.request_bytes == 0
            || self.request_bytes > 1 << 30
            || self.retained_bytes == 0
            || self.retained_bytes > 1 << 30
            || self.field_bytes == 0
            || self.field_bytes > 1 << 20
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
    fn announcement(self, a: &Announcement) -> Result<(), Error> {
        if [
            a.host.bytes(),
            a.port.as_bytes().len(),
            a.genesis.len(),
            a.top.len(),
            a.difficulty.as_bytes().len(),
        ]
        .into_iter()
        .any(|n| n > self.field_bytes)
        {
            return Err(Error::Limit);
        }
        Ok(())
    }
    fn info(self, info: &NodeInfo) -> Result<(), Error> {
        if info.fields().iter().any(|b| b.len() > self.field_bytes) {
            Err(Error::Limit)
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct Config {
    pub enabled: bool,
    pub limits: Limits,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Limit,
    Busy,
    Closed,
    Missing,
    Stale,
    Sequence,
    Clock,
    Spawn,
    Panicked,
    Disconnected,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[cfg(test)]
mod tests;

// Native analytics changes direction limits without reserving capacity. The
// separately configured total connection/transport/queue quotas still apply.
pub fn configure_network(config: &mut ae_p2p::registry::Config, enabled: bool) {
    config.analytics = enabled;
    if enabled {
        config.maximum_inbound = NATIVE_CONNECTION_LIMIT;
        config.maximum_outbound = NATIVE_CONNECTION_LIMIT;
    }
}
