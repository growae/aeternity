use ae_types::PeerPublicKey;
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng};
use sha1::{Digest, Sha1};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{Ipv4Addr, SocketAddrV4},
};
use zeroize::Zeroizing;
mod buckets;
mod pool;
pub use pool::Pool;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    PeerLimit,
    Missing,
    Selected,
    TimeReversed,
    TimeOverflow,
    Entropy,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub id: PeerPublicKey,
    pub address: Ipv4Addr,
    pub source: Ipv4Addr,
    pub host: Vec<u8>,
    pub port: u16,
    pub trusted: bool,
}
impl Peer {
    pub fn socket(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.address, self.port)
    }
}
pub fn address_group(address: Ipv4Addr) -> [u8; 2] {
    let [a, b, _, _] = address.octets();
    [a, b]
}

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub verified_buckets: usize,
    pub verified_size: usize,
    pub verified_group_shard: usize,
    pub unverified_buckets: usize,
    pub unverified_size: usize,
    pub unverified_source_shard: usize,
    pub unverified_group_shard: usize,
    pub maximum_references: usize,
    pub eviction_skew: f64,
    pub verified_probability: f64,
    pub maximum_update_lapse: u64,
    pub standby_times: Vec<u64>,
    pub maximum_rejections: u64,
    pub maximum_host_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            verified_buckets: 256,
            verified_size: 32,
            verified_group_shard: 8,
            unverified_buckets: 1024,
            unverified_size: 64,
            unverified_source_shard: 64,
            unverified_group_shard: 4,
            maximum_references: 8,
            eviction_skew: 1.2,
            verified_probability: 1.0,
            maximum_update_lapse: 3 * 60 * 60 * 1000,
            standby_times: vec![5000, 15000, 30000, 60000, 120000, 300000, 600000],
            maximum_rejections: 7,
            maximum_host_bytes: 1024,
        }
    }
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        for n in [self.verified_buckets, self.unverified_buckets] {
            if n == 0 || n > 65_536 {
                return Err(Error::Configuration);
            }
        }
        for n in [self.verified_size, self.unverified_size] {
            if n == 0 || n > 1024 {
                return Err(Error::Configuration);
            }
        }
        if self.verified_buckets * self.verified_size
            + self.unverified_buckets * self.unverified_size
            > 1_000_000
            || self.verified_group_shard == 0
            || self.verified_group_shard > 65_536
            || self.unverified_group_shard == 0
            || self.unverified_group_shard > 65_536
            || self.unverified_source_shard == 0
            || !self
                .unverified_source_shard
                .is_multiple_of(self.unverified_group_shard)
            || self.unverified_source_shard / self.unverified_group_shard > 65_536
            || self.maximum_references == 0
            || self.maximum_references > 32
            || !self.eviction_skew.is_finite()
            || self.eviction_skew <= 0.0
            || !self.verified_probability.is_finite()
            || !(0.0..=1.0).contains(&self.verified_probability)
            || self.maximum_update_lapse == 0
            || self.maximum_rejections == 0
            || self.standby_times.is_empty()
            || self.standby_times.len() > 1024
            || self.maximum_host_bytes == 0
            || self.maximum_host_bytes > 65_536
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Verified,
    Unverified,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    Both,
    Verified,
    Unverified,
}
impl Target {
    fn accepts(self, class: Class) -> bool {
        matches!(
            (self, class),
            (Self::Both, _)
                | (Self::Verified, Class::Verified)
                | (Self::Unverified, Class::Unverified)
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Count {
    All,
    Available,
    Standby,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Update {
    Verified,
    Unverified,
    Ignored,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    Persist(Peer),
    Delete(Peer),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change<T> {
    pub result: T,
    pub effects: Vec<Effect>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    Selected(Peer),
    Wait(u64),
    Unavailable,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct State {
    pub class: Class,
    pub available: bool,
    pub selected: bool,
    pub standby: bool,
    pub rejections: u64,
    pub updated_at: u64,
    pub selected_at: Option<u64>,
    pub rejected_at: Option<u64>,
    pub references: usize,
}
#[derive(Clone)]
struct Entry {
    peer: Peer,
    verified: Option<usize>,
    unverified: Vec<usize>,
    selected: bool,
    rejected: u64,
    updated: u64,
    selected_at: Option<u64>,
    rejected_at: Option<u64>,
}
impl Entry {
    fn class(&self) -> Class {
        if self.verified.is_some() {
            Class::Verified
        } else {
            Class::Unverified
        }
    }
}

#[derive(Clone, Default)]
struct Lookup {
    ids: Vec<PeerPublicKey>,
    indices: BTreeMap<PeerPublicKey, usize>,
}
impl Lookup {
    fn insert(&mut self, id: PeerPublicKey, rng: &mut StdRng) {
        if self.indices.contains_key(&id) {
            return;
        }
        let end = self.ids.len();
        let index = rng.gen_range(0..=end);
        self.ids.push(id);
        self.ids.swap(index, end);
        self.indices.insert(self.ids[end], end);
        self.indices.insert(id, index);
    }
    fn remove(&mut self, id: PeerPublicKey) {
        if let Some(index) = self.indices.remove(&id) {
            self.ids.swap_remove(index);
            if let Some(moved) = self.ids.get(index) {
                self.indices.insert(*moved, index);
            }
        }
    }
}

// Native bucket hashing interprets SHA-1 as a little-endian 160-bit integer.
fn hash_modulo(parts: &[&[u8]], modulo: usize) -> usize {
    let mut h = Sha1::new();
    for part in parts {
        h.update(part);
    }
    h.finalize()
        .iter()
        .rev()
        .fold(0, |n, &byte| (n * 256 + usize::from(byte)) % modulo)
        + 1
}
