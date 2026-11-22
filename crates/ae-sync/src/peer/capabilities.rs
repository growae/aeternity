use ae_p2p::message::ChainPoi;
use ae_types::PeerPublicKey;
use std::collections::BTreeMap;

pub mod worker;

pub const CHAIN_POI: &str = "chain_poi";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    ChainPoi(Vec<ChainPoi>),
    // Application-owned extension bytes round-trip locally. They are not an
    // Erlang term or an implicit wire schema: production pings reject them.
    Opaque(Vec<u8>),
}
impl Value {
    fn bytes(&self) -> Option<usize> {
        match self {
            Self::Opaque(bytes) => 64usize.checked_add(bytes.len()),
            Self::ChainPoi(records) => records
                .iter()
                .try_fold(64usize, |n, p| n.checked_add(poi_bytes(p)?)),
        }
    }
}
fn poi_bytes(p: &ChainPoi) -> Option<usize> {
    [
        128,
        p.height.as_bytes().len(),
        p.root_hash.len(),
        p.genesis.len(),
        p.top.len(),
        p.poi.len(),
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub local_entries: usize,
    pub peers: usize,
    pub entries_per_peer: usize,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub total_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            local_entries: 64,
            peers: 512,
            entries_per_peer: 64,
            key_bytes: 255,
            value_bytes: 1 << 20,
            total_bytes: 32 << 20,
        }
    }
}
impl Limits {
    pub fn validate(self) -> Result<(), Error> {
        if [
            self.local_entries,
            self.peers,
            self.entries_per_peer,
            self.key_bytes,
        ]
        .iter()
        .any(|n| *n == 0 || *n > 65_536)
            || self.value_bytes == 0
            || self.value_bytes > 1 << 30
            || self.total_bytes < 256
            || self.total_bytes > 1 << 30
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Capacity,
    Stale,
    Closed,
    UnsupportedKey(String),
    InvalidValue,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerValue<G> {
    pub peer: PeerPublicKey,
    pub generation: G,
    pub value: Value,
}
struct Peer<G> {
    generation: G,
    entries: BTreeMap<String, Value>,
    bytes: usize,
}

// Deterministic, bounded ownership. Only the connection admission owner may
// attach a replacement. Registers and removals must carry its exact generation.
pub struct Registry<G> {
    limits: Limits,
    local: BTreeMap<String, Value>,
    peers: BTreeMap<PeerPublicKey, Peer<G>>,
    bytes: usize,
    closed: bool,
}
impl<G: Clone + Eq> Registry<G> {
    pub fn new(limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        Ok(Self {
            limits,
            local: BTreeMap::new(),
            peers: BTreeMap::new(),
            bytes: 0,
            closed: false,
        })
    }
    fn open(&self) -> Result<(), Error> {
        if self.closed {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
    fn entry_bytes(&self, key: &str, value: &Value) -> Result<usize, Error> {
        self.entry_weight(key, value.bytes().ok_or(Error::Capacity)?)
    }
    fn entry_weight(&self, key: &str, bytes: usize) -> Result<usize, Error> {
        if key.len() > self.limits.key_bytes {
            return Err(Error::Capacity);
        }
        if bytes > self.limits.value_bytes {
            return Err(Error::Capacity);
        }
        bytes
            .checked_add(key.len())
            .and_then(|n| n.checked_add(64))
            .ok_or(Error::Capacity)
    }
    fn replacement_bytes(&self, old: usize, new: usize) -> Result<usize, Error> {
        self.bytes
            .checked_sub(old)
            .and_then(|n| n.checked_add(new))
            .filter(|n| *n <= self.limits.total_bytes)
            .ok_or(Error::Capacity)
    }
    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.local.get(key)
    }
    pub fn local(&self) -> &BTreeMap<String, Value> {
        &self.local
    }
    pub fn set(&mut self, key: &str, value: &Value) -> Result<(), Error> {
        self.open()?;
        if !self.local.contains_key(key) && self.local.len() >= self.limits.local_entries {
            return Err(Error::Capacity);
        }
        let old = self
            .local
            .get(key)
            .map(|v| self.entry_bytes(key, v))
            .transpose()?
            .unwrap_or(0);
        let bytes = self.replacement_bytes(old, self.entry_bytes(key, value)?)?;
        self.local.insert(key.to_owned(), value.clone());
        self.bytes = bytes;
        Ok(())
    }
    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.open()?;
        if let Some(value) = self.local.remove(key) {
            self.bytes -= self.entry_bytes(key, &value)?;
        }
        Ok(())
    }
    // The native production template has exactly one chain_poi record. An
    // empty registry encodes successfully; an empty/multiple-record value does
    // not. V1 pings must not call this provider at all.
    pub fn wire(&self) -> Result<Vec<ChainPoi>, Error> {
        self.open()?;
        for key in self.local.keys() {
            if key != CHAIN_POI {
                return Err(Error::UnsupportedKey(key.clone()));
            }
        }
        match self.local.get(CHAIN_POI) {
            None => Ok(vec![]),
            Some(Value::ChainPoi(records)) if records.len() == 1 => Ok(records.clone()),
            Some(_) => Err(Error::InvalidValue),
        }
    }
    pub fn attach(&mut self, peer: PeerPublicKey, generation: G) -> Result<(), Error> {
        self.open()?;
        if self
            .peers
            .get(&peer)
            .is_some_and(|p| p.generation == generation)
        {
            return Ok(());
        }
        if !self.peers.contains_key(&peer) && self.peers.len() >= self.limits.peers {
            return Err(Error::Capacity);
        }
        let old = self.peers.get(&peer).map_or(0, |p| p.bytes);
        let bytes = self.replacement_bytes(old, 128)?;
        self.peers.insert(
            peer,
            Peer {
                generation,
                entries: BTreeMap::new(),
                bytes: 128,
            },
        );
        self.bytes = bytes;
        Ok(())
    }
    pub fn replace(
        &mut self,
        peer: PeerPublicKey,
        generation: &G,
        entries: &BTreeMap<String, Value>,
    ) -> Result<(), Error> {
        self.open()?;
        let old = self
            .peers
            .get(&peer)
            .filter(|p| p.generation == *generation)
            .ok_or(Error::Stale)?;
        if entries.len() > self.limits.entries_per_peer {
            return Err(Error::Capacity);
        }
        let size = entries.iter().try_fold(128usize, |n, (k, v)| {
            n.checked_add(self.entry_bytes(k, v)?)
                .ok_or(Error::Capacity)
        })?;
        let bytes = self.replacement_bytes(old.bytes, size)?;
        self.peers.insert(
            peer,
            Peer {
                generation: generation.clone(),
                entries: entries.clone(),
                bytes: size,
            },
        );
        self.bytes = bytes;
        Ok(())
    }
    pub fn receive(
        &mut self,
        peer: PeerPublicKey,
        generation: &G,
        records: &[ChainPoi],
    ) -> Result<(), Error> {
        self.open()?;
        if !self
            .peers
            .get(&peer)
            .is_some_and(|p| p.generation == *generation)
        {
            return Err(Error::Stale);
        }
        // Empty decoded lists do not invoke native register_peer. A nonempty
        // list replaces the entire map, with the last duplicate key winning.
        if let Some(last) = records.last() {
            // Check borrowed payloads before retaining any owned copies.
            let payload = poi_bytes(last)
                .and_then(|n| n.checked_add(64))
                .ok_or(Error::Capacity)?;
            let size = self
                .entry_weight(CHAIN_POI, payload)?
                .checked_add(128)
                .ok_or(Error::Capacity)?;
            self.replacement_bytes(self.peers[&peer].bytes, size)?;
            self.replace(
                peer,
                generation,
                &BTreeMap::from([(CHAIN_POI.into(), Value::ChainPoi(vec![last.clone()]))]),
            )?;
        }
        Ok(())
    }
    pub fn peers_with(&self, key: &str) -> Vec<PeerValue<G>> {
        self.peers
            .iter()
            .filter_map(|(peer, p)| {
                p.entries.get(key).map(|value| PeerValue {
                    peer: *peer,
                    generation: p.generation.clone(),
                    value: value.clone(),
                })
            })
            .collect()
    }
    // Deliberate operational repair: the original exports remove_peer but has
    // no callback clause. Stale cleanup must not erase a replacement owner.
    pub fn remove(&mut self, peer: PeerPublicKey, generation: &G) -> bool {
        if self
            .peers
            .get(&peer)
            .is_some_and(|p| p.generation == *generation)
        {
            self.bytes -= self.peers.remove(&peer).unwrap().bytes;
            true
        } else {
            false
        }
    }
    pub fn close(&mut self) {
        for peer in self.peers.values() {
            self.bytes -= peer.bytes;
        }
        self.peers.clear();
        self.closed = true;
    }
}
