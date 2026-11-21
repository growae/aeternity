use super::*;
use ae_codec::api::{self, ApiType};
use num_bigint::{BigInt, Sign};
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Binary(Vec<u8>),
    Integer(BigInt),
    List(Vec<i64>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub enabled: bool,
    pub stats: BTreeMap<PeerPublicKey, Stat>,
    pub pending: usize,
}
impl Snapshot {
    pub fn for_client(&self) -> BTreeMap<String, BTreeMap<&'static str, Value>> {
        self.stats
            .iter()
            .map(|(peer, stat)| (api::encode(ApiType::PeerPubkey, &peer.0), stat.for_client()))
            .collect()
    }
}
impl Stat {
    pub fn for_client(&self) -> BTreeMap<&'static str, Value> {
        let unsigned =
            |n: &Unsigned| Value::Integer(BigInt::from_bytes_be(Sign::Plus, n.as_bytes()));
        let mut fields = BTreeMap::from([
            (
                "host",
                match &self.host {
                    Host::Binary(b) => Value::Binary(b.clone()),
                    Host::List(l) => Value::List(l.clone()),
                },
            ),
            ("port", unsigned(&self.port)),
            ("first_seen", Value::Integer(self.first_seen.into())),
            ("last_seen", Value::Integer(self.last_seen.into())),
            (
                "genesis_hash",
                Value::Binary(api::encode(ApiType::KeyBlockHash, &self.genesis).into_bytes()),
            ),
            (
                "top_hash",
                Value::Binary(api::encode(ApiType::KeyBlockHash, &self.top).into_bytes()),
            ),
            ("top_difficulty", unsigned(&self.difficulty)),
        ]);
        if let PeerInfo::Resolved(info) = &self.info {
            for (key, value) in [
                ("network_id", &info.network_id),
                ("node_version", &info.version),
                ("node_os", &info.os),
                ("node_revision", &info.revision),
                ("node_vendor", &info.vendor),
            ] {
                fields.insert(key, Value::Binary(value.clone()));
            }
        }
        fields
    }
}
#[derive(Clone)]
pub struct Engine {
    pub(super) config: Config,
    pub(super) stats: BTreeMap<PeerPublicKey, Stat>,
    pub(super) pending: BTreeMap<PeerPublicKey, u64>,
    sequence: u64,
}
impl Engine {
    pub fn new(config: Config) -> Result<Self, Error> {
        config.limits.validate()?;
        Ok(Self {
            config,
            stats: BTreeMap::new(),
            pending: BTreeMap::new(),
            sequence: 0,
        })
    }
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            enabled: self.config.enabled,
            stats: self.stats.clone(),
            pending: self.pending.len(),
        }
    }
    pub fn retained_bytes(&self) -> usize {
        self.stats.values().map(Stat::bytes).sum::<usize>() + self.pending.len() * 64
    }
    pub fn record(&mut self, a: Announcement, first: i64, last: i64) -> Result<Recorded, Error> {
        if !self.config.enabled {
            return Ok(Recorded::default());
        }
        self.config.limits.announcement(&a)?;
        let old = self.stats.get(&a.peer);
        if old.is_none() && self.stats.len() >= self.config.limits.peers {
            return Err(Error::Limit);
        }
        let stat = Stat {
            host: a.host,
            port: a.port,
            first_seen: old.map_or(first, |s| s.first_seen),
            last_seen: last,
            genesis: a.genesis,
            top: a.top,
            difficulty: a.difficulty,
            info: old.map_or(PeerInfo::Unresolved, |s| s.info.clone()),
        };
        let schedule = !a.temporary
            && (stat.age() > i128::from(VERSION_AGE_SECONDS) || stat.info == PeerInfo::Unresolved)
            && !self.pending.contains_key(&a.peer);
        let deferred = schedule && self.pending.len() >= self.config.limits.probes;
        let pending = schedule && !deferred;
        let bytes = self
            .retained_bytes()
            .saturating_sub(old.map_or(0, Stat::bytes))
            .saturating_add(stat.bytes())
            .saturating_add(usize::from(pending) * 64);
        if bytes > self.config.limits.retained_bytes {
            return Err(Error::Limit);
        }
        let next = if pending {
            Some(self.sequence.checked_add(1).ok_or(Error::Sequence)?)
        } else {
            None
        };
        self.stats.insert(a.peer, stat);
        let probe = next.map(|id| {
            self.sequence = id;
            self.pending.insert(a.peer, id);
            Probe { peer: a.peer, id }
        });
        Ok(Recorded { probe, deferred })
    }
    pub fn update_info(&mut self, peer: PeerPublicKey, info: PeerInfo) -> Result<(), Error> {
        if let PeerInfo::Resolved(i) = &info {
            self.config.limits.info(i)?;
        }
        let old = self.stats.get(&peer).ok_or(Error::Missing)?;
        let mut next = old.clone();
        next.info = info;
        if self
            .retained_bytes()
            .saturating_sub(old.bytes())
            .saturating_add(next.bytes())
            > self.config.limits.retained_bytes
        {
            return Err(Error::Limit);
        }
        self.stats.insert(peer, next);
        Ok(())
    }
    pub fn complete(&mut self, probe: Probe, result: ProbeResult) -> Result<(), Error> {
        if self.pending.get(&probe.peer) != Some(&probe.id) {
            return Err(Error::Stale);
        }
        self.pending.remove(&probe.peer);
        match result {
            ProbeResult::NoConnection => Ok(()),
            ProbeResult::Unknown => self.update_info(probe.peer, PeerInfo::Unknown),
            ProbeResult::Resolved(info) => self.update_info(probe.peer, PeerInfo::Resolved(info)),
        }
    }
    pub fn sweep(&mut self) -> usize {
        let before = self.stats.len();
        self.stats
            .retain(|_, s| s.age() <= i128::from(SWEEP_AGE_SECONDS));
        before - self.stats.len()
    }
}
