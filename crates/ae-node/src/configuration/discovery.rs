// aec_peers_pool:new/1 and native gossip consumers, aeternity fa2a6d7.
use super::resolution::{self, Resolved, Strategy};
use ae_p2p::discovery;
use ae_sync::runtime::connection;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(resolution::Error),
    Field(&'static str),
    Range(&'static str),
    Unsupported(&'static str),
    Family,
}
impl From<resolution::Error> for Error {
    fn from(value: resolution::Error) -> Self {
        Self::Resolution(value)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "discovery configuration error: {self:?}")
    }
}
impl std::error::Error for Error {}

// Map syntax is convenient for unique application options. Ordered pairs also
// represent native proplists: the first occurrence of a key wins.
#[derive(Default)]
struct Options(BTreeMap<String, Value>);
impl Options {
    fn parse(value: Option<&Value>) -> Result<Self, Error> {
        let Some(value) = value else {
            return Ok(Self::default());
        };
        let mut options = BTreeMap::new();
        match value {
            Value::Object(values) => {
                options.extend(values.iter().map(|(k, v)| (k.clone(), v.clone())))
            }
            Value::Array(values) => {
                for value in values {
                    let pair = value
                        .as_array()
                        .filter(|p| p.len() == 2)
                        .ok_or(Error::Field("peer_pool options"))?;
                    let key = pair[0]
                        .as_str()
                        .ok_or(Error::Field("peer_pool option key"))?;
                    options.entry(key.into()).or_insert_with(|| pair[1].clone());
                }
            }
            _ => return Err(Error::Field("peer_pool options")),
        }
        Ok(Self(options))
    }
    fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(name)
    }
}
fn integer(value: &Value, field: &'static str) -> Result<u64, Error> {
    value.as_u64().ok_or(Error::Range(field))
}
fn size(value: &Value, field: &'static str) -> Result<usize, Error> {
    usize::try_from(integer(value, field)?).map_err(|_| Error::Range(field))
}
fn float(value: &Value, field: &'static str) -> Result<f64, Error> {
    value
        .as_f64()
        .filter(|v| v.is_finite())
        .ok_or(Error::Range(field))
}

#[derive(Debug, Clone)]
pub struct Discovery {
    pool: discovery::Config,
    geometry: BTreeSet<&'static str>,
    maximum_gossip: usize,
    gossip_depth: u64,
}
impl Discovery {
    pub fn resolve(resolved: &Resolved<'_>) -> Result<Self, Error> {
        let options = Options::parse(resolved.raw_application("aecore", "peer_pool"))?;
        // These native diagnostic entropy controls need a separate explicit
        // random-source binding; never silently turn them into fresh entropy.
        for name in ["secret", "seed"] {
            if options.get(name).is_some() {
                return Err(Error::Unsupported(name));
            }
        }
        if options
            .get("disable_strong_random")
            .is_some_and(|v| v != &json!(false))
        {
            return Err(Error::Unsupported("disable_strong_random"));
        }
        let read = |name, option, fallback| -> Result<Value, Error> {
            // Native get_opt eagerly evaluates the user-aware getter, then
            // lets the raw application constructor option override its result.
            let default = match resolved.get(&["sync", "peer_pool", name])? {
                Some(user) => user.clone(),
                None => {
                    let expanded = resolved.find(
                        &[],
                        &[Strategy::Application {
                            app: "aecore",
                            key: "peer_pool",
                        }],
                    )?;
                    Options::parse(expanded.as_ref())?
                        .get(name)
                        .cloned()
                        .unwrap_or(fallback)
                }
            };
            Ok(options.get(option).cloned().unwrap_or(default))
        };
        let mut pool = discovery::Config {
            verified_probability: float(
                &read(
                    "select_verified_peer_probability",
                    "select_verif_prob",
                    json!(1.0),
                )?,
                "select_verified_peer_probability",
            )?,
            maximum_update_lapse: integer(
                &read("max_update_lapse", "max_update_lapse", json!(10_800_000))?,
                "max_update_lapse",
            )?,
            maximum_rejections: integer(
                &read("max_rejections", "max_rejections", json!(7))?,
                "max_rejections",
            )?,
            ..Default::default()
        };
        let standby = read(
            "standby_times",
            "standby_times",
            json!([5000, 15000, 30000, 60000, 120000, 300000, 600000]),
        )?;
        pool.standby_times = standby
            .as_array()
            .ok_or(Error::Field("standby_times"))?
            .iter()
            .map(|v| integer(v, "standby_times"))
            .collect::<Result<_, _>>()?;
        let mut geometry = BTreeSet::new();
        macro_rules! option {
            ($name:literal,$field:ident) => {
                if let Some(value) = options.get($name) {
                    pool.$field = size(value, $name)?;
                    geometry.insert($name);
                }
            };
        }
        option!("verif_bcount", verified_buckets);
        option!("verif_bsize", verified_size);
        option!("verif_group_shard", verified_group_shard);
        option!("unver_bcount", unverified_buckets);
        option!("unver_bsize", unverified_size);
        option!("unver_source_shard", unverified_source_shard);
        option!("unver_group_shard", unverified_group_shard);
        option!("unver_max_refs", maximum_references);
        if let Some(value) = options.get("eviction_skew") {
            pool.eviction_skew = float(value, "eviction_skew")?;
            geometry.insert("eviction_skew");
        }
        pool.validate().map_err(|_| Error::Range("peer_pool"))?;
        let maximum_gossip = size(
            &resolved.user_or_application(
                &["sync", "max_gossip"],
                "aecore",
                "sync_max_gossip",
                &json!(16),
            )?,
            "max_gossip",
        )?;
        if maximum_gossip > 65_536 {
            return Err(Error::Range("max_gossip"));
        }
        let gossip_depth = integer(
            &resolved.user_or_application(
                &["sync", "gossip_allowed_height_from_top"],
                "aecore",
                "gossip_allowed_height_from_top",
                &json!(5),
            )?,
            "gossip_allowed_height_from_top",
        )?;
        Ok(Self {
            pool,
            geometry,
            maximum_gossip,
            gossip_depth,
        })
    }
    pub fn pool(&self) -> &discovery::Config {
        &self.pool
    }
    pub fn maximum_gossip(&self) -> usize {
        self.maximum_gossip
    }
    pub fn gossip_depth(&self) -> u64 {
        self.gossip_depth
    }
    pub fn apply(&self, config: &mut connection::Config) -> Result<(), Error> {
        if self.maximum_gossip > config.propagation.peer_limit {
            return Err(Error::Range("max_gossip capacity"));
        }
        let mut pool = config.network.registry.policy.pool.clone();
        pool.verified_probability = self.pool.verified_probability;
        pool.maximum_update_lapse = self.pool.maximum_update_lapse;
        pool.maximum_rejections = self.pool.maximum_rejections;
        pool.standby_times.clone_from(&self.pool.standby_times);
        macro_rules! copy {
            ($name:literal,$field:ident) => {
                if self.geometry.contains($name) {
                    pool.$field = self.pool.$field;
                }
            };
        }
        copy!("verif_bcount", verified_buckets);
        copy!("verif_bsize", verified_size);
        copy!("verif_group_shard", verified_group_shard);
        copy!("unver_bcount", unverified_buckets);
        copy!("unver_bsize", unverified_size);
        copy!("unver_source_shard", unverified_source_shard);
        copy!("unver_group_shard", unverified_group_shard);
        copy!("unver_max_refs", maximum_references);
        copy!("eviction_skew", eviction_skew);
        pool.validate().map_err(|_| Error::Range("peer_pool"))?;
        config.network.registry.policy.pool = pool;
        config.gossip.policy.max_peers = self.maximum_gossip;
        config.propagation.received_peers = self.maximum_gossip;
        config.importer.ledger.policy.gossip_depth = self.gossip_depth;
        Ok(())
    }
}
