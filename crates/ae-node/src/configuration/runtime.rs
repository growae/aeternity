// Core operational settings from the original consumers (aeternity fa2a6d7).
// Other service adapters are separate: this does not validate full-node startup.
use super::resolution::{self, Resolved, Strategy};
use ae_chain::{repository::retention, resilience};
use ae_mempool::pool;
use ae_sync::keys;
use ae_types::Unsigned;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(resolution::Error),
    WorkingDirectory,
    Field(&'static str),
    Range(&'static str),
    Family,
    CacheBinding,
    Cache(ae_mempool::gossip::Error),
    CacheConfiguration(ae_storage::pool::worker::CacheConfigurationError),
}
impl From<resolution::Error> for Error {
    fn from(e: resolution::Error) -> Self {
        Self::Resolution(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "core configuration error: {self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Clone)]
pub struct Core {
    pub(crate) pool: pool::Config,
    pub(crate) failures: pool::FailureSettings,
    pub(crate) gc: retention::Config,
    pub(crate) resilience: resilience::Config,
    pub(crate) keys: keys::Config,
    pub(crate) cache_size: usize,
}
impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreConfiguration").finish_non_exhaustive()
    }
}
fn number(v: &Value, field: &'static str) -> Result<u64, Error> {
    v.as_u64().ok_or(Error::Range(field))
}
fn unsigned(v: &Value, field: &'static str) -> Result<Unsigned, Error> {
    let n = v.as_number().ok_or(Error::Field(field))?;
    Unsigned::from_decimal(n.as_str()).map_err(|_| Error::Range(field))
}
fn flag(v: &Value, field: &'static str) -> Result<bool, Error> {
    v.as_bool().ok_or(Error::Field(field))
}
fn schema(r: &Resolved<'_>, path: &[&str]) -> Result<Value, Error> {
    r.find(path, &[Strategy::User, Strategy::SchemaDefault])?
        .ok_or(Error::Field("schema default"))
}
fn merge_thresholds(target: &mut BTreeMap<String, u64>, v: &Value) -> Result<(), Error> {
    for (key, value) in v.as_object().ok_or(Error::Field("tx_failures"))? {
        target.insert(key.clone(), number(value, "tx_failures")?);
    }
    Ok(())
}
impl Core {
    // cwd is the original explicit startup directory, never the ambient cwd.
    // No file, database, socket or worker is opened by resolution.
    pub fn resolve(r: &Resolved<'_>, cwd: &Path) -> Result<Self, Error> {
        if !cwd.is_absolute() {
            return Err(Error::WorkingDirectory);
        }
        let read = |section, name, application, fallback| {
            r.user_or_application(&[section, name], "aecore", application, &fallback)
        };
        // aec_tx_pool uses consumer constants, not a schema-default strategy.
        let pool = pool::Config {
            tx_ttl: number(
                &read("mempool", "tx_ttl", "mempool_tx_ttl", json!(6720))?,
                "tx_ttl",
            )?,
            invalid_tx_ttl: number(
                &read(
                    "mempool",
                    "invalid_tx_ttl",
                    "mempool_invalid_tx_ttl",
                    json!(5),
                )?,
                "invalid_tx_ttl",
            )?,
            future_nonce_ttl: number(
                &read(
                    "mempool",
                    "future_nonce_tx_ttl",
                    "mempool_future_nonce_tx_ttl",
                    json!(480),
                )?,
                "future_nonce_tx_ttl",
            )?,
            nonce_baseline: unsigned(
                &read(
                    "mempool",
                    "nonce_baseline",
                    "mempool_nonce_baseline",
                    json!(1),
                )?,
                "nonce_baseline",
            )?,
            nonce_offset: unsigned(
                &read("mempool", "nonce_offset", "mempool_nonce_offset", json!(5))?,
                "nonce_offset",
            )?,
            allow_reentry: flag(
                &read(
                    "mempool",
                    "allow_reentry_of_txs",
                    "mempool_allow_reentry",
                    json!(false),
                )?,
                "allow_reentry_of_txs",
            )?,
            minimum_miner_gas_price: unsigned(
                &read(
                    "mining",
                    "min_miner_gas_price",
                    "mining_min_miner_gas_price",
                    json!(1_000_000_000u64),
                )?,
                "min_miner_gas_price",
            )?,
            maximum_auth_gas: unsigned(
                &read(
                    "mining",
                    "max_auth_fun_gas",
                    "mining_max_auth_fun_gas",
                    json!(50_000),
                )?,
                "max_auth_fun_gas",
            )?,
        };
        let mut failures = pool::FailureSettings::default();
        if let Some(v) = r.get(&["mempool", "tx_failures"])? {
            for (key, value) in v.as_object().ok_or(Error::Field("tx_failures"))? {
                match key.as_str() {
                    "enabled" => failures.enabled = flag(value, "tx_failures.enabled")?,
                    "common" => merge_thresholds(&mut failures.common, value)?,
                    _ => merge_thresholds(
                        failures.transactions.entry(key.clone()).or_default(),
                        value,
                    )?,
                }
            }
        }
        let gc_read = |name| schema(r, &["chain", "garbage_collection", name]);
        let selected = gc_read("trees")?;
        let trees = selected
            .as_array()
            .ok_or(Error::Field("garbage_collection.trees"))?
            .iter()
            .map(|v| match v.as_str() {
                Some("accounts") => Ok(retention::Tree::Accounts),
                Some("calls") => Ok(retention::Tree::Calls),
                Some("channels") => Ok(retention::Tree::Channels),
                Some("contracts") => Ok(retention::Tree::Contracts),
                Some("ns") => Ok(retention::Tree::Names),
                Some("oracles") => Ok(retention::Tree::Oracles),
                _ => Err(Error::Field("garbage_collection.trees")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let gc = retention::Config {
            enabled: flag(&gc_read("enabled")?, "garbage_collection.enabled")?,
            history: number(&gc_read("history")?, "garbage_collection.history")?,
            minimum_height: number(
                &gc_read("minimum_height")?,
                "garbage_collection.minimum_height",
            )?,
            during_sync: flag(&gc_read("during_sync")?, "garbage_collection.during_sync")?,
            trees: retention::Selection::new(&trees)
                .map_err(|_| Error::Field("garbage_collection.trees"))?,
        };
        gc.validate()
            .map_err(|_| Error::Range("garbage_collection.history"))?;
        let depth = schema(r, &["sync", "sync_allowed_height_from_top"])?;
        // Native read_and_cache_config treats nonpositive integers as disabled.
        let depth = if depth
            .as_number()
            .is_some_and(|v| v.as_str().starts_with('-'))
        {
            0
        } else {
            number(&depth, "sync_allowed_height_from_top")?
        };
        let resilience = resilience::Config {
            depth,
            from_start: flag(
                &schema(r, &["sync", "resist_forks_from_start"])?,
                "resist_forks_from_start",
            )?,
        };
        let cache_size = usize::try_from(number(
            &read("mempool", "cache_size", "cache_size", json!(200))?,
            "cache_size",
        )?)
        .map_err(|_| Error::Range("cache_size"))?;
        if cache_size == 0 {
            return Err(Error::Cache(ae_mempool::gossip::Error::InvalidCapacity));
        }
        let mut keys = r.keys()?;
        keys.directory = cwd.join(&keys.directory);
        Ok(Self {
            pool,
            failures,
            gc,
            resilience,
            keys,
            cache_size,
        })
    }
    pub fn pool(&self) -> &pool::Config {
        &self.pool
    }
    pub fn failures(&self) -> &pool::FailureSettings {
        &self.failures
    }
    pub fn garbage_collection(&self) -> retention::Config {
        self.gc
    }
    pub fn resilience(&self) -> resilience::Config {
        self.resilience
    }
    pub fn keys(&self) -> &keys::Config {
        &self.keys
    }
    pub fn cache_size(&self) -> usize {
        self.cache_size
    }
}
