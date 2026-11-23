// Startup configuration from aec_consensus_hc (aeternity fa2a6d7).
// This parses the consensus plugin subtree, not the node's complete config file.
use super::{conductor, pinning};
use crate::{parent_blocks, parent_pins};
use ae_chain::repository as repo;
use ae_codec::api::{self, ApiType};
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{parent_cache, signing::Keys};
use ae_storage::Database;
use ae_types::{AccountPublicKey, Unsigned};
use ae_vm_fate::maps::MapOrder;
use serde_json::{Map, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use zeroize::Zeroizing;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Limit,
    Json,
    Field(&'static str),
    KeyPair,
    Binding,
    Backend(parent_blocks::Error),
}
#[derive(Debug, Clone, Default)]
pub struct Services {
    pub blocks: parent_blocks::Config,
    pub pins: parent_pins::Config,
    pub agent: pinning::Config,
}
// No Debug/serialization: the parser owns secret signing material, and accepts
// borrowed bytes so the caller retains responsibility for its config buffer.
pub struct Configuration {
    pub child_block_time: Unsigned,
    pub production_time: Unsigned,
    pub election: [u8; 32],
    pub parent_epoch_length: Option<Unsigned>,
    pub pin_sync_margin: Unsigned,
    pub chain: parent_pins::Chain,
    pub network: Vec<u8>,
    pub cache: parent_cache::Config,
    pub fetch_interval: Duration,
    pub nodes: Vec<parent_pins::Node>,
    pub accounts: BTreeMap<AccountPublicKey, AccountPublicKey>,
    pub default_pinning: bool,
    child_keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
    keys: Keys,
}
fn object<'a>(v: &'a Value, name: &'static str) -> Result<&'a Map<String, Value>, Error> {
    v.as_object().ok_or(Error::Field(name))
}
fn optional<'a>(
    m: &'a Map<String, Value>,
    name: &'static str,
    empty: &'a Map<String, Value>,
) -> Result<&'a Map<String, Value>, Error> {
    m.get(name).map_or(Ok(empty), |v| object(v, name))
}
fn number(m: &Map<String, Value>, name: &'static str, default: u64) -> Result<u64, Error> {
    m.get(name)
        .map_or(Ok(default), |v| v.as_u64().ok_or(Error::Field(name)))
}
fn string<'a>(m: &'a Map<String, Value>, name: &'static str) -> Result<&'a str, Error> {
    m.get(name)
        .and_then(Value::as_str)
        .ok_or(Error::Field(name))
}
fn public(m: &Map<String, Value>, name: &'static str, kind: ApiType) -> Result<[u8; 32], Error> {
    let s = string(m, name)?;
    if s.len() > 128 {
        return Err(Error::Field(name));
    }
    api::decode_as(kind, s)
        .map_err(|_| Error::Field(name))?
        .try_into()
        .map_err(|_| Error::Field(name))
}
fn nibble(b: u8) -> Result<u8, Error> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(Error::KeyPair),
    }
}
fn key_pair(m: &Map<String, Value>) -> Result<(Ed25519SecretKey, Ed25519SecretKey), Error> {
    let public = AccountPublicKey(public(m, "pub", ApiType::AccountPubkey)?);
    let text = string(m, "priv")?.as_bytes();
    if text.len() != 128 {
        return Err(Error::KeyPair);
    }
    let mut decoded = Zeroizing::new([0u8; 64]);
    for (out, pair) in decoded.iter_mut().zip(text.chunks_exact(2)) {
        *out = nibble(pair[0])? * 16 + nibble(pair[1])?;
    }
    let seed: &[u8; 32] = decoded[..32].try_into().expect("32-byte seed");
    let key = Ed25519SecretKey::from_seed(seed);
    if key.public_key() != public || decoded[32..] != public.0 {
        return Err(Error::KeyPair);
    }
    Ok((key, Ed25519SecretKey::from_seed(seed)))
}
impl Configuration {
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > 16 << 20 {
            return Err(Error::Limit);
        }
        let mut value: Value = serde_json::from_slice(bytes).map_err(|_| Error::Json)?;
        let result = Self::from_value(&value);
        // serde_json owns copies of input strings. Scrub all of them, including
        // malformed account fields, before the temporary tree is released.
        fn scrub(v: &mut Value) {
            use zeroize::Zeroize;
            match v {
                Value::String(s) => s.zeroize(),
                Value::Array(a) => a.iter_mut().for_each(scrub),
                Value::Object(o) => o.values_mut().for_each(scrub),
                _ => (),
            }
        }
        scrub(&mut value);
        result
    }
    fn from_value(value: &Value) -> Result<Self, Error> {
        let root = object(value, "config")?;
        let pc = object(
            root.get("parent_chain")
                .ok_or(Error::Field("parent_chain"))?,
            "parent_chain",
        )?;
        let empty = Map::new();
        let consensus = optional(pc, "consensus", &empty)?;
        let polling = optional(pc, "polling", &empty)?;
        let chain = match consensus.get("type").map_or(Some("AE2AE"), Value::as_str) {
            Some("AE2AE") => parent_pins::Chain::Aeternity,
            Some("AE2BTC") => parent_pins::Chain::Bitcoin,
            Some("AE2DOGE") => parent_pins::Chain::Dogecoin,
            _ => return Err(Error::Field("type")),
        };
        let network = consensus
            .get("network_id")
            .map_or(Some("ae_mainnet"), Value::as_str)
            .ok_or(Error::Field("network_id"))?;
        if network.len() > 4096 {
            return Err(Error::Limit);
        }
        let mut nodes = vec![];
        if let Some(v) = polling.get("nodes") {
            let a = v.as_array().ok_or(Error::Field("nodes"))?;
            if a.len() > 32 {
                return Err(Error::Limit);
            }
            for v in a {
                nodes.push(
                    parent_pins::Node::from_config_url(
                        chain,
                        v.as_str().ok_or(Error::Field("nodes"))?,
                    )
                    .map_err(|_| Error::Field("nodes"))?,
                );
            }
        }
        let child_block_time = number(root, "child_block_time", 3000)?;
        let production_time = number(root, "child_block_production_time", 500)?;
        if child_block_time == 0 || production_time == 0 {
            return Err(Error::Field("child timing"));
        }
        let mut child_keys = BTreeMap::new();
        let mut parent_keys = BTreeMap::new();
        let mut accounts = BTreeMap::new();
        let mut count = 0;
        for (name, kind) in [
            ("stakers", "hyper_chain_account"),
            ("pinners", "parent_chain_account"),
        ] {
            if let Some(v) = root.get(name) {
                let a = v.as_array().ok_or(Error::Field(name))?;
                count += a.len();
                if count > 65536 {
                    return Err(Error::Limit);
                }
                for v in a {
                    let row = object(v, name)?;
                    let account = object(row.get(kind).ok_or(Error::Field(kind))?, kind)?;
                    let (child, parent) = key_pair(account)?;
                    let id = child.public_key();
                    child_keys.insert(id, child);
                    parent_keys.insert(id, parent);
                    if name == "pinners" {
                        let owner =
                            AccountPublicKey(public(account, "owner", ApiType::AccountPubkey)?);
                        accounts.insert(owner, id);
                    }
                }
            }
        }
        Ok(Self {
            child_block_time: child_block_time.into(),
            production_time: production_time.into(),
            election: public(root, "election_contract", ApiType::ContractPubkey)?,
            parent_epoch_length: pc
                .get("parent_epoch_length")
                .map(|v| {
                    v.as_u64()
                        .filter(|n| *n > 0)
                        .map(Unsigned::from)
                        .ok_or(Error::Field("parent_epoch_length"))
                })
                .transpose()?,
            pin_sync_margin: number(root, "parent_pin_sync_margin", 0)?.into(),
            chain,
            network: network.as_bytes().to_vec(),
            nodes,
            accounts,
            default_pinning: root
                .get("default_pinning_behavior")
                .map_or(Ok(false), |v| {
                    v.as_bool().ok_or(Error::Field("default_pinning_behavior"))
                })?,
            cache: parent_cache::Config {
                child_start_height: number(pc, "start_height", 0)?.into(),
                finality: number(pc, "finality", 5)?.into(),
                retry_interval: number(polling, "retry_interval", 1000)?.into(),
                max_size: number(polling, "cache_size", 200)?.into(),
            },
            fetch_interval: Duration::from_millis(number(polling, "fetch_interval", 500)?),
            child_keys,
            keys: Keys::new(parent_keys.into_values()),
        })
    }
    pub fn keys(&self) -> &Keys {
        &self.keys
    }
    pub fn public_keys(&self) -> impl Iterator<Item = AccountPublicKey> + '_ {
        self.child_keys.keys().copied()
    }
    // Operational limits remain caller supplied. Configuration must agree with
    // the established execution rules; startup never rewrites consensus rules.
    pub fn configure<O: MapOrder + Clone + Send + Sync + 'static>(
        self,
        execution: repo::HyperchainConfig<O>,
        mut runtime: conductor::Config,
        services: Services,
    ) -> Result<Prepared<O>, Error> {
        let rules = &execution.rules.consensus;
        if self.child_block_time != rules.child_block_time
            || self.parent_epoch_length.as_ref() != Some(&rules.parent_epoch_length)
            || self.cache.child_start_height != rules.parent_start
            || self.cache.finality != rules.parent_finality
            || self.pin_sync_margin != rules.pin_sync_margin
            || self.election != execution.profile.contracts.election
        {
            return Err(Error::Binding);
        }
        let backend = Arc::new(
            parent_blocks::Backend::configured(self.nodes.clone(), services.blocks)
                .map_err(Error::Backend)?,
        );
        runtime.eoe.eoe.block_time = self.child_block_time;
        runtime.eoe.eoe.host.network = execution.profile.network.clone();
        runtime.eoe.eoe.host.election_contract = self.election;
        runtime.eoe.eoe.host.parent_start_height = rules.parent_start.clone();
        runtime.eoe.eoe.host.parent_epoch_length = rules.parent_epoch_length.clone();
        runtime.production.producer.production_time = num_bigint::BigInt::from_bytes_be(
            num_bigint::Sign::Plus,
            self.production_time.as_bytes(),
        );
        runtime.parent.cache = self.cache;
        runtime.parent.connector.fetch_interval = Some(self.fetch_interval);
        runtime.parent.hosts = backend.hosts();
        runtime.parent.pins = Some(conductor::PinConfig {
            chain: self.chain,
            network: self.network.clone(),
            accounts: self.accounts,
            signing: (self.chain == parent_pins::Chain::Aeternity).then_some(
                parent_pins::posting::Signing {
                    network: self.network,
                    keys: self.keys,
                },
            ),
            agent: self.default_pinning.then_some(services.agent),
            nodes: self.nodes,
            service: services.pins,
        });
        Ok(Prepared {
            execution,
            runtime,
            keys: self.child_keys,
            backend,
        })
    }
}
pub struct Prepared<O> {
    execution: repo::HyperchainConfig<O>,
    runtime: conductor::Config,
    keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
    backend: Arc<parent_blocks::Backend>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Prepared<O> {
    pub fn runtime(&self) -> &conductor::Config {
        &self.runtime
    }
    pub fn start(
        self,
        database: Arc<Database>,
    ) -> Result<conductor::Conductor<O>, conductor::Error> {
        conductor::Conductor::start(
            database,
            self.execution,
            self.keys,
            self.runtime,
            self.backend,
        )
    }
    pub fn start_with_targets(
        self,
        database: Arc<Database>,
        targets: impl parent_cache::runtime::Targets,
    ) -> Result<conductor::Conductor<O>, conductor::Error> {
        conductor::Conductor::start_with_targets(
            database,
            self.execution,
            self.keys,
            self.runtime,
            self.backend,
            targets,
        )
    }
}

#[cfg(test)]
#[path = "configuration/tests.rs"]
mod tests;
