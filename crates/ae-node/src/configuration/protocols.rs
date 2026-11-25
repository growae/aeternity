// Protocol configuration and community signalling, aec_hard_forks.erl fa2a6d7.
use super::{
    MAX_BYTES, MAX_DEPTH, MAX_NODES, budget,
    consensus::{self, Kind},
    resolution::{Resolved, Strategy},
};
use ae_protocol::{Activation, NetworkProfile, ProfileError, Protocol, signaling::Fork};
use ae_vm_fate::{
    Limits,
    maps::{Traversal, order::binary_indices},
};
use num_bigint::BigInt;
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(super::resolution::Error),
    Limit,
    Shape,
    Version,
    Height,
    Genesis,
    Order,
    UnsupportedProtocol,
    Fork,
    Profile(ProfileError),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "protocol configuration error: {self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Clone, PartialEq)]
pub struct Protocols {
    network: Vec<u8>,
    entries: Vec<(BigInt, Value)>,
}
impl fmt::Debug for Protocols {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtocolConfiguration")
            .field("entries", &self.entries.len())
            .finish_non_exhaustive()
    }
}
fn fixed(network: &[u8]) -> Option<Vec<Activation>> {
    match network {
        b"ae_mainnet" => return Some(NetworkProfile::mainnet().activations().to_vec()),
        b"ae_uat" => return Some(NetworkProfile::uat().activations().to_vec()),
        _ => (),
    }
    let last = match network {
        b"local_roma_testnet" => Protocol::Roma,
        b"local_minerva_testnet" => Protocol::Minerva,
        b"local_fortuna_testnet" => Protocol::Fortuna,
        b"local_lima_testnet" => Protocol::Lima,
        b"local_iris_testnet" => Protocol::Iris,
        b"local_ceres_testnet" => Protocol::Ceres,
        b"local_arcus_testnet" => Protocol::Arcus,
        b"local_salus_testnet" => Protocol::Salus,
        _ => return None,
    };
    let mut result = vec![Activation {
        height: 0,
        protocol: Protocol::Roma,
    }];
    if last != Protocol::Roma {
        result.push(Activation {
            height: 1,
            protocol: last,
        });
    }
    Some(result)
}
pub(super) fn integer(value: &Value) -> Option<BigInt> {
    let n = value.as_number()?.as_str();
    if n.contains(['.', 'e', 'E']) {
        return None;
    }
    n.parse().ok()
}
impl Protocols {
    pub fn select(network: &[u8], value: Option<&Value>) -> Result<Self, Error> {
        if network.len() > 65536 {
            return Err(Error::Limit);
        }
        let defaults = fixed(network).or_else(|| {
            value.is_none().then(|| {
                vec![
                    Activation {
                        height: 0,
                        protocol: Protocol::Roma,
                    },
                    Activation {
                        height: 1,
                        protocol: Protocol::Ceres,
                    },
                ]
            })
        });
        if let Some(defaults) = defaults {
            return Ok(Self {
                network: network.to_vec(),
                entries: defaults
                    .into_iter()
                    .map(|a| (BigInt::from(a.protocol as u32), json!(a.height)))
                    .collect(),
            });
        }
        let value = value.expect("present");
        budget(value).map_err(|_| Error::Limit)?;
        let object = value.as_object().ok_or(Error::Shape)?;
        if object.len() > 1024 {
            return Err(Error::Limit);
        }
        let entries: Vec<_> = object.iter().collect();
        let keys: Vec<_> = entries.iter().map(|(k, _)| k.as_bytes()).collect();
        let order = binary_indices(
            &keys,
            Traversal::Fold,
            Limits {
                max_nodes: MAX_NODES,
                max_bytes: MAX_BYTES,
                max_depth: MAX_DEPTH,
            },
        )
        .map_err(|_| Error::Limit)?;
        let mut normalized = BTreeMap::new();
        for i in order {
            let (version, v) = entries[i];
            let height = if let Some(h) = v.as_object().and_then(|o| o.get("height")) {
                h.clone()
            } else if integer(v).is_some() {
                v.clone()
            } else {
                return Err(Error::Shape);
            };
            let version = consensus::integer_key(version).ok_or(Error::Version)?;
            normalized.insert(version, height);
        }
        Ok(Self {
            network: network.to_vec(),
            entries: normalized.into_iter().collect(),
        })
    }
    pub fn entries(&self) -> &[(BigInt, Value)] {
        &self.entries
    }
    pub fn network_id(&self) -> &[u8] {
        &self.network
    }
    // Build the existing execution profile, without changing canonical
    // protocol rules. Unsupported versions/widths are explicit boundaries.
    pub fn build(&self, genesis: Kind, fork: Option<&Value>) -> Result<NetworkProfile, Error> {
        let Some((first, height)) = self.entries.first() else {
            return Err(Error::Genesis);
        };
        if integer(height) != Some(0.into())
            || genesis == Kind::Unknown
            || (genesis != Kind::Hyperchain && *first != BigInt::from(1))
        {
            return Err(Error::Genesis);
        }
        let mut activations = Vec::with_capacity(self.entries.len());
        let mut previous = None;
        for (version, height) in &self.entries {
            let version = u32::try_from(version).map_err(|_| Error::UnsupportedProtocol)?;
            let protocol = Protocol::try_from(version).map_err(|_| Error::UnsupportedProtocol)?;
            let height = integer(height).ok_or(Error::Height)?;
            let height = u64::try_from(&height).map_err(|_| Error::Limit)?;
            if previous.is_some_and(|p| height <= p) {
                return Err(Error::Order);
            }
            activations.push(Activation { height, protocol });
            previous = Some(height);
        }
        let profile = match self.network.as_slice() {
            b"ae_mainnet" => NetworkProfile::mainnet(),
            b"ae_uat" => NetworkProfile::uat(),
            _ => {
                NetworkProfile::custom(self.network.clone(), activations).map_err(Error::Profile)?
            }
        };
        let fork = signaling(
            self.network_id(),
            fork,
            *profile.activations().last().expect("nonempty"),
        )?;
        profile.with_community_fork(fork).map_err(Error::Profile)
    }
}
fn signaling(
    network: &[u8],
    value: Option<&Value>,
    previous: Activation,
) -> Result<Option<Fork>, Error> {
    let Some(value) = value else {
        return Ok(None);
    };
    budget(value).map_err(|_| Error::Limit)?;
    let map = value.as_object().ok_or(Error::Fork)?;
    if map.keys().any(|k| {
        ![
            "enabled",
            "signalling_start_height",
            "signalling_end_height",
            "signalling_block_count",
            "info_field",
            "version",
        ]
        .contains(&k.as_str())
    }) {
        return Err(Error::Fork);
    }
    let enabled = map
        .get("enabled")
        .and_then(Value::as_bool)
        .ok_or(Error::Fork)?;
    if matches!(network, b"ae_mainnet" | b"ae_uat") {
        if map.len() != 1 {
            return Err(Error::Fork);
        }
        // The pinned public-network signalling definitions are both absent.
        return Ok(None);
    }
    let number = |key| {
        map.get(key)
            .and_then(integer)
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(Error::Fork)
    };
    let fork = Fork {
        start: number("signalling_start_height")?,
        end: number("signalling_end_height")?,
        threshold: number("signalling_block_count")?,
        info: u32::try_from(number("info_field")?).map_err(|_| Error::Fork)?,
        version: u32::try_from(number("version")?).map_err(|_| Error::Fork)?,
    };
    // Native ensure_env validates disabled private fork settings too.
    fork.validate(previous).map_err(|_| Error::Fork)?;
    Ok(enabled.then_some(fork))
}
impl Resolved<'_> {
    pub fn protocols(&self, network: &[u8]) -> Result<Protocols, Error> {
        if fixed(network).is_some() {
            return Protocols::select(network, None);
        }
        let value = self
            .find(
                &["chain", "hard_forks"],
                &[
                    Strategy::User,
                    Strategy::Application {
                        app: "aecore",
                        key: "hard_forks",
                    },
                ],
            )
            .map_err(Error::Resolution)?;
        Protocols::select(network, value.as_ref())
    }
}
