#![forbid(unsafe_code)]
// Activation reference: aeternity fa2a6d7, aec_hard_forks.erl.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use ae_types::Hash256;

pub mod coinbase;
pub mod contracts;
pub mod gas;
pub mod names;
pub mod oracles;
pub mod signaling;

// The configured consensus family is independent of the fork version or network ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConsensusType {
    #[default]
    Pow,
    Pos,
}

// The native family alone does not identify the validity rules: on-demand
// reports pow, but has its own target, difficulty and validation callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusAlgorithm {
    Cuckoo,
    OnDemand,
    Hyperchain,
}
impl ConsensusAlgorithm {
    pub const fn family(self) -> ConsensusType {
        match self {
            Self::Cuckoo | Self::OnDemand => ConsensusType::Pow,
            Self::Hyperchain => ConsensusType::Pos,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u32)]
pub enum Protocol {
    Roma = 1,
    Minerva = 2,
    Fortuna = 3,
    Lima = 4,
    Iris = 5,
    Ceres = 6,
    Arcus = 7,
    Salus = 8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileError {
    UnknownProtocol(u32),
    WrongProtocol { expected: Protocol, actual: u32 },
    InvalidSchedule,
    ReservedNetworkId,
    InvalidFork,
    MissingForkDecision { height: u64 },
    WrongForkDecision,
}
impl ProfileError {
    pub fn is_operational(&self) -> bool {
        !matches!(self, Self::WrongProtocol { .. })
    }
}

impl TryFrom<u32> for Protocol {
    type Error = ProfileError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Roma),
            2 => Ok(Self::Minerva),
            3 => Ok(Self::Fortuna),
            4 => Ok(Self::Lima),
            5 => Ok(Self::Iris),
            6 => Ok(Self::Ceres),
            7 => Ok(Self::Arcus),
            8 => Ok(Self::Salus),
            _ => Err(ProfileError::UnknownProtocol(value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Activation {
    pub height: u64,
    pub protocol: Protocol,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkProfile {
    network_id: Vec<u8>,
    activations: Vec<Activation>,
    fork: Option<signaling::Fork>,
    resolved: Option<(u64, u32)>,
}

pub const MAINNET_GENESIS_HASH: Hash256 = Hash256([
    0x6c, 0x15, 0xda, 0x6e, 0xbf, 0xaf, 0x02, 0x78, 0xfe, 0xaf, 0x4d, 0xf1, 0xb0, 0xf1, 0xa9, 0x82,
    0x55, 0x07, 0xae, 0x7b, 0x9a, 0x49, 0x4b, 0xc3, 0x4c, 0x91, 0x71, 0x3f, 0x38, 0xdd, 0x57, 0x83,
]);

impl NetworkProfile {
    pub fn mainnet() -> Self {
        Self::public(b"ae_mainnet", [0, 47800, 90800, 161150, 441444, 941700])
    }
    pub fn uat() -> Self {
        Self::public(b"ae_uat", [0, 40900, 82900, 154300, 425900, 939750])
    }
    fn public(id: &[u8], heights: [u64; 6]) -> Self {
        let protocols = [
            Protocol::Roma,
            Protocol::Minerva,
            Protocol::Fortuna,
            Protocol::Lima,
            Protocol::Iris,
            Protocol::Ceres,
        ];
        Self {
            network_id: id.to_vec(),
            fork: None,
            resolved: None,
            activations: heights
                .into_iter()
                .zip(protocols)
                .map(|(height, protocol)| Activation { height, protocol })
                .collect(),
        }
    }
    pub fn custom(id: Vec<u8>, activations: Vec<Activation>) -> Result<Self, ProfileError> {
        if id == b"ae_mainnet" || id == b"ae_uat" {
            return Err(ProfileError::ReservedNetworkId);
        }
        if id.is_empty()
            || activations.first().is_none_or(|a| a.height != 0)
            || activations
                .windows(2)
                .any(|w| w[0].height >= w[1].height || w[0].protocol >= w[1].protocol)
        {
            return Err(ProfileError::InvalidSchedule);
        }
        Ok(Self {
            network_id: id,
            activations,
            fork: None,
            resolved: None,
        })
    }
    pub fn with_community_fork(
        mut self,
        fork: Option<signaling::Fork>,
    ) -> Result<Self, ProfileError> {
        if let Some(fork) = fork {
            if self.fixed_block_gas_limit().is_some() {
                return Err(ProfileError::ReservedNetworkId);
            }
            fork.validate(*self.activations.last().expect("nonempty schedule"))?;
        }
        self.fork = fork;
        self.resolved = None;
        Ok(self)
    }
    pub fn community_fork(&self) -> Option<signaling::Fork> {
        self.fork
    }
    pub fn resolution(&self) -> Option<(u64, u32)> {
        self.resolved
    }
    // A resolution belongs to one height in one captured selected-chain view.
    // Repository insertion recomputes it from its own immutable snapshot.
    pub fn resolve(&self, height: u64, prior: signaling::Prior) -> Result<Self, ProfileError> {
        let mut profile = self.clone();
        profile.resolved = None;
        if let Some(fork) = self.fork {
            let version = fork.version_at(height, self.scheduled_protocol_at(height), prior)?;
            profile.resolved = Some((height, version));
        }
        Ok(profile)
    }
    pub fn network_id(&self) -> &[u8] {
        &self.network_id
    }
    // aec_governance:network_block_gas_limit: public chain limits do not
    // change with local configuration. Other deployments choose explicitly.
    pub fn block_gas_limit(&self, configured: &ae_types::Unsigned) -> ae_types::Unsigned {
        self.fixed_block_gas_limit()
            .map_or_else(|| configured.clone(), Into::into)
    }
    pub fn fixed_block_gas_limit(&self) -> Option<u64> {
        match self.network_id.as_slice() {
            b"ae_mainnet" | b"ae_uat" => Some(6_000_000),
            _ => None,
        }
    }
    // Raw cb_ payloads from the pinned aecore pre-Iris ordering assets.
    // The VM decodes these with its bounded FATE decoder. Custom networks
    // have no bundled overrides; their caller may supply explicit entries.
    pub fn pre_iris_map_ordering(&self) -> &'static [(&'static [u8], &'static [u8])] {
        match self.network_id.as_slice() {
            b"ae_mainnet" => &[(
                include_bytes!("../data/mainnet_pre_iris_map.bin"),
                include_bytes!("../data/mainnet_pre_iris_list.bin"),
            )],
            b"ae_uat" => &[(
                include_bytes!("../data/uat_pre_iris_map.bin"),
                include_bytes!("../data/uat_pre_iris_list.bin"),
            )],
            _ => &[],
        }
    }
    pub fn activations(&self) -> &[Activation] {
        &self.activations
    }
    pub fn scheduled_protocol_at(&self, height: u64) -> Protocol {
        self.activations[self.activations.partition_point(|a| a.height <= height) - 1].protocol
    }
    pub fn version_at(&self, height: u64) -> Result<u32, ProfileError> {
        if self.fork.is_some_and(|fork| height >= fork.end) {
            if let Some((h, version)) = self.resolved
                && height == h
            {
                return Ok(version);
            }
            return Err(ProfileError::MissingForkDecision { height });
        }
        Ok(self.scheduled_protocol_at(height) as u32)
    }
    pub fn protocol_at(&self, height: u64) -> Result<Protocol, ProfileError> {
        Protocol::try_from(self.version_at(height)?)
    }
    pub fn validate_version(&self, height: u64, actual: u32) -> Result<Protocol, ProfileError> {
        let expected = self.protocol_at(height)?;
        if actual != expected as u32 {
            return Err(ProfileError::WrongProtocol { expected, actual });
        }
        Ok(expected)
    }
    pub fn signing_prefix(&self, sponsored_inner: bool) -> Vec<u8> {
        let mut prefix = self.network_id.clone();
        if sponsored_inner {
            prefix.extend_from_slice(b"-inner_tx");
        }
        prefix
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FateStoreGeneration {
    Lima,
    Ceres,
    Arcus,
}

impl Protocol {
    pub const fn fate_store_generation(self) -> FateStoreGeneration {
        match self {
            Self::Roma | Self::Minerva | Self::Fortuna | Self::Lima => FateStoreGeneration::Lima,
            Self::Iris | Self::Ceres => FateStoreGeneration::Ceres,
            Self::Arcus | Self::Salus => FateStoreGeneration::Arcus,
        }
    }
    pub const fn minimum_gas_price(self) -> u64 {
        if matches!(self, Self::Roma) {
            1
        } else {
            1_000_000
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_public_fork_boundary_is_exact() {
        for profile in [NetworkProfile::mainnet(), NetworkProfile::uat()] {
            for (i, activation) in profile.activations().iter().enumerate() {
                assert_eq!(
                    profile.protocol_at(activation.height).unwrap(),
                    activation.protocol
                );
                assert_eq!(
                    profile.protocol_at(activation.height + 1).unwrap(),
                    activation.protocol
                );
                if i > 0 {
                    assert_eq!(
                        profile.protocol_at(activation.height - 1).unwrap(),
                        profile.activations()[i - 1].protocol
                    );
                }
            }
            assert_eq!(profile.protocol_at(u64::MAX).unwrap(), Protocol::Ceres);
            assert!(profile.validate_version(1_500_000, 7).is_err());
        }
    }
    #[test]
    fn custom_profiles_cannot_override_public_networks() {
        let activations = vec![Activation {
            height: 0,
            protocol: Protocol::Ceres,
        }];
        assert_eq!(
            NetworkProfile::custom(b"ae_mainnet".to_vec(), activations.clone()),
            Err(ProfileError::ReservedNetworkId)
        );
        assert!(NetworkProfile::custom(b"ae_dev".to_vec(), activations).is_ok());
        assert!(NetworkProfile::custom(b"ae_dev".to_vec(), vec![]).is_err());
    }
    #[test]
    fn historical_store_and_signature_domains_are_explicit() {
        assert_eq!(
            Protocol::Lima.fate_store_generation(),
            FateStoreGeneration::Lima
        );
        assert_eq!(
            Protocol::Iris.fate_store_generation(),
            FateStoreGeneration::Ceres
        );
        assert_eq!(
            Protocol::Ceres.fate_store_generation(),
            FateStoreGeneration::Ceres
        );
        assert_eq!(
            Protocol::Arcus.fate_store_generation(),
            FateStoreGeneration::Arcus
        );
        assert_eq!(
            NetworkProfile::mainnet().signing_prefix(true),
            b"ae_mainnet-inner_tx"
        );
        assert_eq!(
            NetworkProfile::mainnet().signing_prefix(false),
            b"ae_mainnet"
        );
    }
}
