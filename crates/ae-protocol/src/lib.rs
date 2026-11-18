#![forbid(unsafe_code)]
// Activation reference: aeternity fa2a6d7, aec_hard_forks.erl.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use ae_types::Hash256;

pub mod names;

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
        })
    }
    pub fn network_id(&self) -> &[u8] {
        &self.network_id
    }
    pub fn activations(&self) -> &[Activation] {
        &self.activations
    }
    pub fn protocol_at(&self, height: u64) -> Protocol {
        self.activations[self.activations.partition_point(|a| a.height <= height) - 1].protocol
    }
    pub fn validate_version(&self, height: u64, actual: u32) -> Result<Protocol, ProfileError> {
        let expected = self.protocol_at(height);
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
                assert_eq!(profile.protocol_at(activation.height), activation.protocol);
                assert_eq!(
                    profile.protocol_at(activation.height + 1),
                    activation.protocol
                );
                if i > 0 {
                    assert_eq!(
                        profile.protocol_at(activation.height - 1),
                        profile.activations()[i - 1].protocol
                    );
                }
            }
            assert_eq!(profile.protocol_at(u64::MAX), Protocol::Ceres);
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
