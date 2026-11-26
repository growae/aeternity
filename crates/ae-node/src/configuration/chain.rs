// Fixed startup choices, assembled before a database or worker is opened.
use super::{
    consensus::{self, Checked, Kind},
    protocols,
    resolution::Resolved,
};
use ae_chain::repository::Identity;
use ae_codec::headers::{CommonHeader, Header, KeyHeader};
use ae_protocol::{ConsensusAlgorithm, NetworkProfile};
use ae_types::Hash256;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(super::resolution::Error),
    Consensus(consensus::Error),
    Protocols(protocols::Error),
    GenesisTime,
    MicroCycle,
    Genesis,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "chain startup configuration error: {self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone)]
pub struct Settings {
    profile: NetworkProfile,
    consensus: Checked,
    genesis_time: u64,
    expected_mine_rate: serde_json::Value,
    micro_cycle_ms: u64,
}
impl Settings {
    pub fn resolve(
        resolved: &Resolved<'_>,
        command_line_network: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let network = resolved
            .network_id(command_line_network)
            .map_err(Error::Resolution)?;
        let consensus = resolved
            .consensus(&network)
            .map_err(Error::Consensus)?
            .check()
            .map_err(Error::Consensus)?;
        let fork = resolved
            .get(&["fork_management", "fork"])
            .map_err(Error::Resolution)?;
        let profile = resolved
            .protocols(&network)
            .map_err(Error::Protocols)?
            .build(consensus.genesis().kind(), fork)
            .map_err(Error::Protocols)?;
        let genesis_time = if consensus.genesis().kind() == Kind::Hyperchain {
            // Native HC accessors read user chain.consensus."0" even when
            // the selected schedule came from application configuration.
            resolved
                .get(&["chain", "consensus", "0", "config", "genesis_start_time"])
                .map_err(Error::Resolution)?
                .map_or(Ok(0), |v| {
                    protocols::integer(v)
                        .and_then(|n| u64::try_from(n).ok())
                        .ok_or(Error::GenesisTime)
                })?
        } else {
            0
        };
        Ok(Self {
            profile,
            consensus,
            genesis_time,
            micro_cycle_ms: micro_block_cycle(resolved)
                .map_err(Error::Resolution)?
                .as_u64()
                .filter(|v| *v <= 86_400_000)
                .ok_or(Error::MicroCycle)?,
            // This governance value is not part of canonical chain identity.
            // Preserve the original consumer's user/application/default order
            // and integer domain; status arithmetic handles invalid app values.
            expected_mine_rate: resolved
                .user_or_application(
                    &["mining", "expected_mine_rate"],
                    "aecore",
                    "expected_mine_rate",
                    &serde_json::json!(180_000),
                )
                .map_err(Error::Resolution)?,
        })
    }
    pub fn profile(&self) -> &NetworkProfile {
        &self.profile
    }
    pub fn consensus(&self) -> &Checked {
        &self.consensus
    }
    pub fn expected_mine_rate(&self) -> &serde_json::Value {
        &self.expected_mine_rate
    }
    pub fn micro_cycle_ms(&self) -> u64 {
        self.micro_cycle_ms
    }
    pub fn retarget_rate(&self) -> ae_consensus::target::MineRate {
        protocols::integer(&self.expected_mine_rate).map_or(
            ae_consensus::target::MineRate::NonInteger,
            ae_consensus::target::MineRate::Integer,
        )
    }
    pub fn genesis_header(&self, state_hash: Hash256) -> KeyHeader {
        let target = match self.consensus.genesis().kind() {
            Kind::PowCuckoo if self.profile.network_id() == b"ae_mainnet" => 0x1f1f1f1f,
            Kind::PowCuckoo => 0x2100ffff,
            Kind::OnDemand => 1338,
            Kind::Hyperchain => 0,
            Kind::Unknown => unreachable!("checked consensus"),
        };
        KeyHeader {
            common: CommonHeader {
                version: self.profile.scheduled_protocol_at(0) as u32,
                height: 0,
                previous_hash: ae_state::genesis::CONTRIBUTORS_MESSAGES_HASH,
                previous_key_hash: [0; 32],
                state_hash: state_hash.0,
            },
            miner: [0; 32],
            beneficiary: [0; 32],
            target,
            seal: [0; 42],
            nonce: 0,
            time: self.genesis_time,
            info: None,
            hole: false,
            end_of_epoch: false,
        }
    }
    // Execution choices remain an explicit caller-owned binding. This
    // method does not invent a VM profile or read host architecture defaults.
    pub fn identity(
        &self,
        state_hash: Hash256,
        execution_profile: Hash256,
    ) -> Result<Identity, Error> {
        let header = Header::Key(self.genesis_header(state_hash));
        let hash = header.hash().map_err(|_| Error::Genesis)?;
        let public = match self.profile.network_id() {
            b"ae_mainnet" => Some(ae_state::genesis::Network::Mainnet),
            b"ae_uat" => Some(ae_state::genesis::Network::Uat),
            _ => None,
        };
        if let Some(network) = public
            && ae_state::genesis::header(network).map_err(|_| Error::Genesis)? != header
        {
            return Err(Error::Genesis);
        }
        let algorithm = match self.consensus.genesis().kind() {
            Kind::PowCuckoo => ConsensusAlgorithm::Cuckoo,
            Kind::OnDemand => ConsensusAlgorithm::OnDemand,
            Kind::Hyperchain => ConsensusAlgorithm::Hyperchain,
            Kind::Unknown => unreachable!("checked consensus"),
        };
        Ok(Identity::for_algorithm(
            &self.profile,
            hash,
            execution_profile,
            algorithm,
        ))
    }
}

// aec_governance:micro_block_cycle reads user configuration, application
// environment, then the governance default (not the schema default).
pub fn micro_block_cycle(
    resolved: &Resolved<'_>,
) -> Result<serde_json::Value, super::resolution::Error> {
    resolved.user_or_application(
        &["mining", "micro_block_cycle"],
        "aecore",
        "micro_block_cycle",
        &serde_json::json!(3000),
    )
}
