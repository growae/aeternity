use super::{Config, contracts};
use ae_protocol::{ConsensusType, NetworkProfile};
use ae_vm_fate::{
    machine,
    maps::{MapOrder, listing::Overrides, order::Otp26},
};
use std::{collections::BTreeSet, sync::Arc};

// Keep every execution choice explicit. There is no host-derived or mainnet
// default for the historical VM profiles. Ledger preparation replaces history
// and block context from its captured database view before executing a block.
#[derive(Clone)]
pub struct Owned<O = Otp26> {
    control: Option<ae_types::work::Control>,
    fate_control: Option<ae_types::work::Control>,
    name_claim_bid_timeout: Option<ae_types::Unsigned>,
    network: NetworkProfile,
    consensus: ConsensusType,
    context: machine::Context,
    order: O,
    overrides: Overrides,
    environment: machine::program::remote::context::Environment,
    history: Arc<crate::chain_history::Snapshot>,
    auth_tx: Option<Arc<ae_tx::WireTransaction>>,
    metering_floor: Option<u32>,
    diagnostic_profile: ae_vm_fate::diagnostic::Profile,
    aevm: contracts::aevm::Config,
    signature_limits: super::SignatureLimits,
    channel_limits: crate::channels::unilateral::Limits,
    channel_state_atoms: Option<BTreeSet<Vec<u8>>>,
    oracle_limits: crate::oracles::abi::Limits,
    fate_limits: ae_vm_fate::Limits,
    maximum_fate_steps: usize,
    aevm_limits: ae_vm_aevm::execution::Limits,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    MissingFate,
    MissingAevm,
}
impl<O: Clone> Owned<O> {
    pub fn capture(config: &Config<'_, O>) -> Result<Self, Error> {
        let fate = config.fate.ok_or(Error::MissingFate)?;
        let aevm = config.aevm.ok_or(Error::MissingAevm)?;
        Ok(Self {
            control: config.control.cloned(),
            fate_control: fate.control.clone(),
            name_claim_bid_timeout: config.name_claim_bid_timeout.cloned(),
            network: config.network.clone(),
            consensus: config.consensus,
            context: fate.context.clone(),
            order: fate.order.clone(),
            overrides: fate.overrides.clone(),
            environment: fate.environment.clone(),
            history: fate.history.clone(),
            auth_tx: fate.auth_tx.clone(),
            metering_floor: fate.metering_floor,
            diagnostic_profile: fate.diagnostic_profile,
            aevm: aevm.clone(),
            signature_limits: config.signature_limits,
            channel_limits: config.channel_limits,
            channel_state_atoms: config.channel_state_atoms.cloned(),
            oracle_limits: config.oracle_limits,
            fate_limits: config.fate_limits,
            maximum_fate_steps: config.maximum_fate_steps,
            aevm_limits: config.aevm_limits,
        })
    }
}
impl<O: MapOrder> Owned<O> {
    pub fn network(&self) -> &NetworkProfile {
        &self.network
    }
    pub fn consensus(&self) -> ConsensusType {
        self.consensus
    }
    pub fn with_config<R>(&self, f: impl FnOnce(&Config<'_, O>) -> R) -> R {
        let fate = contracts::fate::Config {
            control: self.fate_control.clone(),
            context: &self.context,
            order: &self.order,
            overrides: &self.overrides,
            environment: &self.environment,
            history: self.history.clone(),
            auth_tx: self.auth_tx.clone(),
            metering_floor: self.metering_floor,
            diagnostic_profile: self.diagnostic_profile,
        };
        f(&Config {
            control: self.control.as_ref(),
            name_claim_bid_timeout: self.name_claim_bid_timeout.as_ref(),
            network: &self.network,
            consensus: self.consensus,
            fate: Some(&fate),
            aevm: Some(&self.aevm),
            signature_limits: self.signature_limits,
            channel_limits: self.channel_limits,
            channel_state_atoms: self.channel_state_atoms.as_ref(),
            oracle_limits: self.oracle_limits,
            fate_limits: self.fate_limits,
            maximum_fate_steps: self.maximum_fate_steps,
            aevm_limits: self.aevm_limits,
        })
    }
}
