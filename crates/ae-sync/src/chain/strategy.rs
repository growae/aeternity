use super::*;
use ae_codec::headers::Header;
use ae_protocol::NetworkProfile;

pub(super) struct Pow<O> {
    pub algorithm: ae_protocol::ConsensusAlgorithm,
    pub execution: Owned<O>,
    pub rules: Rules,
}
pub(super) enum Strategy<O> {
    Pow(Box<Pow<O>>),
    Hyperchain(Box<hyperchain::Strategy>),
}
impl<O: MapOrder> Strategy<O> {
    pub fn mode(&self) -> ae_codec::headers::HeaderMode {
        match self {
            Self::Pow(_) => ae_codec::headers::HeaderMode::ProofOfWork,
            Self::Hyperchain(_) => ae_codec::headers::HeaderMode::Hyperchain,
        }
    }
    pub fn network(&self) -> &NetworkProfile {
        match self {
            Self::Pow(pow) => pow.execution.network(),
            Self::Hyperchain(hc) => &hc.network,
        }
    }
    pub fn genesis(&self) -> Hash256 {
        match self {
            Self::Pow(pow) => pow.rules.genesis_hash,
            Self::Hyperchain(hc) => hc.rules.genesis_hash,
        }
    }
    pub fn checkpoints(&self) -> &BTreeMap<u64, Hash256> {
        match self {
            Self::Pow(pow) => &pow.rules.checkpoints,
            Self::Hyperchain(hc) => &hc.checkpoints,
        }
    }
    pub fn micro_cycle_ms(&self) -> u64 {
        match self {
            Self::Pow(pow) => pow.rules.admission.micro_cycle_ms,
            Self::Hyperchain(hc) => hc.rules.micro_cycle_ms,
        }
    }
    pub fn validate(
        &self,
        block: &Block,
        previous: Option<&Header>,
        network: &NetworkProfile,
        now_ms: u64,
    ) -> Result<(), Error> {
        match self {
            Self::Pow(pow) => {
                let mut rules = pow.rules.admission.clone();
                rules.now_ms = now_ms;
                let validate = if pow.algorithm == ae_protocol::ConsensusAlgorithm::OnDemand {
                    ae_chain::admission::validate_on_demand
                } else {
                    ae_chain::admission::validate_pow
                };
                validate(block, network, previous, &rules).map_err(|e| Error::Validation {
                    operational: e.is_operational(),
                    reason: format!("{e:?}"),
                })
            }
            Self::Hyperchain(hc) => {
                let mut rules = hc.rules.clone();
                rules.now_ms = now_ms;
                ae_chain::hyperchain::standalone(block, previous, network, &rules).map_err(|e| {
                    Error::Validation {
                        operational: e.is_operational(),
                        reason: format!("{e:?}"),
                    }
                })
            }
        }
    }
}
