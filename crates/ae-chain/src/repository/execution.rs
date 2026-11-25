// Shared immutable execution views for PoW and HC; consensus is explicit.
// aec_chain_hc run environments and aetx_env:tx_env_and_trees_from_hash,
// aeternity fa2a6d7. Copyright (c) Aeternity. ISC: repository LICENSE.
use super::{Error, Identity, Read, View, vm_history};
use crate::block::Block;
use ae_codec::headers::Header;
use ae_execution::{Context, contracts};
use ae_hyperchain::chain::{self, LookupError, source};
use ae_protocol::{ConsensusAlgorithm, ConsensusType, NetworkProfile, Protocol};
use ae_trie::{Overlay, OverlayLimits};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use ae_vm_fate::machine::program::remote::context::{Consensus, Environment};
use num_bigint::{BigInt, Sign};
use std::sync::Arc;

pub use ae_hyperchain::chain::Profile;
pub type Reader<S, O> = chain::Reader<Overlay<S>, Overlay<S>, O, Snapshot<S>>;
pub type ContractView<S> = chain::View<Overlay<S>, Overlay<S>>;

#[derive(Debug, Clone)]
pub struct Config {
    pub overlays: OverlayLimits,
    pub maximum_history_headers: usize,
    pub configured_block_gas_limit: Unsigned,
    pub aevm: Option<contracts::aevm::Config>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            overlays: Default::default(),
            maximum_history_headers: 1_024,
            configured_block_gas_limit: 6_000_000.into(),
            aevm: None,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Snapshot<S> {
    repository: View<S>,
    config: Arc<Config>,
    gas_limit: BigInt,
}
// Services that need a transaction context can use the same binding as
// contract queries without manufacturing an unrelated contract-query profile.
pub fn snapshot<S: Read>(
    view: View<S>,
    expected: &Identity,
    network: &NetworkProfile,
    config: Config,
) -> Result<Snapshot<S>, Error> {
    if view.identity() != expected || !expected.matches(network) {
        return Err(Error::Identity);
    }
    let gas_limit = network.block_gas_limit(&config.configured_block_gas_limit);
    Ok(Snapshot {
        repository: view,
        config: Arc::new(config),
        gas_limit: BigInt::from_bytes_be(Sign::Plus, gas_limit.as_bytes()),
    })
}
pub(super) fn reader<S: Read, O>(
    view: View<S>,
    expected: &Identity,
    consensus: ConsensusType,
    profile: Arc<Profile<O>>,
    config: Config,
) -> Result<Reader<S, O>, Error> {
    if view.identity() != expected
        || expected.consensus != consensus
        || !expected.matches(&profile.network)
        || profile.context.in_auth
        || !profile.context.on_chain
        || profile.context.reserved_gas != BigInt::from(0)
    {
        return Err(Error::Identity);
    }
    Ok(chain::Reader::new(
        snapshot(view, expected, &profile.network, config)?,
        profile,
    ))
}
fn backend(error: Error) -> LookupError {
    match error {
        Error::ResourceLimit | Error::Trie(ae_trie::Error::ResourceLimit) => {
            LookupError::ResourceLimit
        }
        Error::Pruned { .. } => LookupError::StateGarbageCollected,
        error => LookupError::Backend(Arc::new(error)),
    }
}
impl<S: Read> View<S> {
    pub fn hyperchain_key_hash(&self, height: &BigInt) -> Result<Option<Hash256>, Error> {
        let Ok(height) = u64::try_from(height) else {
            return Ok(None);
        };
        let Some(hash) = self.canonical_key(height)? else {
            return Ok(None);
        };
        let block = self.require_block(hash)?;
        if !matches!(block.header(), Header::Key(k) if k.common.height == height)
            || !self.is_canonical(hash)?
        {
            return Err(Error::Corrupt("canonical key"));
        }
        Ok(Some(hash))
    }
}
impl<S: Read> Snapshot<S> {
    pub fn repository(&self) -> &View<S> {
        &self.repository
    }
    pub fn contract_view(&self, hash: Hash256) -> Result<Arc<ContractView<S>>, LookupError> {
        let view = &self.repository;
        let block = view
            .block(hash)
            .map_err(backend)?
            .ok_or(LookupError::InvalidHash)?;
        let (key, key_hash, timestamp) = match &block {
            Block::Key(k) => (k.clone(), hash, BigInt::from(k.time) + 1),
            Block::Micro(m) => {
                let key_hash = Hash256(m.header.common.previous_key_hash);
                let Block::Key(k) = view.require_block(key_hash).map_err(backend)? else {
                    return Err(backend(Error::Corrupt("micro leader type")));
                };
                // Hash queries take height and protocol from the leader key.
                // HC micro blocks can advance height before the next key;
                // admission checks belong to insertion, not this lookup.
                (k, key_hash, BigInt::from(m.header.time))
            }
        };
        let protocol = Protocol::try_from(key.common.version)
            .map_err(|_| backend(Error::Corrupt("context protocol")))?;
        let trees = match view.trees(hash, self.config.overlays) {
            Ok(t) => t,
            Err(Error::Missing(h)) if h == hash => return Err(LookupError::StateGarbageCollected),
            Err(e) => return Err(backend(e)),
        };
        let history =
            Arc::new(vm_history(view, &key, self.config.maximum_history_headers).map_err(backend)?);
        let (consensus, difficulty) = match view.identity().algorithm {
            ConsensusAlgorithm::Hyperchain => (Consensus::Pos, BigInt::from(key.target)),
            ConsensusAlgorithm::OnDemand => (Consensus::Pow, ae_consensus::on_demand::TAG.into()),
            ConsensusAlgorithm::Cuckoo => {
                let difficulty = ae_consensus::target::Target(key.target)
                    .difficulty()
                    .map_err(|_| backend(Error::Corrupt("PoW key target")))?;
                (
                    Consensus::Pow,
                    BigInt::from_bytes_be(Sign::Plus, difficulty.as_bytes()),
                )
            }
        };
        let environment = Environment {
            auth_tx_hash: None,
            origin: [0; 32],
            gas_price: 0.into(),
            fee: 0.into(),
            beneficiary: key.beneficiary,
            timestamp: timestamp.clone(),
            difficulty: difficulty.clone(),
            block_gas_limit: self.gas_limit.clone(),
            network_id: view.identity().network.clone(),
            consensus,
            key_hash: key_hash.0,
        };
        let aevm = self.config.aevm.as_ref().map(|template| {
            let mut a = template.clone();
            a.environment.genesis_height = 0;
            a.environment.key_hash = key_hash.0;
            a.environment.history = history.clone();
            a.environment.beneficiary = AccountPublicKey(key.beneficiary);
            a.environment.difficulty = difficulty;
            a.environment.timestamp_ms = timestamp;
            a.environment.block_gas_limit = self.gas_limit.clone();
            a.environment.auth_tx_hash = None;
            a.network_id = view.identity().network.clone().into();
            a.event_context = ae_execution::aevm_chain::EventContext::None;
            a
        });
        Ok(Arc::new(chain::View {
            trees: Arc::new(trees),
            context: Context::transaction(key.common.height, protocol),
            environment,
            history,
            aevm,
        }))
    }
}
impl<S: Read> source::Source<Overlay<S>, Overlay<S>> for Snapshot<S> {
    type Snapshot = Self;
    fn capture(&self) -> Result<Self, LookupError> {
        Ok(self.clone())
    }
}
impl<S: Read> source::Read<Overlay<S>, Overlay<S>> for Snapshot<S> {
    fn top_height(&self) -> Result<Option<BigInt>, LookupError> {
        self.repository
            .status()
            .tip
            .map(|hash| {
                self.repository
                    .require_block(hash)
                    .map(|b| b.header().common().height.into())
                    .map_err(backend)
            })
            .transpose()
    }
    fn key_hash(&self, height: &BigInt) -> Result<Option<Vec<u8>>, LookupError> {
        self.repository
            .hyperchain_key_hash(height)
            .map(|h| h.map(|h| h.0.to_vec()))
            .map_err(backend)
    }
    fn view(&self, hash: &[u8]) -> Result<Arc<ContractView<S>>, LookupError> {
        let hash = Hash256(hash.try_into().map_err(|_| LookupError::InvalidHash)?);
        self.contract_view(hash)
    }
}
