// Selected-view execution of aec_consensus_hc:next_producer. The storage owner
// serializes the same leader cache used by block insertion; no coordinator is
// cloned here. Parent notifications and negotiation remain staged output.
use super::*;
use ae_hyperchain::{block, eoe, parent_cache::Cache, producer as core, signing::Keys};
use ae_vm_fate::{maps::MapOrder, value::ObjectPayload};

#[derive(Debug, Clone)]
pub struct Config {
    pub production_time: BigInt,
    pub maximum_stall_reads: usize,
    pub maximum_negotiation_bytes: usize,
    pub inputs: inputs::Limits,
}
#[derive(Debug, Clone)]
pub struct Input {
    pub now: BigInt,
    pub config: Config,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub network: ae_protocol::NetworkProfile,
    pub election: [u8; 32],
    pub parent_start: Unsigned,
    pub parent_length: Unsigned,
    pub block_time: Unsigned,
}
impl Binding {
    pub fn matches<O>(&self, config: &super::super::HyperchainConfig<O>) -> bool {
        self.network == config.profile.network
            && self.election == config.profile.contracts.election
            && self.parent_start == config.rules.consensus.parent_start
            && self.parent_length == config.rules.consensus.parent_epoch_length
            && self.block_time == config.rules.consensus.child_block_time
    }
}
impl Default for Config {
    fn default() -> Self {
        Self {
            production_time: 100.into(),
            maximum_stall_reads: 4096,
            maximum_negotiation_bytes: 16 << 20,
            inputs: Default::default(),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Repository,
    View,
    Block,
    Chain,
    Host,
    Badarith,
    ResourceLimit,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub kind: Kind,
    pub operational: bool,
    pub reason: String,
}
impl Failure {
    fn new(kind: Kind, operational: bool, reason: impl std::fmt::Debug) -> Self {
        let mut reason = format!("{reason:?}");
        if reason.len() > 4096 {
            let end = (0..=4096)
                .rev()
                .find(|n| reason.is_char_boundary(*n))
                .unwrap();
            reason.truncate(end);
        }
        Self {
            kind,
            operational,
            reason,
        }
    }
    pub fn repository(error: Error) -> Self {
        Self::new(Kind::Repository, true, error)
    }
}
pub struct Prepared {
    pub status: super::super::Status,
    pub source_id: u64,
    pub key_hash: Hash256,
    pub key_height: u64,
    pub decision: Result<core::Decision, Failure>,
    pub keys: Keys,
    pub parents: inputs::Effects,
    pub negotiation: Option<eoe::Negotiation>,
}
enum HostError {
    Input(inputs::InputError),
    Repository(Error),
    MissingTime(BigInt),
    Negotiation,
    ResourceLimit,
}
impl std::fmt::Debug for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Input(e) => f.debug_tuple("Input").field(e).finish(),
            Self::Repository(e) => f.debug_tuple("Repository").field(e).finish(),
            Self::MissingTime(h) => f.debug_tuple("MissingTime").field(h).finish(),
            Self::Negotiation => f.write_str("InvalidNegotiation"),
            Self::ResourceLimit => f.write_str("ResourceLimit"),
        }
    }
}
#[derive(Clone)]
struct Host<S> {
    view: View<S>,
    parent: inputs::Captured<S>,
    keys: Keys,
    negotiation: Option<eoe::Negotiation>,
    maximum_negotiation_bytes: usize,
}
fn address(key: &ObjectPayload) -> Option<AccountPublicKey> {
    match key {
        ObjectPayload::Bytes(b) => b.as_slice().try_into().ok().map(AccountPublicKey),
        _ => None,
    }
}
impl<S: Read> block::Host for Host<S> {
    type Error = HostError;
    fn is_operational(_: &Self::Error) -> bool {
        true
    }
    fn entropy(&mut self, epoch: &BigInt) -> Result<Option<Vec<u8>>, Self::Error> {
        self.parent.entropy(epoch).map_err(HostError::Input)
    }
    fn pin(&mut self, hash: &[u8]) -> Result<Option<block::Pin>, Self::Error> {
        self.parent.pin(hash).map_err(HostError::Input)
    }
    fn key_hash(&mut self, height: &BigInt) -> Result<Option<Vec<u8>>, Self::Error> {
        self.parent.key_hash(height).map_err(HostError::Input)
    }
}
impl<S: Read> core::Host for Host<S> {
    fn key_time(&mut self, height: &BigInt) -> Result<BigInt, Self::Error> {
        let hash = self
            .view
            .hyperchain_key_hash(height)
            .map_err(HostError::Repository)?
            .ok_or_else(|| HostError::MissingTime(height.clone()))?;
        match self
            .view
            .require_block(hash)
            .map_err(HostError::Repository)?
        {
            Block::Key(key) => Ok(key.time.into()),
            _ => Err(HostError::Repository(Error::Corrupt(
                "producer timestamp key",
            ))),
        }
    }
    fn key_present(&mut self, key: &ObjectPayload) -> Result<bool, Self::Error> {
        Ok(address(key).is_some_and(|key| self.keys.contains(key)))
    }
    fn set_candidate(&mut self, key: &ObjectPayload) -> Result<bool, Self::Error> {
        Ok(address(key).is_some_and(|key| self.keys.set_candidate(key).is_ok()))
    }
    fn negotiate(&mut self, negotiation: core::Negotiation) -> Result<(), Self::Error> {
        let input: eoe::Negotiation = negotiation.try_into().map_err(|_| HostError::Negotiation)?;
        let mut bytes = std::mem::size_of::<eoe::Negotiation>();
        for n in [
            input.epoch.as_bytes().len(),
            input.height.as_bytes().len(),
            input.current_length.as_bytes().len(),
            input.hash.len(),
            input.seed.as_ref().map_or(0, Vec::len),
        ] {
            bytes = bytes.checked_add(n).ok_or(HostError::ResourceLimit)?;
        }
        for (_, stake) in &input.validators {
            bytes = bytes
                .checked_add(32)
                .and_then(|n| n.checked_add(stake.as_bytes().len()))
                .ok_or(HostError::ResourceLimit)?;
        }
        if bytes > self.maximum_negotiation_bytes || self.negotiation.is_some() {
            return Err(HostError::ResourceLimit);
        }
        self.negotiation = Some(input);
        Ok(())
    }
}
// The supplied view is a real immutable repository snapshot. The top-key
// height and the selected transaction view intentionally differ after an HC
// microblock: slot comparison uses the leader key, contracts use the top.
pub fn prepare<S: Read + std::fmt::Debug, O: MapOrder + Clone>(
    view: &View<S>,
    execution: &super::super::HyperchainConfig<O>,
    config: &Config,
    now: &BigInt,
    state: &mut block::State,
    keys: Keys,
    parent: Arc<Cache>,
) -> Result<Prepared, Failure> {
    let repository = Failure::repository;
    let maximum_integer = config.inputs.maximum_integer_bytes;
    if usize::try_from(now.bits().div_ceil(8)).unwrap_or(usize::MAX) > maximum_integer
        || usize::try_from(config.production_time.bits().div_ceil(8)).unwrap_or(usize::MAX)
            > maximum_integer
        || config.maximum_negotiation_bytes == 0
        || config.maximum_negotiation_bytes > 1 << 30
    {
        return Err(repository(Error::ResourceLimit));
    }
    let top = view
        .status()
        .tip
        .ok_or_else(|| repository(Error::Uninitialized))?;
    let selected = view.require_block(top).map_err(repository)?;
    let (key_hash, key_height) = match &selected {
        Block::Key(k) => (top, k.common.height),
        Block::Micro(m) => {
            let hash = Hash256(m.header.common.previous_key_hash);
            let Block::Key(key) = view.require_block(hash).map_err(repository)? else {
                return Err(repository(Error::Corrupt("producer leader key")));
            };
            (hash, key.common.height)
        }
    };
    let limits = execution.limits;
    let reader = super::reader(
        view.clone(),
        &execution.identity,
        execution.profile.clone(),
        super::Config {
            overlays: limits.overlay,
            maximum_history_headers: limits.history,
            configured_block_gas_limit: execution.rules.configured_gas_limit.clone(),
            aevm: execution.aevm.clone(),
        },
    )
    .map_err(repository)?;
    let run = reader
        .view(&chain::RunEnv::Hash(top.0.to_vec()))
        .map_err(|e| Failure::new(Kind::View, e.is_operational(), e))?;
    let consensus = &execution.rules.consensus;
    let parent = inputs::Captured::new(
        view.clone(),
        parent,
        Arc::new(Default::default()),
        consensus.parent_start.clone(),
        consensus.parent_epoch_length.clone(),
        Default::default(),
        config.inputs,
    )
    .map_err(|e| Failure::new(Kind::Host, true, e))?;
    let mut host = Host {
        view: view.clone(),
        parent,
        keys,
        negotiation: None,
        maximum_negotiation_bytes: config.maximum_negotiation_bytes,
    };
    let decision = core::next(
        state,
        &reader,
        &run,
        &key_height.into(),
        now,
        &mut host,
        &core::Config {
            block_time: BigInt::from_bytes_be(Sign::Plus, consensus.child_block_time.as_bytes()),
            production_time: config.production_time.clone(),
            maximum_stall_reads: config.maximum_stall_reads,
            cache_limits: consensus.cache_limits,
        },
    )
    .map_err(|e| {
        let kind = match &e {
            core::Error::Block(_) => Kind::Block,
            core::Error::Chain(_) => Kind::Chain,
            core::Error::Host(_) => Kind::Host,
            core::Error::Badarith => Kind::Badarith,
            core::Error::ResourceLimit => Kind::ResourceLimit,
        };
        Failure::new(kind, e.is_operational::<Host<S>>(), e)
    });
    Ok(Prepared {
        status: view.status(),
        source_id: view.source().source_id(),
        key_hash,
        key_height,
        decision,
        keys: host.keys,
        parents: host.parent.into_effects(),
        negotiation: host.negotiation,
    })
}
