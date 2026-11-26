use super::Open;
use crate::configuration::{consensus, presets};
use ae_chain::{hyperchain::Rules, repository as repo};
use ae_hyperchain::{block, chain::Profile, eoe::local_host};
use ae_protocol::{ConsensusAlgorithm, ConsensusType};
use ae_storage::pool;
use ae_sync::chain::hyperchain::{conductor, configuration, eoe};
use ae_vm_fate::maps::order::Otp26;
use std::sync::Arc;

#[derive(Debug)]
pub enum Error {
    Closed,
    Binding,
    Field(&'static str),
    Consensus(consensus::Error),
    Presets(presets::Error),
    Configuration(configuration::Error),
    Conductor(conductor::Error),
}
#[derive(Clone)]
pub struct Limits {
    pub ledger: repo::Limits,
    pub leader: ae_hyperchain::leader::Limits,
    pub body: ae_chain::admission::Limits,
    pub micro_cycle_ms: Option<u64>,
    pub reward_delay: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            ledger: Default::default(),
            leader: Default::default(),
            body: Default::default(),
            micro_cycle_ms: None,
            reward_delay: ae_protocol::coinbase::REWARD_DELAY,
        }
    }
}
pub struct Prepared {
    open: Open,
    execution: repo::HyperchainConfig<Otp26>,
    plugin: configuration::Configuration,
}
pub struct Failure {
    pub error: Error,
    pub open: Open,
}
impl std::fmt::Debug for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperchainStartupFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
pub struct Active {
    pub open: Open,
    pub conductor: conductor::Conductor<Otp26>,
}
impl Prepared {
    pub fn new(open: Open, limits: Limits) -> Result<Self, Box<Failure>> {
        let build = || {
            let micro_cycle_ms = limits
                .micro_cycle_ms
                .unwrap_or(open.chain().settings().micro_cycle_ms());
            let identity = open.chain().identity();
            if identity.algorithm != ConsensusAlgorithm::Hyperchain
                || identity.execution_profile != open.execution().fingerprint()
                || open.execution().execution().consensus() != ConsensusType::Pos
                || limits.ledger.history == 0
                || micro_cycle_ms > 86_400_000
            {
                return Err(Error::Binding);
            }
            let specification = open.chain().settings().consensus().genesis();
            let plugin = specification.hyperchain().map_err(Error::Consensus)?;
            let presets = presets::hyperchain::Settings::from_value(
                specification.config(),
                Default::default(),
            )
            .map_err(Error::Presets)?;
            let mut rules = Rules::new(
                identity.genesis,
                block::Config {
                    parent_epoch_length: plugin
                        .parent_epoch_length
                        .clone()
                        .ok_or(Error::Field("parent_epoch_length"))?,
                    parent_start: plugin.cache.child_start_height.clone(),
                    parent_finality: plugin.cache.finality.clone(),
                    pin_sync_margin: plugin.pin_sync_margin.clone(),
                    child_block_time: plugin.child_block_time.clone(),
                    cache_limits: limits.leader,
                },
            );
            rules.micro_cycle_ms = micro_cycle_ms;
            rules.configured_gas_limit = open.execution().effective_gas_limit().clone();
            rules.body_limits = limits.body;
            rules.reward_delay = limits.reward_delay;
            if let Some(value) = specification.config().get("fixed_coinbase") {
                rules.fixed_coinbase = presets::balance(value).map_err(Error::Presets)?;
            }
            if let Some(value) = specification.config().get("fee_distribution") {
                let values = value.as_array().ok_or(Error::Field("fee_distribution"))?;
                if values.len() != 3 {
                    return Err(Error::Field("fee_distribution"));
                }
                for (out, value) in rules.fee_distribution.iter_mut().zip(values) {
                    *out = presets::balance(value).map_err(Error::Presets)?;
                }
            }
            let owned = Arc::new(open.execution().execution().clone());
            let (profile, aevm) = owned.with_config(|base| {
                let fate = base.fate.expect("configured execution retains FATE");
                (
                    Arc::new(Profile {
                        contracts: presets.contracts,
                        network: base.network.clone(),
                        context: fate.context.clone(),
                        order: fate.order.clone(),
                        overrides: fate.overrides.clone(),
                        diagnostic: fate.diagnostic_profile,
                        limits: base.fate_limits,
                        maximum_steps: base.maximum_fate_steps,
                        maximum_history_reads: limits.ledger.history,
                    }),
                    base.aevm.cloned(),
                )
            });
            Ok((
                repo::HyperchainConfig {
                    identity: identity.clone(),
                    profile,
                    rules,
                    aevm,
                    owned: Some(owned),
                    limits: limits.ledger,
                },
                plugin,
            ))
        };
        match build() {
            Ok((execution, plugin)) => Ok(Self {
                open,
                execution,
                plugin,
            }),
            Err(error) => Err(Box::new(Failure { error, open })),
        }
    }
    pub fn configured(&self) -> &Open {
        &self.open
    }
    pub fn into_open(self) -> Open {
        self.open
    }
    pub fn execution(&self) -> &repo::HyperchainConfig<Otp26> {
        &self.execution
    }
    // Operational tuning remains explicit; plugin configuration supplies all
    // consensus identities/timings when this recipe is activated.
    pub fn runtime(&self) -> conductor::Config {
        let rules = &self.execution.rules.consensus;
        conductor::Config {
            eoe: eoe::owner::Config {
                eoe: eoe::Config {
                    host: local_host::Config {
                        network: self.execution.profile.network.clone(),
                        election_contract: self.execution.profile.contracts.election,
                        parent_epoch_length: rules.parent_epoch_length.clone(),
                        parent_start_height: rules.parent_start.clone(),
                        limits: Default::default(),
                    },
                    block_time: rules.child_block_time.clone(),
                    coordinator: Default::default(),
                    runtime: Default::default(),
                    epochs: Default::default(),
                },
                votes: Default::default(),
                pool: Default::default(),
                calls: Default::default(),
            },
            production: Default::default(),
            pool: pool::Config {
                consensus: ConsensusType::Pos,
                ..Default::default()
            },
            pool_worker: Default::default(),
            admission: Default::default(),
            snapshots: Default::default(),
            resilience: Some(Default::default()),
            gossip: Default::default(),
            parent: conductor::ParentConfig {
                pins: None,
                cache: self.plugin.cache.clone(),
                cache_runtime: Default::default(),
                connector: Default::default(),
                hosts: 0,
                seed: [0; 16],
            },
            events_per_step: 64,
            producing: false,
            block_producer: true,
        }
    }
    pub fn activate(
        self,
        runtime: conductor::Config,
        services: configuration::Services,
    ) -> Result<Active, Box<Failure>> {
        let Self {
            open,
            execution,
            plugin,
        } = self;
        let result = plugin
            .configure(execution, runtime, services)
            .map_err(Error::Configuration)
            .and_then(|p| {
                p.start(open.chain().database().clone())
                    .map_err(Error::Conductor)
            });
        match result {
            Ok(conductor) => Ok(Active { open, conductor }),
            Err(error) => Err(Box::new(Failure { error, open })),
        }
    }
}
