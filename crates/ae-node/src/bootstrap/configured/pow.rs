// Configured PoW/on-demand service inputs share one importer and VM binding.
// The enclosing runtime owns service startup order, retirement and shutdown.
use super::Open;
use crate::configuration::resolution::ApplicationMode;
use ae_chain::{admission, key::Rules, repository::pow as repository};
use ae_execution::transactions::owned::Owned;
use ae_protocol::ConsensusAlgorithm;
use ae_storage::{pool::worker as pool, snapshots};
use ae_sync::{
    chain::{
        self, on_demand,
        pending::service as pending,
        pow::{generator, group, packing, producer},
    },
    keys, observe,
    runtime::{connection, workers},
};
use ae_types::AccountPublicKey;
use ae_vm_fate::maps::order::Otp26;
use std::{sync::Arc, time::Duration};

#[derive(Debug)]
pub enum Error {
    Binding,
    Mode,
    Configuration,
    Importer(chain::Error),
    Snapshots(snapshots::Error),
    Generator(generator::Error),
    Production(group::Error),
    OnDemand(on_demand::runtime::Error),
    Connections(connection::Error),
    Workers(workers::Error),
}
#[derive(Clone)]
pub struct Generator {
    pub key: repository::key::Limits,
    pub candidate: ae_chain::hyperchain::candidate::Limits,
    pub packing: packing::Config,
    pub controls: usize,
    pub publications: usize,
    pub queued_transactions: usize,
    pub queued_bytes: usize,
    pub feed_batch: usize,
    pub poll: Duration,
}
impl Default for Generator {
    fn default() -> Self {
        Self {
            key: Default::default(),
            candidate: Default::default(),
            packing: packing::Config {
                timeout: ae_chain::hyperchain::candidate::Timeout::Disabled,
                window_ms: 3000,
            },
            controls: 8,
            publications: 4,
            queued_transactions: 1000,
            queued_bytes: 1 << 20,
            feed_batch: 16,
            poll: Duration::from_millis(10),
        }
    }
}
#[derive(Clone)]
pub struct Production {
    pub generator: Generator,
    pub producer: producer::Config,
    pub mining: Option<producer::mining::Config>,
    pub poll: Duration,
    pub outputs: usize,
}
impl Default for Production {
    fn default() -> Self {
        Self {
            generator: Default::default(),
            producer: Default::default(),
            mining: None,
            poll: Duration::from_millis(10),
            outputs: 16,
        }
    }
}
pub struct Prepared {
    importer_config: chain::Config,
    snapshots: snapshots::Owner,
    execution: Arc<Owned>,
    rules: Rules,
    open: Open,
}
pub struct PreparationError {
    pub error: Error,
    pub open: Open,
}
impl std::fmt::Debug for PreparationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PowPreparationError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl Open {
    // Public-network fork/reward behavior stays in the existing chain engine.
    // Any explicit custom reward/fork policy remains the enclosing caller's input.
    pub fn pow_rules(&self, micro_cycle_ms: u64, body_limits: admission::Limits) -> Rules {
        let mut rules = Rules::new(
            admission::PowRules {
                edge_bits: self.chain().settings().cuckoo_edge_bits(),
                now_ms: 0,
                micro_cycle_ms,
                configured_gas_limit: num_bigint::BigInt::from_bytes_be(
                    num_bigint::Sign::Plus,
                    self.execution().effective_gas_limit().as_bytes(),
                ),
                body_limits,
            },
            self.chain().identity().genesis,
        );
        rules.checkpoints = self.checkpoints();
        rules.expected_mine_rate = self.chain().settings().retarget_rate();
        rules
    }
}
impl Prepared {
    pub fn new(
        open: Open,
        rules: Rules,
        importer: chain::Config,
        snapshots: snapshots::Config,
    ) -> Result<Self, Error> {
        Self::try_new(open, rules, importer, snapshots).map_err(|failure| failure.error)
    }
    pub fn try_new(
        open: Open,
        rules: Rules,
        importer: chain::Config,
        snapshots: snapshots::Config,
    ) -> Result<Self, Box<PreparationError>> {
        let expected = open.pow_rules(rules.admission.micro_cycle_ms, rules.admission.body_limits);
        if !matches!(
            open.chain().identity().algorithm,
            ConsensusAlgorithm::Cuckoo | ConsensusAlgorithm::OnDemand
        ) || open.chain().identity().execution_profile != open.execution().fingerprint()
            || rules.genesis_hash != expected.genesis_hash
            || rules.checkpoints != expected.checkpoints
            || rules.expected_mine_rate != expected.expected_mine_rate
            || rules.admission.configured_gas_limit != expected.admission.configured_gas_limit
        {
            return Err(Box::new(PreparationError {
                error: Error::Binding,
                open,
            }));
        }
        if rules.admission.micro_cycle_ms > 86_400_000 {
            return Err(Box::new(PreparationError {
                error: Error::Configuration,
                open,
            }));
        }
        let db = open.chain().database();
        let snapshots = match snapshots::Owner::start(db.clone(), snapshots) {
            Ok(owner) => owner,
            Err(error) => {
                return Err(Box::new(PreparationError {
                    error: Error::Snapshots(error),
                    open,
                }));
            }
        };
        let execution = Arc::new(open.execution().execution().clone());
        Ok(Self {
            importer_config: importer,
            snapshots,
            execution,
            rules,
            open,
        })
    }
    pub fn configured(&self) -> &Open {
        &self.open
    }
    pub(crate) fn set_gossip_depth(&mut self, depth: u64) {
        self.importer_config.ledger.policy.gossip_depth = depth;
    }
    pub fn into_open(self) -> Open {
        self.open
    }
    pub fn rules(&self) -> &Rules {
        &self.rules
    }
    pub fn snapshots(&self) -> &snapshots::Owner {
        &self.snapshots
    }
    // The independent worker owner may replace the database policy scope during
    // startup. Capture admission authority only after its initial publication.
    pub fn activate(self, workers: &workers::Handle) -> Result<Services, Box<ActivationError>> {
        let status = workers.status();
        let error = if !workers
            .bindings()
            .admission
            .uses_database(self.open.chain().database())
        {
            Some(Error::Binding)
        } else if !status.running || !status.ready || status.error.is_some() {
            Some(Error::Configuration)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(Box::new(ActivationError {
                error,
                prepared: self,
            }));
        }
        match chain::Importer::new(
            self.open.chain().database().clone(),
            self.execution.as_ref().clone(),
            self.rules.clone(),
            self.importer_config.clone(),
        ) {
            Ok(importer) => Ok(Services {
                importer: Arc::new(importer),
                prepared: self,
            }),
            Err(error) => Err(Box::new(ActivationError {
                error: Error::Importer(error),
                prepared: self,
            })),
        }
    }
    fn generator_config(&self, config: Generator) -> Result<generator::Config<Otp26>, Error> {
        if config.key.history == 0 {
            return Err(Error::Configuration);
        }
        let preparation = self.execution.with_config(|base| {
            let fate = base.fate.expect("bound execution retains FATE");
            repository::Config {
                identity: self.open.chain().identity().clone(),
                profile: Arc::new(repository::Profile {
                    contracts: ae_hyperchain::consensus_contract::Contracts {
                        owner: AccountPublicKey([0; 32]),
                        election: [0; 32],
                        staking: [0; 32],
                        rewards: [0; 32],
                    },
                    network: base.network.clone(),
                    context: fate.context.clone(),
                    order: fate.order.clone(),
                    overrides: fate.overrides.clone(),
                    diagnostic: fate.diagnostic_profile,
                    limits: base.fate_limits,
                    maximum_steps: base.maximum_fate_steps,
                    maximum_history_reads: config.key.history,
                }),
                execution: repository::Execution {
                    overlays: config.key.overlay,
                    maximum_history_headers: config.key.history,
                    configured_block_gas_limit: self.open.execution().effective_gas_limit().clone(),
                    aevm: base.aevm.cloned(),
                },
                micro_cycle_ms: self.rules.admission.micro_cycle_ms,
            }
        });
        Ok(generator::Config {
            preparation,
            candidate: config.candidate,
            packing: config.packing,
            controls: config.controls,
            publications: config.publications,
            queued_transactions: config.queued_transactions,
            queued_bytes: config.queued_bytes,
            feed_batch: config.feed_batch,
            poll_interval: config.poll,
        })
    }
    pub fn workers(
        &self,
        config: workers::Config,
        cache: pool::GossipCache,
    ) -> Result<(workers::Handle, workers::Service), Error> {
        if config.pool.consensus != ae_protocol::ConsensusType::Pow {
            return Err(Error::Binding);
        }
        workers::create(
            self.open.chain().database().clone(),
            self.open.chain().settings().profile().clone(),
            config,
            cache,
        )
        .map_err(Error::Workers)
    }
}
pub struct Services {
    importer: Arc<chain::Importer>,
    prepared: Prepared,
}
impl std::ops::Deref for Services {
    type Target = Prepared;
    fn deref(&self) -> &Prepared {
        &self.prepared
    }
}
pub struct ActivationError {
    pub error: Error,
    pub prepared: Prepared,
}
impl std::fmt::Debug for ActivationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActivationError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl Services {
    pub fn importer(&self) -> &chain::Importer {
        &self.importer
    }
    pub(crate) fn shared_importer(&self) -> Arc<chain::Importer> {
        self.importer.clone()
    }
    pub async fn stop_and_join(&self) {
        self.importer.stop();
        self.importer.join().await;
    }
    pub fn pending_key(
        &self,
        keys: ae_sync::bindings::Binding<keys::Client>,
        config: pending::Config,
        clock: Arc<dyn generator::Clock>,
    ) -> Result<pending::Worker, pending::Error> {
        pending::Worker::spawn(&self.importer, self.snapshots.clone(), keys, clock, config)
    }
    pub fn generator(
        &self,
        pool: pool::Client,
        config: Generator,
        clock: Arc<dyn generator::Clock>,
    ) -> Result<generator::Worker<Otp26>, Error> {
        generator::Worker::spawn_owned(
            self.open.chain().database().clone(),
            self.snapshots.clone(),
            pool,
            self.generator_config(config)?,
            clock,
            self.execution.clone(),
        )
        .map_err(Error::Generator)
    }
    pub fn production(
        &self,
        pool: pool::Client,
        keys: keys::Client,
        config: Production,
        clocks: Arc<dyn group::Clocks>,
    ) -> Result<group::Worker<Otp26>, Error> {
        if !matches!(
            self.open.chain().identity().algorithm,
            ConsensusAlgorithm::Cuckoo | ConsensusAlgorithm::OnDemand
        ) {
            return Err(Error::Binding);
        }
        let config = group::Config {
            generator: self.generator_config(config.generator)?,
            producer: config.producer,
            mining: config.mining,
            poll: config.poll,
            outputs: config.outputs,
        };
        group::Worker::spawn_owned(
            &self.importer,
            self.snapshots.clone(),
            pool,
            keys,
            config,
            clocks,
            self.execution.clone(),
        )
        .map_err(Error::Production)
    }
    pub fn on_demand(
        &self,
        pool: pool::Client,
        keys: keys::Client,
        config: on_demand::runtime::Config,
        clock: Arc<dyn generator::Clock>,
    ) -> Result<on_demand::runtime::Controller, Error> {
        on_demand::runtime::Controller::new(
            &self.importer,
            self.snapshots.clone(),
            pool,
            keys,
            clock,
            config,
        )
        .map_err(Error::OnDemand)
    }
    pub fn connections<F>(
        &self,
        workers: &workers::Handle,
        identity: Arc<ae_p2p::noise::Identity>,
        cache: pool::CacheController,
        config: connection::Config,
        outputs: F,
    ) -> Result<(connection::Handle, connection::Service<Otp26, F>), Error>
    where
        F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
    {
        if self.open.mode().application() != ApplicationMode::Normal {
            return Err(Error::Mode);
        }
        connection::create_pow_with_importer(
            &self.importer,
            workers,
            identity,
            cache,
            config,
            outputs,
        )
        .map_err(Error::Connections)
    }
}
