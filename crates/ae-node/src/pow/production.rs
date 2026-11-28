use super::{Child, Error, Output, Status, configured};
use ae_protocol::ConsensusAlgorithm;
use ae_storage::pool::worker as pool;
use ae_sync::{
    chain::{
        on_demand::service,
        pending::service as pending,
        pow::{generator, group},
    },
    keys,
};
use ae_vm_fate::maps::order::Otp26;
use std::{collections::VecDeque, sync::Arc};

pub use ae_sync::chain::on_demand::Config as Commands;
#[derive(Clone)]
pub struct CommandGeneration {
    pub number: u64,
    pub production: Option<group::GenerationId>,
    pub client: service::Client,
}
pub(super) enum Input {
    Automatic {
        algorithm: ConsensusAlgorithm,
        config: Box<configured::Production>,
        clocks: Arc<dyn group::Clocks>,
        commands: Option<Box<Commands>>,
    },
    Commands {
        config: Box<Commands>,
        clock: Arc<dyn generator::Clock>,
    },
}
impl Input {
    pub fn pending(
        &self,
        limits: pending::Limits,
        stratum: Option<&crate::stratum::configured::Config>,
    ) -> (pending::Config, Arc<dyn generator::Clock>) {
        match self {
            Self::Automatic { config, clocks, .. } => (
                pending::Config {
                    beneficiary: stratum
                        .map(|s| s.beneficiary())
                        .or_else(|| config.mining.as_ref().map(|m| m.beneficiary)),
                    release_info: config.mining.as_ref().map_or(0, |m| m.release_info),
                    key: stratum.map_or_else(
                        || {
                            config
                                .mining
                                .as_ref()
                                .map_or(config.generator.key, |m| m.preparation)
                        },
                        |s| s.preparation,
                    ),
                    limits,
                },
                clocks.preparation(0),
            ),
            Self::Commands { config, clock } => (
                pending::Config {
                    beneficiary: config.controller.beneficiary,
                    release_info: config.controller.release_info,
                    key: config.controller.key,
                    limits,
                },
                clock.clone(),
            ),
        }
    }
    pub fn stratum_compatible(&self) -> bool {
        matches!(self, Self::Automatic { algorithm: ConsensusAlgorithm::Cuckoo, config, .. } if config.mining.is_none() && config.producer.external_key_work)
    }
    pub fn stratum_inputs(&self, generation: u64) -> Option<(bool, Arc<dyn generator::Clock>)> {
        match self {
            Self::Automatic {
                algorithm: ConsensusAlgorithm::Cuckoo,
                config,
                clocks,
                ..
            } => Some((
                config.producer.strictly_follow_top,
                clocks.preparation(generation),
            )),
            _ => None,
        }
    }
    pub fn algorithm(&self) -> ConsensusAlgorithm {
        match self {
            Self::Automatic { algorithm, .. } => *algorithm,
            Self::Commands { .. } => ConsensusAlgorithm::OnDemand,
        }
    }
    pub fn enabled(&self) -> Option<bool> {
        match self {
            Self::Automatic { config, .. } => Some(config.producer.enabled),
            Self::Commands { .. } => None,
        }
    }
    pub fn start_allowed(&self) -> bool {
        match self {
            Self::Automatic {
                commands: Some(commands),
                ..
            } => commands.controller.beneficiary.is_some(),
            _ => true,
        }
    }
    pub fn validate(&self, cycle: u64) -> Result<(), Error> {
        if self.enabled() == Some(true) && !self.start_allowed() {
            return Err(Error::BeneficiaryNotConfigured);
        }
        match self {
            Self::Automatic { config, .. } if config.producer.micro_cycle_ms != cycle => {
                Err(Error::Configuration)
            }
            Self::Automatic { .. } => Ok(()),
            Self::Commands { config, .. } => config.service.validate().map_err(Error::Commands),
        }
    }
    pub fn spawn(
        &self,
        services: &configured::Services,
        pool: pool::Client,
        keys: keys::Client,
    ) -> Result<Live, Box<StartFailure>> {
        let start = |e| {
            Box::new(StartFailure {
                error: Error::Start(format!("{e:?}")),
                retired: None,
            })
        };
        Ok(match self {
            Self::Automatic {
                config,
                clocks,
                commands,
                ..
            } => {
                let mut config = config.as_ref().clone();
                config.producer.enabled = false;
                let worker = if let Some(commands) = commands {
                    services.production_with_commands(
                        pool,
                        keys,
                        config,
                        clocks.clone(),
                        commands.as_ref().clone(),
                    )
                } else {
                    services.production(pool, keys, config, clocks.clone())
                }
                .map_err(start)?;
                Live::Automatic(worker)
            }
            Self::Commands { config, clock } => {
                let controller = services
                    .on_demand(pool, keys, config.controller.clone(), clock.clone())
                    .map_err(start)?;
                match service::Worker::spawn(controller, config.service) {
                    Ok(worker) => Live::Commands(worker),
                    Err(failure) => {
                        let failure = *failure;
                        let error = Error::Commands(failure.error.clone());
                        return Err(Box::new(StartFailure {
                            error,
                            retired: Some(Retired::Commands(Box::new(failure.retire()))),
                        }));
                    }
                }
            }
        })
    }
}
pub(super) struct StartFailure {
    pub error: Error,
    pub retired: Option<Retired>,
}
pub(super) enum Live {
    Automatic(group::Worker<Otp26>),
    Commands(service::Worker),
}
impl Live {
    pub fn ready(&self) -> bool {
        match self {
            Self::Automatic(w) => w.client().current().is_some(),
            Self::Commands(w) => {
                let s = w.client().status();
                s.running && s.accepting
            }
        }
    }
    pub fn update(&self, status: &mut Status) {
        match self {
            Self::Automatic(w) => {
                let client = w.client();
                status.production = Some(client.status());
                status.commands = client
                    .current()
                    .and_then(|g| g.producer.commands())
                    .map(|c| c.status());
            }
            Self::Commands(w) => status.commands = Some(w.client().status()),
        }
    }
    pub fn check(&self) -> Result<(), Error> {
        let running = match self {
            Self::Automatic(w) => {
                let s = w.client().status();
                if let Some(e) = s.failure {
                    return Err(Error::Production(e));
                }
                s.running && !w.is_finished()
            }
            Self::Commands(w) => {
                let s = w.client().status();
                if let Some(e) = s.failure {
                    return Err(Error::Commands(e));
                }
                s.running && !w.is_finished()
            }
        };
        if running {
            Ok(())
        } else {
            Err(Error::ChildStopped(Child::Production))
        }
    }
    pub fn collect(&self, outputs: &mut VecDeque<Output>, limit: usize, generation: u64) {
        // A competing explicit child consumer may win acknowledgement. Bound
        // the attempts too, so that race cannot monopolize the root loop.
        for _ in outputs.len()..limit {
            match self {
                Self::Automatic(w) => {
                    let c = w.client();
                    let Some(output) = c.front() else {
                        break;
                    };
                    if c.acknowledge(&output) {
                        outputs.push_back(Output::Production(output));
                    }
                }
                Self::Commands(w) => {
                    let c = w.client();
                    let Some(completion) = c.front() else {
                        break;
                    };
                    if c.acknowledge(&completion) {
                        outputs.push_back(Output::Command {
                            generation,
                            completion,
                        });
                    }
                }
            }
        }
    }
    pub fn request_shutdown(&self) {
        match self {
            Self::Automatic(w) => w.client().request_shutdown(),
            Self::Commands(w) => w.client().request_shutdown(),
        }
    }
    pub async fn stop(self) -> Result<Retired, Error> {
        tokio::task::spawn_blocking(move || match self {
            Self::Automatic(w) => w
                .shutdown()
                .map(|r| Retired::Automatic(Box::new(r)))
                .map_err(|_| Error::ChildTask(Child::Production)),
            Self::Commands(w) => Ok(Retired::Commands(Box::new(w.shutdown()))),
        })
        .await
        .map_err(|_| Error::ChildTask(Child::Production))?
    }
}
pub(super) enum Retired {
    Automatic(Box<group::Report>),
    Commands(Box<service::Report>),
}
impl Retired {
    pub fn update(&self, status: &mut Status) {
        match self {
            Self::Automatic(r) => {
                status.production = Some(r.status.clone());
                status.commands = r.commands_status();
            }
            Self::Commands(r) => status.commands = Some(r.status.clone()),
        }
    }
    pub fn failure(&self) -> Option<Error> {
        match self {
            Self::Automatic(r) => r.status.failure.clone().map(Error::Production),
            Self::Commands(r) => r.status.failure.clone().map(Error::Commands),
        }
    }
    pub fn poll(&mut self) -> Result<bool, Error> {
        match self {
            Self::Automatic(r) => r.poll().map_err(Error::Production),
            Self::Commands(r) => r.poll().map_err(Error::Commands),
        }
    }
}
pub(super) struct Retirement {
    pub number: u64,
    pub value: Retired,
}
impl Retirement {
    pub fn release(self) -> Result<Output, Self> {
        match self.value {
            Retired::Automatic(r) => match r.release() {
                Ok(released) => Ok(Output::Retired(Box::new(released))),
                Err(r) => Err(Self {
                    number: self.number,
                    value: Retired::Automatic(r),
                }),
            },
            Retired::Commands(r) => match r.release() {
                Ok(released) => Ok(Output::CommandsRetired {
                    generation: self.number,
                    report: Box::new(released),
                }),
                Err(r) => Err(Self {
                    number: self.number,
                    value: Retired::Commands(r),
                }),
            },
        }
    }
}
