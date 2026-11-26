use super::{Child, Error, Output, Status, configured};
use ae_protocol::ConsensusAlgorithm;
use ae_storage::pool::worker as pool;
use ae_sync::{
    chain::{
        on_demand::{runtime, service},
        pending::service as pending,
        pow::{generator, group},
    },
    keys,
};
use ae_types::AccountPublicKey;
use ae_vm_fate::maps::order::Otp26;
use std::{collections::VecDeque, sync::Arc};

#[derive(Clone)]
pub struct Commands {
    pub controller: runtime::Config,
    pub service: service::Config,
}
impl Commands {
    pub fn new(beneficiary: AccountPublicKey) -> Self {
        Self {
            controller: runtime::Config::new(beneficiary),
            service: Default::default(),
        }
    }
}
#[derive(Clone)]
pub struct CommandGeneration {
    pub number: u64,
    pub client: service::Client,
}
pub(super) enum Input {
    Cuckoo {
        config: Box<configured::Production>,
        clocks: Arc<dyn group::Clocks>,
    },
    Commands {
        config: Box<Commands>,
        clock: Arc<dyn generator::Clock>,
    },
}
impl Input {
    pub fn pending(&self, limits: pending::Limits) -> (pending::Config, Arc<dyn generator::Clock>) {
        match self {
            Self::Cuckoo { config, clocks } => (
                pending::Config {
                    beneficiary: config.mining.as_ref().map(|m| m.beneficiary),
                    release_info: config.mining.as_ref().map_or(0, |m| m.release_info),
                    key: config
                        .mining
                        .as_ref()
                        .map_or(config.generator.key, |m| m.preparation),
                    limits,
                },
                clocks.preparation(0),
            ),
            Self::Commands { config, clock } => (
                pending::Config {
                    beneficiary: Some(config.controller.beneficiary),
                    release_info: config.controller.release_info,
                    key: config.controller.key,
                    limits,
                },
                clock.clone(),
            ),
        }
    }
    pub fn algorithm(&self) -> ConsensusAlgorithm {
        match self {
            Self::Cuckoo { .. } => ConsensusAlgorithm::Cuckoo,
            Self::Commands { .. } => ConsensusAlgorithm::OnDemand,
        }
    }
    pub fn enabled(&self) -> Option<bool> {
        match self {
            Self::Cuckoo { config, .. } => Some(config.producer.enabled),
            Self::Commands { .. } => None,
        }
    }
    pub fn validate(&self, cycle: u64) -> Result<(), Error> {
        match self {
            Self::Cuckoo { config, .. } if config.producer.micro_cycle_ms != cycle => {
                Err(Error::Configuration)
            }
            Self::Cuckoo { .. } => Ok(()),
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
            Self::Cuckoo { config, clocks } => {
                let mut config = config.as_ref().clone();
                config.producer.enabled = false;
                Live::Cuckoo(
                    services
                        .production(pool, keys, config, clocks.clone())
                        .map_err(start)?,
                )
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
    Cuckoo(group::Worker<Otp26>),
    Commands(service::Worker),
}
impl Live {
    pub fn ready(&self) -> bool {
        match self {
            Self::Cuckoo(w) => w.client().current().is_some(),
            Self::Commands(w) => {
                let s = w.client().status();
                s.running && s.accepting
            }
        }
    }
    pub fn update(&self, status: &mut Status) {
        match self {
            Self::Cuckoo(w) => status.production = Some(w.client().status()),
            Self::Commands(w) => status.commands = Some(w.client().status()),
        }
    }
    pub fn check(&self) -> Result<(), Error> {
        let running = match self {
            Self::Cuckoo(w) => {
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
                Self::Cuckoo(w) => {
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
            Self::Cuckoo(w) => w.client().request_shutdown(),
            Self::Commands(w) => w.client().request_shutdown(),
        }
    }
    pub async fn stop(self) -> Result<Retired, Error> {
        tokio::task::spawn_blocking(move || match self {
            Self::Cuckoo(w) => w
                .shutdown()
                .map(|r| Retired::Cuckoo(Box::new(r)))
                .map_err(|_| Error::ChildTask(Child::Production)),
            Self::Commands(w) => Ok(Retired::Commands(Box::new(w.shutdown()))),
        })
        .await
        .map_err(|_| Error::ChildTask(Child::Production))?
    }
}
pub(super) enum Retired {
    Cuckoo(Box<group::Report>),
    Commands(Box<service::Report>),
}
impl Retired {
    pub fn update(&self, status: &mut Status) {
        match self {
            Self::Cuckoo(r) => status.production = Some(r.status.clone()),
            Self::Commands(r) => status.commands = Some(r.status.clone()),
        }
    }
    pub fn failure(&self) -> Option<Error> {
        match self {
            Self::Cuckoo(r) => r.status.failure.clone().map(Error::Production),
            Self::Commands(r) => r.status.failure.clone().map(Error::Commands),
        }
    }
    pub fn poll(&mut self) -> Result<bool, Error> {
        match self {
            Self::Cuckoo(r) => r.poll().map_err(Error::Production),
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
            Retired::Cuckoo(r) => match r.release() {
                Ok(released) => Ok(Output::Retired(Box::new(released))),
                Err(r) => Err(Self {
                    number: self.number,
                    value: Retired::Cuckoo(r),
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
