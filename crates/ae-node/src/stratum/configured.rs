// One Stratum generation owns its reward signer, accounting and TCP service.
// Retirement joins accepted work before another generation can acquire Writer.
use super::*;
use ae_sync::{
    chain::pow::{generator, group},
    preset_keys,
};
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    prepared: Arc<crate::configuration::stratum::Prepared>,
    pub transport: runtime::Config,
    pub accounting: storage::Limits,
    pub ingress: IngressLimits,
    pub queue: Limits,
    pub mining: storage::mining::Limits,
    pub rounds: storage::rounds::Limits,
    pub payouts: storage::payouts::Limits,
    pub signer: preset_keys::Config,
    pub preparation: ae_chain::repository::pow::key::Limits,
    pub payout_interval: Duration,
    pub confirmation_interval: Duration,
}
impl Config {
    pub fn new(
        prepared: crate::configuration::stratum::Prepared,
    ) -> Result<Self, crate::configuration::stratum::Error> {
        let transport = prepared.transport(Default::default())?;
        prepared.rounds()?;
        Ok(Self {
            prepared: Arc::new(prepared),
            transport,
            accounting: Default::default(),
            ingress: Default::default(),
            queue: Default::default(),
            mining: Default::default(),
            rounds: Default::default(),
            payouts: Default::default(),
            signer: Default::default(),
            preparation: Default::default(),
            payout_interval: Duration::from_secs(120),
            confirmation_interval: Duration::from_secs(10),
        })
    }
    pub fn beneficiary(&self) -> ae_types::AccountPublicKey {
        self.prepared.caller()
    }
}
pub struct Inputs {
    pub database: Arc<Database>,
    pub snapshots: ae_storage::snapshots::Owner,
    pub admitter: ae_sync::chain::pow::stratum::Admitter,
    pub producers: group::Client<ae_vm_fate::maps::order::Otp26>,
    pub pool: ae_storage::pool::worker::Client,
    pub clock: Arc<dyn generator::Clock>,
    pub strictly_follow_top: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    Configuration,
    Accounting,
    Transport,
    Signer,
    Unsettled,
    Task,
    RestartLimit,
}
#[derive(Debug, Default)]
pub struct Report {
    pub transport: Option<runtime::Report>,
    pub accounting: Option<super::Report>,
    pub signer: Option<preset_keys::Info>,
}
impl Report {
    pub fn settled(&self) -> bool {
        self.accounting.as_ref().is_none_or(|a| {
            a.admission.is_none()
                && a.payout.as_ref().is_none_or(|p| p.unsettled.is_none())
                && a.pending.iter().all(|p| !p.unknown)
                && a.controls.iter().all(|p| !p.unknown)
        })
    }
    pub fn failure(&self) -> Option<Failure> {
        if !self.settled() {
            Some(Failure::Unsettled)
        } else if self
            .accounting
            .as_ref()
            .is_some_and(|r| r.failure.is_some())
        {
            Some(Failure::Accounting)
        } else if self.transport.as_ref().is_some_and(|r| r.failure.is_some()) {
            Some(Failure::Transport)
        } else if self
            .signer
            .is_some_and(|s| s.exit == Some(preset_keys::Exit::Panicked) || !s.joined)
        {
            Some(Failure::Signer)
        } else {
            None
        }
    }
}
#[derive(Debug)]
pub struct StartFailure {
    pub error: Error,
    pub report: Report,
}
#[derive(Clone)]
pub struct Handle {
    pub transport: runtime::Client,
    pub accounting: Client,
    pub signer: preset_keys::Client,
    dropped: Arc<Mutex<Option<Report>>>,
}
impl Handle {
    pub fn request_shutdown(&self) {
        if let Some(m) = self.accounting.mining() {
            m.stop_production();
        }
        self.transport.request_shutdown();
        self.accounting.close();
    }
    pub fn failure(&self) -> Option<Failure> {
        if self.accounting.is_closed() {
            Some(Failure::Accounting)
        } else if self.transport.is_closed() {
            Some(Failure::Transport)
        } else if !self.signer.info().accepting {
            Some(Failure::Signer)
        } else {
            None
        }
    }
    pub fn take_dropped_report(&self) -> Option<Report> {
        self.dropped
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
}
pub struct Live {
    service: Option<Service>,
    signer: Option<preset_keys::Worker>,
    handle: Handle,
}
impl Live {
    pub fn start(config: &Config, inputs: Inputs) -> Result<Self, Box<StartFailure>> {
        let fail = |error, report| Box::new(StartFailure { error, report });
        if !inputs.admitter.uses_database(&inputs.database)
            || !inputs.pool.uses_database(&inputs.database)
            || !inputs.snapshots.uses_database(&inputs.database)
            || !inputs.producers.uses_snapshots(&inputs.snapshots)
        {
            return Err(fail(Error::Configuration, Report::default()));
        }
        config
            .transport
            .validate()
            .map_err(|e| fail(Error::Runtime(e), Report::default()))?;
        let rounds = config
            .prepared
            .rounds()
            .map_err(|e| fail(Error::Setup(e), Report::default()))?;
        let signer = config
            .prepared
            .signer(config.signer)
            .map_err(|e| fail(Error::Setup(e), Report::default()))?;
        let mut production = mining::production::Inputs::supervised(
            inputs.snapshots.clone(),
            inputs.producers,
            config.beneficiary(),
        );
        production.preparation = config.preparation;
        let mut payouts = mining::payouts::Inputs::new(
            config.prepared.payouts(),
            signer.client(),
            inputs.pool,
            inputs.snapshots,
        );
        payouts.limits = config.payouts;
        payouts.preflight = config.preparation;
        payouts.payout_interval = config.payout_interval;
        payouts.confirmation_interval = config.confirmation_interval;
        let accounting = match Accounting::start_mining(
            inputs.database,
            config.accounting,
            config.ingress,
            config.queue,
            mining::Inputs {
                admitter: inputs.admitter,
                limits: config.mining,
                strictly_follow_top: inputs.strictly_follow_top,
                clock: inputs.clock,
                production: Some(production),
                rounds: Some(mining::rounds::Inputs {
                    config: rounds,
                    limits: config.rounds,
                }),
                payouts: Some(payouts),
            },
        ) {
            Ok(a) => a,
            Err(error) => {
                return Err(fail(
                    error,
                    Report {
                        signer: Some(signer.retire()),
                        ..Default::default()
                    },
                ));
            }
        };
        let transport = match runtime::Owner::with_pool(
            config.transport.clone(),
            Arc::new(accounting.client()),
        ) {
            Ok(t) => t,
            Err(e) => {
                return Err(fail(
                    Error::Runtime(e),
                    Report {
                        accounting: Some(accounting.shutdown()),
                        signer: Some(signer.retire()),
                        transport: None,
                    },
                ));
            }
        };
        if let Err(error) = Service::attach_production(&accounting, &transport) {
            let service = Service {
                transport: Some(transport),
                accounting: Some(accounting),
            }
            .shutdown();
            return Err(fail(
                error,
                Report {
                    transport: Some(service.transport),
                    accounting: Some(service.accounting),
                    signer: Some(signer.retire()),
                },
            ));
        }
        let handle = Handle {
            transport: transport.client(),
            accounting: accounting.client(),
            signer: signer.client(),
            dropped: Arc::new(Mutex::new(None)),
        };
        Ok(Self {
            service: Some(Service {
                transport: Some(transport),
                accounting: Some(accounting),
            }),
            signer: Some(signer),
            handle,
        })
    }
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    fn retire(&mut self) -> Report {
        self.handle.request_shutdown();
        let service = self.service.take().expect("Stratum service").shutdown();
        // Signing remains available until all accepted payout work has joined.
        let signer = self.signer.take().expect("reward signer").retire();
        Report {
            transport: Some(service.transport),
            accounting: Some(service.accounting),
            signer: Some(signer),
        }
    }
    pub fn shutdown(mut self) -> Report {
        self.retire()
    }
}
impl Drop for Live {
    fn drop(&mut self) {
        if self.service.is_some() {
            let report = self.retire();
            *self
                .handle
                .dropped
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(report);
        }
    }
}
