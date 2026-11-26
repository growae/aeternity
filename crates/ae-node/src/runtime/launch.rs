use super::*;
use conductor::runtime::supervision;

pub struct Cuckoo {
    pub config: pow::Config,
    pub clocks: Arc<dyn group::Clocks>,
}
pub struct OnDemand {
    pub config: pow::Config<pow::Commands>,
    pub clock: Arc<dyn generator::Clock>,
}
pub struct Hyperchain {
    pub config: hyperchain::Config,
    pub runtime: conductor::Config,
    pub services: ae_sync::chain::hyperchain::configuration::Services,
    pub cache: ae_storage::pool::worker::CacheController,
    pub identity: hyperchain::Identity,
    pub clocks: Arc<dyn supervision::Factory + Sync>,
}
pub enum Settings {
    Cuckoo(Box<Cuckoo>),
    OnDemand(Box<OnDemand>),
    Hyperchain(Box<Hyperchain>),
}
impl Settings {
    pub fn family(&self) -> Family {
        match self {
            Self::Cuckoo(_) => Family::Cuckoo,
            Self::OnDemand(_) => Family::OnDemand,
            Self::Hyperchain(_) => Family::Hyperchain,
        }
    }
}
pub struct StartError {
    pub error: Error,
    pub prepared: Prepared,
    pub settings: Settings,
}
impl std::fmt::Debug for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FamilyStartError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
struct Clocks(Arc<dyn supervision::Factory + Sync>);
impl supervision::Factory for Clocks {
    fn clock(&self, generation: u64) -> Result<Box<dyn conductor::runtime::Clock>, String> {
        self.0.clock(generation)
    }
}
impl Prepared {
    pub fn launch<F>(self, settings: Settings, outputs: F) -> Result<Running, Box<StartError>>
    where
        F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
    {
        if self.family() != settings.family() {
            return Err(Box::new(StartError {
                error: Error::Family {
                    configured: self.family(),
                    requested: settings.family(),
                },
                prepared: self,
                settings,
            }));
        }
        let metrics_config = self.1.clone();
        let prefix = match &settings {
            Settings::Cuckoo(s) => &s.config.connections.observer.prefix,
            Settings::OnDemand(s) => &s.config.connections.observer.prefix,
            Settings::Hyperchain(s) => &s.config.connections.observer.prefix,
        }
        .clone();
        let mut application = match self
            .1
            .clone()
            .map(|s| {
                metrics::Owner::prepare(
                    s,
                    prefix,
                    self.configured().chain().database().instrumentation(),
                )
            })
            .transpose()
        {
            Ok(owner) => owner,
            Err(error) => {
                return Err(Box::new(StartError {
                    error: Error::Metrics(error),
                    prepared: self,
                    settings,
                }));
            }
        };
        let exporter = application.as_ref().map(|m| m.view().client);
        let mut outputs = outputs;
        let outputs = move || {
            let mut out = outputs()?;
            if let Some(client) = &exporter {
                if out.statsd.is_some() || out.metrics.is_some() {
                    return Err(observe::Error::Configuration);
                }
                out.metrics = Some(client.clone());
            }
            Ok(out)
        };
        let running = match (self.0, settings) {
            (PreparedInner::Pow(p), Settings::Cuckoo(s)) => {
                match pow::launch(*p, s.config.clone(), s.clocks.clone(), outputs) {
                    Ok(r) => RunningInner::Cuckoo(r),
                    Err(e) => {
                        return Err(Box::new(StartError {
                            error: Error::Pow(Box::new(e.error)),
                            prepared: Prepared(
                                PreparedInner::Pow(Box::new(e.prepared)),
                                metrics_config,
                            ),
                            settings: Settings::Cuckoo(s),
                        }));
                    }
                }
            }
            (PreparedInner::Pow(p), Settings::OnDemand(s)) => {
                match pow::launch_commands(*p, s.config.clone(), s.clock.clone(), outputs) {
                    Ok(r) => RunningInner::OnDemand(r),
                    Err(e) => {
                        return Err(Box::new(StartError {
                            error: Error::Pow(Box::new(e.error)),
                            prepared: Prepared(
                                PreparedInner::Pow(Box::new(e.prepared)),
                                metrics_config,
                            ),
                            settings: Settings::OnDemand(s),
                        }));
                    }
                }
            }
            (PreparedInner::Hyperchain(p), Settings::Hyperchain(s)) => {
                let Hyperchain {
                    config,
                    runtime,
                    services,
                    cache,
                    identity,
                    clocks,
                } = *s;
                let inputs = hyperchain::ConfiguredInputs {
                    prepared: *p,
                    runtime,
                    services,
                    cache,
                    identity,
                };
                match hyperchain::launch_configured_supervised(
                    inputs,
                    config.clone(),
                    Clocks(clocks.clone()),
                    outputs,
                ) {
                    Ok(r) => RunningInner::Hyperchain(r),
                    Err(e) => {
                        let hyperchain::ConfiguredInputs {
                            prepared,
                            runtime,
                            services,
                            cache,
                            identity,
                        } = e.inputs;
                        return Err(Box::new(StartError {
                            error: Error::Hyperchain(Box::new(e.error)),
                            prepared: Prepared(
                                PreparedInner::Hyperchain(Box::new(prepared)),
                                metrics_config,
                            ),
                            settings: Settings::Hyperchain(Box::new(Hyperchain {
                                config,
                                runtime,
                                services,
                                cache,
                                identity,
                                clocks,
                            })),
                        }));
                    }
                }
            }
            _ => unreachable!("checked family and prepared identity"),
        };
        let mut running = Running {
            inner: running,
            metrics: None,
            http: None,
            notice_next: Arc::new(AtomicBool::new(false)),
        };
        if let Some(application) = &mut application {
            application.bind(running.handle());
        }
        running.metrics = application;
        Ok(running)
    }
}
