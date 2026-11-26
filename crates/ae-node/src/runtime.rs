// Checked family dispatch composes the actual owners. It neither reopens the
// database nor rebuilds the historical execution configuration during launch.
use crate::configuration::resolution::ApplicationMode;
use crate::{
    bootstrap::configured::{self, hyperchain as hc, pow as configured_pow},
    configuration::consensus::Kind,
    http, hyperchain, pow,
};
use ae_protocol::{ConsensusAlgorithm, ConsensusType};
use ae_sync::{
    chain::{
        self,
        hyperchain::conductor,
        pow::{generator, group},
    },
    observe,
    runtime::connection,
};
use ae_vm_fate::maps::order::Otp26;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
mod configuration;
mod launch;
mod metrics;
pub use launch::{Cuckoo, Hyperchain, OnDemand, Settings, StartError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    Cuckoo,
    OnDemand,
    Hyperchain,
}
impl Family {
    fn from_algorithm(algorithm: ConsensusAlgorithm) -> Self {
        match algorithm {
            ConsensusAlgorithm::Cuckoo => Self::Cuckoo,
            ConsensusAlgorithm::OnDemand => Self::OnDemand,
            ConsensusAlgorithm::Hyperchain => Self::Hyperchain,
        }
    }
}
pub struct PowPreparation {
    pub micro_cycle_ms: u64,
    pub body: ae_chain::admission::Limits,
    // Some supplies the complete checked rule policy. Otherwise micro_cycle_ms
    // and body select the defaults derived from this exact opened chain.
    pub rules: Option<ae_chain::key::Rules>,
    pub importer: chain::Config,
    pub snapshots: ae_storage::snapshots::Config,
}
impl Default for PowPreparation {
    fn default() -> Self {
        Self {
            micro_cycle_ms: 3000,
            body: Default::default(),
            rules: None,
            importer: Default::default(),
            snapshots: Default::default(),
        }
    }
}
#[derive(Default)]
pub struct Preparation {
    pub pow: PowPreparation,
    pub hyperchain: hc::Limits,
}
#[derive(Debug)]
pub enum PrepareError {
    Binding,
    UnsupportedSchedule,
    Pow(configured_pow::Error),
    Hyperchain(hc::Error),
}
pub struct PrepareFailure {
    pub error: PrepareError,
    pub open: configured::Open,
}
impl std::fmt::Debug for PrepareFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FamilyPrepareFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
enum PreparedInner {
    Pow(Box<configured_pow::Prepared>),
    Hyperchain(Box<hc::Prepared>),
}
pub struct Prepared(PreparedInner, Option<observe::statsd::Settings>);
impl Prepared {
    pub fn new(open: configured::Open, config: Preparation) -> Result<Self, Box<PrepareFailure>> {
        // Opened already enforces this at the durable boundary. Preserve the
        // check here: selecting only genesis is not transition support.
        if open.chain().settings().consensus().entries().len() != 1 {
            return Err(Box::new(PrepareFailure {
                error: PrepareError::UnsupportedSchedule,
                open,
            }));
        }
        let family = Family::from_algorithm(open.chain().identity().algorithm);
        let (kind, consensus) = match family {
            Family::Cuckoo => (Kind::PowCuckoo, ConsensusType::Pow),
            Family::OnDemand => (Kind::OnDemand, ConsensusType::Pow),
            Family::Hyperchain => (Kind::Hyperchain, ConsensusType::Pos),
        };
        if open.chain().settings().consensus().genesis().kind() != kind
            || open.execution().execution().consensus() != consensus
            || open.execution().fingerprint() != open.chain().identity().execution_profile
        {
            return Err(Box::new(PrepareFailure {
                error: PrepareError::Binding,
                open,
            }));
        }
        match family {
            Family::Cuckoo | Family::OnDemand => {
                let p = config.pow;
                let rules = p
                    .rules
                    .unwrap_or_else(|| open.pow_rules(p.micro_cycle_ms, p.body));
                configured_pow::Prepared::try_new(open, rules, p.importer, p.snapshots)
                    .map(|p| Self(PreparedInner::Pow(Box::new(p)), None))
                    .map_err(|f| {
                        Box::new(PrepareFailure {
                            error: PrepareError::Pow(f.error),
                            open: f.open,
                        })
                    })
            }
            Family::Hyperchain => hc::Prepared::new(open, config.hyperchain)
                .map(|p| Self(PreparedInner::Hyperchain(Box::new(p)), None))
                .map_err(|f| {
                    Box::new(PrepareFailure {
                        error: PrepareError::Hyperchain(f.error),
                        open: f.open,
                    })
                }),
        }
    }
    pub fn configured(&self) -> &configured::Open {
        match &self.0 {
            PreparedInner::Pow(p) => p.configured(),
            PreparedInner::Hyperchain(p) => p.configured(),
        }
    }
    pub fn family(&self) -> Family {
        Family::from_algorithm(self.configured().chain().identity().algorithm)
    }
    pub fn pow(&self) -> Option<&configured_pow::Prepared> {
        match &self.0 {
            PreparedInner::Pow(p) => Some(p),
            _ => None,
        }
    }
    pub fn hyperchain(&self) -> Option<&hc::Prepared> {
        match &self.0 {
            PreparedInner::Hyperchain(p) => Some(p),
            _ => None,
        }
    }
    pub fn into_open(self) -> configured::Open {
        match self.0 {
            PreparedInner::Pow(p) => p.into_open(),
            PreparedInner::Hyperchain(p) => p.into_open(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Family {
        configured: Family,
        requested: Family,
    },
    UnsupportedControl,
    Metrics(observe::Error),
    Http(http::Error),
    Pow(Box<pow::Error>),
    Hyperchain(Box<hyperchain::Error>),
}
#[derive(Debug, Clone)]
pub enum Details {
    Cuckoo(Box<pow::Status>),
    OnDemand(Box<pow::Status>),
    Hyperchain(Box<hyperchain::Status>),
}
#[derive(Debug, Clone)]
pub struct Status {
    pub family: Family,
    pub metrics: Option<observe::SinkInfo>,
    pub metrics_error: Option<observe::Error>,
    pub http: Option<http::Status>,
    pub running: bool,
    pub ready: bool,
    pub joined: bool,
    pub failure: Option<Error>,
    pub details: Details,
}
impl Status {
    fn pow(status: pow::Status, family: Family) -> Self {
        Self {
            family,
            metrics: None,
            metrics_error: None,
            http: None,
            running: status.running,
            ready: status.ready,
            joined: status.joined,
            failure: status
                .failure
                .clone()
                .map(|error| Error::Pow(Box::new(error))),
            details: if family == Family::Cuckoo {
                Details::Cuckoo(Box::new(status))
            } else {
                Details::OnDemand(Box::new(status))
            },
        }
    }
    fn hyperchain(status: hyperchain::Status) -> Self {
        Self {
            family: Family::Hyperchain,
            metrics: None,
            metrics_error: None,
            http: None,
            running: status.running,
            ready: status.ready,
            joined: status.joined,
            failure: status
                .failure
                .clone()
                .map(|error| Error::Hyperchain(Box::new(error))),
            details: Details::Hyperchain(Box::new(status)),
        }
    }
}
pub enum Output {
    Cuckoo(pow::Output),
    OnDemand(pow::Output),
    Hyperchain(Box<conductor::Output>),
    HyperchainProduction(conductor::runtime::supervision::Notice),
}
pub enum Exit {
    Http {
        inner: Box<Exit>,
        status: http::Status,
    },
    Application {
        inner: Box<Exit>,
        metrics: observe::SinkInfo,
        error: Option<observe::Error>,
    },
    Cuckoo(Box<pow::Exit>),
    OnDemand(Box<pow::Exit>),
    Hyperchain(Box<hyperchain::Exit<Otp26>>),
}
impl Exit {
    pub fn status(&self) -> Status {
        match self {
            Self::Http { inner, status } => apply_http(inner.status(), status.clone()),
            Self::Application {
                inner,
                metrics,
                error,
            } => {
                let mut status = inner.status();
                status.joined &= metrics.joined;
                status.metrics = Some(metrics.clone());
                if let Some(error) = error {
                    status.metrics_error = Some(error.clone());
                }
                status
            }
            Self::Cuckoo(e) => Status::pow(e.status.clone(), Family::Cuckoo),
            Self::OnDemand(e) => Status::pow(e.status.clone(), Family::OnDemand),
            Self::Hyperchain(e) => Status::hyperchain(e.status.clone()),
        }
    }
    pub fn configured(&self) -> Option<&configured::Open> {
        match self {
            Self::Http { inner, .. } => inner.configured(),
            Self::Application { inner, .. } => inner.configured(),
            Self::Cuckoo(e) | Self::OnDemand(e) => e
                .services
                .as_ref()
                .map(|s| s.configured())
                .or_else(|| e.prepared.as_ref().map(|p| p.configured())),
            Self::Hyperchain(e) => e.configured.as_ref(),
        }
    }
}
#[derive(Clone)]
enum HandleInner {
    Cuckoo(pow::Handle),
    OnDemand(pow::Handle),
    Hyperchain(hyperchain::Handle<Otp26>),
}
#[derive(Clone)]
pub struct Handle {
    inner: HandleInner,
    mode: ApplicationMode,
    metrics: Option<metrics::View>,
    http: Option<http::Handle>,
    notice_next: Arc<AtomicBool>,
}
impl Handle {
    pub fn mode(&self) -> ApplicationMode {
        self.mode
    }
    pub fn status(&self) -> Status {
        let status = match &self.inner {
            HandleInner::Cuckoo(h) => Status::pow(h.status(), Family::Cuckoo),
            HandleInner::OnDemand(h) => Status::pow(h.status(), Family::OnDemand),
            HandleInner::Hyperchain(h) => Status::hyperchain(h.status()),
        };
        let status = match &self.metrics {
            Some(metrics) => metrics.apply(status),
            None => status,
        };
        match &self.http {
            Some(http) => apply_http(status, http.status()),
            None => status,
        }
    }
    pub fn metrics(&self) -> Option<observe::statsd::Client> {
        self.metrics.as_ref().map(|m| m.client.clone())
    }
    pub fn http(&self) -> Option<http::Handle> {
        self.http.clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        let changed = async {
            match &mut self.inner {
                HandleInner::Cuckoo(h) | HandleInner::OnDemand(h) => {
                    h.changed()
                        .await
                        .map_err(|error| Error::Pow(Box::new(error)))?;
                }
                HandleInner::Hyperchain(h) => {
                    h.changed()
                        .await
                        .map_err(|error| Error::Hyperchain(Box::new(error)))?;
                }
            }
            Ok::<_, Error>(())
        };
        tokio::select! {
            result = changed => result?,
            _ = async { self.metrics.as_mut().expect("configured metrics").changed().await }, if self.metrics.is_some() => (),
            result = async { self.http.as_mut().expect("configured HTTP").changed().await }, if self.http.as_ref().is_some_and(|h| !h.status().joined) => { result.map_err(Error::Http)?; },
        }
        Ok(self.status())
    }
    pub fn request_shutdown(&self) {
        if let Some(http) = &self.http {
            http.request_shutdown();
        }
        if let Some(metrics) = &self.metrics {
            metrics.stop();
            if !metrics.done() {
                return;
            }
        }
        match &self.inner {
            HandleInner::Cuckoo(h) | HandleInner::OnDemand(h) => h.request_shutdown(),
            HandleInner::Hyperchain(h) => h.request_shutdown(),
        }
    }
    pub fn importer(&self) -> Option<Arc<chain::Importer>> {
        match &self.inner {
            HandleInner::Cuckoo(h) | HandleInner::OnDemand(h) => h.importer(),
            HandleInner::Hyperchain(h) => h.importer(),
        }
    }
    pub fn connections(&self) -> Option<connection::Handle> {
        match &self.inner {
            HandleInner::Cuckoo(h) | HandleInner::OnDemand(h) => h.connections(),
            HandleInner::Hyperchain(h) => h.connections(),
        }
    }
    pub fn pow(&self) -> Option<pow::Handle> {
        match &self.inner {
            HandleInner::Cuckoo(h) | HandleInner::OnDemand(h) => Some(h.clone()),
            _ => None,
        }
    }
    pub fn hyperchain(&self) -> Option<hyperchain::Handle<Otp26>> {
        match &self.inner {
            HandleInner::Hyperchain(h) => Some(h.clone()),
            _ => None,
        }
    }
    pub fn commands(&self) -> Option<pow::CommandGeneration> {
        match &self.inner {
            HandleInner::OnDemand(h) => h.commands(),
            _ => None,
        }
    }
    pub fn set_producing(&self, enabled: bool) -> Result<(), Error> {
        match &self.inner {
            HandleInner::Cuckoo(h) => h
                .set_enabled(enabled)
                .map_err(|error| Error::Pow(Box::new(error))),
            HandleInner::OnDemand(_) => Err(Error::UnsupportedControl),
            HandleInner::Hyperchain(h) => if enabled {
                h.start_production()
            } else {
                h.stop_production()
            }
            .map_err(|error| Error::Hyperchain(Box::new(error))),
        }
    }
    // No forwarding task or extra queue: consume exactly one original output.
    // Alternation prevents a continuous HC block stream from starving the
    // bounded supervision notices required to make replacement progress.
    pub fn try_output(&self) -> Option<Output> {
        match &self.inner {
            HandleInner::Cuckoo(h) => h.try_output().map(Output::Cuckoo),
            HandleInner::OnDemand(h) => h.try_output().map(Output::OnDemand),
            HandleInner::Hyperchain(h) => {
                let block = || h.try_output().map(|o| Output::Hyperchain(Box::new(o)));
                let notice = || h.try_production_notice().map(Output::HyperchainProduction);
                if self.notice_next.fetch_xor(true, Ordering::Relaxed) {
                    notice().or_else(block)
                } else {
                    block().or_else(notice)
                }
            }
        }
    }
    pub fn take_exit(&self) -> Option<Exit> {
        if self.http.as_ref().is_some_and(|h| !h.status().joined) {
            return None;
        }
        if self.metrics.as_ref().is_some_and(|m| !m.done()) {
            return None;
        }
        let exit = match &self.inner {
            HandleInner::Cuckoo(h) => h.take_exit().map(|e| Exit::Cuckoo(Box::new(e))),
            HandleInner::OnDemand(h) => h.take_exit().map(|e| Exit::OnDemand(Box::new(e))),
            HandleInner::Hyperchain(h) => h.take_exit().map(|e| Exit::Hyperchain(Box::new(e))),
        }?;
        let exit = match &self.metrics {
            Some(m) => m.exit(exit),
            None => exit,
        };
        Some(match &self.http {
            Some(http) => Exit::Http {
                inner: Box::new(exit),
                status: http.status(),
            },
            None => exit,
        })
    }
}
enum RunningInner {
    Cuckoo(pow::Running),
    OnDemand(pow::Running),
    Hyperchain(hyperchain::Running<Otp26>),
}
pub struct Running {
    inner: RunningInner,
    mode: ApplicationMode,
    metrics: Option<metrics::Owner>,
    http: Option<http::Running>,
    notice_next: Arc<AtomicBool>,
}
impl Running {
    pub async fn listen_http(&mut self, config: http::Config) -> Result<http::Handle, http::Error> {
        if self.http.is_some() {
            return Err(http::Error::AlreadyListening);
        }
        let server = http::Running::start(self.handle(), config).await?;
        let handle = server.handle();
        self.http = Some(server);
        Ok(handle)
    }
    pub fn handle(&self) -> Handle {
        Handle {
            mode: self.mode,
            inner: match &self.inner {
                RunningInner::Cuckoo(r) => HandleInner::Cuckoo(r.handle()),
                RunningInner::OnDemand(r) => HandleInner::OnDemand(r.handle()),
                RunningInner::Hyperchain(r) => HandleInner::Hyperchain(r.handle()),
            },
            notice_next: self.notice_next.clone(),
            metrics: self.metrics.as_ref().map(metrics::Owner::view),
            http: self.http.as_ref().map(http::Running::handle),
        }
    }
    pub async fn wait(self) -> Result<Exit, Error> {
        let mut metrics = self.metrics;
        let http = self.http;
        let exit = match self.inner {
            RunningInner::Cuckoo(r) => r
                .wait()
                .await
                .map(|e| Exit::Cuckoo(Box::new(e)))
                .map_err(|error| Error::Pow(Box::new(error))),
            RunningInner::OnDemand(r) => r
                .wait()
                .await
                .map(|e| Exit::OnDemand(Box::new(e)))
                .map_err(|error| Error::Pow(Box::new(error))),
            RunningInner::Hyperchain(r) => r
                .wait()
                .await
                .map(|e| Exit::Hyperchain(Box::new(e)))
                .map_err(|error| Error::Hyperchain(Box::new(error))),
        };
        if let Some(metrics) = &mut metrics {
            metrics.finish().await;
        }
        let exit = exit.map(|e| match &metrics {
            Some(m) => m.view().exit(e),
            None => e,
        });
        let http = match http {
            Some(http) => Some(http.finish().await),
            None => None,
        };
        exit.map(|inner| match http {
            Some(status) => Exit::Http {
                inner: Box::new(inner),
                status,
            },
            None => inner,
        })
    }
    pub async fn shutdown(self) -> Result<Exit, Error> {
        self.handle().request_shutdown();
        self.wait().await
    }
}

fn apply_http(mut status: Status, http: http::Status) -> Status {
    status.ready &= http.running;
    status.joined &= http.joined;
    status.running |= http.running;
    if let Some(error) = &http.failure {
        status.failure = Some(Error::Http(error.clone()));
    }
    status.http = Some(http);
    status
}
