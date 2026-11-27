// Application assembly owns the immutable configuration used by every service.
// Embedding callers may still use the lower-level runtime APIs independently.
use crate::{
    bootstrap,
    configuration::{
        self, Configuration,
        execution::Specification,
        resolution::{Context, Snapshot},
    },
    http, runtime,
};
use std::{path::PathBuf, sync::Arc};

pub mod command;
mod production;

#[derive(Debug)]
pub struct Error {
    pub phase: &'static str,
    detail: String,
}
impl Error {
    fn at(phase: &'static str, error: impl std::fmt::Debug) -> Self {
        Self {
            phase,
            detail: format!("{error:?}"),
        }
    }
    pub fn detail(&self) -> &str {
        &self.detail
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node startup failed during {}", self.phase)
    }
}
impl std::error::Error for Error {}

pub struct Options {
    pub cwd: PathBuf,
    pub database: PathBuf,
    pub network: Option<Vec<u8>>,
    pub execution: configuration::execution::Resources,
    pub storage: bootstrap::Config,
    pub preparation: runtime::Preparation,
    pub miner_directories: ae_sync::chain::pow::configuration::Directories,
    pub stratum_rewards: Option<configuration::stratum::Rewards>,
    pub http: http::ConfiguredLimits,
    pub dry_run: http::DryRunConfig,
}
impl Options {
    pub fn new(cwd: PathBuf, database: PathBuf, dry_run: http::DryRunConfig) -> Self {
        Self {
            cwd,
            database,
            dry_run,
            network: None,
            execution: Default::default(),
            storage: Default::default(),
            preparation: Default::default(),
            miner_directories: Default::default(),
            stratum_rewards: None,
            http: Default::default(),
        }
    }
}
pub struct Prepared {
    node: runtime::Prepared,
    settings: runtime::Settings,
    http: http::ConfiguredLimits,
    dry_run: http::DryRunConfig,
}
impl Prepared {
    pub fn load(
        configuration: &Configuration,
        context: Arc<Context>,
        specification: &Specification,
        options: Options,
    ) -> Result<Self, Error> {
        if !options.cwd.is_absolute() {
            return Err(Error::at("working directory", "absolute path required"));
        }
        let database = options.cwd.join(&options.database);
        for marker in ["schema.DAT", "mnesia"] {
            if database
                .join(marker)
                .try_exists()
                .map_err(|e| Error::at("database path", e))?
            {
                return Err(Error::at(
                    "database format",
                    "native Mnesia import required",
                ));
            }
        }
        let snapshot = configuration
            .capture(context)
            .map_err(|e| Error::at("configuration capture", e))?;
        Self::from_snapshot(snapshot, specification, options)
    }
    fn from_snapshot(
        snapshot: Snapshot,
        specification: &Specification,
        options: Options,
    ) -> Result<Self, Error> {
        let resolved = snapshot.resolved();
        let stratum_directory = options.cwd.join(
            resolved
                .data_directory("aestratum")
                .map_err(|e| Error::at("Stratum resource directory", e))?,
        );
        // Validate cached user values before opening storage. Resolve the payment
        // contract against the actual network once the genesis binding exists.
        let stratum_enabled =
            configuration::stratum::Plan::resolve(&resolved, b"", &stratum_directory)
                .map_err(|e| Error::at("Stratum configuration", e))?
                .is_some();
        if resolved
            .get(&["chain", "persist"])
            .map_err(|e| Error::at("storage configuration", e))?
            == Some(&serde_json::json!(false))
        {
            return Err(Error::at(
                "nonpersistent storage integration",
                "not yet connected",
            ));
        }
        // Validate HTTP before opening storage or starting any runtime owner.
        configuration::http::startup::Startup::resolve(&resolved)
            .map_err(|e| Error::at("HTTP configuration", e))?
            .configs(&options.http)
            .map_err(|e| Error::at("HTTP limits", e))?;
        let initial = ae_state::Trees {
            accounts: Default::default(),
            contracts: Default::default(),
            calls: Default::default(),
            channels: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        let core = configuration::runtime::Core::resolve(&resolved, &options.cwd)
            .map_err(|e| Error::at("core configuration", e))?;
        let open = bootstrap::configured::Prepared::load(
            &resolved,
            options.network.as_deref(),
            &options.cwd,
            &initial,
            specification,
            options.execution,
            &6_000_000.into(),
            Default::default(),
            Default::default(),
        )
        .map_err(|e| Error::at("genesis and execution", e))?
        .open(&options.database, options.storage)
        .map_err(|e| Error::at("database", e))?;
        let mut node = runtime::Prepared::new(open, options.preparation)
            .map_err(|e| Error::at("consensus preparation", e.error))?;
        let stratum = if stratum_enabled {
            if node.family() != runtime::Family::Cuckoo {
                return Err(Error::at("Stratum consensus", "Cuckoo required"));
            }
            let prepared = configuration::stratum::Plan::resolve(
                &resolved,
                node.configured().chain().settings().profile().network_id(),
                &stratum_directory,
            )
            .map_err(|e| Error::at("Stratum configuration", e))?
            .expect("validated enabled section")
            .load_or_create()
            .map_err(|e| Error::at("Stratum reward keys", e))?;
            let prepared = if let Some(rewards) = options.stratum_rewards {
                prepared.with_rewards(rewards)
            } else {
                prepared
            };
            Some(
                crate::stratum::configured::Config::new(prepared)
                    .map_err(|e| Error::at("Stratum runtime configuration", e))?,
            )
        } else {
            None
        };
        let production = production::Plan::resolve(&resolved, node.family(), stratum_enabled)?;
        let mut settings = production.settings(
            &node,
            &options.miner_directories,
            core.cache_size(),
            stratum,
        )?;
        settings
            .apply_core(&core)
            .map_err(|e| Error::at("core services", e))?;
        node.configure_network(&resolved, &mut settings)
            .map_err(|e| Error::at("network configuration", e))?;
        node.configure_optional_network(&resolved, &mut settings)
            .map_err(|e| Error::at("optional network configuration", e))?;
        node.configure_discovery(&resolved, &mut settings)
            .map_err(|e| Error::at("discovery configuration", e))?;
        node.configure_metrics(&resolved, &mut settings)
            .map_err(|e| Error::at("metrics configuration", e))?;
        Ok(Self {
            node,
            settings,
            http: options.http,
            dry_run: options.dry_run,
        })
    }
    pub fn configured(&self) -> &bootstrap::configured::Open {
        self.node.configured()
    }
    pub fn launch(self) -> Result<Running, Error> {
        let node = self
            .node
            .launch(self.settings, || Ok(Default::default()))
            .map_err(|e| Error::at("runtime launch", e.error))?;
        Ok(Running {
            node,
            http: self.http,
            dry_run: Some(self.dry_run),
        })
    }
}
pub struct Running {
    node: runtime::Running,
    http: http::ConfiguredLimits,
    dry_run: Option<http::DryRunConfig>,
}
impl Running {
    pub fn handle(&self) -> runtime::Handle {
        self.node.handle()
    }
    // The caller retains this owner while awaiting startup. Cancelling this
    // future does not detach ownership; shutdown still joins every service.
    pub async fn start_http(&mut self) -> Result<Vec<http::Handle>, Error> {
        let mut handle = self.handle();
        while !handle.status().ready {
            let status = handle.status();
            if !status.running || status.failure.is_some() {
                return Err(Error::at("runtime readiness", status.failure));
            }
            handle
                .changed()
                .await
                .map_err(|e| Error::at("runtime readiness", e))?;
        }
        self.node
            .listen_configured_http(self.http.clone(), self.dry_run.clone())
            .await
            .map_err(|e| Error::at("HTTP bind", e))
    }
    pub async fn shutdown(self) -> Result<runtime::Exit, Error> {
        self.node
            .shutdown()
            .await
            .map_err(|e| Error::at("joined shutdown", e))
    }
}
