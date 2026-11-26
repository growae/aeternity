// One immutable startup assembly keeps the executed VM configuration paired
// with the identity written to storage. No worker or network is started here.
use super::{Config, Opened, genesis};
use crate::configuration::{
    auxiliary,
    chain::Settings,
    consensus::Kind,
    currency, execution, presets,
    resolution::{Mode, Resolved},
};
use ae_state::Trees;
use ae_types::{Hash256, Unsigned};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub mod hyperchain;
pub mod pow;

#[derive(Debug)]
pub enum Error {
    Resolution(crate::configuration::resolution::Error),
    Chain(crate::configuration::chain::Error),
    Auxiliary(auxiliary::Error),
    Execution(execution::Error),
    Genesis(genesis::Error),
}
pub struct Prepared {
    cwd: PathBuf,
    mode: Mode,
    genesis: genesis::Prepared,
    execution: execution::Bound,
    auxiliary: auxiliary::Loaded,
    currency: Result<std::sync::Arc<currency::Plan>, currency::Error>,
    dry_run_gas_limit:
        Result<std::sync::Arc<serde_json::Value>, crate::configuration::resolution::Error>,
}
pub struct Open {
    cwd: PathBuf,
    mode: Mode,
    chain: Opened,
    execution: execution::Bound,
    auxiliary: auxiliary::Loaded,
    currency: Result<std::sync::Arc<currency::Plan>, currency::Error>,
    dry_run_gas_limit:
        Result<std::sync::Arc<serde_json::Value>, crate::configuration::resolution::Error>,
}
impl Prepared {
    #[allow(clippy::too_many_arguments)]
    pub fn load(
        resolved: &Resolved<'_>,
        network: Option<&[u8]>,
        cwd: &Path,
        initial: &Trees,
        specification: &execution::Specification,
        resources: execution::Resources,
        gas_limit: &Unsigned,
        preset_limits: presets::Limits,
        auxiliary_limits: auxiliary::Limits,
    ) -> Result<Self, Error> {
        let settings = Settings::resolve(resolved, network).map_err(Error::Chain)?;
        // Metadata resolution failures belong to the request boundary. They
        // must not prevent a chain from opening or change its durable identity.
        let currency = currency::Plan::resolve(resolved, settings.profile().network_id(), cwd)
            .map(std::sync::Arc::new);
        let dry_run_gas_limit = crate::configuration::http::external_dry_run_gas_limit(resolved)
            .map(std::sync::Arc::new);
        let auxiliary = auxiliary::Plan::resolve(resolved, settings.profile().network_id(), cwd)
            .map_err(Error::Auxiliary)?
            .load(auxiliary_limits)
            .map_err(Error::Auxiliary)?;
        let consensus = if settings.consensus().genesis().kind() == Kind::Hyperchain {
            ae_protocol::ConsensusType::Pos
        } else {
            ae_protocol::ConsensusType::Pow
        };
        let execution = execution::Bound::build(
            specification,
            &auxiliary,
            settings.profile(),
            consensus,
            gas_limit,
            resources,
        )
        .map_err(Error::Execution)?;
        let genesis = genesis::Prepared::load_at(
            resolved,
            network,
            cwd,
            initial,
            execution.execution(),
            execution.fingerprint(),
            gas_limit,
            preset_limits,
        )
        .map_err(Error::Genesis)?;
        Ok(Self {
            cwd: cwd.into(),
            mode: resolved.mode().map_err(Error::Resolution)?,
            genesis,
            execution,
            auxiliary,
            currency,
            dry_run_gas_limit,
        })
    }
    pub fn genesis(&self) -> &genesis::Prepared {
        &self.genesis
    }
    pub fn execution(&self) -> &execution::Bound {
        &self.execution
    }
    pub fn auxiliary(&self) -> &auxiliary::Loaded {
        &self.auxiliary
    }
    pub fn open(self, path: impl AsRef<Path>, config: Config) -> Result<Open, Error> {
        let path = self.cwd.join(path);
        let chain = self.genesis.open(path, config).map_err(Error::Genesis)?;
        Ok(Open {
            cwd: self.cwd,
            mode: self.mode,
            chain,
            execution: self.execution,
            auxiliary: self.auxiliary,
            currency: self.currency,
            dry_run_gas_limit: self.dry_run_gas_limit,
        })
    }
}
impl Open {
    pub fn external_dry_run_gas_limit(
        &self,
    ) -> &Result<std::sync::Arc<serde_json::Value>, crate::configuration::resolution::Error> {
        &self.dry_run_gas_limit
    }
    pub(crate) fn currency(&self) -> &Result<std::sync::Arc<currency::Plan>, currency::Error> {
        &self.currency
    }
    pub fn startup_directory(&self) -> &Path {
        &self.cwd
    }
    pub fn mode(&self) -> Mode {
        self.mode
    }
    pub fn chain(&self) -> &Opened {
        &self.chain
    }
    pub fn execution(&self) -> &execution::Bound {
        &self.execution
    }
    pub fn auxiliary(&self) -> &auxiliary::Loaded {
        &self.auxiliary
    }
    pub fn checkpoints(&self) -> BTreeMap<u64, Hash256> {
        self.auxiliary.whitelist().checkpoints()
    }
}
