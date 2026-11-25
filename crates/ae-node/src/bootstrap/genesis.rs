// Configuration -> actual genesis trees -> checked durable publication.
use super::{Config, Opened};
use crate::configuration::{
    chain::Settings,
    consensus::Kind,
    presets::{self, Plan},
    resolution::Resolved,
};
use ae_execution::transactions::owned::Owned;
use ae_state::{Trees, accounts::Account};
use ae_types::{Hash256, Unsigned};
use ae_vm_fate::maps::MapOrder;
use std::path::Path;

#[derive(Debug)]
pub enum Error {
    WorkingDirectory,
    Configuration(crate::configuration::chain::Error),
    Presets(presets::Error),
    Binding,
    Account(ae_state::accounts::AccountError),
    Hyperchain(Box<ae_hyperchain::genesis::Error>),
    Database(super::Error),
}
#[derive(Debug, Clone)]
pub struct Input {
    pub source: presets::Source,
    pub hash: Hash256,
    pub bytes: usize,
}
pub struct Prepared {
    settings: Settings,
    trees: Trees,
    execution_profile: Hash256,
    inputs: Vec<Input>,
}
impl Prepared {
    #[allow(clippy::too_many_arguments)]
    pub fn load<O: MapOrder>(
        resolved: &Resolved<'_>,
        network: Option<&[u8]>,
        initial: &Trees,
        execution: &Owned<O>,
        execution_profile: Hash256,
        configured_gas_limit: &Unsigned,
        limits: presets::Limits,
    ) -> Result<Self, Error> {
        Self::load_from(
            resolved,
            network,
            Path::new("."),
            initial,
            execution,
            execution_profile,
            configured_gas_limit,
            limits,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn load_at<O: MapOrder>(
        resolved: &Resolved<'_>,
        network: Option<&[u8]>,
        cwd: &Path,
        initial: &Trees,
        execution: &Owned<O>,
        execution_profile: Hash256,
        configured_gas_limit: &Unsigned,
        limits: presets::Limits,
    ) -> Result<Self, Error> {
        if !cwd.is_absolute() {
            return Err(Error::WorkingDirectory);
        }
        Self::load_from(
            resolved,
            network,
            cwd,
            initial,
            execution,
            execution_profile,
            configured_gas_limit,
            limits,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn load_from<O: MapOrder>(
        resolved: &Resolved<'_>,
        network: Option<&[u8]>,
        cwd: &Path,
        initial: &Trees,
        execution: &Owned<O>,
        execution_profile: Hash256,
        configured_gas_limit: &Unsigned,
        limits: presets::Limits,
    ) -> Result<Self, Error> {
        let settings = Settings::resolve(resolved, network).map_err(Error::Configuration)?;
        if settings.profile() != execution.network()
            || (if settings.consensus().genesis().kind() == Kind::Hyperchain {
                ae_protocol::ConsensusType::Pos
            } else {
                ae_protocol::ConsensusType::Pow
            }) != execution.consensus()
        {
            return Err(Error::Binding);
        }
        let plan = Plan::resolve(resolved, &settings, limits).map_err(Error::Presets)?;
        let mut inputs = vec![];
        let mut read = |source: &presets::Source| -> Result<Vec<u8>, Error> {
            let bytes = source.read_at(cwd, limits.bytes).map_err(Error::Presets)?;
            inputs.push(Input {
                source: source.clone(),
                hash: ae_crypto::blake2b_256(&bytes),
                bytes: bytes.len(),
            });
            Ok(bytes)
        };
        let accounts =
            presets::parse_accounts(&read(&plan.accounts)?, limits).map_err(Error::Presets)?;
        let accounts = presets::merge_accounts(accounts, plan.configured_accounts, limits)
            .map_err(Error::Presets)?;
        let mut trees = initial.clone();
        for a in accounts {
            let value = presets::balance(&a.amount).map_err(Error::Presets)?;
            trees
                .accounts
                .enter(&Account::new(a.public_key, value))
                .map_err(Error::Account)?;
        }
        if settings.consensus().genesis().kind() == Kind::Hyperchain {
            let spec =
                presets::hyperchain::Settings::resolve(resolved, limits).map_err(Error::Presets)?;
            let spec = spec
                .plan(
                    &read(plan.contracts.as_ref().ok_or(Error::Binding)?)?,
                    limits,
                )
                .map_err(Error::Presets)?;
            let raw = settings.genesis_header(Hash256([0; 32]));
            trees = execution
                .with_config(|cfg| {
                    ae_hyperchain::genesis::initialize(
                        &trees,
                        &spec,
                        &raw,
                        cfg,
                        configured_gas_limit,
                    )
                })
                .map_err(|e| Error::Hyperchain(Box::new(e)))?;
        }
        // Reject a changed public genesis before even creating a DB directory.
        settings
            .identity(trees.roots().hash(), execution_profile)
            .map_err(Error::Configuration)?;
        Ok(Self {
            settings,
            trees,
            execution_profile,
            inputs,
        })
    }
    pub fn settings(&self) -> &Settings {
        &self.settings
    }
    pub fn trees(&self) -> &Trees {
        &self.trees
    }
    pub fn inputs(&self) -> &[Input] {
        &self.inputs
    }
    pub fn open(self, path: impl AsRef<Path>, config: Config) -> Result<Opened, Error> {
        Opened::open(
            path,
            self.settings,
            &self.trees,
            self.execution_profile,
            config,
        )
        .map_err(Error::Database)
    }
}
