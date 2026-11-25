// Durable genesis publication, after pure startup configuration is fixed.
use crate::configuration::chain::{self, Settings};
use ae_chain::repository::{self as repository, Identity, ReadConfig, ledger};
use ae_codec::headers::KeyHeader;
use ae_state::Trees;
use ae_storage::Database;
use ae_types::Hash256;
use std::{fmt, path::Path, sync::Arc};

pub mod genesis;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration(chain::Error),
    Repository(repository::Error),
    Genesis,
    UnsupportedSchedule,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node database startup error: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<repository::Error> for Error {
    fn from(error: repository::Error) -> Self {
        Self::Repository(error)
    }
}
#[derive(Debug, Clone, Default)]
pub struct Config {
    pub read: ReadConfig,
    pub recovery: repository::rollback::Limits,
    pub genesis: ledger::Limits,
}
pub struct Opened {
    settings: Settings,
    identity: Identity,
    genesis: KeyHeader,
    database: Arc<Database>,
}
impl Opened {
    // Trees and VM identity must come from the caller's genesis/execution
    // owners. This opens a local database; it does not start any network service.
    pub fn open(
        path: impl AsRef<Path>,
        settings: Settings,
        trees: &Trees,
        execution_profile: Hash256,
        mut config: Config,
    ) -> Result<Self, Error> {
        // The current ledger is bound to one consensus family. Preserve native
        // schedule parsing, but do not silently start without transition dispatch.
        if settings.consensus().entries().len() != 1 {
            return Err(Error::UnsupportedSchedule);
        }
        let state_hash = trees.roots().hash();
        let identity = settings
            .identity(state_hash, execution_profile)
            .map_err(Error::Configuration)?;
        let genesis = settings.genesis_header(state_hash);
        config.read.mode = identity.header_mode();
        let database = Arc::new(Database::open_with_recovery_limits(
            path,
            identity.clone(),
            config.read,
            config.recovery,
        )?);
        {
            let view = database.view()?;
            if view.status().tip.is_none() {
                let update =
                    ledger::bootstrap(&view, settings.profile(), &genesis, trees, config.genesis)
                        .map_err(|e| match e {
                        ledger::InsertError::Repository(error) => Error::Repository(error),
                        _ => Error::Genesis,
                    })?;
                database.commit(update)?;
            }
        }
        Ok(Self {
            settings,
            identity,
            genesis,
            database,
        })
    }
    pub fn settings(&self) -> &Settings {
        &self.settings
    }
    pub fn identity(&self) -> &Identity {
        &self.identity
    }
    pub fn genesis(&self) -> &KeyHeader {
        &self.genesis
    }
    pub fn database(&self) -> &Arc<Database> {
        &self.database
    }
}
