use crate::{Committed, Database, Snapshot};
use ae_chain::{block::Block, repository as repo};
use ae_hyperchain::block::{Host, State};
use ae_vm_fate::maps::MapOrder;
use std::sync::{Arc, Mutex};
#[cfg(test)]
mod tests;

pub use repo::{HyperchainConfig as Config, HyperchainError as PrepareError};

#[derive(Debug)]
pub enum Error<E, S, G = std::convert::Infallible> {
    Repository(repo::Error),
    Prepare(Box<PrepareError<E, S>>),
    RecoveryRequired,
    Guard(G),
}
impl<E, S, G> From<repo::Error> for Error<E, S, G> {
    fn from(e: repo::Error) -> Self {
        Self::Repository(e)
    }
}
struct Live<H> {
    revision: u64,
    state: State,
    host: H,
    failed: bool,
}
// Exactly one execution owner per open database. Hosts must provide isolated
// snapshots: Clone cannot undo I/O or mutations through shared interior state.
// Commit acknowledgement is the only boundary that advances accepted state.
pub struct Owner<O, H> {
    database: Arc<Database>,
    config: Config<O>,
    live: Mutex<Live<H>>,
    _lease: Arc<()>,
}
impl<O: MapOrder + Clone, H: Host> Owner<O, H> {
    pub fn start(database: Arc<Database>, config: Config<O>, host: H) -> Result<Self, repo::Error> {
        let checkpoint = {
            let mut lease = database
                .hyperchain_owner
                .lock()
                .map_err(|_| repo::Error::Backend("poisoned Hyperchain lease".into()))?;
            if lease.upgrade().is_some() {
                return Err(repo::Error::Backend(
                    "Hyperchain owner already active".into(),
                ));
            }
            if database.events().info().failure.is_some() {
                return Err(repo::Error::Backend(
                    "Hyperchain storage requires reopen".into(),
                ));
            }
            let view = database.view()?;
            if view.identity() != &config.identity
                || !config.identity.matches(&config.profile.network)
                || config.identity.genesis != config.rules.genesis_hash
            {
                return Err(repo::Error::Identity);
            }
            view.status().tip.ok_or(repo::Error::Uninitialized)?;
            let checkpoint = view.hyperchain_checkpoint()?;
            let token = Arc::new(());
            *lease = Arc::downgrade(&token);
            (checkpoint, token)
        };
        Ok(Self {
            database,
            config,
            live: Mutex::new(Live {
                revision: checkpoint.0.revision,
                state: checkpoint.0.state,
                host,
                failed: false,
            }),
            _lease: checkpoint.1,
        })
    }
    pub fn insert(
        &self,
        block: &Block,
        now_ms: u64,
        origin: repo::ledger::Origin,
    ) -> Result<Committed, Error<H::Error, Snapshot<'_>>> {
        self.insert_inner(
            block,
            now_ms,
            origin,
            None,
            |_, host| Ok(host.clone()),
            || Ok(()),
        )
    }
    // Capture parent inputs once for this attempt, against the same immutable
    // child view used by preparation. Returned hosts must be isolated snapshots.
    pub fn insert_guarded<G>(
        &self,
        block: &Block,
        now_ms: u64,
        origin: repo::ledger::Origin,
        scope: &crate::policy::Scope,
        inputs: impl FnOnce(&repo::View<Snapshot<'_>>, &H) -> Result<H, G>,
        check: impl FnMut() -> Result<(), G>,
    ) -> Result<Committed, Error<H::Error, Snapshot<'_>, G>> {
        self.insert_inner(block, now_ms, origin, Some(scope), inputs, check)
    }
    fn insert_inner<G>(
        &self,
        block: &Block,
        now_ms: u64,
        origin: repo::ledger::Origin,
        scope: Option<&crate::policy::Scope>,
        inputs: impl FnOnce(&repo::View<Snapshot<'_>>, &H) -> Result<H, G>,
        mut check: impl FnMut() -> Result<(), G>,
    ) -> Result<Committed, Error<H::Error, Snapshot<'_>, G>> {
        let mut live = self.live.lock().map_err(|_| Error::RecoveryRequired)?;
        if live.failed || self.database.events().info().failure.is_some() {
            return Err(Error::RecoveryRequired);
        }
        check().map_err(Error::Guard)?;
        let view = self.database.view()?;
        // An acknowledged administrative rollback or another sealed writer
        // may have advanced storage while this owner was idle.
        if live.revision != view.status().revision {
            let checkpoint = view.hyperchain_checkpoint()?;
            live.revision = checkpoint.revision;
            live.state = checkpoint.state;
        }
        let mut host = inputs(&view, &live.host).map_err(Error::Guard)?;
        check().map_err(Error::Guard)?;
        let mut state = live.state.clone();
        let mut config = self.config.clone();
        config.limits.policy.origin = origin;
        let prepared =
            repo::prepare_hyperchain(&view, block, &config, now_ms, &mut state, &mut host);
        check().map_err(Error::Guard)?;
        let update = match prepared {
            Ok(update) => update,
            Err(error) => {
                // Native modeled rejections can populate leader caches. They
                // have no durable block/event and disappear on restart.
                if !error.is_operational::<H>() {
                    live.state = state;
                    live.host = host;
                }
                return Err(Error::Prepare(Box::new(error)));
            }
        };
        let result = match scope {
            Some(scope) => self.database.commit_guarded(update, scope, check),
            None => self
                .database
                .commit(update)
                .map_err(crate::policy::CommitError::Repository),
        };
        match result {
            Ok(receipt) => {
                live.revision = receipt.change().revision;
                live.state = state;
                live.host = host;
                Ok(receipt)
            }
            Err(crate::policy::CommitError::Guard(error)) => Err(Error::Guard(error)),
            Err(crate::policy::CommitError::Repository(error)) => {
                live.failed = self.database.events().info().failure.is_some();
                Err(Error::Repository(error))
            }
        }
    }
    #[cfg(test)]
    fn commit_staged(
        &self,
        live: &mut Live<H>,
        update: repo::Update,
        state: State,
        host: H,
    ) -> Result<Committed, Error<H::Error, Snapshot<'_>>> {
        match self.database.commit(update) {
            Ok(receipt) => {
                live.revision = receipt.change().revision;
                live.state = state;
                live.host = host;
                Ok(receipt)
            }
            Err(error) => {
                // A WAL write with a lost acknowledgement cannot be retried
                // safely against this live cache. Recovery owns that decision.
                live.failed = self.database.events().info().failure.is_some();
                Err(Error::Repository(error))
            }
        }
    }
    pub fn snapshot(&self) -> Result<(u64, State, H), repo::Error> {
        let mut live = self
            .live
            .lock()
            .map_err(|_| repo::Error::Backend("poisoned Hyperchain owner".into()))?;
        if live.failed || self.database.events().info().failure.is_some() {
            return Err(repo::Error::Backend(
                "Hyperchain storage requires reopen".into(),
            ));
        }
        let view = self.database.view()?;
        if live.revision != view.status().revision {
            let checkpoint = view.hyperchain_checkpoint()?;
            live.revision = checkpoint.revision;
            live.state = checkpoint.state;
        }
        Ok((live.revision, live.state.clone(), live.host.clone()))
    }
}
