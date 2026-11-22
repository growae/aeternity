use crate::{Committed, Database, Snapshot};
use ae_chain::{block::Block, repository as repo};
use ae_hyperchain::block::{Host, State};
use ae_vm_fate::maps::MapOrder;
use std::sync::{Arc, Mutex, Weak};
pub mod candidate;
#[cfg(test)]
mod tests;

pub use repo::hyperchain::producer;
pub use repo::{HyperchainConfig as Config, HyperchainError as PrepareError};

pub struct Produced {
    pub prepared: producer::Prepared,
    pub cursor: crate::events::Cursor,
}
pub struct ProducerRequest {
    pub binding: producer::Binding,
    pub input: producer::Input,
    pub keys: ae_hyperchain::signing::Keys,
    pub parent: Arc<ae_hyperchain::parent_cache::Cache>,
}
#[derive(Debug)]
pub enum ProducerError<G> {
    Repository(repo::Error),
    Execution(producer::Failure),
    Guard(G),
    RecoveryRequired,
}

// A scoped execution host may borrow the insertion's RocksDB snapshot. Only
// its isolated, owned acceptance state survives the attempt. Capturing and
// detaching must not publish external effects; consumers own their delivery.
pub trait Inputs<H, G> {
    type Error;
    type Host<'db>: Host<Error = Self::Error>;
    fn capture<'db>(
        &mut self,
        view: &repo::View<Snapshot<'db>>,
        block: &Block,
        previous: &H,
    ) -> Result<Self::Host<'db>, G>;
    // Called only after an operational preparation failure, outside the
    // database writer. Add bounded immutable facts to the original host and
    // return true to replay against the same child snapshot and base state.
    fn acquire<'db>(
        &mut self,
        _host: &mut Self::Host<'db>,
        _error: &PrepareError<Self::Error, Snapshot<'db>>,
        _check: &mut dyn FnMut() -> Result<(), G>,
    ) -> Result<bool, G> {
        Ok(false)
    }
    fn detach(host: Self::Host<'_>) -> H;
}

struct OwnedInputs<F>(Option<F>);
impl<H: Host, G, F> Inputs<H, G> for OwnedInputs<F>
where
    F: FnOnce(&repo::View<Snapshot<'_>>, &H) -> Result<H, G>,
{
    type Error = H::Error;
    type Host<'db> = H;
    fn capture<'db>(
        &mut self,
        view: &repo::View<Snapshot<'db>>,
        _: &Block,
        previous: &H,
    ) -> Result<H, G> {
        self.0.take().expect("one input capture")(view, previous)
    }
    fn detach(host: H) -> H {
        host
    }
}

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
// Durable state advances only on commit acknowledgement. Native rejections
// and producer reads can retain transient cache entries without a block write.
pub struct Owner<O, H> {
    database: Arc<Database>,
    config: Config<O>,
    live: Mutex<Live<H>>,
    _lease: Arc<()>,
    pinning: Mutex<Weak<()>>,
}
impl<O: MapOrder + Clone, H: Clone> Owner<O, H> {
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
            pinning: Mutex::new(Weak::new()),
        })
    }
    pub fn pinning_lease(&self) -> Result<Arc<()>, repo::Error> {
        let mut lease = self
            .pinning
            .lock()
            .map_err(|_| repo::Error::Backend("poisoned pinning lease".into()))?;
        if lease.upgrade().is_some() {
            return Err(repo::Error::Backend("pinning owner already active".into()));
        }
        let token = Arc::new(());
        *lease = Arc::downgrade(&token);
        Ok(token)
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
    // aec_consensus_hc:leader_for_height is cache-only; it must never compute
    // or install a schedule on behalf of the pinning agent.
    pub fn cached_leader(
        &self,
        height: &num_bigint::BigInt,
    ) -> Result<Option<ae_types::AccountPublicKey>, repo::Error> {
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
        match live.state.leaders.leader(height) {
            Ok(ae_vm_fate::value::ObjectPayload::Bytes(bytes)) => {
                Ok(Some(ae_types::AccountPublicKey(
                    bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| repo::Error::Corrupt("cached leader width"))?,
                )))
            }
            Ok(_) => Err(repo::Error::Corrupt("cached leader type")),
            Err(ae_hyperchain::leader::Error::NotInCache) => Ok(None),
            Err(_) => Err(repo::Error::Corrupt("cached leader lookup")),
        }
    }
    // Producer and validation share this serial cache authority. The parent
    // snapshot is immutable; this callback performs no network/service I/O.
    // Prepared effects belong to the caller and are not insertion's outbox.
    pub fn prepare_producer<G>(
        &self,
        request: ProducerRequest,
        mut check: impl FnMut() -> Result<(), G>,
    ) -> Result<Produced, ProducerError<G>> {
        let mut live = self
            .live
            .lock()
            .map_err(|_| ProducerError::RecoveryRequired)?;
        if live.failed || self.database.events().info().failure.is_some() {
            return Err(ProducerError::RecoveryRequired);
        }
        if !request.binding.matches(&self.config) {
            return Err(ProducerError::Repository(repo::Error::Identity));
        }
        check().map_err(ProducerError::Guard)?;
        let (view, cursor) = self
            .database
            .event_snapshot()
            .map_err(ProducerError::Repository)?;
        if live.revision != view.status().revision {
            let checkpoint = view
                .hyperchain_checkpoint()
                .map_err(ProducerError::Repository)?;
            live.revision = checkpoint.revision;
            live.state = checkpoint.state;
        }
        let mut state = live.state.clone();
        let prepared = producer::prepare(
            &view,
            &self.config,
            &request.input.config,
            &request.input.now,
            &mut state,
            request.keys,
            request.parent,
        )
        .map_err(ProducerError::Execution)?;
        check().map_err(ProducerError::Guard)?;
        if self.database.events().info().failure.is_some() {
            return Err(ProducerError::RecoveryRequired);
        }
        live.state = state;
        Ok(Produced { prepared, cursor })
    }
    // Transfer accepted input effects to their single consumer. This never
    // drains an in-flight candidate or a host awaiting recovery.
    pub fn take_host(&self) -> Result<H, repo::Error>
    where
        H: Default,
    {
        let mut live = self
            .live
            .lock()
            .map_err(|_| repo::Error::Backend("poisoned Hyperchain owner".into()))?;
        if live.failed || self.database.events().info().failure.is_some() {
            return Err(repo::Error::Backend(
                "Hyperchain storage requires reopen".into(),
            ));
        }
        Ok(std::mem::take(&mut live.host))
    }
    // Nonblocking transfer for an external delivery loop. Refused batches
    // stay in the owner; no candidate state is exposed during execution.
    pub fn try_take_host_if(
        &self,
        accept: impl FnOnce(&H) -> bool,
    ) -> Result<Option<H>, repo::Error>
    where
        H: Default,
    {
        let mut live = match self.live.try_lock() {
            Ok(live) => live,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(repo::Error::Backend("poisoned Hyperchain owner".into()));
            }
        };
        if live.failed || self.database.events().info().failure.is_some() {
            return Err(repo::Error::Backend(
                "Hyperchain storage requires reopen".into(),
            ));
        }
        Ok(accept(&live.host).then(|| std::mem::take(&mut live.host)))
    }
    pub fn insert_scoped<G, I: Inputs<H, G>>(
        &self,
        block: &Block,
        now_ms: u64,
        origin: repo::ledger::Origin,
        scope: &crate::policy::Scope,
        inputs: I,
        check: impl FnMut() -> Result<(), G>,
    ) -> Result<Committed, Error<I::Error, Snapshot<'_>, G>> {
        self.insert_inputs(block, now_ms, origin, Some(scope), inputs, check)
    }
    fn insert_inputs<G, I: Inputs<H, G>>(
        &self,
        block: &Block,
        now_ms: u64,
        origin: repo::ledger::Origin,
        scope: Option<&crate::policy::Scope>,
        mut inputs: I,
        mut check: impl FnMut() -> Result<(), G>,
    ) -> Result<Committed, Error<I::Error, Snapshot<'_>, G>> {
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
        let mut host = inputs
            .capture(&view, block, &live.host)
            .map_err(Error::Guard)?;
        check().map_err(Error::Guard)?;
        let mut state = live.state.clone();
        let mut config = self.config.clone();
        config.limits.policy.origin = origin;
        let update = loop {
            let prepared =
                repo::prepare_hyperchain(&view, block, &config, now_ms, &mut state, &mut host);
            check().map_err(Error::Guard)?;
            match prepared {
                Ok(update) => break update,
                Err(error) => {
                    if error.is_operational::<I::Host<'_>>()
                        && inputs
                            .acquire(&mut host, &error, &mut check)
                            .map_err(Error::Guard)?
                    {
                        check().map_err(Error::Guard)?;
                        state = live.state.clone();
                        continue;
                    }
                    // Native modeled rejections can populate leader caches. They
                    // have no durable block/event and disappear on restart.
                    if !error.is_operational::<I::Host<'_>>() {
                        live.state = state;
                        live.host = I::detach(host);
                    }
                    return Err(Error::Prepare(Box::new(error)));
                }
            }
        };
        // Detach before the writer: it may allocate, but it must have no
        // external effects. Failed commits discard this owned candidate.
        let host = I::detach(host);
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
}
impl<O: MapOrder + Clone, H: Host> Owner<O, H> {
    pub fn insert(
        &self,
        block: &Block,
        now_ms: u64,
        origin: repo::ledger::Origin,
    ) -> Result<Committed, Error<H::Error, Snapshot<'_>>> {
        self.insert_inputs(
            block,
            now_ms,
            origin,
            None,
            OwnedInputs(Some(|_: &repo::View<Snapshot<'_>>, host: &H| {
                Ok(host.clone())
            })),
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
        self.insert_inputs(
            block,
            now_ms,
            origin,
            Some(scope),
            OwnedInputs(Some(inputs)),
            check,
        )
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
}
