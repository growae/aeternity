use super::*;
use ae_chain::repository::hyperchain::inputs;
use ae_hyperchain::parent_cache::runtime as cache;
use std::sync::Weak;
mod entropy;

// Capture may perform bounded service I/O; the returned host must contain
// only immutable snapshots and isolated staged effects. Its child reads may
// borrow the insertion snapshot without copying the complete canonical index.
pub trait ParentInputs: Send + Sync + 'static {
    type State: Clone + Default + Send + 'static;
    type Error: std::fmt::Debug;
    type HostError: std::fmt::Debug;
    type Host<'db>: Host<Error = Self::HostError>;
    // Runs under the storage writer: only nonblocking lifetime checks, no I/O.
    fn check(&self) -> Result<(), Self::Error>;
    fn capture<'db>(
        &self,
        child: &repository::View<Snapshot<'db>>,
        block: &Block,
        previous: &Self::State,
    ) -> Result<Self::Host<'db>, Self::Error>;
    // Bounded service acquisition between deterministic preparations. The
    // guard is checked during waits; this never runs under the writer.
    fn acquire<'db>(
        &self,
        _host: &mut Self::Host<'db>,
        _error: &repository::HyperchainError<Self::HostError, Snapshot<'db>>,
        _check: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<bool, Error> {
        Ok(false)
    }
    fn detach(host: Self::Host<'_>) -> Self::State;
}

fn input_error(error: impl std::fmt::Debug) -> Error {
    Error::Validation {
        operational: true,
        reason: format!("Hyperchain parent snapshot: {error:?}"),
    }
}
struct Capture<'p, P>(&'p P);
impl<P: ParentInputs> storage::Inputs<P::State, Error> for Capture<'_, P> {
    type Error = P::HostError;
    type Host<'db> = P::Host<'db>;
    fn capture<'db>(
        &mut self,
        child: &repository::View<Snapshot<'db>>,
        block: &Block,
        previous: &P::State,
    ) -> Result<Self::Host<'db>, Error> {
        self.0.capture(child, block, previous).map_err(input_error)
    }
    fn detach(host: Self::Host<'_>) -> P::State {
        P::detach(host)
    }
    fn acquire<'db>(
        &mut self,
        host: &mut Self::Host<'db>,
        error: &repository::HyperchainError<Self::Error, Snapshot<'db>>,
        check: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<bool, Error> {
        self.0.acquire(host, error, check)
    }
}
struct ScopedExecution<O, P: ParentInputs> {
    owner: Arc<storage::Owner<O, P::State>>,
    parents: P,
}
impl<O, P> Engine for ScopedExecution<O, P>
where
    O: MapOrder + Clone + Send + Sync + 'static,
    P: ParentInputs,
{
    fn insert(
        &self,
        block: &Block,
        now_ms: u64,
        origin: ledger::Origin,
        scope: &ae_storage::policy::Scope,
        check: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<Committed, Error> {
        self.owner.insert_scoped(block, now_ms, origin, scope, Capture(&self.parents), || {
            check()?;
            self.parents.check().map_err(input_error)
        }).map_err(|error| match error {
            storage::Error::Guard(error) => error,
            storage::Error::RecoveryRequired => Error::RecoveryRequired,
            storage::Error::Repository(error) => error.into(),
            storage::Error::Prepare(error) => match *error {
                repository::HyperchainError::Repository(repository::Error::AlreadyPresent) => Error::Storage(repository::Error::Conflict),
                repository::HyperchainError::Repository(error) | repository::HyperchainError::Common(ledger::InsertError::Repository(error)) => error.into(),
                repository::HyperchainError::Common(ledger::InsertError::Policy { height, top }) => Error::Policy { height, top },
                repository::HyperchainError::Common(ledger::InsertError::WrongConsensus) => Error::WrongConsensus,
                error => Error::Validation { operational: error.is_operational::<P::Host<'_>>(), reason: format!("{error:?}") },
            }
        })
    }
}

// Single-consumer access to accepted parent effects; this weak handle cannot
// keep a retired importer alive or redirect to its replacement.
pub struct Accepted<O, H> {
    owner: Weak<storage::Owner<O, H>>,
    database: Weak<Database>,
    context: Weak<crate::chain::Context<O>>,
}
// Read/prepare capability for this exact execution owner. It does not keep a
// retired importer alive or acquire a second leader cache.
pub struct Producer<O> {
    owner: Weak<storage::Owner<O, inputs::Effects>>,
    database: Weak<Database>,
    context: Weak<crate::chain::Context<O>>,
}
impl<O> Clone for Producer<O> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            database: self.database.clone(),
            context: self.context.clone(),
        }
    }
}
impl<O: MapOrder + Clone> Accepted<O, inputs::Effects> {
    pub fn producer(&self) -> Producer<O> {
        Producer {
            owner: self.owner.clone(),
            database: self.database.clone(),
            context: self.context.clone(),
        }
    }
}
impl<O: MapOrder + Clone> Producer<O> {
    pub fn uses_importer(&self, importer: &Importer<O>) -> bool {
        self.context.ptr_eq(&Arc::downgrade(&importer.context))
    }
    pub fn check(&self) -> Result<(), Error> {
        let context = self.context.upgrade().ok_or(Error::Closed)?;
        let _owner = self.owner.upgrade().ok_or(Error::Closed)?;
        producer_guard(&context)
    }
    pub fn candidate(
        &self,
        snapshots: &ae_storage::snapshots::Owner,
        request: storage::candidate::Request,
        check: impl FnMut() -> Result<(), Error>,
    ) -> Result<storage::candidate::Prepared<O>, Error> {
        self.candidate_inner(snapshots, request, None, check)
    }
    pub fn candidate_with_pins(
        &self,
        snapshots: &ae_storage::snapshots::Owner,
        request: storage::candidate::Request,
        pins: &crate::parent_pins::Client,
        check: impl FnMut() -> Result<(), Error>,
    ) -> Result<storage::candidate::Prepared<O>, Error> {
        self.candidate_inner(snapshots, request, Some(pins), check)
    }
    fn candidate_inner(
        &self,
        snapshots: &ae_storage::snapshots::Owner,
        request: storage::candidate::Request,
        pins: Option<&crate::parent_pins::Client>,
        mut check: impl FnMut() -> Result<(), Error>,
    ) -> Result<storage::candidate::Prepared<O>, Error> {
        let context = self.context.upgrade().ok_or(Error::Closed)?;
        self.owner
            .upgrade()
            .ok_or(Error::Closed)?
            .prepare_candidate_with_inputs(
                snapshots,
                request,
                |host, error, check| {
                    if let (Some(pins), inputs::InputError::PinNotCaptured(hash)) = (pins, error) {
                        acquire_pin(pins, host, hash, check)?;
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                },
                || {
                    producer_guard(&context)?;
                    if let Some(pins) = pins {
                        pins.check().map_err(input_error)?;
                    }
                    check()
                },
            )
            .map_err(|e| match e {
                storage::candidate::Error::Repository(e) => Error::Storage(e),
                storage::candidate::Error::Snapshot(ae_storage::snapshots::Error::Repository(
                    e,
                )) => Error::Storage(e),
                storage::candidate::Error::Snapshot(e) => input_error(e),
                storage::candidate::Error::RecoveryRequired => Error::RecoveryRequired,
                storage::candidate::Error::Guard(e) => e,
                storage::candidate::Error::Execution(e) => Error::Validation {
                    operational: e.operational,
                    reason: e.reason,
                },
            })
    }
    pub fn uses_database(&self, database: &Arc<Database>) -> bool {
        self.database.ptr_eq(&Arc::downgrade(database))
    }
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        self.owner.ptr_eq(&other.owner) && self.context.ptr_eq(&other.context)
    }
    pub(crate) fn pinning_lease(&self) -> Result<Arc<()>, Error> {
        self.check()?;
        Ok(self.owner.upgrade().ok_or(Error::Closed)?.pinning_lease()?)
    }
    pub fn cached_leader(
        &self,
        height: &num_bigint::BigInt,
    ) -> Result<Option<ae_types::AccountPublicKey>, Error> {
        let context = self.context.upgrade().ok_or(Error::Closed)?;
        producer_guard(&context)?;
        let result = self
            .owner
            .upgrade()
            .ok_or(Error::Closed)?
            .cached_leader(height)?;
        producer_guard(&context)?;
        Ok(result)
    }
    pub fn prepare(
        &self,
        request: storage::ProducerRequest,
        mut check: impl FnMut() -> Result<(), Error>,
    ) -> Result<storage::Produced, Error> {
        let context = self.context.upgrade().ok_or(Error::Closed)?;
        self.owner
            .upgrade()
            .ok_or(Error::Closed)?
            .prepare_producer(request, || {
                producer_guard(&context)?;
                check()
            })
            .map_err(|e| match e {
                storage::ProducerError::Repository(e) => Error::Storage(e),
                storage::ProducerError::RecoveryRequired => Error::RecoveryRequired,
                storage::ProducerError::Guard(e) => e,
                storage::ProducerError::Execution(e) => Error::Validation {
                    operational: e.operational,
                    reason: e.reason,
                },
            })
    }
}
fn producer_guard<O>(context: &crate::chain::Context<O>) -> Result<(), Error> {
    if context.stopped.load(Ordering::Acquire)
        || context.policy_scope.is_closed()
        || !context.db.policy_ready()
    {
        return Err(Error::Closed);
    }
    if context.db.policy_recovery_required()
        || context
            .recovery
            .lock()
            .map_err(|_| Error::RecoveryRequired)?
            .is_some()
    {
        return Err(Error::RecoveryRequired);
    }
    Ok(())
}
impl<O: MapOrder + Clone, H: Clone + Default> Accepted<O, H> {
    pub fn uses_database(&self, database: &Arc<Database>) -> bool {
        self.database.ptr_eq(&Arc::downgrade(database))
    }
    pub fn try_take_if(&mut self, accept: impl FnOnce(&H) -> bool) -> Result<Option<H>, Error> {
        self.owner
            .upgrade()
            .ok_or(Error::Closed)?
            .try_take_host_if(accept)
            .map_err(Error::Storage)
    }
    pub fn take(&mut self) -> Result<H, Error> {
        self.owner
            .upgrade()
            .ok_or(Error::Closed)?
            .take_host()
            .map_err(Error::Storage)
    }
}

impl<O: MapOrder + Clone + Send + Sync + 'static> Importer<O> {
    pub fn new_hyperchain_scoped<P: ParentInputs>(
        db: Arc<Database>,
        mut execution: repository::HyperchainConfig<O>,
        parents: P,
        config: Config,
    ) -> Result<(Self, Accepted<O, P::State>), Error> {
        Self::validate_config(&config)?;
        if db.events().info().failure.is_some() || db.policy_recovery_required() {
            return Err(Error::RecoveryRequired);
        }
        {
            let view = db.view()?;
            if view.identity().consensus != ConsensusType::Pos {
                return Err(Error::WrongConsensus);
            }
            if view.identity() != &execution.identity
                || !execution.identity.matches(&execution.profile.network)
                || execution.identity.genesis != execution.rules.genesis_hash
            {
                return Err(repository::Error::Identity.into());
            }
            if view.config().mode != ae_codec::headers::HeaderMode::Hyperchain
                || view.status().tip.is_none()
            {
                return Err(Error::Configuration);
            }
        }
        parents.check().map_err(input_error)?;
        execution.limits = config.ledger;
        let network = execution.profile.network.clone();
        let rules = execution.rules.clone();
        let owner = Arc::new(storage::Owner::start(
            db.clone(),
            execution,
            P::State::default(),
        )?);
        let weak_owner = Arc::downgrade(&owner);
        let database = Arc::downgrade(&db);
        let importer = Self::with_strategy(
            db,
            strategy::Strategy::Hyperchain(Box::new(Strategy {
                network,
                rules,
                checkpoints: BTreeMap::new(),
                engine: Box::new(ScopedExecution { owner, parents }),
            })),
            config,
        )?;
        let accepted = Accepted {
            owner: weak_owner,
            database,
            context: Arc::downgrade(&importer.context),
        };
        Ok((importer, accepted))
    }
}

// The pin provider returns an immutable bounded set of explicit lookup
// outcomes. Missing entries remain operational input misses. RPC adapters
// populate this snapshot before execution, never from Host::pin itself.
pub trait PinInputs: Send + Sync + 'static {
    type Error: std::fmt::Debug;
    // Runs under the storage writer: only nonblocking lifetime checks, no I/O.
    fn check(&self) -> Result<(), Self::Error>;
    fn capture(
        &self,
        child: &repository::View<Snapshot<'_>>,
        block: &Block,
    ) -> Result<Arc<inputs::Pins>, Self::Error>;
    fn acquire<S: repository::Read>(
        &self,
        _host: &mut inputs::Captured<S>,
        _hash: &[u8],
        _check: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<bool, Error> {
        Ok(false)
    }
}
#[derive(Clone)]
pub enum PinSource {
    Captured(Arc<inputs::Pins>),
    Remote(crate::parent_pins::Client),
}
impl PinInputs for PinSource {
    type Error = crate::parent_pins::Error;
    fn check(&self) -> Result<(), Self::Error> {
        match self {
            Self::Captured(_) => Ok(()),
            Self::Remote(client) => client.check(),
        }
    }
    fn capture(
        &self,
        _: &repository::View<Snapshot<'_>>,
        _: &Block,
    ) -> Result<Arc<inputs::Pins>, Self::Error> {
        self.check()?;
        Ok(match self {
            Self::Captured(pins) => pins.clone(),
            Self::Remote(_) => Arc::new(Default::default()),
        })
    }
    fn acquire<S: repository::Read>(
        &self,
        host: &mut inputs::Captured<S>,
        hash: &[u8],
        check: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<bool, Error> {
        match self {
            Self::Captured(_) => Ok(false),
            Self::Remote(client) => acquire_pin(client, host, hash, check).map(|()| true),
        }
    }
}
pub(crate) fn acquire_pin<S: repository::Read>(
    client: &crate::parent_pins::Client,
    host: &mut inputs::Captured<S>,
    hash: &[u8],
    check: &mut dyn FnMut() -> Result<(), Error>,
) -> Result<(), Error> {
    let service_error = |e| match e {
        crate::parent_pins::Error::Busy => Error::Busy,
        other => input_error(other),
    };
    check()?;
    let mut request = client.request(hash).map_err(service_error)?;
    let pin = loop {
        check()?;
        if let Some(result) = request.try_result() {
            break result.map_err(service_error)?;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    check()?;
    host.capture_pin(hash.to_vec(), pin).map_err(input_error)
}
impl PinInputs for Arc<inputs::Pins> {
    type Error = std::convert::Infallible;
    fn check(&self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn capture(
        &self,
        _: &repository::View<Snapshot<'_>>,
        _: &Block,
    ) -> Result<Arc<inputs::Pins>, Self::Error> {
        Ok(self.clone())
    }
}
pub struct Cached<P> {
    pub cache: cache::Client,
    pub pins: P,
    pub parent_start: ae_types::Unsigned,
    pub parent_length: ae_types::Unsigned,
    pub limits: inputs::Limits,
}
#[derive(Debug)]
pub enum CachedError<E> {
    Closed,
    Cache(cache::Health),
    Pins(E),
    Inputs(inputs::InputError),
}
impl<P: PinInputs> ParentInputs for Cached<P> {
    type State = inputs::Effects;
    type Error = CachedError<P::Error>;
    type HostError = inputs::InputError;
    type Host<'db> = inputs::Captured<Snapshot<'db>>;
    fn check(&self) -> Result<(), Self::Error> {
        if self.cache.is_closed() {
            return Err(CachedError::Closed);
        }
        self.pins.check().map_err(CachedError::Pins)
    }
    fn capture<'db>(
        &self,
        child: &repository::View<Snapshot<'db>>,
        block: &Block,
        previous: &inputs::Effects,
    ) -> Result<Self::Host<'db>, Self::Error> {
        self.check()?;
        let cache = self.cache.snapshot();
        if cache.health != cache::Health::Running {
            return Err(CachedError::Cache(cache.health));
        }
        let pins = self.pins.capture(child, block).map_err(CachedError::Pins)?;
        self.check()?;
        let mut host = inputs::Captured::new(
            child.clone(),
            cache.cache,
            pins,
            self.parent_start.clone(),
            self.parent_length.clone(),
            previous.clone(),
            self.limits,
        )
        .map_err(CachedError::Inputs)?;
        host.require_entropy();
        Ok(host)
    }
    fn acquire<'db>(
        &self,
        host: &mut Self::Host<'db>,
        error: &repository::HyperchainError<inputs::InputError, Snapshot<'db>>,
        check: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<bool, Error> {
        if let Some(inputs::InputError::PinNotCaptured(hash)) = error.host_error() {
            return self.pins.acquire(host, hash, check);
        }
        entropy::acquire(self, host, error, check)
    }
    fn detach(host: Self::Host<'_>) -> inputs::Effects {
        host.into_effects()
    }
}

// One fixed service incarnation; check() is atomic and safe under the writer.
pub struct RemotePins<O> {
    pub execution: ledger::HyperchainConfig<O>,
    pub client: crate::parent_pins::Client,
    pub limits: inputs::Limits,
}
#[derive(Debug)]
pub enum RemotePinError {
    Service(crate::parent_pins::Error),
    Query(String),
    Inputs(inputs::InputError),
}
impl<O: MapOrder + Clone + Send + Sync + 'static> PinInputs for RemotePins<O> {
    type Error = RemotePinError;
    fn check(&self) -> Result<(), Self::Error> {
        self.client.check().map_err(RemotePinError::Service)
    }
    fn capture(
        &self,
        child: &repository::View<Snapshot<'_>>,
        block: &Block,
    ) -> Result<Arc<inputs::Pins>, Self::Error> {
        self.check()?;
        let hash = match ledger::hyperchain_pin_request(child, block, &self.execution) {
            Ok(hash) => hash,
            Err(e) if !e.is_operational() => None, // execution reproduces native rejection
            Err(e) => return Err(RemotePinError::Query(format!("{e:?}"))),
        };
        let mut pins = inputs::Pins::default();
        if let Some(hash) = hash {
            if hash.len() > self.limits.maximum_pin_bytes || self.limits.maximum_pin_entries == 0 {
                return Err(RemotePinError::Inputs(inputs::InputError::ResourceLimit));
            }
            let pin = self.client.fetch(&hash).map_err(RemotePinError::Service)?;
            pins.insert(hash, pin, self.limits)
                .map_err(RemotePinError::Inputs)?;
        }
        self.check()?;
        Ok(Arc::new(pins))
    }
}
