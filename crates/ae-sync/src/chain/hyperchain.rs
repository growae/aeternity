use super::*;
use ae_hyperchain::block::Host;
use ae_storage::{Committed, Snapshot, hyperchain as storage};
pub mod delivery;
pub mod scoped;

// The runtime captures immutable parent-chain data once per block attempt.
// Child-chain reads use this insertion's snapshot. Host clones must isolate
// execution effects; a capture may fail operationally while parents catch up.
pub trait ParentInputs: Send + Sync + 'static {
    type Host: Host + Send + 'static;
    type Error: std::fmt::Debug;
    fn capture(&self, child: &repository::View<Snapshot<'_>>) -> Result<Self::Host, Self::Error>;
}

pub(super) struct Strategy {
    pub network: ae_protocol::NetworkProfile,
    pub rules: ae_chain::hyperchain::Rules,
    pub checkpoints: BTreeMap<u64, Hash256>,
    pub engine: Box<dyn Engine>,
}
pub(super) trait Engine: Send + Sync {
    fn insert(
        &self,
        block: &Block,
        now_ms: u64,
        origin: ledger::Origin,
        scope: &ae_storage::policy::Scope,
        check: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<Committed, Error>;
}
struct Execution<O, P: ParentInputs> {
    owner: storage::Owner<O, P::Host>,
    parents: P,
}
impl<O, P> Engine for Execution<O, P>
where
    O: MapOrder + Clone + Send + Sync + 'static,
    P: ParentInputs,
    <P::Host as Host>::Error: std::fmt::Debug,
{
    fn insert(
        &self,
        block: &Block,
        now_ms: u64,
        origin: ledger::Origin,
        scope: &ae_storage::policy::Scope,
        check: &mut dyn FnMut() -> Result<(), Error>,
    ) -> Result<Committed, Error> {
        self.owner
            .insert_guarded(
                block,
                now_ms,
                origin,
                scope,
                |view, _| {
                    self.parents.capture(view).map_err(|e| Error::Validation {
                        operational: true,
                        reason: format!("Hyperchain parent snapshot: {e:?}"),
                    })
                },
                check,
            )
            .map_err(|e| match e {
                storage::Error::Guard(e) => e,
                storage::Error::RecoveryRequired => Error::RecoveryRequired,
                storage::Error::Repository(e) => e.into(),
                storage::Error::Prepare(e) => match *e {
                    // Another acknowledged writer may advance storage between
                    // the importer's admission view and the owner's capture.
                    repository::HyperchainError::Repository(repository::Error::AlreadyPresent) => {
                        Error::Storage(repository::Error::Conflict)
                    }
                    repository::HyperchainError::Repository(e)
                    | repository::HyperchainError::Common(ledger::InsertError::Repository(e)) => {
                        e.into()
                    }
                    repository::HyperchainError::Common(ledger::InsertError::Policy {
                        height,
                        top,
                    }) => Error::Policy { height, top },
                    repository::HyperchainError::Common(ledger::InsertError::WrongConsensus) => {
                        Error::WrongConsensus
                    }
                    other => Error::Validation {
                        operational: other.is_operational::<P::Host>(),
                        reason: format!("{other:?}"),
                    },
                },
            })
    }
}

impl<O: MapOrder + Clone + Send + Sync + 'static> Importer<O> {
    pub fn new_hyperchain<P: ParentInputs>(
        db: Arc<Database>,
        mut execution: repository::HyperchainConfig<O>,
        parents: P,
        config: Config,
    ) -> Result<Self, Error>
    where
        <P::Host as Host>::Error: std::fmt::Debug,
    {
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
        let host = parents
            .capture(&db.view()?)
            .map_err(|e| Error::Validation {
                operational: true,
                reason: format!("Hyperchain parent snapshot: {e:?}"),
            })?;
        execution.limits = config.ledger;
        let network = execution.profile.network.clone();
        let rules = execution.rules.clone();
        let owner = storage::Owner::start(db.clone(), execution, host)?;
        Self::with_strategy(
            db,
            strategy::Strategy::Hyperchain(Box::new(Strategy {
                network,
                rules,
                checkpoints: BTreeMap::new(),
                engine: Box::new(Execution { owner, parents }),
            })),
            config,
        )
    }
}
