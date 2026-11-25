use super::*;
use ae_protocol::{ConsensusType, NetworkProfile};

enum Ledger<O> {
    Owned(Box<(Owned<O>, Rules)>),
    Borrowed(chain::Importer<O>),
}
// A root owner can retain admission across connection generations. Legacy
// standalone inputs still construct one importer per connection generation.
pub(super) struct Sources<O> {
    pub database: Arc<Database>,
    pub admission: admission::Client,
    pub gc: Option<gc::Client>,
    pub identity: Arc<noise::Identity>,
    ledger: Ledger<O>,
}
impl<O> From<Inputs<O>> for Sources<O> {
    fn from(inputs: Inputs<O>) -> Self {
        Self {
            database: inputs.database,
            admission: inputs.admission,
            gc: inputs.gc,
            identity: inputs.identity,
            ledger: Ledger::Owned(Box::new((inputs.execution, inputs.rules))),
        }
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Sources<O> {
    pub fn pow(
        importer: &chain::Importer<O>,
        admission: admission::Client,
        identity: Arc<noise::Identity>,
    ) -> Result<Self, Error> {
        if importer.consensus() != ConsensusType::Pow {
            return Err(Error::Configuration);
        }
        importer
            .check_open()
            .map_err(|_| Error::Dependency(Child::Importer))?;
        Ok(Self {
            database: importer.database().clone(),
            admission,
            gc: None,
            identity,
            ledger: Ledger::Borrowed(importer.borrowed()),
        })
    }
    pub fn accepts_gc(&self, configured: bool) -> bool {
        // Borrowed paths receive a managed worker handle. Its GC may still
        // be awaiting initial publication or an independent replacement.
        matches!(self.ledger, Ledger::Borrowed(_)) || self.gc.is_some() == configured
    }
    pub fn hyperchain(
        producer: &chain::hyperchain::conductor::Conductor<O>,
        identity: Arc<noise::Identity>,
    ) -> Self {
        let importer = producer.importer();
        Self {
            database: importer.database().clone(),
            admission: producer.pool(),
            gc: None,
            identity,
            ledger: Ledger::Borrowed(importer.borrowed()),
        }
    }
    pub fn network(&self) -> &NetworkProfile {
        match &self.ledger {
            Ledger::Owned(input) => input.0.network(),
            Ledger::Borrowed(importer) => importer.network(),
        }
    }
    pub fn consensus(&self) -> ConsensusType {
        match &self.ledger {
            Ledger::Owned(input) => input.0.consensus(),
            Ledger::Borrowed(importer) => importer.consensus(),
        }
    }
    pub fn importer(&self, config: &chain::Config) -> Result<chain::Importer<O>, chain::Error> {
        match &self.ledger {
            Ledger::Owned(input) => chain::Importer::new(
                self.database.clone(),
                input.0.clone(),
                input.1.clone(),
                config.clone(),
            ),
            Ledger::Borrowed(importer) => {
                importer.check_open()?;
                if importer.gossip_depth() != config.ledger.policy.gossip_depth {
                    return Err(chain::Error::Configuration);
                }
                Ok(importer.shared())
            }
        }
    }
    pub fn health(&self) -> Result<(), Error> {
        if let Ledger::Borrowed(importer) = &self.ledger {
            importer.check_open().map_err(|e| {
                if e == chain::Error::RecoveryRequired {
                    Error::RecoveryRequired
                } else {
                    Error::Dependency(Child::Importer)
                }
            })?;
        }
        Ok(())
    }
}
