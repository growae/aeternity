use super::*;
use ae_chain::repository::{self as repo, Read, execution::Snapshot};
use ae_protocol::{NetworkProfile, Protocol};
use ae_storage::pool::Stored;

// The immutable repository includes both chain and persisted pool locations.
// No live-top or replacement-pool lookup can be introduced during preparation.
pub struct Repository<S> {
    snapshot: Snapshot<S>,
    network: Arc<NetworkProfile>,
    maximum_signaling_headers: usize,
}
impl<S: Read> Repository<S> {
    pub fn new(
        snapshot: Snapshot<S>,
        network: Arc<NetworkProfile>,
        maximum_signaling_headers: usize,
    ) -> Result<Self, repo::Error> {
        if !snapshot.repository().identity().matches(&network) {
            return Err(repo::Error::Identity);
        }
        Ok(Self {
            snapshot,
            network,
            maximum_signaling_headers,
        })
    }
    pub fn snapshot(&self) -> &Snapshot<S> {
        &self.snapshot
    }
}
impl<S: Read> http::Source for Repository<S> {
    type Error = repo::Error;
    fn top_height(&mut self) -> Result<Option<u64>, Self::Error> {
        self.snapshot
            .repository()
            .status()
            .tip
            .map(|h| {
                self.snapshot
                    .repository()
                    .require_header(h)
                    .map(|h| h.common().height)
            })
            .transpose()
    }
    fn protocol(&mut self, height: u64) -> Result<Protocol, Self::Error> {
        self.snapshot
            .repository()
            .protocol_profile(&self.network, height, self.maximum_signaling_headers)?
            .protocol_at(height)
            .map_err(repo::Error::Protocol)
    }
    fn top_hash(&mut self) -> Result<Option<Hash256>, Self::Error> {
        Ok(self.snapshot.repository().status().tip)
    }
    fn transaction(
        &mut self,
        hash: Hash256,
    ) -> Result<http::Location, http::LookupError<Self::Error>> {
        let stored = Stored::load(self.snapshot.repository().clone())
            .map_err(http::LookupError::Operational)?;
        match stored
            .find_transaction(hash)
            .map_err(http::LookupError::Operational)?
        {
            Some((ae_mempool::pool::Location::Pool, tx)) => {
                Ok(http::Location::Mempool(Arc::new(tx)))
            }
            _ => Ok(http::Location::Other),
        }
    }
}
impl<S: Read + Send + Sync + 'static> Source for Repository<S> {
    type Accounts = S;
    type Cache = S;
    type SelectionError = ae_hyperchain::chain::LookupError;
    fn select(
        &mut self,
        hash: Hash256,
    ) -> Result<Arc<repo::execution::ContractView<S>>, ae_chain::dry_run::Error<Self::SelectionError>>
    {
        self.snapshot
            .dry_run_view(&ae_chain::dry_run::Selector::Hash(hash))
    }
}
