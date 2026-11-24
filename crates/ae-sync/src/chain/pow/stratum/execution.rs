// The same importer supplies the database, historical AEVM configuration,
// operational VM limits and block-gas policy. No alternate runtime defaults.
use crate::chain::{Importer, strategy::Strategy};
use ae_chain::repository::{self as repo, Read};
use ae_hyperchain::chain::LookupError;
use ae_storage::{Database, snapshots};
use ae_vm_fate::maps::MapOrder;
use std::sync::Arc;

pub(super) type Build =
    dyn Fn(snapshots::Owner, repo::pow::key::Limits) -> Result<Source, Error> + Send + Sync;
type Capture = dyn Fn() -> Result<Frame, Error> + Send + Sync;
#[derive(Debug)]
pub enum Error {
    Binding,
    Repository(repo::Error),
    Snapshot(snapshots::Error),
    Lookup(LookupError),
}
pub struct Frame {
    metadata: snapshots::Metadata,
    view: Arc<repo::execution::ContractView<snapshots::Reader>>,
    vm: ae_vm_aevm::execution::Limits,
}
impl Frame {
    pub fn metadata(&self) -> &snapshots::Metadata {
        &self.metadata
    }
    pub fn view(&self) -> &repo::execution::ContractView<snapshots::Reader> {
        &self.view
    }
    pub fn vm_limits(&self) -> ae_vm_aevm::execution::Limits {
        self.vm
    }
}
#[derive(Clone)]
pub struct Source {
    database: Arc<Database>,
    limits: repo::pow::key::Limits,
    capture: Arc<Capture>,
}
impl Source {
    pub(super) fn new<O: MapOrder + Clone + Send + Sync + 'static>(
        importer: &Importer<O>,
        snapshots: snapshots::Owner,
        limits: repo::pow::key::Limits,
    ) -> Result<Self, Error> {
        if !snapshots.uses_database(importer.database()) || limits.history == 0 {
            return Err(Error::Binding);
        }
        let Strategy::Pow(profile) = &importer.context.strategy else {
            return Err(Error::Binding);
        };
        let identity = importer
            .database()
            .view()
            .map_err(Error::Repository)?
            .identity()
            .clone();
        let execution = profile.execution.clone();
        let gas_limit: ae_types::Unsigned = match execution.network().fixed_block_gas_limit() {
            Some(limit) => limit.into(),
            None => {
                let configured = &profile.rules.admission.configured_gas_limit;
                if configured.sign() == num_bigint::Sign::Minus {
                    return Err(Error::Binding);
                }
                ae_types::Unsigned::from_field_bytes(&configured.to_bytes_be().1)
                    .map_err(|_| Error::Binding)?
            }
        };
        Ok(Self {
            database: importer.database().clone(),
            limits,
            capture: Arc::new(move || {
                let captured = snapshots.view().map_err(Error::Snapshot)?;
                let tip = captured
                    .metadata()
                    .status
                    .tip
                    .ok_or(Error::Repository(repo::Error::Uninitialized))?;
                execution.with_config(|config| {
                    let snapshot = repo::execution::snapshot(
                        captured.value.clone(),
                        &identity,
                        config.network,
                        repo::execution::Config {
                            overlays: limits.overlay,
                            maximum_history_headers: limits.history,
                            configured_block_gas_limit: gas_limit.clone(),
                            aevm: Some(config.aevm.ok_or(Error::Binding)?.clone()),
                        },
                    )
                    .map_err(Error::Repository)?;
                    let view = snapshot.contract_view(tip).map_err(Error::Lookup)?;
                    Ok(Frame {
                        metadata: captured.metadata().clone(),
                        view,
                        vm: config.aevm_limits,
                    })
                })
            }),
        })
    }
    pub fn capture(&self) -> Result<Frame, Error> {
        (self.capture)()
    }
    pub fn limits(&self) -> repo::pow::key::Limits {
        self.limits
    }
    pub fn uses_database(&self, database: &Arc<Database>) -> bool {
        Arc::ptr_eq(&self.database, database)
    }
    // This is a current-view observation, not a lock on future chain commits.
    // A caller must check again at every asynchronous publication boundary.
    pub fn is_current(&self, source: &snapshots::Metadata) -> Result<bool, Error> {
        let view = self.database.view().map_err(Error::Repository)?;
        Ok(view.source().source_id() == source.source_id
            && view.status() == source.status
            && view.resilience() == source.resilience)
    }
}
