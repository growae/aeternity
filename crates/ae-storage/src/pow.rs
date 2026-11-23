// PoW candidate preparation retains one bounded, owned RocksDB snapshot.
use crate::snapshots;
use ae_chain::repository::pow;
use ae_vm_fate::maps::MapOrder;

pub struct Prepared<O> {
    pub value: pow::Prepared<snapshots::Reader, O>,
    pub metadata: snapshots::Metadata,
}
#[derive(Debug)]
pub enum Error {
    Snapshot(snapshots::Error),
    Candidate(pow::Error<snapshots::Reader>),
}
pub fn prepare_micro<O: MapOrder + Clone>(
    snapshots: &snapshots::Owner,
    config: &pow::Config<O>,
    input: pow::Input,
) -> Result<Prepared<O>, Error> {
    let captured = snapshots.view().map_err(Error::Snapshot)?;
    let value = pow::micro(&captured.value, config, input).map_err(Error::Candidate)?;
    Ok(Prepared {
        value,
        metadata: captured.metadata().clone(),
    })
}
