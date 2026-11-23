use crate::snapshots;
use ae_chain::{
    key::Rules,
    repository::{Identity, pow::key},
};
use ae_execution::transactions;
use ae_vm_fate::maps::MapOrder;

pub struct Prepared {
    pub value: key::Prepared<snapshots::Reader>,
    pub metadata: snapshots::Metadata,
}
#[derive(Debug)]
pub enum Error {
    Snapshot(snapshots::Error),
    Candidate(key::Error<snapshots::Reader>),
}
pub fn prepare<O: MapOrder>(
    owner: &snapshots::Owner,
    identity: &Identity,
    execution: &transactions::Config<'_, O>,
    rules: &Rules,
    input: key::Input,
    limits: key::Limits,
) -> Result<Prepared, Error> {
    let captured = owner.view().map_err(Error::Snapshot)?;
    let value = key::prepare(&captured.value, identity, execution, rules, input, limits)
        .map_err(Error::Candidate)?;
    Ok(Prepared {
        value,
        metadata: captured.metadata().clone(),
    })
}
