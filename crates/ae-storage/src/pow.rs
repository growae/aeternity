// PoW candidate preparation retains one bounded, owned RocksDB snapshot.
use crate::snapshots;
use ae_chain::repository::pow;
use ae_vm_fate::maps::MapOrder;
use std::sync::Arc;

pub mod key;

// The volatile generator is exclusive for this opened database. A joined
// exit may retain this lease while an accepted pool receipt is quarantined.
pub struct GeneratorLease(Arc<()>);
impl GeneratorLease {
    pub fn acquire(database: &crate::Database) -> Result<Self, snapshots::Error> {
        let mut owner = database
            .pow_generator_owner
            .lock()
            .map_err(|_| snapshots::Error::Worker("poisoned PoW generator ownership".into()))?;
        if owner.upgrade().is_some() {
            return Err(snapshots::Error::AlreadyRunning);
        }
        let lease = Self(Arc::new(()));
        *owner = Arc::downgrade(&lease.0);
        Ok(lease)
    }
}

pub struct Prepared<O> {
    pub value: pow::Prepared<snapshots::Reader, O>,
    pub metadata: snapshots::Metadata,
}

// A conductor may retain its existing authority for an owned command worker.
// Cloning never creates another owner or permits a replacement to acquire it.
#[derive(Clone)]
pub struct ProducerLease(Arc<()>);
impl ProducerLease {
    pub fn acquire(database: &crate::Database) -> Result<Self, snapshots::Error> {
        let mut owner = database
            .pow_producer_owner
            .lock()
            .map_err(|_| snapshots::Error::Worker("poisoned PoW producer ownership".into()))?;
        if owner.upgrade().is_some() {
            return Err(snapshots::Error::AlreadyRunning);
        }
        let lease = Self(Arc::new(()));
        *owner = Arc::downgrade(&lease.0);
        Ok(lease)
    }
}
#[derive(Debug)]
pub enum Error {
    Snapshot(snapshots::Error),
    Candidate(pow::Error<snapshots::Reader>),
}

// The composed supervisor keeps this lease across child retirement and restart.
pub struct GroupLease(Arc<()>);
impl GroupLease {
    pub fn acquire(database: &crate::Database) -> Result<Self, snapshots::Error> {
        let mut owner = database
            .pow_group_owner
            .lock()
            .map_err(|_| snapshots::Error::Worker("poisoned PoW group ownership".into()))?;
        if owner.upgrade().is_some() {
            return Err(snapshots::Error::AlreadyRunning);
        }
        let lease = Self(Arc::new(()));
        *owner = Arc::downgrade(&lease.0);
        Ok(lease)
    }
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
