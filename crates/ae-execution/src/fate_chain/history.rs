// aefa_chain_api:blockhash, including the VM1/2 fork badmatch, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC: repository LICENSE.
use super::Error;
pub use crate::chain_history::{HeaderLink, Snapshot};
use ae_vm_fate::{
    machine::{Exception, program::remote::HostFailure},
    types::Version,
};
pub fn block_hash(
    snapshot: &Snapshot,
    height: u64,
    key_hash: [u8; 32],
    vm: Version,
    mut maximum_reads: usize,
) -> Result<[u8; 32], HostFailure<Error>> {
    use crate::chain_history::{self, Error as E};
    let selection = chain_history::select(snapshot, height, key_hash, || {
        maximum_reads = maximum_reads.checked_sub(1).ok_or(E::ResourceLimit)?;
        Ok(())
    })
    .map_err(|e| match e {
        E::ResourceLimit => HostFailure::ResourceLimit,
        E::NeedHeight(h) => HostFailure::Backend(Error::NeedHeight(h)),
        E::NeedHeader(k) => HostFailure::Backend(Error::NeedHeader(k)),
        E::NoCommonAncestor => HostFailure::Exception(Exception::Native("case_clause")),
    })?;
    if vm <= Version::Two && selection.selected != selection.branch {
        Err(HostFailure::Exception(Exception::Native("badmatch")))
    } else {
        Ok(selection.branch)
    }
}
