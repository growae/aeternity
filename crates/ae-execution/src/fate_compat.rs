// aec_fork_block_settings:pre_iris_map_ordering, fa2a6d7.
// Copyright (c) 2021, Aeternity Anstalt. ISC notice in repository LICENSE.
use ae_protocol::NetworkProfile;
use ae_vm_fate::{Error, Limits, maps::listing::Overrides};

pub fn map_ordering(profile: &NetworkProfile, limits: Limits) -> Result<Overrides, Error> {
    Overrides::from_encoded(profile.pre_iris_map_ordering().iter().copied(), limits)
}
