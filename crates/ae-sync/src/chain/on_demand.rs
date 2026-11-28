// aec_consensus_on_demand:client_request, aeternity fa2a6d7.
// Copyright (c) 2020, Aeternity Anstalt. ISC: repository LICENSE.
pub mod control;
pub mod runtime;
pub mod service;

#[derive(Clone)]
pub struct Config {
    pub controller: runtime::Config,
    pub service: service::Config,
}
impl Config {
    pub fn new(beneficiary: ae_types::AccountPublicKey) -> Self {
        Self {
            controller: runtime::Config::new(beneficiary),
            service: Default::default(),
        }
    }
}
