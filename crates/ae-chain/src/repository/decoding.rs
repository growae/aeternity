use crate::block::DecodeConfig;
use std::{collections::BTreeSet, sync::Arc};

#[derive(Debug, Clone)]
pub struct ReadConfig {
    pub mode: ae_codec::headers::HeaderMode,
    pub max_block_bytes: usize,
    pub max_transactions: usize,
    pub body: ae_codec::rlp::DecodeLimits,
    pub transaction: ae_tx::TransactionLimits,
    pub state_limits: ae_state::transport::Limits,
    pub existing_name_status_atoms: Option<Arc<BTreeSet<Vec<u8>>>>,
    pub max_transaction_record_bytes: usize,
}
impl Default for ReadConfig {
    fn default() -> Self {
        let c = DecodeConfig::default();
        Self {
            mode: c.mode,
            max_block_bytes: c.max_block_bytes,
            max_transactions: c.max_transactions,
            body: c.body,
            transaction: c.transaction,
            state_limits: c.state.limits,
            existing_name_status_atoms: None,
            max_transaction_record_bytes: ae_tx::storage::Config::default().max_record_bytes,
        }
    }
}
impl ReadConfig {
    pub fn block(&self) -> DecodeConfig<'_> {
        DecodeConfig {
            mode: self.mode,
            max_block_bytes: self.max_block_bytes,
            max_transactions: self.max_transactions,
            body: self.body,
            transaction: self.transaction,
            state: self.state(),
        }
    }
    fn state(&self) -> ae_state::transport::Config<'_> {
        ae_state::transport::Config {
            limits: self.state_limits,
            existing_name_status_atoms: self.existing_name_status_atoms.as_deref(),
        }
    }
    pub fn transaction_record(&self) -> ae_tx::storage::Config<'_> {
        ae_tx::storage::Config {
            transaction: self.transaction,
            state: self.state(),
            max_record_bytes: self.max_transaction_record_bytes,
        }
    }
}
