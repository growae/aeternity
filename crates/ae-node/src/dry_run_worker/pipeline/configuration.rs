use crate::configuration::{
    Configuration, dry_run, http,
    resolution::{Context, Error, Snapshot},
};
use ae_execution::dry_run::Profile;
use ae_types::Unsigned;
use std::sync::Arc;

// Construct once for a runtime generation. User configuration expansion occurs
// once; application settings are resolved lazily from this immutable context.
pub struct Settings {
    snapshot: Snapshot,
    word_bytes: usize,
}
impl Settings {
    pub fn capture(
        configuration: &Configuration,
        context: Arc<Context>,
        word_bytes: usize,
    ) -> Result<Self, Error> {
        Self::from_snapshot(configuration.capture(context)?, word_bytes)
    }
    pub fn from_snapshot(snapshot: Snapshot, word_bytes: usize) -> Result<Self, Error> {
        if !matches!(word_bytes, 4 | 8) {
            return Err(Error::Field("dry_run_heap_budget"));
        }
        Ok(Self {
            snapshot,
            word_bytes,
        })
    }
    pub fn gas_limit(&self) -> Result<serde_json::Value, Error> {
        http::external_dry_run_gas_limit(&self.snapshot.resolved())
    }
    pub fn internal_debug_endpoints(&self) -> Result<bool, Error> {
        http::internal_debug_endpoints(&self.snapshot.resolved())
    }
    pub fn timeout(&self, profile: Profile) -> Result<ae_sync::dry_run::Timeout, Error> {
        dry_run::timeout(&self.snapshot.resolved(), profile, None)?.runtime()
    }
    pub fn heap(&self) -> Result<Option<usize>, Error> {
        dry_run::native_heap_bytes(
            &dry_run::max_heap_words(&self.snapshot.resolved())?,
            self.word_bytes,
        )
    }
    pub fn metering(&self) -> Result<bool, Error> {
        dry_run::store_read_gas_metering(&self.snapshot.resolved())?
            .as_bool()
            .ok_or(Error::Field("store_read_gas_metering"))
    }
    pub fn reporting_floor(&self) -> Result<Option<Unsigned>, Error> {
        http::reporting_floor(&self.snapshot.resolved())
    }
}
