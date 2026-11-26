use super::{Cache, Data, Error as CacheError};
use crate::{
    blocking,
    chain::{self, Context, Importer, query::Ticket},
};
use ae_chain::{
    gas_prices::{self as gas, Source},
    repository::{self, Read},
};
use ae_types::Hash256;
use ae_vm_fate::maps::MapOrder;
use num_bigint::BigInt;
use std::{sync::Arc, time::Instant};

#[derive(Debug)]
pub enum QueryError {
    Observation(gas::Error),
    Cache(CacheError),
}
struct Cached<'a, S, O> {
    reader: gas::repository::Reader<S>,
    cache: &'a Cache,
    context: &'a Context<O>,
    cancel: &'a blocking::Cancellation,
    failure: Option<CacheError>,
}
impl<S: Read, O: MapOrder + Send + Sync + 'static> Source for Cached<'_, S, O> {
    fn header(&mut self, hash: Hash256) -> Result<Option<gas::Header>, gas::Error> {
        self.reader.header(hash)
    }
    fn stats(&mut self, hash: Hash256, protocol: u32) -> Result<gas::Stats, gas::Error> {
        let result = self.cache.get_or_fill(
            hash,
            || self.context.check(self.cancel, None),
            |deadline| {
                self.reader.uncached_stats(hash, protocol, || {
                    // Context cancellation/recovery is monotonic. The cache checks
                    // it again after the fill, preserving its typed failure; the
                    // owner also rejects any result past this shared deadline.
                    self.context
                        .check(self.cancel, None)
                        .map_err(|_| gas::Error::ResourceLimit)?;
                    if Instant::now() >= deadline {
                        return Err(gas::Error::ResourceLimit);
                    }
                    Ok(())
                })
            },
        );
        match result {
            Ok(value) => match value.data() {
                Data::Observed(stats) => Ok(stats.clone()),
                Data::MissingState => Err(gas::Error::MissingState(hash)),
            },
            Err(error) => {
                // Source uses the core observation error type. Preserve this
                // runtime error separately and restore it when traversal ends.
                self.failure = Some(error);
                Err(gas::Error::InvalidObject)
            }
        }
    }
}
impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    pub fn recent_gas_prices<T: Send + 'static>(
        &self,
        cache: Arc<Cache>,
        now_ms: i128,
        block_gas_limit: BigInt,
        rendering_bytes: usize,
        render: impl FnOnce(Result<[gas::Summary; 4], QueryError>) -> T + Send + 'static,
    ) -> Result<Ticket<T>, chain::Error> {
        self.check_open()?;
        if block_gas_limit.bits().div_ceil(8) > gas::Limits::default().integer_bytes as u64 {
            return Err(chain::Error::Busy);
        }
        let weight = gas::repository::service_read_weight(self.context.db.read_config())
            .map_err(|_| chain::Error::Busy)?
            .checked_add(rendering_bytes)
            .ok_or(chain::Error::Busy)?;
        let reservation = self.jobs.reserve(weight)?;
        let context = self.context.clone();
        let pending = reservation.submit(move |cancel| {
            context.check(&cancel, None)?;
            let view = context.db.view()?;
            let result = (|| {
                let top = view
                    .status()
                    .tip
                    .ok_or(gas::Error::Backend(repository::Error::Uninitialized))
                    .map_err(QueryError::Observation)?;
                let reader = gas::repository::Reader::service(&view, block_gas_limit)
                    .map_err(QueryError::Observation)?;
                let mut source = Cached {
                    reader,
                    cache: &cache,
                    context: &context,
                    cancel: &cancel,
                    failure: None,
                };
                let result = gas::recent(&mut source, top, now_ms, Default::default(), || {
                    context
                        .check(&cancel, None)
                        .map_err(|_| gas::Error::ResourceLimit)
                });
                match source.failure {
                    Some(error) => Err(QueryError::Cache(error)),
                    None => result.map_err(QueryError::Observation),
                }
            })();
            drop(view);
            context.check(&cancel, None)?;
            let result = render(result);
            context.check(&cancel, None)?;
            Ok(result)
        })?;
        Ok(Ticket {
            pending,
            timeout: self.context.config.timeout,
        })
    }
}
