// aehttp_dispatch_ext:GetRecentGasPrices, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{
    Config,
    response::{self, Response},
};
use crate::{configuration::http::GasReporting, runtime};
use ae_chain::gas_prices::{self as gas, Summary};
use ae_sync::chain::{
    self,
    query::{
        Retained,
        gas_prices::{Cache, Error as CacheError, QueryError},
    },
};
use num_bigint::{BigInt, Sign};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

// Runtime ownership is distinct from cloned request/handle references. Closing
// wakes pending fills and clears entries even while old handles remain alive.
pub(crate) struct Owner(pub Arc<Cache>);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.close();
    }
}

const RENDER_BYTES: usize = 8 << 20;
const INTEGER_BYTES: usize = 4096;
fn bounded(value: &BigInt) -> Result<(), Response> {
    if value.bits().div_ceil(8) > INTEGER_BYTES as u64 {
        Err(Response::unavailable(true))
    } else {
        Ok(())
    }
}
fn body(mut summaries: [Summary; 4], reporting: &GasReporting) -> Result<Value, Response> {
    // Check borrowed configuration before cloning an override into each row.
    // Integer conversion/schema parsing is also bounded here, inside the
    // already-reserved worker. Eight 4 KiB integers need well under 8 MiB of
    // overlapping numeric, JSON, string-mode and serialized response storage.
    if let Some(price) = &reporting.price {
        bounded(price)?;
    }
    for row in &summaries {
        bounded(&row.minimum)?;
        bounded(&row.utilization)?;
    }
    let mut work = 64usize << 20;
    for row in &summaries {
        for n in [
            reporting.price.as_ref().unwrap_or(&row.minimum),
            &row.utilization,
        ] {
            let bytes = n.bits().div_ceil(8) as usize;
            work = bytes
                .checked_mul(bytes)
                .and_then(|n| work.checked_sub(n))
                .ok_or_else(|| Response::unavailable(true))?;
        }
    }
    let mut rows = Vec::with_capacity(4);
    for row in &mut summaries {
        reporting.apply(row);
        let number = |n: &BigInt| {
            serde_json::from_str::<Value>(&n.to_string()).map_err(|_| Response::internal())
        };
        rows.push(json!({"minutes":row.minutes,"min_gas_price":number(&row.minimum)?,"utilization":number(&row.utilization)?}));
    }
    Ok(Value::Array(rows))
}
pub(super) fn finish(mut value: Value, v3: bool, strings: bool) -> Response {
    if strings {
        response::integers_as_strings(&mut value);
    }
    if super::model::valid("GetRecentGasPrices", &value, v3) {
        Response::json(200, value)
    } else {
        Response::invalid_response()
    }
}
fn observation(error: &gas::Error) -> Response {
    match error {
        // The original dispatcher returns 404, which neither wrapper's
        // operation schema permits; Cowboy sends an empty stream error.
        gas::Error::MissingHeader(_) => Response::invalid_response(),
        gas::Error::ResourceLimit => Response::unavailable(true),
        _ => Response::internal(),
    }
}
fn failure(error: QueryError) -> Response {
    match error {
        QueryError::Observation(e) => observation(&e),
        QueryError::Cache(CacheError::Fill(e)) => observation(&e),
        QueryError::Cache(CacheError::Busy) => Response::unavailable(true),
        QueryError::Cache(CacheError::Closed) => Response::unavailable(false),
        QueryError::Cache(CacheError::Check(e)) => super::transport::failure(e),
        QueryError::Cache(_) => Response::internal(),
    }
}
pub(super) async fn query(
    parent: &runtime::Handle,
    config: &Config,
    captured: Option<&Arc<chain::Importer>>,
    v3: bool,
    strings: bool,
) -> Result<Retained<Response>, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let importer = captured.ok_or_else(|| Response::unavailable(false))?;
    let information = parent.information.clone();
    // Only this small bounded governance argument is copied before submission;
    // reporting overrides remain borrowed through the captured Arc until render.
    let gas = information.block_gas_limit.as_bytes();
    if gas.len() > INTEGER_BYTES {
        return Err(Response::unavailable(true));
    }
    let gas = BigInt::from_bytes_be(Sign::Plus, gas);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Response::internal())?;
    let now = i128::try_from(now.as_millis()).map_err(|_| Response::internal())?;
    let ticket = importer
        .recent_gas_prices(
            information.gas_cache.clone(),
            now,
            gas,
            RENDER_BYTES,
            move |result| match result {
                Err(error) => failure(error),
                Ok(summaries) => match &information.gas_reporting {
                    Err(_) => Response::internal(),
                    Ok(reporting) => match body(summaries, reporting) {
                        Ok(body) => finish(body, v3, strings),
                        Err(error) => error,
                    },
                },
            },
        )
        .map_err(super::transport::failure)?;
    let reply = tokio::time::timeout(config.timeout, ticket.wait_retained())
        .await
        .map_err(|_| Response::internal())?
        .map_err(super::transport::failure)?;
    if !parent.status().ready
        || parent
            .importer()
            .is_none_or(|live| !Arc::ptr_eq(&live, importer))
    {
        return Err(Response::unavailable(false));
    }
    Ok(reply)
}

#[cfg(test)]
mod tests;
