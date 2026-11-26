// aehttp_dispatch_ext:GetNodeSettings, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{
    Config,
    response::{self, Response},
};
use crate::runtime;
use ae_storage::pool::{PolicySettings, worker};
use ae_sync::chain::{self, query::Retained};
use serde_json::{Value, json};
use std::sync::Arc;

pub(super) struct Reply {
    pub response: Response,
    pub pool_credits: worker::accounts::Credits,
}
pub(super) fn finish(mut body: Value, strings: bool) -> Response {
    if strings {
        response::integers_as_strings(&mut body);
    }
    if super::model::valid("GetNodeSettings", &body, true) {
        Response::json(200, body)
    } else {
        Response::invalid_response()
    }
}
fn body(policy: PolicySettings, information: &runtime::Information) -> Result<Value, Response> {
    let dry = information
        .dry_run_gas_limit
        .as_ref()
        .map_err(|_| Response::internal())?;
    let length = match dry.as_ref() {
        Value::String(s) => s.len(),
        Value::Number(n) => n.as_str().len(),
        _ => return Err(Response::invalid_response()),
    };
    if length > 1 << 20 {
        return Err(Response::unavailable(true));
    }
    let mut work = 64usize << 20;
    for n in [
        &policy.minimum_miner_gas_price,
        &policy.maximum_auth_gas,
        &policy.nonce_offset,
        &information.block_gas_limit,
    ] {
        work = n
            .as_bytes()
            .len()
            .checked_mul(n.as_bytes().len())
            .and_then(|n| work.checked_sub(n))
            .ok_or_else(|| Response::unavailable(true))?;
    }
    Ok(json!({
        "min_miner_gas_price":super::accounts::number(&policy.minimum_miner_gas_price)?.to_string(),
        "max_auth_fun_gas":super::accounts::number(&policy.maximum_auth_gas)?,
        "mempool_tx_ttl":policy.tx_ttl,"mempool_nonce_offset":super::accounts::number(&policy.nonce_offset)?,
        "dry_run_gas_limit":dry.as_ref(),"block_gas_limit":super::accounts::number(&information.block_gas_limit)?
    }))
}
fn failure(error: worker::Error) -> Response {
    match error {
        worker::Error::Saturated
        | worker::Error::RequestTooLarge
        | worker::Error::Service(ae_storage::pool::Failure::Policy(
            ae_mempool::pool::Error::ResourceLimit,
        )) => Response::unavailable(true),
        worker::Error::Stopped | worker::Error::Disconnected => Response::unavailable(false),
        _ => Response::internal(),
    }
}
pub(super) async fn query(
    parent: &runtime::Handle,
    config: &Config,
    captured: Option<&Arc<chain::Importer>>,
    strings: bool,
) -> Result<Retained<Reply>, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let importer = captured.ok_or_else(|| Response::unavailable(false))?;
    let permit = importer
        .service_work(48 << 20)
        .map_err(super::transport::failure)?;
    let information = parent.information.clone();
    // Native reads this configuration before the pool getter values.
    information
        .dry_run_gas_limit
        .as_ref()
        .map_err(|_| Response::internal())?;
    let pool = parent
        .worker_bindings()
        .and_then(|b| b.admission.current())
        .ok_or_else(|| Response::unavailable(false))?
        .client;
    let read = async {
        let policy = pool
            .policy_settings(1 << 20)
            .map_err(failure)?
            .await
            .map_err(failure)?;
        let ticket = permit
            .submit(move || {
                let response = match body(policy.value, &information) {
                    Ok(v) => finish(v, strings),
                    Err(r) => r,
                };
                Reply {
                    response,
                    pool_credits: policy.credits,
                }
            })
            .map_err(super::transport::failure)?;
        ticket
            .wait_retained()
            .await
            .map_err(super::transport::failure)
    };
    let reply = tokio::time::timeout(config.timeout, read)
        .await
        .map_err(|_| Response::internal())??;
    if !parent.status().ready
        || parent.importer().is_none_or(|p| !Arc::ptr_eq(&p, importer))
        || parent
            .worker_bindings()
            .and_then(|b| b.admission.current())
            .is_none_or(|live| !pool.same_worker(&live.client))
    {
        return Err(Response::unavailable(false));
    }
    Ok(reply)
}
