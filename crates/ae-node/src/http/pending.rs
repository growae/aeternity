// aehttp_dispatch_ext:GetPendingKeyBlock, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{Config, response::Response};
use crate::runtime;
use ae_sync::chain::{self, pending::service, query};
use serde_json::{Value, json};
use std::sync::Arc;

fn missing(code: &str) -> Response {
    Response::json(404, json!({"reason":"Block not found","error_code":code}))
}
fn failure(error: &service::Error) -> Response {
    match error {
        service::Error::NotFound => missing("not_found"),
        service::Error::BeneficiaryNotConfigured => Response::json(
            400,
            json!({"reason":"Beneficiary not configured","error_code":"beneficiary_not_configured"}),
        ),
        service::Error::Busy | service::Error::Keys(ae_sync::keys::Error::Busy) => {
            Response::unavailable(true)
        }
        service::Error::Closed | service::Error::KeysUnavailable => Response::unavailable(false),
        service::Error::Source(error) => super::transport::failure(error.clone()),
        _ => Response::internal(),
    }
}
pub(super) fn body(header: Option<query::blocks::ClientHeader>) -> Result<Value, Response> {
    let header = header.ok_or_else(|| Response::json(404, json!({"reason":"Block not found"})))?;
    super::blocks::header(&header, Some(false))
}
pub(super) struct Read {
    pub value: Value,
    pub credits: query::ReadCredits,
    pub candidate: Arc<service::Completion>,
}
pub(super) async fn query(
    parent: &runtime::Handle,
    config: &Config,
    importer: Option<&Arc<chain::Importer>>,
) -> Result<Read, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let importer = importer.ok_or_else(|| Response::unavailable(false))?;
    let reader = importer
        .pending_header_query()
        .map_err(super::transport::failure)?;
    let pending = parent
        .pending_key()
        .ok_or_else(|| Response::unavailable(false))?;
    let ticket = pending.get().map_err(|error| failure(&error))?;
    let candidate = tokio::time::timeout(config.timeout, ticket)
        .await
        .map_err(|_| Response::internal())?
        .map_err(|error| failure(&error))?;
    let header = candidate.result.as_ref().map_err(failure)?.clone();
    let header = reader.submit(header).map_err(super::transport::failure)?;
    let header = tokio::time::timeout(config.timeout, header.wait_retained())
        .await
        .map_err(|_| Response::unavailable(false))?
        .map_err(super::transport::failure)?;
    if !parent.status().ready
        || parent
            .importer()
            .is_none_or(|live| !Arc::ptr_eq(&live, importer))
        || parent
            .pending_key()
            .is_none_or(|live| !pending.same_service(&live))
    {
        return Err(Response::unavailable(false));
    }
    Ok(Read {
        value: body(header.value)?,
        credits: header.credits,
        candidate,
    })
}

#[cfg(test)]
mod tests;
