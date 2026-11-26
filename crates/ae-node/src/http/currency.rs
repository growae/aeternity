// aehttp_dispatch_ext:GetCurrency, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{
    Config,
    response::{self, Response},
};
use crate::{
    configuration::currency::{Error, Limits, Metadata},
    runtime,
};
use ae_sync::chain::{self, query::Retained};
use serde_json::Value;
use std::sync::Arc;

pub(super) fn finish(mut body: Value, strings: bool) -> Response {
    if strings {
        response::integers_as_strings(&mut body);
    }
    if super::model::valid("GetCurrency", &body, true) {
        Response::json(200, body)
    } else {
        Response::invalid_response()
    }
}
fn render(result: Result<Metadata, Error>, strings: bool) -> Response {
    match result {
        Ok(mut metadata) => {
            metadata.fields.insert(
                "network_name".into(),
                super::text::binary(&metadata.network_name).into(),
            );
            finish(Value::Object(metadata.fields), strings)
        }
        Err(Error::ResourceLimit | Error::FileType) => Response::unavailable(true),
        Err(_) => Response::internal(),
    }
}
pub(super) async fn query(
    parent: &runtime::Handle,
    config: &Config,
    captured: Option<&Arc<chain::Importer>>,
    strings: bool,
) -> Result<Retained<Response>, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let importer = captured.ok_or_else(|| Response::unavailable(false))?;
    // Native GetCurrency enters http_update via the dispatcher catch-all. This
    // Rust owner bounds file work, metadata, schema checks and serialized output
    // together; no chain state is read and no transaction is admitted.
    let permit = importer
        .service_work(48 << 20)
        .map_err(super::transport::failure)?;
    let plan = parent.information.currency.clone();
    let ticket = permit
        .submit(move || render(plan.and_then(|p| p.read(Limits::default())), strings))
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
