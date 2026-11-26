// aehttp_dispatch_ext:GetChannelByPubkey, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{
    Config,
    response::{self, Response},
};
use crate::runtime;
use ae_chain::channel::Error;
use ae_codec::api::{self, ApiType, SafeDecodeError};
use ae_state::channels::{Channel, Delegates};
use ae_sync::chain::{self, query::Retained};
use serde_json::{Value, json};
use std::sync::Arc;
const JSON_BYTES: usize = 8 << 20;
const INTEGER_WORK: usize = 64 << 20;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Operation {
    pub key: Vec<u8>,
}
pub(super) fn route(parts: &[&[u8]]) -> Option<Operation> {
    match parts {
        [b"channels", key] => Some(Operation { key: key.to_vec() }),
        _ => None,
    }
}
impl Operation {
    fn decode(&self) -> Result<[u8; 32], Response> {
        if self.key.len() > 4096 {
            return Err(Response::unavailable(true));
        }
        std::str::from_utf8(&self.key).map_err(|_|SafeDecodeError::InvalidEncoding)
            .and_then(|s|api::decode_as(ApiType::Channel,s))
            .map(|b|b.try_into().expect("channel key width"))
            .map_err(|e|Response::json(400,json!({"reason":"Invalid public key","error_code":match e {
                SafeDecodeError::InvalidPrefix=>"invalid_prefix",SafeDecodeError::InvalidEncoding=>"invalid_encoding"
            }})))
    }
}
fn failure(error: Error) -> Response {
    let code = match error {
        Error::NotPresent => "not_found",
        Error::NoStateTrees => "no_state_trees",
        Error::ResourceLimit => return Response::unavailable(true),
        _ => return Response::internal(),
    };
    Response::json(404, json!({"reason":"Channel not found","error_code":code}))
}
fn body(c: Channel) -> Result<Value, Response> {
    let count = match (&c.delegates, c.object_version) {
        (Delegates::Shared(ids), 1 | 2) => ids.len(),
        (
            Delegates::Separate {
                initiator,
                responder,
            },
            3,
        ) => initiator.len().saturating_add(responder.len()),
        _ => return Err(Response::internal()),
    };
    if count > 4096 {
        return Err(Response::unavailable(true));
    }
    let mut bytes = count
        .saturating_mul(160)
        .saturating_add(c.state_hash.len().saturating_mul(2))
        .saturating_add(2048);
    let mut work = INTEGER_WORK;
    // Bound the complete response before decimal conversion or allocating the
    // encoded delegate lists. Decoding policy stays in the historical object.
    for n in [
        &c.channel_amount,
        &c.initiator_amount,
        &c.responder_amount,
        &c.channel_reserve,
        &c.round,
        &c.solo_round,
        &c.lock_period,
        &c.locked_until,
    ] {
        let len = n.as_bytes().len();
        work = len
            .checked_mul(len)
            .and_then(|n| work.checked_sub(n))
            .ok_or_else(|| Response::unavailable(true))?;
        bytes = bytes.saturating_add(len.saturating_mul(3).saturating_add(16));
    }
    if bytes > JSON_BYTES {
        return Err(Response::unavailable(true));
    }
    let ids = |ids: Vec<ae_types::Id>| ids.into_iter().map(api::encode_id).collect::<Vec<_>>();
    let delegates = match c.delegates {
        Delegates::Shared(v) => json!(ids(v)),
        Delegates::Separate {
            initiator,
            responder,
        } => json!({"initiator":ids(initiator),"responder":ids(responder)}),
    };
    Ok(json!({
        "id":api::encode(ApiType::Channel,&c.public_key),
        "initiator_id":api::encode(ApiType::AccountPubkey,c.initiator.as_ref()),
        "responder_id":api::encode(ApiType::AccountPubkey,c.responder.as_ref()),
        "channel_amount":super::accounts::number(&c.channel_amount)?,
        "initiator_amount":super::accounts::number(&c.initiator_amount)?,
        "responder_amount":super::accounts::number(&c.responder_amount)?,
        "channel_reserve":super::accounts::number(&c.channel_reserve)?,
        "delegate_ids":delegates,"state_hash":api::encode(ApiType::State,&c.state_hash),
        "round":super::accounts::number(&c.round)?,"solo_round":super::accounts::number(&c.solo_round)?,
        "lock_period":super::accounts::number(&c.lock_period)?,"locked_until":super::accounts::number(&c.locked_until)?
    }))
}
pub(super) fn finish(_op: &Operation, mut value: Value, v3: bool, strings: bool) -> Response {
    if strings {
        response::integers_as_strings(&mut value);
    }
    if super::model::valid("GetChannelByPubkey", &value, v3) {
        Response::json(200, value)
    } else {
        Response::invalid_response()
    }
}
fn render(op: &Operation, result: Result<Channel, Error>, v3: bool, strings: bool) -> Response {
    match result.map_err(failure).and_then(body) {
        Ok(value) => finish(op, value, v3, strings),
        Err(r) => r,
    }
}
pub(super) async fn query(
    parent: &runtime::Handle,
    config: &Config,
    captured: Option<&Arc<chain::Importer>>,
    op: &Operation,
    v3: bool,
    strings: bool,
) -> Result<Retained<Response>, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let importer = captured.ok_or_else(|| Response::unavailable(false))?;
    let permit = importer
        .channel_query(48 << 20)
        .map_err(super::transport::failure)?;
    let key = op.decode()?;
    let operation = op.clone();
    let ticket = permit
        .submit(key, move |result| render(&operation, result, v3, strings))
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
