// aehttp_dispatch_ext name and auction operations, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{
    Config,
    response::{self, Response},
};
use crate::runtime;
use ae_chain::name::{Error, Kind, Reply};
use ae_codec::{
    api::{self, ApiType, SafeDecodeError},
    idna,
};
use ae_state::names::PointerValue;
use ae_sync::chain::{self, query::Retained};
use ae_types::Unsigned;
use serde_json::{Value, json};
use std::sync::Arc;

const JSON_BYTES: usize = 8 << 20;
const INTEGER_WORK: usize = 64 << 20;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Operation {
    pub input: Vec<u8>,
    pub kind: Kind,
}
pub(super) fn route(v3: bool, parts: &[&[u8]]) -> Option<Operation> {
    let (input, kind) = match parts {
        [b"names", input] => (*input, Kind::Name),
        [b"names", b"hash", input] if v3 => (*input, Kind::Hash),
        [b"auctions", input] => (*input, Kind::Auction),
        _ => return None,
    };
    Some(Operation {
        input: input.to_vec(),
        kind,
    })
}
impl Operation {
    fn name(&self) -> &'static str {
        match self.kind {
            Kind::Name => "GetNameEntryByName",
            Kind::Hash => "GetNameEntryByNameHash",
            Kind::Auction => "GetAuctionEntryByName",
        }
    }
    fn decode(&self) -> Result<Vec<u8>, Response> {
        if self.input.len() > 4096 {
            return Err(Response::unavailable(true));
        }
        if self.kind != Kind::Hash {
            // Text normalization follows the reader's current-state lookup.
            return Ok(self.input.clone());
        }
        std::str::from_utf8(&self.input)
            .map_err(|_| SafeDecodeError::InvalidEncoding)
            .and_then(|s| api::decode_as(ApiType::Name, s))
            .map_err(|e| {
                Response::json(
                    400,
                    json!({
                        "reason":"Invalid name hash",
                        "error_code":match e {
                            SafeDecodeError::InvalidPrefix => "invalid_prefix",
                            SafeDecodeError::InvalidEncoding => "invalid_encoding",
                        }
                    }),
                )
            })
    }
}
fn failure(error: Error) -> Response {
    let code = match error {
        Error::NotPresent => {
            return Response::json(
                404,
                json!({"reason":"Name not found","error_code":"name_not_found"}),
            );
        }
        Error::Revoked => {
            return Response::json(
                404,
                json!({"reason":"Name revoked","error_code":"name_revoked"}),
            );
        }
        Error::NoStateTrees => "no_state_trees",
        Error::Normalization(e) => match e {
            idna::Error::NoRegistrar => "no_registrar",
            idna::Error::RegistrarUnknown => "registrar_unknown",
            idna::Error::MultipleNamespaces => "multiple_namespaces",
            idna::Error::NoLabelInRegistrar => "no_label_in_registrar",
            idna::Error::InvalidCodepoint(_) | idna::Error::BadLabel(_) => "invalid_name",
            idna::Error::ReferenceException { .. } => return Response::internal(),
        },
        Error::ResourceLimit => return Response::unavailable(true),
        _ => return Response::internal(),
    };
    Response::json(
        400,
        json!({"reason":format!("Name validation failed with a reason: {code}"),"error_code":code}),
    )
}
struct Budget {
    json: usize,
    integers: usize,
}
impl Budget {
    fn charge(&mut self, bytes: usize) -> Result<(), Response> {
        self.json = self
            .json
            .checked_sub(bytes)
            .ok_or_else(|| Response::unavailable(true))?;
        Ok(())
    }
    fn number(&mut self, n: &Unsigned) -> Result<(), Response> {
        self.integers = n
            .as_bytes()
            .len()
            .checked_mul(n.as_bytes().len())
            .and_then(|n| self.integers.checked_sub(n))
            .ok_or_else(|| Response::unavailable(true))?;
        self.charge(n.as_bytes().len().saturating_mul(3).saturating_add(16))
    }
}
fn body(reply: Reply) -> Result<Value, Response> {
    let mut budget = Budget {
        json: JSON_BYTES,
        integers: INTEGER_WORK,
    };
    budget.charge(1024)?;
    // Preflight aggregate decimal work and escaped/encoded output before any
    // conversion. Preserve ordered historical pointers without sanitizing them.
    match &reply {
        Reply::Name(n) => {
            if n.pointers.len() > 4096 {
                return Err(Response::unavailable(true));
            }
            budget.number(&n.expires_by)?;
            for p in &n.pointers {
                budget.charge(p.key.len().saturating_mul(8).saturating_add(256))?;
                if let PointerValue::Data(data) = &p.value {
                    budget.charge(data.len().saturating_mul(2).saturating_add(16))?;
                }
            }
        }
        Reply::Auction(a) => {
            for n in [&a.bid, &a.started, &a.expires_by] {
                budget.number(n)?;
            }
        }
    }
    match reply {
        Reply::Name(n) => Ok(json!({
            "id":api::encode(ApiType::Name,&n.hash),
            "owner":api::encode_id(n.owner),
            "ttl":super::accounts::number(&n.expires_by)?,
            "pointers":n.pointers.into_iter().map(|p| json!({
                "key":super::text::binary(&p.key),
                "encoded_key":api::encode(ApiType::Bytearray,&p.key),
                "id":match p.value { PointerValue::Id(id) => api::encode_id(id), PointerValue::Data(data) => api::encode(ApiType::Bytearray,&data) },
            })).collect::<Vec<_>>()
        })),
        Reply::Auction(a) => Ok(json!({
            "id":api::encode(ApiType::Name,&a.name_hash),
            "highest_bidder":api::encode_id(a.bidder),
            "highest_bid":super::accounts::number(&a.bid)?,
            "started_at":super::accounts::number(&a.started)?,
            "ends_at":super::accounts::number(&a.expires_by)?,
        })),
    }
}
pub(super) fn finish(op: &Operation, mut value: Value, v3: bool, strings: bool) -> Response {
    if strings {
        response::integers_as_strings(&mut value);
    }
    if super::model::valid(op.name(), &value, v3) {
        Response::json(200, value)
    } else {
        Response::invalid_response()
    }
}
fn render(op: &Operation, result: Result<Reply, Error>, v3: bool, strings: bool) -> Response {
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
        .name_query(48 << 20)
        .map_err(super::transport::failure)?;
    let input = op.decode()?;
    let operation = op.clone();
    let ticket = permit
        .submit(input, op.kind, move |result| {
            render(&operation, result, v3, strings)
        })
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
