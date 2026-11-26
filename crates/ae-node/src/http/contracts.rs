// aehttp_dispatch_ext contract operations, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{
    Config,
    response::{self, Response},
};
use crate::runtime;
use ae_chain::contract::{Error, Kind, Reply};
use ae_codec::api::{self, ApiType, SafeDecodeError};
use ae_sync::chain::{self, query::Retained};
use serde_json::{Value, json};
use std::sync::Arc;

const JSON_BYTES: usize = 8 << 20;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Operation {
    pub key: Vec<u8>,
    pub kind: Kind,
}
pub(super) fn route(parts: &[&[u8]]) -> Option<Operation> {
    let (key, kind) = match parts {
        [b"contracts", key] => (*key, Kind::Metadata),
        [b"contracts", key, b"code"] => (*key, Kind::Code),
        [b"contracts", key, b"poi"] => (*key, Kind::Proof),
        _ => return None,
    };
    Some(Operation {
        key: key.to_vec(),
        kind,
    })
}
impl Operation {
    fn name(&self) -> &'static str {
        match self.kind {
            Kind::Metadata => "GetContract",
            Kind::Code => "GetContractCode",
            Kind::Proof => "GetContractPoI",
        }
    }
    fn decode(&self) -> Result<[u8; 32], Response> {
        if self.key.len() > 4096 {
            return Err(Response::unavailable(true));
        }
        let bytes = std::str::from_utf8(&self.key)
            .map_err(|_| SafeDecodeError::InvalidEncoding)
            .and_then(|s| api::decode_as(ApiType::ContractPubkey, s))
            .map_err(|e| {
                let code = match e {
                    SafeDecodeError::InvalidPrefix => "invalid_prefix",
                    SafeDecodeError::InvalidEncoding => "invalid_encoding",
                };
                Response::json(
                    400,
                    match self.kind {
                        Kind::Metadata => json!({"reason":"Invalid public key"}),
                        Kind::Code => json!({"reason":"Invalid public key","error_code":code}),
                        Kind::Proof => json!({"reason":"Invalid hash: pubkey"}),
                    },
                )
            })?;
        Ok(bytes.try_into().expect("contract key width"))
    }
}
fn failure(kind: Kind, error: Error) -> Response {
    let (code, body) = match error {
        Error::NotPresent if kind == Kind::Proof => {
            (404, json!({"reason":"Proof for contract not found"}))
        }
        Error::AccountNotPresent if kind == Kind::Proof => {
            (404, json!({"reason":"Proof for account not found"}))
        }
        Error::NotPresent => (
            404,
            json!({"reason":"Contract not found","error_code":"not_present"}),
        ),
        Error::NoStateTrees if kind != Kind::Proof => (
            404,
            json!({"reason":"Contract not found","error_code":"no_state_trees"}),
        ),
        Error::ResourceLimit => return Response::unavailable(true),
        _ => return Response::internal(),
    };
    Response::json(code, body)
}
fn body(reply: Reply) -> Result<Value, Response> {
    Ok(match reply {
        Reply::Metadata(c) => {
            if c.referrers
                .len()
                .saturating_mul(160)
                .saturating_add(c.deposit.as_bytes().len().saturating_mul(3))
                .saturating_add(1024)
                > JSON_BYTES
            {
                return Err(Response::unavailable(true));
            }
            json!({"id":api::encode(ApiType::ContractPubkey,&c.public_key),"owner_id":api::encode(ApiType::AccountPubkey,&c.owner.0),"vm_version":c.version.vm,"abi_version":c.version.abi,"active":c.active,"deposit":super::accounts::number(&c.deposit)?,"referrer_ids":c.referrers.iter().map(|key|api::encode(ApiType::ContractPubkey,key)).collect::<Vec<_>>()})
        }
        Reply::Code(bytes) => return encoded(bytes, false),
        Reply::Proof(bytes) => return encoded(bytes, true),
    })
}
fn encoded(bytes: Vec<u8>, proof: bool) -> Result<Value, Response> {
    if bytes.len().saturating_mul(2).saturating_add(128) > JSON_BYTES {
        return Err(Response::unavailable(true));
    }
    Ok(if proof {
        json!({"poi":api::encode(ApiType::Poi,&bytes)})
    } else {
        json!({"bytecode":api::encode(ApiType::ContractBytearray,&bytes)})
    })
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
    let value = result.map_err(|e| failure(op.kind, e)).and_then(body);
    match value {
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
        .contract_query(48 << 20)
        .map_err(super::transport::failure)?;
    let key = op.decode()?;
    let operation = op.clone();
    let ticket = permit
        .submit(key, op.kind, move |result| {
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
