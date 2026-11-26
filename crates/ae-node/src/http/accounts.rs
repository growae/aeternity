// aehttp_dispatch_ext account operations, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{
    Config,
    response::{self, Response},
    transaction,
};
use crate::runtime;
use ae_chain::account::{self, Selection};
use ae_codec::api::{self, ApiType, SafeDecodeError};
use ae_mempool::pool::accounts::Strategy;
use ae_storage::pool::worker;
use ae_sync::chain::{
    self,
    query::{
        Retained,
        accounts::{Kind, Rendered, Reply, Request},
    },
};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use num_bigint::BigUint;
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Operation {
    Current(Vec<u8>),
    Height { key: Vec<u8>, height: Vec<u8> },
    Hash { key: Vec<u8>, hash: Vec<u8> },
    Pending(Vec<u8>),
    Nonce { key: Vec<u8>, strategy: Strategy },
}
pub(super) fn route(parts: &[&[u8]]) -> Option<Operation> {
    Some(match parts {
        [b"accounts", key] => Operation::Current(key.to_vec()),
        [b"accounts", key, b"height", height] => Operation::Height {
            key: key.to_vec(),
            height: height.to_vec(),
        },
        [b"accounts", key, b"hash", hash] => Operation::Hash {
            key: key.to_vec(),
            hash: hash.to_vec(),
        },
        [b"accounts", key, b"transactions", b"pending"] => Operation::Pending(key.to_vec()),
        [b"accounts", key, b"next-nonce"] => Operation::Nonce {
            key: key.to_vec(),
            strategy: Strategy::Max,
        },
        _ => return None,
    })
}
fn error_code(e: SafeDecodeError) -> &'static str {
    match e {
        SafeDecodeError::InvalidPrefix => "invalid_prefix",
        SafeDecodeError::InvalidEncoding => "invalid_encoding",
    }
}
fn text(raw: &[u8]) -> Result<&str, Response> {
    // Bound work before the variable-width Base58 decoder as well as fixed keys.
    if raw.len() > 4096 {
        return Err(Response::unavailable(true));
    }
    std::str::from_utf8(raw).map_err(|_| {
        Response::json(
            400,
            json!({"reason":"Invalid public key","error_code":"invalid_encoding"}),
        )
    })
}
impl Operation {
    fn name(&self) -> &'static str {
        match self {
            Self::Current(_) => "GetAccountByPubkey",
            Self::Height { .. } => "GetAccountByPubkeyAndHeight",
            Self::Hash { .. } => "GetAccountByPubkeyAndHash",
            Self::Pending(_) => "GetPendingAccountTransactionsByPubkey",
            Self::Nonce { .. } => "GetAccountNextNonce",
        }
    }
    pub(super) fn validate(&mut self, v3: bool, query: &str) -> Result<(), Response> {
        if let Self::Height { height, .. } = self {
            super::blocks::height(height, v3)?;
        }
        if let Self::Nonce { strategy, .. } = self {
            let raw = super::transport::query_value(query, b"strategy")?;
            if matches!(raw, Some(None)) {
                return Err(Response::stream_error(500));
            }
            *strategy = match raw.as_ref().map(|v| v.as_deref().unwrap_or(b"true")) {
                None | Some(b"max") => Strategy::Max,
                Some(b"continuity") => Strategy::Continuity,
                Some(value) => {
                    return Err(Response::json(
                        400,
                        json!({"reason":"validation_error","parameter":"strategy","info":{"error":"not_in_enum","data":super::text::binary(value)}}),
                    ));
                }
            };
        }
        Ok(())
    }
    fn request(&self, v3: bool) -> Result<Request, Response> {
        let raw = match self {
            Self::Current(k)
            | Self::Pending(k)
            | Self::Height { key: k, .. }
            | Self::Hash { key: k, .. }
            | Self::Nonce { key: k, .. } => k,
        };
        let text = text(raw)?;
        let decoded = if matches!(self, Self::Pending(_) | Self::Nonce { .. }) {
            api::decode_as(ApiType::AccountPubkey, text)
        } else {
            api::decode_id(text, &[ApiType::AccountPubkey, ApiType::ContractPubkey])
                .map(|id| id.value.to_vec())
        };
        let owner = decoded.map_err(|e| {
            Response::json(
                400,
                json!({"reason":"Invalid public key","error_code":error_code(e)}),
            )
        })?;
        let kind = match self {
            Self::Current(_) => Kind::State(Selection::Current),
            Self::Height { height, .. } => {
                Kind::State(Selection::Height(super::blocks::height(height, v3)?))
            }
            Self::Hash { hash, .. } => {
                if hash.len() > 4096 {
                    return Err(Response::unavailable(true));
                }
                let result = std::str::from_utf8(hash)
                    .map_err(|_| SafeDecodeError::InvalidEncoding)
                    .and_then(api::decode_block_hash);
                let (_, bytes) = result.map_err(|e| {
                    Response::json(
                        400,
                        json!({"reason":format!("Illegal hash: {}",error_code(e))}),
                    )
                })?;
                Kind::State(Selection::Hash(Hash256(
                    bytes.try_into().expect("block hash width"),
                )))
            }
            Self::Pending(_) => Kind::Pending,
            Self::Nonce { strategy, .. } => Kind::Nonce(*strategy),
        };
        Ok(Request {
            account: AccountPublicKey(owner.try_into().expect("account key width")),
            kind,
        })
    }
}
fn limits() -> transaction::Limits {
    transaction::Limits {
        json_bytes: 8 << 20,
        work_bytes: 64 << 20,
        ..Default::default()
    }
}
pub(super) fn number(n: &Unsigned) -> Result<Value, Response> {
    if n.as_bytes()
        .len()
        .checked_mul(n.as_bytes().len())
        .is_none_or(|n| n > limits().work_bytes)
    {
        return Err(Response::unavailable(true));
    }
    Ok(
        serde_json::from_str(&BigUint::from_bytes_be(n.as_bytes()).to_str_radix(10))
            .expect("integer JSON"),
    )
}
fn account_body(info: account::Info) -> Result<Value, Response> {
    let mut body = json!({"id":api::encode(ApiType::AccountPubkey,&info.account.public_key().0),"balance":number(info.account.balance())?,"nonce":number(info.account.nonce())?,"payable":info.account.is_payable(),"kind":"basic"});
    if let Some(auth) = info.account.authorization() {
        let name = info.auth_fun.ok_or_else(Response::internal)?;
        if name.len().saturating_mul(6).saturating_add(1024) > limits().json_bytes {
            return Err(Response::unavailable(true));
        }
        body["kind"] = json!("generalized");
        body["contract_id"] = json!(api::encode_id(auth.contract));
        body["auth_fun"] = json!(super::text::binary(&name));
    }
    Ok(body)
}
fn state_error(operation: &Operation, error: account::Error) -> Response {
    use account::Error as E;
    let (code, reason, error_code) = match error {
        E::AccountNotFound => (
            404,
            "Account not found",
            if matches!(operation, Operation::Nonce { .. }) {
                Some("account_not_found")
            } else {
                None
            },
        ),
        E::NoStateTrees if matches!(operation, Operation::Hash { .. }) => {
            (404, "Hash not available", Some("no_state_trees"))
        }
        E::NoStateTrees if matches!(operation, Operation::Pending(_)) => {
            (404, "Account not found", None)
        }
        E::HeightNotAvailable => (404, "Height not available", Some("chain_too_short")),
        E::GarbageCollected => (
            410,
            "State data at the requested height has been garbage-collected",
            Some("garbage_collected"),
        ),
        E::ResourceLimit => return Response::unavailable(true),
        _ => return Response::internal(),
    };
    let mut body = json!({"reason":reason});
    if let Some(code) = error_code {
        body["error_code"] = json!(code);
    }
    Response::json(code, body)
}
fn pool_error(error: worker::Error) -> Response {
    match error {
        worker::Error::Saturated | worker::Error::RequestTooLarge => Response::unavailable(true),
        worker::Error::Service(ae_storage::pool::Failure::Policy(e)) if e.is_operational() => {
            Response::unavailable(true)
        }
        worker::Error::Stopped => Response::unavailable(false),
        _ => Response::internal(),
    }
}
pub(super) fn finish(operation: &Operation, mut body: Value, v3: bool, strings: bool) -> Response {
    if strings {
        response::integers_as_strings(&mut body);
    }
    if super::model::valid(operation.name(), &body, v3) {
        Response::json(200, body)
    } else {
        Response::invalid_response()
    }
}
fn finish_error(response: Response, v3: bool) -> Response {
    if response.status == 410 && !v3 {
        Response::invalid_response()
    } else {
        response
    }
}
fn body(operation: &Operation, reply: Reply) -> Result<Value, Response> {
    match reply {
        Reply::Account(info) => account_body(info),
        Reply::Nonce(n) => number(&n).map(|n| json!({"next_nonce":n})),
        Reply::Pending(txs) => transaction::list(&txs, transaction::Location::Pending, limits())
            .map_err(|e| {
                if e == transaction::Error::ResourceLimit {
                    Response::unavailable(true)
                } else {
                    Response::internal()
                }
            }),
        Reply::StateError(e) => Err(state_error(operation, e)),
        Reply::PoolError(e) => Err(pool_error(e)),
    }
}
fn render(operation: &Operation, reply: Reply, v3: bool, strings: bool) -> Response {
    match body(operation, reply) {
        Ok(body) => finish(operation, body, v3, strings),
        Err(r) => finish_error(r, v3),
    }
}
pub(super) async fn query(
    parent: &runtime::Handle,
    config: &Config,
    captured: Option<&Arc<chain::Importer>>,
    operation: &Operation,
    v3: bool,
    strings: bool,
) -> Result<Retained<Rendered<Response>>, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let importer = captured.ok_or_else(|| Response::unavailable(false))?;
    let permit = importer
        .account_query(limits().work_bytes + limits().json_bytes * 2)
        .map_err(super::transport::failure)?;
    let request = operation.request(v3)?;
    let bindings = parent.worker_bindings();
    let gc_enabled = if matches!(operation, Operation::Height { .. }) {
        match bindings.as_ref().and_then(|b| b.gc.as_ref()) {
            None => false,
            Some(b) => {
                b.current()
                    .and_then(|c| c.info())
                    .filter(|i| i.running)
                    .ok_or_else(|| Response::unavailable(false))?
                    .enabled
            }
        }
    } else {
        false
    };
    let pool = bindings
        .and_then(|b| b.admission.current())
        .map(|b| b.client);
    let op = operation.clone();
    let ticket = permit
        .submit(
            request,
            gc_enabled,
            pool.clone(),
            config.timeout,
            move |reply| render(&op, reply, v3, strings),
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
    if reply.value.pool_credits.is_some()
        && pool.as_ref().is_none_or(|old| {
            parent
                .worker_bindings()
                .and_then(|b| b.admission.current())
                .is_none_or(|live| !old.same_worker(&live.client))
        })
    {
        return Err(Response::unavailable(false));
    }
    Ok(reply)
}

#[cfg(test)]
mod tests;
