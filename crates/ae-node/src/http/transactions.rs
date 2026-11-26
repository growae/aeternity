// aehttp_dispatch_ext transaction reads, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{
    Config,
    response::{self, Response},
    transaction,
};
use crate::runtime;
use ae_codec::{api::ApiType, headers::Header};
use ae_sync::chain::{
    self,
    query::{
        Retained,
        transactions::{Reply, Request},
    },
};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use serde_json::{Value, json};
use std::sync::Arc;

mod model;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Operation {
    Hash(Vec<u8>),
    Micro(Vec<u8>),
    Index { hash: Vec<u8>, index: Vec<u8> },
}
pub(super) fn route(parts: &[&[u8]]) -> Option<Operation> {
    Some(match parts {
        [b"transactions", hash] => Operation::Hash((*hash).into()),
        [b"micro-blocks", b"hash", hash, b"transactions"] => Operation::Micro((*hash).into()),
        [
            b"micro-blocks",
            b"hash",
            hash,
            b"transactions",
            b"index",
            index,
        ] => Operation::Index {
            hash: (*hash).into(),
            index: (*index).into(),
        },
        _ => return None,
    })
}
fn index(raw: &[u8]) -> Result<Option<usize>, Response> {
    let integer = std::str::from_utf8(raw).ok().and_then(|s| {
        let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
        (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse::<BigInt>().ok())
            .flatten()
    });
    if let Some(n) = integer.as_ref().filter(|n| *n > &BigInt::from(0)) {
        return Ok(n.to_usize());
    }
    let (error, data) = match integer {
        Some(n) => (
            "not_in_range",
            serde_json::from_str::<Value>(&n.to_string()).expect("integer JSON"),
        ),
        None => ("wrong_type", json!(super::text::binary(raw))),
    };
    Err(Response::json(
        400,
        json!({"reason":"validation_error","parameter":"index","info":{"error":error,"data":data}}),
    ))
}
impl Operation {
    fn name(&self) -> &'static str {
        match self {
            Self::Hash(_) => "GetTransactionByHash",
            Self::Micro(_) => "GetMicroBlockTransactionsByHash",
            Self::Index { .. } => "GetMicroBlockTransactionByHashAndIndex",
        }
    }
    pub(super) fn group(&self) -> &'static str {
        if matches!(self, Self::Hash(_)) {
            "transaction"
        } else {
            "chain"
        }
    }
    pub(super) fn validate_path(&self) -> Result<(), Response> {
        if let Self::Index { index: raw, .. } = self {
            index(raw)?;
        }
        Ok(())
    }
    fn request(&self) -> Result<Request, Response> {
        let hash = super::blocks::hash;
        Ok(match self {
            Self::Hash(raw) => Request::Hash(hash(raw, ApiType::TxHash, true)?),
            Self::Micro(raw) => Request::Micro(hash(raw, ApiType::MicroBlockHash, true)?),
            Self::Index {
                hash: raw,
                index: i,
            } => Request::MicroIndex {
                hash: hash(raw, ApiType::MicroBlockHash, false)
                    .map_err(|_| Response::json(400, json!({"reason":"Invalid hash or index"})))?,
                index: index(i)?,
            },
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
fn location(header: Option<&Header>) -> Result<transaction::Location, Response> {
    match header {
        Some(h) => Ok(transaction::Location::Mined {
            height: h.common().height,
            block: h.hash().map_err(|_| Response::internal())?,
        }),
        None => Ok(transaction::Location::Pending),
    }
}
fn body(operation: &Operation, reply: Reply) -> Result<Value, Response> {
    match reply {
        Reply::Transaction {
            transaction: tx,
            header,
        } => transaction::signed(&tx, location(header.as_ref())?, limits())
            .map_err(|_| Response::internal()),
        Reply::Transactions {
            transactions,
            header,
        } => transaction::list(&transactions, location(Some(&header))?, limits())
            .map_err(|_| Response::internal()),
        Reply::MissingTransaction => Err(Response::json(
            404,
            json!({"reason":"Transaction not found"}),
        )),
        Reply::MissingBlock => Err(Response::json(
            404,
            if matches!(operation, Operation::Micro(_)) {
                json!({"reason":"Block not found","error_code":"block_not_found"})
            } else {
                json!({"reason":"Block not found"})
            },
        )),
        Reply::InvalidIndex => Err(Response::json(
            400,
            json!({"reason":"Invalid hash or index"}),
        )),
    }
}
pub(super) fn finish(operation: &Operation, mut body: Value, v3: bool, strings: bool) -> Response {
    if strings {
        response::integers_as_strings(&mut body);
    }
    if model::valid(operation.name(), &body, v3) {
        Response::json(200, body)
    } else {
        Response::invalid_response()
    }
}
pub(super) async fn query(
    parent: &runtime::Handle,
    config: &Config,
    captured: Option<&Arc<chain::Importer>>,
    operation: &Operation,
    v3: bool,
    strings: bool,
) -> Result<Retained<Response>, Response> {
    operation.validate_path()?;
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let importer = captured.ok_or_else(|| Response::unavailable(false))?;
    // Credit covers the bounded JSON tree and its encoded response together.
    // Keep it until the socket owner drops the response, including cancellation.
    let permit = importer
        .transaction_query(limits().json_bytes * 2)
        .map_err(super::transport::failure)?;
    let request = operation.request()?;
    let operation = operation.clone();
    let ticket = permit
        .submit(request, move |reply| match body(&operation, reply) {
            Ok(body) => finish(&operation, body, v3, strings),
            Err(response) => response,
        })
        .map_err(super::transport::failure)?;
    let result = tokio::time::timeout(config.timeout, ticket.wait_retained())
        .await
        .map_err(|_| Response::unavailable(false))?
        .map_err(super::transport::failure)?;
    if !parent.status().ready
        || parent
            .importer()
            .is_none_or(|live| !Arc::ptr_eq(&live, importer))
    {
        return Err(Response::unavailable(false));
    }
    Ok(result)
}
