// aehttp_dispatch_ext oracle operations, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{
    Config,
    response::{self, Response},
};
use crate::runtime;
use ae_chain::oracle::{Error, Kind, Reply};
use ae_codec::api::{self, ApiType, SafeDecodeError};
use ae_state::oracles::{Oracle, Query, QueryFilter};
use ae_sync::chain::{self, query::Retained};
use ae_types::Unsigned;
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use serde_json::{Value, json};
use std::sync::Arc;

const JSON_BYTES: usize = 8 << 20;
const INTEGER_WORK: usize = 64 << 20;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Selection {
    Metadata,
    Query(Vec<u8>),
    Queries {
        cursor: Option<Option<Vec<u8>>>,
        filter: QueryFilter,
        limit: usize,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Operation {
    pub key: Vec<u8>,
    pub selection: Selection,
}
pub(super) fn route(parts: &[&[u8]]) -> Option<Operation> {
    let (key, selection) = match parts {
        [b"oracles", key] => (*key, Selection::Metadata),
        [b"oracles", key, b"queries"] => (
            *key,
            Selection::Queries {
                cursor: None,
                filter: QueryFilter::All,
                limit: 20,
            },
        ),
        [b"oracles", key, b"queries", id] => (*key, Selection::Query(id.to_vec())),
        _ => return None,
    };
    Some(Operation {
        key: key.to_vec(),
        selection,
    })
}
fn validation(parameter: &str, error: &str, data: Value) -> Response {
    Response::json(
        400,
        json!({"reason":"validation_error","parameter":parameter,"info":{"error":error,"data":data}}),
    )
}
fn limit(raw: Option<Option<Vec<u8>>>) -> Result<usize, Response> {
    let raw = match raw {
        None => return Ok(20),
        Some(None) => return Err(Response::stream_error(500)),
        Some(Some(raw)) => raw,
    };
    if raw.len() > 4096 {
        return Err(Response::unavailable(true));
    }
    let integer = std::str::from_utf8(&raw).ok().and_then(|s| {
        let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
        (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse::<BigInt>().ok())
            .flatten()
    });
    if let Some(n) = integer
        .as_ref()
        .and_then(ToPrimitive::to_usize)
        .filter(|n| (1..=1000).contains(n))
    {
        return Ok(n);
    }
    let (error, data) = match integer {
        Some(n) => (
            "not_in_range",
            serde_json::from_str(&n.to_string()).expect("integer JSON"),
        ),
        None => ("wrong_type", json!(super::text::binary(&raw))),
    };
    Err(validation("limit", error, data))
}
fn decoded(raw: &[u8], kind: ApiType) -> Result<[u8; 32], SafeDecodeError> {
    let s = std::str::from_utf8(raw).map_err(|_| SafeDecodeError::InvalidEncoding)?;
    Ok(api::decode_as(kind, s)?
        .try_into()
        .expect("fixed identifier width"))
}
fn identifier_error(e: SafeDecodeError, singular_query: bool) -> Response {
    let code = match e {
        SafeDecodeError::InvalidPrefix => "invalid_prefix",
        SafeDecodeError::InvalidEncoding => "invalid_encoding",
    };
    Response::json(
        400,
        json!({"reason":if singular_query {"Invalid public key or query ID"} else {"Invalid public key"},"error_code":code}),
    )
}
impl Operation {
    fn name(&self) -> &'static str {
        match self.selection {
            Selection::Metadata => "GetOracleByPubkey",
            Selection::Query(_) => "GetOracleQueryByPubkeyAndQueryId",
            Selection::Queries { .. } => "GetOracleQueriesByPubkey",
        }
    }
    pub(super) fn validate(&mut self, query: &str) -> Result<(), Response> {
        if let Selection::Queries {
            cursor,
            filter,
            limit: count,
        } = &mut self.selection
        {
            // Native wrapper order: from (unvalidated string), limit, type.
            *cursor = super::transport::query_value(query, b"from")?;
            *count = limit(super::transport::query_value(query, b"limit")?)?;
            *filter = match super::transport::query_value(query, b"type")? {
                None => QueryFilter::All,
                Some(None) => return Err(Response::stream_error(500)),
                Some(Some(raw)) => match raw.as_slice() {
                    b"all" => QueryFilter::All,
                    b"open" => QueryFilter::Open,
                    b"closed" => QueryFilter::Closed,
                    _ => {
                        return Err(validation(
                            "type",
                            "not_in_enum",
                            json!(super::text::binary(&raw)),
                        ));
                    }
                },
            };
        }
        Ok(())
    }
    fn decode(&self) -> Result<([u8; 32], Kind), Response> {
        if self.key.len() > 4096 {
            return Err(Response::unavailable(true));
        }
        let singular_query = matches!(self.selection, Selection::Query(_));
        let key = decoded(&self.key, ApiType::OraclePubkey)
            .map_err(|e| identifier_error(e, singular_query))?;
        let kind = match &self.selection {
            Selection::Metadata => Kind::Metadata,
            Selection::Query(raw) => {
                if raw.len() > 4096 {
                    return Err(Response::unavailable(true));
                }
                Kind::Query(
                    decoded(raw, ApiType::OracleQueryId).map_err(|e| identifier_error(e, true))?,
                )
            }
            Selection::Queries {
                cursor,
                filter,
                limit,
            } => {
                let after = match cursor {
                    None => None,
                    Some(None) => return Err(Response::internal()),
                    Some(Some(raw)) => {
                        if raw.len() > 4096 {
                            return Err(Response::unavailable(true));
                        }
                        // Native code pattern-matches successful cursor decoding.
                        Some(
                            decoded(raw, ApiType::OracleQueryId)
                                .map_err(|_| Response::internal())?,
                        )
                    }
                };
                Kind::Queries {
                    after,
                    filter: *filter,
                    limit: *limit,
                }
            }
        };
        Ok((key, kind))
    }
}
fn failure(selection: &Selection, error: Error) -> Response {
    let code = match error {
        Error::NotPresent => "not_found",
        Error::NoStateTrees if matches!(selection, Selection::Queries { .. }) => {
            return Response::json(200, json!({"oracle_queries":[]}));
        }
        Error::NoStateTrees => "no_state_trees",
        Error::ResourceLimit => return Response::unavailable(true),
        _ => return Response::internal(),
    };
    Response::json(
        404,
        json!({"reason":if matches!(selection,Selection::Metadata) {"Oracle not found"} else {"Query not found"},"error_code":code}),
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
    fn binary(&mut self, raw: &[u8], text: bool) -> Result<(), Response> {
        self.charge(
            raw.len()
                .saturating_mul(if text { 6 } else { 2 })
                .saturating_add(16),
        )
    }
    fn number(&mut self, n: &Unsigned) -> Result<(), Response> {
        self.integers = n
            .as_bytes()
            .len()
            .checked_mul(n.as_bytes().len())
            .and_then(|n| self.integers.checked_sub(n))
            .ok_or_else(|| Response::unavailable(true))?;
        self.charge(n.as_bytes().len().saturating_mul(3).saturating_add(4))
    }
    fn oracle(&mut self, o: &Oracle) -> Result<(), Response> {
        self.charge(1024)?;
        let text = o.abi_version != 1.into() && o.abi_version != 3.into();
        self.binary(&o.query_format, text)?;
        self.binary(&o.response_format, text)?;
        for n in [&o.query_fee, &o.expires_by, &o.abi_version] {
            self.number(n)?;
        }
        Ok(())
    }
    fn query(&mut self, q: &Query) -> Result<(), Response> {
        self.charge(1024)?;
        self.binary(&q.query, false)?;
        self.binary(q.response.as_deref().unwrap_or_default(), false)?;
        for n in [&q.sender_nonce, &q.expires_by, &q.response_ttl, &q.fee] {
            self.number(n)?;
        }
        Ok(())
    }
}
fn query_body(q: Query) -> Result<Value, Response> {
    Ok(
        json!({"id":api::encode(ApiType::OracleQueryId,&q.id),"sender_id":api::encode_id(q.sender),"sender_nonce":super::accounts::number(&q.sender_nonce)?,"oracle_id":api::encode_id(q.oracle),"query":api::encode(ApiType::OracleQuery,&q.query),"response":api::encode(ApiType::OracleResponse,q.response.as_deref().unwrap_or_default()),"ttl":super::accounts::number(&q.expires_by)?,"response_ttl":{"type":"delta","value":super::accounts::number(&q.response_ttl)?},"fee":super::accounts::number(&q.fee)?}),
    )
}
fn body(reply: Reply) -> Result<Value, Response> {
    let mut budget = Budget {
        json: JSON_BYTES,
        integers: INTEGER_WORK,
    };
    // Charge the whole response before any decimal/Base64/text conversion.
    match &reply {
        Reply::Metadata(o) => budget.oracle(o)?,
        Reply::Query(q) => budget.query(q)?,
        Reply::Queries(qs) => {
            if qs.len() > 1000 {
                return Err(Response::unavailable(true));
            }
            budget.charge(64)?;
            for q in qs {
                budget.query(q)?;
            }
        }
    }
    match reply {
        Reply::Metadata(o) => {
            let format = |raw: &[u8]| {
                if o.abi_version == 1.into() || o.abi_version == 3.into() {
                    api::encode(ApiType::ContractBytearray, raw)
                } else {
                    super::text::binary(raw)
                }
            };
            Ok(
                json!({"id":api::encode(ApiType::OraclePubkey,&o.public_key),"query_format":format(&o.query_format),"response_format":format(&o.response_format),"query_fee":super::accounts::number(&o.query_fee)?,"ttl":super::accounts::number(&o.expires_by)?,"abi_version":super::accounts::number(&o.abi_version)?}),
            )
        }
        Reply::Query(q) => query_body(q),
        Reply::Queries(qs) => Ok(
            json!({"oracle_queries":qs.into_iter().map(query_body).collect::<Result<Vec<_>,_>>()?}),
        ),
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
    // Absence of state is a successful empty list only for the list operation.
    let result = match result {
        Err(Error::NoStateTrees) if matches!(op.selection, Selection::Queries { .. }) => {
            Ok(Reply::Queries(vec![]))
        }
        result => result,
    };
    match result.map_err(|e| failure(&op.selection, e)).and_then(body) {
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
        .oracle_query(48 << 20)
        .map_err(super::transport::failure)?;
    let (key, kind) = op.decode()?;
    let operation = op.clone();
    let ticket = permit
        .submit(key, kind, move |result| {
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
