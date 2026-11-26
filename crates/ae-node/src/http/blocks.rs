// aehttp_dispatch_ext / aec_headers client responses, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{Config, response::Response};
use crate::runtime;
use ae_codec::{
    api::{self, ApiType},
    headers::Header,
};
use ae_sync::chain::{
    self,
    query::blocks::{ClientHeader, Kind, Missing, Reply, Request},
};
use ae_types::Hash256;
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use serde_json::{Value, json};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Operation {
    Top { envelope: bool },
    CurrentKey,
    KeyHash(Vec<u8>),
    KeyHeight(Vec<u8>),
    MicroHeader(Vec<u8>),
    MicroCount(Vec<u8>),
    CurrentGeneration,
    GenerationHash(Vec<u8>),
    GenerationHeight(Vec<u8>),
}

pub(super) fn route(v3: bool, parts: &[&[u8]]) -> Option<Operation> {
    Some(match parts {
        [b"blocks", b"top"] if !v3 => Operation::Top { envelope: true },
        [b"headers", b"top"] if v3 => Operation::Top { envelope: false },
        [b"key-blocks", b"current"] => Operation::CurrentKey,
        [b"key-blocks", b"hash", hash] => Operation::KeyHash((*hash).into()),
        [b"key-blocks", b"height", height] => Operation::KeyHeight((*height).into()),
        [b"micro-blocks", b"hash", hash, b"header"] => Operation::MicroHeader((*hash).into()),
        [b"micro-blocks", b"hash", hash, b"transactions", b"count"] => {
            Operation::MicroCount((*hash).into())
        }
        [b"generations", b"current"] => Operation::CurrentGeneration,
        [b"generations", b"hash", hash] => Operation::GenerationHash((*hash).into()),
        [b"generations", b"height", height] => Operation::GenerationHeight((*height).into()),
        _ => return None,
    })
}

pub(super) fn height(raw: &[u8], v3: bool) -> Result<u128, Response> {
    let integer = std::str::from_utf8(raw).ok().and_then(|raw| {
        let digits = raw.strip_prefix(['+', '-']).unwrap_or(raw);
        (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
            .then(|| raw.parse::<BigInt>().ok())
            .flatten()
    });
    let maximum = if v3 {
        18_446_744_073_709_552_000
    } else {
        u64::MAX as u128
    };
    if let Some(n) = integer
        .as_ref()
        .and_then(ToPrimitive::to_u128)
        .filter(|n| *n <= maximum)
    {
        return Ok(n);
    }
    // The OAS validator's error term is not JSX encodable. Cowboy emits an
    // empty stream error; the v2 coercion wrapper produces a structured error.
    if v3 {
        return Err(Response::stream_error(500));
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
        json!({"reason":"validation_error","parameter":"height","info":{"error":error,"data":data}}),
    ))
}
pub(super) fn hash(raw: &[u8], kind: ApiType, code: bool) -> Result<Hash256, Response> {
    // Base58 hash/public-key encodings have fixed widths. Reject impossible
    // lengths before the quadratic decoder; variable-width types use base64.
    let decoded = std::str::from_utf8(raw)
        .map_err(|_| api::SafeDecodeError::InvalidEncoding)
        .and_then(|raw| {
            let impossible = raw.split_once('_').is_some_and(|(prefix, body)| {
                ApiType::from_prefix(prefix).is_some_and(|kind| {
                    !kind.is_base64()
                        && kind
                            .payload_size()
                            .is_some_and(|n| body.len() > (n + 4) * 138 / 100 + 1)
                })
            });
            if impossible {
                Err(api::SafeDecodeError::InvalidEncoding)
            } else {
                api::decode_as(kind, raw)
            }
        });
    match decoded {
        Ok(bytes) => Ok(Hash256(
            bytes.try_into().expect("hash encoder enforces size"),
        )),
        Err(error) => {
            let mut body = json!({"reason":"Invalid hash"});
            if code {
                body["error_code"] = json!(match error {
                    api::SafeDecodeError::InvalidPrefix => "invalid_prefix",
                    api::SafeDecodeError::InvalidEncoding => "invalid_encoding",
                });
            }
            Err(Response::json(400, body))
        }
    }
}
impl Operation {
    pub(super) fn validate_path(&self, v3: bool) -> Result<(), Response> {
        if let Self::KeyHeight(raw) | Self::GenerationHeight(raw) = self {
            height(raw, v3)?;
        }
        Ok(())
    }
    pub(super) fn request(&self, v3: bool) -> Result<Request, Response> {
        Ok(match self {
            Self::Top { .. } => Request::Top,
            Self::CurrentKey => Request::CurrentKey,
            Self::KeyHash(raw) => Request::KeyHash(hash(raw, ApiType::KeyBlockHash, false)?),
            Self::KeyHeight(raw) => Request::KeyHeight(height(raw, v3)?),
            Self::MicroHeader(raw) => {
                Request::MicroHeader(hash(raw, ApiType::MicroBlockHash, true)?)
            }
            Self::MicroCount(raw) => Request::MicroCount(hash(raw, ApiType::MicroBlockHash, true)?),
            Self::CurrentGeneration => Request::CurrentGeneration,
            Self::GenerationHash(raw) => {
                Request::GenerationHash(hash(raw, ApiType::KeyBlockHash, true)?)
            }
            Self::GenerationHeight(raw) => Request::GenerationHeight(height(raw, v3)?),
        })
    }
}

// Stored canonical bytes normalize all-zero seals to the Erlang no_value
// representation. Constructed candidates can carry an explicit seal-presence
// decision without changing the canonical header type or its encoding.
pub(super) fn header(value: &ClientHeader, seal_present: Option<bool>) -> Result<Value, Response> {
    let h = &value.header;
    let wire = h.encode().map_err(|_| Response::internal())?;
    let hash = h.hash().map_err(|_| Response::internal())?;
    let common = h.common();
    let mut body = json!({
        "hash":api::encode(match h {Header::Key(_)=>ApiType::KeyBlockHash,Header::Micro(_)=>ApiType::MicroBlockHash},hash.as_ref()),
        "height":common.height,
        "prev_hash":api::encode(match value.previous {Kind::Key=>ApiType::KeyBlockHash,Kind::Micro=>ApiType::MicroBlockHash},&common.previous_hash),
        "prev_key_hash":api::encode(ApiType::KeyBlockHash,&common.previous_key_hash),
        "state_hash":api::encode(ApiType::BlockStateHash,&common.state_hash),
        "version":common.version,
        "flags":api::encode(ApiType::Bytearray,&wire[4..8]),
    });
    match h {
        Header::Key(k) => {
            let info: &[u8] = k.info.as_ref().map_or(&[], |i| i);
            body["miner"] = json!(api::encode(ApiType::AccountPubkey, &k.miner));
            body["beneficiary"] = json!(api::encode(ApiType::AccountPubkey, &k.beneficiary));
            body["target"] = json!(k.target);
            body["time"] = json!(k.time);
            body["info"] = json!(api::encode(ApiType::ContractBytearray, info));
            if seal_present.unwrap_or_else(|| k.seal.iter().any(|n| *n != 0)) {
                body["pow"] = json!(k.seal.as_slice());
                body["nonce"] = json!(k.nonce);
            }
        }
        Header::Micro(m) => {
            // The pinned reference calls an unsupported pof_hash encoder type
            // for a nonempty proof. Preserve its public dispatcher failure;
            // generation hash lists do not invoke micro-header serialization.
            if m.proof_of_fraud.is_some() {
                return Err(Response::internal());
            }
            body["pof_hash"] = json!("no_fraud");
            body["signature"] = json!(api::encode(ApiType::Signature, &m.signature));
            body["txs_hash"] = json!(api::encode(ApiType::BlockTxHash, &m.transactions_hash));
            body["time"] = json!(m.time);
        }
    }
    Ok(body)
}
pub(super) fn body(operation: &Operation, reply: Reply) -> Result<Value, Response> {
    match reply {
        Reply::Header(h) => {
            let value = header(&h, None)?;
            if matches!(operation, Operation::Top { envelope: true }) {
                // The native height-zero branch always uses key_block.
                Ok(
                    if matches!(h.header, Header::Key(_)) || h.header.common().height == 0 {
                        json!({"key_block":value})
                    } else {
                        json!({"micro_block":value})
                    },
                )
            } else {
                Ok(value)
            }
        }
        Reply::Count(count) => Ok(json!({"count":count})),
        Reply::Generation { key, micros } => Ok(
            json!({"key_block":header(&key,None)?,"micro_blocks":micros.iter().map(|h|api::encode(ApiType::MicroBlockHash,h.as_ref())).collect::<Vec<_>>()}),
        ),
        Reply::Missing(missing) => {
            let (status, mut value) = match missing {
                Missing::NotOnChain => (400, json!({"reason":"Hash not on main chain"})),
                Missing::ChainTooShort if matches!(operation, Operation::GenerationHeight(_)) => {
                    (404, json!({"reason":"Chain too short"}))
                }
                _ => (404, json!({"reason":"Block not found"})),
            };
            let code = match missing {
                Missing::Key => Some("no_key_block"),
                Missing::Micro => Some("block_not_found"),
                Missing::ChainTooShort if matches!(operation, Operation::KeyHeight(_)) => {
                    Some("chain_too_short")
                }
                Missing::Block if matches!(operation, Operation::KeyHeight(_)) => {
                    Some("block_not_found")
                }
                _ => None,
            };
            if let Some(code) = code {
                value["error_code"] = json!(code);
            }
            Err(Response::json(status, value))
        }
    }
}
pub(super) async fn query(
    parent: &runtime::Handle,
    config: &Config,
    captured: Option<&Arc<chain::Importer>>,
    operation: &Operation,
    v3: bool,
) -> Result<Value, Response> {
    // Schema parameter handling runs before the stability/queue wrapper;
    // dispatcher hash decoding runs after it.
    operation.validate_path(v3)?;
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let importer = captured.ok_or_else(|| Response::unavailable(false))?;
    let permit = importer.block_query().map_err(super::transport::failure)?;
    let request = operation.request(v3)?;
    let read = async {
        let result = permit
            .submit(request)
            .map_err(super::transport::failure)?
            .wait()
            .await
            .map_err(super::transport::failure)?;
        if !parent.status().ready
            || parent
                .importer()
                .is_none_or(|live| !Arc::ptr_eq(&live, importer))
        {
            return Err(Response::unavailable(false));
        }
        body(operation, result)
    };
    tokio::time::timeout(config.timeout, read)
        .await
        .map_err(|_| Response::unavailable(false))?
}

#[cfg(test)]
pub(super) mod tests;
