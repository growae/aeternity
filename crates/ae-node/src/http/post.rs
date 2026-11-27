// aehttp_api_validate / aehttp_dispatch_ext, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{Config, response::Response};
use crate::runtime;
use ae_codec::api::{self, ApiType};
use ae_mempool::pool as policy;
use ae_storage::pool::{self, worker};
use ae_sync::blocking;
use ae_tx::{SignedTransaction, Transaction};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone)]
pub struct Limits {
    pub threads: usize,
    pub requests: usize,
    pub work_bytes: usize,
    pub updates: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            threads: 2,
            requests: 16,
            work_bytes: 512 << 20,
            updates: 16,
        }
    }
}
impl Limits {
    pub(super) fn valid(&self, body: usize) -> bool {
        (1..=32).contains(&self.threads)
            && (1..=4096).contains(&self.requests)
            && (1..=4096).contains(&self.updates)
            && self.work_bytes >= weight(body)
            && self.work_bytes <= (u32::MAX as usize).min(Semaphore::MAX_PERMITS)
    }
}
fn weight(body: usize) -> usize {
    // Includes raw/cleaned JSON, bounded value trees, RLP/state decoding,
    // canonical bytes and the pool's storage-record preflight. Retain this
    // reservation through socket output, including validation-error echoes.
    (128 << 20) + body * 24
}
#[derive(Clone)]
pub(super) struct Service {
    cpu: blocking::Pool,
    updates: Arc<Semaphore>,
}
impl Service {
    pub fn new(config: &Config) -> Result<Self, super::Error> {
        let limits = &config.submissions;
        Ok(Self {
            cpu: blocking::Pool::named(
                "ae-http-post",
                limits.threads,
                limits.requests,
                limits.work_bytes,
            )
            .map_err(|_| super::Error::Task)?,
            updates: Arc::new(Semaphore::new(limits.updates)),
        })
    }
    pub fn reserve(&self, config: &Config) -> Result<blocking::Reservation, Response> {
        self.cpu
            .reserve(weight(config.body_bytes))
            .map_err(cpu_error)
    }
    pub async fn finish(&self) {
        self.updates.close();
        self.cpu.stop();
        self.cpu.join().await;
    }
}
pub(super) enum Body {
    Complete(Vec<u8>),
    More(Vec<u8>),
}
fn validation(parameter: &str, info: Value) -> Response {
    Response::json(
        400,
        json!({"reason":"validation_error","parameter":parameter,"info":info}),
    )
}
pub(super) fn body_value(body: Body, v3: bool, maximum: usize) -> Result<Value, Response> {
    let body = match &body {
        Body::Complete(raw) => super::json::Body::Complete(raw),
        Body::More(raw) => super::json::Body::More(raw),
    };
    let value = super::json::parse(body, maximum).map_err(super::json::Error::response)?;
    let object = value
        .as_object()
        .ok_or_else(|| validation("body", json!({"error":"wrong_type","data":value,"path":[]})))?;
    let tx = object.get("tx").ok_or_else(|| {
        validation(
            "body",
            json!({"error":"missing_required_property","data":"tx","path":[]}),
        )
    })?;
    let text = tx.as_str().ok_or_else(|| {
        validation(
            "body",
            json!({"error":"wrong_type","data":tx,"path":["tx"]}),
        )
    })?;
    if v3 && !encoded_pattern(text) {
        return Err(validation(
            "body",
            json!({"error":"no_match","data":tx,"path":["tx"]}),
        ));
    }
    Ok(value)
}
fn encoded_pattern(text: &str) -> bool {
    // Jesse's anchored PCRE pattern permits one final LF after the match.
    let text = text.strip_suffix('\n').unwrap_or(text);
    text.strip_prefix("tx_").is_some_and(|s| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'='))
    })
}
fn invalid(code: &str) -> Response {
    Response::json(400, json!({"reason":"Invalid tx","error_code":code}))
}
fn decode(text: &str, maximum: usize) -> Result<Arc<policy::Signed>, Response> {
    if text
        .split_once('_')
        .and_then(|(prefix, _)| ApiType::from_prefix(prefix))
        .is_some_and(|kind| !kind.is_base64())
        && text.len() > 4096
    {
        // Base58 decoding has quadratic work. This is an operational bound,
        // including the variable-width nm_ type, not a prefix-validity claim.
        return Err(Response::unavailable(true));
    }
    let (kind, bytes) = api::decode_with_limit(text, maximum).map_err(|_| {
        Response::json(
            400,
            json!({"reason":"Invalid api encoding","error_code":"invalid_encoding"}),
        )
    })?;
    if kind != ApiType::Transaction {
        return Err(Response::json(
            400,
            json!({"reason":"Invalid api encoding","error_code":"invalid_prefix"}),
        ));
    }
    let rlp = ae_codec::rlp::DecodeLimits {
        max_input_bytes: maximum,
        max_items: 65536,
        max_depth: 64,
    };
    let limits = ae_tx::TransactionLimits {
        rlp,
        max_layers: 64,
        max_total_bytes: 32 << 20,
    };
    let state = ae_state::transport::Config {
        limits: ae_state::transport::Limits {
            max_input_bytes: maximum,
            max_items: 65536,
            max_depth: 64,
            max_entries: 65536,
            max_work_bytes: 32 << 20,
            ..Default::default()
        },
        ..Default::default()
    };
    SignedTransaction::<Transaction>::decode_with_state_config(&bytes, limits, state)
        .map(Arc::new)
        .map_err(|e| {
            if e.is_operational() {
                Response::unavailable(true)
            } else {
                invalid("broken_tx")
            }
        })
}
fn policy_error(error: policy::Error) -> Response {
    use policy::Error as E;
    if error.is_operational() {
        return Response::unavailable(true);
    }
    let code = match error {
        E::AlreadyKnown => "already_known",
        E::AlreadyAccepted => "already_accepted",
        E::NoOrigin => "no_origin",
        E::NonceTooLow => "nonce_too_low",
        E::NonceTooHigh => "nonce_too_high",
        E::GeneralizedAccount => "generalized_account_cant_sign_non_meta_tx",
        E::TooMuchAuthGas => "too_much_gas_for_auth_function",
        E::InvalidAtProtocol => "invalid_at_protocol",
        E::Signature(_) => "signature_check_failed",
        E::TooLowFee => "too_low_fee",
        E::TooLowGasPrice => "too_low_gas_price",
        E::TooLowMinerGasPrice => "too_low_gas_price_for_miner",
        E::NotPayable => "account_is_not_payable",
        E::UnresolvedRecipient => "could_not_resolve_recipient",
        E::TtlExpired => "ttl_expired",
        _ => return Response::internal(),
    };
    invalid(code)
}
fn pool_error(error: worker::Error) -> Response {
    match error {
        worker::Error::Service(pool::Failure::Policy(error)) => policy_error(error),
        worker::Error::Saturated | worker::Error::RequestTooLarge => Response::unavailable(true),
        worker::Error::Stopped => Response::unavailable(false),
        // Native gen_server call timeouts are exits and become HTTP 500.
        // Neither timeout nor reply loss proves the transaction was rejected.
        _ => Response::internal(),
    }
}
fn cpu_error(error: blocking::Error) -> Response {
    match error {
        blocking::Error::Busy => Response::unavailable(true),
        blocking::Error::Stopped | blocking::Error::Cancelled => Response::unavailable(false),
        _ => Response::internal(),
    }
}
struct Submitted {
    hash: ae_types::Hash256,
    ticket: worker::Ticket<pool::Submission>,
    _update: OwnedSemaphorePermit,
}
pub(super) async fn submit(
    service: &Service,
    reservation: blocking::Reservation,
    parent: runtime::Handle,
    config: &Config,
    body: Body,
    options: (bool, String),
) -> Result<blocking::Retained<Response>, Response> {
    let (v3, query) = options;
    let updates = service.updates.clone();
    let maximum = config.body_bytes;
    let pending = reservation
        .submit(move |cancel| {
            let value = body_value(body, v3, maximum)?;
            if v3 {
                super::transport::strings(&query)?;
            }
            if cancel.cancelled() || !parent.status().ready {
                return Err(Response::unavailable(false));
            }
            let binding = parent
                .worker_bindings()
                .and_then(|w| w.admission.current())
                .ok_or_else(|| Response::unavailable(false))?;
            let update = updates
                .try_acquire_owned()
                .map_err(|_| Response::unavailable(true))?;
            let tx = decode(value["tx"].as_str().expect("validated tx string"), maximum)?;
            let hash = tx.hash();
            if cancel.cancelled() {
                return Err(Response::unavailable(false));
            }
            // Exactly one push against this captured owner. No retry or generation
            // rebind is safe once the worker has accepted this mutation.
            let ticket = binding
                .client
                .push(tx, policy::Event::Created, false)
                .map_err(pool_error)?;
            Ok(Submitted {
                hash,
                ticket,
                _update: update,
            })
        })
        .map_err(cpu_error)?;
    let reply = tokio::time::timeout(config.timeout, pending.retained())
        .await
        .map_err(|_| Response::internal())?
        .map_err(cpu_error)?;
    let response = match reply.value {
        Err(response) => response,
        Ok(submitted) => {
            let Submitted {
                hash,
                ticket,
                _update,
            } = submitted;
            let response = match tokio::time::timeout(config.timeout, ticket).await {
                Ok(Ok(result)) => match result.outcome {
                    policy::PushOutcome::Accepted { hash: actual, .. } if actual == hash => {
                        Response::json(
                            200,
                            json!({"tx_hash":api::encode(ApiType::TxHash, hash.as_ref())}),
                        )
                    }
                    _ => Response::internal(),
                },
                Ok(Err(error)) => pool_error(error),
                Err(_) => Response::internal(),
            };
            drop(_update);
            response
        }
    };
    Ok(blocking::Retained {
        value: response,
        credits: reply.credits,
    })
}

#[cfg(test)]
mod tests;
