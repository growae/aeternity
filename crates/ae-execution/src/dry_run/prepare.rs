// aec_dry_run:prepare_txs/prepare_call_req, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::{MAGIC_ACCOUNT, Prepared};
use ae_codec::api::{self, ApiType};
use ae_tx::{
    CanonicalTransaction, ContractOperation, ContractTransaction, SignedTransaction, Transaction,
    TransactionLeaf, TransactionLimits, WireTransaction,
};
use ae_types::{Hash256, Id, IdTag, Signature, Unsigned};
use serde_json::{Map, Value};
use std::sync::Arc;

// This boundary receives already parsed JSON-shaped call maps or already
// decoded native transactions. HTTP schema/type policy and mempool lookup
// precede it; no chain state is read and nothing is executed during preparation.
pub enum Input<'a> {
    Transaction(Box<Transaction>),
    CallRequest(&'a Value),
}
impl Input<'_> {
    pub fn transaction(transaction: Transaction) -> Self {
        Self::Transaction(Box::new(transaction))
    }
}
#[derive(Debug)]
pub struct Owned {
    pub transaction: SignedTransaction<Transaction>,
    pub stateless: bool,
    pub auth_tx: Option<Arc<WireTransaction>>,
    pub auth_tx_hash: Option<Hash256>,
}
impl Owned {
    pub fn as_prepared(&self) -> Prepared<'_> {
        Prepared {
            transaction: &self.transaction,
            stateless: self.stateless,
            auth_tx: self.auth_tx.clone(),
            auth_tx_hash: self.auth_tx_hash,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub requests: usize,
    pub encoded_bytes: usize,
    pub number_chars: usize,
    // Cumulative conservative payload charge, not an allocator/RSS bound.
    pub work_bytes: usize,
    pub transaction: TransactionLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            requests: 1024,
            encoded_bytes: 1024 * 1024,
            number_chars: 4096,
            work_bytes: 32 * 1024 * 1024,
            transaction: TransactionLimits {
                rlp: ae_codec::rlp::DecodeLimits {
                    max_input_bytes: 1024 * 1024,
                    max_items: 65536,
                    max_depth: 64,
                },
                max_layers: 64,
                max_total_bytes: 8 * 1024 * 1024,
            },
        }
    }
}
#[derive(Debug)]
pub enum Error {
    BadCallRequest,
    ResourceLimit,
    Operational(ae_tx::Error),
}
#[derive(Debug)]
pub struct Failure {
    pub index: Option<usize>,
    pub error: Error,
}
struct Budget {
    remaining: usize,
    limits: Limits,
}
impl Budget {
    fn charge(&mut self, n: usize) -> Result<(), Error> {
        self.remaining = self.remaining.checked_sub(n).ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    fn payload(&mut self, n: usize) -> Result<(), Error> {
        self.charge(
            n.checked_mul(16)
                .and_then(|n| n.checked_add(512))
                .ok_or(Error::ResourceLimit)?,
        )
    }
    fn encoded(&mut self, value: &Value, expected: ApiType) -> Result<Vec<u8>, Error> {
        let text = value.as_str().ok_or(Error::BadCallRequest)?;
        if text.len() > self.limits.encoded_bytes {
            return Err(Error::ResourceLimit);
        }
        self.payload(text.len())?;
        let (kind, bytes) =
            api::decode_with_limit(text, self.limits.encoded_bytes).map_err(|e| match e {
                api::ApiError::ResourceLimit => Error::ResourceLimit,
                _ => Error::BadCallRequest,
            })?;
        if kind != expected {
            return Err(Error::BadCallRequest);
        }
        Ok(bytes)
    }
    fn unsigned(
        &mut self,
        map: &Map<String, Value>,
        field: &str,
        default: u64,
    ) -> Result<Unsigned, Error> {
        let Some(value) = map.get(field) else {
            return Ok(default.into());
        };
        let text = value.as_number().ok_or(Error::BadCallRequest)?.as_str();
        if text.len() > self.limits.number_chars {
            return Err(Error::ResourceLimit);
        }
        self.payload(text.len())?;
        // Integer negative zero is zero after JSX parsing; floating-point
        // negative zero still fails the integer-only constructor below.
        let text = if text == "-0" { "0" } else { text };
        if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
            return Err(Error::BadCallRequest);
        }
        Unsigned::from_decimal(text).map_err(|_| Error::BadCallRequest)
    }
}
fn required<'a>(map: &'a Map<String, Value>, key: &str) -> Result<&'a Value, Error> {
    map.get(key).ok_or(Error::BadCallRequest)
}
fn decode_error(e: ae_tx::Error) -> Error {
    if e.is_operational() {
        Error::Operational(e)
    } else {
        Error::BadCallRequest
    }
}
fn dummy(transaction: Transaction) -> Owned {
    Owned {
        transaction: SignedTransaction::new(transaction, vec![Signature([0; 64])]),
        stateless: false,
        auth_tx: None,
        auth_tx_hash: None,
    }
}
fn call(
    value: &Value,
    budget: &mut Budget,
    state: ae_state::transport::Config<'_>,
) -> Result<Owned, Error> {
    let map = value.as_object().ok_or(Error::BadCallRequest)?;
    let calldata = budget.encoded(required(map, "calldata")?, ApiType::ContractBytearray)?;
    let contract = budget.encoded(required(map, "contract")?, ApiType::ContractPubkey)?;
    let amount = budget.unsigned(map, "amount", 0)?;
    let caller = match map.get("caller") {
        Some(v) => budget
            .encoded(v, ApiType::AccountPubkey)?
            .try_into()
            .expect("API account width"),
        None => MAGIC_ACCOUNT.0,
    };
    let gas = budget.unsigned(map, "gas", 1_000_000)?;
    let abi_version = budget.unsigned(map, "abi_version", 1)?;
    let nonce = budget.unsigned(map, "nonce", 1)?;
    let mut tx = ContractTransaction::new(
        Id {
            tag: IdTag::Account,
            value: caller,
        },
        ContractOperation::Call {
            contract: Id {
                tag: IdTag::Contract,
                value: contract.try_into().expect("API contract width"),
            },
            abi_version,
            call_data: calldata,
        },
    )
    .map_err(decode_error)?;
    tx.set_amount(amount);
    tx.set_gas(gas);
    tx.set_nonce(nonce);
    tx.set_fee(1_000_000_000_000u64.into());
    tx.set_gas_price(1_000_000.into());
    let mut result = dummy(Transaction::new(TransactionLeaf::Contract(tx)));
    let context = match map.get("context") {
        None => None,
        Some(v) => Some(v.as_object().ok_or(Error::BadCallRequest)?),
    };
    if let Some(value) = context.and_then(|c| c.get("tx")) {
        let bytes = budget.encoded(value, ApiType::Transaction)?;
        let limits = budget.limits.transaction;
        let mut state = state;
        state.limits.max_input_bytes = state.limits.max_input_bytes.min(limits.rlp.max_input_bytes);
        state.limits.max_items = state.limits.max_items.min(limits.rlp.max_items);
        state.limits.max_depth = state.limits.max_depth.min(limits.rlp.max_depth);
        state.limits.max_work_bytes = state.limits.max_work_bytes.min(limits.max_total_bytes);
        // Native aetx deserialization validates wrapper/leaf transports, not
        // only the outer wire schema. Retain the full native transaction for
        // authentication opcodes after that validation succeeds.
        let transaction =
            Transaction::decode_with_state_config(&bytes, limits, state).map_err(decode_error)?;
        result.auth_tx = Some(Arc::new(
            WireTransaction::decode_with_limits(&transaction.encode(), limits.rlp)
                .map_err(decode_error)?,
        ));
    }
    if let Some(value) = context.and_then(|c| c.get("tx_hash")) {
        result.auth_tx_hash = Some(Hash256(
            budget
                .encoded(value, ApiType::TxHash)?
                .try_into()
                .expect("API hash width"),
        ));
    }
    result.stateless = context.and_then(|c| c.get("stateful")) != Some(&Value::Bool(true));
    Ok(result)
}

// All requests are prepared before a caller can obtain any executable input.
// A failure discards the complete prefix, matching native prepare_txs ordering.
pub fn all(
    inputs: Vec<Input<'_>>,
    limits: Limits,
    state: ae_state::transport::Config<'_>,
) -> Result<Vec<Owned>, Failure> {
    if inputs.len() > limits.requests {
        return Err(Failure {
            index: None,
            error: Error::ResourceLimit,
        });
    }
    let mut budget = Budget {
        remaining: limits.work_bytes,
        limits,
    };
    budget
        .charge(
            inputs
                .len()
                .checked_mul(std::mem::size_of::<Owned>())
                .ok_or(Failure {
                    index: None,
                    error: Error::ResourceLimit,
                })?,
        )
        .map_err(|error| Failure { index: None, error })?;
    let mut prepared = Vec::with_capacity(inputs.len());
    for (index, input) in inputs.into_iter().enumerate() {
        let result = match input {
            Input::Transaction(tx) => budget.payload(tx.view().wire_size()).map(|()| dummy(*tx)),
            Input::CallRequest(value) => call(value, &mut budget, state),
        }
        .map_err(|error| Failure {
            index: Some(index),
            error,
        })?;
        prepared.push(result);
    }
    Ok(prepared)
}
