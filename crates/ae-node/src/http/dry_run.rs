// aehttp_dispatch_ext/int and aehttp_helpers dry-run preparation, fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
pub mod schema;
use ae_codec::api::{self, ApiType};
use ae_execution::dry_run::prepare::Input;
use ae_protocol::Protocol;
use ae_tx::{SignedTransaction, Transaction, TransactionKind, TransactionLimits};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use ae_vm_fate::{
    maps::{MapOrder, Traversal},
    value::Value as Fate,
};
use num_bigint::BigInt;
use num_traits::{FromPrimitive, ToPrimitive};
use serde_json::{Map, Value};
use std::{collections::BTreeSet, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Public,
    Internal,
}
pub enum Location {
    Mempool(Arc<SignedTransaction<Transaction>>),
    Other,
}
#[derive(Debug)]
pub enum LookupError<E> {
    Native,
    Operational(E),
}
// The runtime supplies a captured source. A native lookup exception is distinct
// from local storage/cancellation failures, which must never become bad input.
pub trait Source {
    type Error;
    fn top_height(&mut self) -> Result<Option<u64>, Self::Error>;
    fn protocol(&mut self, height: u64) -> Result<Protocol, Self::Error>;
    fn top_hash(&mut self) -> Result<Option<Hash256>, Self::Error>;
    fn transaction(&mut self, hash: Hash256) -> Result<Location, LookupError<Self::Error>>;
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub requests: usize,
    pub accounts: usize,
    pub encoded_bytes: usize,
    pub base58_bytes: usize,
    pub number_chars: usize,
    pub key_bytes: usize,
    pub map_entries: usize,
    pub map_depth: usize,
    pub work_bytes: usize,
    pub transaction: TransactionLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            requests: 1024,
            accounts: 1024,
            encoded_bytes: 1024 * 1024,
            base58_bytes: 4096,
            number_chars: 4096,
            key_bytes: 256,
            map_entries: 4096,
            map_depth: 64,
            work_bytes: 64 * 1024 * 1024,
            transaction: ae_execution::dry_run::prepare::Limits::default().transaction,
        }
    }
}
pub struct Config<'a, O> {
    pub profile: Profile,
    pub gas_limit: &'a BigInt,
    // parse_map_to_atom_keys uses the original runtime's existing atom set.
    // Request strings never add atoms to this explicit compatibility profile.
    pub existing_keys: &'a BTreeSet<String>,
    pub order: &'a O,
    pub state: ae_state::transport::Config<'a>,
    pub limits: Limits,
}
#[derive(Debug)]
pub enum Error<E> {
    Rejected { code: u16, reason: String },
    Capacity,
    Source(E),
    Decode(ae_tx::Error),
    MapOrder(ae_vm_fate::Error),
    InvalidOrder,
    MissingAtomProfile,
}
#[derive(Debug)]
pub struct Account {
    pub public_key: AccountPublicKey,
    // Native HTTP preparation accepts signed amounts. Funding happens only
    // after selecting state; do not reject a negative amount at this boundary.
    pub amount: BigInt,
}
pub struct Request<'a> {
    pub top: Hash256,
    pub accounts: Vec<Account>,
    // Call maps remain borrowed and unprepared until state lookup/funding has
    // succeeded, preserving errors before a malformed call request is examined.
    pub inputs: Vec<Input<'a>>,
    pub tx_events: bool,
    pub profile: Profile,
}
fn reject<E>(code: u16, reason: impl Into<String>) -> Error<E> {
    Error::Rejected {
        code,
        reason: reason.into(),
    }
}
fn bad<E>(reason: &str) -> Error<E> {
    reject(400, format!("Bad request: {reason}"))
}
fn internal<E>() -> Error<E> {
    reject(500, "Internal server error")
}
struct Budget {
    remaining: usize,
    limits: Limits,
}
impl Budget {
    fn charge<E>(&mut self, n: usize) -> Result<(), Error<E>> {
        self.remaining = self.remaining.checked_sub(n).ok_or(Error::Capacity)?;
        Ok(())
    }
    fn payload<E>(&mut self, n: usize) -> Result<(), Error<E>> {
        self.charge(
            n.checked_mul(24)
                .and_then(|n| n.checked_add(512))
                .ok_or(Error::Capacity)?,
        )
    }
    fn integer<E>(&mut self, text: &str) -> Result<BigInt, Error<E>> {
        if text.len() > self.limits.number_chars {
            return Err(Error::Capacity);
        }
        self.payload(text.len())?;
        let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(internal());
        }
        text.parse().map_err(|_| internal())
    }
    fn decode<E>(&mut self, value: &Value) -> Result<(ApiType, Vec<u8>), Error<E>> {
        let text = value.as_str().ok_or_else(|| bad("invalid_encoding"))?;
        if text.len() > self.limits.encoded_bytes {
            return Err(Error::Capacity);
        }
        if text
            .split_once('_')
            .and_then(|(p, _)| ApiType::from_prefix(p))
            .is_some_and(|p| !p.is_base64())
            && text.len() > self.limits.base58_bytes
        {
            return Err(Error::Capacity);
        }
        self.payload(text.len())?;
        api::decode_with_limit(text, self.limits.encoded_bytes).map_err(|e| match e {
            api::ApiError::ResourceLimit => Error::Capacity,
            _ => bad("invalid_encoding"),
        })
    }
    fn as_type<E>(&mut self, value: &Value, expected: ApiType) -> Result<Vec<u8>, Error<E>> {
        let (kind, bytes) = self.decode(value)?;
        if kind != expected {
            return Err(bad("invalid_prefix"));
        }
        Ok(bytes)
    }
}
fn keys<E, O: MapOrder>(
    map: &Map<String, Value>,
    config: &Config<'_, O>,
    budget: &mut Budget,
    depth: usize,
) -> Result<(), Error<E>> {
    if depth > config.limits.map_depth || map.len() > config.limits.map_entries {
        return Err(Error::Capacity);
    }
    for key in map.keys() {
        if key.len() > config.limits.key_bytes {
            return Err(Error::Capacity);
        }
        budget.payload(key.len())?;
    }
    let entries: Vec<_> = map.iter().collect();
    let values: Vec<_> = entries
        .iter()
        .map(|(k, _)| Fate::String(k.as_bytes().to_vec()))
        .collect();
    let refs: Vec<_> = values.iter().collect();
    let order = config
        .order
        .indices(&refs, Traversal::ToList, ae_vm_fate::Limits::default())
        .map_err(Error::MapOrder)?;
    if order.len() != entries.len() {
        return Err(Error::InvalidOrder);
    }
    let mut seen = vec![false; entries.len()];
    for i in order {
        let mark = seen.get_mut(i).ok_or(Error::InvalidOrder)?;
        if *mark {
            return Err(Error::InvalidOrder);
        }
        *mark = true;
        let (key, value) = entries[i];
        if !config.existing_keys.contains(key) {
            return Err(reject(400, format!("Invalid parameter: {key}")));
        }
        // Erlang recurses through map values, but never into an array of maps.
        if let Some(child) = value.as_object() {
            keys(child, config, budget, depth + 1)?;
        }
    }
    Ok(())
}
fn raw<E, O>(
    value: &Value,
    config: &Config<'_, O>,
    budget: &mut Budget,
) -> Result<Transaction, Error<E>> {
    let bytes = budget.as_type(value, ApiType::Transaction)?;
    let limits = config.limits.transaction;
    let mut state = config.state;
    state.limits.max_input_bytes = state.limits.max_input_bytes.min(limits.rlp.max_input_bytes);
    state.limits.max_items = state.limits.max_items.min(limits.rlp.max_items);
    state.limits.max_depth = state.limits.max_depth.min(limits.rlp.max_depth);
    state.limits.max_work_bytes = state.limits.max_work_bytes.min(limits.max_total_bytes);
    Transaction::decode_with_state_config(&bytes, limits, state).map_err(|e| {
        if e.is_operational() {
            Error::Decode(e)
        } else {
            bad("malformed transaction")
        }
    })
}
fn pool<S: Source>(
    value: &Value,
    source: &mut S,
    budget: &mut Budget,
) -> Result<Arc<SignedTransaction<Transaction>>, Error<S::Error>> {
    let hash = Hash256(
        budget
            .as_type(value, ApiType::TxHash)?
            .try_into()
            .expect("API hash width"),
    );
    match source.transaction(hash) {
        Ok(Location::Mempool(tx)) => {
            if tx.transaction().layers().len() > budget.limits.transaction.max_layers
                || tx.transaction().view().wire_size()
                    > budget.limits.transaction.rlp.max_input_bytes
            {
                return Err(Error::Capacity);
            }
            budget.payload(tx.transaction().view().wire_size())?;
            Ok(tx)
        }
        Ok(Location::Other) => Err(bad("Transaction not found in mempool")),
        Err(LookupError::Native) => Err(bad("malformed transaction hash")),
        Err(LookupError::Operational(e)) => Err(Error::Source(e)),
    }
}
enum Gas {
    Integer(BigInt),
    Float(f64),
}
impl Gas {
    fn add<E>(self, other: Self) -> Result<Self, Error<E>> {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => Ok(Self::Integer(a + b)),
            (a, b) => {
                let to_float = |n: Self| match n {
                    Self::Integer(n) => n.to_f64(),
                    Self::Float(n) => Some(n),
                };
                let value = to_float(a).ok_or_else(internal)? + to_float(b).ok_or_else(internal)?;
                if !value.is_finite() {
                    return Err(internal());
                }
                Ok(Self::Float(value))
            }
        }
    }
    fn over(&self, limit: &BigInt) -> bool {
        match self {
            Self::Integer(n) => n > limit,
            Self::Float(n) => {
                let floor = BigInt::from_f64(n.floor()).expect("finite sum");
                floor > *limit || (floor == *limit && n.fract() != 0.0)
            }
        }
    }
}
fn preflight<S: Source, O>(
    txs: &Value,
    source: &mut S,
    config: &Config<'_, O>,
    budget: &mut Budget,
) -> Result<(), Error<S::Error>> {
    let height = source
        .top_height()
        .map_err(Error::Source)?
        .ok_or_else(internal)?;
    let protocol = source.protocol(height).map_err(Error::Source)?;
    let txs = txs.as_array().ok_or_else(internal)?;
    if txs.len() > config.limits.requests {
        return Err(Error::Capacity);
    }
    if config.gas_limit.bits() > config.limits.number_chars.saturating_mul(4) as u64 {
        return Err(Error::Capacity);
    }
    let mut sum = Gas::Integer(0.into());
    for tx in txs {
        let tx = tx.as_object().ok_or_else(internal)?;
        let gas = if let Some(value) = tx.get("tx") {
            // This order intentionally differs from helper preparation below.
            match raw(value, config, budget) {
                Ok(tx) => Gas::Integer(tx.view().gas_limit(&Unsigned::from(height), protocol)),
                Err(Error::Rejected { .. }) => Gas::Integer(0.into()),
                Err(e) => return Err(e),
            }
        } else if let Some(value) = tx.get("tx_hash") {
            match pool(value, source, budget) {
                Ok(tx) => Gas::Integer(
                    tx.transaction()
                        .view()
                        .gas_limit(&Unsigned::from(height), protocol),
                ),
                Err(Error::Rejected { .. }) => Gas::Integer(0.into()),
                Err(e) => return Err(e),
            }
        } else if let Some(value) = tx.get("call_req") {
            let call = value.as_object().ok_or_else(internal)?;
            match call.get("gas") {
                None => Gas::Integer(1_000_000.into()),
                Some(value) => {
                    let number = value.as_number().ok_or_else(internal)?;
                    let text = number.as_str();
                    if text.len() > config.limits.number_chars {
                        return Err(Error::Capacity);
                    }
                    if text.contains(['.', 'e', 'E']) {
                        Gas::Float(
                            number
                                .as_f64()
                                .filter(|n| n.is_finite())
                                .ok_or_else(internal)?,
                        )
                    } else {
                        Gas::Integer(budget.integer(text)?)
                    }
                }
            }
        } else {
            return Err(internal());
        };
        sum = sum.add(gas)?;
    }
    if sum.over(config.gas_limit) {
        Err(reject(403, "Over the gas limit"))
    } else {
        Ok(())
    }
}
fn account_amount<E>(value: &Value, budget: &mut Budget) -> Result<BigInt, Error<E>> {
    match value {
        Value::Number(number) => budget.integer(number.as_str()),
        Value::String(text) => budget.integer(text),
        Value::Array(chars) => {
            if chars.len() > budget.limits.number_chars {
                return Err(Error::Capacity);
            }
            budget.payload(chars.len())?;
            let bytes: Vec<_> = chars
                .iter()
                .map(|v| {
                    v.as_u64()
                        .and_then(|n| u8::try_from(n).ok())
                        .ok_or_else(internal)
                })
                .collect::<Result<_, Error<E>>>()?;
            budget.integer(std::str::from_utf8(&bytes).map_err(|_| internal())?)
        }
        _ => Err(internal()),
    }
}
// Produces the arguments passed by the original helper to aec_dry_run. It does
// not select state, fund accounts, prepare call maps, execute, or render results.
pub fn prepare<'a, S: Source, O: MapOrder>(
    body: &'a Value,
    source: &mut S,
    config: &Config<'_, O>,
) -> Result<Request<'a>, Error<S::Error>> {
    for key in ["txs", "top", "accounts", "tx_events"] {
        if !config.existing_keys.contains(key) {
            return Err(Error::MissingAtomProfile);
        }
    }
    let mut budget = Budget {
        remaining: config.limits.work_bytes,
        limits: config.limits,
    };
    let map = body.as_object().ok_or_else(internal)?;
    keys(map, config, &mut budget, 0)?;
    let txs = map.get("txs").ok_or_else(internal)?;
    if config.profile == Profile::Public {
        preflight(txs, source, config, &mut budget)?;
    }
    let top = match map.get("top") {
        None => source
            .top_hash()
            .map_err(Error::Source)?
            .ok_or_else(|| bad("No top block hash"))?,
        Some(value) if value.is_string() => {
            let (kind, bytes) = budget.decode(value).map_err(|e| match e {
                Error::Rejected { .. } => internal(),
                e => e,
            })?;
            if !matches!(kind, ApiType::KeyBlockHash | ApiType::MicroBlockHash) {
                return Err(bad("top should be micro block hash or key block hash"));
            }
            Hash256(bytes.try_into().expect("API block hash width"))
        }
        Some(_) => return Err(bad("Bad parameter top")),
    };
    let txs = txs.as_array().ok_or_else(internal)?;
    if txs.len() > config.limits.requests {
        return Err(Error::Capacity);
    }
    budget.payload(
        txs.len()
            .checked_mul(std::mem::size_of::<Input<'_>>())
            .ok_or(Error::Capacity)?,
    )?;
    let mut inputs = Vec::with_capacity(txs.len());
    for item in txs {
        let item = item.as_object().ok_or_else(internal)?;
        if let Some(value) = item.get("tx_hash") {
            let tx = pool(value, source, &mut budget)?;
            inputs.push(Input::transaction(tx.transaction().clone()));
        } else if let Some(value) = item.get("tx") {
            let tx = raw(value, config, &mut budget)?;
            match tx.view().kind() {
                TransactionKind::PayingFor => {
                    return Err(bad("Unsupported transaction type paying_for_tx"));
                }
                TransactionKind::GaMeta => {
                    return Err(bad("Unsupported transaction type ga_meta_tx"));
                }
                _ => inputs.push(Input::transaction(tx)),
            }
        } else if let Some(value) = item.get("call_req") {
            inputs.push(Input::CallRequest(value));
        } else {
            return Err(internal());
        }
    }
    let mut accounts = vec![];
    if let Some(value) = map.get("accounts") {
        let values = value.as_array().ok_or_else(internal)?;
        if values.len() > config.limits.accounts {
            return Err(Error::Capacity);
        }
        budget.payload(
            values
                .len()
                .checked_mul(std::mem::size_of::<Account>())
                .ok_or(Error::Capacity)?,
        )?;
        accounts.reserve(values.len());
        for value in values {
            let account = value.as_object().ok_or_else(|| bad("Bad account"))?;
            let (Some(key), Some(amount)) = (account.get("pub_key"), account.get("amount")) else {
                return Err(bad("Bad account"));
            };
            let public_key = AccountPublicKey(
                budget
                    .as_type(key, ApiType::AccountPubkey)?
                    .try_into()
                    .expect("API account width"),
            );
            accounts.push(Account {
                public_key,
                amount: account_amount(amount, &mut budget)?,
            });
        }
    }
    let tx_events = match map.get("tx_events") {
        None => false,
        Some(Value::Bool(b)) => *b,
        _ => return Err(bad("Bad parameter, tx_events")),
    };
    Ok(Request {
        top,
        accounts,
        inputs,
        tx_events,
        profile: config.profile,
    })
}
