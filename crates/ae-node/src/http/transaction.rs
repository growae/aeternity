// aetx / aetx_sign and transaction for_client callbacks, aeternity fa2a6d7.
// Copyright (c) 2017-2026, Aeternity developers. ISC: repository LICENSE.
use ae_codec::api::{self, ApiType};
use ae_tx::{SignedTransaction, Transaction, TransactionKind, TransactionRef, Wrapper};
use ae_types::{Hash256, Id, Signature, Unsigned};
use num_bigint::BigUint;
use serde_json::{Map, Value};
use std::cell::Cell;

pub mod calls;
mod channels;
mod leaf;

// Operational rendering limits; these do not change transaction validity.
// JSON credit conservatively charges escaped text, keys and value overhead.
// It is not an exact allocator-byte measurement. Work credit also charges
// quadratic base58/integer conversions and repeated wrapper encoding.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub layers: usize,
    pub json_bytes: usize,
    pub work_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            layers: 64,
            json_bytes: 64 << 20,
            work_bytes: 256 << 20,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ResourceLimit,
    InvalidInfo,
    State(ae_state::transport::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, Copy)]
pub enum Location {
    Inner,
    Pending,
    Mined { height: u64, block: Hash256 },
}

struct Budget {
    json: Cell<usize>,
    work: Cell<usize>,
}
impl Budget {
    fn new(tx: TransactionRef<'_>, limits: Limits) -> Result<Self, Error> {
        let b = Self {
            json: Cell::new(limits.json_bytes),
            work: Cell::new(limits.work_bytes),
        };
        b.input(tx, limits)?;
        Ok(b)
    }
    fn input(&self, tx: TransactionRef<'_>, limits: Limits) -> Result<(), Error> {
        // serde_json Value serialization and destruction recurse. Bound that
        // depth even when callers request a larger local wrapper allowance.
        if tx.layers().len() > limits.layers.min(256) {
            return Err(Error::ResourceLimit);
        }
        self.work(tx.leaf().wire_size())?;
        for layer in tx.layers() {
            self.work(layer.wire_size())?;
        }
        Ok(())
    }
    fn take(credit: &Cell<usize>, n: usize) -> Result<(), Error> {
        credit.set(credit.get().checked_sub(n).ok_or(Error::ResourceLimit)?);
        Ok(())
    }
    fn work(&self, n: usize) -> Result<(), Error> {
        Self::take(&self.work, n)
    }
    fn charge(&self, n: usize) -> Result<(), Error> {
        Self::take(&self.json, n)
    }
    fn string(&self, raw: &[u8]) -> Result<Value, Error> {
        self.charge(raw.len().saturating_mul(6).saturating_add(64))?;
        self.work(raw.len())?;
        Ok(Value::String(super::text::binary(raw)))
    }
    fn number(&self, n: &Unsigned) -> Result<Value, Error> {
        self.charge(n.as_bytes().len().saturating_mul(3).saturating_add(64))?;
        self.work(n.as_bytes().len().saturating_mul(n.as_bytes().len()))?;
        let decimal = BigUint::from_bytes_be(n.as_bytes()).to_str_radix(10);
        Ok(serde_json::from_str(&decimal).expect("unsigned integer JSON"))
    }
    fn encoded(&self, kind: ApiType, bytes: &[u8]) -> Result<Value, Error> {
        self.charge(bytes.len().saturating_mul(2).saturating_add(128))?;
        self.work(if kind.is_base64() {
            bytes.len()
        } else {
            bytes.len().saturating_add(4).saturating_pow(2)
        })?;
        Ok(Value::String(api::encode(kind, bytes)))
    }
    fn id(&self, id: Id) -> Result<Value, Error> {
        self.charge(160)?;
        self.work(36 * 36)?;
        Ok(Value::String(api::encode_id(id)))
    }
    fn array<T>(
        &self,
        values: impl IntoIterator<Item = T>,
        mut f: impl FnMut(T) -> Result<Value, Error>,
    ) -> Result<Value, Error> {
        self.charge(64)?;
        let mut out = Vec::new();
        for value in values {
            out.push(f(value)?);
        }
        Ok(Value::Array(out))
    }
    fn signatures(&self, values: &[Signature]) -> Result<Value, Error> {
        self.array(values, |s| self.encoded(ApiType::Signature, s.as_ref()))
    }
}
struct Object<'a> {
    budget: &'a Budget,
    fields: Map<String, Value>,
}
impl<'a> Object<'a> {
    fn new(budget: &'a Budget) -> Result<Self, Error> {
        budget.charge(64)?;
        Ok(Self {
            budget,
            fields: Map::new(),
        })
    }
    fn insert(&mut self, key: &str, value: Value) -> Result<(), Error> {
        self.budget
            .charge(key.len().saturating_mul(6).saturating_add(64))?;
        self.fields.insert(key.into(), value);
        Ok(())
    }
    fn raw(&mut self, key: &str, bytes: &[u8]) -> Result<(), Error> {
        self.insert(key, self.budget.string(bytes)?)
    }
    fn text(&mut self, key: &str, text: &str) -> Result<(), Error> {
        self.raw(key, text.as_bytes())
    }
    fn number(&mut self, key: &str, n: &Unsigned) -> Result<(), Error> {
        self.insert(key, self.budget.number(n)?)
    }
    fn small(&mut self, key: &str, n: u64) -> Result<(), Error> {
        self.number(key, &n.into())
    }
    fn encoded(&mut self, key: &str, kind: ApiType, bytes: &[u8]) -> Result<(), Error> {
        self.insert(key, self.budget.encoded(kind, bytes)?)
    }
    fn id(&mut self, key: &str, id: Id) -> Result<(), Error> {
        self.insert(key, self.budget.id(id)?)
    }
    fn ttl(&mut self, n: &Unsigned) -> Result<(), Error> {
        if n != &0.into() {
            self.number("ttl", n)?;
        }
        Ok(())
    }
    fn finish(self) -> Value {
        Value::Object(self.fields)
    }
}

pub fn unsigned(tx: TransactionRef<'_>, limits: Limits) -> Result<Value, Error> {
    unsigned_with(tx, &Budget::new(tx, limits)?)
}
fn signed_inner(
    value: Value,
    kind: TransactionKind,
    signatures: &[Signature],
    budget: &Budget,
) -> Result<Value, Error> {
    let mut out = Object::new(budget)?;
    out.insert("tx", value)?;
    if kind != TransactionKind::GaMeta {
        out.insert("signatures", budget.signatures(signatures)?)?;
    }
    Ok(out.finish())
}
fn unsigned_with(tx: TransactionRef<'_>, budget: &Budget) -> Result<Value, Error> {
    let mut value = leaf::render(tx.leaf(), budget)?;
    let mut kind = tx.leaf().kind();
    // Build from the inside out without recursive transaction traversal.
    for layer in tx.layers() {
        let inner = signed_inner(value, kind, layer.inner_signatures(), budget)?;
        let mut out = Object::new(budget)?;
        match layer.wrapper() {
            Wrapper::GaMeta(m) => {
                out.text("type", "GAMetaTx")?;
                out.small("version", if m.ttl.is_some() { 1 } else { 2 })?;
                out.encoded("ga_id", ApiType::AccountPubkey, m.account.as_ref())?;
                out.encoded("auth_data", ApiType::ContractBytearray, &m.auth_data)?;
                out.number("abi_version", &m.abi_version)?;
                out.number("fee", &m.fee)?;
                out.number("gas", &m.gas)?;
                out.number("gas_price", &m.gas_price)?;
                if let Some(ttl) = &m.ttl {
                    out.ttl(ttl)?;
                }
            }
            Wrapper::PayingFor(p) => {
                out.text("type", "PayingForTx")?;
                out.small("version", 1)?;
                out.encoded("payer_id", ApiType::AccountPubkey, p.payer.as_ref())?;
                out.number("nonce", &p.nonce)?;
                out.number("fee", &p.fee)?;
            }
        }
        out.insert("tx", inner)?;
        kind = layer.wrapper().kind();
        value = out.finish();
    }
    Ok(value)
}
pub fn signed(
    tx: &SignedTransaction<Transaction>,
    location: Location,
    limits: Limits,
) -> Result<Value, Error> {
    signed_with(tx, location, &Budget::new(tx.transaction().view(), limits)?)
}
pub(super) fn list<T: std::borrow::Borrow<SignedTransaction<Transaction>>>(
    txs: &[T],
    location: Location,
    limits: Limits,
) -> Result<Value, Error> {
    let budget = Budget {
        json: Cell::new(limits.json_bytes),
        work: Cell::new(limits.work_bytes),
    };
    let mut out = Object::new(&budget)?;
    out.insert(
        "transactions",
        budget.array(txs, |tx| {
            let tx = tx.borrow();
            budget.input(tx.transaction().view(), limits)?;
            signed_with(tx, location, &budget)
        })?,
    )?;
    Ok(out.finish())
}
fn signed_with(
    tx: &SignedTransaction<Transaction>,
    location: Location,
    budget: &Budget,
) -> Result<Value, Error> {
    let view = tx.transaction().view();
    let value = unsigned_with(view, budget)?;
    let Value::Object(fields) = signed_inner(value, view.kind(), tx.signatures(), budget)? else {
        unreachable!()
    };
    if matches!(location, Location::Inner) {
        return Ok(Value::Object(fields));
    }
    budget.work(tx.signatures().len().saturating_mul(80))?;
    let bytes = tx.encode();
    budget.work(bytes.len())?;
    let mut out = Object { budget, fields };
    match location {
        Location::Pending => {
            budget.charge(64)?;
            out.insert("block_height", Value::from(-1))?;
            out.text("block_hash", "none")?;
        }
        Location::Mined { height, block } => {
            out.small("block_height", height)?;
            out.encoded("block_hash", ApiType::MicroBlockHash, block.as_ref())?;
        }
        Location::Inner => unreachable!(),
    }
    out.encoded(
        "hash",
        ApiType::TxHash,
        ae_crypto::blake2b_256(&bytes).as_ref(),
    )?;
    out.encoded("encoded_tx", ApiType::Transaction, &bytes)?;
    Ok(out.finish())
}

#[cfg(test)]
mod tests;
