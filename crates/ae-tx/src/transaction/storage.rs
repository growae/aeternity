// Rust-owned database records, distinct from consensus transaction transport.
// Original aec_db stores the complete signed_tx/aetx records, including sizes
// and unsorted signatures; serializing only canonical wire bytes loses them.
use super::{Transaction, TransactionLeaf};
use crate::{ContractOperation, SignedTransaction};
use ae_trie::Value;
use ae_types::{Signature, Unsigned};

pub const MAGIC: &[u8] = b"\0aetx\x01";
#[derive(Debug, Clone, Copy)]
pub struct Config<'a> {
    pub transaction: super::TransactionLimits,
    pub state: ae_state::transport::Config<'a>,
    pub max_record_bytes: usize,
}
impl Default for Config<'_> {
    fn default() -> Self {
        Self {
            transaction: Default::default(),
            state: Default::default(),
            max_record_bytes: 32 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Version,
    Malformed,
    ResourceLimit,
    Transaction(crate::Error),
}
impl Error {
    pub fn is_operational(&self) -> bool {
        matches!(self, Self::ResourceLimit)
            || matches!(self, Self::Transaction(e) if e.is_operational())
    }
}
fn bytes(b: impl AsRef<[u8]>) -> Value {
    Value::Bytes(b.as_ref().to_vec())
}
fn list(a: Vec<Value>) -> Value {
    Value::List(a)
}
fn number(n: usize) -> Result<Value, Error> {
    Ok(bytes(
        Unsigned::from(u64::try_from(n).map_err(|_| Error::ResourceLimit)?).as_bytes(),
    ))
}
fn signatures(s: &[Signature]) -> Value {
    list(s.iter().map(|s| bytes(s.0)).collect())
}
fn blob(v: &Value) -> Result<&[u8], Error> {
    if let Value::Bytes(b) = v {
        Ok(b)
    } else {
        Err(Error::Malformed)
    }
}
fn array(v: &Value) -> Result<&[Value], Error> {
    if let Value::List(a) = v {
        Ok(a)
    } else {
        Err(Error::Malformed)
    }
}
fn fields(v: &Value, count: usize) -> Result<&[Value], Error> {
    let a = array(v)?;
    if a.len() == count {
        Ok(a)
    } else {
        Err(Error::Malformed)
    }
}
fn integer(v: &Value) -> Result<Unsigned, Error> {
    Unsigned::from_field_bytes(blob(v)?).map_err(|_| Error::Malformed)
}
fn size(v: &Value) -> Result<usize, Error> {
    usize::try_from(integer(v)?.to_u64().map_err(|_| Error::ResourceLimit)?)
        .map_err(|_| Error::ResourceLimit)
}
fn restore_signatures(v: &Value, canonical: &[Signature]) -> Result<Vec<Signature>, Error> {
    let original = array(v)?
        .iter()
        .map(|v| {
            Ok(Signature(
                blob(v)?.try_into().map_err(|_| Error::Malformed)?,
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let mut sorted = original.clone();
    sorted.sort();
    if sorted != canonical {
        return Err(Error::Malformed);
    }
    Ok(original)
}
fn check_sizes(t: &Transaction, config: Config<'_>) -> Result<(), Error> {
    if t.layers.len() > config.transaction.max_layers {
        return Err(Error::ResourceLimit);
    }
    let mut total = 0usize;
    for n in std::iter::once(t.leaf.wire_size()).chain(t.layers.iter().map(|l| l.wire_size)) {
        if n > config.transaction.rlp.max_input_bytes {
            return Err(Error::ResourceLimit);
        }
        total = total.checked_add(n).ok_or(Error::ResourceLimit)?;
    }
    if total > config.transaction.max_total_bytes {
        return Err(Error::ResourceLimit);
    }
    Ok(())
}
fn set_size(t: &mut TransactionLeaf, n: usize) {
    match t {
        TransactionLeaf::Spend(t) => t.wire_size = n,
        TransactionLeaf::Name(t) => t.wire_size = n,
        TransactionLeaf::Oracle(t) => t.wire_size = n,
        TransactionLeaf::Contract(t) => t.wire_size = n,
        TransactionLeaf::GaAttach(t) => t.wire_size = n,
        TransactionLeaf::Channel(t) => t.wire_size = n,
        TransactionLeaf::HcVote(t) => t.wire_size = n,
    }
}
pub fn encode(
    signed: &SignedTransaction<Transaction>,
    config: Config<'_>,
) -> Result<Vec<u8>, Error> {
    let tx = signed.transaction();
    check_sizes(tx, config)?;
    let context = match &tx.leaf {
        TransactionLeaf::Contract(t) if matches!(t.operation(), ContractOperation::Call { .. }) => {
            list(vec![
                bytes(t.call_origin()),
                list(t.call_stack().iter().map(|n| bytes(n.as_bytes())).collect()),
            ])
        }
        _ => list(vec![]),
    };
    let layers = tx
        .layers
        .iter()
        .map(|l| Ok(list(vec![number(l.wire_size)?, signatures(&l.signatures)])))
        .collect::<Result<_, Error>>()?;
    let payload = list(vec![
        bytes(signed.encode()),
        number(tx.leaf.wire_size())?,
        list(layers),
        signatures(signed.signatures()),
        context,
    ])
    .encode();
    if payload
        .len()
        .checked_add(MAGIC.len())
        .ok_or(Error::ResourceLimit)?
        > config.max_record_bytes
    {
        return Err(Error::ResourceLimit);
    }
    let mut out = MAGIC.to_vec();
    out.extend(payload);
    Ok(out)
}
pub fn decode(encoded: &[u8], config: Config<'_>) -> Result<SignedTransaction<Transaction>, Error> {
    if encoded.len() > config.max_record_bytes {
        return Err(Error::ResourceLimit);
    }
    let payload = encoded.strip_prefix(MAGIC).ok_or(Error::Version)?;
    let rlp = ae_codec::rlp::decode_with_limits(
        payload,
        ae_codec::rlp::DecodeLimits {
            max_input_bytes: config.max_record_bytes,
            ..config.transaction.rlp
        },
    )
    .map_err(|e| {
        if e == ae_codec::rlp::DecodeError::ResourceLimit {
            Error::ResourceLimit
        } else {
            Error::Malformed
        }
    })?;
    let value = Value::from_rlp(&rlp);
    let f = fields(&value, 5)?;
    let canonical = blob(&f[0])?;
    let (inner, sigs) = crate::signed::decode_parts(canonical, config.transaction.rlp)
        .map_err(Error::Transaction)?;
    let signed = SignedTransaction::from_stored_parts(
        Transaction::decode_record(inner, config.transaction, config.state, true)
            .map_err(Error::Transaction)?,
        sigs,
    );
    if signed.encode() != canonical {
        return Err(Error::Malformed);
    }
    let outer = restore_signatures(&f[3], signed.signatures())?;
    let mut tx = signed.transaction().clone();
    set_size(&mut tx.leaf, size(&f[1])?);
    let layers = array(&f[2])?;
    if layers.len() != tx.layers.len() {
        return Err(Error::Malformed);
    }
    for (l, v) in tx.layers.iter_mut().zip(layers) {
        let f = fields(v, 2)?;
        l.wire_size = size(&f[0])?;
        l.signatures = restore_signatures(&f[1], &l.signatures)?;
    }
    match &mut tx.leaf {
        TransactionLeaf::Contract(t) if matches!(t.operation(), ContractOperation::Call { .. }) => {
            let c = fields(&f[4], 2)?;
            let origin = blob(&c[0])?.try_into().map_err(|_| Error::Malformed)?;
            let stack = array(&c[1])?
                .iter()
                .map(integer)
                .collect::<Result<_, _>>()?;
            t.set_call_context(origin, stack)
                .map_err(Error::Transaction)?;
        }
        _ => {
            fields(&f[4], 0)?;
        }
    }
    check_sizes(&tx, config)?;
    let result = SignedTransaction::from_stored_parts(tx, outer);
    if encode(&result, config)? != encoded {
        return Err(Error::Malformed);
    }
    Ok(result)
}
