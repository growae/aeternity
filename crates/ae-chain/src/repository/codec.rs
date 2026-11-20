use super::{Error, Hash256, Unsigned, Value};
pub(super) fn binary(b: impl AsRef<[u8]>) -> Value {
    Value::Bytes(b.as_ref().to_vec())
}
pub(super) fn list(v: Vec<Value>) -> Value {
    Value::List(v)
}
pub(super) fn number(n: u64) -> Value {
    binary(Unsigned::from(n).as_bytes())
}
pub(super) fn optional(h: Option<Hash256>) -> Value {
    binary(h.map(|h| h.0.to_vec()).unwrap_or_default())
}
pub(super) fn decode(b: &[u8]) -> Result<Value, Error> {
    Value::decode(b).map_err(|_| Error::Corrupt("record RLP"))
}
pub(super) fn blob(v: &Value) -> Result<&[u8], Error> {
    match v {
        Value::Bytes(b) => Ok(b),
        _ => Err(Error::Corrupt("expected bytes")),
    }
}
pub(super) fn items(v: &Value) -> Result<&[Value], Error> {
    match v {
        Value::List(a) => Ok(a),
        _ => Err(Error::Corrupt("expected list")),
    }
}
pub(super) fn fields(v: &Value, n: usize) -> Result<&[Value], Error> {
    let a = items(v)?;
    if a.len() == n {
        Ok(a)
    } else {
        Err(Error::Corrupt("field count"))
    }
}
pub(super) fn unsigned(v: &Value) -> Result<Unsigned, Error> {
    Unsigned::from_field_bytes(blob(v)?).map_err(|_| Error::Corrupt("integer"))
}
pub(super) fn integer(v: &Value) -> Result<u64, Error> {
    unsigned(v)?
        .to_u64()
        .map_err(|_| Error::Corrupt("integer overflow"))
}
pub(super) fn fixed_hash(b: &[u8]) -> Result<Hash256, Error> {
    b.try_into()
        .map(Hash256)
        .map_err(|_| Error::Corrupt("hash length"))
}
pub(super) fn hash(v: &Value) -> Result<Hash256, Error> {
    fixed_hash(blob(v)?)
}
pub(super) fn optional_hash(v: &Value) -> Result<Option<Hash256>, Error> {
    let b = blob(v)?;
    if b.is_empty() {
        Ok(None)
    } else {
        Ok(Some(fixed_hash(b)?))
    }
}
