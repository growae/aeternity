// aeser_hc parent/child pin payloads, aeserialization dependency pinned with
// aeternity fa2a6d7. Copyright (c) 2024 Aeternity Anstalt. ISC: LICENSE.
use ae_codec::api::{self, ApiType};
use num_bigint::BigInt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parent {
    pub epoch: BigInt,
    pub height: BigInt,
    pub block_hash: Vec<u8>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    ParentPayload,
    ChildPayload,
    ResourceLimit,
}
// The original encoder accepts arbitrary hash widths; safe decoding requires
// a 32-byte key-block hash. Do not make encoding silently validate decoding.
pub fn encode_parent(pin: &Parent, maximum: usize) -> Result<Vec<u8>, Error> {
    if pin.epoch.bits().div_ceil(8) > maximum as u64
        || pin.height.bits().div_ceil(8) > maximum as u64
        || pin.block_hash.len() > maximum
    {
        return Err(Error::ResourceLimit);
    }
    let bytes = format!(
        "{:X}:{:X} {}",
        pin.epoch,
        pin.height,
        api::encode(ApiType::KeyBlockHash, &pin.block_hash)
    )
    .into_bytes();
    if bytes.len() > maximum {
        return Err(Error::ResourceLimit);
    }
    Ok(bytes)
}
fn integer(bytes: &[u8]) -> Result<BigInt, Error> {
    let digits = match bytes.first() {
        Some(b'+' | b'-') => &bytes[1..],
        _ => bytes,
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_hexdigit) {
        return Err(Error::ParentPayload);
    }
    BigInt::parse_bytes(bytes, 16).ok_or(Error::ParentPayload)
}
pub fn decode_parent(bytes: &[u8], maximum: usize) -> Result<Parent, Error> {
    if bytes.len() > maximum {
        return Err(Error::ResourceLimit);
    }
    // binary:split/3 splits on either delimiter, without enforcing their order.
    let mut fields = bytes.split(|b| matches!(b, b':' | b' '));
    let epoch = integer(fields.next().ok_or(Error::ParentPayload)?)?;
    let height = integer(fields.next().ok_or(Error::ParentPayload)?)?;
    let encoded = fields.next().ok_or(Error::ParentPayload)?;
    if fields.next().is_some() {
        return Err(Error::ParentPayload);
    }
    let encoded = std::str::from_utf8(encoded).map_err(|_| Error::ParentPayload)?;
    let (kind, block_hash) =
        api::decode_with_limit(encoded, maximum).map_err(|_| Error::ParentPayload)?;
    if kind != ApiType::KeyBlockHash {
        return Err(Error::ParentPayload);
    }
    Ok(Parent {
        epoch,
        height,
        block_hash,
    })
}
pub fn encode_child(hash: &[u8], maximum: usize) -> Result<Vec<u8>, Error> {
    if hash.len().checked_add(3).is_none_or(|n| n > maximum) {
        return Err(Error::ResourceLimit);
    }
    let mut result = b"pin".to_vec();
    result.extend_from_slice(hash);
    Ok(result)
}
pub fn decode_child(bytes: &[u8], maximum: usize) -> Result<&[u8], Error> {
    if bytes.len() > maximum {
        return Err(Error::ResourceLimit);
    }
    bytes.strip_prefix(b"pin").ok_or(Error::ChildPayload)
}
