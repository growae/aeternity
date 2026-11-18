use ae_crypto::api_checksum;
use ae_types::{Id, IdTag};
use base64::{
    Engine, alphabet,
    engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig},
};
use std::fmt;

mod kinds;
pub use kinds::ApiType;

const BASE64: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true),
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    MissingPrefix,
    UnknownPrefix,
    InvalidBase,
    Checksum,
    IncorrectSize,
    ResourceLimit,
}
impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ApiError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SafeDecodeError {
    InvalidEncoding,
    InvalidPrefix,
}

pub fn encode(kind: ApiType, payload: &[u8]) -> String {
    let mut checked = payload.to_vec();
    checked.extend_from_slice(&api_checksum(payload));
    let body = if kind.is_base64() {
        BASE64.encode(checked)
    } else {
        bs58::encode(checked).into_string()
    };
    format!("{}_{}", kind.prefix(), body)
}

pub fn decode(text: &str) -> Result<(ApiType, Vec<u8>), ApiError> {
    decode_with_limit(text, 16 * 1024 * 1024)
}

pub fn decode_with_limit(
    text: &str,
    max_encoded_bytes: usize,
) -> Result<(ApiType, Vec<u8>), ApiError> {
    if text.len() > max_encoded_bytes {
        return Err(ApiError::ResourceLimit);
    }
    let (prefix, body) = text.split_once('_').ok_or(ApiError::MissingPrefix)?;
    let kind = ApiType::from_prefix(prefix).ok_or(ApiError::UnknownPrefix)?;
    let checked = if kind.is_base64() {
        // OTP 24 base64:decode accepts exactly these whitespace characters and
        // ignores nonzero unused bits in the final padded quantum.
        let body: Vec<u8> = body
            .bytes()
            .filter(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
            .collect();
        BASE64.decode(body).map_err(|_| ApiError::InvalidBase)?
    } else {
        bs58::decode(body)
            .into_vec()
            .map_err(|_| ApiError::InvalidBase)?
    };
    if checked.len() < 4 {
        return Err(ApiError::Checksum);
    }
    let (payload, checksum) = checked.split_at(checked.len() - 4);
    if api_checksum(payload) != checksum {
        return Err(ApiError::Checksum);
    }
    if kind
        .payload_size()
        .is_some_and(|size| size != payload.len())
    {
        return Err(ApiError::IncorrectSize);
    }
    Ok((kind, payload.to_vec()))
}

pub fn decode_as(expected: ApiType, text: &str) -> Result<Vec<u8>, SafeDecodeError> {
    let (actual, payload) = decode(text).map_err(|_| SafeDecodeError::InvalidEncoding)?;
    if actual != expected {
        return Err(SafeDecodeError::InvalidPrefix);
    }
    Ok(payload)
}

pub fn decode_block_hash(text: &str) -> Result<(ApiType, Vec<u8>), SafeDecodeError> {
    let (actual, payload) = decode(text).map_err(|_| SafeDecodeError::InvalidEncoding)?;
    if !matches!(actual, ApiType::KeyBlockHash | ApiType::MicroBlockHash) {
        return Err(SafeDecodeError::InvalidPrefix);
    }
    Ok((actual, payload))
}

pub fn encode_id(id: Id) -> String {
    let kind = match id.tag {
        IdTag::Account => ApiType::AccountPubkey,
        IdTag::Name => ApiType::Name,
        IdTag::Commitment => ApiType::Commitment,
        IdTag::Oracle => ApiType::OraclePubkey,
        IdTag::Contract => ApiType::ContractPubkey,
        IdTag::Channel => ApiType::Channel,
    };
    encode(kind, &id.value)
}

pub fn decode_id(text: &str, allowed: &[ApiType]) -> Result<Id, SafeDecodeError> {
    let (actual, payload) = decode(text).map_err(|_| SafeDecodeError::InvalidEncoding)?;
    if !allowed.contains(&actual) {
        return Err(SafeDecodeError::InvalidPrefix);
    }
    let tag = match actual {
        ApiType::AccountPubkey => IdTag::Account,
        ApiType::Name => IdTag::Name,
        ApiType::Commitment => IdTag::Commitment,
        ApiType::OraclePubkey => IdTag::Oracle,
        ApiType::ContractPubkey => IdTag::Contract,
        ApiType::Channel => IdTag::Channel,
        _ => return Err(SafeDecodeError::InvalidPrefix),
    };
    let value = payload
        .try_into()
        .map_err(|_| SafeDecodeError::InvalidPrefix)?;
    Ok(Id::new(tag, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_prefixes_and_widths() {
        for kind in ApiType::ALL {
            let payload = vec![42; kind.payload_size().unwrap_or(7)];
            let text = encode(kind, &payload);
            assert_eq!(decode(&text), Ok((kind, payload)));
            if kind.payload_size().is_some() {
                assert_eq!(decode(&encode(kind, &[])), Err(ApiError::IncorrectSize));
            }
        }
        assert_eq!(
            encode(ApiType::AccountPubkey, &[0; 32]),
            "ak_11111111111111111111111111111111273Yts"
        );
    }
    #[test]
    fn typed_errors_and_checksums() {
        let text = encode(ApiType::AccountPubkey, &[1; 32]);
        assert_eq!(
            decode_as(ApiType::ContractPubkey, &text),
            Err(SafeDecodeError::InvalidPrefix)
        );
        assert_eq!(
            decode_as(ApiType::AccountPubkey, "ak_0"),
            Err(SafeDecodeError::InvalidEncoding)
        );
        assert_eq!(decode("hello"), Err(ApiError::MissingPrefix));
        assert_eq!(decode("zz_hello"), Err(ApiError::UnknownPrefix));
        assert_eq!(decode("ba_AAAAAA=="), Err(ApiError::Checksum));
        assert_eq!(decode_with_limit(&text, 3), Err(ApiError::ResourceLimit));
    }
    #[test]
    fn erlang_base64_whitespace_and_unused_bits() {
        let normal = encode(ApiType::Bytearray, &[]);
        assert_eq!(normal, "ba_Xfbg4g==");
        assert_eq!(decode("ba_ Xf\tbg\r4g==\n"), decode(&normal));
        assert_eq!(decode("ba_Xfbg4v=="), decode(&normal));
        assert!(decode("ba_Xfbg4g").is_err());
        assert!(decode("ba_Xfbg4g==AA==").is_err());
        assert!(decode("ba_Xfbg\u{000b}4g==").is_err());
    }
    #[test]
    fn name_prefix_is_variable_width_but_name_id_is_not() {
        let text = encode(ApiType::Name, b"abc");
        assert!(decode(&text).is_ok());
        assert_eq!(
            decode_id(&text, &[ApiType::Name]),
            Err(SafeDecodeError::InvalidPrefix)
        );
        let id = Id::new(IdTag::Name, [1; 32]);
        assert_eq!(decode_id(&encode_id(id), &[ApiType::Name]), Ok(id));
    }
}
