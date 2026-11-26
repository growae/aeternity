// aec_chain account lookups and aec_accounts:serialize_for_client, fa2a6d7.
// Copyright (c) 2017-2026, Aeternity developers. ISC: repository LICENSE.
use ae_codec::{rlp, sophia::SophiaCode};
use ae_state::accounts::Account;
use ae_types::Hash256;
use ae_vm_fate::{code::Code as FateCode, value::Value as FateValue};

pub mod repository;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
    Current,
    Height(u128),
    Hash(Hash256),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    AccountNotFound,
    NoStateTrees,
    HeightNotAvailable,
    GarbageCollected,
    InvalidObject,
    ResourceLimit,
    Backend(crate::repository::Error),
}
impl From<crate::repository::Error> for Error {
    fn from(error: crate::repository::Error) -> Self {
        use crate::repository::Error as E;
        match error {
            E::ResourceLimit
            | E::Trie(ae_trie::Error::ResourceLimit)
            | E::Trie(ae_trie::Error::Decode(rlp::DecodeError::ResourceLimit)) => {
                Self::ResourceLimit
            }
            error => Self::Backend(error),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    pub reads: crate::repository::point::Limits,
    pub fate: ae_vm_fate::Limits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub account: Account,
    pub auth_fun: Option<Vec<u8>>,
}

// Preserve byte names until the HTTP adapter applies native JSON text handling.
pub fn authentication_name(
    abi: u16,
    function: &[u8],
    code: &[u8],
    limits: Limits,
) -> Result<Vec<u8>, Error> {
    if !matches!(abi, 1 | 3) {
        return Err(Error::InvalidObject);
    }
    let code = SophiaCode::decode_with_limits(
        code,
        rlp::DecodeLimits {
            max_input_bytes: limits
                .reads
                .value_bytes
                .min(limits.reads.rlp.max_input_bytes),
            ..limits.reads.rlp
        },
    )
    .map_err(|e| match e {
        ae_codec::object::ObjectError::Rlp(rlp::DecodeError::ResourceLimit) => Error::ResourceLimit,
        _ => Error::InvalidObject,
    })?;
    match abi {
        1 => code
            .type_info
            .into_iter()
            .find(|info| info.hash == function)
            .map(|info| info.name)
            .ok_or(Error::InvalidObject),
        3 => {
            let function = match function.len() {
                4 | 32 => &function[..4],
                _ => return Err(Error::InvalidObject),
            };
            let code = FateCode::decode(&code.byte_code, limits.fate).map_err(|e| match e {
                ae_vm_fate::code::CodeError::Data(ae_vm_fate::Error::ResourceLimit) => {
                    Error::ResourceLimit
                }
                _ => Error::InvalidObject,
            })?;
            let FateValue::Map(symbols) = code.symbols else {
                return Err(Error::InvalidObject);
            };
            match symbols.get(&FateValue::String(function.to_vec())) {
                Some(FateValue::String(name)) => Ok(name.clone()),
                _ => Err(Error::InvalidObject),
            }
        }
        _ => Err(Error::InvalidObject),
    }
}
