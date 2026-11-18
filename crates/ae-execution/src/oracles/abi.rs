// aeprimop.erl oracle ABI admission, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use ae_protocol::{Protocol, oracles::abi_available};
use ae_types::Unsigned;
use ae_vm_aevm::heap;
use ae_vm_fate::{self as fate, encoding, types};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    BadAbiVersion,
    BadQueryFormat,
    BadResponseFormat,
    BadFormat,
    StoredAevmFormat(heap::Error),
    StoredFateFormat(fate::Error),
    UnknownStoredAbi,
    ResourceLimit,
}
#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    pub aevm: heap::Limits,
    pub fate: fate::Limits,
}

pub fn validate_registration(
    abi: &Unsigned,
    query_format: &[u8],
    response_format: &[u8],
    protocol: Protocol,
    limits: Limits,
) -> Result<(), Error> {
    if !abi_available(abi, protocol) {
        return Err(Error::BadAbiVersion);
    }
    match abi.to_u64().ok() {
        Some(0) => Ok(()),
        Some(1) if protocol < Protocol::Minerva => Ok(()),
        Some(1) => {
            aevm_format(query_format, Error::BadQueryFormat, limits.aevm)?;
            aevm_format(response_format, Error::BadResponseFormat, limits.aevm)
        }
        Some(3) => {
            fate_format(query_format, Error::BadQueryFormat, limits.fate)?;
            fate_format(response_format, Error::BadResponseFormat, limits.fate)
        }
        _ => Err(Error::BadAbiVersion),
    }
}
fn aevm_format(format: &[u8], failure: Error, limits: heap::Limits) -> Result<(), Error> {
    heap::validate_type(format, limits)
        .map(|_| ())
        .map_err(|e| {
            if e == heap::Error::ResourceLimit {
                Error::ResourceLimit
            } else {
                failure
            }
        })
}
fn fate_format(format: &[u8], failure: Error, limits: fate::Limits) -> Result<(), Error> {
    let result =
        encoding::decode_type(format, limits).and_then(|t| encoding::encode_type(&t, limits));
    match result {
        Ok(bytes) if bytes == format => Ok(()),
        Err(fate::Error::ResourceLimit) => Err(Error::ResourceLimit),
        _ => Err(failure),
    }
}

// Registration version checks do not apply to existing oracles' content. In
// particular, legacy AEVM oracles remain usable after ABI 1 registration ends.
// Corrupt stored formats fail outside the reference's bad_format catch block.
pub fn validate_content(
    abi: &Unsigned,
    format: &[u8],
    content: &[u8],
    protocol: Protocol,
    limits: Limits,
) -> Result<(), Error> {
    match abi.to_u64().ok() {
        Some(0) => Ok(()),
        Some(1) => {
            let ty = heap::decode_type(format, limits.aevm).map_err(|e| {
                if e == heap::Error::ResourceLimit {
                    Error::ResourceLimit
                } else {
                    Error::StoredAevmFormat(e)
                }
            })?;
            heap::validate_value(&ty, content, limits.aevm)
                .map(|_| ())
                .map_err(|e| {
                    if e == heap::Error::ResourceLimit {
                        Error::ResourceLimit
                    } else {
                        Error::BadFormat
                    }
                })
        }
        Some(3) => {
            let ty = encoding::decode_type(format, limits.fate).map_err(|e| {
                if e == fate::Error::ResourceLimit {
                    Error::ResourceLimit
                } else {
                    Error::StoredFateFormat(e)
                }
            })?;
            let version = if protocol > Protocol::Iris {
                types::Version::Three
            } else {
                types::Version::One
            };
            let result = encoding::decode(content, limits.fate)
                .and_then(|v| types::check_type(&ty, &v, version, limits.fate));
            match result {
                Ok(Some(_)) => Ok(()),
                Err(fate::Error::ResourceLimit) => Err(Error::ResourceLimit),
                _ => Err(Error::BadFormat),
            }
        }
        _ => Err(Error::UnknownStoredAbi),
    }
}
