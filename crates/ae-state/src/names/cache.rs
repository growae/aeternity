// SPDX-License-Identifier: Apache-2.0
// Copyright 2014-16 Ulf Wiger.
// Original author: Ulf Wiger, Erlang Solutions, 2009.
// Copyright transfered to Ulf Wiger 2014.
// Modified: Rust port of the naming-cache subset; scalar code is in crate::sext.
// Licensed under Apache-2.0; see crates/ae-state/LICENSE-APACHE.
// Aeternity fa2a6d7, aens_state_tree tuple layout, encoded with sext 1.8.0.
use super::Error;
use crate::sext::{decode_binary, decode_integer, encode_binary, encode_integer, take};
use ae_types::Unsigned;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Name,
    Commitment,
    Auction,
}
impl Kind {
    fn module(self) -> &'static [u8] {
        match self {
            Self::Name => b"aens_names",
            Self::Commitment => b"aens_commitments",
            Self::Auction => b"aens_auctions",
        }
    }
}
pub(super) fn encode(height: &Unsigned, hash: &[u8], kind: Kind) -> Vec<u8> {
    let mut out = vec![16, 0, 0, 0, 3];
    encode_integer(height, &mut out);
    out.push(18);
    encode_binary(hash, &mut out);
    out.push(12);
    encode_binary(kind.module(), &mut out);
    out
}
pub(super) fn decode(mut bytes: &[u8]) -> Result<(Unsigned, Vec<u8>, Kind), Error> {
    if take(&mut bytes, 5)? != [16, 0, 0, 0, 3] {
        return Err(Error::InvalidCache);
    }
    let height = decode_integer(&mut bytes)?;
    if take(&mut bytes, 1)?[0] != 18 {
        return Err(Error::InvalidCache);
    }
    let hash = decode_binary(&mut bytes)?;
    if take(&mut bytes, 1)?[0] != 12 {
        return Err(Error::InvalidCache);
    }
    let kind = match decode_binary(&mut bytes)?.as_slice() {
        b"aens_names" => Kind::Name,
        b"aens_commitments" => Kind::Commitment,
        b"aens_auctions" => Kind::Auction,
        _ => return Err(Error::InvalidCache),
    };
    if !bytes.is_empty() || hash.len() != if kind == Kind::Auction { 33 } else { 32 } {
        return Err(Error::InvalidCache);
    }
    Ok((height, hash, kind))
}
