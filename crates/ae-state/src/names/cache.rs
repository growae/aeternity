// SPDX-License-Identifier: Apache-2.0
// Copyright 2014-16 Ulf Wiger.
// Original author: Ulf Wiger, Erlang Solutions, 2009.
// Copyright transfered to Ulf Wiger 2014.
// Modified: Rust port of the naming-cache subset, with checked decoding.
// Licensed under Apache-2.0; see crates/ae-state/LICENSE-APACHE.
// Only the positive-integer/binary/atom tuple used by the naming expiry index.
// Byte format: pinned sext 1.8.0. This is not a general Erlang-term decoder.
use super::Error;
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
    if let Ok(n @ 0..=0x7fffffff) = height.to_u64() {
        out.push(10);
        out.extend_from_slice(&((n as u32) << 1).to_be_bytes());
    } else {
        out.push(11);
        let mut raw = height.as_bytes().to_vec();
        if raw[0] == 255 {
            raw.insert(0, 0);
        }
        let mut sized = vec![255];
        encode_size(raw.len(), &mut sized);
        sized.extend(raw);
        encode_binary(&sized, &mut out);
        out.push(0);
    }
    out.push(18);
    encode_binary(hash, &mut out);
    out.push(12);
    encode_binary(kind.module(), &mut out);
    out
}
fn encode_size(n: usize, out: &mut Vec<u8>) {
    if n <= 127 {
        out.push(n as u8);
        return;
    }
    let mut bits = (usize::BITS - n.leading_zeros()).div_ceil(8) * 8;
    while bits > 7 {
        bits -= 7;
        out.push(128 | ((n >> bits) as u8 & 127));
    }
    out.push((n & ((1 << bits) - 1)) as u8);
}
fn encode_binary(bytes: &[u8], out: &mut Vec<u8>) {
    if bytes.is_empty() {
        out.push(8);
        return;
    }
    let mut bits = 0u32;
    let mut buffer = 0u32;
    for byte in bytes {
        buffer = (buffer << 9) | 0x100 | u32::from(*byte);
        bits += 9;
        while bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    out.push((buffer << (8 - bits)) as u8);
    out.push(8);
}
pub(super) fn decode(mut bytes: &[u8]) -> Result<(Unsigned, Vec<u8>, Kind), Error> {
    if take(&mut bytes, 5)? != [16, 0, 0, 0, 3] {
        return Err(Error::InvalidCache);
    }
    let height = match take(&mut bytes, 1)?[0] {
        10 => {
            let n = u32::from_be_bytes(take(&mut bytes, 4)?.try_into().expect("length"));
            if n & 1 != 0 {
                return Err(Error::InvalidCache);
            }
            Unsigned::from(u64::from(n >> 1))
        }
        11 => {
            let raw = decode_binary(&mut bytes)?;
            let mut raw = raw.as_slice();
            if raw.first() == Some(&255) {
                raw = &raw[1..];
                while take(&mut raw, 1)?[0] & 128 != 0 {}
            }
            let raw = &raw[raw.iter().position(|b| *b != 0).unwrap_or(raw.len())..];
            if take(&mut bytes, 1)?[0] != 0 {
                return Err(Error::InvalidCache);
            }
            Unsigned::from_field_bytes(raw).map_err(|_| Error::InvalidCache)?
        }
        _ => return Err(Error::InvalidCache),
    };
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
fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8], Error> {
    let (head, tail) = bytes.split_at_checked(n).ok_or(Error::InvalidCache)?;
    *bytes = tail;
    Ok(head)
}
fn decode_binary(bytes: &mut &[u8]) -> Result<Vec<u8>, Error> {
    if bytes.first() == Some(&8) {
        *bytes = &bytes[1..];
        return Ok(vec![]);
    }
    let mut position = 0usize;
    let mut out = Vec::new();
    let bit = |i: usize| -> Result<u8, Error> {
        bytes
            .get(i / 8)
            .map(|b| (b >> (7 - i % 8)) & 1)
            .ok_or(Error::InvalidCache)
    };
    while bit(position)? == 1 {
        position += 1;
        let mut byte = 0;
        for _ in 0..8 {
            byte = (byte << 1) | bit(position)?;
            position += 1;
        }
        out.push(byte);
    }
    if out.is_empty() {
        return Err(Error::InvalidCache);
    }
    let padding = 8 - position % 8;
    for _ in 0..padding {
        if bit(position)? != 0 {
            return Err(Error::InvalidCache);
        }
        position += 1;
    }
    let consumed = position / 8 + 1;
    if bytes.get(position / 8) != Some(&8) {
        return Err(Error::InvalidCache);
    }
    *bytes = &bytes[consumed..];
    Ok(out)
}
