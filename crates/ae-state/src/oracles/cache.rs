// Aeternity fa2a6d7, aeo_state_tree expiry keys encoded with sext 1.8.0.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::Error;
use crate::sext::{decode_binary, decode_integer, encode_binary, encode_integer, take};
use ae_types::Unsigned;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Key {
    Oracle([u8; 32]),
    Query([u8; 32], [u8; 32]),
}
pub(super) fn encode(height: &Unsigned, key: Key) -> Vec<u8> {
    let mut out = vec![16, 0, 0, 0, 2];
    encode_integer(height, &mut out);
    let (arity, atom, oracle, query) = match key {
        Key::Oracle(o) => (2, b"oracle".as_slice(), o, None),
        Key::Query(o, q) => (3, b"query".as_slice(), o, Some(q)),
    };
    out.extend_from_slice(&[16, 0, 0, 0, arity, 12]);
    encode_binary(atom, &mut out);
    out.push(18);
    encode_binary(&oracle, &mut out);
    if let Some(q) = query {
        out.push(18);
        encode_binary(&q, &mut out);
    }
    out
}
pub(super) fn decode(mut bytes: &[u8]) -> Result<(Unsigned, Key), Error> {
    if take(&mut bytes, 5)? != [16, 0, 0, 0, 2] {
        return Err(Error::InvalidCache);
    }
    let height = decode_integer(&mut bytes)?;
    if take(&mut bytes, 4)? != [16, 0, 0, 0] {
        return Err(Error::InvalidCache);
    }
    let arity = take(&mut bytes, 1)?[0];
    if take(&mut bytes, 1)? != [12] {
        return Err(Error::InvalidCache);
    }
    let atom = decode_binary(&mut bytes)?;
    if take(&mut bytes, 1)? != [18] {
        return Err(Error::InvalidCache);
    }
    let oracle = decode_binary(&mut bytes)?
        .try_into()
        .map_err(|_| Error::InvalidCache)?;
    let key = match (arity, atom.as_slice()) {
        (2, b"oracle") => Key::Oracle(oracle),
        (3, b"query") => {
            if take(&mut bytes, 1)? != [18] {
                return Err(Error::InvalidCache);
            }
            Key::Query(
                oracle,
                decode_binary(&mut bytes)?
                    .try_into()
                    .map_err(|_| Error::InvalidCache)?,
            )
        }
        _ => return Err(Error::InvalidCache),
    };
    if !bytes.is_empty() {
        return Err(Error::InvalidCache);
    }
    Ok((height, key))
}
