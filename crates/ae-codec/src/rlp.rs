// Behavioral reference: aeserialization a1204df, aeser_rlp.erl.
// Copyright (c) 2017, aeternity developers. ISC notice in the repository LICENSE.
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rlp<'a> {
    Bytes(&'a [u8]),
    List(Vec<Rlp<'a>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    EmptyInput,
    Truncated,
    LeadingZeroLength,
    LengthOverflow,
    TrailingBytes,
    ResourceLimit,
}
impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DecodeError {}

// These are caller resource budgets, not consensus validity rules.
#[derive(Debug, Clone, Copy)]
pub struct DecodeLimits {
    pub max_input_bytes: usize,
    pub max_items: usize,
    pub max_depth: usize,
}
impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 16 * 1024 * 1024,
            max_items: 1_000_000,
            max_depth: 256,
        }
    }
}

pub fn decode(bytes: &[u8]) -> Result<Rlp<'_>, DecodeError> {
    decode_with_limits(bytes, DecodeLimits::default())
}

pub fn decode_with_limits(bytes: &[u8], limits: DecodeLimits) -> Result<Rlp<'_>, DecodeError> {
    let (value, tail) = decode_one_with_limits(bytes, limits)?;
    if !tail.is_empty() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok(value)
}

pub fn decode_one(bytes: &[u8]) -> Result<(Rlp<'_>, &[u8]), DecodeError> {
    decode_one_with_limits(bytes, DecodeLimits::default())
}

pub fn decode_one_with_limits(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<(Rlp<'_>, &[u8]), DecodeError> {
    if bytes.is_empty() {
        return Err(DecodeError::EmptyInput);
    }
    if bytes.len() > limits.max_input_bytes {
        return Err(DecodeError::ResourceLimit);
    }
    struct Frame<'a> {
        end: usize,
        items: Vec<Rlp<'a>>,
    }
    let mut frames: Vec<Frame<'_>> = Vec::new();
    let mut position = 0usize;
    let mut items = 0usize;
    loop {
        items = items.checked_add(1).ok_or(DecodeError::ResourceLimit)?;
        if items > limits.max_items {
            return Err(DecodeError::ResourceLimit);
        }
        let boundary = frames.last().map_or(bytes.len(), |f| f.end);
        let prefix = *bytes.get(position).ok_or(DecodeError::Truncated)?;
        let (list, start, length) = match prefix {
            0..=0x7f => (false, position, 1),
            0x80..=0xb7 => (false, position + 1, usize::from(prefix - 0x80)),
            0xc0..=0xf7 => (true, position + 1, usize::from(prefix - 0xc0)),
            _ => {
                let list = prefix >= 0xf8;
                let length_bytes = usize::from(prefix - if list { 0xf7 } else { 0xb7 });
                let start = position
                    .checked_add(1 + length_bytes)
                    .ok_or(DecodeError::LengthOverflow)?;
                if bytes.get(position + 1).filter(|_| position + 1 < boundary) == Some(&0) {
                    return Err(DecodeError::LeadingZeroLength);
                }
                if start > boundary {
                    return Err(DecodeError::Truncated);
                }
                let raw = &bytes[position + 1..start];
                let length = raw.iter().try_fold(0usize, |n, b| {
                    n.checked_mul(256)
                        .and_then(|n| n.checked_add(usize::from(*b)))
                        .ok_or(DecodeError::LengthOverflow)
                })?;
                (list, start, length)
            }
        };
        let end = start
            .checked_add(length)
            .ok_or(DecodeError::LengthOverflow)?;
        if end > boundary {
            return Err(DecodeError::Truncated);
        }
        if list && length != 0 {
            if frames.len() >= limits.max_depth {
                return Err(DecodeError::ResourceLimit);
            }
            frames.push(Frame {
                end,
                items: Vec::new(),
            });
            position = start;
            continue;
        }
        position = end;
        let mut value = if list {
            Rlp::List(Vec::new())
        } else {
            Rlp::Bytes(&bytes[start..end])
        };
        loop {
            let Some(frame) = frames.last_mut() else {
                return Ok((value, &bytes[position..]));
            };
            frame.items.push(value);
            if position != frame.end {
                break;
            }
            value = Rlp::List(frames.pop().expect("frame just inspected").items);
        }
    }
}

pub fn encode(value: &Rlp<'_>) -> Vec<u8> {
    match value {
        Rlp::Bytes([byte]) if *byte <= 0x7f => vec![*byte],
        Rlp::Bytes(bytes) => with_size(0x80, bytes),
        Rlp::List(values) => {
            let mut bytes = Vec::new();
            for value in values {
                bytes.extend(encode(value));
            }
            with_size(0xc0, &bytes)
        }
    }
}

pub(crate) fn with_size(offset: u8, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    if bytes.len() <= 55 {
        out.push(offset + bytes.len() as u8);
    } else {
        let length = bytes.len().to_be_bytes();
        let start = length
            .iter()
            .position(|b| *b != 0)
            .expect("nonempty length");
        out.push(offset + 55 + (length.len() - start) as u8);
        out.extend_from_slice(&length[start..]);
    }
    out.extend_from_slice(bytes);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reference_vectors() {
        let cases: &[(&[u8], Rlp<'_>)] = &[
            (&[0x80], Rlp::Bytes(b"")),
            (&[0], Rlp::Bytes(&[0])),
            (&[0x81, 0x80], Rlp::Bytes(&[0x80])),
            (&[0x83, b'c', b'a', b't'], Rlp::Bytes(b"cat")),
            (
                &[0xc8, 0x83, b'c', b'a', b't', 0x83, b'd', b'o', b'g'],
                Rlp::List(vec![Rlp::Bytes(b"cat"), Rlp::Bytes(b"dog")]),
            ),
            (&[0xc0], Rlp::List(vec![])),
            (
                &[0xc4, 0xc0, 0xc2, 0xc0, 0xc0],
                Rlp::List(vec![
                    Rlp::List(vec![]),
                    Rlp::List(vec![Rlp::List(vec![]), Rlp::List(vec![])]),
                ]),
            ),
        ];
        for (raw, value) in cases {
            assert_eq!(encode(value), *raw);
            assert_eq!(decode(raw), Ok(value.clone()));
        }
    }
    #[test]
    fn length_boundaries() {
        for size in [0, 1, 55, 56, 255, 256, 65535, 65536] {
            let bytes = vec![42; size];
            let raw = encode(&Rlp::Bytes(&bytes));
            assert_eq!(decode(&raw), Ok(Rlp::Bytes(&bytes)));
            if size == 55 {
                assert_eq!(raw[0], 0xb7);
            }
            if size == 56 {
                assert_eq!(&raw[..2], &[0xb8, 56]);
            }
            if size == 256 {
                assert_eq!(&raw[..3], &[0xb9, 1, 0]);
            }
        }
    }
    #[test]
    fn reference_decoder_is_deliberately_not_strict_ethereum_rlp() {
        for raw in [&[0x81, 0x01][..], &[0xb8, 0x01, 0x01][..]] {
            assert_eq!(decode(raw), Ok(Rlp::Bytes(&[1])));
        }
        assert_eq!(decode(&[0xf8, 1, 1]), Ok(Rlp::List(vec![Rlp::Bytes(&[1])])));
        assert_eq!(
            decode(&[0xb9, 0, 1, 1]),
            Err(DecodeError::LeadingZeroLength)
        );
        assert_eq!(
            decode(&[0xf9, 0, 1, 1]),
            Err(DecodeError::LeadingZeroLength)
        );
    }
    #[test]
    fn malformed_and_bounded_inputs() {
        for raw in [
            &[][..],
            &[0x81][..],
            &[0xc1, 0x81][..],
            &[0xb8][..],
            &[0xc2, 0x82, 1][..],
        ] {
            assert!(decode(raw).is_err());
        }
        assert_eq!(decode(&[1, 2]), Err(DecodeError::TrailingBytes));
        assert_eq!(decode_one(&[1, 2]), Ok((Rlp::Bytes(&[1]), &[2][..])));
        let limits = DecodeLimits {
            max_items: 1,
            ..DecodeLimits::default()
        };
        assert_eq!(
            decode_with_limits(&[0xc1, 1], limits),
            Err(DecodeError::ResourceLimit)
        );
        let limits = DecodeLimits {
            max_depth: 0,
            ..DecodeLimits::default()
        };
        assert_eq!(
            decode_with_limits(&[0xc1, 1], limits),
            Err(DecodeError::ResourceLimit)
        );
    }
}
