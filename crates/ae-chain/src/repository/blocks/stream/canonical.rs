use super::{Error, Rlp, binary_size, framed_size};

// The block-envelope parser limits list nesting to two nonempty levels.
// Compare canonical bytes in place instead of allocating another full block.
pub(super) fn matches(value: &Rlp<'_>, mut bytes: &[u8]) -> Result<bool, Error> {
    Ok(compare(value, &mut bytes)? && bytes.is_empty())
}
fn payload(values: &[Rlp<'_>]) -> Result<usize, Error> {
    values.iter().try_fold(0usize, |total, value| {
        total.checked_add(size(value)?).ok_or(Error::ResourceLimit)
    })
}
fn size(value: &Rlp<'_>) -> Result<usize, Error> {
    match value {
        Rlp::Bytes(bytes) => binary_size(bytes),
        Rlp::List(values) => framed_size(payload(values)?),
    }
}
fn consume(expected: &[u8], bytes: &mut &[u8]) -> bool {
    if let Some(tail) = bytes.strip_prefix(expected) {
        *bytes = tail;
        true
    } else {
        false
    }
}
fn prefix(offset: u8, length: usize, bytes: &mut &[u8]) -> bool {
    if length <= 55 {
        consume(&[offset + length as u8], bytes)
    } else {
        let encoded = length.to_be_bytes();
        let start = encoded
            .iter()
            .position(|b| *b != 0)
            .expect("nonzero length");
        consume(&[offset + 55 + (encoded.len() - start) as u8], bytes)
            && consume(&encoded[start..], bytes)
    }
}
fn compare(value: &Rlp<'_>, bytes: &mut &[u8]) -> Result<bool, Error> {
    Ok(match value {
        Rlp::Bytes([byte]) if *byte <= 0x7f => consume(&[*byte], bytes),
        Rlp::Bytes(value) => prefix(0x80, value.len(), bytes) && consume(value, bytes),
        Rlp::List(values) => {
            if !prefix(0xc0, payload(values)?, bytes) {
                return Ok(false);
            }
            for value in values {
                if !compare(value, bytes)? {
                    return Ok(false);
                }
            }
            true
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ae_codec::rlp;

    #[test]
    fn canonical_comparison_covers_length_boundaries_without_building_an_output() {
        for size in [0, 1, 2, 54, 55, 56, 255, 256, 65535, 65536, 1 << 20] {
            let data = vec![0x80; size];
            for value in [
                Rlp::Bytes(&data),
                Rlp::List(vec![Rlp::Bytes(&data)]),
                Rlp::List(vec![Rlp::Bytes(&[101]), Rlp::List(vec![Rlp::Bytes(&data)])]),
            ] {
                let encoded = rlp::encode(&value);
                assert!(matches(&value, &encoded).unwrap());
                assert!(!matches(&value, &encoded[..encoded.len() - 1]).unwrap());
                let mut trailing = encoded;
                trailing.push(0);
                assert!(!matches(&value, &trailing).unwrap());
            }
        }
        for encoded in [vec![0x81, 1], vec![0xb8, 1, 0x80], vec![0xf8, 1, 1]] {
            let value = rlp::decode(&encoded).unwrap();
            assert!(!matches(&value, &encoded).unwrap());
            assert!(matches(&value, &rlp::encode(&value)).unwrap());
        }
    }
}
