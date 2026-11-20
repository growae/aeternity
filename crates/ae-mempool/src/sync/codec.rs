use super::*;
use ae_codec::rlp::{self, Rlp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unfold {
    Node { path: Nibbles, node: Vec<u8> },
    Leaf(Nibbles),
    Subtree(Nibbles),
    Key(Vec<u8>),
}
pub(super) fn bytes(path: &Nibbles) -> Result<Vec<u8>, Error> {
    if !path.as_slice().len().is_multiple_of(2) {
        return Err(Error::InvalidMessage);
    }
    Ok(path
        .as_slice()
        .chunks_exact(2)
        .map(|c| (c[0] << 4) | c[1])
        .collect())
}
fn pack(path: &Nibbles) -> Vec<u8> {
    let ns = path.as_slice();
    let odd = !ns.len().is_multiple_of(2);
    let mut out = vec![if odd { 0x10 | ns[0] } else { 0 }];
    out.extend(
        ns[usize::from(odd)..]
            .chunks_exact(2)
            .map(|p| (p[0] << 4) | p[1]),
    );
    out
}
fn unpack(raw: &[u8], limit: usize) -> Result<Nibbles, Error> {
    let (&first, rest) = raw.split_first().ok_or(Error::InvalidMessage)?;
    let odd = first >> 4 == 1;
    if !odd && first != 0 {
        return Err(Error::InvalidMessage);
    }
    let len = rest
        .len()
        .checked_mul(2)
        .and_then(|n| n.checked_add(usize::from(odd)))
        .ok_or(Error::ResourceLimit)?;
    if len > limit {
        return Err(Error::ResourceLimit);
    }
    let mut path = Vec::with_capacity(len);
    if odd {
        path.push(first & 15);
    }
    for b in rest {
        path.extend([b >> 4, b & 15]);
    }
    Ok(Nibbles::new(path)?)
}
impl Unfold {
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        let (tag, path, node) = match self {
            Self::Node { path, node } => (0, Some(path), node.as_slice()),
            Self::Leaf(path) => (1, Some(path), &[][..]),
            Self::Subtree(path) => (2, Some(path), &[][..]),
            Self::Key(key) => (3, None, key.as_slice()),
        };
        if path.is_some_and(|p| p.as_slice().len() > limits.work.path_nibbles)
            || node.len() > limits.wire_bytes
        {
            return Err(Error::ResourceLimit);
        }
        let tag = [tag];
        let encoded_path = path.map(pack);
        let mut fields = vec![Rlp::Bytes(&tag)];
        if let Some(p) = &encoded_path {
            fields.push(Rlp::Bytes(p));
        }
        if tag[0] == 0 || tag[0] == 3 {
            fields.push(Rlp::Bytes(node));
        }
        let out = rlp::encode(&Rlp::List(fields));
        if out.len() > limits.wire_bytes {
            return Err(Error::ResourceLimit);
        }
        Ok(out)
    }
    pub fn decode(raw: &[u8], limits: Limits) -> Result<Self, Error> {
        let Rlp::List(fields) = rlp::decode_with_limits(
            raw,
            rlp::DecodeLimits {
                max_input_bytes: limits.wire_bytes,
                max_items: 8,
                max_depth: 2,
            },
        )?
        else {
            return Err(Error::InvalidMessage);
        };
        let Some(Rlp::Bytes(tag)) = fields.first() else {
            return Err(Error::InvalidMessage);
        };
        // aeserialization rejects leading zeroes, but accepts an empty zero.
        let kind = match *tag {
            [] | [0] => 0,
            [1] => 1,
            [2] => 2,
            [3] => 3,
            _ => return Err(Error::InvalidMessage),
        };
        match (kind, fields.as_slice()) {
            (0, [_, Rlp::Bytes(p), Rlp::Bytes(node)]) => Ok(Self::Node {
                path: unpack(p, limits.work.path_nibbles)?,
                node: node.to_vec(),
            }),
            (1, [_, Rlp::Bytes(p)]) => Ok(Self::Leaf(unpack(p, limits.work.path_nibbles)?)),
            (2, [_, Rlp::Bytes(p)]) => Ok(Self::Subtree(unpack(p, limits.work.path_nibbles)?)),
            (3, [_, Rlp::Bytes(k)]) => Ok(Self::Key(k.to_vec())),
            _ => Err(Error::InvalidMessage),
        }
    }
}
pub fn encode_batch(values: &[Unfold], limits: Limits) -> Result<Vec<Vec<u8>>, Error> {
    if values.len() > limits.items {
        return Err(Error::ResourceLimit);
    }
    let mut remaining = limits.wire_bytes;
    let mut out = vec![];
    for u in values {
        let b = u.encode(Limits {
            wire_bytes: remaining,
            ..limits
        })?;
        remaining -= b.len();
        out.push(b);
    }
    Ok(out)
}
pub fn decode_batch(values: &[Vec<u8>], limits: Limits) -> Result<Vec<Unfold>, Error> {
    if values.len() > limits.items {
        return Err(Error::ResourceLimit);
    }
    let mut remaining = limits.wire_bytes;
    let mut out = vec![];
    for b in values {
        remaining = remaining.checked_sub(b.len()).ok_or(Error::ResourceLimit)?;
        out.push(Unfold::decode(b, limits)?);
    }
    Ok(out)
}
