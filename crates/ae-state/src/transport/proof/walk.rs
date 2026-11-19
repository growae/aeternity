// aeu_mp_trees:verify_proof/lookup_in_proof/int_verify_proof, fa2a6d7.
use super::*;
use ae_crypto::blake2b_256;

enum Node {
    Empty,
    Pair(Nibbles, bool, TreeValue),
    Branch(Vec<TreeValue>),
}
impl Proof {
    fn node(&self, reference: &[u8], b: &Budget) -> Result<Node, Error> {
        b.charge(reference.len().saturating_add(1))?;
        if reference.is_empty() {
            return Ok(Node::Empty);
        }
        let raw = if reference.len() == 32 {
            let values = self.nodes.get(reference).ok_or(Error::BadProof)?;
            for v in values {
                b.charge(v.len().saturating_add(1))?;
            }
            let raw = rlp::encode(&rlp::Rlp::List(
                values.iter().map(|v| rlp::Rlp::Bytes(v)).collect(),
            ));
            b.charge(raw.len())?;
            let hash = Hash256(reference.try_into().unwrap());
            if blake2b_256(&raw) != hash {
                return Err(Error::BadHash(hash));
            }
            raw
        } else if reference.len() < 32 {
            reference.to_vec()
        } else {
            return Err(Error::BadProof);
        };
        let decoded = rlp::decode_with_limits(&raw, b.decode_limits()).map_err(|e| {
            if e == rlp::DecodeError::ResourceLimit {
                Error::ResourceLimit
            } else {
                Error::BadProof
            }
        })?;
        let rlp::Rlp::List(values) = decoded else {
            return Err(Error::BadProof);
        };
        match values.as_slice() {
            [rlp::Rlp::Bytes(path), value] => {
                let (path, leaf) =
                    ae_trie::decode_compact_path(path).map_err(|_| Error::BadProof)?;
                Ok(Node::Pair(path, leaf, TreeValue::from_rlp(value)))
            }
            [] => Err(Error::BadProof),
            _ => Ok(Node::Branch(
                values.iter().map(TreeValue::from_rlp).collect(),
            )),
        }
    }
    // `None` represents Erlang's atom `lookup`, distinct from every tree value.
    pub(super) fn walk(
        &self,
        key: &[u8],
        expected: Option<&TreeValue>,
        b: &Budget,
    ) -> Result<(), Error> {
        let path = Nibbles::from_bytes(key);
        let mut path = path.as_slice();
        let mut node = self.node(&self.root.0, b)?;
        loop {
            b.charge(1)?;
            let actual = match node {
                Node::Empty => {
                    return if expected.is_some_and(TreeValue::is_empty) {
                        Ok(())
                    } else {
                        Err(Error::BadProof)
                    };
                }
                Node::Pair(prefix, leaf, value) => {
                    if leaf && prefix.as_slice() == path {
                        value
                    } else if !leaf && path.starts_with(prefix.as_slice()) {
                        path = &path[prefix.as_slice().len()..];
                        let TreeValue::Bytes(reference) = value else {
                            return Err(Error::BadProof);
                        };
                        node = self.node(&reference, b)?;
                        continue;
                    } else {
                        TreeValue::empty()
                    }
                }
                Node::Branch(values) => {
                    if let Some((&next, rest)) = path.split_first() {
                        let Some(TreeValue::Bytes(reference)) = values.get(usize::from(next))
                        else {
                            return Err(Error::BadProof);
                        };
                        node = self.node(reference, b)?;
                        path = rest;
                        continue;
                    }
                    // branch_value is in the `of` body, outside its catch.
                    values.get(16).cloned().ok_or(Error::NativeBadarg)?
                }
            };
            return if expected == Some(&actual) {
                Ok(())
            } else {
                Err(Error::BadValue(actual))
            };
        }
    }
}
