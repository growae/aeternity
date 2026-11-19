#![forbid(unsafe_code)]
// Aeternity fa2a6d7, aeu_mp_trees.erl. Extensions always reference hashes;
// branch children inline RLP shorter than 32 bytes as byte strings.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use ae_codec::rlp::{self, Rlp};
use ae_crypto::blake2b_256;
use ae_types::Hash256;
use std::{collections::BTreeMap, fmt};

mod traversal;
pub use traversal::{IteratorOptions, OwnedReadView, ReadView, TrieIterator, transaction_root};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    EmptyKey,
    InvalidNibble,
    InvalidPath,
    InvalidNode,
    InvalidReference,
    MissingNode(Hash256),
    HashMismatch(Hash256),
    Backend(String),
    Decode(rlp::DecodeError),
    WrongValue,
    NoSuchSubtree,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<rlp::DecodeError> for Error {
    fn from(e: rlp::DecodeError) -> Self {
        Self::Decode(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Nibbles(Vec<u8>);
impl Nibbles {
    pub fn new(nibbles: Vec<u8>) -> Result<Self, Error> {
        if nibbles.iter().any(|n| *n > 15) {
            return Err(Error::InvalidNibble);
        }
        Ok(Self(nibbles))
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(bytes.iter().flat_map(|b| [b >> 4, b & 15]).collect())
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Bytes(Vec<u8>),
    List(Vec<Value>),
}
impl Value {
    pub fn empty() -> Self {
        Self::Bytes(Vec::new())
    }
    pub fn is_empty(&self) -> bool {
        matches!(self,Self::Bytes(b) if b.is_empty())
    }
    pub fn encode(&self) -> Vec<u8> {
        rlp::encode(&self.borrowed())
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_rlp(&rlp::decode(bytes)?))
    }
    fn from_rlp(node: &Rlp<'_>) -> Self {
        match node {
            Rlp::Bytes(b) => Self::Bytes(b.to_vec()),
            Rlp::List(items) => Self::List(items.iter().map(Self::from_rlp).collect()),
        }
    }
    fn borrowed(&self) -> Rlp<'_> {
        match self {
            Self::Bytes(b) => Rlp::Bytes(b),
            Self::List(items) => Rlp::List(items.iter().map(Self::borrowed).collect()),
        }
    }
}

pub trait NodeReader {
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, Error>;
}
pub trait NodeWriter: NodeReader {
    fn put_node(&mut self, hash: Hash256, bytes: Vec<u8>) -> Result<(), Error>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryStore {
    nodes: BTreeMap<Hash256, Vec<u8>>,
}
impl MemoryStore {
    pub fn nodes(&self) -> &BTreeMap<Hash256, Vec<u8>> {
        &self.nodes
    }
}
impl NodeReader for MemoryStore {
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.nodes.get(hash).cloned())
    }
}
impl NodeWriter for MemoryStore {
    fn put_node(&mut self, hash: Hash256, bytes: Vec<u8>) -> Result<(), Error> {
        if blake2b_256(&bytes) != hash {
            return Err(Error::HashMismatch(hash));
        }
        self.nodes.insert(hash, bytes);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Reference {
    Empty,
    Inline(Vec<u8>),
    Hash(Hash256),
}
impl Reference {
    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        match bytes.len() {
            0 => Ok(Self::Empty),
            1..=31 => Ok(Self::Inline(bytes.to_vec())),
            32 => Ok(Self::Hash(Hash256(
                bytes.try_into().expect("length checked"),
            ))),
            _ => Err(Error::InvalidReference),
        }
    }
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Empty => &[],
            Self::Inline(b) => b,
            Self::Hash(h) => h.as_ref(),
        }
    }
    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
}

#[derive(Debug, Clone)]
enum Node {
    Empty,
    Leaf {
        path: Vec<u8>,
        value: Value,
    },
    Extension {
        path: Vec<u8>,
        child: Hash256,
    },
    Branch {
        children: Box<[Reference; 16]>,
        value: Value,
    },
}

pub fn compact_path(path: &Nibbles, leaf: bool) -> Vec<u8> {
    let odd = path.0.len() % 2 == 1;
    let mut out = vec![(u8::from(leaf) * 2 + u8::from(odd)) << 4 | if odd { path.0[0] } else { 0 }];
    for pair in path.0[usize::from(odd)..].chunks_exact(2) {
        out.push((pair[0] << 4) | pair[1]);
    }
    out
}
pub fn decode_compact_path(bytes: &[u8]) -> Result<(Nibbles, bool), Error> {
    let first = *bytes.first().ok_or(Error::InvalidPath)?;
    let flag = first >> 4;
    if flag > 3 || (flag % 2 == 0 && first & 15 != 0) {
        return Err(Error::InvalidPath);
    }
    let mut path = Vec::new();
    if flag % 2 == 1 {
        path.push(first & 15);
    }
    for byte in &bytes[1..] {
        path.extend([byte >> 4, byte & 15]);
    }
    Ok((Nibbles(path), flag >= 2))
}

fn read_node<S: NodeReader>(reference: &Reference, store: &S) -> Result<Node, Error> {
    let raw = match reference {
        Reference::Empty => return Ok(Node::Empty),
        Reference::Inline(b) => b.clone(),
        Reference::Hash(hash) => {
            let bytes = store.node(hash)?.ok_or(Error::MissingNode(*hash))?;
            if blake2b_256(&bytes) != *hash {
                return Err(Error::HashMismatch(*hash));
            }
            bytes
        }
    };
    let Rlp::List(items) = rlp::decode(&raw)? else {
        return Err(Error::InvalidNode);
    };
    match items.as_slice() {
        [Rlp::Bytes(compact), value] => {
            let (path, leaf) = decode_compact_path(compact)?;
            if leaf {
                let value = Value::from_rlp(value);
                if value.is_empty() {
                    return Err(Error::InvalidNode);
                }
                Ok(Node::Leaf {
                    path: path.0,
                    value,
                })
            } else {
                if path.0.is_empty() {
                    return Err(Error::InvalidPath);
                }
                let Rlp::Bytes(hash) = value else {
                    return Err(Error::InvalidReference);
                };
                let hash = Hash256::try_from(*hash).map_err(|_| Error::InvalidReference)?;
                Ok(Node::Extension {
                    path: path.0,
                    child: hash,
                })
            }
        }
        _ if items.len() == 17 => {
            let mut children = Box::new(std::array::from_fn(|_| Reference::Empty));
            for (child, item) in children.iter_mut().zip(&items[..16]) {
                let Rlp::Bytes(bytes) = item else {
                    return Err(Error::InvalidReference);
                };
                *child = Reference::from_bytes(bytes)?;
            }
            Ok(Node::Branch {
                children,
                value: Value::from_rlp(&items[16]),
            })
        }
        _ => Err(Error::InvalidNode),
    }
}

fn node_bytes(node: &Node) -> Result<Vec<u8>, Error> {
    let value = match node {
        Node::Empty => return Err(Error::InvalidNode),
        Node::Leaf { path, value } => Value::List(vec![
            Value::Bytes(compact_path(&Nibbles(path.clone()), true)),
            value.clone(),
        ]),
        Node::Extension { path, child } => Value::List(vec![
            Value::Bytes(compact_path(&Nibbles(path.clone()), false)),
            Value::Bytes(child.as_ref().to_vec()),
        ]),
        Node::Branch { children, value } => {
            let mut items: Vec<_> = children
                .iter()
                .map(|r| Value::Bytes(r.as_bytes().to_vec()))
                .collect();
            items.push(value.clone());
            Value::List(items)
        }
    };
    Ok(value.encode())
}

fn store_node<S: NodeWriter>(node: Node, store: &mut S) -> Result<Reference, Error> {
    if matches!(node, Node::Empty) {
        return Ok(Reference::Empty);
    }
    let bytes = node_bytes(&node)?;
    if bytes.len() < 32 {
        return Ok(Reference::Inline(bytes));
    }
    let hash = blake2b_256(&bytes);
    store.put_node(hash, bytes)?;
    Ok(Reference::Hash(hash))
}
fn force_hash<S: NodeWriter>(reference: Reference, store: &mut S) -> Result<Hash256, Error> {
    match reference {
        Reference::Empty => Err(Error::InvalidReference),
        Reference::Hash(hash) => Ok(hash),
        Reference::Inline(bytes) => {
            let hash = blake2b_256(&bytes);
            store.put_node(hash, bytes)?;
            Ok(hash)
        }
    }
}
fn resolve_short<S: NodeReader>(reference: Reference, store: &S) -> Result<Reference, Error> {
    if let Reference::Hash(hash) = reference {
        let node = read_node(&Reference::Hash(hash), store)?;
        let bytes = node_bytes(&node)?;
        if bytes.len() < 32 {
            Ok(Reference::Inline(bytes))
        } else {
            Ok(Reference::Hash(hash))
        }
    } else {
        Ok(reference)
    }
}
fn extension<S: NodeWriter>(
    path: &[u8],
    child: Reference,
    store: &mut S,
) -> Result<Reference, Error> {
    if path.is_empty() {
        return resolve_short(child, store);
    }
    let child = force_hash(child, store)?;
    store_node(
        Node::Extension {
            path: path.to_vec(),
            child,
        },
        store,
    )
}
fn leaf<S: NodeWriter>(path: &[u8], value: Value, store: &mut S) -> Result<Reference, Error> {
    store_node(
        Node::Leaf {
            path: path.to_vec(),
            value,
        },
        store,
    )
}
fn common(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(a, b)| a == b).count()
}

fn put<S: NodeWriter>(
    reference: Reference,
    key: &[u8],
    value: Value,
    store: &mut S,
) -> Result<Reference, Error> {
    match read_node(&reference, store)? {
        Node::Empty => leaf(key, value, store),
        Node::Leaf { path, value: old } => {
            let n = common(key, &path);
            if n == key.len() && n == path.len() {
                return leaf(key, value, store);
            }
            let mut children = Box::new(std::array::from_fn(|_| Reference::Empty));
            let mut branch_value = Value::empty();
            if n == path.len() {
                branch_value = old;
            } else {
                children[usize::from(path[n])] = leaf(&path[n + 1..], old, store)?;
            }
            if n == key.len() {
                branch_value = value;
            } else {
                children[usize::from(key[n])] = leaf(&key[n + 1..], value, store)?;
            }
            let branch = store_node(
                Node::Branch {
                    children,
                    value: branch_value,
                },
                store,
            )?;
            extension(&key[..n], branch, store)
        }
        Node::Extension { path, child } => {
            let n = common(key, &path);
            if n == path.len() {
                let child = put(Reference::Hash(child), &key[n..], value, store)?;
                return extension(&path, child, store);
            }
            let mut children = Box::new(std::array::from_fn(|_| Reference::Empty));
            children[usize::from(path[n])] =
                extension(&path[n + 1..], Reference::Hash(child), store)?;
            let branch_value = if n == key.len() {
                value
            } else {
                children[usize::from(key[n])] = leaf(&key[n + 1..], value, store)?;
                Value::empty()
            };
            let branch = store_node(
                Node::Branch {
                    children,
                    value: branch_value,
                },
                store,
            )?;
            extension(&key[..n], branch, store)
        }
        Node::Branch {
            mut children,
            value: old,
        } => {
            let value = if let Some((first, rest)) = key.split_first() {
                let slot = &mut children[usize::from(*first)];
                *slot = put(slot.clone(), rest, value, store)?;
                old
            } else {
                value
            };
            store_node(Node::Branch { children, value }, store)
        }
    }
}

fn prepend<S: NodeWriter>(
    prefix: &[u8],
    reference: Reference,
    store: &mut S,
) -> Result<Reference, Error> {
    match read_node(&reference, store)? {
        Node::Empty => Ok(Reference::Empty),
        Node::Leaf { path, value } => leaf(&[prefix, &path].concat(), value, store),
        Node::Extension { path, child } => {
            extension(&[prefix, &path].concat(), Reference::Hash(child), store)
        }
        Node::Branch { .. } => extension(prefix, reference, store),
    }
}
fn reduce_branch<S: NodeWriter>(
    children: Box<[Reference; 16]>,
    value: Value,
    store: &mut S,
) -> Result<Reference, Error> {
    let count = children.iter().filter(|r| !r.is_empty()).count() + usize::from(!value.is_empty());
    if count == 0 {
        return Ok(Reference::Empty);
    }
    if count == 1 {
        if !value.is_empty() {
            return leaf(&[], value, store);
        }
        let (index, child) = children
            .iter()
            .enumerate()
            .find(|(_, r)| !r.is_empty())
            .expect("one child");
        return prepend(&[index as u8], child.clone(), store);
    }
    store_node(Node::Branch { children, value }, store)
}
fn delete<S: NodeWriter>(
    reference: Reference,
    key: &[u8],
    store: &mut S,
) -> Result<Reference, Error> {
    match read_node(&reference, store)? {
        Node::Empty => Ok(reference),
        Node::Leaf { path, .. } => {
            if key == path {
                Ok(Reference::Empty)
            } else {
                Ok(reference)
            }
        }
        Node::Extension { path, child } => {
            if !key.starts_with(&path) {
                return Ok(reference);
            }
            let child = delete(Reference::Hash(child), &key[path.len()..], store)?;
            prepend(&path, child, store)
        }
        Node::Branch {
            mut children,
            mut value,
        } => {
            if let Some((first, rest)) = key.split_first() {
                let slot = &mut children[usize::from(*first)];
                *slot = delete(slot.clone(), rest, store)?;
            } else {
                value = Value::empty();
            }
            reduce_branch(children, value, store)
        }
    }
}

#[derive(Debug, Clone)]
pub struct Trie<S = MemoryStore> {
    root: Option<Hash256>,
    store: S,
}
// Savepoints are valid only in the tree/backend that created them. Rolling
// back changes root publication, not append-only content-addressed writes.
#[derive(Debug, Clone, Copy)]
pub struct Savepoint(Option<Hash256>);
impl Default for Trie<MemoryStore> {
    fn default() -> Self {
        Self::new(MemoryStore::default())
    }
}
impl<S: NodeReader> Trie<S> {
    pub fn new(store: S) -> Self {
        Self { root: None, store }
    }
    pub fn from_root(root: Option<Hash256>, store: S) -> Result<Self, Error> {
        if let Some(hash) = root {
            read_node(&Reference::Hash(hash), &store)?;
        }
        Ok(Self { root, store })
    }
    pub fn root_hash(&self) -> Option<Hash256> {
        self.root
    }
    pub fn clear(&mut self) {
        self.root = None;
    }
    pub fn savepoint(&self) -> Savepoint {
        Savepoint(self.root)
    }
    pub fn rollback(&mut self, savepoint: Savepoint) {
        self.root = savepoint.0;
    }
    pub fn store(&self) -> &S {
        &self.store
    }
    pub fn into_store(self) -> S {
        self.store
    }
    fn root_reference(&self) -> Reference {
        self.root.map_or(Reference::Empty, Reference::Hash)
    }
    pub fn get(&self, key: &Nibbles) -> Result<Option<Value>, Error> {
        self.walk(key, None)
    }
    pub fn lookup_with_proof(&self, key: &Nibbles) -> Result<(Option<Value>, MemoryStore), Error> {
        let mut proof = MemoryStore::default();
        let value = self.walk(key, Some(&mut proof))?;
        Ok((value, proof))
    }
    fn walk(
        &self,
        key: &Nibbles,
        mut proof: Option<&mut MemoryStore>,
    ) -> Result<Option<Value>, Error> {
        let mut reference = self.root_reference();
        let mut remaining = key.as_slice();
        loop {
            if let (Some(proof), Reference::Hash(hash)) = (proof.as_deref_mut(), &reference) {
                let bytes = self.store.node(hash)?.ok_or(Error::MissingNode(*hash))?;
                proof.put_node(*hash, bytes)?;
            }
            match read_node(&reference, &self.store)? {
                Node::Empty => return Ok(None),
                Node::Leaf { path, value } => {
                    return Ok((path == remaining).then_some(value));
                }
                Node::Extension { path, child } => {
                    if !remaining.starts_with(&path) {
                        return Ok(None);
                    }
                    remaining = &remaining[path.len()..];
                    reference = Reference::Hash(child);
                }
                Node::Branch { children, value } => {
                    if let Some((first, rest)) = remaining.split_first() {
                        reference = children[usize::from(*first)].clone();
                        remaining = rest;
                    } else {
                        return Ok((!value.is_empty()).then_some(value));
                    }
                }
            }
        }
    }
}
impl<S: NodeWriter> Trie<S> {
    // Publish the new root only after every update succeeds. Content-addressed
    // nodes written before a backend failure may remain unreachable until GC.
    pub fn apply_batch(
        &mut self,
        updates: impl IntoIterator<Item = (Nibbles, Value)>,
    ) -> Result<(), Error> {
        let root = self.root;
        for (key, value) in updates {
            if let Err(error) = self.put(&key, value) {
                self.root = root;
                return Err(error);
            }
        }
        Ok(())
    }
    pub fn put(&mut self, key: &Nibbles, value: Value) -> Result<(), Error> {
        if key.0.is_empty() {
            return Err(Error::EmptyKey);
        }
        if value.is_empty() {
            return self.delete(key);
        }
        let root = put(
            self.root_reference(),
            key.as_slice(),
            value,
            &mut self.store,
        )?;
        self.root = Some(force_hash(root, &mut self.store)?);
        Ok(())
    }
    pub fn delete(&mut self, key: &Nibbles) -> Result<(), Error> {
        let root = delete(self.root_reference(), key.as_slice(), &mut self.store)?;
        let hash = if root.is_empty() {
            None
        } else {
            Some(force_hash(root, &mut self.store)?)
        };
        self.root = hash;
        Ok(())
    }
}

pub fn verify_proof<S: NodeReader>(
    root: Option<Hash256>,
    key: &Nibbles,
    expected: Option<&Value>,
    proof: S,
) -> Result<(), Error> {
    let actual = Trie::from_root(root, proof)?.get(key)?;
    if actual.as_ref() == expected {
        Ok(())
    } else {
        Err(Error::WrongValue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_paths_preserve_odd_nibbles_and_leaf_flags() {
        for raw in [vec![], vec![1], vec![1, 2], vec![1, 2, 3]] {
            for leaf in [false, true] {
                let path = Nibbles::new(raw.clone()).unwrap();
                assert_eq!(
                    decode_compact_path(&compact_path(&path, leaf)),
                    Ok((path, leaf))
                );
            }
        }
        assert!(decode_compact_path(&[0x21]).is_err());
        assert!(decode_compact_path(&[0x40]).is_err());
        assert!(Nibbles::new(vec![16]).is_err());
    }
    #[test]
    fn tiny_root_is_forced_to_hash_and_empty_value_deletes() {
        let mut tree = Trie::default();
        let key = Nibbles::from_bytes(&[1]);
        tree.put(&key, Value::Bytes(vec![2])).unwrap();
        assert_eq!(
            hex::encode(tree.root_hash().unwrap().0),
            "0addd93179631f2545173d56ec7fd76acb76854ae6c8d479f2a5ece469b5354a"
        );
        assert_eq!(tree.store().nodes().len(), 1);
        tree.put(&key, Value::empty()).unwrap();
        assert_eq!(tree.root_hash(), None);
        tree.put(&key, Value::List(vec![])).unwrap();
        assert_eq!(tree.get(&key).unwrap(), Some(Value::List(vec![])));
    }
    #[test]
    fn insertion_order_and_collapse_preserve_roots() {
        let keys = [vec![1], vec![1, 2], vec![1, 2, 3], vec![1, 3], vec![2]];
        let mut a = Trie::default();
        let mut b = Trie::default();
        for key in &keys {
            a.put(
                &Nibbles::new(key.clone()).unwrap(),
                Value::Bytes(key.clone()),
            )
            .unwrap();
        }
        for key in keys.iter().rev() {
            b.put(
                &Nibbles::new(key.clone()).unwrap(),
                Value::Bytes(key.clone()),
            )
            .unwrap();
        }
        assert_eq!(a.root_hash(), b.root_hash());
        for key in &keys {
            a.delete(&Nibbles::new(key.clone()).unwrap()).unwrap();
        }
        assert_eq!(a.root_hash(), None);
    }
    #[test]
    fn presence_absence_and_incomplete_proofs_are_distinct() {
        let mut tree = Trie::default();
        let key = Nibbles::from_bytes(&[1, 2]);
        let value = Value::Bytes(vec![7; 100]);
        tree.put(&key, value.clone()).unwrap();
        let (found, proof) = tree.lookup_with_proof(&key).unwrap();
        assert_eq!(found, Some(value.clone()));
        verify_proof(tree.root_hash(), &key, Some(&value), proof.clone()).unwrap();
        assert_eq!(
            verify_proof(tree.root_hash(), &key, None, proof),
            Err(Error::WrongValue)
        );
        assert!(verify_proof(tree.root_hash(), &key, None, MemoryStore::default()).is_err());
        let absent = Nibbles::from_bytes(&[2]);
        let (found, proof) = tree.lookup_with_proof(&absent).unwrap();
        assert_eq!(found, None);
        verify_proof(tree.root_hash(), &absent, None, proof).unwrap();
    }
}
