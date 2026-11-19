// aec_trees, aec_poi and the *_state_tree binary transports, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use ae_codec::{
    object::{self, FieldType, ObjectTag, Value},
    rlp,
};
use ae_trie::{Nibbles, NodeReader, NodeWriter, Trie};
use std::{cell::Cell, collections::BTreeSet};

mod mtree;
pub mod proof;
mod trees;
pub use proof::{Proof, Proofs};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Object(object::ObjectError),
    Tree(ae_trie::Error),
    Account(crate::accounts::AccountError),
    Contract(crate::contracts::Error),
    Name(crate::names::Error),
    Oracle(crate::oracles::Error),
    InvalidValue,
    AlreadyPresent,
    InvalidExistingAtom(Vec<u8>),
    MissingAtomProfile(Vec<u8>),
    ResourceLimit,
    NotPresent,
    WrongRootHash,
    EmptyAccountsProof,
    EmptyContractsProof,
    BadProof,
    NativeFunctionClause,
    NativeBadarg,
    BadHash(ae_types::Hash256),
    BadValue(ae_trie::Value),
}
impl From<object::ObjectError> for Error {
    fn from(e: object::ObjectError) -> Self {
        match e {
            object::ObjectError::Rlp(rlp::DecodeError::ResourceLimit) => Self::ResourceLimit,
            e => Self::Object(e),
        }
    }
}
impl From<ae_trie::Error> for Error {
    fn from(e: ae_trie::Error) -> Self {
        match e {
            ae_trie::Error::ResourceLimit
            | ae_trie::Error::Decode(rlp::DecodeError::ResourceLimit) => Self::ResourceLimit,
            e => Self::Tree(e),
        }
    }
}

// Local work limits, separate from validity and consensus gas. A caller can
// retry an operation with larger limits without altering canonical output.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_input_bytes: usize,
    pub max_items: usize,
    pub max_depth: usize,
    pub max_entries: usize,
    pub max_key_bytes: usize,
    pub max_work_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_input_bytes: 16 * 1024 * 1024,
            max_items: 1_000_000,
            max_depth: 256,
            max_entries: 100_000,
            max_key_bytes: 256,
            max_work_bytes: 256 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Copy, Default)]
pub struct Config<'a> {
    pub limits: Limits,
    // Erlang name decoding uses binary_to_existing_atom. Only the two atoms
    // guaranteed by aens_names itself are assumed without an explicit profile.
    pub existing_name_status_atoms: Option<&'a BTreeSet<Vec<u8>>>,
}
pub(crate) struct Budget {
    pub(crate) limits: Limits,
    bytes: Cell<usize>,
    entries: Cell<usize>,
}
impl Budget {
    pub(crate) fn envelope<'a>(&self, bytes: &'a [u8]) -> Result<object::Envelope<'a>, Error> {
        self.charge(bytes.len())?;
        Ok(object::decode_envelope_with_limits(
            bytes,
            self.decode_limits(),
        )?)
    }
    pub(crate) fn decode_limits(&self) -> rlp::DecodeLimits {
        rlp::DecodeLimits {
            max_input_bytes: self.limits.max_input_bytes,
            max_items: self.limits.max_items,
            max_depth: self.limits.max_depth,
        }
    }
    pub(crate) fn new(limits: Limits) -> Self {
        Self {
            limits,
            bytes: Cell::new(limits.max_work_bytes),
            entries: Cell::new(limits.max_entries),
        }
    }
    pub(crate) fn charge(&self, bytes: usize) -> Result<(), Error> {
        self.bytes.set(
            self.bytes
                .get()
                .checked_sub(bytes)
                .ok_or(Error::ResourceLimit)?,
        );
        Ok(())
    }
    pub(crate) fn entry(&self, key: &[u8]) -> Result<(), Error> {
        if key.len() > self.limits.max_key_bytes {
            return Err(Error::ResourceLimit);
        }
        self.entries.set(
            self.entries
                .get()
                .checked_sub(1)
                .ok_or(Error::ResourceLimit)?,
        );
        self.charge(key.len().saturating_add(1))
    }
    pub(crate) fn decode(
        &self,
        bytes: &[u8],
        tag: ObjectTag,
        schema: &[FieldType],
    ) -> Result<Vec<Value>, Error> {
        self.charge(bytes.len())?;
        let e = object::decode_envelope_with_limits(
            bytes,
            rlp::DecodeLimits {
                max_input_bytes: self.limits.max_input_bytes,
                max_items: self.limits.max_items,
                max_depth: self.limits.max_depth,
            },
        )?;
        if e.tag != tag {
            return Err(object::ObjectError::WrongTag.into());
        }
        if e.version != version(tag).into() {
            return Err(object::ObjectError::WrongVersion.into());
        }
        if e.fields.len() != schema.len() {
            return Err(object::ObjectError::FieldCount.into());
        }
        e.fields
            .iter()
            .zip(schema)
            .map(|(v, t)| Ok(object::decode_field(t, v)?))
            .collect()
    }
}
pub(crate) fn encode(
    tag: ObjectTag,
    fields: Vec<(&str, FieldType, Value)>,
) -> Result<Vec<u8>, Error> {
    let schema = fields
        .iter()
        .map(|(n, t, _)| object::Field {
            name: (*n).into(),
            kind: t.clone(),
        })
        .collect::<Vec<_>>();
    let fields = fields
        .into_iter()
        .map(|(n, _, v)| (n.into(), v))
        .collect::<Vec<_>>();
    Ok(object::serialize(
        tag,
        &version(tag).into(),
        &schema,
        &fields,
    )?)
}
fn version(tag: ObjectTag) -> u64 {
    match tag {
        ObjectTag::StateTrees => 0,
        _ => 1,
    }
}
pub(crate) fn byte_key(key: &Nibbles) -> Result<Vec<u8>, Error> {
    if !key.as_slice().len().is_multiple_of(2) {
        return Err(Error::InvalidValue);
    }
    Ok(key
        .as_slice()
        .chunks_exact(2)
        .map(|p| (p[0] << 4) | p[1])
        .collect())
}
pub(crate) struct ReadBudget<'a, S> {
    pub(crate) store: &'a S,
    pub(crate) budget: &'a Budget,
}
impl<S: NodeReader> NodeReader for ReadBudget<'_, S> {
    fn verify_node_hashes(&self) -> bool {
        self.store.verify_node_hashes()
    }
    fn decode_limits(&self) -> rlp::DecodeLimits {
        self.budget.decode_limits()
    }
    fn node(&self, h: &ae_types::Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.budget
            .charge(1)
            .map_err(|_| ae_trie::Error::ResourceLimit)?;
        let bytes = self.store.node(h)?;
        if let Some(b) = &bytes {
            self.budget
                .charge(b.len())
                .map_err(|_| ae_trie::Error::ResourceLimit)?;
        }
        Ok(bytes)
    }
}

impl From<crate::accounts::AccountError> for Error {
    fn from(e: crate::accounts::AccountError) -> Self {
        match e {
            crate::accounts::AccountError::Tree(
                ae_trie::Error::ResourceLimit
                | ae_trie::Error::Decode(rlp::DecodeError::ResourceLimit),
            )
            | crate::accounts::AccountError::Object(object::ObjectError::Rlp(
                rlp::DecodeError::ResourceLimit,
            )) => Self::ResourceLimit,
            e => Self::Account(e),
        }
    }
}

impl From<crate::contracts::Error> for Error {
    fn from(e: crate::contracts::Error) -> Self {
        match e {
            crate::contracts::Error::Tree(
                ae_trie::Error::ResourceLimit
                | ae_trie::Error::Decode(rlp::DecodeError::ResourceLimit),
            )
            | crate::contracts::Error::Object(object::ObjectError::Rlp(
                rlp::DecodeError::ResourceLimit,
            )) => Self::ResourceLimit,
            crate::contracts::Error::ResourceLimit => Self::ResourceLimit,
            e => Self::Contract(e),
        }
    }
}

impl From<crate::names::Error> for Error {
    fn from(e: crate::names::Error) -> Self {
        match e {
            crate::names::Error::Tree(
                ae_trie::Error::ResourceLimit
                | ae_trie::Error::Decode(rlp::DecodeError::ResourceLimit),
            )
            | crate::names::Error::Object(object::ObjectError::Rlp(
                rlp::DecodeError::ResourceLimit,
            )) => Self::ResourceLimit,
            e => Self::Name(e),
        }
    }
}

impl From<crate::oracles::Error> for Error {
    fn from(e: crate::oracles::Error) -> Self {
        match e {
            crate::oracles::Error::Tree(
                ae_trie::Error::ResourceLimit
                | ae_trie::Error::Decode(rlp::DecodeError::ResourceLimit),
            )
            | crate::oracles::Error::Object(object::ObjectError::Rlp(
                rlp::DecodeError::ResourceLimit,
            )) => Self::ResourceLimit,
            e => Self::Oracle(e),
        }
    }
}
