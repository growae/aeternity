// aehttp_helpers:get_info_object_signed_tx and aega_call, aeternity fa2a6d7.
// Copyright (c) 2017-2026, Aeternity developers. ISC: repository LICENSE.
use ae_state::{
    calls::{Call, ReturnType, call_id, ga_call_id},
    contracts::{Nonce, contract_id},
};
use ae_tx::{
    ContractOperation, SignedTransaction, Transaction, TransactionKind, TransactionLeaf,
    TransactionRef, Wrapper,
    channels::{Operation, Update},
};
use ae_types::{AccountPublicKey, Hash256, Id, Unsigned};
use std::{cell::OnceCell, collections::BTreeMap};

pub mod repository;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    WithoutInfo,
    NoStateTrees,
    CallNotFound,
    GarbageCollected,
    InvalidObject,
    ResourceLimit,
    Backend(crate::repository::Error),
}
impl Error {
    fn inner_absence(&self) -> bool {
        matches!(
            self,
            Self::WithoutInfo | Self::NoStateTrees | Self::CallNotFound | Self::GarbageCollected
        )
    }
}
impl From<crate::repository::Error> for Error {
    fn from(error: crate::repository::Error) -> Self {
        Self::Backend(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ga {
    pub caller: Id,
    pub height: Unsigned,
    pub gas_price: Unsigned,
    pub gas_used: Unsigned,
    pub return_value: Vec<u8>,
    pub return_type: ReturnType,
}
impl Ga {
    pub fn from_call(call: Call) -> Result<Self, Error> {
        // aega_call:new requires positive height but does not reject revert.
        if call.height == 0.into() {
            return Err(Error::InvalidObject);
        }
        Ok(Self {
            caller: call.caller,
            height: call.height,
            gas_price: call.gas_price,
            gas_used: call.gas_used,
            return_value: call.return_value,
            return_type: call.return_type,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Info {
    Call(Box<Call>),
    Ga { call: Ga, inner: Box<Info> },
    Type(TransactionKind),
    Undefined,
}
// Original aega_call:set_inner accepts these tagged object tuples only.
pub fn inner(kind: TransactionKind, info: Info) -> Result<Info, Error> {
    match (&info, kind) {
        (Info::Call(call), TransactionKind::ContractCall | TransactionKind::ContractCreate)
            if call
                .call_target
                .as_ref()
                .is_none_or(|target| target.len() == 32) =>
        {
            Ok(info)
        }
        (Info::Ga { .. }, TransactionKind::GaMeta) => Ok(info),
        _ => Err(Error::InvalidObject),
    }
}
pub trait Lookup {
    fn contract(&mut self, contract: [u8; 32], id: Hash256, block: Hash256) -> Result<Call, Error>;
    fn ga(&mut self, owner: AccountPublicKey, auth: Hash256, block: Hash256) -> Result<Ga, Error>;
}
// Operational limits, independent of transaction or block validity.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub layers: usize,
    pub work_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            layers: 64,
            work_bytes: 64 << 20,
        }
    }
}
struct Selection<'a, P> {
    original: &'a SignedTransaction<Transaction>,
    original_hash: OnceCell<Hash256>,
    outer_auth: Option<Hash256>,
    block: Hash256,
    provider: &'a mut P,
}
pub fn select(
    transaction: &SignedTransaction<Transaction>,
    block: Hash256,
    provider: &mut impl Lookup,
    limits: Limits,
) -> Result<Info, Error> {
    let tx = transaction.transaction().view();
    if tx.layers().len() > limits.layers.min(256) {
        return Err(Error::ResourceLimit);
    }
    let mut remaining = limits.work_bytes;
    let mut charge = |n| {
        remaining = remaining.checked_sub(n).ok_or(Error::ResourceLimit)?;
        Ok::<_, Error>(())
    };
    charge(tx.leaf().wire_size())?;
    charge(
        transaction
            .signatures()
            .len()
            .checked_mul(128)
            .ok_or(Error::ResourceLimit)?,
    )?;
    for layer in tx.layers() {
        charge(layer.wire_size())?;
    }
    // Preserve the original meta_auth_id callback: its paying_for clause does
    // not match the actual paying_for_tx atom. Only an outer GA supplies this ID.
    let outer_auth = match tx.layer().map(|l| l.wrapper()) {
        Some(Wrapper::GaMeta(meta)) => Some(meta.auth_id()),
        _ => None,
    };
    Selection {
        original: transaction,
        original_hash: OnceCell::new(),
        outer_auth,
        block,
        provider,
    }
    .walk(tx, &mut BTreeMap::new())
}
impl<P: Lookup> Selection<'_, P> {
    fn walk(
        &mut self,
        tx: TransactionRef<'_>,
        ga_ids: &mut BTreeMap<AccountPublicKey, Hash256>,
    ) -> Result<Info, Error> {
        if let Some(layer) = tx.layer() {
            let inner = tx.inner().expect("wrapper has inner transaction");
            return match layer.wrapper() {
                Wrapper::PayingFor(_) => self.walk(inner, ga_ids),
                Wrapper::GaMeta(meta) => {
                    let auth = meta.auth_id();
                    let call = self.provider.ga(meta.account, auth, self.block)?;
                    if call.height == 0.into() {
                        return Err(Error::InvalidObject);
                    }
                    let result = if call.return_type == ReturnType::Ok {
                        ga_ids.insert(meta.account, auth);
                        match self.walk(inner, ga_ids) {
                            Err(e) if e.inner_absence() => Info::Type(inner.kind()),
                            Err(e) => return Err(e),
                            Ok(info) => crate::transaction_info::inner(inner.kind(), info)?,
                        }
                    } else {
                        Info::Type(inner.kind())
                    };
                    Ok(Info::Ga {
                        call,
                        inner: Box::new(result),
                    })
                }
            };
        }
        let (target, id) = match tx.leaf() {
            TransactionLeaf::Contract(contract) => {
                let target = match contract.operation() {
                    ContractOperation::Call { contract, .. } => contract.value,
                    ContractOperation::Create { .. } => {
                        contract_id(
                            contract.signer(),
                            self.outer_auth
                                .map_or(Nonce::Number(contract.nonce()), |h| Nonce::Hash(h.0)),
                        )
                        .0
                    }
                };
                let id = self.outer_auth.map_or_else(
                    || call_id(contract.actor().value, contract.nonce(), target),
                    |h| ga_call_id(h.0, target),
                );
                (target, id)
            }
            TransactionLeaf::GaAttach(attach) => {
                if self.outer_auth.is_some() {
                    return Err(Error::InvalidObject);
                }
                let target = contract_id(attach.owner(), Nonce::Number(attach.nonce())).0;
                (target, call_id(attach.owner().0, attach.nonce(), target))
            }
            TransactionLeaf::Channel(channel) => {
                let Operation::ForceProgress {
                    round,
                    update: Update::CallContract { caller, .. },
                    ..
                } = channel.operation()
                else {
                    return if channel.kind() == TransactionKind::ChannelForceProgress {
                        Err(Error::InvalidObject)
                    } else {
                        Err(Error::WithoutInfo)
                    };
                };
                let target = self.original_hash.get_or_init(|| self.original.hash()).0;
                let id = ga_ids.get(caller).map_or_else(
                    || call_id(caller.0, round, target),
                    |h| ga_call_id(h.0, target),
                );
                (target, id)
            }
            _ => return Err(Error::WithoutInfo),
        };
        self.provider
            .contract(target, id, self.block)
            .map(|call| Info::Call(Box::new(call)))
    }
}
