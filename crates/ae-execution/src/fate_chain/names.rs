// aefa_chain_api AENS reads and writes, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{Error, TransferEvent, remote::State};
use crate::{Context, Origin, names as primitive};
use ae_codec::{
    idna,
    name_pointer::{Pointer, PointerValue},
};
use ae_protocol::{Protocol, names as rules};
use ae_state::names::{Auction, Commitment, Name, Names, hashing};
use ae_trie::{NodeReader, NodeWriter};
use ae_tx::{NameOperation, NameTransaction};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use ae_vm_fate::{
    Limits,
    machine::{
        Exception,
        program::remote::{
            HostFailure,
            names::{Pointee, RawPointer, Read, Request},
        },
    },
    types::Version,
    value::{ObjectPayload, Value},
};
use num_bigint::{BigInt, Sign};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub struct Cache {
    pub names: BTreeMap<[u8; 32], Name>,
    pub commitments: BTreeMap<[u8; 32], Commitment>,
    pub auctions: BTreeMap<[u8; 33], Auction>,
}
impl Cache {
    pub(super) fn budget(&self, mut limits: Limits) -> Result<(), HostFailure<Error>> {
        let mut take = |n: usize| {
            limits.max_bytes = limits
                .max_bytes
                .checked_sub(n)
                .ok_or(HostFailure::ResourceLimit)?;
            limits.max_nodes = limits
                .max_nodes
                .checked_sub(1)
                .ok_or(HostFailure::ResourceLimit)?;
            Ok::<_, HostFailure<Error>>(())
        };
        for n in self.names.values() {
            take(n.serialize().map_err(backend)?.len())?;
        }
        for n in self.commitments.values() {
            take(n.serialize().map_err(backend)?.len())?;
        }
        for n in self.auctions.values() {
            take(n.serialize().map_err(backend)?.len())?;
        }
        Ok(())
    }
    fn flush<S: NodeWriter, C: NodeWriter>(
        &self,
        names: &mut Names<S, C>,
    ) -> Result<(), HostFailure<Error>> {
        for n in self.names.values() {
            names.enter_name(n).map_err(backend)?;
        }
        for c in self.commitments.values() {
            names.insert_commitment(c).map_err(backend)?;
        }
        for a in self.auctions.values() {
            names.enter_auction(a).map_err(backend)?;
        }
        Ok(())
    }
}
fn native(s: &'static str) -> HostFailure<Error> {
    HostFailure::Exception(Exception::Native(s))
}
fn backend(e: ae_state::names::Error) -> HostFailure<Error> {
    HostFailure::Backend(Error::Names(e))
}
fn encoding(op: &'static str, e: idna::Error) -> HostFailure<Error> {
    match e {
        idna::Error::ReferenceException {
            class: "exit",
            reason,
        } => HostFailure::Exception(Exception::Exit(reason)),
        idna::Error::ReferenceException { reason, .. } => native(reason),
        error => HostFailure::NameEncoding {
            operation: op,
            error,
        },
    }
}
pub fn hash(op: &'static str, name: &[u8], limits: Limits) -> Result<[u8; 32], HostFailure<Error>> {
    if name.len() > limits.max_bytes || name.len() > limits.max_nodes {
        return Err(HostFailure::ResourceLimit);
    }
    let ascii = idna::to_ascii(name).map_err(|e| {
        // Claim uses aens:get_name_hash, which hides these two IDNA errors.
        if op == "aens_claim"
            && matches!(
                e,
                idna::Error::BadLabel(_) | idna::Error::InvalidCodepoint(_)
            )
        {
            HostFailure::Primop {
                operation: op,
                reason: "invalid_name",
            }
        } else {
            encoding(op, e)
        }
    })?;
    if ascii.len() > limits.max_bytes {
        return Err(HostFailure::ResourceLimit);
    }
    Ok(hashing::name_hash(&ascii).0)
}
pub fn read<S: NodeReader, C: NodeReader>(
    names: &Names<S, C>,
    cache: &mut Cache,
    name: &[u8],
    pointer: Option<&[u8]>,
    limits: Limits,
) -> Result<Option<Read>, HostFailure<Error>> {
    cache.budget(limits)?;
    let key = hash(
        if pointer.is_some() {
            "aens_resolve"
        } else {
            "aens_lookup"
        },
        name,
        limits,
    )?;
    read_hash(names, cache, key, pointer, limits)
}
pub(super) fn read_hash<S: NodeReader, C: NodeReader>(
    names: &Names<S, C>,
    cache: &mut Cache,
    key: [u8; 32],
    pointer: Option<&[u8]>,
    limits: Limits,
) -> Result<Option<Read>, HostFailure<Error>> {
    cache.budget(limits)?;
    let found = match cache.names.get(&key) {
        Some(n) => Some(n.clone()),
        None => names.name(key).map_err(backend)?,
    };
    let Some(n) = found else { return Ok(None) };
    match n.status.as_slice() {
        b"revoked" => return Ok(None),
        b"claimed" => {}
        _ => return Err(native("case_clause")),
    }
    if n.owner.tag != IdTag::Account {
        return Err(native("function_clause"));
    }
    let result = if let Some(key) = pointer {
        let Some(p) = n.pointers.iter().find(|p| p.key == key) else {
            return Ok(None);
        };
        Read::Pointer(p.value.clone())
    } else {
        Read::Name {
            owner: n.owner.value,
            expires: scalar(&n.expires_by),
            pointers: ae_state::names::sanitize_pointers(&n.pointers),
        }
    };
    let mut next = cache.clone();
    next.names.insert(key, n);
    next.budget(limits)?;
    *cache = next;
    Ok(Some(result))
}
fn scalar(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
fn unsigned(n: &BigInt) -> Result<Unsigned, HostFailure<Error>> {
    if n.sign() == Sign::Minus {
        return Err(native("function_clause"));
    }
    Ok(Unsigned::from_field_bytes(&n.to_bytes_be().1).expect("positive magnitude"))
}
fn integer(v: &Value) -> Result<Unsigned, HostFailure<Error>> {
    if let Value::Integer(n) = v {
        unsigned(n)
    } else {
        Err(native("function_clause"))
    }
}
fn fixed(b: &[u8]) -> Result<[u8; 32], HostFailure<Error>> {
    b.try_into().map_err(|_| native("function_clause"))
}
fn fail(op: &'static str, e: crate::Error) -> HostFailure<Error> {
    use crate::{Error as E, names::Failure as F};
    let reason = match e {
        E::Naming(F::Encoding(e)) => return encoding(op, e),
        E::Naming(f) => match f {
            F::CommitmentAlreadyPresent => "commitment_already_present",
            F::CommitmentNotOwned => "commitment_not_owned",
            F::CommitmentDeltaTooSmall => "commitment_delta_too_small",
            F::NameNotPreclaimed => "name_not_preclaimed",
            F::NameAlreadyTaken => "name_already_taken",
            F::NameAlreadyInAuction => "name_already_in_auction",
            F::NameNotInAuction => "name_not_in_auction",
            F::InvalidNameFee => "invalid_name_fee",
            F::IllegalNameFee => "illegal_name_fee",
            F::NameFeeIncrementTooLow => "name_fee_increment_too_low",
            F::InvalidRegistrar => "invalid_registrar",
            F::NameNotOwned => "name_not_owned",
            F::TtlTooHigh => "ttl_too_high",
            F::TtlTooLow => "ttl_too_low",
            F::InvalidPointers => "invalid_pointers",
            F::Encoding(_) => unreachable!(),
        },
        E::Name(ae_state::names::Error::NameNotFound) => "name_does_not_exist",
        E::Name(ae_state::names::Error::NameRevoked) => "name_revoked",
        E::Name(ae_state::names::Error::IllegalSalt) => "illegal_salt_value",
        E::Name(e) => return backend(e),
        E::State(e) => return HostFailure::Backend(Error::Accounts(e)),
        E::AccountNotFound => "account_not_found",
        E::InsufficientFunds => "insufficient_funds",
        E::ReferenceException { reason, .. } => return native(reason),
        e => return HostFailure::Backend(Error::Primitive(Box::new(e))),
    };
    HostFailure::Primop {
        operation: op,
        reason,
    }
}
fn pointers(raw: &[RawPointer]) -> Result<Vec<Pointer>, HostFailure<Error>> {
    raw.iter()
        .map(|p| {
            Ok(Pointer {
                key: p.key.clone(),
                value: match &p.value {
                    Pointee::Data(b) => PointerValue::Data(b.clone()),
                    Pointee::Id(tag, ObjectPayload::Bytes(b)) => PointerValue::Id(Id::new(
                        *tag,
                        b.as_slice()
                            .try_into()
                            .map_err(|_| native("illegal_field"))?,
                    )),
                    _ => return Err(native("illegal_field")),
                },
            })
        })
        .collect()
}
pub fn write<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    state: &mut State<A, C>,
    request: &Request,
    protocol: u32,
    vm: Version,
    limits: Limits,
) -> Result<BigInt, HostFailure<Error>> {
    let op = request.name();
    if state.onchain.is_some() {
        return Err(native("function_clause"));
    }
    let protocol = Protocol::try_from(protocol)
        .map_err(|_| HostFailure::Backend(Error::UnsupportedProtocol(protocol)))?;
    state.name_cache.budget(limits)?;
    let mut next = state.clone();
    let mut base = state.name_base.as_ref().unwrap_or(&state.names).clone();
    next.names = base.clone();
    next.name_cache.flush(&mut next.names)?;
    let context = Context {
        height: next.height,
        protocol,
        origin: Origin::Contract,
        dry_run: next.authentication.dry_run,
        payer: next.authentication.payer,
        ga_authenticated: next.authentication.ga_authenticated.clone(),
        ga_nonces: next.authentication.ga_nonces.clone(),
    };
    let mut dynamic = 0.into();
    let (owner, operation, ttl, info) = match request {
        Request::Preclaim { owner, commitment } => {
            let owner = AccountPublicKey(fixed(owner)?);
            let delta: Unsigned = if vm >= Version::Two {
                rules::PRECLAIM_EXPIRATION.into()
            } else {
                0.into()
            };
            primitive::preclaim(
                &mut next.names,
                owner,
                *commitment,
                delta.clone(),
                next.height,
            )
            .map_err(|e| fail(op, e))?;
            next.name_cache.commitments.insert(
                *commitment,
                next.names
                    .commitment(*commitment)
                    .map_err(backend)?
                    .expect("inserted"),
            );
            (
                owner,
                NameOperation::Preclaim {
                    commitment: *commitment,
                },
                delta,
                "AENS.preclaim",
            )
        }
        Request::Claim {
            owner,
            name,
            salt,
            fee,
        } => {
            let owner = AccountPublicKey(fixed(owner)?);
            let salt = unsigned(salt)?;
            let fee = unsigned(fee)?;
            let mut changes = BTreeMap::new();
            let mut events = Vec::new();
            primitive::process_claim(
                &next.accounts,
                &mut changes,
                &mut next.names,
                &primitive::Claim {
                    owner,
                    name,
                    salt: &salt,
                    fee: &fee,
                },
                &context,
                &mut events,
            )
            .map_err(|e| fail(op, e))?;
            next.accounts
                .apply(&changes.into_values().collect::<Vec<_>>())
                .map_err(|e| HostFailure::Backend(Error::Accounts(e)))?;
            for event in events {
                let (account, amount, debit, info) = match event {
                    crate::Event::NameDebit { account, amount } => {
                        (account, amount, true, "Name.lock")
                    }
                    crate::Event::NameRefund { account, amount } => {
                        (account, amount, false, "Name.refund")
                    }
                    crate::Event::NameFeeLocked { amount } => {
                        (AccountPublicKey::ZERO, amount, false, "Chain.lock")
                    }
                    _ => unreachable!("claim primitive events"),
                };
                next.events.insert(
                    0,
                    TransferEvent::NameDelta {
                        info,
                        account,
                        amount,
                        debit,
                    },
                );
            }
            let ascii = idna::to_ascii(name).map_err(|e| encoding(op, e))?;
            let hash = hashing::name_hash(&ascii).0;
            if let Some(n) = next.names.name(hash).map_err(backend)? {
                next.name_cache.names.insert(hash, n);
            }
            if let Some(a) = next
                .names
                .auction(hashing::auction_hash(hash))
                .map_err(backend)?
            {
                next.name_cache.auctions.insert(a.hash(), a);
            }
            if let Ok(c) = hashing::commitment_hash(&ascii, &salt)
                && next.names.commitment(c.0).map_err(backend)?.is_none()
            {
                next.name_cache.commitments.remove(&c.0);
                base.delete_commitment(c.0).map_err(backend)?;
            }
            dynamic = BigInt::from(name.len());
            (
                owner,
                NameOperation::Claim {
                    name: name.clone(),
                    salt,
                    name_fee: Some(fee),
                },
                rules::PRECLAIM_DELAY.into(),
                "AENS.claim",
            )
        }
        Request::Revoke { owner, hash } => {
            let owner = AccountPublicKey(fixed(owner)?);
            primitive::revoke(&mut next.names, owner, *hash, next.height)
                .map_err(|e| fail(op, e))?;
            next.name_cache.names.insert(
                *hash,
                next.names.name(*hash).map_err(backend)?.expect("updated"),
            );
            (
                owner,
                NameOperation::Revoke { name: *hash },
                0.into(),
                "AENS.revoke",
            )
        }
        Request::Transfer {
            owner,
            hash,
            recipient,
        } => {
            let owner = AccountPublicKey(fixed(owner)?);
            let recipient = Id::new(IdTag::Account, fixed(recipient)?);
            primitive::transfer(&mut next.names, owner, *hash, recipient)
                .map_err(|e| fail(op, e))?;
            next.name_cache.names.insert(
                *hash,
                next.names.name(*hash).map_err(backend)?.expect("updated"),
            );
            (
                owner,
                NameOperation::Transfer {
                    name: *hash,
                    recipient,
                },
                0.into(),
                "AENS.transfer",
            )
        }
        Request::Update {
            owner,
            hash,
            ttl,
            client_ttl,
            pointers: raw,
        } => {
            let owner = AccountPublicKey(fixed(owner)?);
            let ttl = ttl
                .as_ref()
                .map(|t| {
                    Ok(if t.relative {
                        primitive::Expiration::Relative(integer(&t.value)?)
                    } else {
                        primitive::Expiration::Fixed(integer(&t.value)?)
                    })
                })
                .transpose()?;
            let client_ttl = client_ttl.as_ref().map(integer).transpose()?;
            let pointers = raw.as_deref().map(pointers).transpose()?;
            let event_ttl = match &ttl {
                None => 0.into(),
                Some(primitive::Expiration::Relative(n)) => Unsigned::from(next.height).add(n),
                Some(primitive::Expiration::Fixed(n)) => n.clone(),
            };
            let operation = NameOperation::Update {
                name: *hash,
                name_ttl: event_ttl,
                client_ttl: client_ttl.clone().unwrap_or_else(|| 0.into()),
                pointers: pointers.clone().unwrap_or_default(),
            };
            // The internal transaction is constructed before eval_primops.
            NameTransaction::new(owner, operation.clone()).map_err(|_| native("illegal_field"))?;
            primitive::update_name(
                &mut next.names,
                owner,
                *hash,
                &primitive::Update {
                    ttl,
                    client_ttl,
                    pointers,
                },
                &context,
            )
            .map_err(|e| fail(op, e))?;
            next.name_cache.names.insert(
                *hash,
                next.names.name(*hash).map_err(backend)?.expect("updated"),
            );
            (owner, operation, 0.into(), "AENS.update")
        }
    };
    let mut transaction =
        NameTransaction::new(owner, operation).map_err(|_| native("illegal_field"))?;
    transaction.set_ttl(ttl);
    next.events.insert(
        0,
        TransferEvent::NameCall {
            info,
            transaction: if matches!(request, Request::Revoke { .. }) {
                None
            } else {
                Some(transaction)
            },
        },
    );
    next.name_cache.budget(limits)?;
    next.name_base = Some(base.clone());
    next.names = base;
    next.name_cache.flush(&mut next.names)?;
    *state = next;
    Ok(dynamic)
}
