// aec_vm_chain AENS transactions and delegation, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::{
    state::{append_events, native},
    *,
};
use ae_codec::idna;
use ae_state::names::hashing;
use ae_trie::NodeWriter;
use ae_tx::{NameOperation as O, NameTransaction};
use ae_types::{Id, IdTag, Signature};
use ae_vm_aevm::{
    execution::{Budget, Stop},
    primops::{HostResult, NameWrite},
    runtime::{Fault, Native},
};
fn unsigned(w: Word) -> Unsigned {
    Unsigned::from_field_bytes(&w.unsigned().to_bytes_be()).expect("word magnitude")
}
fn encoding<T>(e: idna::Error) -> HostResult<T, Error> {
    let reason = match e {
        idna::Error::NoRegistrar => "no_registrar",
        idna::Error::RegistrarUnknown => "registrar_unknown",
        idna::Error::MultipleNamespaces => "multiple_namespaces",
        idna::Error::NoLabelInRegistrar => "no_label_in_registrar",
        idna::Error::BadLabel(_) | idna::Error::InvalidCodepoint(_) => "invalid_name",
        idna::Error::ReferenceException { class, reason } => {
            return Err(Fault::Native(Native::ChainException { class, reason }));
        }
    };
    Ok(Err(reason.into()))
}
fn result<T>(e: crate::Error) -> HostResult<T, Error> {
    use crate::{Error as E, names::Failure as F};
    let reason = match e {
        E::Naming(F::Encoding(e)) => return encoding(e),
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
        E::Name(e) => return Err(Fault::Host(Error::Names(e))),
        e => return state::result(e),
    };
    Ok(Err(reason.into()))
}
impl<A: NodeReader, C: NodeReader> Host<A, C> {
    pub fn hash_name(&self, name: &[u8], b: &mut Budget) -> HostResult<Word, Error> {
        b.bytes(name.len())?;
        b.work(name.len().max(1))?;
        let ascii = match idna::to_ascii(name) {
            Ok(a) => a,
            Err(e) => return encoding(e),
        };
        b.bytes(ascii.len())?;
        b.work(ascii.len())?;
        Ok(Ok(Word(hashing::name_hash(&ascii).0)))
    }
    pub fn prepare_name_write(
        &self,
        r: NameWrite<'_>,
        s: &State<A, C>,
        b: &mut Budget,
    ) -> HostResult<(NameTransaction, usize), Error> {
        b.work(1)?;
        if s.onchain.is_some() {
            return Ok(Err("not_allowed_off_chain".into()));
        }
        let account = match r {
            NameWrite::Preclaim { account, .. }
            | NameWrite::Claim { account, .. }
            | NameWrite::Transfer { account, .. }
            | NameWrite::Revoke { account, .. } => AccountPublicKey(account.0),
        };
        let nonce = self.transaction_nonce(account, s, b)?;
        let operation = match r {
            NameWrite::Preclaim { commitment, .. } => O::Preclaim {
                commitment: commitment.0,
            },
            NameWrite::Claim {
                name, salt, fee, ..
            } => {
                b.bytes(name.len())?;
                O::Claim {
                    name: name.to_vec(),
                    salt: unsigned(salt),
                    name_fee: fee.map(unsigned),
                }
            }
            NameWrite::Transfer {
                recipient, hash, ..
            } => O::Transfer {
                name: hash.0,
                recipient: Id::new(IdTag::Account, recipient.0),
            },
            NameWrite::Revoke { hash, .. } => O::Revoke { name: hash.0 },
        };
        let mut tx =
            NameTransaction::new(account, operation).map_err(|_| native("function_clause"))?;
        tx.set_nonce(nonce);
        let size = tx.wire_size();
        b.bytes(size)?;
        Ok(Ok((tx, size)))
    }
    fn name_signature(
        &self,
        tx: &NameTransaction,
        signature: [u8; 64],
        s: &State<A, C>,
        b: &mut Budget,
    ) -> Result<bool, Fault<Error>> {
        let hash = match tx.operation() {
            O::Preclaim { .. } => None,
            O::Claim { name, .. } => match self.hash_name(name, b)? {
                Ok(hash) => Some(hash.0),
                Err(e) => return Err(Fault::Native(Native::BadMatchChainError(e))),
            },
            O::Transfer { name, .. } | O::Revoke { name } => Some(*name),
            O::Update { .. } => return Err(native("function_clause")),
        };
        // Claim's name hashing precedes even the self-delegation bypass.
        if tx.account() == s.account {
            return Ok(true);
        }
        b.work(1)?;
        let Some(account) = s
            .trees
            .accounts
            .lookup(tx.account())
            .map_err(|e| Fault::Host(Error::Accounts(e)))?
        else {
            return Ok(false);
        };
        if account.authorization().is_some() {
            return Ok(false);
        }
        let size = s
            .network_id
            .len()
            .checked_add(if hash.is_some() { 96 } else { 64 })
            .ok_or(Stop::ResourceLimit)?;
        b.bytes(size)?;
        b.work(1)?;
        let mut message = s.network_id.to_vec();
        message.extend(tx.account().0);
        if let Some(hash) = hash {
            message.extend(hash);
        }
        message.extend(s.account.0);
        Ok(ae_crypto::verify_ed25519(
            &tx.account(),
            &message,
            &Signature(signature),
        ))
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> Host<A, C> {
    pub fn apply_name_write(
        &self,
        tx: &NameTransaction,
        signature: [u8; 64],
        s: &State<A, C>,
        b: &mut Budget,
    ) -> HostResult<State<A, C>, Error> {
        b.work(1)?;
        if s.onchain.is_some() {
            return Ok(Err("not_allowed_off_chain".into()));
        }
        if !self.name_signature(tx, signature, s, b)? {
            return Ok(Err("signature_check_failed".into()));
        }
        b.bytes(tx.wire_size())?;
        b.work(tx.wire_size())?;
        let mut next = s.clone();
        let receipt = match crate::names::process(
            tx,
            &mut next.trees.accounts,
            &mut next.trees.names,
            &next.context,
        ) {
            Ok(r) => r,
            Err(e) => return result(e),
        };
        next.events = append_events(s, receipt.events, b)?;
        Ok(Ok(next))
    }
}
