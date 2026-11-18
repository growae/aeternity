use crate::{Error, Event};
use ae_protocol::Protocol;
use ae_state::{
    accounts::{Account, Accounts},
    names::Names,
};
use ae_trie::NodeWriter;
use ae_types::AccountPublicKey;

// Called once per key-block height, in the pre-block transformation sequence.
// Accounts, naming objects and their expiry index roll back together on error.
pub fn prune<S: NodeWriter, N: NodeWriter, C: NodeWriter>(
    accounts: &mut Accounts<S>,
    names: &mut Names<N, C>,
    next_height: u64,
    protocol: Protocol,
) -> Result<Vec<Event>, Error> {
    accounts.transaction(|accounts| {
        names.transaction(|names| {
            let locks = names.prune(next_height, protocol)?;
            let mut events = Vec::new();
            if !locks.is_empty() {
                let key = AccountPublicKey::ZERO;
                let mut locked = accounts
                    .lookup(key)?
                    .unwrap_or_else(|| Account::new(key, 0.into()));
                for fee in locks {
                    locked.earn(&fee.amount);
                    events.push(Event::NameFeeLocked { amount: fee.amount });
                }
                accounts.enter(&locked)?;
            }
            events.reverse();
            Ok(events)
        })
    })
}

use crate::{
    Context, Receipt, check_simple, debit, load, prepare_sender, simple_gas, simple_minimum_fee,
};
use ae_codec::{idna, name_pointer::Pointer};
use ae_protocol::{NetworkProfile, names as rules};
use ae_state::names::{self as state, Auction, Commitment, Name, NameResolver, hashing};
use ae_trie::NodeReader;
use ae_tx::{NameOperation, NameTransaction, SignedTransaction};
use ae_types::{Id, IdTag, Unsigned};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Encoding(idna::Error),
    CommitmentAlreadyPresent,
    CommitmentNotOwned,
    CommitmentDeltaTooSmall,
    NameNotPreclaimed,
    NameAlreadyTaken,
    NameAlreadyInAuction,
    NameNotInAuction,
    InvalidNameFee,
    IllegalNameFee,
    NameFeeIncrementTooLow,
    InvalidRegistrar,
    NameNotOwned,
    TtlTooHigh,
    TtlTooLow,
    InvalidPointers,
}
impl From<Failure> for Error {
    fn from(e: Failure) -> Self {
        Self::Naming(e)
    }
}
fn ascii_error(e: idna::Error) -> Error {
    match e {
        idna::Error::ReferenceException { class, reason } => {
            Error::ReferenceException { class, reason }
        }
        _ => Failure::Encoding(e).into(),
    }
}
pub fn minimum_fee(tx: &NameTransaction, context: &Context) -> Unsigned {
    simple_minimum_fee(tx.wire_size(), context)
}
pub fn execute_signed<S: NodeWriter, N: NodeWriter, C: NodeWriter>(
    signed: &SignedTransaction<NameTransaction>,
    accounts: &mut Accounts<S>,
    names: &mut Names<N, C>,
    network: &NetworkProfile,
    context: &Context,
) -> Result<Receipt, Error> {
    let account = signed.transaction().account();
    let signers = if context.ga_authenticated.contains(&account) {
        vec![]
    } else {
        vec![account]
    };
    signed.verify_signers(&signers, network, context.protocol, context.payer.is_some())?;
    let mut receipt = process(signed.transaction(), accounts, names, context)?;
    receipt.transaction_hash = Some(signed.hash());
    Ok(receipt)
}
pub fn process<S: NodeWriter, N: NodeWriter, C: NodeWriter>(
    tx: &NameTransaction,
    accounts: &mut Accounts<S>,
    names: &mut Names<N, C>,
    context: &Context,
) -> Result<Receipt, Error> {
    check_simple(
        tx.wire_size(),
        tx.fee(),
        tx.ttl(),
        tx.valid_at_protocol(context.protocol),
        context,
    )?;
    // Claim's callback checks its fee-layout version before evaluating primops.
    let claim_fee = match tx.operation() {
        NameOperation::Claim { name_fee: None, .. } if context.protocol < Protocol::Lima => {
            Some(rules::LEGACY_LOCKED_FEE.into())
        }
        NameOperation::Claim {
            name_fee: Some(fee),
            ..
        } if context.protocol >= Protocol::Lima => Some(fee.clone()),
        NameOperation::Claim { .. } => return Err(Error::BadTransaction),
        _ => None,
    };
    accounts.transaction(|accounts| {
        names.transaction(|names| {
            let mut changes = BTreeMap::new();
            let account = tx.account();
            prepare_sender(accounts, &mut changes, account, tx.nonce(), context)?;
            let payer = crate::fee_payer(account, tx.fee(), context);
            debit(accounts, &mut changes, payer, tx.fee())?;
            let mut events = vec![Event::FeeDebit {
                account: payer,
                amount: tx.fee().clone(),
            }];
            match tx.operation() {
                NameOperation::Preclaim { commitment } => {
                    if names.commitment(*commitment)?.is_some() {
                        return Err(Failure::CommitmentAlreadyPresent.into());
                    }
                    names.insert_commitment(&Commitment::new(
                        *commitment,
                        account,
                        rules::PRECLAIM_EXPIRATION.into(),
                        context.height.into(),
                    ))?;
                }
                NameOperation::Claim { name, salt, .. } => {
                    let claim = Claim {
                        owner: account,
                        name,
                        salt,
                        fee: claim_fee.as_ref().expect("claim version checked"),
                    };
                    process_claim(accounts, &mut changes, names, &claim, context, &mut events)?;
                }
                NameOperation::Revoke { name } => {
                    let mut record = owned_name(names, account, *name)?;
                    record.status = b"revoked".to_vec();
                    record.expires_by =
                        Unsigned::from(context.height).add(&rules::PROTECTION_PERIOD.into());
                    names.enter_name(&record)?;
                }
                NameOperation::Transfer { name, recipient } => {
                    let mut record = owned_name(names, account, *name)?;
                    let recipient = if recipient.tag == IdTag::Name {
                        names.resolve_account(recipient.value)?
                    } else {
                        AccountPublicKey(recipient.value)
                    };
                    record.owner = Id::new(IdTag::Account, recipient.0);
                    names.enter_name(&record)?;
                }
                NameOperation::Update {
                    name,
                    name_ttl,
                    client_ttl,
                    pointers,
                } => update_name(
                    names,
                    account,
                    *name,
                    &Update {
                        ttl: Some(Expiration::Relative(name_ttl.clone())),
                        client_ttl: Some(client_ttl.clone()),
                        pointers: Some(pointers.clone()),
                    },
                    context,
                )?,
            }
            accounts.apply(&changes.into_values().collect::<Vec<_>>())?;
            events.reverse();
            Ok(Receipt {
                gas: simple_gas(tx.wire_size()),
                fee: tx.fee().clone(),
                transaction_hash: None,
                events,
            })
        })
    })
}
fn owned_name<N: NodeReader, C: NodeReader>(
    names: &Names<N, C>,
    owner: AccountPublicKey,
    hash: [u8; 32],
) -> Result<Name, Error> {
    let name = names.name(hash)?.ok_or(state::Error::NameNotFound)?;
    check_owner(&name, owner)?;
    check_claimed(&name)?;
    Ok(name)
}
fn check_owner(name: &Name, owner: AccountPublicKey) -> Result<(), Error> {
    if name.owner.tag != IdTag::Account {
        return Err(Error::ReferenceException {
            class: "error",
            reason: "function_clause",
        });
    }
    if name.owner.value != owner.0 {
        return Err(Failure::NameNotOwned.into());
    }
    Ok(())
}
fn check_claimed(name: &Name) -> Result<(), Error> {
    match name.status.as_slice() {
        b"claimed" => Ok(()),
        b"revoked" => Err(state::Error::NameRevoked.into()),
        _ => Err(Error::ReferenceException {
            class: "error",
            reason: "case_clause",
        }),
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expiration {
    Relative(Unsigned),
    Fixed(Unsigned),
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Update {
    pub ttl: Option<Expiration>,
    pub client_ttl: Option<Unsigned>,
    pub pointers: Option<Vec<Pointer>>,
}
// FATE uses omitted fields to retain TTLs and sanitize legacy pointers. The VM
// layer must authenticate the owner and compose this with its enclosing journal.
pub fn update_name<N: NodeWriter, C: NodeWriter>(
    names: &mut Names<N, C>,
    owner: AccountPublicKey,
    hash: [u8; 32],
    update: &Update,
    context: &Context,
) -> Result<(), Error> {
    let height = Unsigned::from(context.height);
    let requested = match &update.ttl {
        Some(Expiration::Relative(delta)) => {
            if delta > &rules::max_expiration(context.protocol).into() {
                return Err(Failure::TtlTooHigh.into());
            }
            Some(height.add(delta))
        }
        Some(Expiration::Fixed(abs)) => {
            let delta = abs.checked_sub(&height).map_err(|_| Failure::TtlTooLow)?;
            if delta > rules::max_expiration(context.protocol).into() {
                return Err(Failure::TtlTooHigh.into());
            }
            Some(abs.clone())
        }
        None => None,
    };
    let mut record = names.name(hash)?.ok_or(state::Error::NameNotFound)?;
    let expiry = requested.unwrap_or_else(|| record.expires_by.clone());
    if expiry < height {
        return Err(Failure::TtlTooLow.into());
    }
    check_owner(&record, owner)?;
    check_claimed(&record)?;
    let pointers = match &update.pointers {
        None => state::sanitize_pointers(&record.pointers),
        Some(pointers) => {
            if !state::pointers_valid_at(pointers, context.protocol) {
                return Err(Failure::InvalidPointers.into());
            }
            pointers.clone()
        }
    };
    record.expires_by = expiry;
    record.client_ttl = update.client_ttl.clone().unwrap_or(record.client_ttl);
    record.pointers = pointers;
    names.enter_name(&record)?;
    Ok(())
}
struct Claim<'a> {
    owner: AccountPublicKey,
    name: &'a [u8],
    salt: &'a Unsigned,
    fee: &'a Unsigned,
}
fn process_claim<S: NodeReader, N: NodeWriter, C: NodeWriter>(
    accounts: &Accounts<S>,
    changes: &mut BTreeMap<AccountPublicKey, Account>,
    names: &mut Names<N, C>,
    claim: &Claim<'_>,
    context: &Context,
    events: &mut Vec<Event>,
) -> Result<(), Error> {
    let ascii = idna::to_ascii(claim.name).map_err(ascii_error)?;
    let registrar = claim
        .name
        .split(|b| *b == b'.')
        .rfind(|p| !p.is_empty())
        .expect("validated registrar");
    // Governance normalizes the already-normalized name again. A mapped
    // separator can therefore pass the first call and fail this pattern match.
    let size = if context.protocol >= Protocol::Lima {
        let normalized = idna::to_ascii(&ascii).map_err(|_| Error::ReferenceException {
            class: "error",
            reason: "badmatch",
        })?;
        let domain = normalized
            .split(|b| *b == b'.')
            .rfind(|p| !p.is_empty())
            .expect("validated name");
        normalized.len() - domain.len() - 1
    } else {
        0
    };
    let hash = hashing::name_hash(&ascii).0;
    let auction_hash = hashing::auction_hash(hash);
    let timeout = rules::bid_timeout(size, context.protocol, None);
    debit(accounts, changes, claim.owner, claim.fee)?;
    events.push(Event::NameDebit {
        account: claim.owner,
        amount: claim.fee.clone(),
    });
    if (context.protocol >= Protocol::Lima && claim.fee == &0.into())
        || (context.protocol < Protocol::Lima
            && (claim.fee != &rules::LEGACY_LOCKED_FEE.into() || timeout != 0))
    {
        return Err(Failure::IllegalNameFee.into());
    }
    if timeout == 0 {
        credit(accounts, changes, AccountPublicKey::ZERO, claim.fee, true)?;
        events.push(Event::NameFeeLocked {
            amount: claim.fee.clone(),
        });
    }
    if names.name(hash)?.is_some() {
        return Err(Failure::NameAlreadyTaken.into());
    }
    let zero_salt = claim.salt == &0.into();
    if timeout == 0 {
        if context.protocol < Protocol::Ceres || !zero_salt {
            claim_after_preclaim(names, claim, &ascii, registrar, size, context)?;
        }
        names.enter_name(&Name::new(
            hash,
            claim.owner,
            Unsigned::from(context.height).add(&rules::max_expiration(context.protocol).into()),
        ))?;
    } else if zero_salt {
        match names.auction(auction_hash)? {
            None if context.protocol >= Protocol::Ceres => names.enter_auction(&Auction::new(
                hash,
                claim.owner,
                claim.fee.clone(),
                timeout.into(),
                context.height.into(),
            ))?,
            None => return Err(Failure::NameNotInAuction.into()),
            Some(previous) => {
                minimum_name_fee(size, claim.fee, context.protocol)?;
                if !rules::valid_overbid(claim.fee, &previous.bid) {
                    return Err(Failure::NameFeeIncrementTooLow.into());
                }
                if previous.bidder.tag != IdTag::Account {
                    return Err(Error::ReferenceException {
                        class: "error",
                        reason: "function_clause",
                    });
                }
                let bidder = AccountPublicKey(previous.bidder.value);
                credit(accounts, changes, bidder, &previous.bid, false)?;
                events.push(Event::NameRefund {
                    account: bidder,
                    amount: previous.bid.clone(),
                });
                names.enter_auction(&previous.extend(
                    claim.owner,
                    claim.fee.clone(),
                    rules::bid_extension(size, context.protocol, None).into(),
                    context.height.into(),
                ))?;
            }
        }
    } else {
        if names.auction(auction_hash)?.is_some() {
            return Err(Failure::NameAlreadyInAuction.into());
        }
        claim_after_preclaim(names, claim, &ascii, registrar, size, context)?;
        names.enter_auction(&Auction::new(
            hash,
            claim.owner,
            claim.fee.clone(),
            timeout.into(),
            context.height.into(),
        ))?;
    }
    Ok(())
}
fn claim_after_preclaim<N: NodeWriter, C: NodeWriter>(
    names: &mut Names<N, C>,
    claim: &Claim<'_>,
    ascii: &[u8],
    registrar: &[u8],
    size: usize,
    context: &Context,
) -> Result<(), Error> {
    let hash = hashing::commitment_hash(ascii, claim.salt)?.0;
    let commitment = names.commitment(hash)?.ok_or(Failure::NameNotPreclaimed)?;
    if commitment.owner.tag != IdTag::Account {
        return Err(Error::ReferenceException {
            class: "error",
            reason: "function_clause",
        });
    }
    if commitment.owner.value != claim.owner.0 {
        return Err(Failure::CommitmentNotOwned.into());
    }
    if commitment.created.add(&rules::PRECLAIM_DELAY.into()) > context.height.into() {
        return Err(Failure::CommitmentDeltaTooSmall.into());
    }
    if names.name(hashing::name_hash(ascii).0)?.is_some() {
        return Err(Failure::NameAlreadyTaken.into());
    }
    let expected = if context.protocol < Protocol::Lima {
        b"test".as_slice()
    } else {
        b"chain".as_slice()
    };
    if registrar != expected {
        return Err(Failure::InvalidRegistrar.into());
    }
    minimum_name_fee(size, claim.fee, context.protocol)?;
    names.delete_commitment(hash)?;
    Ok(())
}
fn minimum_name_fee(size: usize, fee: &Unsigned, protocol: Protocol) -> Result<(), Error> {
    if protocol < Protocol::Lima {
        return Ok(());
    }
    let size = size.try_into().map_err(|_| Error::ReferenceException {
        class: "error",
        reason: "function_clause",
    })?;
    if fee < &rules::claim_fee(size, protocol) {
        Err(Failure::InvalidNameFee.into())
    } else {
        Ok(())
    }
}
fn credit<S: NodeReader>(
    accounts: &Accounts<S>,
    changes: &mut BTreeMap<AccountPublicKey, Account>,
    key: AccountPublicKey,
    amount: &Unsigned,
    create: bool,
) -> Result<(), Error> {
    let mut account = load(accounts, changes, key, create)?;
    account.earn(amount);
    changes.insert(key, account);
    Ok(())
}
