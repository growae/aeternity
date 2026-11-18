#![forbid(unsafe_code)]
// Behavioral reference: aeternity fa2a6d7, aetx.erl and aeprimop.erl.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use ae_protocol::{NetworkProfile, Protocol};
use ae_state::{
    accounts::{Account, AccountError, Accounts},
    names::{self as name_state, NameResolver},
};
use ae_trie::{NodeReader, NodeWriter};
use ae_tx::{SignedTransaction, Spend};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

pub mod names;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Transaction,
    Contract,
    Generalized,
}

// Authentication entries and payer identity must come from already validated
// wrapper/VM execution, never from caller-supplied HTTP transaction fields.
#[derive(Debug, Clone)]
pub struct Context {
    pub height: u64,
    pub protocol: Protocol,
    pub origin: Origin,
    pub dry_run: bool,
    pub payer: Option<AccountPublicKey>,
    pub ga_authenticated: BTreeSet<AccountPublicKey>,
    pub ga_nonces: BTreeMap<AccountPublicKey, Vec<u8>>,
}
impl Context {
    pub fn transaction(height: u64, protocol: Protocol) -> Self {
        Self {
            height,
            protocol,
            origin: Origin::Transaction,
            dry_run: false,
            payer: None,
            ga_authenticated: BTreeSet::new(),
            ga_nonces: BTreeMap::new(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    State(AccountError),
    Name(name_state::Error),
    Signature(ae_tx::Error),
    Naming(names::Failure),
    InvalidAtProtocol,
    BadTransaction,
    TooLowFee,
    TtlExpired,
    AccountNotFound,
    NotBasicAccount,
    NotGeneralizedAccount,
    GeneralizedAccountsUnavailable,
    GaNonceMustBeZero,
    BadGaEnvironment,
    NonceAlreadyUsed,
    NonceTooHigh,
    InsufficientFunds,
    AccountNotPayable,
    ReferenceException {
        class: &'static str,
        reason: &'static str,
    },
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<AccountError> for Error {
    fn from(e: AccountError) -> Self {
        Self::State(e)
    }
}
impl From<name_state::Error> for Error {
    fn from(e: name_state::Error) -> Self {
        match e {
            name_state::Error::PointerNotId => Self::ReferenceException {
                class: "error",
                reason: "function_clause",
            },
            _ => Self::Name(e),
        }
    }
}
impl From<ae_tx::Error> for Error {
    fn from(e: ae_tx::Error) -> Self {
        Self::Signature(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    NameDebit {
        account: AccountPublicKey,
        amount: Unsigned,
    },
    NameRefund {
        account: AccountPublicKey,
        amount: Unsigned,
    },
    NameFeeLocked {
        amount: Unsigned,
    },
    Spend {
        from: AccountPublicKey,
        to: AccountPublicKey,
        amount: Unsigned,
    },
    FeeDebit {
        account: AccountPublicKey,
        amount: Unsigned,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub gas: Unsigned,
    pub fee: Unsigned,
    pub transaction_hash: Option<Hash256>,
    // New events in aetx_env order (most recent first).
    pub events: Vec<Event>,
}
fn simple_gas(size: usize) -> Unsigned {
    Unsigned::from(size as u64).mul_u64(20).add(&15000.into())
}
fn simple_minimum_fee(size: usize, context: &Context) -> Unsigned {
    let size = if context.origin == Origin::Generalized && context.protocol < Protocol::Iris {
        0
    } else {
        size
    };
    simple_gas(size).mul_u64(context.protocol.minimum_gas_price())
}
pub fn spend_gas(tx: &Spend) -> Unsigned {
    simple_gas(tx.wire_size())
}
pub fn spend_minimum_fee(tx: &Spend, context: &Context) -> Unsigned {
    simple_minimum_fee(tx.wire_size(), context)
}
fn check_simple(
    size: usize,
    fee: &Unsigned,
    ttl: &Unsigned,
    valid_at_protocol: bool,
    context: &Context,
) -> Result<(), Error> {
    if context.origin != Origin::Contract {
        if fee < &simple_minimum_fee(size, context) {
            return Err(Error::TooLowFee);
        }
        if ttl != &0.into() && ttl < &context.height.into() {
            return Err(Error::TtlExpired);
        }
        if !valid_at_protocol {
            return Err(Error::InvalidAtProtocol);
        }
    }
    Ok(())
}

pub fn execute_signed_spend<S: NodeWriter, N: NameResolver>(
    signed: &SignedTransaction<Spend>,
    accounts: &mut Accounts<S>,
    names: &N,
    network: &NetworkProfile,
    context: &Context,
) -> Result<Receipt, Error> {
    let sender = signed.transaction().sender();
    let signers = if context.ga_authenticated.contains(&sender) {
        vec![]
    } else {
        vec![sender]
    };
    // The reference's dont_verify_signature option is independent of dry_run.
    // Simulation callers can explicitly use process_spend after their checks.
    signed.verify_signers(&signers, network, context.protocol, context.payer.is_some())?;
    let mut receipt = process_spend(signed.transaction(), accounts, names, context)?;
    receipt.transaction_hash = Some(signed.hash());
    Ok(receipt)
}

// Entry point shared with VM-created spends. Signature/GA authentication is the
// enclosing execution layer's responsibility. All mutations are staged; neither
// account roots nor events are published on any semantic or backend failure.
pub fn process_spend<S: NodeWriter, N: NameResolver>(
    tx: &Spend,
    accounts: &mut Accounts<S>,
    names: &N,
    context: &Context,
) -> Result<Receipt, Error> {
    check_simple(tx.wire_size(), tx.fee(), tx.ttl(), true, context)?;
    let mut changes = BTreeMap::new();
    let sender = tx.sender();
    prepare_sender(accounts, &mut changes, sender, tx.nonce(), context)?;
    let recipient = if tx.recipient().tag == ae_types::IdTag::Name {
        names.resolve_account(tx.recipient().value)?
    } else {
        AccountPublicKey(tx.recipient().value)
    };
    let payer = context.payer.unwrap_or(sender);
    debit(accounts, &mut changes, payer, tx.fee())?;
    debit(accounts, &mut changes, sender, tx.amount())?;
    let mut receiver = load(accounts, &mut changes, recipient, true)?;
    if !receiver.is_payable() {
        return Err(Error::AccountNotPayable);
    }
    receiver.earn(tx.amount());
    changes.insert(recipient, receiver);
    accounts.apply(&changes.into_values().collect::<Vec<_>>())?;
    Ok(Receipt {
        gas: spend_gas(tx),
        fee: tx.fee().clone(),
        transaction_hash: None,
        events: vec![
            Event::Spend {
                from: sender,
                to: recipient,
                amount: tx.amount().clone(),
            },
            Event::FeeDebit {
                account: payer,
                amount: tx.fee().clone(),
            },
        ],
    })
}
fn prepare_sender<S: NodeReader>(
    accounts: &Accounts<S>,
    changes: &mut BTreeMap<AccountPublicKey, Account>,
    sender: AccountPublicKey,
    nonce: &Unsigned,
    context: &Context,
) -> Result<(), Error> {
    let mut account = load(accounts, changes, sender, context.payer.is_some())?;
    if context.ga_authenticated.contains(&sender) {
        if context.protocol < Protocol::Fortuna {
            return Err(Error::GeneralizedAccountsUnavailable);
        }
        if account.authorization().is_none() {
            return Err(Error::NotGeneralizedAccount);
        }
        if nonce != &0.into() {
            return Err(Error::GaNonceMustBeZero);
        }
        if !context.ga_nonces.contains_key(&sender) {
            return Err(Error::BadGaEnvironment);
        }
    } else if context.dry_run && account.authorization().is_some() {
        if nonce != &0.into() {
            return Err(Error::GaNonceMustBeZero);
        }
    } else {
        if account.authorization().is_some() {
            return Err(Error::NotBasicAccount);
        }
        if nonce <= account.nonce() {
            return Err(Error::NonceAlreadyUsed);
        }
        if nonce != &account.nonce().add(&1.into()) {
            return Err(Error::NonceTooHigh);
        }
        if context.origin != Origin::Contract || context.protocol == Protocol::Roma {
            account.set_nonce(nonce.clone());
        }
    }
    changes.insert(sender, account);
    Ok(())
}
fn load<S: NodeReader>(
    accounts: &Accounts<S>,
    changes: &mut BTreeMap<AccountPublicKey, Account>,
    key: AccountPublicKey,
    create: bool,
) -> Result<Account, Error> {
    if let Some(account) = changes.get(&key) {
        return Ok(account.clone());
    }
    if let Some(account) = accounts.lookup(key)? {
        return Ok(account);
    }
    if create {
        Ok(Account::new(key, 0.into()))
    } else {
        Err(Error::AccountNotFound)
    }
}
fn debit<S: NodeReader>(
    accounts: &Accounts<S>,
    changes: &mut BTreeMap<AccountPublicKey, Account>,
    key: AccountPublicKey,
    amount: &Unsigned,
) -> Result<(), Error> {
    let mut account = load(accounts, changes, key, false)?;
    if account.balance() < amount {
        return Err(Error::InsufficientFunds);
    }
    account.spend_without_nonce_bump(amount)?;
    changes.insert(key, account);
    Ok(())
}
