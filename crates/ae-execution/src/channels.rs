// aesc_*_tx and aeprimop channel operations, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    Context, Event as Primitive,
    transactions::{Event, Receipt},
};
use ae_protocol::Protocol;
use ae_state::{
    Trees,
    accounts::{Account, Accounts},
    channels::{Channel, Delegates, Nonce},
};
use ae_trie::{NodeReader, NodeWriter};
use ae_tx::{
    ChannelTransaction, SignedTransaction, Transaction, TransactionKind, TransactionLeaf, Wrapper,
    channels::{CreateDelegates, Operation},
};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use std::collections::BTreeMap;

pub mod authentication;
pub mod force_progress;
pub mod offchain;
pub mod unilateral;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeFailure {
    MissingSignedTransaction,
    ChannelKey(&'static str),
    NonceCase(Unsigned),
    CounterpartyKind {
        generalized: bool,
        authenticated: bool,
    },
    FunctionClause,
    MissingAuthenticationStore,
    AuthenticationStoreAlreadyPresent(Vec<u8>),
    LegacyDelegates {
        initiator: Vec<Id>,
        responder: Vec<Id>,
    },
    Badarg,
    OffchainUpdate(&'static str),
    ForceProgressCatch(Vec<u8>),
    IllegalContractVersion(ae_protocol::contracts::Version),
    ProofAccount {
        value: ae_trie::Value,
        error: ae_state::accounts::AccountError,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Profile(ae_protocol::ProfileError),
    Primitive(crate::Error),
    State(ae_state::channels::Error),
    Contract(ae_state::contracts::Error),
    Proof(ae_state::transport::Error),
    Codec(ae_tx::Error),
    Rejected(&'static str),
    Native(NativeFailure),
    Unsupported(TransactionKind),
    ResourceLimit,
}
impl From<crate::Error> for Error {
    fn from(e: crate::Error) -> Self {
        Self::Primitive(e)
    }
}
impl From<ae_state::accounts::AccountError> for Error {
    fn from(e: ae_state::accounts::AccountError) -> Self {
        Self::Primitive(e.into())
    }
}
impl From<ae_state::channels::Error> for Error {
    fn from(e: ae_state::channels::Error) -> Self {
        Self::State(e)
    }
}
impl From<ae_state::contracts::Error> for Error {
    fn from(e: ae_state::contracts::Error) -> Self {
        Self::Contract(e)
    }
}
impl From<ae_tx::Error> for Error {
    fn from(e: ae_tx::Error) -> Self {
        Self::Codec(e)
    }
}
fn reject<T>(reason: &'static str) -> Result<T, Error> {
    Err(Error::Rejected(reason))
}
fn require(ok: bool, reason: &'static str) -> Result<(), Error> {
    if ok { Ok(()) } else { reject(reason) }
}
fn delta(
    events: &mut Vec<Event>,
    account: AccountPublicKey,
    amount: Unsigned,
    debit: bool,
    info: &'static str,
) {
    events.push(Event::Primitive(Primitive::ContractDelta {
        account,
        amount,
        debit,
        info,
    }));
}
fn fee<S: NodeReader>(
    accounts: &Accounts<S>,
    changes: &mut BTreeMap<AccountPublicKey, Account>,
    sender: AccountPublicKey,
    delegated: &Unsigned,
    amount: &Unsigned,
    context: &Context,
    events: &mut Vec<Event>,
) -> Result<(), Error> {
    let payer = context.payer.unwrap_or(sender);
    let payments = if payer == sender {
        vec![(sender, amount.add(delegated))]
    } else if delegated == &0.into() {
        vec![(sender, amount.clone())]
    } else if amount == &0.into() {
        vec![(payer, delegated.clone())]
    } else {
        vec![(sender, amount.clone()), (payer, delegated.clone())]
    };
    for (key, amount) in payments {
        crate::debit(accounts, changes, key, &amount)?;
        events.push(Event::Primitive(Primitive::FeeDebit {
            account: key,
            amount,
        }));
    }
    Ok(())
}
fn earn<S: NodeReader>(
    accounts: &Accounts<S>,
    changes: &mut BTreeMap<AccountPublicKey, Account>,
    key: AccountPublicKey,
    amount: &Unsigned,
    create: bool,
) -> Result<(), Error> {
    let mut a = crate::load(accounts, changes, key, create)?;
    a.earn(amount);
    changes.insert(key, a);
    Ok(())
}
fn party(account: &Account, context: &Context) -> Result<(), Error> {
    if context.protocol < Protocol::Lima {
        return Ok(());
    }
    let generalized = account.authorization().is_some();
    let authenticated = context.ga_authenticated.contains(&account.public_key());
    match (generalized, authenticated) {
        (false, false) | (true, true) => Ok(()),
        (true, false) => reject("ga_using_signature_not_allowed"),
        _ => Err(Error::Native(NativeFailure::CounterpartyKind {
            generalized,
            authenticated,
        })),
    }
}
fn peer(c: &Channel, key: AccountPublicKey) -> Result<(), Error> {
    require(c.peers().contains(&key), "account_not_peer")
}
fn other_party<S: NodeReader>(
    c: &Channel,
    key: AccountPublicKey,
    accounts: &Accounts<S>,
    changes: &mut BTreeMap<AccountPublicKey, Account>,
    context: &Context,
) -> Result<(), Error> {
    let other = if key == c.initiator {
        c.responder
    } else {
        c.initiator
    };
    let account = crate::load(accounts, changes, other, false)?;
    party(&account, context)
}
fn round(c: &Channel, next: &Unsigned) -> Result<(), Error> {
    let min = if c.is_last_state_forced() {
        c.solo_round
            .checked_sub(&1.into())
            .expect("positive solo round")
    } else {
        c.round.clone()
    };
    if next == &min {
        reject("same_round")
    } else {
        require(next > &min, "old_round")
    }
}
fn locked<S: NodeReader>(
    accounts: &Accounts<S>,
    changes: &mut BTreeMap<AccountPublicKey, Account>,
    amount: &Unsigned,
    events: &mut Vec<Event>,
) -> Result<(), Error> {
    if amount != &0.into() {
        earn(accounts, changes, AccountPublicKey::ZERO, amount, true)?;
        delta(
            events,
            AccountPublicKey::ZERO,
            amount.clone(),
            false,
            "Chain.lock",
        );
    }
    Ok(())
}
fn event_view(source: &SignedTransaction<Transaction>) -> ae_tx::TransactionRef<'_> {
    let mut tx = source.transaction().view();
    while matches!(tx.layer().map(|l| l.wrapper()), Some(Wrapper::GaMeta(_))) {
        tx = tx.inner().expect("meta inner");
    }
    tx
}
fn create_event_key(source: Option<&SignedTransaction<Transaction>>) -> Result<[u8; 32], Error> {
    let source = source.ok_or(Error::Native(NativeFailure::MissingSignedTransaction))?;
    let inner = event_view(source);
    if inner.layer().is_some() {
        return Err(Error::Native(NativeFailure::ChannelKey("not_channel_tx")));
    }
    let TransactionLeaf::Channel(t) = inner.leaf() else {
        return Err(Error::Native(NativeFailure::ChannelKey("not_channel_tx")));
    };
    let Operation::Create {
        initiator,
        responder,
        ..
    } = t.operation()
    else {
        return t
            .operation()
            .channel()
            .ok_or(Error::Native(NativeFailure::ChannelKey("not_channel_tx")));
    };
    let mut auth = None;
    let mut view = source.transaction().view();
    while let Some(Wrapper::GaMeta(m)) = view.layer().map(|l| l.wrapper()) {
        if m.account == *initiator {
            auth = Some(m.auth_id().0);
        }
        view = view.inner().expect("meta inner");
    }
    let nonce = t.nonce();
    let n = if nonce == &0.into() {
        Nonce::Hash(auth.ok_or(Error::Native(NativeFailure::ChannelKey(
            "missing_authentication",
        )))?)
    } else if auth.is_none() {
        Nonce::Number(nonce)
    } else {
        return Err(Error::Native(NativeFailure::NonceCase(nonce.clone())));
    };
    Ok(ae_state::channels::channel_id(*initiator, n, *responder).0)
}
fn copy_auth<A: NodeWriter, C: NodeWriter>(
    trees: &mut Trees<A, C>,
    c: &Channel,
    a: &Account,
) -> Result<(), Error> {
    if let Some(auth) = a.authorization() {
        let loaded = trees
            .contracts
            .lookup(auth.contract.value, Default::default())
            .map_err(|e| match e {
                ae_state::contracts::Error::NoSuchStore => {
                    Error::Native(NativeFailure::MissingAuthenticationStore)
                }
                e => Error::Contract(e),
            })?
            .ok_or(Error::Rejected("contract_does_not_exist"))?;
        let contents = loaded
            .store
            .ok_or(Error::Contract(ae_state::contracts::Error::NoSuchStore))?
            .contents()?;
        let target = c
            .auth_store_key(Id::new(IdTag::Account, a.public_key().0))
            .0;
        if let Err(e) = trees.contracts.copy_contents(&contents, target) {
            if e == ae_state::contracts::Error::AlreadyPresent {
                let prefix = ae_state::contracts::store_id(target);
                for key in std::iter::once(prefix.to_vec()).chain(
                    contents
                        .keys()
                        .filter(|k| !k.is_empty())
                        .map(|k| [prefix.as_slice(), k].concat()),
                ) {
                    if trees
                        .contracts
                        .tree()
                        .get(&ae_trie::Nibbles::from_bytes(&key))
                        .map_err(ae_state::contracts::Error::from)?
                        .is_some()
                    {
                        return Err(Error::Native(
                            NativeFailure::AuthenticationStoreAlreadyPresent(key),
                        ));
                    }
                }
            }
            return Err(e.into());
        }
    }
    Ok(())
}

// Source is the original signed envelope retained by the caller's execution
// environment. It may differ from the leaf while wrappers are being evaluated.
pub fn process<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &ChannelTransaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    source: Option<&SignedTransaction<Transaction>>,
    max_envelope_bytes: usize,
) -> Result<Receipt, Error> {
    if !matches!(
        tx.operation(),
        Operation::Create { .. }
            | Operation::Deposit { .. }
            | Operation::Withdraw { .. }
            | Operation::CloseMutual { .. }
            | Operation::Settle { .. }
    ) {
        return Err(Error::Unsupported(tx.kind()));
    }
    crate::check_simple(
        tx.wire_size(),
        tx.fee(),
        tx.ttl(),
        tx.valid_at_protocol(context.protocol)?,
        context,
    )?;
    let event_key = if matches!(tx.operation(), Operation::Create { .. }) {
        create_event_key(source)?
    } else {
        tx.operation().channel().expect("channel key")
    };
    let origin = tx
        .origin()
        .map_err(|_| Error::Native(NativeFailure::FunctionClause))?;
    let mut next = trees.clone();
    let mut changes = BTreeMap::new();
    let mut events = vec![];
    crate::prepare_sender_with_nonce(
        &next.accounts,
        &mut changes,
        origin,
        tx.nonce(),
        context,
        matches!(tx.operation(), Operation::Create { .. }),
    )?;
    match tx.operation() {
        Operation::Create {
            initiator,
            initiator_amount,
            responder,
            responder_amount,
            reserve,
            lock_period,
            delegates,
            state_hash,
        } => {
            fee(
                &next.accounts,
                &mut changes,
                *initiator,
                tx.fee(),
                initiator_amount,
                context,
                &mut events,
            )?;
            fee(
                &next.accounts,
                &mut changes,
                *responder,
                &0.into(),
                responder_amount,
                context,
                &mut events,
            )?;
            require(initiator_amount >= reserve, "insufficient_initiator_amount")?;
            require(responder_amount >= reserve, "insufficient_responder_amount")?;
            require(initiator != responder, "initiator_is_responder")?;
            let ia = crate::load(&next.accounts, &mut changes, *initiator, false)?;
            let ra = crate::load(&next.accounts, &mut changes, *responder, false)?;
            party(&ra, context)?;
            let nonce = match context.ga_nonces.get(initiator) {
                Some(n) => Nonce::Hash(
                    n.as_slice()
                        .try_into()
                        .map_err(|_| Error::Native(NativeFailure::FunctionClause))?,
                ),
                None => Nonce::Number(tx.nonce()),
            };
            let ids =
                |ks: &[AccountPublicKey]| ks.iter().map(|k| Id::new(IdTag::Account, k.0)).collect();
            let delegates = match delegates {
                CreateDelegates::Shared(v) => Delegates::Shared(ids(v)),
                CreateDelegates::Separate {
                    initiator,
                    responder,
                } => Delegates::Separate {
                    initiator: ids(initiator),
                    responder: ids(responder),
                },
            };
            let c = Channel::new(
                &ia,
                initiator_amount.clone(),
                &ra,
                responder_amount.clone(),
                reserve.clone(),
                delegates,
                state_hash.to_vec(),
                lock_period.clone(),
                nonce,
                context.protocol,
                1.into(),
            )?;
            require(
                next.channels.lookup(c.public_key)?.is_none(),
                "channel_exists",
            )?;
            copy_auth(&mut next, &c, &ia)?;
            copy_auth(&mut next, &c, &ra)?;
            next.channels.enter(&c)?;
        }
        Operation::Deposit {
            channel,
            from,
            amount,
            state_hash,
            round: next_round,
        } => {
            fee(
                &next.accounts,
                &mut changes,
                *from,
                tx.fee(),
                amount,
                context,
                &mut events,
            )?;
            let mut c = next
                .channels
                .lookup(*channel)?
                .ok_or(Error::Rejected("channel_does_not_exist"))?;
            require(c.is_active(), "channel_not_active")?;
            peer(&c, *from)?;
            other_party(&c, *from, &next.accounts, &mut changes, context)?;
            round(&c, next_round)?;
            c.deposit(amount, next_round.clone(), state_hash.to_vec());
            next.channels.enter(&c)?;
        }
        Operation::Withdraw {
            channel,
            to,
            amount,
            state_hash,
            round: next_round,
        } => {
            fee(
                &next.accounts,
                &mut changes,
                *to,
                tx.fee(),
                &0.into(),
                context,
                &mut events,
            )?;
            let mut c = next
                .channels
                .lookup(*channel)?
                .ok_or(Error::Rejected("channel_does_not_exist"))?;
            require(c.is_active(), "channel_not_active")?;
            peer(&c, *to)?;
            other_party(&c, *to, &next.accounts, &mut changes, context)?;
            require(
                c.channel_amount >= c.channel_reserve.mul_u64(2).add(amount),
                "not_enough_channel_funds",
            )?;
            round(&c, next_round)?;
            c.withdraw(amount, next_round.clone(), state_hash.to_vec())?;
            earn(&next.accounts, &mut changes, *to, amount, false)?;
            delta(&mut events, *to, amount.clone(), false, "Channel.withdraw");
            next.channels.enter(&c)?;
        }
        Operation::CloseMutual {
            channel,
            initiator_amount,
            responder_amount,
            ..
        } => {
            let c = next
                .channels
                .lookup(*channel)?
                .ok_or(Error::Rejected("channel_does_not_exist"))?;
            peer(&c, origin)?;
            other_party(&c, origin, &next.accounts, &mut changes, context)?;
            if context.protocol < Protocol::Lima {
                require(c.is_active(), "channel_not_active")?;
            }
            let mut total = initiator_amount.add(responder_amount);
            if let Some(payer) = context.payer.filter(|_| tx.fee() != &0.into()) {
                crate::debit(&next.accounts, &mut changes, payer, tx.fee())?;
                delta(&mut events, payer, tx.fee().clone(), true, "Channel.fee");
            } else {
                total = total.add(tx.fee());
            }
            let lock = c
                .channel_amount
                .checked_sub(&total)
                .map_err(|_| Error::Rejected("wrong_amounts"))?;
            // Load both before crediting either, preserving original cache order.
            let mut ia = crate::load(&next.accounts, &mut changes, c.initiator, false)?;
            let mut ra = crate::load(&next.accounts, &mut changes, c.responder, false)?;
            ia.earn(initiator_amount);
            changes.insert(c.initiator, ia);
            delta(
                &mut events,
                c.initiator,
                initiator_amount.clone(),
                false,
                "Channel.amount",
            );
            ra.earn(responder_amount);
            changes.insert(c.responder, ra);
            delta(
                &mut events,
                c.responder,
                responder_amount.clone(),
                false,
                "Channel.amount",
            );
            locked(&next.accounts, &mut changes, &lock, &mut events)?;
            next.channels.delete(*channel)?;
        }
        Operation::Settle {
            channel,
            initiator_amount,
            responder_amount,
            ..
        } => {
            fee(
                &next.accounts,
                &mut changes,
                origin,
                tx.fee(),
                &0.into(),
                context,
                &mut events,
            )?;
            let c = next
                .channels
                .lookup(*channel)?
                .ok_or(Error::Rejected("channel_does_not_exist"))?;
            peer(&c, origin)?;
            require(
                c.is_solo_closed(&context.height.into()),
                "channel_not_closed",
            )?;
            let lock = c
                .channel_amount
                .checked_sub(&initiator_amount.add(responder_amount))
                .map_err(|_| Error::Rejected("insufficient_channel_funds"))?;
            require(initiator_amount == &c.initiator_amount, "wrong_amt")?;
            require(responder_amount == &c.responder_amount, "wrong_amt")?;
            let mut ra = crate::load(&next.accounts, &mut changes, c.responder, false)?;
            let mut ia = crate::load(&next.accounts, &mut changes, c.initiator, false)?;
            ra.earn(responder_amount);
            changes.insert(c.responder, ra);
            ia.earn(initiator_amount);
            changes.insert(c.initiator, ia);
            delta(
                &mut events,
                c.responder,
                responder_amount.clone(),
                false,
                "Channel.settle",
            );
            delta(
                &mut events,
                c.initiator,
                initiator_amount.clone(),
                false,
                "Channel.settle",
            );
            locked(&next.accounts, &mut changes, &lock, &mut events)?;
            next.channels.delete(*channel)?;
        }
        _ => unreachable!(),
    }
    if let Some(source) = source {
        if source
            .signatures()
            .len()
            .checked_mul(64)
            .and_then(|n| n.checked_add(source.transaction().view().wire_size()))
            .is_none_or(|n| n > max_envelope_bytes)
        {
            return Err(Error::ResourceLimit);
        }
        let encoded = source.encode();
        if encoded.len() > max_envelope_bytes {
            return Err(Error::ResourceLimit);
        }
        events.push(Event::Channel {
            channel: event_key,
            transaction_hash: ae_crypto::blake2b_256(&encoded),
            transaction_type: event_view(source).kind(),
        });
    }
    next.accounts
        .apply(&changes.into_values().collect::<Vec<_>>())?;
    events.reverse();
    *trees = next;
    Ok(Receipt {
        transaction_hash: None,
        gas: crate::simple_gas(tx.wire_size()),
        call: None,
        events,
        context: context.clone(),
    })
}
