// Aeternity fa2a6d7, aeo_*_tx callbacks, aeprimop and aeo_state_tree.
// Copyright (c) 2017, 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{Context, Origin};
use crate::{Error, Event};
use ae_protocol::{Protocol, oracles::state_gas};
use ae_state::{accounts::Accounts, oracles::Oracles};
use ae_trie::NodeWriter;
use ae_tx::OracleTransaction;
use ae_types::Unsigned;

pub mod abi;

pub fn gas(tx: &OracleTransaction, context: &Context) -> Unsigned {
    gas_for_size(tx, context, tx.wire_size())
}
pub fn minimum_fee(tx: &OracleTransaction, context: &Context) -> Unsigned {
    let size = if context.origin == Origin::Generalized && context.protocol < Protocol::Iris {
        0
    } else {
        tx.wire_size()
    };
    gas_for_size(tx, context, size).mul_u64(context.protocol.minimum_gas_price())
}
fn gas_for_size(tx: &OracleTransaction, context: &Context, size: usize) -> Unsigned {
    let Ok(delta) = tx.operation().state_ttl().delta(&context.height.into()) else {
        return 0.into();
    };
    crate::simple_gas(size).add(&state_gas(&delta))
}

pub fn prune<S: NodeWriter, O: NodeWriter, C: NodeWriter>(
    accounts: &mut Accounts<S>,
    oracles: &mut Oracles<O, C>,
    next_height: u64,
) -> Result<Vec<Event>, Error> {
    accounts.transaction(|accounts| {
        let mut events = Vec::new();
        oracles.prune_with_refunds(next_height, |refund| -> Result<(), Error> {
            let mut account = accounts
                .lookup(refund.account)?
                .ok_or(Error::OracleRefundAccountMissing(refund.account))?;
            // Refunds ignore payability and retain account nonce/GA metadata.
            account.earn(&refund.amount);
            accounts.enter(&account)?;
            if refund.emit_event {
                events.push(Event::OracleRefund {
                    account: refund.account,
                    amount: refund.amount,
                });
            }
            Ok(())
        })?;
        events.reverse();
        Ok(events)
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    TooLowAbsoluteTtl,
    TooLowHeight,
    AlreadyOracle,
    NotActiveOracle,
    ZeroExtensionTtl,
    OracleDoesNotExist,
    QueryFeeTooLow,
    TooLongTtl,
    NamedQueryUnavailable,
    QueryAlreadyPresent,
    IllegalQuerySpec,
    NoMatchingQuery,
    WrongResponseTtl,
    WrongOracle,
    QueryClosed,
}
impl From<Failure> for Error {
    fn from(e: Failure) -> Self {
        Self::OracleTransaction(e)
    }
}
impl From<abi::Error> for Error {
    fn from(e: abi::Error) -> Self {
        match e {
            abi::Error::StoredAevmFormat(_) => Self::ReferenceException {
                class: "error",
                reason: "badmatch",
            },
            abi::Error::UnknownStoredAbi => Self::ReferenceException {
                class: "error",
                reason: "case_clause",
            },
            _ => Self::OracleAbi(e),
        }
    }
}

pub fn execute_signed<
    S: NodeWriter,
    O: NodeWriter,
    C: NodeWriter,
    N: ae_state::names::NamePointerResolver,
>(
    signed: &ae_tx::SignedTransaction<OracleTransaction>,
    accounts: &mut Accounts<S>,
    oracles: &mut Oracles<O, C>,
    names: &N,
    network: &ae_protocol::NetworkProfile,
    context: &Context,
) -> Result<crate::Receipt, Error> {
    let account = signed.transaction().account();
    let signers = if context.ga_authenticated.contains(&account) {
        vec![]
    } else {
        vec![account]
    };
    signed.verify_signers(&signers, network, context.protocol, context.payer.is_some())?;
    let mut receipt = process(signed.transaction(), accounts, oracles, names, context)?;
    receipt.transaction_hash = Some(signed.hash());
    Ok(receipt)
}

pub fn process<
    S: NodeWriter,
    O: NodeWriter,
    C: NodeWriter,
    N: ae_state::names::NamePointerResolver,
>(
    tx: &OracleTransaction,
    accounts: &mut Accounts<S>,
    oracles: &mut Oracles<O, C>,
    names: &N,
    context: &Context,
) -> Result<crate::Receipt, Error> {
    process_with_limits(
        tx,
        accounts,
        oracles,
        names,
        context,
        abi::Limits::default(),
    )
}

pub fn process_with_limits<
    S: NodeWriter,
    O: NodeWriter,
    C: NodeWriter,
    N: ae_state::names::NamePointerResolver,
>(
    tx: &OracleTransaction,
    accounts: &mut Accounts<S>,
    oracles: &mut Oracles<O, C>,
    names: &N,
    context: &Context,
    limits: abi::Limits,
) -> Result<crate::Receipt, Error> {
    use ae_state::oracles::{Oracle, Query, ga_query_id};
    use ae_tx::OracleOperation;
    use ae_types::{Id, IdTag};
    if context.origin != Origin::Contract {
        let min = minimum_fee(tx, context);
        if min == 0.into() {
            return Err(Failure::TooLowAbsoluteTtl.into());
        }
        if tx.fee() < &min {
            return Err(Error::TooLowFee);
        }
        if tx.ttl() != &0.into() && tx.ttl() < &context.height.into() {
            return Err(Error::TtlExpired);
        }
        if !tx.valid_at_protocol(context.protocol) {
            return Err(Error::InvalidAtProtocol);
        }
    }
    // The callback resolves absolute TTLs and constructs all operations before
    // evaluating nonce or fee primitives (including the response ID guard).
    let delta = tx
        .operation()
        .state_ttl()
        .delta(&context.height.into())
        .map_err(|_| Failure::TooLowHeight)?;
    let response_id = if let OracleOperation::Response { query_id, .. } = tx.operation() {
        Some(<[u8; 32]>::try_from(query_id.as_slice()).map_err(|_| function_clause())?)
    } else {
        None
    };
    accounts.transaction(|accounts| {
        oracles.transaction(|oracles| {
            let actor = tx.account();
            let mut changes = std::collections::BTreeMap::new();
            let mut events = Vec::new();
            // Response semantics precede nonce/fee checks. Its state write remains
            // inside both savepoints so any later failure restores all roots.
            let response_fee = if let OracleOperation::Response {
                response,
                response_ttl,
                ..
            } = tx.operation()
            {
                let mut query = oracles
                    .query(actor.0, response_id.expect("response ID checked"))?
                    .ok_or(Failure::NoMatchingQuery)?;
                if &query.response_ttl != response_ttl {
                    return Err(Failure::WrongResponseTtl.into());
                }
                let oracle = oracles
                    .oracle(actor.0)?
                    .ok_or(Failure::OracleDoesNotExist)?;
                if query.oracle.tag != IdTag::Oracle {
                    return Err(function_clause());
                }
                if query.oracle.value != actor.0 {
                    return Err(Failure::WrongOracle.into());
                }
                abi::validate_content(
                    &oracle.abi_version,
                    &oracle.response_format,
                    response,
                    context.protocol,
                    limits,
                )?;
                if !query.is_open() {
                    return Err(Failure::QueryClosed.into());
                }
                query.add_response(&context.height.into(), Some(response.clone()));
                oracles.enter_query(&query)?;
                Some(query.fee)
            } else {
                None
            };
            crate::prepare_sender_with_nonce(
                accounts,
                &mut changes,
                actor,
                tx.nonce(),
                context,
                matches!(tx.operation(), OracleOperation::Query { .. }),
            )?;
            let query_fee = match tx.operation() {
                OracleOperation::Query { query_fee, .. } => query_fee.clone(),
                _ => 0.into(),
            };
            spend_fees(
                accounts,
                &mut changes,
                actor,
                context.payer.unwrap_or(actor),
                tx.fee(),
                &query_fee,
                &mut events,
            )?;
            match tx.operation() {
                OracleOperation::Register {
                    query_format,
                    response_format,
                    query_fee,
                    abi_version,
                    ..
                } => {
                    if oracles.oracle(actor.0)?.is_some() {
                        return Err(Failure::AlreadyOracle.into());
                    }
                    abi::validate_registration(
                        abi_version,
                        query_format,
                        response_format,
                        context.protocol,
                        limits,
                    )?;
                    oracles.enter_oracle(&Oracle {
                        public_key: actor.0,
                        query_format: query_format.clone(),
                        response_format: response_format.clone(),
                        query_fee: query_fee.clone(),
                        expires_by: Unsigned::from(context.height).add(&delta),
                        abi_version: abi_version.clone(),
                    })?;
                }
                OracleOperation::Extend { delta } => {
                    if delta == &0.into() {
                        return Err(Failure::ZeroExtensionTtl.into());
                    }
                    let mut oracle = oracles.oracle(actor.0)?.ok_or(Failure::NotActiveOracle)?;
                    oracle.expires_by = oracle.expires_by.add(delta);
                    oracles.enter_oracle(&oracle)?;
                }
                OracleOperation::Query {
                    oracle: ident,
                    query,
                    query_fee,
                    response_ttl,
                    ..
                } => {
                    let oracle_key = if ident.tag == IdTag::Name {
                        if context.protocol < Protocol::Iris {
                            return Err(Failure::NamedQueryUnavailable.into());
                        }
                        names.resolve_id(ident.value, b"oracle_pubkey")?.value
                    } else {
                        ident.value
                    };
                    let oracle = oracles
                        .oracle(oracle_key)?
                        .ok_or(Failure::OracleDoesNotExist)?;
                    if query_fee < &oracle.query_fee {
                        return Err(Failure::QueryFeeTooLow.into());
                    }
                    let expires_by = Unsigned::from(context.height).add(&delta);
                    if expires_by.add(response_ttl) > oracle.expires_by {
                        return Err(Failure::TooLongTtl.into());
                    }
                    abi::validate_content(
                        &oracle.abi_version,
                        &oracle.query_format,
                        query,
                        context.protocol,
                        limits,
                    )?;
                    let mut query = Query::new(
                        *ident,
                        actor,
                        tx.nonce().clone(),
                        query.clone(),
                        query_fee.clone(),
                        expires_by,
                        response_ttl.clone(),
                    )
                    .map_err(|e| match e {
                        ae_state::oracles::Error::ZeroResponseTtl => {
                            Error::from(Failure::IllegalQuerySpec)
                        }
                        _ => e.into(),
                    })?;
                    query.oracle = Id::new(IdTag::Oracle, oracle_key);
                    if let Some(nonce) = context.ga_nonces.get(&actor) {
                        query.id = ga_query_id(
                            nonce.as_slice().try_into().map_err(|_| function_clause())?,
                            oracle_key,
                        )
                        .0;
                    }
                    if oracles.query(oracle_key, query.id)?.is_some() {
                        return Err(Failure::QueryAlreadyPresent.into());
                    }
                    oracles.enter_query(&query)?;
                }
                OracleOperation::Response { .. } => {
                    let amount = response_fee.expect("response checked before nonce and fee");
                    let mut account = crate::load(accounts, &mut changes, actor, false)?;
                    account.earn(&amount);
                    changes.insert(actor, account);
                    events.push(Event::OracleQueryFee {
                        account: actor,
                        amount,
                    });
                }
            }
            accounts.apply(&changes.into_values().collect::<Vec<_>>())?;
            events.reverse();
            Ok(crate::Receipt {
                gas: gas(tx, context),
                fee: tx.fee().clone(),
                transaction_hash: None,
                events,
            })
        })
    })
}
fn function_clause() -> Error {
    Error::ReferenceException {
        class: "error",
        reason: "function_clause",
    }
}
fn spend_fees<S: ae_trie::NodeReader>(
    accounts: &Accounts<S>,
    changes: &mut std::collections::BTreeMap<
        ae_types::AccountPublicKey,
        ae_state::accounts::Account,
    >,
    actor: ae_types::AccountPublicKey,
    payer: ae_types::AccountPublicKey,
    fee: &Unsigned,
    escrow: &Unsigned,
    events: &mut Vec<Event>,
) -> Result<(), Error> {
    let debits = if actor == payer {
        vec![(actor, fee.add(escrow))]
    } else if fee == &0.into() {
        vec![(actor, escrow.clone())]
    } else if escrow == &0.into() {
        vec![(payer, fee.clone())]
    } else {
        vec![(actor, escrow.clone()), (payer, fee.clone())]
    };
    for (account, amount) in debits {
        crate::debit(accounts, changes, account, &amount)?;
        events.push(Event::FeeDebit { account, amount });
    }
    Ok(())
}
