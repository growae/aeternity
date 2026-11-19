// FATE oracle chain hosts, aefa_chain_api.erl / aeprimop.erl fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{Error, TransferEvent, oracle, remote::State};
use crate::{
    Context, Origin,
    oracles::{abi, primitives},
};
use ae_protocol::{
    Protocol,
    oracles::{Ttl as ChainTtl, state_gas},
};
use ae_state::oracles::Oracle;
use ae_trie::NodeWriter;
use ae_tx::{OracleOperation, OracleTransaction};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use ae_vm_fate::{
    Limits,
    machine::{
        Exception,
        program::remote::{
            HostFailure,
            oracle_write::{Request, ResultValue, Ttl},
        },
    },
    value::{ObjectKind, ObjectPayload, Value},
};
use num_bigint::{BigInt, Sign};
use std::collections::BTreeMap;
fn native(s: &'static str) -> HostFailure<Error> {
    HostFailure::Exception(Exception::Native(s))
}
fn error(op: &'static str, reason: &'static str) -> HostFailure<Error> {
    HostFailure::Primop {
        operation: op,
        reason,
    }
}
fn primitive(op: &'static str, e: crate::Error) -> HostFailure<Error> {
    use crate::{Error as E, oracles::Failure as F};
    let reason = match e {
        E::AccountNotFound => "account_not_found",
        E::NotBasicAccount => "not_a_basic_account",
        E::NotGeneralizedAccount => "not_a_generalized_account",
        E::GeneralizedAccountsUnavailable => "generalize_accounts_not_available_at_protocol",
        E::GaNonceMustBeZero => "nonce_in_ga_tx_should_be_zero",
        E::BadGaEnvironment => "bad_ga_tx_env",
        E::NonceAlreadyUsed => "tx_nonce_already_used_for_account",
        E::NonceTooHigh => "tx_nonce_too_high_for_account",
        E::InsufficientFunds => "insufficient_funds",
        E::ReferenceException { reason, .. } => return native(reason),
        E::OracleTransaction(f) => match f {
            F::AlreadyOracle => "account_is_already_an_oracle",
            F::NotActiveOracle => "account_is_not_an_active_oracle",
            F::ZeroExtensionTtl => "zero_relative_oracle_extension_ttl",
            F::OracleDoesNotExist => "oracle_does_not_exist",
            F::QueryFeeTooLow => "query_fee_too_low",
            F::TooLongTtl => "too_long_ttl",
            F::QueryAlreadyPresent => "oracle_query_already_present",
            F::IllegalQuerySpec => "illegal_oracle_query_spec",
            F::NoMatchingQuery => "no_matching_oracle_query",
            F::WrongResponseTtl => "oracle_response_has_wrong_response_ttl",
            F::WrongOracle => "oracle_does_not_match_query_id",
            F::QueryClosed => "oracle_closed_for_response",
            _ => return HostFailure::Backend(Error::Primitive(Box::new(E::OracleTransaction(f)))),
        },
        E::OracleAbi(a) => match a {
            abi::Error::BadAbiVersion => "bad_abi_version",
            abi::Error::BadQueryFormat => "bad_query_format",
            abi::Error::BadResponseFormat => "bad_response_format",
            abi::Error::BadFormat => "bad_format",
            abi::Error::ResourceLimit => return HostFailure::ResourceLimit,
            abi::Error::StoredFateFormat(ae_vm_fate::Error::TrailingBytes) => {
                return native("badmatch");
            }
            abi::Error::StoredFateFormat(_) => return native("function_clause"),
            _ => return HostFailure::Backend(Error::Primitive(Box::new(E::OracleAbi(a)))),
        },
        E::State(e) => return HostFailure::Backend(Error::Accounts(e)),
        E::Oracle(e) => return HostFailure::Backend(Error::Oracles(e)),
        e => return HostFailure::Backend(Error::Primitive(Box::new(e))),
    };
    error(op, reason)
}
fn unsigned(n: &BigInt) -> Result<Unsigned, HostFailure<Error>> {
    if n.sign() == Sign::Minus {
        return Err(native("function_clause"));
    }
    Ok(Unsigned::from_field_bytes(&n.to_bytes_be().1).expect("unsigned magnitude"))
}
fn fixed(key: &[u8]) -> Result<[u8; 32], HostFailure<Error>> {
    key.try_into().map_err(|_| native("function_clause"))
}
fn relative(ttl: &Ttl, height: u64, op: &'static str) -> Result<Unsigned, HostFailure<Error>> {
    let delta = if ttl.relative {
        if ttl.value < 0.into() {
            return Err(error(op, "negative_ttl"));
        }
        ttl.value.clone()
    } else {
        if ttl.value < height.into() {
            return Err(error(op, "too_low_ttl"));
        }
        &ttl.value - BigInt::from(height)
    };
    // aefa_chain_api:ttl_gas only accepts a strictly positive delta.
    if delta == 0.into() {
        return Err(native("function_clause"));
    }
    unsigned(&delta)
}
fn ttl_wire(ttl: &Ttl) -> Result<ChainTtl, HostFailure<Error>> {
    Ok(if ttl.relative {
        ChainTtl::Delta(unsigned(&ttl.value)?)
    } else {
        ChainTtl::Block(unsigned(&ttl.value)?)
    })
}
fn gas(delta: &Unsigned, size: usize) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, state_gas(delta).as_bytes()) + size
}
fn object(kind: ObjectKind, key: [u8; 32]) -> Value {
    Value::Object(kind, ObjectPayload::Bytes(key.to_vec()))
}
fn event(
    account: [u8; 32],
    operation: OracleOperation,
    nonce: Unsigned,
    info: &'static str,
) -> TransferEvent {
    let mut transaction = OracleTransaction::new(AccountPublicKey(account), operation)
        .expect("oracle host ID types are valid");
    transaction.set_nonce(nonce);
    TransferEvent::OracleCall { info, transaction }
}
fn encode_term<A: NodeWriter, C: NodeWriter>(
    state: &mut State<A, C>,
    key: &[u8],
    q: &Value,
    r: &Value,
    content: &Value,
    op: &'static str,
    limits: Limits,
) -> Result<(Oracle, Vec<u8>), HostFailure<Error>> {
    let oracle = oracle::lookup(&state.oracles, &mut state.oracle_reads, key)?;
    if let Some(reason) = oracle::check_types(oracle.as_ref(), q, r, limits)? {
        return Err(error(op, reason));
    }
    let oracle = oracle.expect("type-matched oracle");
    let bytes = if oracle.abi_version == 0.into() {
        if let Value::String(b) = content {
            if b.len() > limits.max_bytes {
                return Err(HostFailure::ResourceLimit);
            }
            b.clone()
        } else {
            return Err(error(op, "no_vm_oracles_needs_strings"));
        }
    } else {
        oracle::encode(content, limits)?
    };
    Ok((oracle, bytes))
}
pub(super) fn execute<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    state: &mut State<A, C>,
    request: &Request,
    protocol: u32,
    limits: Limits,
) -> Result<ResultValue, HostFailure<Error>> {
    let op = request.name();
    let protocol = Protocol::try_from(protocol)
        .map_err(|_| HostFailure::Backend(Error::UnsupportedProtocol(protocol)))?;
    if state.onchain.is_some() && !matches!(request, Request::Register { .. }) {
        return Err(native("function_clause"));
    }
    state.oracle_reads.budget(limits)?;
    let mut next = state.clone();
    let base = state.oracle_base.as_ref().unwrap_or(&state.oracles).clone();
    next.oracle_base = Some(base.clone());
    next.oracles = base.clone();
    // Read overlays may contain canonicalized legacy objects. The same values
    // must be visible to primitives and retained by final publication.
    for o in next.oracle_reads.oracles.values() {
        next.oracles
            .enter_oracle(o)
            .map_err(|e| HostFailure::Backend(Error::Oracles(e)))?;
    }
    for q in next.oracle_reads.queries.values() {
        next.oracles
            .enter_query(q)
            .map_err(|e| HostFailure::Backend(Error::Oracles(e)))?;
    }
    let context = Context {
        height: next.height,
        protocol,
        origin: Origin::Contract,
        dry_run: next.authentication.dry_run,
        payer: next.authentication.payer,
        ga_authenticated: next.authentication.ga_authenticated.clone(),
        ga_nonces: next.authentication.ga_nonces.clone(),
        ga_transaction: next.auth_tx.clone(),
        ga_transaction_hash: next
            .environment
            .auth_tx_hash
            .as_ref()
            .and_then(|h| h.as_slice().try_into().ok())
            .map(ae_types::Hash256),
    };
    let abi_limits = abi::Limits {
        fate: limits,
        ..Default::default()
    };
    let result = match request {
        Request::Register {
            key,
            query_format,
            response_format,
            fee,
            ttl,
        } => {
            let delta = relative(ttl, next.height, op)?;
            let dynamic = gas(
                &delta,
                query_format
                    .len()
                    .checked_add(response_format.len())
                    .ok_or(HostFailure::ResourceLimit)?,
            );
            let key = fixed(key)?;
            let fee = unsigned(fee)?;
            let oracle = primitives::register(
                &mut next.oracles,
                Oracle {
                    public_key: key,
                    query_format: query_format.clone(),
                    response_format: response_format.clone(),
                    query_fee: fee.clone(),
                    expires_by: Unsigned::from(next.height).add(&delta),
                    abi_version: 3.into(),
                },
                &context,
                abi_limits,
            )
            .map_err(|e| primitive(op, e))?;
            next.oracle_reads.oracles.insert(key, oracle);
            next.events.insert(
                0,
                event(
                    key,
                    OracleOperation::Register {
                        query_format: query_format.clone(),
                        response_format: response_format.clone(),
                        query_fee: fee,
                        oracle_ttl: ttl_wire(ttl)?,
                        abi_version: 3.into(),
                    },
                    0.into(),
                    "Oracle.register",
                ),
            );
            ResultValue {
                value: Some(object(ObjectKind::Oracle, key)),
                gas: dynamic,
            }
        }
        Request::Extend { key, ttl } => {
            let delta = relative(ttl, next.height, op)?;
            let key = fixed(key)?;
            let oracle =
                primitives::extend(&mut next.oracles, key, &delta).map_err(|e| primitive(op, e))?;
            next.oracle_reads.oracles.insert(key, oracle);
            next.events.insert(
                0,
                event(
                    key,
                    OracleOperation::Extend {
                        delta: unsigned(&ttl.value)?,
                    },
                    0.into(),
                    "Oracle.extend",
                ),
            );
            ResultValue {
                value: None,
                gas: gas(&delta, 0),
            }
        }
        Request::Query {
            key,
            sender,
            content,
            fee,
            query_ttl,
            response_ttl,
            question_type,
            response_type,
        } => {
            // Relative TTL conversion precedes ABI lookup, but its strictly
            // positive gas helper runs only after the sender lookup.
            let delta = relative_query(query_ttl, next.height, op)?;
            let (oracle, content) = encode_term(
                &mut next,
                key,
                question_type,
                response_type,
                content,
                op,
                limits,
            )?;
            let sender = AccountPublicKey(*sender);
            let account = next
                .accounts
                .lookup(sender)
                .map_err(|e| HostFailure::Backend(Error::Accounts(e)))?
                .ok_or_else(|| {
                    HostFailure::Exception(Exception::Host {
                        operation: "get_account",
                        detail: "account_not_found".into(),
                    })
                })?;
            let nonce = account.nonce().add(&1.into());
            if delta == 0.into() {
                return Err(native("function_clause"));
            }
            let dynamic = gas(&delta, content.len());
            let key = fixed(key)?;
            let fee = unsigned(fee)?;
            // The internal transaction is constructed before the query op;
            // its integer serializer rejects a negative response TTL first.
            if response_ttl.sign() == Sign::Minus {
                return Err(native("illegal_field"));
            }
            let response_ttl = unsigned(response_ttl)?;
            let mut changes = BTreeMap::new();
            crate::prepare_sender_with_nonce(
                &next.accounts,
                &mut changes,
                sender,
                &nonce,
                &context,
                true,
            )
            .map_err(|e| primitive(op, e))?;
            let payer = crate::fee_payer(sender, &fee, &context);
            let mut account = crate::load(&next.accounts, &mut changes, payer, false)
                .map_err(|e| primitive(op, e))?;
            if account.balance() < &fee {
                return Err(error(op, "insufficient_funds"));
            }
            account
                .spend_without_nonce_bump(&fee)
                .map_err(|e| HostFailure::Backend(Error::Accounts(e)))?;
            changes.insert(payer, account);
            next.accounts
                .apply(&changes.into_values().collect::<Vec<_>>())
                .map_err(|e| HostFailure::Backend(Error::Accounts(e)))?;
            next.events.insert(
                0,
                TransferEvent::OracleDelta {
                    info: "Chain.fee",
                    account: payer,
                    debit: true,
                    amount: fee.clone(),
                },
            );
            next.events.insert(
                0,
                event(
                    sender.0,
                    OracleOperation::Query {
                        oracle: Id::new(IdTag::Oracle, key),
                        query: content.clone(),
                        query_fee: fee.clone(),
                        query_ttl: ttl_wire(query_ttl)?,
                        response_ttl: response_ttl.clone(),
                    },
                    nonce.clone(),
                    "Oracle.query",
                ),
            );
            let query = primitives::query(
                &mut next.oracles,
                primitives::NewQuery {
                    ident: Id::new(IdTag::Oracle, key),
                    oracle: key,
                    sender,
                    nonce: &nonce,
                    content: &content,
                    fee: &fee,
                    delta: &delta,
                    response_ttl: &response_ttl,
                },
                &context,
                abi_limits,
            )
            .map_err(|e| primitive(op, e))?;
            let id = query.id;
            next.oracle_reads.oracles.insert(key, oracle);
            next.oracle_reads.queries.insert((key, id), query);
            ResultValue {
                value: Some(object(ObjectKind::OracleQuery, id)),
                gas: dynamic,
            }
        }
        Request::Respond {
            key,
            query,
            content,
            question_type,
            response_type,
        } => {
            let (oracle, content) = encode_term(
                &mut next,
                key,
                question_type,
                response_type,
                content,
                op,
                limits,
            )?;
            let key = fixed(key)?;
            let id = fixed(query)?;
            let query = primitives::respond(
                &mut next.oracles,
                key,
                id,
                &content,
                None,
                &context,
                abi_limits,
            )
            .map_err(|e| primitive(op, e))?;
            let mut account = next
                .accounts
                .lookup(AccountPublicKey(key))
                .map_err(|e| HostFailure::Backend(Error::Accounts(e)))?
                .ok_or_else(|| error(op, "account_not_found"))?;
            account.earn(&query.fee);
            next.accounts
                .enter(&account)
                .map_err(|e| HostFailure::Backend(Error::Accounts(e)))?;
            next.events.insert(
                0,
                TransferEvent::OracleDelta {
                    info: "Oracle.queryfee",
                    account: AccountPublicKey(key),
                    debit: false,
                    amount: query.fee.clone(),
                },
            );
            if query.response_ttl == 0.into() {
                return Err(native("function_clause"));
            }
            let dynamic = gas(&query.response_ttl, content.len());
            next.events.insert(
                0,
                event(
                    key,
                    OracleOperation::Response {
                        query_id: id.to_vec(),
                        response: content,
                        response_ttl: query.response_ttl.clone(),
                    },
                    0.into(),
                    "Oracle.respond",
                ),
            );
            next.oracle_reads.oracles.insert(key, oracle);
            next.oracle_reads.queries.insert((key, id), query);
            ResultValue {
                value: None,
                gas: dynamic,
            }
        }
    };
    // Intermediate versions exist only in the primop overlay. Materialize
    // final records against the invocation's original tree so superseded TTLs
    // from earlier host calls do not leak into the expiry cache.
    next.oracle_reads.budget(limits)?;
    next.oracles = base;
    for o in next.oracle_reads.oracles.values() {
        next.oracles
            .enter_oracle(o)
            .map_err(|e| HostFailure::Backend(Error::Oracles(e)))?;
    }
    for q in next.oracle_reads.queries.values() {
        next.oracles
            .enter_query(q)
            .map_err(|e| HostFailure::Backend(Error::Oracles(e)))?;
    }
    *state = next;
    Ok(result)
}
fn relative_query(
    ttl: &Ttl,
    height: u64,
    op: &'static str,
) -> Result<Unsigned, HostFailure<Error>> {
    if ttl.relative {
        if ttl.value < 0.into() {
            Err(error(op, "negative_ttl"))
        } else {
            unsigned(&ttl.value)
        }
    } else if ttl.value < height.into() {
        Err(error(op, "too_low_ttl"))
    } else {
        unsigned(&(&ttl.value - BigInt::from(height)))
    }
}
