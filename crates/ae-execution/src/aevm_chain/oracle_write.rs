// aec_vm_chain oracle transactions and delegation, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::state::{append_events, native};
use super::*;
use ae_protocol::oracles::Ttl;
use ae_trie::NodeWriter;
use ae_tx::{OracleOperation as O, OracleTransaction};
use ae_types::{Id, IdTag, Signature};
use ae_vm_aevm::{
    execution::{Budget, Stop},
    heap::{self, Value},
    primops::{HostResult, OracleTtl, OracleWrite},
    runtime::Fault,
};
use num_bigint::{BigInt, BigUint, Sign};
fn uint(n: &BigUint) -> Unsigned {
    Unsigned::from_field_bytes(&n.to_bytes_be()).expect("unsigned")
}
fn ttl(t: &OracleTtl) -> Ttl {
    if t.absolute {
        Ttl::Block(uint(&t.value))
    } else {
        Ttl::Delta(uint(&t.value))
    }
}
fn relative(t: &OracleTtl) -> Result<Unsigned, Fault<Error>> {
    if t.absolute {
        Err(native("function_clause"))
    } else {
        Ok(uint(&t.value))
    }
}
fn result<T>(e: crate::Error) -> HostResult<T, Error> {
    use crate::{
        Error as E,
        oracles::{Failure as F, abi::Error as A},
    };
    let s = match e {
        E::OracleTransaction(f) => match f {
            F::TooLowAbsoluteTtl => "too_low_abs_ttl",
            F::TooLowHeight => "too_low_height",
            F::AlreadyOracle => "account_is_already_an_oracle",
            F::NotActiveOracle => "account_is_not_an_active_oracle",
            F::ZeroExtensionTtl => "zero_relative_oracle_extension_ttl",
            F::OracleDoesNotExist => "oracle_does_not_exist",
            F::QueryFeeTooLow => "query_fee_too_low",
            F::TooLongTtl => "too_long_ttl",
            F::NamedQueryUnavailable => "oracle_query_by_name_hash_not_available_at_protocol",
            F::QueryAlreadyPresent => "oracle_query_already_present",
            F::IllegalQuerySpec => "illegal_oracle_query_spec",
            F::NoMatchingQuery => "no_matching_oracle_query",
            F::WrongResponseTtl => "oracle_response_has_wrong_response_ttl",
            F::WrongOracle => "oracle_does_not_match_query_id",
            F::QueryClosed => "oracle_closed_for_response",
        },
        E::OracleAbi(a) => match a {
            A::BadAbiVersion => "bad_abi_version",
            A::BadQueryFormat => "bad_query_format",
            A::BadResponseFormat => "bad_response_format",
            A::BadFormat => "bad_format",
            A::ResourceLimit => return Err(Stop::ResourceLimit.into()),
            a => return Err(Fault::Host(Error::Primitive(Box::new(E::OracleAbi(a))))),
        },
        E::Oracle(e) => return Err(Fault::Host(Error::OracleState(e))),
        e => return state::result(e),
    };
    Ok(Err(s.into()))
}
fn encode(value: &Value, b: &mut Budget) -> Result<Vec<u8>, Fault<Error>> {
    heap::encode_with_budget(value, b).map_err(|e| {
        if e == heap::Error::ResourceLimit {
            Stop::ResourceLimit.into()
        } else {
            native("function_clause")
        }
    })
}
impl<A: NodeReader, C: NodeReader> Host<A, C> {
    fn oracle_content(
        &self,
        key: Word,
        v: &Value,
        s: &State<A, C>,
        b: &mut Budget,
    ) -> Result<Vec<u8>, Fault<Error>> {
        b.work(1)?;
        let o = s
            .trees
            .oracles
            .oracle(key.0)
            .map_err(|e| Fault::Host(Error::OracleState(e)))?;
        match o.map(|o| o.abi_version) {
            Some(a) if a == 1.into() => encode(v, b),
            None => match v {
                Value::String(bytes) => {
                    b.bytes(bytes.len())?;
                    Ok(bytes.clone())
                }
                _ => Err(native("function_clause")),
            },
            Some(a) if a == 0.into() => match v {
                Value::String(bytes) => {
                    b.bytes(bytes.len())?;
                    Ok(bytes.clone())
                }
                _ => Err(native("function_clause")),
            },
            _ => Err(native("case_clause")),
        }
    }
    pub fn prepare_oracle_write(
        &self,
        r: OracleWrite<'_>,
        s: &State<A, C>,
        b: &mut Budget,
    ) -> HostResult<(OracleTransaction, usize), Error> {
        b.work(1)?;
        if s.onchain.is_some() {
            return Ok(Err("not_allowed_off_chain".into()));
        }
        let actor = match r {
            OracleWrite::Register { account, .. } => AccountPublicKey(account.0),
            OracleWrite::Query { .. } => s.account,
            OracleWrite::Respond { oracle, .. } | OracleWrite::Extend { oracle, .. } => {
                AccountPublicKey(oracle.0)
            }
        };
        let nonce = self.transaction_nonce(actor, s, b)?;
        let operation = match r {
            OracleWrite::Register {
                fee,
                ttl: t,
                query,
                response,
                abi,
                ..
            } => O::Register {
                query_format: encode(&Value::Type(query.clone()), b)?,
                response_format: encode(&Value::Type(response.clone()), b)?,
                query_fee: uint(&fee.unsigned()),
                oracle_ttl: ttl(t),
                abi_version: u64::from(abi).into(),
            },
            OracleWrite::Query {
                oracle,
                value,
                fee,
                query_ttl,
                response_ttl,
            } => {
                let query = self.oracle_content(oracle, value, s, b)?;
                if fee.sign() == Sign::Minus {
                    return Err(native("function_clause"));
                }
                O::Query {
                    oracle: Id::new(IdTag::Oracle, oracle.0),
                    query,
                    query_fee: uint(&fee.to_biguint().unwrap()),
                    query_ttl: ttl(query_ttl),
                    response_ttl: relative(response_ttl)?,
                }
            }
            OracleWrite::Respond {
                oracle,
                query,
                value,
                ttl,
            } => O::Response {
                query_id: query.0.to_vec(),
                response: self.oracle_content(oracle, value, s, b)?,
                response_ttl: uint(ttl),
            },
            OracleWrite::Extend { ttl, .. } => O::Extend {
                delta: relative(ttl)?,
            },
        };
        let mut tx =
            OracleTransaction::new(actor, operation).map_err(|_| native("function_clause"))?;
        tx.set_nonce(nonce);
        let size = tx.wire_size();
        b.bytes(size)?;
        Ok(Ok((tx, size)))
    }
    fn oracle_signature(
        &self,
        tx: &OracleTransaction,
        signature: Option<[u8; 64]>,
        s: &State<A, C>,
        b: &mut Budget,
    ) -> Result<bool, Fault<Error>> {
        if matches!(tx.operation(), O::Query { .. }) || tx.account() == s.account {
            return Ok(true);
        }
        b.work(1)?;
        let Some(a) = s
            .trees
            .accounts
            .lookup(tx.account())
            .map_err(|e| Fault::Host(Error::Accounts(e)))?
        else {
            return Ok(false);
        };
        if a.authorization().is_some() {
            return Ok(false);
        }
        let Some(signature) = signature else {
            return Err(native("function_clause"));
        };
        let account = tx.account();
        let key = match tx.operation() {
            O::Response { query_id, .. } => query_id.as_slice(),
            _ => account.0.as_slice(),
        };
        let size = s
            .network_id
            .len()
            .checked_add(key.len())
            .and_then(|n| n.checked_add(32))
            .ok_or(Stop::ResourceLimit)?;
        b.bytes(size)?;
        b.work(1)?;
        let mut message = s.network_id.to_vec();
        message.extend_from_slice(key);
        message.extend(s.account.0);
        Ok(ae_crypto::verify_ed25519(
            &tx.account(),
            &message,
            &Signature(signature),
        ))
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> Host<A, C> {
    pub fn apply_oracle_write(
        &self,
        tx: &OracleTransaction,
        signature: Option<[u8; 64]>,
        s: &State<A, C>,
        b: &mut Budget,
    ) -> HostResult<(Value, State<A, C>), Error> {
        b.work(1)?;
        if s.onchain.is_some() {
            return Ok(Err("not_allowed_off_chain".into()));
        }
        if !self.oracle_signature(tx, signature, s, b)? {
            return Ok(Err("signature_check_failed".into()));
        }
        let remaining = b.remaining();
        let limits = crate::oracles::abi::Limits {
            aevm: heap::Limits {
                max_depth: remaining.max_depth,
                max_nodes: remaining.max_work,
                max_bytes: remaining.max_bytes,
            },
            ..Default::default()
        };
        b.bytes(tx.wire_size())?;
        b.work(tx.wire_size())?;
        let mut next = s.clone();
        let receipt = match crate::oracles::process_with_limits(
            tx,
            &mut next.trees.accounts,
            &mut next.trees.oracles,
            &next.trees.names,
            &next.context,
            limits,
        ) {
            Ok(r) => r,
            Err(e) => return result(e),
        };
        next.events = append_events(s, receipt.events, b)?;
        let out = match tx.operation() {
            O::Register { .. } => {
                Value::Integer(BigInt::from_bytes_be(Sign::Plus, &tx.account().0))
            }
            O::Query { oracle, .. } => Value::Integer(BigInt::from_bytes_be(
                Sign::Plus,
                &ae_state::oracles::query_id(tx.account(), tx.nonce(), oracle.value).0,
            )),
            _ => Value::Tuple(vec![]),
        };
        Ok(Ok((out, next)))
    }
}
