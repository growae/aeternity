// aehttp_helpers dry_run_results/floor_dry_run_gas_prices, fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::Profile;
use crate::http::{text, transaction};
use ae_codec::api::{self, ApiType};
use ae_execution::{
    contracts, dry_run,
    transactions::{self, batch},
};
use ae_tx::{Transaction, TransactionKind, TransactionLeaf};
use ae_types::Unsigned;
use ae_vm_fate::diagnostic::{self, Style, Term};
use serde_json::{Value, json};
use std::borrow::Cow;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub results: usize,
    pub events: usize,
    pub json_bytes: usize,
    pub transaction: transaction::Limits,
    pub diagnostic: ae_vm_fate::Limits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            results: 1024,
            events: 65536,
            json_bytes: 64 << 20,
            transaction: Default::default(),
            diagnostic: Default::default(),
        }
    }
}
#[derive(Debug)]
pub enum Error {
    Capacity,
    Transaction(transaction::Error),
    Diagnostic(ae_vm_fate::Error),
    // An unrepresented native shape is never replaced with an invented reason.
    NativeShape,
}
struct Budget {
    remaining: usize,
    limits: Limits,
}
impl Budget {
    fn charge(&mut self, n: usize) -> Result<(), Error> {
        self.remaining = self.remaining.checked_sub(n).ok_or(Error::Capacity)?;
        Ok(())
    }
    fn value(&mut self, v: &Value, depth: usize) -> Result<(), Error> {
        if depth > 64 {
            return Err(Error::Capacity);
        }
        self.charge(32)?;
        match v {
            Value::String(s) => self.charge(s.len().checked_mul(6).ok_or(Error::Capacity)?)?,
            Value::Number(n) => self.charge(n.as_str().len())?,
            Value::Array(a) => {
                for v in a {
                    self.value(v, depth + 1)?;
                }
            }
            Value::Object(m) => {
                for (k, v) in m {
                    self.charge(k.len().checked_mul(6).ok_or(Error::Capacity)?)?;
                    self.value(v, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn transaction(&self) -> transaction::Limits {
        transaction::Limits {
            json_bytes: self.limits.transaction.json_bytes.min(self.remaining),
            ..self.limits.transaction
        }
    }
}
pub fn kind(kind: TransactionKind) -> Result<&'static str, Error> {
    use TransactionKind::*;
    Ok(match kind {
        Spend => "spend",
        PayingFor => "paying_for_tx",
        GaMeta => "ga_meta_tx",
        GaAttach => "ga_attach",
        OracleRegister => "oracle_register",
        OracleExtend => "oracle_extend",
        OracleQuery => "oracle_query",
        OracleResponse => "oracle_response",
        NamePreclaim => "name_preclaim",
        NameClaim => "name_claim",
        NameTransfer => "name_transfer",
        NameUpdate => "name_update",
        NameRevoke => "name_revoke",
        ContractCall => "contract_call",
        ContractCreate => "contract_create",
        ChannelCreate => "channel_create",
        ChannelDeposit => "channel_deposit",
        ChannelWithdraw => "channel_withdraw",
        ChannelForceProgress => "channel_force_progress",
        ChannelCloseSolo => "channel_close_solo",
        ChannelCloseMutual => "channel_close_mutual",
        ChannelSlash => "channel_slash",
        ChannelSettle => "channel_settle",
        ChannelSnapshotSolo => "channel_snapshot_solo",
        ChannelOffchain | ChannelSetDelegates | HcVote => return Err(Error::NativeShape),
    })
}
fn native_kind(kind: TransactionKind) -> Result<String, Error> {
    let name = match kind {
        TransactionKind::ChannelOffchain => "channel_offchain",
        TransactionKind::ChannelSetDelegates => "channel_set_delegates",
        TransactionKind::HcVote => "hc_vote",
        _ => self::kind(kind)?,
    };
    Ok(
        if matches!(kind, TransactionKind::GaMeta | TransactionKind::PayingFor) {
            name.to_owned()
        } else {
            format!("{name}_tx")
        },
    )
}
pub fn reason<A, C>(
    failure: &batch::Failure<A, C>,
    limits: ae_vm_fate::Limits,
) -> Result<String, Error> {
    let term = if failure.is_native() {
        Term::Atom(Cow::Borrowed("crash"))
    } else {
        match failure {
            batch::Failure::GasLimitExceeded => Term::Atom(Cow::Borrowed("gas_limit_exceeded")),
            batch::Failure::Transaction(error) => match error.as_ref() {
                transactions::Error::NotAllowedToPayFor(kind) => Term::Tuple(vec![
                    Term::Atom(Cow::Borrowed("not_allowed_to_pay_for")),
                    Term::Atom(Cow::Owned(native_kind(*kind)?)),
                ]),
                e => {
                    let r = transactions::reasons::transaction(e)
                        .filter(|r| !r.tuple)
                        .ok_or(Error::NativeShape)?;
                    Term::Atom(Cow::Borrowed(r.tag))
                }
            },
            _ => return Err(Error::NativeShape),
        }
    };
    let (bytes, _) = diagnostic::format_with_width(&term, Style::Pretty, -1, 3, 120, limits)
        .map_err(Error::Diagnostic)?;
    Ok(format!("Internal error:\n  {}\n", text::binary(&bytes)))
}
fn client(tx: TransactionLeaf, budget: &Budget) -> Result<Value, Error> {
    let tx = Transaction::new(tx);
    let mut value =
        transaction::unsigned(tx.view(), budget.transaction()).map_err(Error::Transaction)?;
    let object = value.as_object_mut().ok_or(Error::NativeShape)?;
    object.remove("type");
    object.remove("version");
    // aetx:serialize_for_client removes a zero TTL, while event serialization
    // calls the transaction callback directly and retains that field.
    object.entry("ttl").or_insert_with(|| json!(0));
    Ok(value)
}
fn event(event: &batch::Event, budget: &Budget) -> Result<Option<Value>, Error> {
    use ae_execution::fate_chain::TransferEvent;
    let (kind, hash) = (native_kind(event.transaction_type)?, event.transaction_hash);
    let (key, info) = match &event.value {
        transactions::Event::Channel {
            channel,
            transaction_hash,
            transaction_type,
        } => {
            return Ok(Some(
                json!({"kind":"channel","key":text::binary(channel),"type":native_kind(*transaction_type)?,"tx_hash":text::binary(&transaction_hash.0)}),
            ));
        }
        transactions::Event::Contract(contracts::Event::Internal(e)) => match e {
            TransferEvent::InternalCall(tx) => (
                text::binary(tx.payload()),
                client(TransactionLeaf::Spend(tx.clone()), budget)?,
            ),
            TransferEvent::NameCall {
                info,
                transaction: Some(tx),
            } => (
                info.to_string(),
                client(TransactionLeaf::Name(tx.clone()), budget)?,
            ),
            TransferEvent::OracleCall { info, transaction } => (
                info.to_string(),
                client(TransactionLeaf::Oracle(transaction.clone()), budget)?,
            ),
            TransferEvent::NameCall {
                transaction: None, ..
            }
            | TransferEvent::DeploymentCall { .. } => return Err(Error::NativeShape),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    Ok(Some(
        json!({"kind":"internal_call_tx","key":key,"type":kind,"tx_hash":api::encode(ApiType::TxHash,&hash.0),"info":info}),
    ))
}
// This constructs native helper output. v2/v3 validation and integer conversion
// follow in schema::render; reader/admission ownership belongs to the caller.
pub fn body<A, C>(
    output: &dry_run::Output<A, C>,
    profile: Profile,
    events: bool,
    floor: Option<&Unsigned>,
    limits: Limits,
) -> Result<Value, Error> {
    if output.results.len() > limits.results || output.events.len() > limits.events {
        return Err(Error::Capacity);
    }
    let mut budget = Budget {
        remaining: limits.json_bytes,
        limits,
    };
    // Include the top-level object, result/event arrays and their field names,
    // including an empty response. Child values are charged below.
    budget.charge(32 + 7 * 6 + 32 + if events { 9 * 6 + 32 } else { 0 })?;
    budget.charge(
        output
            .results
            .len()
            .checked_mul(32)
            .ok_or(Error::Capacity)?,
    )?;
    let mut results = Vec::with_capacity(output.results.len());
    for item in &output.results {
        let mut value = json!({"type":kind(item.transaction_type)?});
        match &item.result {
            Ok(result) => {
                value["result"] = json!("ok");
                if let Some(call) = &result.call {
                    let price = if profile == Profile::Public {
                        floor
                            .filter(|floor| *floor > &call.gas_price)
                            .unwrap_or(&call.gas_price)
                    } else {
                        &call.gas_price
                    };
                    value["call_obj"] =
                        transaction::calls::call_with_gas_price(call, price, budget.transaction())
                            .map_err(Error::Transaction)?;
                }
            }
            Err(failure) => {
                value["result"] = json!("error");
                value["reason"] = json!(reason(failure, limits.diagnostic)?);
            }
        }
        budget.value(&value, 0)?;
        results.push(value);
    }
    let mut out = json!({"results":results});
    if events {
        let mut rendered = Vec::new();
        // Native foldr/prepending preserves the order of the source event list.
        for source in &output.events {
            if let Some(value) = event(source, &budget)? {
                budget.value(&value, 0)?;
                rendered.push(value);
            }
        }
        out["tx_events"] = Value::Array(rendered);
    }
    Ok(out)
}
