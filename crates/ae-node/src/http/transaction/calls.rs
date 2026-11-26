// aect_call/aega_call client JSON, aeternity fa2a6d7.
// Copyright (c) 2017-2026, Aeternity developers. ISC: repository LICENSE.
use super::*;
use ae_chain::transaction_info::{Ga, Info};
use ae_state::calls::{Call, ReturnType};
#[cfg(test)]
mod tests;

fn budget(limits: Limits) -> Budget {
    Budget {
        json: Cell::new(limits.json_bytes),
        work: Cell::new(limits.work_bytes),
    }
}
fn return_type(value: ReturnType) -> &'static str {
    match value {
        ReturnType::Ok => "ok",
        ReturnType::Error => "error",
        ReturnType::Revert => "revert",
    }
}
pub fn call(value: &Call, limits: Limits) -> Result<Value, Error> {
    call_with(value, &budget(limits))
}
fn call_with(value: &Call, budget: &Budget) -> Result<Value, Error> {
    let mut out = Object::new(budget)?;
    out.id("caller_id", value.caller)?;
    out.number("caller_nonce", &value.nonce)?;
    out.number("height", &value.height)?;
    out.encoded("contract_id", ApiType::ContractPubkey, &value.contract)?;
    out.number("gas_price", &value.gas_price)?;
    out.number("gas_used", &value.gas_used)?;
    out.encoded(
        "return_value",
        ApiType::ContractBytearray,
        &value.return_value,
    )?;
    out.text("return_type", return_type(value.return_type))?;
    out.insert(
        "log",
        budget.array(&value.log, |log| {
            let mut out = Object::new(budget)?;
            out.encoded("address", ApiType::ContractPubkey, &log.address)?;
            out.encoded("data", ApiType::ContractBytearray, &log.data)?;
            // Erlang's <<Topic:256>> comprehension silently filters other lengths.
            // Charge every topic visit, even when it contributes no JSON value.
            budget.work(log.topics.len())?;
            out.insert(
                "topics",
                budget.array(log.topics.iter().filter(|t| t.len() == 32), |topic| {
                    let first = topic.iter().position(|b| *b != 0).unwrap_or(topic.len());
                    budget.number(
                        &Unsigned::from_field_bytes(&topic[first..])
                            .expect("trimmed unsigned topic"),
                    )
                })?,
            )?;
            Ok(out.finish())
        })?,
    )?;
    Ok(out.finish())
}
fn depth(value: &Info, limits: Limits) -> Result<(), Error> {
    let mut current = value;
    let mut left = limits.layers.min(256);
    while let Info::Ga { inner, .. } = current {
        left = left.checked_sub(1).ok_or(Error::ResourceLimit)?;
        current = inner;
    }
    Ok(())
}
pub fn ga(call: &Ga, inner: &Info, limits: Limits) -> Result<Value, Error> {
    depth(
        inner,
        Limits {
            layers: limits
                .layers
                .min(256)
                .checked_sub(1)
                .ok_or(Error::ResourceLimit)?,
            ..limits
        },
    )?;
    ga_with(call, inner, &budget(limits))
}
pub fn info(value: &Info, outer: TransactionKind, limits: Limits) -> Result<Value, Error> {
    depth(value, limits)?;
    match (outer, value) {
        (TransactionKind::GaMeta, Info::Ga { .. }) => (),
        (
            TransactionKind::ContractCall
            | TransactionKind::ContractCreate
            | TransactionKind::GaAttach
            | TransactionKind::PayingFor
            | TransactionKind::ChannelForceProgress,
            Info::Call(_),
        ) => (),
        _ => return Err(Error::InvalidInfo),
    }
    envelope(value, &budget(limits))
}
fn ga_with(call: &Ga, inner: &Info, budget: &Budget) -> Result<Value, Error> {
    let mut out = Object::new(budget)?;
    out.id("caller_id", call.caller)?;
    out.number("height", &call.height)?;
    out.number("gas_price", &call.gas_price)?;
    out.number("gas_used", &call.gas_used)?;
    out.encoded(
        "return_value",
        ApiType::ContractBytearray,
        &call.return_value,
    )?;
    out.text("return_type", return_type(call.return_type))?;
    out.insert("inner_object", envelope(inner, budget)?)?;
    Ok(out.finish())
}
fn envelope(value: &Info, budget: &Budget) -> Result<Value, Error> {
    let mut out = Object::new(budget)?;
    match value {
        Info::Call(call) => out.insert("call_info", call_with(call, budget)?)?,
        Info::Ga { call, inner } => out.insert("ga_info", ga_with(call, inner, budget)?)?,
        Info::Type(kind) => out.text("tx_info", type_name(*kind))?,
        Info::Undefined => out.text("tx_info", "undefined")?,
    }
    Ok(out.finish())
}
fn type_name(kind: TransactionKind) -> &'static str {
    use TransactionKind::*;
    match kind {
        ChannelCloseMutual => "channel_close_mutual_tx",
        ChannelCloseSolo => "channel_close_solo_tx",
        ChannelCreate => "channel_create_tx",
        ChannelDeposit => "channel_deposit_tx",
        ChannelForceProgress => "channel_force_progress_tx",
        ChannelOffchain => "channel_offchain_tx",
        ChannelSetDelegates => "channel_set_delegates_tx",
        ChannelSettle => "channel_settle_tx",
        ChannelSlash => "channel_slash_tx",
        ChannelSnapshotSolo => "channel_snapshot_solo_tx",
        ChannelWithdraw => "channel_withdraw_tx",
        ContractCall => "contract_call_tx",
        ContractCreate => "contract_create_tx",
        GaAttach => "ga_attach_tx",
        GaMeta => "ga_meta_tx",
        HcVote => "hc_vote_tx",
        NameClaim => "name_claim_tx",
        NamePreclaim => "name_preclaim_tx",
        NameRevoke => "name_revoke_tx",
        NameTransfer => "name_transfer_tx",
        NameUpdate => "name_update_tx",
        OracleExtend => "oracle_extend_tx",
        OracleQuery => "oracle_query_tx",
        OracleRegister => "oracle_register_tx",
        OracleResponse => "oracle_response_tx",
        PayingFor => "paying_for_tx",
        Spend => "spend_tx",
    }
}
