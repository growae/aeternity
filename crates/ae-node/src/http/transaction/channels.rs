use super::*;
use ae_tx::{
    ChannelTransaction,
    channels::{CreateDelegates, Operation, Update},
};

fn accounts(budget: &Budget, values: &[ae_types::AccountPublicKey]) -> Result<Value, Error> {
    budget.array(values, |k| {
        budget.encoded(ApiType::AccountPubkey, k.as_ref())
    })
}
pub(super) fn render(
    t: &ChannelTransaction,
    out: &mut Object<'_>,
) -> Result<(&'static str, u64), Error> {
    let b = out.budget;
    let op = t.operation();
    let mut version = 1;
    if let Some(channel) = op.channel() {
        out.encoded("channel_id", ApiType::Channel, &channel)?;
    }
    if let Some(payload) = op.payload() {
        out.encoded("payload", ApiType::Transaction, payload)?;
    }
    if !matches!(op, Operation::Offchain { .. }) {
        out.number("nonce", t.nonce())?;
        out.number("fee", t.fee())?;
        out.ttl(t.ttl())?;
    }
    let kind = match op {
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
            out.encoded("initiator_id", ApiType::AccountPubkey, initiator.as_ref())?;
            out.encoded("responder_id", ApiType::AccountPubkey, responder.as_ref())?;
            out.number("initiator_amount", initiator_amount)?;
            out.number("responder_amount", responder_amount)?;
            out.number("channel_reserve", reserve)?;
            out.number("lock_period", lock_period)?;
            out.encoded("state_hash", ApiType::State, state_hash)?;
            let delegates = match delegates {
                CreateDelegates::Shared(keys) => accounts(b, keys)?,
                CreateDelegates::Separate {
                    initiator,
                    responder,
                } => {
                    version = 2;
                    let mut v = Object::new(b)?;
                    v.insert("initiator", accounts(b, initiator)?)?;
                    v.insert("responder", accounts(b, responder)?)?;
                    v.finish()
                }
            };
            out.insert("delegate_ids", delegates)?;
            "ChannelCreateTx"
        }
        Operation::Deposit {
            from,
            amount,
            state_hash,
            round,
            ..
        }
        | Operation::Withdraw {
            to: from,
            amount,
            state_hash,
            round,
            ..
        } => {
            let deposit = matches!(op, Operation::Deposit { .. });
            out.encoded(
                if deposit { "from_id" } else { "to_id" },
                ApiType::AccountPubkey,
                from.as_ref(),
            )?;
            out.number("amount", amount)?;
            out.encoded("state_hash", ApiType::State, state_hash)?;
            out.number("round", round)?;
            if deposit {
                "ChannelDepositTx"
            } else {
                "ChannelWithdrawTx"
            }
        }
        Operation::CloseMutual {
            from,
            initiator_amount,
            responder_amount,
            ..
        } => {
            out.id("from_id", *from)?;
            out.number("initiator_amount_final", initiator_amount)?;
            out.number("responder_amount_final", responder_amount)?;
            "ChannelCloseMutualTx"
        }
        Operation::Settle {
            from,
            initiator_amount,
            responder_amount,
            ..
        } => {
            out.encoded("from_id", ApiType::AccountPubkey, from.as_ref())?;
            out.number("initiator_amount_final", initiator_amount)?;
            out.number("responder_amount_final", responder_amount)?;
            "ChannelSettleTx"
        }
        Operation::CloseSolo { from, proof, .. } | Operation::Slash { from, proof, .. } => {
            out.encoded("from_id", ApiType::AccountPubkey, from.as_ref())?;
            let limits = ae_state::transport::Limits {
                max_work_bytes: b.work.get(),
                ..Default::default()
            };
            let encoded = proof.serialize(limits).map_err(|e| match e {
                ae_state::transport::Error::ResourceLimit => Error::ResourceLimit,
                e => Error::State(e),
            })?;
            out.encoded("poi", ApiType::Poi, &encoded)?;
            if matches!(op, Operation::Slash { .. }) {
                "ChannelSlashTx"
            } else {
                "ChannelCloseSoloTx"
            }
        }
        Operation::SnapshotSolo { from, .. } => {
            out.encoded("from_id", ApiType::AccountPubkey, from.as_ref())?;
            "ChannelSnapshotSoloTx"
        }
        Operation::SetDelegates {
            from,
            initiator_delegates,
            responder_delegates,
            state_hash,
            round,
            ..
        } => {
            out.encoded("from_id", ApiType::AccountPubkey, from.as_ref())?;
            out.insert(
                "initiator_delegate_ids",
                b.array(initiator_delegates, |id| b.id(*id))?,
            )?;
            out.insert(
                "responder_delegate_ids",
                b.array(responder_delegates, |id| b.id(*id))?,
            )?;
            out.encoded("state_hash", ApiType::State, state_hash)?;
            out.number("round", round)?;
            "ChannelSetDelegatesTx"
        }
        Operation::ForceProgress {
            from,
            round,
            update: u,
            state_hash,
            trees,
            ..
        } => {
            out.encoded("from_id", ApiType::AccountPubkey, from.as_ref())?;
            out.number("round", round)?;
            out.encoded("state_hash", ApiType::State, state_hash)?;
            out.encoded("offchain_trees", ApiType::StateTrees, trees.bytes())?;
            out.insert("update", update(u, b)?)?;
            "ChannelForceProgressTx"
        }
        Operation::Offchain {
            updates,
            state_hash,
            ..
        } => {
            version = if updates.is_some() { 1 } else { 2 };
            out.number("round", t.nonce())?;
            out.encoded("state_hash", ApiType::State, state_hash)?;
            "ChannelOffchainTx"
        }
    };
    Ok((kind, version))
}
fn update(u: &Update, b: &Budget) -> Result<Value, Error> {
    let mut out = Object::new(b)?;
    let kind = match u {
        Update::Transfer { from, to, amount } => {
            out.encoded("from", ApiType::AccountPubkey, from.as_ref())?;
            out.encoded("to", ApiType::AccountPubkey, to.as_ref())?;
            out.number("amount", amount)?;
            "OffChainTransfer"
        }
        Update::Deposit { from, amount } => {
            out.encoded("from", ApiType::AccountPubkey, from.as_ref())?;
            out.number("amount", amount)?;
            "OffChainDeposit"
        }
        Update::Withdraw { to, amount } => {
            out.encoded("to", ApiType::AccountPubkey, to.as_ref())?;
            out.number("amount", amount)?;
            "OffChainWithdrawal"
        }
        Update::CreateContract {
            owner,
            version,
            code,
            deposit,
            call_data,
        } => {
            out.encoded("owner", ApiType::AccountPubkey, owner.as_ref())?;
            out.small("vm_version", u64::from(version.vm))?;
            out.small("abi_version", u64::from(version.abi))?;
            out.encoded("code", ApiType::ContractBytearray, code)?;
            out.number("deposit", deposit)?;
            out.encoded("call_data", ApiType::ContractBytearray, call_data)?;
            "OffChainNewContract"
        }
        Update::CallContract {
            caller,
            contract,
            abi_version,
            amount,
            gas,
            gas_price,
            call_data,
            call_stack,
        } => {
            out.encoded("caller_id", ApiType::AccountPubkey, caller.as_ref())?;
            out.encoded("contract_id", ApiType::ContractPubkey, contract)?;
            out.number("abi_version", abi_version)?;
            out.number("amount", amount)?;
            out.number("gas", gas)?;
            out.number("gas_price", gas_price)?;
            out.encoded("call_data", ApiType::ContractBytearray, call_data)?;
            out.insert("call_stack", b.array(call_stack, |n| b.number(n))?)?;
            "OffChainCallContract"
        }
        Update::Meta { data } => {
            out.raw("data", data)?;
            "OffChainMeta"
        }
    };
    out.text("op", kind)?;
    Ok(out.finish())
}
