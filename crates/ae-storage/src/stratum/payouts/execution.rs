use super::*;
use ae_state::calls::{Call, ReturnType, call_id};
use ae_tx::{ContractOperation, TransactionLeaf};
use ae_types::IdTag;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inclusion {
    pub sequence: u64,
    pub payment: core::PaymentId,
    pub hash: Hash256,
    pub block: Hash256,
    pub height: u64,
    pub chain: repo::Status,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReason {
    ContractError,
    ContractRevert,
    UnexpectedReturn,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionFailure {
    pub inclusion: Inclusion,
    pub reason: FailureReason,
    pub call: Call,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnavailableReason {
    MissingCall,
    Pruned { floor: u64 },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptUnavailable {
    pub inclusion: Inclusion,
    pub reason: UnavailableReason,
}
fn call_error(e: ae_state::contracts::Error) -> Failure {
    match e {
        ae_state::contracts::Error::Tree(e) => Failure::Storage(e.into()),
        ae_state::contracts::Error::ResourceLimit => Error::ResourceLimit.into(),
        _ => Error::Corrupt("Stratum payout call receipt").into(),
    }
}
// Failed/unavailable observations are read-only. The exact pending request and
// payment remain durable; a new owner rechecks the same canonical receipt.
#[allow(clippy::too_many_arguments)]
pub(super) fn check(
    view: &repo::View<Snapshot<'_>>,
    config: &Config,
    expected: &Record,
    signed: &ae_mempool::pool::Signed,
    block: Hash256,
    height: u64,
    limits: Limits,
) -> Result<Option<Confirmation>, Failure> {
    let inclusion = Inclusion {
        sequence: expected.sequence,
        payment: expected.payment.id,
        hash: signed.hash(),
        block,
        height,
        chain: view.status(),
    };
    let TransactionLeaf::Contract(tx) = signed.transaction().leaf() else {
        return Err(Error::Corrupt("Stratum payout transaction kind").into());
    };
    let ContractOperation::Call {
        contract,
        abi_version,
        ..
    } = tx.operation()
    else {
        return Err(Error::Corrupt("Stratum payout transaction operation").into());
    };
    if !signed.transaction().layers().is_empty()
        || tx.actor().tag != IdTag::Account
        || tx.signer() != config.caller
        || contract.tag != IdTag::Contract
        || contract.value != config.contract.0
        || abi_version != &1.into()
    {
        return Err(Error::Corrupt("Stratum payout call identity").into());
    }
    let trees = match view.trees(block, limits.tree) {
        Ok(t) => t,
        Err(Error::Pruned { floor, .. }) => {
            return Ok(Some(Confirmation::Unavailable(ReceiptUnavailable {
                inclusion,
                reason: UnavailableReason::Pruned { floor },
            })));
        }
        Err(error) => return Err(error.into()),
    };
    let id = call_id(config.caller.0, tx.nonce(), config.contract.0).0;
    let key = ae_trie::Nibbles::from_bytes(&[config.contract.0.as_slice(), id.as_slice()].concat());
    let raw = match trees.calls.tree().get(&key).map_err(Error::from)? {
        None => {
            return Ok(Some(Confirmation::Unavailable(ReceiptUnavailable {
                inclusion,
                reason: UnavailableReason::MissingCall,
            })));
        }
        Some(ae_trie::Value::Bytes(raw)) => raw,
        _ => return Err(Error::Corrupt("Stratum payout call trie value").into()),
    };
    if raw.len() > limits.bytes {
        return Err(Error::ResourceLimit.into());
    }
    let call = Call::deserialize(id, &raw).map_err(call_error)?;
    if call.serialize().map_err(call_error)? != raw
        || call.caller != tx.actor()
        || &call.nonce != tx.nonce()
        || call.contract != config.contract.0
        || call.call_target.is_some()
        || call.height != height.into()
        || &call.gas_price != tx.gas_price()
        || &call.gas_used > tx.gas()
    {
        return Err(Error::Corrupt("Stratum payout call binding").into());
    }
    let reason = match call.return_type {
        ReturnType::Error => FailureReason::ContractError,
        ReturnType::Revert => FailureReason::ContractRevert,
        ReturnType::Ok if !payout::confirms_total(tx.amount(), &call.return_value) => {
            FailureReason::UnexpectedReturn
        }
        ReturnType::Ok => return Ok(None),
    };
    Ok(Some(Confirmation::Failed(Box::new(ExecutionFailure {
        inclusion,
        reason,
        call,
    }))))
}
