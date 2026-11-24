// Deterministic payout simulation. Inputs supply the selected execution
// context; this module neither chooses a live top nor signs or publishes.
use ae_chain::repository::Roots;
use ae_codec::api::{self, ApiType};
use ae_execution::{Context, Origin, contracts::aevm};
use ae_state::{
    Trees,
    accounts::{Account, AccountError, Accounts},
    calls::{Call, Calls, ReturnType, call_id},
    channels::Channels,
    contracts::Contracts,
    names::Names,
    oracles::Oracles,
};
use ae_stratum::payout;
use ae_trie::{MemoryStore, NodeReader, Overlay, OverlayLimits};
use ae_tx::ContractOperation;
use ae_types::{AccountPublicKey, Hash256, IdTag, Unsigned};
use num_bigint::{BigInt, Sign};

#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    pub payout: payout::Limits,
    pub overlays: OverlayLimits,
    pub vm: ae_vm_aevm::execution::Limits,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptFailure {
    Identity,
    Gas,
    Return(ReturnType),
    Total,
}
#[derive(Debug)]
pub enum Error<A = MemoryStore, C = MemoryStore> {
    Input(Box<payout::Error>),
    Context,
    Allocation,
    BlockGas,
    Execution(Box<aevm::Error<Overlay<A>, Overlay<C>>>),
    Accounts(AccountError),
    Contracts(ae_state::contracts::Error),
    Channels(ae_state::channels::Error),
    Names(ae_state::names::Error),
    Oracles(ae_state::oracles::Error),
    Receipt(ReceiptFailure),
    Effects {
        expected: Box<Roots>,
        actual: Box<Roots>,
    },
}
// Only evaluation constructs a successful check. The runtime must still bind
// this input-root/request pair to an authoritative, current repository view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checked {
    preparation: Hash256,
    before: Roots,
    after: Roots,
    call: Call,
}
impl Checked {
    pub fn preparation_hash(&self) -> Hash256 {
        self.preparation
    }
    pub fn before(&self) -> &Roots {
        &self.before
    }
    pub fn after(&self) -> &Roots {
        &self.after
    }
    pub fn call(&self) -> &Call {
        &self.call
    }
}
fn big(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
fn uint(n: &BigInt) -> Option<Unsigned> {
    (n.sign() != Sign::Minus)
        .then(|| Unsigned::from_field_bytes(&n.to_bytes_be().1).ok())
        .flatten()
}

// Even a caller-provided store with write capabilities is used only as a
// reader. All simulation and expected-state writes stay in these overlays.
fn fork<A: NodeReader + Clone, C: NodeReader + Clone>(
    input: &Trees<A, C>,
    limits: OverlayLimits,
) -> Result<Trees<Overlay<A>, Overlay<C>>, Error<A, C>> {
    let r = Roots::of(input);
    Ok(Trees {
        accounts: Accounts::from_root(
            r.state.accounts,
            Overlay::new(input.accounts.tree().store().clone(), limits),
        )
        .map_err(Error::Accounts)?,
        calls: Calls::from_root(
            r.state.calls,
            Overlay::new(input.calls.tree().store().clone(), limits),
        )
        .map_err(Error::Contracts)?,
        contracts: Contracts::from_root(
            r.state.contracts,
            Overlay::new(input.contracts.tree().store().clone(), limits),
        )
        .map_err(Error::Contracts)?,
        channels: Channels::from_root(
            r.state.channels,
            Overlay::new(input.channels.tree().store().clone(), limits),
        )
        .map_err(Error::Channels)?,
        names: Names::from_roots(
            r.state.names,
            r.names,
            Overlay::new(input.names.tree().store().clone(), limits),
            Overlay::new(input.names.cache().store().clone(), limits),
        )
        .map_err(Error::Names)?,
        oracles: Oracles::from_roots(
            r.state.oracles,
            r.oracles,
            Overlay::new(input.oracles.tree().store().clone(), limits),
            Overlay::new(input.oracles.cache().store().clone(), limits),
        )
        .map_err(Error::Oracles)?,
    })
}
fn receipt<A, C>(
    prepared: &payout::Prepared,
    context: &Context,
    call: &Call,
) -> Result<(), Error<A, C>> {
    let tx = prepared.call().transaction();
    let ContractOperation::Call { contract, .. } = tx.operation() else {
        return Err(Error::Context);
    };
    if call.id != call_id(tx.signer().0, tx.nonce(), contract.value).0
        || call.caller != tx.actor()
        || call.nonce != *tx.nonce()
        || call.contract != contract.value
        || call.call_target.is_some()
        || call.height != context.height.into()
        || call.gas_price != *tx.gas_price()
    {
        return Err(Error::Receipt(ReceiptFailure::Identity));
    }
    if call.gas_used > *tx.gas() {
        return Err(Error::Receipt(ReceiptFailure::Gas));
    }
    if call.return_type != ReturnType::Ok {
        return Err(Error::Receipt(ReceiptFailure::Return(call.return_type)));
    }
    if !payout::confirms_total(tx.amount(), &call.return_value) {
        return Err(Error::Receipt(ReceiptFailure::Total));
    }
    Ok(())
}
fn effects<A: NodeReader + Clone, C: NodeReader + Clone>(
    prepared: &payout::Prepared,
    input: &Trees<A, C>,
    call: &Call,
    actual: &Roots,
    limits: OverlayLimits,
) -> Result<(), Error<A, C>> {
    let mut expected = fork(input, limits)?;
    let tx = prepared.call().transaction();
    let charge = tx
        .amount()
        .add(tx.fee())
        .add(&uint(&(big(&call.gas_used) * big(tx.gas_price()))).ok_or(Error::Allocation)?);
    let mut caller = expected
        .accounts
        .lookup(tx.signer())
        .map_err(Error::Accounts)?
        .ok_or(Error::Allocation)?;
    caller
        .spend(&charge, tx.nonce().clone())
        .map_err(Error::Accounts)?;
    expected.accounts.enter(&caller).map_err(Error::Accounts)?;
    // Debiting first naturally handles a caller that is also a recipient.
    for (address, amount) in prepared.absolute() {
        let text = std::str::from_utf8(address).map_err(|_| Error::Allocation)?;
        let (kind, key) = api::decode(text).map_err(|_| Error::Allocation)?;
        if kind != ApiType::AccountPubkey {
            return Err(Error::Allocation);
        }
        let key = AccountPublicKey(key.try_into().map_err(|_| Error::Allocation)?);
        let mut account = expected
            .accounts
            .lookup(key)
            .map_err(Error::Accounts)?
            .unwrap_or_else(|| Account::new(key, 0.into()));
        account.earn(&uint(amount).ok_or(Error::Allocation)?);
        expected.accounts.enter(&account).map_err(Error::Accounts)?;
    }
    expected.calls.insert(call).map_err(Error::Contracts)?;
    let expected = Roots::of(&expected);
    if &expected != actual {
        return Err(Error::Effects {
            expected: Box::new(expected),
            actual: Box::new(actual.clone()),
        });
    }
    Ok(())
}

pub fn evaluate<A: NodeReader + Clone, C: NodeReader + Clone>(
    prepared: &payout::Prepared,
    input: &Trees<A, C>,
    context: &Context,
    config: &aevm::Config,
    limits: Limits,
) -> Result<Checked, Error<A, C>> {
    let encoded = prepared
        .encode(limits.payout)
        .map_err(|e| Error::Input(Box::new(e)))?;
    let tx = prepared.call().transaction();
    if context.origin != Origin::Transaction
        || context.dry_run
        || context.payer.is_some()
        || !context.ga_authenticated.is_empty()
        || !context.ga_nonces.is_empty()
        || context.ga_transaction.is_some()
        || context.ga_transaction_hash.is_some()
        || config.environment.auth_tx_hash.is_some()
        || config.network_id.as_ref() != prepared.request().network
        || !matches!(tx.operation(), ContractOperation::Call { contract, abi_version, .. }
            if contract.tag == IdTag::Contract && *abi_version == 1.into())
    {
        return Err(Error::Context);
    }
    let configured = &config.environment.block_gas_limit;
    let ceiling = if matches!(config.network_id.as_ref(), b"ae_mainnet" | b"ae_uat") {
        configured.clone().min(6_000_000.into())
    } else {
        configured.clone()
    };
    if ceiling.sign() != Sign::Plus || big(&tx.gas_limit()) > ceiling {
        return Err(Error::BlockGas);
    }
    if prepared
        .absolute()
        .values()
        .any(|n| n.sign() != Sign::Plus || n.bits() > 256)
        || prepared.absolute().values().sum::<BigInt>() != big(tx.amount())
    {
        return Err(Error::Allocation);
    }
    let mut executed = fork(input, limits.overlays)?;
    let result = aevm::process_call(tx, &mut executed, context, config, limits.vm)
        .map_err(|e| Error::Execution(Box::new(e)))?;
    receipt(prepared, context, &result.call)?;
    let after = Roots::of(&executed);
    effects(prepared, input, &result.call, &after, limits.overlays)?;
    Ok(Checked {
        preparation: ae_crypto::blake2b_256(&encoded),
        before: Roots::of(input),
        after,
        call: result.call,
    })
}

#[cfg(test)]
pub(super) mod tests;
