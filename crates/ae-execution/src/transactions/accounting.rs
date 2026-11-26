// aetx:used_gas/4 and deep_fee/2, aega_meta_tx, aeternity fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use ae_state::{
    calls::{ReturnType, call_id, ga_call_id},
    contracts::{Nonce, contract_id},
};
use ae_trie::NodeReader;

#[derive(Debug)]
pub enum Error {
    Account(ae_state::accounts::AccountError),
    Call(ae_state::contracts::Error),
    MissingAccount(AccountPublicKey),
    NotGeneralized(AccountPublicKey),
    MissingCall { namespace: [u8; 32], id: [u8; 32] },
    AuthenticationReverted,
}
impl Error {
    pub fn is_operational(&self) -> bool {
        matches!(self, Self::Account(_) | Self::Call(_))
    }
}
fn call<A: NodeReader, C: NodeReader>(
    trees: &Trees<A, C>,
    namespace: [u8; 32],
    id: [u8; 32],
) -> Result<Call, Error> {
    trees
        .calls
        .lookup(&namespace, id)
        .map_err(Error::Call)?
        .ok_or(Error::MissingCall { namespace, id })
}
fn auth<A: NodeReader, C: NodeReader>(
    meta: &ae_tx::GaMeta,
    trees: &Trees<A, C>,
) -> Result<Call, Error> {
    let account = trees
        .accounts
        .lookup(meta.account)
        .map_err(Error::Account)?
        .ok_or(Error::MissingAccount(meta.account))?;
    let authorization = account
        .authorization()
        .ok_or(Error::NotGeneralized(meta.account))?;
    call(
        trees,
        meta.account.0,
        ga_call_id(meta.auth_id().0, authorization.contract.value).0,
    )
}
fn succeeded(call: &Call) -> Result<bool, Error> {
    match call.return_type {
        ReturnType::Ok => Ok(true),
        ReturnType::Error => Ok(false),
        ReturnType::Revert => Err(Error::AuthenticationReverted),
    }
}
// Query the supplied state, not a cached execution receipt. Authentication
// results decide whether the inner fee contributes to the total.
pub fn deep_fee<A: NodeReader, C: NodeReader>(
    mut tx: TransactionRef<'_>,
    trees: &Trees<A, C>,
) -> Result<BigInt, Error> {
    let mut fee = BigInt::ZERO;
    loop {
        fee += big(tx.fee());
        match tx.layer().map(|l| l.wrapper()) {
            Some(Wrapper::GaMeta(meta)) if !succeeded(&auth(meta, trees)?)? => return Ok(fee),
            None => return Ok(fee),
            _ => tx = tx.inner().expect("wrapper inner"),
        }
    }
}
pub fn used_gas<A: NodeReader, C: NodeReader>(
    mut tx: TransactionRef<'_>,
    height: &Unsigned,
    protocol: Protocol,
    trees: &Trees<A, C>,
) -> Result<BigInt, Error> {
    let mut gas = BigInt::ZERO;
    let mut ga_nonce = None;
    while let Some(layer) = tx.layer() {
        let inner = tx.inner().expect("wrapper inner");
        match layer.wrapper() {
            Wrapper::PayingFor(_) => {
                gas += BigInt::from(ae_protocol::gas::PAYING_FOR) + size_gas(tx.wire_size())
                    - size_gas(inner.wire_size())
            }
            Wrapper::GaMeta(meta) => {
                let result = auth(meta, trees)?;
                gas += BigInt::from(ae_protocol::contracts::CREATE_BASE_GAS)
                    + size_gas(tx.wire_size())
                    + big(&result.gas_used);
                if !succeeded(&result)? {
                    return Ok(gas);
                }
                if protocol >= Protocol::Arcus {
                    gas -= size_gas(inner.wire_size());
                }
                ga_nonce = Some(meta.auth_id().0);
            }
        }
        tx = inner;
    }
    let (owner, nonce, namespace, base, generalized) = match tx.leaf() {
        TransactionLeaf::Contract(t) => {
            let namespace = match t.operation() {
                ContractOperation::Create { .. } => {
                    contract_id(
                        t.signer(),
                        ga_nonce.map_or(Nonce::Number(t.nonce()), Nonce::Hash),
                    )
                    .0
                }
                ContractOperation::Call { contract, .. } => contract.value,
            };
            (t.signer(), t.nonce(), namespace, t.base_gas(), true)
        }
        TransactionLeaf::GaAttach(t) => (
            t.owner(),
            t.nonce(),
            contract_id(t.owner(), Nonce::Number(t.nonce())).0,
            ae_protocol::contracts::CREATE_BASE_GAS,
            false,
        ),
        _ => return Ok(gas + tx.pricing(height, protocol).gas_limit),
    };
    let id = if generalized {
        ga_nonce
            .map(|n| ga_call_id(n, namespace))
            .unwrap_or_else(|| call_id(owner.0, nonce, namespace))
    } else {
        call_id(owner.0, nonce, namespace)
    };
    Ok(gas
        + BigInt::from(base)
        + size_gas(tx.wire_size())
        + big(&call(trees, namespace, id.0)?.gas_used))
}
