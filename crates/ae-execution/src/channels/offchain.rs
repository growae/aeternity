// aesc_offchain_update:apply_on_trees, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use crate::{
    contracts::{
        channel,
        channel_auth::{error, native},
    },
    transactions::{Config, Error as TxError},
};
use ae_state::{
    calls::{Call, ReturnType},
    contracts::{Code, Contract, Nonce},
};
use ae_tx::{ContractOperation, ContractTransaction, channels::Update};
use ae_vm_fate::{maps::MapOrder, native::Term};

fn tokens<A: NodeWriter, C: NodeReader>(
    trees: &mut Trees<A, C>,
    key: AccountPublicKey,
    amount: &Unsigned,
    reserve: Option<&Unsigned>,
) -> Result<(), TxError<A, C>> {
    let mut a = trees
        .accounts
        .lookup(key)
        .map_err(|e| error(e.into()))?
        .ok_or_else(|| {
            native(Term::tagged(
                "not_present",
                vec![Term::Binary(key.0.to_vec())],
            ))
        })?;
    if let Some(reserve) = reserve {
        if a.balance() < &amount.add(reserve) {
            return Err(
                Error::Native(NativeFailure::OffchainUpdate("insufficient_balance")).into(),
            );
        }
        a.spend_without_nonce_bump(amount)
            .map_err(|e| error(e.into()))?;
    } else {
        a.earn(amount);
    }
    trees.accounts.enter(&a).map_err(|e| error(e.into()))?;
    Ok(())
}
// The caller owns publication. Both VM changes and preliminary amount moves
// remain staged until the complete update has returned successfully.
#[allow(clippy::too_many_arguments)]
pub fn apply<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    update: &Update,
    trees: &mut Trees<A, C>,
    onchain: &Trees<A, C>,
    context: &Context,
    round: &Unsigned,
    reserve: &Unsigned,
    config: &Config<'_, O>,
) -> Result<Option<Call>, TxError<A, C>> {
    let mut next = trees.clone();
    let call = match update {
        Update::Transfer { from, to, amount } => {
            tokens(&mut next, *from, amount, Some(reserve))?;
            tokens(&mut next, *to, amount, None)?;
            None
        }
        Update::Deposit { from, amount } => {
            tokens(&mut next, *from, amount, None)?;
            None
        }
        Update::Withdraw { to, amount } => {
            tokens(&mut next, *to, amount, Some(reserve))?;
            None
        }
        Update::Meta { .. } => None,
        Update::CreateContract {
            owner,
            version,
            code,
            deposit,
            call_data,
        } => {
            if !version.legal() {
                return Err(Error::Native(NativeFailure::IllegalContractVersion(*version)).into());
            }
            let contract = Contract::new(
                *owner,
                Nonce::Number(round),
                *version,
                Code::Inline(code.clone()),
                deposit.clone(),
            )
            .map_err(|e| error(e.into()))?;
            for key in [
                contract.public_key.as_slice(),
                ae_state::contracts::store_id(contract.public_key).as_slice(),
            ] {
                if next
                    .contracts
                    .tree()
                    .get(&ae_trie::Nibbles::from_bytes(key))
                    .map_err(|e| error(ae_state::contracts::Error::from(e).into()))?
                    .is_some()
                {
                    return Err(native(Term::tagged(
                        "already_present",
                        vec![Term::Binary(key.to_vec())],
                    )));
                }
            }
            next.contracts
                .insert(&contract, &BTreeMap::new())
                .map_err(|e| error(e.into()))?;
            tokens(&mut next, *owner, deposit, Some(reserve))?;
            let key = AccountPublicKey(contract.public_key);
            next.accounts
                .enter(&Account::new(key, 0.into()))
                .map_err(|e| error(e.into()))?;
            tokens(&mut next, key, deposit, None)?;
            let mut tx = ContractTransaction::new(
                Id::new(IdTag::Account, owner.0),
                ContractOperation::Create {
                    version: *version,
                    code: code.clone(),
                    deposit: deposit.clone(),
                    call_data: call_data.clone(),
                },
            )
            .map_err(Error::Codec)?;
            tx.set_nonce(round.clone());
            tx.set_gas(1_000_000.into());
            tx.set_gas_price(1.into());
            let call = Call::new(
                tx.actor(),
                round.clone(),
                contract.public_key,
                None,
                round.clone(),
                0.into(),
            );
            let completed = channel::run(&tx, &contract, call, next, onchain, context, config)?;
            next = completed.trees;
            insert_call(&mut next, &completed.call)?;
            None
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
            tokens(&mut next, *caller, amount, Some(reserve))?;
            tokens(&mut next, AccountPublicKey(*contract), amount, None)?;
            let call = Call::new(
                Id::new(IdTag::Account, caller.0),
                round.clone(),
                *contract,
                None,
                round.clone(),
                gas_price.clone(),
            );
            let loaded = channel::load(*contract, &next)?;
            if abi_version != &u64::from(loaded.version.abi).into() {
                return Err(native(Term::Atom("wrong_abi_version")));
            }
            let mut tx = ContractTransaction::new(
                call.caller,
                ContractOperation::Call {
                    contract: Id::new(IdTag::Contract, *contract),
                    abi_version: abi_version.clone(),
                    call_data: call_data.clone(),
                },
            )
            .map_err(Error::Codec)?;
            tx.set_nonce(round.clone());
            tx.set_amount(amount.clone());
            tx.set_gas(gas.clone());
            tx.set_gas_price(gas_price.clone());
            tx.set_call_context(caller.0, call_stack.clone())
                .map_err(Error::Codec)?;
            let completed = channel::run(&tx, &loaded, call, next, onchain, context, config)?;
            next = completed.trees;
            let call = completed.call;
            insert_call(&mut next, &call)?;
            if call.return_type != ReturnType::Ok
                && config
                    .network
                    .protocol_at(context.height)
                    .map_err(Error::Profile)?
                    >= Protocol::Iris
            {
                tokens(
                    &mut next,
                    AccountPublicKey(*contract),
                    amount,
                    Some(&0.into()),
                )?;
                tokens(&mut next, *caller, amount, None)?;
            }
            Some(call)
        }
    };
    *trees = next;
    Ok(call)
}
fn insert_call<A: NodeReader, C: NodeWriter>(
    trees: &mut Trees<A, C>,
    call: &Call,
) -> Result<(), TxError<A, C>> {
    trees.calls.insert(call).map_err(|e| match e {
        ae_state::contracts::Error::AlreadyPresent => native(Term::tagged(
            "already_present",
            vec![Term::Binary([call.target(), call.id.as_slice()].concat())],
        )),
        e => error(e.into()),
    })
}
