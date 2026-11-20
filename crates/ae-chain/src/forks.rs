// aec_block_fork and aec_fork_block_settings, aeternity fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::rewards::{self, number, unsigned};
use ae_codec::api::{self, ApiType};
use ae_execution::{Context, transactions};
use ae_protocol::{NetworkProfile, Protocol, contracts::Version};
use ae_state::{
    Trees,
    accounts::Account,
    calls::{self, ReturnType},
    contracts::{self, LoadOptions},
};
use ae_trie::{MemoryStore, NodeWriter};
use ae_tx::{ContractOperation, ContractTransaction, Transaction, TransactionLeaf};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use ae_vm_fate::maps::MapOrder;
use num_bigint::BigInt;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub struct Allocations {
    pub accounts: Vec<(AccountPublicKey, Unsigned)>,
    pub extra: Vec<(AccountPublicKey, Unsigned)>,
}
#[derive(Debug, Clone)]
pub struct Deployment {
    pub key: [u8; 32],
    pub nonce: Unsigned,
    pub amount: Unsigned,
    pub version: Version,
    pub code: Vec<u8>,
    pub calldata: Vec<u8>,
}
#[derive(Debug, Clone, Default)]
pub struct Migration {
    pub allocations: Allocations,
    pub contracts: Vec<Deployment>,
}
pub type CustomForks = BTreeMap<Protocol, Allocations>;
#[derive(Debug)]
pub enum Error<A = MemoryStore, C = MemoryStore> {
    Profile(ae_protocol::ProfileError),
    Asset,
    ResourceLimit,
    Account(ae_state::accounts::AccountError),
    Contract(contracts::Error),
    Allocation(rewards::Error),
    Transaction(ae_tx::Error),
    Execution(Box<transactions::Error<A, C>>),
    UnexpectedPublicKey,
    MissingAccount,
    MissingContract,
    MissingCall,
    CallFailed {
        return_type: ReturnType,
        value: Vec<u8>,
    },
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_accounts: usize,
    pub max_contracts: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_accounts: 100_000,
            max_contracts: 1000,
        }
    }
}
fn amount(v: &serde_json::Value) -> Result<Unsigned, Error> {
    let n = v.as_number().ok_or(Error::Asset)?;
    Unsigned::from_decimal(&n.to_string()).map_err(|_| Error::Asset)
}
fn accounts(json: &str) -> Result<Vec<(AccountPublicKey, Unsigned)>, Error> {
    let v: serde_json::Value = serde_json::from_str(json).map_err(|_| Error::Asset)?;
    let o = v.as_object().ok_or(Error::Asset)?;
    let mut out = Vec::with_capacity(o.len());
    for (k, v) in o {
        let k = api::decode_as(ApiType::AccountPubkey, k).map_err(|_| Error::Asset)?;
        out.push((
            AccountPublicKey(k.try_into().map_err(|_| Error::Asset)?),
            amount(v)?,
        ));
    }
    out.sort_by_key(|(k, _)| *k);
    Ok(out)
}
pub fn public(network: &NetworkProfile, protocol: Protocol) -> Result<Migration, Error> {
    let (primary, extra, code) = match (network.network_id(), protocol) {
        (b"ae_mainnet", Protocol::Minerva) => (
            include_str!("../data/mainnet_minerva_accounts.json"),
            "{}",
            "{}",
        ),
        (b"ae_uat", Protocol::Minerva) => (
            include_str!("../data/uat_minerva_accounts.json"),
            "{}",
            "{}",
        ),
        (b"ae_mainnet", Protocol::Fortuna) => (
            include_str!("../data/mainnet_fortuna_accounts.json"),
            "{}",
            "{}",
        ),
        (b"ae_uat", Protocol::Fortuna) => (
            include_str!("../data/uat_fortuna_accounts.json"),
            "{}",
            "{}",
        ),
        (b"ae_mainnet", Protocol::Lima) => (
            include_str!("../data/mainnet_lima_accounts.json"),
            include_str!("../data/mainnet_lima_extra_accounts.json"),
            include_str!("../data/mainnet_lima_contracts.json"),
        ),
        (b"ae_uat", Protocol::Lima) => (
            include_str!("../data/uat_lima_accounts.json"),
            include_str!("../data/uat_lima_extra_accounts.json"),
            include_str!("../data/uat_lima_contracts.json"),
        ),
        _ => return Ok(Migration::default()),
    };
    let v: serde_json::Value = serde_json::from_str(code).map_err(|_| Error::Asset)?;
    let o = v.as_object().ok_or(Error::Asset)?;
    let mut contracts = vec![];
    for (k, v) in o {
        let o = v.as_object().ok_or(Error::Asset)?;
        if o.len() != 6 {
            return Err(Error::Asset);
        }
        let field = |k: &str| o.get(k).ok_or(Error::Asset);
        let binary = |k: &str| {
            api::decode_as(
                ApiType::ContractBytearray,
                field(k)?.as_str().ok_or(Error::Asset)?,
            )
            .map_err(|_| Error::Asset)
        };
        let byte = |k: &str| {
            u16::try_from(amount(field(k)?)?.to_u64().map_err(|_| Error::Asset)?)
                .map_err(|_| Error::Asset)
        };
        contracts.push(Deployment {
            key: api::decode_as(ApiType::ContractPubkey, k)
                .map_err(|_| Error::Asset)?
                .try_into()
                .map_err(|_| Error::Asset)?,
            amount: amount(field("amount")?)?,
            nonce: amount(field("nonce")?)?,
            version: Version {
                vm: byte("vm_version")?,
                abi: byte("abi_version")?,
            },
            code: binary("code")?,
            calldata: binary("call_data")?,
        });
    }
    Ok(Migration {
        allocations: Allocations {
            accounts: accounts(primary)?,
            extra: accounts(extra)?,
        },
        contracts,
    })
}
pub fn create_transaction<A, C>(
    s: &Deployment,
    context: &Context,
    gas: &Unsigned,
) -> Result<ContractTransaction, Error<A, C>> {
    let mut tx = ContractTransaction::new(
        Id::new(IdTag::Account, [0; 32]),
        ContractOperation::Create {
            version: s.version,
            code: s.code.clone(),
            deposit: 0.into(),
            call_data: s.calldata.clone(),
        },
    )
    .map_err(Error::Transaction)?;
    tx.set_nonce(s.nonce.clone());
    tx.set_amount(s.amount.clone());
    tx.set_gas(gas.clone());
    tx.set_gas_price(context.protocol.minimum_gas_price().into());
    tx.set_fee(1_000_000_000_000_000u64.into());
    let fee = Transaction::new(TransactionLeaf::Contract(tx.clone()))
        .view()
        .pricing(&context.height.into(), context.protocol)
        .minimum_fee;
    tx.set_fee(unsigned(fee));
    if contracts::contract_id(AccountPublicKey::ZERO, contracts::Nonce::Number(&s.nonce)).0 != s.key
    {
        return Err(Error::UnexpectedPublicKey);
    }
    Ok(tx)
}
pub fn apply<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    trees: &mut Trees<A, C>,
    migration: &Migration,
    context: &Context,
    execution: &transactions::Config<'_, O>,
    configured_gas: &Unsigned,
    limits: Limits,
) -> Result<(), Error<A, C>> {
    if migration
        .allocations
        .accounts
        .len()
        .saturating_add(migration.allocations.extra.len())
        > limits.max_accounts
        || migration.contracts.len() > limits.max_contracts
    {
        return Err(Error::ResourceLimit);
    }
    let mut original = trees.clone();
    for accounts in [
        &migration.allocations.accounts,
        &migration.allocations.extra,
    ] {
        let mut accounts = accounts.iter().collect::<Vec<_>>();
        accounts.sort_by_key(|(k, _)| *k);
        for (k, n) in accounts {
            rewards::credit(&mut original.accounts, *k, n).map_err(Error::Allocation)?;
        }
    }
    if migration.contracts.is_empty() {
        *trees = original;
        return Ok(());
    }
    let gas = execution.network.block_gas_limit(configured_gas);
    let mut specs = migration.contracts.iter().collect::<Vec<_>>();
    specs.sort_by(|a, b| a.nonce.cmp(&b.nonce));
    let txs = specs
        .iter()
        .map(|s| create_transaction(s, context, &gas))
        .collect::<Result<Vec<_>, _>>()?;
    let first = &txs[0];
    let first_gas = Transaction::new(TransactionLeaf::Contract(first.clone()))
        .view()
        .pricing(
            &context.height.into(),
            execution
                .network
                .protocol_at(context.height)
                .map_err(Error::Profile)?,
        )
        .gas_limit;
    let funds = number(first.gas_price()) * first_gas * txs.len()
        + txs
            .iter()
            .map(|t| number(t.fee()) + number(t.amount()))
            .sum::<BigInt>();
    let mut working = original.clone();
    rewards::credit(
        &mut working.accounts,
        AccountPublicKey::ZERO,
        &unsigned(funds),
    )
    .map_err(Error::Allocation)?;
    let mut deployed = vec![];
    for (s, tx) in specs.into_iter().zip(txs) {
        transactions::process(
            &Transaction::new(TransactionLeaf::Contract(tx)),
            &mut working,
            context,
            execution,
        )
        .map_err(|e| Error::Execution(Box::new(e)))?;
        let id = calls::call_id([0; 32], &s.nonce, s.key).0;
        let call = working
            .calls
            .lookup(&s.key, id)
            .map_err(Error::Contract)?
            .ok_or(Error::MissingCall)?;
        if call.return_type != ReturnType::Ok {
            return Err(Error::CallFailed {
                return_type: call.return_type,
                value: call.return_value,
            });
        }
        let c = working
            .contracts
            .lookup(
                s.key,
                LoadOptions {
                    full_store_cache: true,
                    ..Default::default()
                },
            )
            .map_err(Error::Contract)?
            .ok_or(Error::MissingContract)?;
        let contents = c
            .store
            .ok_or(Error::MissingContract)?
            .contents()
            .map_err(Error::Contract)?;
        let a = working
            .accounts
            .lookup(AccountPublicKey(s.key))
            .map_err(Error::Account)?
            .ok_or(Error::MissingAccount)?;
        deployed.push((c.contract, contents, a));
    }
    let nonce = working
        .accounts
        .lookup(AccountPublicKey::ZERO)
        .map_err(Error::Account)?
        .ok_or(Error::MissingAccount)?
        .nonce()
        .clone();
    // Keep each deployment's captured result, not later contracts' side effects.
    for (c, store, a) in deployed {
        original
            .contracts
            .insert(&c, &store)
            .map_err(Error::Contract)?;
        original.accounts.enter(&a).map_err(Error::Account)?;
    }
    let mut owner = original
        .accounts
        .lookup(AccountPublicKey::ZERO)
        .map_err(Error::Account)?
        .unwrap_or_else(|| Account::new(AccountPublicKey::ZERO, 0.into()));
    owner.set_nonce(nonce);
    original.accounts.enter(&owner).map_err(Error::Account)?;
    *trees = original;
    Ok(())
}
