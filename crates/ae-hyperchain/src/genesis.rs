// aec_consensus_hc genesis feeding and aec_block_fork, aeternity fa2a6d7.
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use crate::{consensus_contract::Contracts, epochs};
use ae_execution::{Context, transactions};
use ae_protocol::contracts::Version;
use ae_state::{
    Trees,
    accounts::{Account, AccountError},
    calls::{self, ReturnType},
    contracts::{self, LoadOptions},
};
use ae_trie::{MemoryStore, NodeWriter};
use ae_tx::{ContractOperation, ContractTransaction, Transaction, TransactionLeaf};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use ae_vm_fate::{
    abi,
    maps::MapOrder,
    value::{ObjectKind, ObjectPayload, Value},
};
use num_bigint::{BigInt, Sign};

// aec_consensus_hc:genesis_raw_header/0 followed by the state-root assignment
// in aec_block_genesis. Genesis has no release-info extension at any protocol.
pub fn header(
    network: &ae_protocol::NetworkProfile,
    start_time: u64,
    roots: &ae_state::StateRoots,
) -> Result<ae_codec::headers::KeyHeader, ae_protocol::ProfileError> {
    Ok(ae_codec::headers::KeyHeader {
        common: ae_codec::headers::CommonHeader {
            version: network.protocol_at(0)? as u32,
            height: 0,
            previous_hash: ae_state::genesis::CONTRIBUTORS_MESSAGES_HASH,
            previous_key_hash: [0; 32],
            state_hash: roots.hash().0,
        },
        miner: [0; 32],
        beneficiary: [0; 32],
        target: 0,
        seal: [0; 42],
        nonce: 0,
        time: start_time,
        info: None,
        hole: false,
        end_of_epoch: false,
    })
}

pub const CREATE_GAS: u64 = 10_000_000;
pub const CREATE_GAS_PRICE: u64 = 10_000_000_000;
pub const DUMMY_FEE: u64 = 1_000_000_000_000_000;
#[derive(Debug, Clone)]
pub struct Deployment {
    pub owner: AccountPublicKey,
    pub public_key: [u8; 32],
    pub nonce: Unsigned,
    pub amount: Unsigned,
    pub version: Version,
    pub code: Vec<u8>,
    pub calldata: Vec<u8>,
}
#[derive(Debug, Clone)]
pub struct InitialCall {
    pub caller: AccountPublicKey,
    pub contract: [u8; 32],
    pub nonce: Unsigned,
    pub abi: Unsigned,
    // The supplied fee contributes to the dummy transaction's encoded size.
    // Replace it once with that transaction's minimum fee, as the original does.
    pub fee: Unsigned,
    pub amount: Unsigned,
    pub gas: Unsigned,
    pub gas_price: Unsigned,
    pub calldata: Vec<u8>,
}
#[derive(Debug, Clone)]
pub struct Validator {
    pub owner: AccountPublicKey,
    pub sign_key: AccountPublicKey,
    pub caller: AccountPublicKey,
    pub stake: Unsigned,
    pub restake: bool,
}
#[derive(Debug, Clone)]
pub struct Plan {
    pub contracts: Contracts,
    pub deployments: Vec<Deployment>,
    pub calls: Vec<InitialCall>,
    pub validators: Vec<Validator>,
    pub epoch_length: BigInt,
    pub pin_reward: BigInt,
}
#[derive(Debug)]
pub enum Error<A = MemoryStore, C = MemoryStore> {
    Profile(ae_protocol::ProfileError),
    Transaction(ae_tx::Error),
    Account(AccountError),
    Contract(contracts::Error),
    Calldata(ae_vm_fate::Error),
    Execution(Box<transactions::Error<A, C>>),
    Consensus(Box<crate::consensus_contract::Error<A, C>>),
    UnexpectedPublicKey {
        computed: [u8; 32],
        expected: [u8; 32],
    },
    MissingAccount(AccountPublicKey),
    MissingContract([u8; 32]),
    MissingCall([u8; 32]),
    CallFailed {
        return_type: ReturnType,
        value: Vec<u8>,
    },
    InvalidAmount,
}
fn number(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
fn unsigned<A, C>(n: BigInt) -> Result<Unsigned, Error<A, C>> {
    if n.sign() == Sign::Minus {
        return Err(Error::InvalidAmount);
    }
    Ok(Unsigned::from_field_bytes(&n.to_bytes_be().1).expect("nonnegative amount"))
}
fn price<A, C>(tx: &mut ContractTransaction, context: &Context) -> Result<(), Error<A, C>> {
    let fee = Transaction::new(TransactionLeaf::Contract(tx.clone()))
        .view()
        .pricing(&context.height.into(), context.protocol)
        .minimum_fee;
    tx.set_fee(unsigned(fee)?);
    Ok(())
}
pub fn create_transaction<A, C>(
    s: &Deployment,
    context: &Context,
) -> Result<ContractTransaction, Error<A, C>> {
    let mut tx = ContractTransaction::new(
        Id::new(IdTag::Account, s.owner.0),
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
    tx.set_gas(CREATE_GAS.into());
    tx.set_gas_price(CREATE_GAS_PRICE.into());
    tx.set_fee(DUMMY_FEE.into());
    price(&mut tx, context)?;
    let computed = contracts::contract_id(s.owner, contracts::Nonce::Number(&s.nonce)).0;
    if computed != s.public_key {
        return Err(Error::UnexpectedPublicKey {
            computed,
            expected: s.public_key,
        });
    }
    Ok(tx)
}
pub fn call_transaction<A, C>(
    s: &InitialCall,
    context: &Context,
) -> Result<ContractTransaction, Error<A, C>> {
    let mut tx = ContractTransaction::new(
        Id::new(IdTag::Account, s.caller.0),
        ContractOperation::Call {
            contract: Id::new(IdTag::Contract, s.contract),
            abi_version: s.abi.clone(),
            call_data: s.calldata.clone(),
        },
    )
    .map_err(Error::Transaction)?;
    tx.set_nonce(s.nonce.clone());
    tx.set_amount(s.amount.clone());
    tx.set_gas(s.gas.clone());
    tx.set_gas_price(s.gas_price.clone());
    tx.set_fee(s.fee.clone());
    price(&mut tx, context)?;
    Ok(tx)
}
// Calls here retain the calls tree until final genesis pruning, unlike ordinary
// internal consensus calls. Every stage owns its candidate roots.
fn apply<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    mut trees: Trees<A, C>,
    tx: ContractTransaction,
    key: [u8; 32],
    context: &Context,
    execution: &transactions::Config<'_, O>,
) -> Result<Trees<A, C>, Error<A, C>> {
    let id = calls::call_id(tx.signer().0, tx.nonce(), key).0;
    transactions::process(
        &Transaction::new(TransactionLeaf::Contract(tx)),
        &mut trees,
        context,
        execution,
    )
    .map_err(|e| Error::Execution(Box::new(e)))?;
    let call = trees
        .calls
        .lookup(&key, id)
        .map_err(Error::Contract)?
        .ok_or(Error::MissingCall(id))?;
    if call.return_type != ReturnType::Ok {
        return Err(Error::CallFailed {
            return_type: call.return_type,
            value: call.return_value,
        });
    }
    trees
        .contracts
        .lookup(
            key,
            LoadOptions {
                full_store_cache: true,
                ..Default::default()
            },
        )
        .map_err(Error::Contract)?
        .ok_or(Error::MissingContract(key))?;
    trees
        .accounts
        .lookup(AccountPublicKey(key))
        .map_err(Error::Account)?
        .ok_or(Error::MissingAccount(AccountPublicKey(key)))?;
    Ok(trees)
}
pub fn deploy<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    trees: &Trees<A, C>,
    spec: &Deployment,
    context: &Context,
    execution: &transactions::Config<'_, O>,
) -> Result<Trees<A, C>, Error<A, C>> {
    let tx = create_transaction(spec, context)?;
    // prepare_contract_owner uses the effective network protocol for gas_limit,
    // while min_fee above uses the explicit transaction-environment protocol.
    let gas = Transaction::new(TransactionLeaf::Contract(tx.clone()))
        .view()
        .pricing(
            &context.height.into(),
            execution
                .network
                .protocol_at(context.height)
                .map_err(Error::Profile)?,
        )
        .gas_limit;
    let funds = unsigned(number(tx.gas_price()) * gas + number(tx.amount()) + number(tx.fee()))?;
    let mut owner = trees
        .accounts
        .lookup(spec.owner)
        .map_err(Error::Account)?
        .unwrap_or_else(|| Account::new(spec.owner, 0.into()));
    owner.earn(&funds);
    let mut next = trees.clone();
    next.accounts.enter(&owner).map_err(Error::Account)?;
    apply(next, tx, spec.public_key, context, execution)
}
pub fn call<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    trees: &Trees<A, C>,
    spec: &InitialCall,
    context: &Context,
    execution: &transactions::Config<'_, O>,
) -> Result<Trees<A, C>, Error<A, C>> {
    apply(
        trees.clone(),
        call_transaction(spec, context)?,
        spec.contract,
        context,
        execution,
    )
}
pub fn register_validator<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    trees: &Trees<A, C>,
    staking: [u8; 32],
    v: &Validator,
    context: &Context,
    execution: &transactions::Config<'_, O>,
) -> Result<Trees<A, C>, Error<A, C>> {
    let calldata = abi::create_calldata(
        b"new_validator",
        vec![
            Value::Object(
                ObjectKind::Address,
                ObjectPayload::Bytes(v.owner.0.to_vec()),
            ),
            Value::Object(
                ObjectKind::Address,
                ObjectPayload::Bytes(v.sign_key.0.to_vec()),
            ),
            Value::Boolean(v.restake),
        ],
        execution.fate_limits,
    )
    .map_err(Error::Calldata)?;
    let caller = trees
        .accounts
        .lookup(v.caller)
        .map_err(Error::Account)?
        .ok_or(Error::MissingAccount(v.caller))?;
    let contract = trees
        .contracts
        .lookup(staking, LoadOptions::default())
        .map_err(Error::Contract)?
        .ok_or(Error::MissingContract(staking))?;
    call(
        trees,
        &InitialCall {
            caller: v.caller,
            contract: staking,
            nonce: caller.nonce().add(&1.into()),
            abi: u64::from(contract.contract.version.abi).into(),
            fee: DUMMY_FEE.into(),
            amount: v.stake.clone(),
            gas: 1_000_000.into(),
            gas_price: 1_000_000_000.into(),
            calldata,
        },
        context,
        execution,
    )
}
pub fn transform<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    trees: &Trees<A, C>,
    plan: &Plan,
    context: &Context,
    execution: &transactions::Config<'_, O>,
) -> Result<Trees<A, C>, Error<A, C>> {
    let mut next = trees.clone();
    for spec in &plan.deployments {
        next = deploy(&next, spec, context, execution)?;
    }
    for spec in &plan.calls {
        next = call(&next, spec, context, execution)?;
    }
    for validator in &plan.validators {
        next = register_validator(&next, plan.contracts.staking, validator, context, execution)?;
    }
    next = epochs::apply(
        &next,
        plan.contracts,
        &epochs::Transition::Initialize {
            length: plan.epoch_length.clone(),
            pin_reward: plan.pin_reward.clone(),
        },
        context,
        execution,
    )
    .map_err(|e| Error::Consensus(Box::new(e)))?;
    next.calls.prune();
    Ok(next)
}
