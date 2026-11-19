// aec_consensus_hc:call_consensus_contract_result/call_consensus_contract_,
// aeternity fa2a6d7. Copyright (c) Aeternity. ISC notice in repository LICENSE.
use ae_execution::{Context, transactions};
use ae_state::{
    Trees,
    calls::{self, Call, ReturnType},
    contracts::LoadOptions,
};
use ae_trie::{MemoryStore, NodeWriter};
use ae_tx::{ContractOperation, ContractTransaction, Transaction, TransactionLeaf};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use ae_vm_fate::{
    Limits, abi, encoding,
    maps::MapOrder,
    native::{self, Term},
    value::Value,
};
use num_bigint::{BigInt, Sign};

pub const FEE: u64 = 5_000_000_000_000_000_000;
pub const GAS: u64 = FEE;
pub const GAS_PRICE: u64 = 50_000_000_000;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Contract {
    Election,
    Staking,
    Rewards,
}
#[derive(Debug, Clone, Copy)]
pub struct Contracts {
    pub owner: AccountPublicKey,
    pub election: [u8; 32],
    pub staking: [u8; 32],
    pub rewards: [u8; 32],
}
impl Contracts {
    pub fn public_key(&self, contract: Contract) -> [u8; 32] {
        match contract {
            Contract::Election => self.election,
            Contract::Staking => self.staking,
            Contract::Rewards => self.rewards,
        }
    }
}
#[derive(Debug)]
pub enum Error<A = MemoryStore, C = MemoryStore> {
    Calldata(ae_vm_fate::Error),
    ContractState(ae_state::contracts::Error),
    AccountState(ae_state::accounts::AccountError),
    MissingContract([u8; 32]),
    MissingOwner(AccountPublicKey),
    Transaction(ae_tx::Error),
    Execution(Box<transactions::Error<A, C>>),
    MissingCall([u8; 32]),
    CallState(ae_state::contracts::Error),
    Failed(Value),
    FailureDecode(encoding::DecodingFailure),
    ResultDecode(encoding::DecodingFailure),
}
impl<A, C> Error<A, C> {
    // Unknown backend/execution failures remain operational. Only modeled
    // original rejections/exceptions may terminate a cache target callback.
    pub fn is_native(&self) -> bool {
        match self {
            Self::MissingContract(_)
            | Self::MissingOwner(_)
            | Self::MissingCall(_)
            | Self::Transaction(_)
            | Self::Failed(_) => true,
            Self::Calldata(e) => *e != ae_vm_fate::Error::ResourceLimit,
            Self::FailureDecode(e) | Self::ResultDecode(e) => e.exception.is_some(),
            Self::Execution(e) => match e.as_ref() {
                transactions::Error::Contract(e) => match e.as_ref() {
                    ae_execution::contracts::Error::Primitive(e) => native_primitive(e),
                    e => matches!(
                        e,
                        ae_execution::contracts::Error::Rejected(_)
                            | ae_execution::contracts::Error::Exception(_)
                            | ae_execution::contracts::Error::NativeException(_)
                            | ae_execution::contracts::Error::IllegalContractVersion(_)
                            | ae_execution::contracts::Error::AuthenticationResultCase(_)
                    ),
                },
                transactions::Error::Primitive(e) => native_primitive(e),
                transactions::Error::MinimumFeeCaseClause(_)
                | transactions::Error::ContractCallContextCase(_) => true,
                _ => false,
            },
            _ => false,
        }
    }
    // Exceptions raised in Erlang's try-of clause body are not caught by that
    // try's catch. Failed-return decoding and consensus_call_failed therefore
    // escape directly, just like successful-result decoding in the caller.
    pub fn return_exception(&self) -> Option<native::Exception> {
        match self {
            Self::Failed(value) => Some(native::Exception::error(Term::tagged(
                "consensus_call_failed",
                vec![Term::tagged(
                    "error",
                    vec![Term::Data(Box::new(value.clone()))],
                )],
            ))),
            Self::FailureDecode(f) | Self::ResultDecode(f) => f.exception.map(|e| match e {
                encoding::DecodeException::Error(s) => native::Exception::error(Term::Atom(s)),
                encoding::DecodeException::Exit(s) => native::Exception::exit(Term::Atom(s)),
            }),
            _ => None,
        }
    }
}
fn native_primitive(e: &ae_execution::Error) -> bool {
    matches!(
        e,
        ae_execution::Error::ReferenceException { .. }
            | ae_execution::Error::TooLowFee
            | ae_execution::Error::TtlExpired
            | ae_execution::Error::InvalidAtProtocol
            | ae_execution::Error::AccountNotFound
            | ae_execution::Error::NotBasicAccount
            | ae_execution::Error::InsufficientFunds
            | ae_execution::Error::NonceAlreadyUsed
            | ae_execution::Error::NonceTooHigh
            | ae_execution::Error::AccountNotPayable
            | ae_execution::Error::BadTransaction
            | ae_execution::Error::NotGeneralizedAccount
            | ae_execution::Error::GeneralizedAccountsUnavailable
            | ae_execution::Error::GaNonceMustBeZero
            | ae_execution::Error::BadGaEnvironment
    )
}
pub struct Prepared<A, C> {
    pub trees: Trees<A, C>,
    pub transaction: ContractTransaction,
}
pub struct Executed<A, C> {
    pub trees: Trees<A, C>,
    pub call: Call,
}

pub fn prepare<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    contracts: Contracts,
    contract: Contract,
    trees: &Trees<A, C>,
    calldata: Vec<u8>,
    amount: Unsigned,
) -> Result<Prepared<A, C>, Error<A, C>> {
    let key = contracts.public_key(contract);
    // Original lookup order includes the contract store before the owner.
    let record = trees
        .contracts
        .lookup(key, LoadOptions::default())
        .map_err(Error::ContractState)?
        .ok_or(Error::MissingContract(key))?;
    let mut owner = trees
        .accounts
        .lookup(contracts.owner)
        .map_err(Error::AccountState)?
        .ok_or(Error::MissingOwner(contracts.owner))?;
    let nonce = owner.nonce().add(&1.into());
    let funding = BigInt::from(FEE) + BigInt::from(GAS) * BigInt::from(GAS_PRICE);
    let (sign, bytes) = funding.to_bytes_be();
    debug_assert_ne!(sign, Sign::Minus);
    owner.earn(&Unsigned::from_field_bytes(&bytes).expect("nonnegative funding"));
    let mut next = trees.clone();
    next.accounts.enter(&owner).map_err(Error::AccountState)?;
    let mut transaction = ContractTransaction::new(
        Id::new(IdTag::Account, contracts.owner.0),
        ContractOperation::Call {
            contract: Id::new(IdTag::Contract, key),
            abi_version: u64::from(record.contract.version.abi).into(),
            call_data: calldata,
        },
    )
    .map_err(Error::Transaction)?;
    transaction.set_nonce(nonce);
    transaction.set_fee(FEE.into());
    transaction.set_gas(GAS.into());
    transaction.set_gas_price(GAS_PRICE.into());
    transaction.set_amount(amount);
    Ok(Prepared {
        trees: next,
        transaction,
    })
}
// The caller owns the returned roots. Neither success nor failure mutates the
// input view; a read-only query discards all funded/executed/pruned roots.
pub fn execute<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    prepared: Prepared<A, C>,
    context: &Context,
    execution: &transactions::Config<'_, O>,
) -> Result<Executed<A, C>, Error<A, C>> {
    let Prepared {
        mut trees,
        transaction,
    } = prepared;
    let ContractOperation::Call { contract, .. } = transaction.operation() else {
        unreachable!("prepared call")
    };
    let key = contract.value;
    let id = calls::call_id(transaction.signer().0, transaction.nonce(), key).0;
    transactions::process(
        &Transaction::new(TransactionLeaf::Contract(transaction)),
        &mut trees,
        context,
        execution,
    )
    .map_err(|e| Error::Execution(Box::new(e)))?;
    // Do the actual state lookup: GA call IDs must not be substituted from a
    // receipt when the original plain call ID would be absent.
    let call = trees
        .calls
        .lookup(&key, id)
        .map_err(Error::CallState)?
        .ok_or(Error::MissingCall(id))?;
    if call.return_type != ReturnType::Ok {
        let value = encoding::decode_for_vm(&call.return_value, execution.fate_limits)
            .map_err(Error::FailureDecode)?;
        return Err(Error::Failed(value));
    }
    trees.calls.prune();
    Ok(Executed { trees, call })
}
#[allow(clippy::too_many_arguments)]
pub fn query<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    contracts: Contracts,
    contract: Contract,
    trees: &Trees<A, C>,
    context: &Context,
    execution: &transactions::Config<'_, O>,
    endpoint: &[u8],
    arguments: Vec<Value>,
    limits: Limits,
) -> Result<Value, Error<A, C>> {
    let calldata = abi::create_calldata(endpoint, arguments, limits).map_err(Error::Calldata)?;
    let prepared = prepare(contracts, contract, trees, calldata, 0.into())?;
    let executed = execute(prepared, context, execution)?;
    encoding::decode_for_vm(&executed.call.return_value, limits).map_err(Error::ResultDecode)
}
