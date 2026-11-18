// aefa_chain_api.erl / aeprimop.erl, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use ae_codec::{object::ObjectError, rlp, sophia::SophiaCode};
use ae_state::{
    accounts::{Account, AccountError, Accounts},
    contracts::{self, Code as ContractCode, Contracts},
};
use ae_trie::{NodeReader, NodeWriter};
use ae_tx::Spend;
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use ae_vm_fate::{
    Limits,
    code::{Code, CodeError},
    types::Version,
};
pub mod history;
pub mod oracle;
pub mod remote;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Accounts(AccountError),
    Contracts(contracts::Error),
    ResourceLimit,
    InvalidReferenceId(Id),
    UnsupportedContractKey(Vec<u8>),
    Oracles(ae_state::oracles::Error),
    UnsupportedAccountKey(Vec<u8>),
    UnsupportedOracleKey(Vec<u8>),
    NeedHeader([u8; 32]),
    NeedHeight(u64),
}
impl From<AccountError> for Error {
    fn from(e: AccountError) -> Self {
        Self::Accounts(e)
    }
}
impl From<contracts::Error> for Error {
    fn from(e: contracts::Error) -> Self {
        Self::Contracts(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedCode {
    pub code: Code,
    // A clone runs under its own VM version, not that of its code reference.
    pub vm_version: Version,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeException {
    InvalidReferenceId(Id),
    MissingReference(Id),
    DeepReference(Id),
    Sophia(ObjectError),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeLookup {
    Loaded(LoadedCode),
    Unavailable,
    // These occur outside the original bytecode decoder's catch boundary.
    Exception(CodeException),
}

// Loading does not populate the VM code cache or charge its fetch gas. Those
// checkpoints belong to the engine, which may discard a failed lookup charge.
pub fn contract_fate_bytecode<S: NodeReader>(
    contracts: &Contracts<S>,
    public_key: [u8; 32],
    limits: Limits,
) -> Result<CodeLookup, Error> {
    let Some(contract) = contracts.record(public_key)? else {
        return Ok(CodeLookup::Unavailable);
    };
    let vm_version = match contract.version.vm {
        5 => Version::One,
        7 => Version::Two,
        8 => Version::Three,
        _ => return Ok(CodeLookup::Unavailable),
    };
    // No activation, ABI, active flag, payable flag or store validation here:
    // this is the original chain lookup, not transaction admission.
    let bytes = match contract.code {
        ContractCode::Inline(bytes) => bytes,
        ContractCode::Reference(id) => {
            if id.tag != IdTag::Contract {
                return Ok(CodeLookup::Exception(CodeException::InvalidReferenceId(id)));
            }
            let Some(reference) = contracts.record(id.value)? else {
                return Ok(CodeLookup::Exception(CodeException::MissingReference(id)));
            };
            match reference.code {
                ContractCode::Inline(bytes) => bytes,
                ContractCode::Reference(id) => {
                    return Ok(CodeLookup::Exception(CodeException::DeepReference(id)));
                }
            }
        }
    };
    let wrapper = match SophiaCode::decode_with_limits(
        &bytes,
        rlp::DecodeLimits {
            max_input_bytes: limits.max_bytes,
            max_items: limits.max_nodes,
            max_depth: limits.max_depth,
        },
    ) {
        Ok(code) => code,
        Err(ObjectError::Rlp(rlp::DecodeError::ResourceLimit)) => return Err(Error::ResourceLimit),
        Err(e) => return Ok(CodeLookup::Exception(CodeException::Sophia(e))),
    };
    match Code::decode(&wrapper.byte_code, limits) {
        Ok(code) => Ok(CodeLookup::Loaded(LoadedCode { code, vm_version })),
        Err(CodeError::Data(ae_vm_fate::Error::ResourceLimit)) => Err(Error::ResourceLimit),
        Err(_) => Ok(CodeLookup::Unavailable),
    }
}

pub fn contract_vm_version<S: NodeReader>(
    contracts: &Contracts<S>,
    public_key: [u8; 32],
) -> Result<Option<u16>, Error> {
    Ok(contracts.record(public_key)?.map(|c| c.version.vm))
}

// Unlike bytecode loading, clone creation resolves arbitrary reference chains
// and reports the final contract's version. A cycle/work limit is operational;
// the original recursion does not manufacture a VM rejection for a cycle.
pub fn contract_find_final_ref<S: NodeReader>(
    contracts: &Contracts<S>,
    mut public_key: [u8; 32],
    maximum_lookups: usize,
) -> Result<Option<([u8; 32], u16)>, Error> {
    for _ in 0..maximum_lookups {
        let Some(contract) = contracts.record(public_key)? else {
            return Ok(None);
        };
        match contract.code {
            ContractCode::Inline(_) => return Ok(Some((public_key, contract.version.vm))),
            ContractCode::Reference(id) => {
                if id.tag != IdTag::Contract {
                    return Err(Error::InvalidReferenceId(id));
                }
                public_key = id.value;
            }
        }
    }
    Err(Error::ResourceLimit)
}

pub fn account_balance<S: NodeReader, C: NodeReader>(
    accounts: &Accounts<S>,
    onchain: Option<&Accounts<C>>,
    public_key: AccountPublicKey,
) -> Result<Option<Unsigned>, Error> {
    if let Some(account) = accounts.lookup(public_key)? {
        return Ok(Some(account.balance().clone()));
    }
    Ok(match onchain {
        Some(chain) => chain.lookup(public_key)?.map(|a| a.balance().clone()),
        None => None,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferFailure {
    AccountNotFound,
    InsufficientFunds,
    AccountNotPayable,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferEvent {
    Delta {
        account: AccountPublicKey,
        debit: bool,
        amount: Unsigned,
    },
    InternalCall(Spend),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transfer {
    // Newest first, as in aetx_env:events. The enclosing transaction adapter
    // attaches its signed transaction hash/type and prepends these events.
    Transferred(Box<[TransferEvent; 3]>),
    Rejected(TransferFailure),
}

// This host primitive accepts zero and still creates the recipient/emits
// events. CALL_R's zero-value shortcut belongs to the instruction layer.
pub fn transfer_value<S: NodeWriter>(
    accounts: &mut Accounts<S>,
    from: AccountPublicKey,
    to: AccountPublicKey,
    amount: &Unsigned,
) -> Result<Transfer, Error> {
    move_value(accounts, from, to, amount, false)
}
pub fn spend<S: NodeWriter>(
    accounts: &mut Accounts<S>,
    from: AccountPublicKey,
    to: AccountPublicKey,
    amount: &Unsigned,
) -> Result<Transfer, Error> {
    move_value(accounts, from, to, amount, true)
}
#[derive(Debug)]
enum TransferFault {
    Reject(TransferFailure),
    Backend(Error),
}
impl From<AccountError> for TransferFault {
    fn from(e: AccountError) -> Self {
        Self::Backend(e.into())
    }
}
fn move_value<S: NodeWriter>(
    accounts: &mut Accounts<S>,
    from: AccountPublicKey,
    to: AccountPublicKey,
    amount: &Unsigned,
    check_payable: bool,
) -> Result<Transfer, Error> {
    let result = accounts.transaction(|accounts| -> Result<_, TransferFault> {
        let Some(mut sender) = accounts.lookup(from)? else {
            return Err(TransferFault::Reject(TransferFailure::AccountNotFound));
        };
        if sender.balance() < amount {
            return Err(TransferFault::Reject(TransferFailure::InsufficientFunds));
        }
        sender.spend_without_nonce_bump(amount)?;
        accounts.enter(&sender)?;
        // Read after the debit: From == To restores the original balance.
        let mut receiver = accounts
            .lookup(to)?
            .unwrap_or_else(|| Account::new(to, 0.into()));
        if check_payable && !receiver.is_payable() {
            return Err(TransferFault::Reject(TransferFailure::AccountNotPayable));
        }
        receiver.earn(amount);
        accounts.enter(&receiver)?;
        let mut transaction = Spend::new(from, Id::new(IdTag::Account, to.0))
            .expect("account recipient is a valid Spend ID");
        transaction.set_amount(amount.clone());
        transaction.set_payload(if check_payable {
            b"Chain.spend".to_vec()
        } else {
            b"Call.amount".to_vec()
        });
        Ok(Transfer::Transferred(Box::new([
            TransferEvent::InternalCall(transaction),
            TransferEvent::Delta {
                account: to,
                debit: false,
                amount: amount.clone(),
            },
            TransferEvent::Delta {
                account: from,
                debit: true,
                amount: amount.clone(),
            },
        ])))
    });
    match result {
        Ok(v) => Ok(v),
        Err(TransferFault::Reject(r)) => Ok(Transfer::Rejected(r)),
        Err(TransferFault::Backend(e)) => Err(e),
    }
}
