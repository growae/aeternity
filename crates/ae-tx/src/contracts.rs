// aect_create_tx/aect_call_tx/aetx, aeternity fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{CanonicalTransaction, Error, TransactionKind, WireTransaction};
use ae_codec::object::Value;
use ae_protocol::{
    Protocol,
    contracts::{CREATE_BASE_GAS, Version, call_base_gas},
};
use ae_types::{AccountPublicKey, Id, IdTag, TypeError, Unsigned};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractOperation {
    Create {
        version: Version,
        code: Vec<u8>,
        deposit: Unsigned,
        call_data: Vec<u8>,
    },
    Call {
        contract: Id,
        abi_version: Unsigned,
        call_data: Vec<u8>,
    },
}
impl ContractOperation {
    fn validate(&self) -> Result<(), Error> {
        if let Self::Call { contract, .. } = self
            && !matches!(contract.tag, IdTag::Contract | IdTag::Name)
        {
            return Err(Error::InvalidIdType);
        }
        Ok(())
    }
    pub fn kind(&self) -> TransactionKind {
        match self {
            Self::Create { .. } => TransactionKind::ContractCreate,
            Self::Call { .. } => TransactionKind::ContractCall,
        }
    }
    pub fn abi_version(&self) -> Unsigned {
        match self {
            Self::Create { version, .. } => u64::from(version.abi).into(),
            Self::Call { abi_version, .. } => abi_version.clone(),
        }
    }
    pub fn call_data(&self) -> &[u8] {
        match self {
            Self::Create { call_data, .. } | Self::Call { call_data, .. } => call_data,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractTransaction {
    actor: Id,
    operation: ContractOperation,
    nonce: Unsigned,
    fee: Unsigned,
    ttl: Unsigned,
    amount: Unsigned,
    gas: Unsigned,
    gas_price: Unsigned,
    wire_size: usize,
    // Internal contract calls carry these fields; neither is serialized.
    call_origin: [u8; 32],
    call_stack: Vec<Unsigned>,
}
impl ContractTransaction {
    pub fn new(actor: Id, operation: ContractOperation) -> Result<Self, Error> {
        operation.validate()?;
        if actor.tag != IdTag::Account
            && !(actor.tag == IdTag::Contract
                && matches!(operation, ContractOperation::Call { .. }))
        {
            return Err(Error::InvalidIdType);
        }
        let mut tx = Self {
            actor,
            operation,
            nonce: 0.into(),
            fee: 0.into(),
            ttl: 0.into(),
            amount: 0.into(),
            gas: 0.into(),
            gas_price: 0.into(),
            wire_size: 0,
            call_origin: actor.value,
            call_stack: vec![],
        };
        tx.refresh_size();
        Ok(tx)
    }
    pub fn actor(&self) -> Id {
        self.actor
    }
    pub fn signer(&self) -> AccountPublicKey {
        AccountPublicKey(self.actor.value)
    }
    pub fn operation(&self) -> &ContractOperation {
        &self.operation
    }
    pub fn nonce(&self) -> &Unsigned {
        &self.nonce
    }
    pub fn fee(&self) -> &Unsigned {
        &self.fee
    }
    pub fn ttl(&self) -> &Unsigned {
        &self.ttl
    }
    pub fn amount(&self) -> &Unsigned {
        &self.amount
    }
    pub fn gas(&self) -> &Unsigned {
        &self.gas
    }
    pub fn gas_price(&self) -> &Unsigned {
        &self.gas_price
    }
    pub fn wire_size(&self) -> usize {
        self.wire_size
    }
    pub fn call_origin(&self) -> [u8; 32] {
        self.call_origin
    }
    pub fn call_stack(&self) -> &[Unsigned] {
        &self.call_stack
    }
    pub fn set_call_context(
        &mut self,
        origin: [u8; 32],
        stack: Vec<Unsigned>,
    ) -> Result<(), Error> {
        if !matches!(self.operation, ContractOperation::Call { .. }) {
            return Err(Error::WrongTransactionKind);
        }
        self.call_origin = origin;
        self.call_stack = stack;
        Ok(())
    }
    pub fn set_nonce(&mut self, n: Unsigned) {
        self.nonce = n;
        self.refresh_size();
    }
    pub fn set_fee(&mut self, n: Unsigned) {
        self.fee = n;
        self.refresh_size();
    }
    pub fn set_ttl(&mut self, n: Unsigned) {
        self.ttl = n;
        self.refresh_size();
    }
    pub fn set_amount(&mut self, n: Unsigned) {
        self.amount = n;
        self.refresh_size();
    }
    pub fn set_gas(&mut self, n: Unsigned) {
        self.gas = n;
        self.refresh_size();
    }
    pub fn set_gas_price(&mut self, n: Unsigned) {
        self.gas_price = n;
        self.refresh_size();
    }
    pub fn valid_at_protocol(&self, protocol: Protocol) -> bool {
        !matches!(&self.operation,ContractOperation::Call{contract,..} if contract.tag==IdTag::Name && protocol<Protocol::Ceres)
    }
    pub fn base_gas(&self) -> u64 {
        match &self.operation {
            ContractOperation::Create { .. } => CREATE_BASE_GAS,
            ContractOperation::Call { abi_version, .. } => call_base_gas(abi_version),
        }
    }
    fn fee_gas_for_size(&self, size: usize) -> Unsigned {
        Unsigned::from(size as u64)
            .mul_u64(20)
            .add(&self.base_gas().into())
    }
    pub fn fee_gas(&self) -> Unsigned {
        self.fee_gas_for_size(self.wire_size)
    }
    pub fn gas_limit(&self) -> Unsigned {
        self.fee_gas().add(&self.gas)
    }
    pub fn inner_gas_limit(&self) -> Unsigned {
        self.gas.add(&self.base_gas().into())
    }
    pub fn used_gas(&self, vm_gas: &Unsigned) -> Unsigned {
        self.fee_gas().add(vm_gas)
    }
    pub fn minimum_fee(&self, protocol: Protocol, ga_inner: bool) -> Unsigned {
        let size = if ga_inner && protocol < Protocol::Iris {
            0
        } else {
            self.wire_size
        };
        self.fee_gas_for_size(size)
            .mul_u64(protocol.minimum_gas_price())
    }
    // Pre-Iris recursive GA pricing excludes the inner transaction's bytes.
    // Return a resource/size conversion error instead of narrowing a divisor.
    pub fn minimum_gas_price(&self, omit_size: bool) -> Result<Unsigned, TypeError> {
        let divisor = self
            .fee_gas_for_size(if omit_size { 0 } else { self.wire_size })
            .to_u64()?;
        let (quotient, remainder) = self.fee.div_rem_u64(divisor)?;
        let price = if remainder == 0 {
            quotient
        } else {
            quotient.add(&1.into())
        };
        Ok(price.min(self.gas_price.clone()))
    }
    fn refresh_size(&mut self) {
        self.wire_size = self.encode().len();
    }
}
impl CanonicalTransaction for ContractTransaction {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let wire = WireTransaction::decode(bytes)?;
        let (actor, operation) = match wire.kind() {
            TransactionKind::ContractCreate => (
                id(&wire, "owner_id")?,
                ContractOperation::Create {
                    version: Version::unpack(&int(&wire, "ct_version")),
                    code: binary(&wire, "code"),
                    deposit: int(&wire, "deposit"),
                    call_data: binary(&wire, "call_data"),
                },
            ),
            TransactionKind::ContractCall => (
                id(&wire, "caller_id")?,
                ContractOperation::Call {
                    contract: id(&wire, "contract_id")?,
                    abi_version: int(&wire, "abi_version"),
                    call_data: binary(&wire, "call_data"),
                },
            ),
            _ => return Err(Error::WrongTransactionKind),
        };
        if actor.tag != IdTag::Account {
            return Err(Error::InvalidIdType);
        }
        operation.validate()?;
        Ok(Self {
            actor,
            operation,
            nonce: int(&wire, "nonce"),
            fee: int(&wire, "fee"),
            ttl: int(&wire, "ttl"),
            amount: int(&wire, "amount"),
            gas: int(&wire, "gas"),
            gas_price: int(&wire, "gas_price"),
            wire_size: bytes.len(),
            call_origin: actor.value,
            call_stack: vec![],
        })
    }
    fn encode(&self) -> Vec<u8> {
        let mut fields = vec![
            (
                match self.operation {
                    ContractOperation::Create { .. } => "owner_id",
                    ContractOperation::Call { .. } => "caller_id",
                }
                .into(),
                Value::Id(self.actor),
            ),
            ("nonce".into(), Value::Integer(self.nonce.clone())),
        ];
        match &self.operation {
            ContractOperation::Create { version, code, .. } => fields.extend([
                ("code".into(), Value::Binary(code.clone())),
                ("ct_version".into(), Value::Integer(version.pack())),
            ]),
            ContractOperation::Call {
                contract,
                abi_version,
                ..
            } => fields.extend([
                ("contract_id".into(), Value::Id(*contract)),
                ("abi_version".into(), Value::Integer(abi_version.clone())),
            ]),
        }
        fields.extend([
            ("fee".into(), Value::Integer(self.fee.clone())),
            ("ttl".into(), Value::Integer(self.ttl.clone())),
        ]);
        if let ContractOperation::Create { deposit, .. } = &self.operation {
            fields.push(("deposit".into(), Value::Integer(deposit.clone())));
        }
        fields.extend([
            ("amount".into(), Value::Integer(self.amount.clone())),
            ("gas".into(), Value::Integer(self.gas.clone())),
            ("gas_price".into(), Value::Integer(self.gas_price.clone())),
            (
                "call_data".into(),
                Value::Binary(self.operation.call_data().to_vec()),
            ),
        ]);
        WireTransaction::new(self.operation.kind(), 1, fields)
            .expect("typed contract schema")
            .encode()
    }
}
fn int(wire: &WireTransaction, name: &str) -> Unsigned {
    let Some(Value::Integer(n)) = wire.field(name) else {
        unreachable!("checked schema")
    };
    n.clone()
}
fn binary(wire: &WireTransaction, name: &str) -> Vec<u8> {
    let Some(Value::Binary(b)) = wire.field(name) else {
        unreachable!("checked schema")
    };
    b.clone()
}
fn id(wire: &WireTransaction, name: &str) -> Result<Id, Error> {
    let Some(Value::Id(id)) = wire.field(name) else {
        unreachable!("checked schema")
    };
    Ok(*id)
}
