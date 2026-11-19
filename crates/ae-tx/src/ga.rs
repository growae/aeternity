// aega_attach_tx.erl, aeternity fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{CanonicalTransaction, Error, TransactionKind, WireTransaction};
use ae_codec::object::Value;
use ae_protocol::{Protocol, contracts::Version};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GaAttachTransaction {
    owner: AccountPublicKey,
    nonce: Unsigned,
    code: Vec<u8>,
    auth_fun: Vec<u8>,
    version: Version,
    fee: Unsigned,
    ttl: Unsigned,
    gas: Unsigned,
    gas_price: Unsigned,
    call_data: Vec<u8>,
    wire_size: usize,
}
impl GaAttachTransaction {
    pub fn new(
        owner: AccountPublicKey,
        version: Version,
        code: Vec<u8>,
        auth_fun: Vec<u8>,
        call_data: Vec<u8>,
    ) -> Self {
        let mut tx = Self {
            owner,
            version,
            code,
            auth_fun,
            call_data,
            nonce: 0.into(),
            fee: 0.into(),
            ttl: 0.into(),
            gas: 0.into(),
            gas_price: 0.into(),
            wire_size: 0,
        };
        tx.refresh_size();
        tx
    }
    pub fn owner(&self) -> AccountPublicKey {
        self.owner
    }
    pub fn nonce(&self) -> &Unsigned {
        &self.nonce
    }
    pub fn code(&self) -> &[u8] {
        &self.code
    }
    pub fn auth_fun(&self) -> &[u8] {
        &self.auth_fun
    }
    pub fn version(&self) -> Version {
        self.version
    }
    pub fn fee(&self) -> &Unsigned {
        &self.fee
    }
    pub fn ttl(&self) -> &Unsigned {
        &self.ttl
    }
    pub fn gas(&self) -> &Unsigned {
        &self.gas
    }
    pub fn gas_price(&self) -> &Unsigned {
        &self.gas_price
    }
    pub fn call_data(&self) -> &[u8] {
        &self.call_data
    }
    pub fn wire_size(&self) -> usize {
        self.wire_size
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
    pub fn set_gas(&mut self, n: Unsigned) {
        self.gas = n;
        self.refresh_size();
    }
    pub fn set_gas_price(&mut self, n: Unsigned) {
        self.gas_price = n;
        self.refresh_size();
    }
    pub fn valid_at_protocol(&self, protocol: Protocol) -> bool {
        protocol >= Protocol::Fortuna && (protocol <= Protocol::Iris || self.nonce <= 1.into())
    }
    // This callback gate is distinct from contract-object version validity.
    pub fn version_available(&self, protocol: Protocol) -> bool {
        match (self.version.abi, protocol) {
            (1, Protocol::Fortuna) => self.version.vm == 4,
            (1, p) if p >= Protocol::Lima => self.version.vm == 6,
            (3, Protocol::Lima) => self.version.vm == 5,
            (3, Protocol::Iris) => self.version.vm == 7,
            (3, p) if p >= Protocol::Ceres => self.version.vm >= 8,
            _ => false,
        }
    }
    pub fn fee_gas(&self) -> Unsigned {
        Unsigned::from(self.wire_size as u64)
            .mul_u64(20)
            .add(&75_000.into())
    }
    pub fn minimum_fee(&self, protocol: Protocol, generalized: bool) -> Unsigned {
        let gas = if generalized && protocol < Protocol::Iris {
            75_000.into()
        } else {
            self.fee_gas()
        };
        gas.mul_u64(protocol.minimum_gas_price())
    }
    pub fn used_gas(&self, vm_gas: &Unsigned) -> Unsigned {
        self.fee_gas().add(vm_gas)
    }
    fn refresh_size(&mut self) {
        self.wire_size = self.encode().len();
    }
}
impl CanonicalTransaction for GaAttachTransaction {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let w = WireTransaction::decode(bytes)?;
        if w.kind() != TransactionKind::GaAttach {
            return Err(Error::WrongTransactionKind);
        }
        Ok(Self {
            owner: account(&w, "owner_id")?,
            nonce: int(&w, "nonce"),
            code: binary(&w, "code"),
            auth_fun: binary(&w, "auth_fun"),
            version: Version::unpack(&int(&w, "ct_version")),
            fee: int(&w, "fee"),
            ttl: int(&w, "ttl"),
            gas: int(&w, "gas"),
            gas_price: int(&w, "gas_price"),
            call_data: binary(&w, "call_data"),
            wire_size: bytes.len(),
        })
    }
    fn encode(&self) -> Vec<u8> {
        WireTransaction::new(
            TransactionKind::GaAttach,
            1,
            vec![
                (
                    "owner_id".into(),
                    Value::Id(Id::new(IdTag::Account, self.owner.0)),
                ),
                ("nonce".into(), Value::Integer(self.nonce.clone())),
                ("code".into(), Value::Binary(self.code.clone())),
                ("auth_fun".into(), Value::Binary(self.auth_fun.clone())),
                ("ct_version".into(), Value::Integer(self.version.pack())),
                ("fee".into(), Value::Integer(self.fee.clone())),
                ("ttl".into(), Value::Integer(self.ttl.clone())),
                ("gas".into(), Value::Integer(self.gas.clone())),
                ("gas_price".into(), Value::Integer(self.gas_price.clone())),
                ("call_data".into(), Value::Binary(self.call_data.clone())),
            ],
        )
        .expect("typed GA attach schema")
        .encode()
    }
}
pub(crate) fn int(w: &WireTransaction, name: &str) -> Unsigned {
    let Some(Value::Integer(n)) = w.field(name) else {
        unreachable!("checked schema")
    };
    n.clone()
}
pub(crate) fn binary(w: &WireTransaction, name: &str) -> Vec<u8> {
    let Some(Value::Binary(b)) = w.field(name) else {
        unreachable!("checked schema")
    };
    b.clone()
}
pub(crate) fn account(w: &WireTransaction, name: &str) -> Result<AccountPublicKey, Error> {
    let Some(Value::Id(id)) = w.field(name) else {
        unreachable!("checked schema")
    };
    if id.tag != IdTag::Account {
        return Err(Error::InvalidIdType);
    }
    Ok(AccountPublicKey(id.value))
}
