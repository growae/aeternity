// Aeternity fa2a6d7, aens_{preclaim,claim,update,transfer,revoke}_tx.
// Copyright (c) 2018, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{CanonicalTransaction, Error, TransactionKind, WireTransaction};
use ae_codec::{
    name_pointer::{Pointer, PointerValue},
    object::Value,
};
use ae_protocol::Protocol;
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameOperation {
    Preclaim {
        commitment: [u8; 32],
    },
    Claim {
        name: Vec<u8>,
        salt: Unsigned,
        name_fee: Option<Unsigned>,
    },
    Update {
        name: [u8; 32],
        name_ttl: Unsigned,
        client_ttl: Unsigned,
        pointers: Vec<Pointer>,
    },
    Transfer {
        name: [u8; 32],
        recipient: Id,
    },
    Revoke {
        name: [u8; 32],
    },
}
impl NameOperation {
    fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Transfer { recipient, .. }
                if !matches!(recipient.tag, IdTag::Account | IdTag::Name) =>
            {
                Err(Error::InvalidIdType)
            }
            Self::Update { pointers, .. } => pointers
                .iter()
                .try_for_each(|p| p.validate().map_err(|_| Error::InvalidPointer)),
            _ => Ok(()),
        }
    }
    pub fn kind(&self) -> TransactionKind {
        match self {
            Self::Preclaim { .. } => TransactionKind::NamePreclaim,
            Self::Claim { .. } => TransactionKind::NameClaim,
            Self::Update { .. } => TransactionKind::NameUpdate,
            Self::Transfer { .. } => TransactionKind::NameTransfer,
            Self::Revoke { .. } => TransactionKind::NameRevoke,
        }
    }
    fn version(&self) -> u64 {
        match self {
            Self::Claim {
                name_fee: Some(_), ..
            } => 2,
            Self::Update { pointers, .. }
                if pointers
                    .iter()
                    .any(|p| matches!(p.value, PointerValue::Data(_))) =>
            {
                2
            }
            _ => 1,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameTransaction {
    account: AccountPublicKey,
    operation: NameOperation,
    nonce: Unsigned,
    fee: Unsigned,
    ttl: Unsigned,
    wire_size: usize,
}
impl NameTransaction {
    pub fn new(account: AccountPublicKey, operation: NameOperation) -> Result<Self, Error> {
        operation.validate()?;
        let mut tx = Self {
            account,
            operation,
            nonce: 0.into(),
            fee: 0.into(),
            ttl: 0.into(),
            wire_size: 0,
        };
        tx.refresh_size();
        Ok(tx)
    }
    pub fn account(&self) -> AccountPublicKey {
        self.account
    }
    pub fn operation(&self) -> &NameOperation {
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
    pub fn set_operation(&mut self, op: NameOperation) -> Result<(), Error> {
        op.validate()?;
        self.operation = op;
        self.refresh_size();
        Ok(())
    }
    pub fn valid_at_protocol(&self, p: Protocol) -> bool {
        match &self.operation {
            NameOperation::Claim { name_fee, .. } => p >= Protocol::Lima || name_fee.is_none(),
            NameOperation::Update { .. } => p >= Protocol::Ceres || self.operation.version() == 1,
            _ => true,
        }
    }
    fn refresh_size(&mut self) {
        self.wire_size = self.encode().len();
    }
}
impl CanonicalTransaction for NameTransaction {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let wire = WireTransaction::decode(bytes)?;
        if !matches!(
            wire.kind(),
            TransactionKind::NamePreclaim
                | TransactionKind::NameClaim
                | TransactionKind::NameUpdate
                | TransactionKind::NameTransfer
                | TransactionKind::NameRevoke
        ) {
            return Err(Error::WrongTransactionKind);
        }
        let account = AccountPublicKey(id(&wire, "account_id", &[IdTag::Account])?.value);
        let operation = match wire.kind() {
            TransactionKind::NamePreclaim => NameOperation::Preclaim {
                commitment: id(&wire, "commitment_id", &[IdTag::Commitment])?.value,
            },
            TransactionKind::NameClaim => {
                let Some(Value::Binary(name)) = wire.field("name") else {
                    unreachable!("checked schema")
                };
                NameOperation::Claim {
                    name: name.clone(),
                    salt: int(&wire, "name_salt"),
                    name_fee: if wire.version() == 2 {
                        Some(int(&wire, "name_fee"))
                    } else {
                        None
                    },
                }
            }
            TransactionKind::NameRevoke => NameOperation::Revoke {
                name: id(&wire, "name_id", &[IdTag::Name])?.value,
            },
            TransactionKind::NameTransfer => NameOperation::Transfer {
                name: id(&wire, "name_id", &[IdTag::Name])?.value,
                recipient: id(&wire, "recipient_id", &[IdTag::Account, IdTag::Name])?,
            },
            TransactionKind::NameUpdate => {
                let name = id(&wire, "name_id", &[IdTag::Name])?.value;
                let Some(Value::List(pointers)) = wire.field("pointers") else {
                    unreachable!("checked schema")
                };
                let pointers = pointers
                    .iter()
                    .cloned()
                    .map(|p| Pointer::decode(p, wire.version()).map_err(|_| Error::InvalidPointer))
                    .collect::<Result<Vec<_>, _>>()?;
                NameOperation::Update {
                    name,
                    name_ttl: int(&wire, "name_ttl"),
                    client_ttl: int(&wire, "client_ttl"),
                    pointers,
                }
            }
            _ => unreachable!(),
        };
        if operation.version() != wire.version() {
            return Err(Error::InvalidSerializationVersion);
        }
        Ok(Self {
            account,
            operation,
            nonce: int(&wire, "nonce"),
            fee: int(&wire, "fee"),
            ttl: int(&wire, "ttl"),
            wire_size: bytes.len(),
        })
    }
    fn encode(&self) -> Vec<u8> {
        let mut fields = vec![
            (
                "account_id".into(),
                Value::Id(Id::new(IdTag::Account, self.account.0)),
            ),
            ("nonce".into(), Value::Integer(self.nonce.clone())),
        ];
        let mut push = |name: &str, value: Value| fields.push((name.into(), value));
        match &self.operation {
            NameOperation::Preclaim { commitment } => push(
                "commitment_id",
                Value::Id(Id::new(IdTag::Commitment, *commitment)),
            ),
            NameOperation::Claim {
                name,
                salt,
                name_fee,
            } => {
                push("name", Value::Binary(name.clone()));
                push("name_salt", Value::Integer(salt.clone()));
                if let Some(fee) = name_fee {
                    push("name_fee", Value::Integer(fee.clone()));
                }
            }
            NameOperation::Revoke { name } => {
                push("name_id", Value::Id(Id::new(IdTag::Name, *name)))
            }
            NameOperation::Transfer { name, recipient } => {
                push("name_id", Value::Id(Id::new(IdTag::Name, *name)));
                push("recipient_id", Value::Id(*recipient));
            }
            NameOperation::Update {
                name,
                name_ttl,
                client_ttl,
                pointers,
            } => {
                push("name_id", Value::Id(Id::new(IdTag::Name, *name)));
                push("name_ttl", Value::Integer(name_ttl.clone()));
                push(
                    "pointers",
                    Value::List(
                        pointers
                            .iter()
                            .map(|p| {
                                p.encode(self.operation.version())
                                    .expect("validated pointer")
                            })
                            .collect(),
                    ),
                );
                push("client_ttl", Value::Integer(client_ttl.clone()));
            }
        }
        push("fee", Value::Integer(self.fee.clone()));
        push("ttl", Value::Integer(self.ttl.clone()));
        WireTransaction::new(self.operation.kind(), self.operation.version(), fields)
            .expect("typed naming schema")
            .encode()
    }
}
fn int(wire: &WireTransaction, field: &str) -> Unsigned {
    let Some(Value::Integer(n)) = wire.field(field) else {
        unreachable!("checked schema")
    };
    n.clone()
}
fn id(wire: &WireTransaction, field: &str, allowed: &[IdTag]) -> Result<Id, Error> {
    let Some(Value::Id(id)) = wire.field(field) else {
        unreachable!("checked schema")
    };
    if allowed.contains(&id.tag) {
        Ok(*id)
    } else {
        Err(Error::InvalidIdType)
    }
}
