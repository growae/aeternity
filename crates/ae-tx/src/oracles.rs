// Aeternity fa2a6d7, aeo_{register,extend,query,response}_tx.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{CanonicalTransaction, Error, TransactionKind, WireTransaction};
use ae_codec::object::Value;
use ae_protocol::{Protocol, oracles::Ttl};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleOperation {
    Register {
        query_format: Vec<u8>,
        response_format: Vec<u8>,
        query_fee: Unsigned,
        oracle_ttl: Ttl,
        abi_version: Unsigned,
    },
    Extend {
        delta: Unsigned,
    },
    Query {
        oracle: Id,
        query: Vec<u8>,
        query_fee: Unsigned,
        query_ttl: Ttl,
        response_ttl: Unsigned,
    },
    Response {
        query_id: Vec<u8>,
        response: Vec<u8>,
        response_ttl: Unsigned,
    },
}
impl OracleOperation {
    fn validate(&self) -> Result<(), Error> {
        if let Self::Query { oracle, .. } = self
            && !matches!(oracle.tag, IdTag::Oracle | IdTag::Name)
        {
            return Err(Error::InvalidIdType);
        }
        Ok(())
    }
    fn kind(&self) -> TransactionKind {
        match self {
            Self::Register { .. } => TransactionKind::OracleRegister,
            Self::Extend { .. } => TransactionKind::OracleExtend,
            Self::Query { .. } => TransactionKind::OracleQuery,
            Self::Response { .. } => TransactionKind::OracleResponse,
        }
    }
    pub fn state_ttl(&self) -> Ttl {
        match self {
            Self::Register { oracle_ttl, .. } => oracle_ttl.clone(),
            Self::Query { query_ttl, .. } => query_ttl.clone(),
            Self::Extend { delta } => Ttl::Delta(delta.clone()),
            Self::Response { response_ttl, .. } => Ttl::Delta(response_ttl.clone()),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleTransaction {
    account: AccountPublicKey,
    operation: OracleOperation,
    nonce: Unsigned,
    fee: Unsigned,
    ttl: Unsigned,
    wire_size: usize,
}
impl OracleTransaction {
    pub fn new(account: AccountPublicKey, operation: OracleOperation) -> Result<Self, Error> {
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
    pub fn operation(&self) -> &OracleOperation {
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
    pub fn set_operation(&mut self, op: OracleOperation) -> Result<(), Error> {
        op.validate()?;
        self.operation = op;
        self.refresh_size();
        Ok(())
    }
    pub fn valid_at_protocol(&self, protocol: Protocol) -> bool {
        !matches!(&self.operation,OracleOperation::Query{oracle,..} if oracle.tag==IdTag::Name&&protocol<Protocol::Iris)
    }
    fn refresh_size(&mut self) {
        self.wire_size = self.encode().len();
    }
}
impl CanonicalTransaction for OracleTransaction {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let w = WireTransaction::decode(bytes)?;
        let (account, operation) = match w.kind() {
            TransactionKind::OracleRegister => {
                let oracle_ttl = ttl(&w, "ttl_type", "ttl_value", false)?;
                (
                    id(&w, "account_id", &[IdTag::Account])?.value,
                    OracleOperation::Register {
                        query_format: binary(&w, "query_format"),
                        response_format: binary(&w, "response_format"),
                        query_fee: int(&w, "query_fee"),
                        oracle_ttl,
                        abi_version: int(&w, "abi_version"),
                    },
                )
            }
            TransactionKind::OracleExtend => {
                let Ttl::Delta(delta) = ttl(&w, "oracle_ttl_type", "oracle_ttl_value", true)?
                else {
                    unreachable!()
                };
                (
                    id(&w, "oracle_id", &[IdTag::Oracle])?.value,
                    OracleOperation::Extend { delta },
                )
            }
            TransactionKind::OracleQuery => {
                let Ttl::Delta(response_ttl) =
                    ttl(&w, "response_ttl_type", "response_ttl_value", true)?
                else {
                    unreachable!()
                };
                let query_ttl = ttl(&w, "query_ttl_type", "query_ttl_value", false)?;
                (
                    id(&w, "sender_id", &[IdTag::Account])?.value,
                    OracleOperation::Query {
                        oracle: id(&w, "oracle_id", &[IdTag::Oracle, IdTag::Name])?,
                        query: binary(&w, "query"),
                        query_fee: int(&w, "query_fee"),
                        query_ttl,
                        response_ttl,
                    },
                )
            }
            TransactionKind::OracleResponse => {
                let Ttl::Delta(response_ttl) =
                    ttl(&w, "response_ttl_type", "response_ttl_value", true)?
                else {
                    unreachable!()
                };
                // The original callback does not check query-id length while
                // decoding. Primitive execution performs that check later.
                (
                    id(&w, "oracle_id", &[IdTag::Oracle])?.value,
                    OracleOperation::Response {
                        query_id: binary(&w, "query_id"),
                        response: binary(&w, "response"),
                        response_ttl,
                    },
                )
            }
            _ => return Err(Error::WrongTransactionKind),
        };
        Ok(Self {
            account: AccountPublicKey(account),
            operation,
            nonce: int(&w, "nonce"),
            fee: int(&w, "fee"),
            ttl: int(&w, "ttl"),
            wire_size: bytes.len(),
        })
    }
    fn encode(&self) -> Vec<u8> {
        let (account_field, account_tag) = match self.operation {
            OracleOperation::Register { .. } => ("account_id", IdTag::Account),
            OracleOperation::Query { .. } => ("sender_id", IdTag::Account),
            _ => ("oracle_id", IdTag::Oracle),
        };
        let mut fields = vec![
            (
                account_field.into(),
                Value::Id(Id::new(account_tag, self.account.0)),
            ),
            ("nonce".into(), Value::Integer(self.nonce.clone())),
        ];
        let mut push = |name: &str, value: Value| fields.push((name.into(), value));
        let mut abi = None;
        match &self.operation {
            OracleOperation::Register {
                query_format,
                response_format,
                query_fee,
                oracle_ttl,
                abi_version,
            } => {
                push("query_format", Value::Binary(query_format.clone()));
                push("response_format", Value::Binary(response_format.clone()));
                push("query_fee", Value::Integer(query_fee.clone()));
                let (kind, n) = ttl_fields(oracle_ttl);
                push("ttl_type", kind);
                push("ttl_value", n);
                abi = Some(abi_version.clone());
            }
            OracleOperation::Extend { delta } => {
                push("oracle_ttl_type", Value::Integer(0.into()));
                push("oracle_ttl_value", Value::Integer(delta.clone()));
            }
            OracleOperation::Query {
                oracle,
                query,
                query_fee,
                query_ttl,
                response_ttl,
            } => {
                push("oracle_id", Value::Id(*oracle));
                push("query", Value::Binary(query.clone()));
                push("query_fee", Value::Integer(query_fee.clone()));
                let (kind, n) = ttl_fields(query_ttl);
                push("query_ttl_type", kind);
                push("query_ttl_value", n);
                push("response_ttl_type", Value::Integer(0.into()));
                push("response_ttl_value", Value::Integer(response_ttl.clone()));
            }
            OracleOperation::Response {
                query_id,
                response,
                response_ttl,
            } => {
                push("query_id", Value::Binary(query_id.clone()));
                push("response", Value::Binary(response.clone()));
                push("response_ttl_type", Value::Integer(0.into()));
                push("response_ttl_value", Value::Integer(response_ttl.clone()));
            }
        }
        push("fee", Value::Integer(self.fee.clone()));
        push("ttl", Value::Integer(self.ttl.clone()));
        if let Some(abi) = abi {
            push("abi_version", Value::Integer(abi));
        }
        WireTransaction::new(self.operation.kind(), 1, fields)
            .expect("typed oracle schema")
            .encode()
    }
}
fn ttl_fields(ttl: &Ttl) -> (Value, Value) {
    let (tag, n) = match ttl {
        Ttl::Delta(n) => (0, n),
        Ttl::Block(n) => (1, n),
    };
    (Value::Integer(tag.into()), Value::Integer(n.clone()))
}
fn ttl(w: &WireTransaction, kind: &str, value: &str, relative_only: bool) -> Result<Ttl, Error> {
    match int(w, kind).to_u64() {
        Ok(0) => Ok(Ttl::Delta(int(w, value))),
        Ok(1) if !relative_only => Ok(Ttl::Block(int(w, value))),
        _ => Err(Error::InvalidTtlType),
    }
}
fn int(w: &WireTransaction, name: &str) -> Unsigned {
    let Some(Value::Integer(n)) = w.field(name) else {
        unreachable!("checked schema")
    };
    n.clone()
}
fn binary(w: &WireTransaction, name: &str) -> Vec<u8> {
    let Some(Value::Binary(b)) = w.field(name) else {
        unreachable!("checked schema")
    };
    b.clone()
}
fn id(w: &WireTransaction, name: &str, allowed: &[IdTag]) -> Result<Id, Error> {
    let Some(Value::Id(id)) = w.field(name) else {
        unreachable!("checked schema")
    };
    if allowed.contains(&id.tag) {
        Ok(*id)
    } else {
        Err(Error::InvalidIdType)
    }
}
