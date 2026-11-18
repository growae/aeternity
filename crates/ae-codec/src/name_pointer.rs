// Aeternity fa2a6d7, aens_pointer. Shared wire representation for state and transactions.
// Copyright (c) 2018, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::object::Value;
use ae_types::{Id, IdTag};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PointerError;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PointerValue {
    Id(Id),
    Data(Vec<u8>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pointer {
    pub key: Vec<u8>,
    pub value: PointerValue,
}
impl Pointer {
    pub fn validate(&self) -> Result<(), PointerError> {
        match &self.value {
            PointerValue::Id(id)
                if matches!(
                    id.tag,
                    IdTag::Account | IdTag::Oracle | IdTag::Contract | IdTag::Channel
                ) =>
            {
                Ok(())
            }
            PointerValue::Data(bytes) if bytes.len() <= 1024 => Ok(()),
            _ => Err(PointerError),
        }
    }
    pub fn encode(&self, version: u64) -> Result<Value, PointerError> {
        self.validate()?;
        let value = match (&self.value, version) {
            (PointerValue::Id(id), 1) => Value::Id(*id),
            (PointerValue::Id(id), 2) => {
                let mut bytes = vec![1];
                bytes.extend_from_slice(&id.to_bytes());
                Value::Binary(bytes)
            }
            (PointerValue::Data(data), 2) => {
                let mut bytes = vec![2];
                bytes.extend_from_slice(data);
                Value::Binary(bytes)
            }
            _ => return Err(PointerError),
        };
        Ok(Value::Tuple(vec![Value::Binary(self.key.clone()), value]))
    }
    pub fn decode(value: Value, version: u64) -> Result<Self, PointerError> {
        let Value::Tuple(mut fields) = value else {
            return Err(PointerError);
        };
        if fields.len() != 2 {
            return Err(PointerError);
        }
        let data = fields.pop().ok_or(PointerError)?;
        let Some(Value::Binary(key)) = fields.pop() else {
            return Err(PointerError);
        };
        let value = match (data, version) {
            (Value::Id(id), 1) => PointerValue::Id(id),
            (Value::Binary(bytes), 2) => match bytes.split_first() {
                Some((1, id)) => PointerValue::Id(Id::from_bytes(id).map_err(|_| PointerError)?),
                Some((2, data)) => PointerValue::Data(data.to_vec()),
                _ => return Err(PointerError),
            },
            _ => return Err(PointerError),
        };
        let pointer = Self { key, value };
        pointer.validate()?;
        Ok(pointer)
    }
}
