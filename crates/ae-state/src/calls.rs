// aect_call and aect_call_state_tree, fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::contracts::Error;
use ae_codec::object::{self, Field, FieldType, ObjectTag, Value};
use ae_crypto::blake2b_256;
use ae_trie::{MemoryStore, Nibbles, NodeReader, NodeWriter, Trie};
use ae_types::{Hash256, Id, IdTag, Unsigned};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnType {
    Ok,
    Error,
    Revert,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub address: Vec<u8>,
    pub topics: Vec<Vec<u8>>,
    pub data: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    pub id: [u8; 32],
    pub caller: Id,
    pub nonce: Unsigned,
    pub height: Unsigned,
    pub contract: [u8; 32],
    pub call_target: Option<Vec<u8>>,
    pub gas_price: Unsigned,
    pub gas_used: Unsigned,
    pub return_value: Vec<u8>,
    pub return_type: ReturnType,
    pub log: Vec<LogEntry>,
}
pub fn call_id(caller: [u8; 32], nonce: &Unsigned, target: [u8; 32]) -> Hash256 {
    let mut bytes = caller.to_vec();
    let mut word = [0; 32];
    let raw = nonce.as_bytes();
    let n = raw.len().min(32);
    word[32 - n..].copy_from_slice(&raw[raw.len() - n..]);
    bytes.extend_from_slice(&word);
    bytes.extend_from_slice(&target);
    blake2b_256(&bytes)
}
pub fn ga_call_id(nonce: [u8; 32], contract: [u8; 32]) -> Hash256 {
    blake2b_256(&[nonce.as_slice(), &contract].concat())
}
impl Call {
    pub fn new(
        caller: Id,
        nonce: Unsigned,
        contract: [u8; 32],
        target: Option<[u8; 32]>,
        height: Unsigned,
        gas_price: Unsigned,
    ) -> Self {
        let target = target.unwrap_or(contract);
        Self {
            id: call_id(caller.value, &nonce, target).0,
            caller,
            nonce,
            height,
            contract,
            call_target: (target != contract).then(|| target.to_vec()),
            gas_price,
            gas_used: 0.into(),
            return_value: vec![],
            return_type: ReturnType::Ok,
            log: vec![],
        }
    }
    pub fn target(&self) -> &[u8] {
        self.call_target.as_deref().unwrap_or(&self.contract)
    }
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let version = if self.call_target.is_some() { 3 } else { 2 };
        let mut values = vec![
            Value::Id(self.caller),
            Value::Integer(self.nonce.clone()),
            Value::Integer(self.height.clone()),
            Value::Id(Id::new(IdTag::Contract, self.contract)),
        ];
        if let Some(target) = &self.call_target {
            values.push(Value::Binary(target.clone()));
        }
        let log = self
            .log
            .iter()
            .map(|e| {
                Value::Tuple(vec![
                    Value::Binary(e.address.clone()),
                    Value::List(e.topics.iter().cloned().map(Value::Binary).collect()),
                    Value::Binary(e.data.clone()),
                ])
            })
            .collect();
        values.extend([
            Value::Integer(self.gas_price.clone()),
            Value::Integer(self.gas_used.clone()),
            Value::Binary(self.return_value.clone()),
            Value::Integer(match self.return_type {
                ReturnType::Ok => 0.into(),
                ReturnType::Error => 1.into(),
                ReturnType::Revert => 2.into(),
            }),
            Value::List(log),
        ]);
        let schema = schema(version)?;
        let fields = schema
            .iter()
            .zip(values)
            .map(|(f, v)| (f.name.clone(), v))
            .collect::<Vec<_>>();
        Ok(object::serialize(
            ObjectTag::ContractCall,
            &u64::from(version).into(),
            &schema,
            &fields,
        )?)
    }
    pub fn deserialize(id: [u8; 32], bytes: &[u8]) -> Result<Self, Error> {
        let env = object::decode_envelope(bytes)?;
        let version = u8::try_from(env.version.to_u64().map_err(|_| Error::InvalidVersion)?)
            .map_err(|_| Error::InvalidVersion)?;
        let fields = object::deserialize(
            bytes,
            ObjectTag::ContractCall,
            &env.version,
            &schema(version)?,
        )?;
        let mut vs = fields.into_iter().map(|(_, v)| v);
        let Some(Value::Id(caller)) = vs.next() else {
            return Err(Error::InvalidValue);
        };
        let Some(Value::Integer(nonce)) = vs.next() else {
            return Err(Error::InvalidValue);
        };
        let Some(Value::Integer(height)) = vs.next() else {
            return Err(Error::InvalidValue);
        };
        let Some(Value::Id(contract)) = vs.next() else {
            return Err(Error::InvalidValue);
        };
        if contract.tag != IdTag::Contract {
            return Err(Error::InvalidIdType);
        }
        let call_target = if version == 3 {
            let Some(Value::Binary(v)) = vs.next() else {
                return Err(Error::InvalidValue);
            };
            Some(v)
        } else {
            None
        };
        let Some(Value::Integer(gas_price)) = vs.next() else {
            return Err(Error::InvalidValue);
        };
        let Some(Value::Integer(gas_used)) = vs.next() else {
            return Err(Error::InvalidValue);
        };
        let Some(Value::Binary(return_value)) = vs.next() else {
            return Err(Error::InvalidValue);
        };
        let Some(Value::Integer(rt)) = vs.next() else {
            return Err(Error::InvalidValue);
        };
        let return_type = match rt.to_u64().ok() {
            Some(0) => ReturnType::Ok,
            Some(1) => ReturnType::Error,
            Some(2) => ReturnType::Revert,
            _ => return Err(Error::InvalidValue),
        };
        let Some(Value::List(log)) = vs.next() else {
            return Err(Error::InvalidValue);
        };
        let log = log
            .into_iter()
            .map(|v| {
                let Value::Tuple(parts) = v else {
                    return Err(Error::InvalidValue);
                };
                let [
                    Value::Binary(address),
                    Value::List(topics),
                    Value::Binary(data),
                ] = parts.as_slice()
                else {
                    return Err(Error::InvalidValue);
                };
                let topics = topics
                    .iter()
                    .map(|v| match v {
                        Value::Binary(b) => Ok(b.clone()),
                        _ => Err(Error::InvalidValue),
                    })
                    .collect::<Result<_, _>>()?;
                Ok(LogEntry {
                    address: address.clone(),
                    topics,
                    data: data.clone(),
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            id,
            caller,
            nonce,
            height,
            contract: contract.value,
            call_target,
            gas_price,
            gas_used,
            return_value,
            return_type,
            log,
        })
    }
}
fn schema(version: u8) -> Result<Vec<Field>, Error> {
    if !matches!(version, 2 | 3) {
        return Err(Error::InvalidVersion);
    }
    let mut fields = vec![
        ("caller_id", FieldType::Id),
        ("caller_nonce", FieldType::Integer),
        ("height", FieldType::Integer),
        ("contract_id", FieldType::Id),
    ];
    if version == 3 {
        fields.push(("ct_call_id", FieldType::Binary));
    }
    fields.extend([
        ("gas_price", FieldType::Integer),
        ("gas_used", FieldType::Integer),
        ("return_value", FieldType::Binary),
        ("return_type", FieldType::Integer),
        (
            "log",
            FieldType::List(Box::new(FieldType::Tuple(vec![
                FieldType::Binary,
                FieldType::List(Box::new(FieldType::Binary)),
                FieldType::Binary,
            ]))),
        ),
    ]);
    Ok(fields
        .into_iter()
        .map(|(name, kind)| Field {
            name: name.into(),
            kind,
        })
        .collect())
}

#[derive(Debug, Clone)]
pub struct Calls<S = MemoryStore> {
    tree: Trie<S>,
}
impl Default for Calls {
    fn default() -> Self {
        Self::new(MemoryStore::default())
    }
}
impl<S: NodeReader> Calls<S> {
    pub fn new(store: S) -> Self {
        Self {
            tree: Trie::new(store),
        }
    }
    pub fn from_root(root: Option<Hash256>, store: S) -> Result<Self, Error> {
        Ok(Self {
            tree: Trie::from_root(root, store)?,
        })
    }
    pub fn tree(&self) -> &Trie<S> {
        &self.tree
    }
    pub fn root_hash(&self) -> Option<Hash256> {
        self.tree.root_hash()
    }
    pub fn lookup(&self, namespace: &[u8], id: [u8; 32]) -> Result<Option<Call>, Error> {
        match self
            .tree
            .get(&Nibbles::from_bytes(&[namespace, &id].concat()))?
        {
            Some(ae_trie::Value::Bytes(b)) => {
                ae_codec::rlp::decode_with_limits(&b, self.tree.store().decode_limits())
                    .map_err(|e| Error::Object(object::ObjectError::Rlp(e)))?;
                Ok(Some(Call::deserialize(id, &b)?))
            }
            None => Ok(None),
            _ => Err(Error::InvalidValue),
        }
    }
    // The calls tree is emptied before every new block; results are not
    // cumulative consensus state. Durable historical indexing is separate.
    pub fn prune(&mut self) {
        self.tree.clear();
    }
    pub fn transaction<T, E>(
        &mut self,
        action: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<T, E> {
        let root = self.tree.savepoint();
        let result = action(self);
        if result.is_err() {
            self.tree.rollback(root);
        }
        result
    }
}
impl<S: NodeWriter> Calls<S> {
    pub fn insert(&mut self, call: &Call) -> Result<(), Error> {
        self.write(call.target(), call, true)
    }
    pub fn enter_auth(&mut self, call: &Call) -> Result<(), Error> {
        self.write(&call.caller.value, call, false)
    }
    fn write(&mut self, namespace: &[u8], call: &Call, insert: bool) -> Result<(), Error> {
        let key = Nibbles::from_bytes(&[namespace, &call.id].concat());
        if insert && self.tree.get(&key)?.is_some() {
            return Err(Error::AlreadyPresent);
        }
        self.tree
            .put(&key, ae_trie::Value::Bytes(call.serialize()?))?;
        Ok(())
    }
}
