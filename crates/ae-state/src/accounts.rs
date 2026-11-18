// Account layouts and flag behavior: aeternity fa2a6d7, aec_accounts.erl.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use ae_codec::object::{self, Field, FieldType, ObjectError, ObjectTag, Value};
use ae_trie::{MemoryStore, Nibbles, NodeReader, NodeWriter, Trie};
use ae_types::{AccountPublicKey, Hash256, Id, IdTag, TypeError, Unsigned};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountError {
    Object(ObjectError),
    Numeric(TypeError),
    Tree(ae_trie::Error),
    InvalidVersion,
    InvalidAccountValue,
    InvalidContractId,
}
impl From<ObjectError> for AccountError {
    fn from(e: ObjectError) -> Self {
        Self::Object(e)
    }
}
impl From<TypeError> for AccountError {
    fn from(e: TypeError) -> Self {
        Self::Numeric(e)
    }
}
impl From<ae_trie::Error> for AccountError {
    fn from(e: ae_trie::Error) -> Self {
        Self::Tree(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorization {
    pub contract: Id,
    pub function: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Account {
    public_key: AccountPublicKey,
    balance: Unsigned,
    nonce: Unsigned,
    flags: Unsigned,
    authorization: Option<Authorization>,
}
impl Account {
    pub fn new(public_key: AccountPublicKey, balance: Unsigned) -> Self {
        Self {
            public_key,
            balance,
            nonce: 0.into(),
            flags: 0.into(),
            authorization: None,
        }
    }
    pub fn public_key(&self) -> AccountPublicKey {
        self.public_key
    }
    pub fn balance(&self) -> &Unsigned {
        &self.balance
    }
    pub fn nonce(&self) -> &Unsigned {
        &self.nonce
    }
    pub fn flags(&self) -> &Unsigned {
        &self.flags
    }
    pub fn authorization(&self) -> Option<&Authorization> {
        self.authorization.as_ref()
    }
    pub fn is_payable(&self) -> bool {
        !self.flags.low_bit()
    }
    pub fn set_nonce(&mut self, nonce: Unsigned) {
        self.nonce = nonce;
    }
    pub fn set_non_payable(&mut self, non_payable: bool) {
        self.flags = self.flags.with_low_bit(non_payable);
    }
    pub fn set_payable(&mut self, argument: bool) {
        // Preserve the reference setter's inverted argument semantics. The FATE
        // existing-account path calls this function; changing it changes state.
        self.flags = self.flags.with_low_bit(argument);
    }
    pub fn attach_authorization(
        &mut self,
        contract: Id,
        function: [u8; 32],
    ) -> Result<(), AccountError> {
        if contract.tag != IdTag::Contract {
            return Err(AccountError::InvalidContractId);
        }
        self.authorization = Some(Authorization {
            contract,
            function: function.to_vec(),
        });
        Ok(())
    }
    pub fn earn(&mut self, amount: &Unsigned) {
        self.balance = self.balance.add(amount);
    }
    pub fn spend(&mut self, amount: &Unsigned, nonce: Unsigned) -> Result<(), AccountError> {
        self.spend_without_nonce_bump(amount)?;
        self.nonce = nonce;
        Ok(())
    }
    pub fn spend_without_nonce_bump(&mut self, amount: &Unsigned) -> Result<(), AccountError> {
        self.balance = self.balance.checked_sub(amount)?;
        Ok(())
    }

    pub fn serialize(&self) -> Result<Vec<u8>, AccountError> {
        let version = if self.authorization.is_some() {
            2
        } else if self.flags == 0.into() {
            1
        } else {
            3
        };
        let mut values = Vec::new();
        if version != 1 {
            values.push(("flags".into(), Value::Integer(self.flags.clone())));
        }
        values.push(("nonce".into(), Value::Integer(self.nonce.clone())));
        values.push(("balance".into(), Value::Integer(self.balance.clone())));
        if let Some(auth) = &self.authorization {
            values.push(("ga_contract".into(), Value::Id(auth.contract)));
            values.push(("ga_auth_fun".into(), Value::Binary(auth.function.clone())));
        }
        Ok(object::serialize(
            ObjectTag::Account,
            &version.into(),
            &template(version)?,
            &values,
        )?)
    }

    pub fn deserialize(public_key: AccountPublicKey, bytes: &[u8]) -> Result<Self, AccountError> {
        let envelope = object::decode_envelope(bytes)?;
        let version = envelope
            .version
            .to_u64()
            .map_err(|_| AccountError::InvalidVersion)?;
        let fields = object::deserialize(
            bytes,
            ObjectTag::Account,
            &version.into(),
            &template(version)?,
        )?;
        let mut account = Self::new(public_key, 0.into());
        let mut contract = None;
        let mut function = None;
        for (name, value) in fields {
            match (name.as_str(), value) {
                ("nonce", Value::Integer(n)) => account.nonce = n,
                ("balance", Value::Integer(n)) => account.balance = n,
                ("flags", Value::Integer(n)) => account.flags = n,
                ("ga_contract", Value::Id(id)) => contract = Some(id),
                ("ga_auth_fun", Value::Binary(bytes)) => function = Some(bytes),
                _ => return Err(AccountError::InvalidAccountValue),
            }
        }
        if version == 2 {
            account.authorization = Some(Authorization {
                contract: contract.ok_or(AccountError::InvalidAccountValue)?,
                function: function.ok_or(AccountError::InvalidAccountValue)?,
            });
        }
        Ok(account)
    }
}

fn template(version: u64) -> Result<Vec<Field>, AccountError> {
    let fields = match version {
        1 => vec![
            ("nonce", FieldType::Integer),
            ("balance", FieldType::Integer),
        ],
        2 => vec![
            ("flags", FieldType::Integer),
            ("nonce", FieldType::Integer),
            ("balance", FieldType::Integer),
            ("ga_contract", FieldType::Id),
            ("ga_auth_fun", FieldType::Binary),
        ],
        3 => vec![
            ("flags", FieldType::Integer),
            ("nonce", FieldType::Integer),
            ("balance", FieldType::Integer),
        ],
        _ => return Err(AccountError::InvalidVersion),
    };
    Ok(fields
        .into_iter()
        .map(|(name, kind)| Field {
            name: name.into(),
            kind,
        })
        .collect())
}

#[derive(Debug, Clone)]
pub struct Accounts<S = MemoryStore> {
    tree: Trie<S>,
}
impl Default for Accounts<MemoryStore> {
    fn default() -> Self {
        Self::new(MemoryStore::default())
    }
}
impl<S: NodeReader> Accounts<S> {
    pub fn new(store: S) -> Self {
        Self {
            tree: Trie::new(store),
        }
    }
    pub fn from_root(root: Option<Hash256>, store: S) -> Result<Self, AccountError> {
        Ok(Self {
            tree: Trie::from_root(root, store)?,
        })
    }
    pub fn root_hash(&self) -> Option<Hash256> {
        self.tree.root_hash()
    }
    pub fn tree(&self) -> &Trie<S> {
        &self.tree
    }
    pub fn lookup(&self, key: AccountPublicKey) -> Result<Option<Account>, AccountError> {
        match self.tree.get(&Nibbles::from_bytes(key.as_ref()))? {
            None => Ok(None),
            Some(ae_trie::Value::Bytes(bytes)) => Ok(Some(Account::deserialize(key, &bytes)?)),
            Some(_) => Err(AccountError::InvalidAccountValue),
        }
    }
}
impl<S: NodeWriter> Accounts<S> {
    pub fn apply(&mut self, accounts: &[Account]) -> Result<(), AccountError> {
        let updates = accounts
            .iter()
            .map(|account| {
                Ok((
                    Nibbles::from_bytes(account.public_key.as_ref()),
                    ae_trie::Value::Bytes(account.serialize()?),
                ))
            })
            .collect::<Result<Vec<_>, AccountError>>()?;
        self.tree.apply_batch(updates)?;
        Ok(())
    }
    pub fn enter(&mut self, account: &Account) -> Result<(), AccountError> {
        self.tree.put(
            &Nibbles::from_bytes(account.public_key.as_ref()),
            ae_trie::Value::Bytes(account.serialize()?),
        )?;
        Ok(())
    }
    pub fn delete(&mut self, key: AccountPublicKey) -> Result<(), AccountError> {
        self.tree.delete(&Nibbles::from_bytes(key.as_ref()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_three_account_layouts_and_reference_payability_setter() {
        let key = AccountPublicKey([1; 32]);
        let mut account = Account::new(key, 42.into());
        assert_eq!(account.serialize().unwrap(), [0xc4, 10, 1, 0, 42]);
        account.set_payable(true);
        assert!(!account.is_payable());
        assert_eq!(account.serialize().unwrap(), [0xc5, 10, 3, 1, 0, 42]);
        account.set_payable(false);
        assert!(account.is_payable());
        account
            .attach_authorization(Id::new(IdTag::Contract, [2; 32]), [3; 32])
            .unwrap();
        let bytes = account.serialize().unwrap();
        assert_eq!(object::decode_envelope(&bytes).unwrap().version, 2.into());
        assert_eq!(Account::deserialize(key, &bytes).unwrap(), account);
    }
    #[test]
    fn spend_failure_does_not_mutate_balance_or_nonce() {
        let mut account = Account::new(AccountPublicKey([1; 32]), 5.into());
        let before = account.clone();
        assert!(account.spend(&6.into(), 1.into()).is_err());
        assert_eq!(account, before);
        account.spend(&2.into(), 1.into()).unwrap();
        assert_eq!(account.balance(), &3.into());
        assert_eq!(account.nonce(), &1.into());
    }
}
