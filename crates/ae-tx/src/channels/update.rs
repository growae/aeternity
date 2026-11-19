// aesc_offchain_update.erl, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::Error;
use ae_codec::{
    object::{self, Field, FieldType as T, ObjectTag, Value as V},
    rlp::DecodeLimits,
};
use ae_protocol::contracts::Version;
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Update {
    Transfer {
        from: AccountPublicKey,
        to: AccountPublicKey,
        amount: Unsigned,
    },
    Deposit {
        from: AccountPublicKey,
        amount: Unsigned,
    },
    Withdraw {
        to: AccountPublicKey,
        amount: Unsigned,
    },
    CreateContract {
        owner: AccountPublicKey,
        version: Version,
        code: Vec<u8>,
        deposit: Unsigned,
        call_data: Vec<u8>,
    },
    CallContract {
        caller: AccountPublicKey,
        contract: [u8; 32],
        abi_version: Unsigned,
        amount: Unsigned,
        gas: Unsigned,
        gas_price: Unsigned,
        call_data: Vec<u8>,
        call_stack: Vec<Unsigned>,
    },
    Meta {
        data: Vec<u8>,
    },
}
impl Update {
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode_with_limits(bytes, DecodeLimits::default())
    }
    pub fn decode_with_limits(bytes: &[u8], limits: DecodeLimits) -> Result<Self, Error> {
        let e = object::decode_envelope_with_limits(bytes, limits)?;
        let schema = template(
            e.tag,
            e.version.to_u64().map_err(|_| Error::UnsupportedVersion)?,
        )?;
        if e.fields.len() != schema.len() {
            return Err(object::ObjectError::FieldCount.into());
        }
        let fields = e
            .fields
            .iter()
            .zip(&schema)
            .map(|(v, f)| object::decode_field(&f.kind, v))
            .collect::<Result<Vec<_>, _>>()?;
        let account = |id: &Id| {
            if id.tag == IdTag::Account {
                Ok(AccountPublicKey(id.value))
            } else {
                Err(Error::InvalidIdType)
            }
        };
        Ok(match (e.tag, fields.as_slice()) {
            (
                ObjectTag::ChannelOffchainUpdateTransfer,
                [V::Id(from), V::Id(to), V::Integer(amount)],
            ) => Self::Transfer {
                from: account(from)?,
                to: account(to)?,
                amount: amount.clone(),
            },
            (ObjectTag::ChannelOffchainUpdateDeposit, [V::Id(from), V::Integer(amount)]) => {
                Self::Deposit {
                    from: account(from)?,
                    amount: amount.clone(),
                }
            }
            (ObjectTag::ChannelOffchainUpdateWithdraw, [V::Id(to), V::Integer(amount)]) => {
                Self::Withdraw {
                    to: account(to)?,
                    amount: amount.clone(),
                }
            }
            (
                ObjectTag::ChannelOffchainUpdateCreateContract,
                [
                    V::Id(owner),
                    V::Integer(version),
                    V::Binary(code),
                    V::Integer(deposit),
                    V::Binary(call_data),
                ],
            ) => Self::CreateContract {
                owner: account(owner)?,
                version: Version::unpack(version),
                code: code.clone(),
                deposit: deposit.clone(),
                call_data: call_data.clone(),
            },
            (
                ObjectTag::ChannelOffchainUpdateCallContract,
                [
                    V::Id(caller),
                    V::Id(contract),
                    V::Integer(abi_version),
                    V::Integer(amount),
                    V::Integer(gas),
                    V::Integer(gas_price),
                    V::Binary(call_data),
                    V::List(call_stack),
                ],
            ) => {
                if contract.tag != IdTag::Contract {
                    return Err(Error::InvalidIdType);
                }
                Self::CallContract {
                    caller: account(caller)?,
                    contract: contract.value,
                    abi_version: abi_version.clone(),
                    amount: amount.clone(),
                    gas: gas.clone(),
                    gas_price: gas_price.clone(),
                    call_data: call_data.clone(),
                    call_stack: call_stack
                        .iter()
                        .map(|v| match v {
                            V::Integer(n) => n.clone(),
                            _ => unreachable!("validated integer list"),
                        })
                        .collect(),
                }
            }
            (ObjectTag::ChannelOffchainUpdateMeta, [V::Binary(data)]) => {
                Self::Meta { data: data.clone() }
            }
            _ => return Err(object::ObjectError::WrongType.into()),
        })
    }
    pub fn encode(&self) -> Vec<u8> {
        let i = |k: AccountPublicKey| V::Id(Id::new(IdTag::Account, k.0));
        let n = |v: &Unsigned| V::Integer(v.clone());
        let (tag, version, values) = match self {
            Self::Transfer { from, to, amount } => (
                ObjectTag::ChannelOffchainUpdateTransfer,
                1,
                vec![i(*from), i(*to), n(amount)],
            ),
            Self::Deposit { from, amount } => (
                ObjectTag::ChannelOffchainUpdateDeposit,
                1,
                vec![i(*from), n(amount)],
            ),
            Self::Withdraw { to, amount } => (
                ObjectTag::ChannelOffchainUpdateWithdraw,
                1,
                vec![i(*to), n(amount)],
            ),
            Self::CreateContract {
                owner,
                version,
                code,
                deposit,
                call_data,
            } => (
                ObjectTag::ChannelOffchainUpdateCreateContract,
                1,
                vec![
                    i(*owner),
                    n(&version.pack()),
                    V::Binary(code.clone()),
                    n(deposit),
                    V::Binary(call_data.clone()),
                ],
            ),
            Self::CallContract {
                caller,
                contract,
                abi_version,
                amount,
                gas,
                gas_price,
                call_data,
                call_stack,
            } => (
                ObjectTag::ChannelOffchainUpdateCallContract,
                1,
                vec![
                    i(*caller),
                    V::Id(Id::new(IdTag::Contract, *contract)),
                    n(abi_version),
                    n(amount),
                    n(gas),
                    n(gas_price),
                    V::Binary(call_data.clone()),
                    V::List(call_stack.iter().map(n).collect()),
                ],
            ),
            Self::Meta { data } => (
                ObjectTag::ChannelOffchainUpdateMeta,
                2,
                vec![V::Binary(data.clone())],
            ),
        };
        let schema = template(tag, version).expect("typed update");
        let fields = schema
            .iter()
            .zip(values)
            .map(|(f, v)| (f.name.clone(), v))
            .collect::<Vec<_>>();
        object::serialize(tag, &version.into(), &schema, &fields).expect("typed update fields")
    }
    pub fn amounts(&self) -> Option<(&Unsigned, &Unsigned, &Unsigned)> {
        match self {
            Self::CallContract {
                amount,
                gas_price,
                gas,
                ..
            } => Some((amount, gas_price, gas)),
            _ => None,
        }
    }
}
fn template(tag: ObjectTag, version: u64) -> Result<Vec<Field>, Error> {
    let fields = match (tag, version) {
        (ObjectTag::ChannelOffchainUpdateTransfer, 1) => {
            vec![("from", T::Id), ("to", T::Id), ("amount", T::Integer)]
        }
        (ObjectTag::ChannelOffchainUpdateDeposit, 1) => {
            vec![("from", T::Id), ("amount", T::Integer)]
        }
        (ObjectTag::ChannelOffchainUpdateWithdraw, 1) => {
            vec![("to", T::Id), ("amount", T::Integer)]
        }
        (ObjectTag::ChannelOffchainUpdateCreateContract, 1) => vec![
            ("owner", T::Id),
            ("ct_version", T::Integer),
            ("code", T::Binary),
            ("deposit", T::Integer),
            ("call_data", T::Binary),
        ],
        (ObjectTag::ChannelOffchainUpdateCallContract, 1) => vec![
            ("caller", T::Id),
            ("contract", T::Id),
            ("abi_version", T::Integer),
            ("amount", T::Integer),
            ("gas", T::Integer),
            ("gas_price", T::Integer),
            ("call_data", T::Binary),
            ("call_stack", T::List(Box::new(T::Integer))),
        ],
        (ObjectTag::ChannelOffchainUpdateMeta, 2) => vec![("data", T::Binary)],
        _ => return Err(Error::UnsupportedVersion),
    };
    Ok(fields
        .into_iter()
        .map(|(n, kind)| Field {
            name: n.into(),
            kind,
        })
        .collect())
}
