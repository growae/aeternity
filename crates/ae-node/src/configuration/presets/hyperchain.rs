use super::{Error, Limits, balance};
use crate::configuration::{protocols, resolution::Resolved};
use ae_codec::api::{self, ApiType};
use ae_hyperchain::{
    consensus_contract::Contracts,
    genesis::{self, Deployment, InitialCall, Validator},
};
use ae_protocol::contracts::Version;
use ae_types::AccountPublicKey;
use num_bigint::BigInt;
use serde_json::{Map, Value};

#[derive(Debug, Clone)]
pub struct Settings {
    pub contracts: Contracts,
    pub validators: Vec<Validator>,
    pub epoch_length: BigInt,
    pub pin_reward: BigInt,
}
fn field<'a>(m: &'a Map<String, Value>, name: &'static str) -> Result<&'a Value, Error> {
    m.get(name).ok_or(Error::Configuration(name))
}
fn object<'a>(v: &'a Value, name: &'static str) -> Result<&'a Map<String, Value>, Error> {
    v.as_object().ok_or(Error::Configuration(name))
}
fn binary(m: &Map<String, Value>, name: &'static str, kind: ApiType) -> Result<Vec<u8>, Error> {
    api::decode_as(
        kind,
        field(m, name)?.as_str().ok_or(Error::Configuration(name))?,
    )
    .map_err(|_| Error::Configuration(name))
}
fn public(m: &Map<String, Value>, name: &'static str, kind: ApiType) -> Result<[u8; 32], Error> {
    binary(m, name, kind)?
        .try_into()
        .map_err(|_| Error::Configuration(name))
}
fn unsigned(m: &Map<String, Value>, name: &'static str) -> Result<ae_types::Unsigned, Error> {
    balance(field(m, name)?).map_err(|_| Error::Configuration(name))
}
fn array<'a>(
    m: &'a Map<String, Value>,
    name: &'static str,
    max: usize,
) -> Result<&'a [Value], Error> {
    let values = field(m, name)?
        .as_array()
        .ok_or(Error::Configuration(name))?;
    if values.len() > max {
        return Err(Error::Limit);
    }
    Ok(values)
}
impl Settings {
    pub fn resolve(resolved: &Resolved<'_>, limits: Limits) -> Result<Self, Error> {
        // The native accessors deliberately read the user height-zero subtree.
        let cfg = resolved
            .get(&["chain", "consensus", "0", "config"])
            .map_err(Error::Resolution)?
            .ok_or(Error::Configuration("consensus config"))?;
        Self::from_value(cfg, limits)
    }
    pub fn from_value(cfg: &Value, limits: Limits) -> Result<Self, Error> {
        super::super::budget(cfg).map_err(|_| Error::Limit)?;
        let m = object(cfg, "consensus config")?;
        let contracts = Contracts {
            owner: AccountPublicKey(public(m, "contract_owner", ApiType::AccountPubkey)?),
            election: public(m, "election_contract", ApiType::ContractPubkey)?,
            staking: public(m, "staking_contract", ApiType::ContractPubkey)?,
            rewards: public(m, "rewards_contract", ApiType::ContractPubkey)?,
        };
        let epoch_length = protocols::integer(field(m, "child_epoch_length")?)
            .ok_or(Error::Configuration("child_epoch_length"))?;
        let pin_reward = m
            .get("pinning_reward_value")
            .map_or(Ok(BigInt::from(0)), |v| {
                protocols::integer(v).ok_or(Error::Configuration("pinning_reward_value"))
            })?;
        let mut validators = vec![];
        if m.contains_key("initial_validators") {
            for v in array(m, "initial_validators", limits.validators)? {
                let v = object(v, "validator")?;
                let owner = AccountPublicKey(public(v, "owner", ApiType::AccountPubkey)?);
                let key = |name| {
                    if v.contains_key(name) {
                        public(v, name, ApiType::AccountPubkey).map(AccountPublicKey)
                    } else {
                        Ok(owner)
                    }
                };
                validators.push(Validator {
                    owner,
                    sign_key: key("sign_key")?,
                    caller: key("caller")?,
                    stake: v.get("stake").map_or(Ok(0.into()), balance)?,
                    restake: v.get("restake").map_or(Ok(false), |b| {
                        b.as_bool().ok_or(Error::Configuration("restake"))
                    })?,
                });
            }
        }
        Ok(Self {
            contracts,
            validators,
            epoch_length,
            pin_reward,
        })
    }
    pub fn plan(&self, bytes: &[u8], limits: Limits) -> Result<genesis::Plan, Error> {
        if bytes.len() > limits.bytes {
            return Err(Error::Limit);
        }
        let v: Value = serde_json::from_slice(bytes).map_err(|_| Error::Json)?;
        super::super::budget(&v).map_err(|_| Error::Limit)?;
        let m = object(&v, "contracts file")?;
        let mut deployments = vec![];
        for v in array(m, "contracts", limits.contracts)? {
            let m = object(v, "contract")?;
            let version = |name| {
                unsigned(m, name)?
                    .to_u64()
                    .ok()
                    .and_then(|n| u16::try_from(n).ok())
                    .ok_or(Error::Configuration(name))
            };
            deployments.push(Deployment {
                owner: AccountPublicKey(public(m, "owner_pubkey", ApiType::AccountPubkey)?),
                public_key: public(m, "pubkey", ApiType::ContractPubkey)?,
                nonce: unsigned(m, "nonce")?,
                amount: unsigned(m, "amount")?,
                version: Version {
                    vm: version("vm_version")?,
                    abi: version("abi_version")?,
                },
                code: binary(m, "code", ApiType::ContractBytearray)?,
                calldata: binary(m, "call_data", ApiType::ContractBytearray)?,
            });
        }
        let mut calls = vec![];
        for v in array(m, "calls", limits.calls)? {
            let m = object(v, "call")?;
            calls.push(InitialCall {
                caller: AccountPublicKey(public(m, "caller", ApiType::AccountPubkey)?),
                contract: public(m, "contract_pubkey", ApiType::ContractPubkey)?,
                nonce: unsigned(m, "nonce")?,
                abi: unsigned(m, "abi_version")?,
                fee: unsigned(m, "fee")?,
                amount: unsigned(m, "amount")?,
                gas: unsigned(m, "gas")?,
                gas_price: unsigned(m, "gas_price")?,
                calldata: binary(m, "call_data", ApiType::ContractBytearray)?,
            });
        }
        Ok(genesis::Plan {
            contracts: self.contracts,
            deployments,
            calls,
            validators: self.validators.clone(),
            epoch_length: self.epoch_length.clone(),
            pin_reward: self.pin_reward.clone(),
        })
    }
}
