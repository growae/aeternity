use super::*;
use crate::{
    Trees, accounts::Accounts, calls::Calls, channels::Channels, contracts::Contracts,
    names::Names, oracles::Oracles,
};

impl<A: NodeReader, S: NodeReader> Trees<A, S> {
    pub fn serialize_to_client(&self, limits: Limits) -> Result<String, Error> {
        Ok(ae_codec::api::encode(
            ae_codec::api::ApiType::StateTrees,
            &self.serialize_to_binary(limits)?,
        ))
    }
    pub fn serialize_to_binary(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        let b = Budget::new(limits);
        let fields = vec![
            (
                "contracts",
                FieldType::Binary,
                Value::Binary(mtree::serialize(
                    self.contracts.tree(),
                    ObjectTag::ContractsMtree,
                    "contracts",
                    &b,
                )?),
            ),
            (
                "calls",
                FieldType::Binary,
                Value::Binary(mtree::serialize(
                    self.calls.tree(),
                    ObjectTag::CallsMtree,
                    "calls",
                    &b,
                )?),
            ),
            (
                "channels",
                FieldType::Binary,
                Value::Binary(mtree::serialize(
                    self.channels.tree(),
                    ObjectTag::ChannelsMtree,
                    "channels",
                    &b,
                )?),
            ),
            (
                "ns",
                FieldType::Binary,
                Value::Binary(mtree::serialize(
                    self.names.tree(),
                    ObjectTag::NameserviceMtree,
                    "mtree",
                    &b,
                )?),
            ),
            (
                "oracles",
                FieldType::Binary,
                Value::Binary(mtree::serialize(
                    self.oracles.tree(),
                    ObjectTag::OraclesMtree,
                    "otree",
                    &b,
                )?),
            ),
            (
                "accounts",
                FieldType::Binary,
                Value::Binary(mtree::serialize(
                    self.accounts.tree(),
                    ObjectTag::AccountsMtree,
                    "accounts",
                    &b,
                )?),
            ),
        ];
        let out = encode(ObjectTag::StateTrees, fields)?;
        b.charge(out.len())?;
        Ok(out)
    }
}
impl<A: NodeWriter, S: NodeWriter + Clone> Trees<A, S> {
    pub fn deserialize_from_binary(
        bytes: &[u8],
        accounts_store: A,
        state_store: S,
        config: Config<'_>,
    ) -> Result<Self, Error> {
        let b = Budget::new(config.limits);
        let fields = b.decode(
            bytes,
            ObjectTag::StateTrees,
            &[
                FieldType::Binary,
                FieldType::Binary,
                FieldType::Binary,
                FieldType::Binary,
                FieldType::Binary,
                FieldType::Binary,
            ],
        )?;
        let mut fields = fields.into_iter();
        let mut next = || match fields.next() {
            Some(Value::Binary(bytes)) => Ok(bytes),
            _ => Err(Error::InvalidValue),
        };
        let c = mtree::deserialize(&next()?, ObjectTag::ContractsMtree, state_store.clone(), &b)?;
        let calls = mtree::deserialize(&next()?, ObjectTag::CallsMtree, state_store.clone(), &b)?;
        let channels = Channels::from(mtree::deserialize(
            &next()?,
            ObjectTag::ChannelsMtree,
            state_store.clone(),
            &b,
        )?);
        let n = mtree::deserialize(
            &next()?,
            ObjectTag::NameserviceMtree,
            state_store.clone(),
            &b,
        )?;
        let names = Names::from_transport(n, state_store.clone(), config, &b)?;
        let o = mtree::deserialize(&next()?, ObjectTag::OraclesMtree, state_store.clone(), &b)?;
        let oracles = Oracles::from_transport(o, state_store, &b)?;
        let a = mtree::deserialize(&next()?, ObjectTag::AccountsMtree, accounts_store, &b)?;
        Ok(Self {
            accounts: Accounts::from_root(a.root_hash(), a.into_store()).map_err(Error::from)?,
            calls: Calls::from_root(calls.root_hash(), calls.into_store()).map_err(Error::from)?,
            channels,
            contracts: Contracts::from_root(c.root_hash(), c.into_store()).map_err(Error::from)?,
            names,
            oracles,
        })
    }
}
