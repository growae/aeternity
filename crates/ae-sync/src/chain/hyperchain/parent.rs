// aec_parent_connector utilities, aeternity fa2a6d7. ISC: repository LICENSE.
use super::{eoe, scoped};
use crate::{chain, parent_pins};
use ae_chain::repository as repo;
use ae_hyperchain::{chain::RunEnv, pin};
use ae_storage::Database;
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use ae_vm_fate::maps::MapOrder;
use num_bigint::BigInt;
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    Source(chain::Error),
    Parent(parent_pins::Error),
    State(&'static str),
    LeaderUnavailable,
    MissingAccount(AccountPublicKey),
    Payload(pin::Error),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signer {
    PresetKeys,
    None,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub chain: parent_pins::Chain,
    pub network: Arc<[u8]>,
    pub signer: Signer,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Data {
    pub pin: pin::Parent,
    pub payload: Vec<u8>,
    pub last_leader: AccountPublicKey,
    pub parent: Metadata,
    pub selected_tip: Hash256,
    pub selected_revision: u64,
}
pub(crate) struct Inputs<O> {
    pub database: Arc<Database>,
    pub source: Arc<eoe::Source>,
    pub execution: repo::HyperchainConfig<O>,
    pub producer: scoped::Producer<O>,
    pub pins: parent_pins::Client,
    pub network: Vec<u8>,
    pub accounts: BTreeMap<AccountPublicKey, AccountPublicKey>,
    pub maximum_payload_bytes: usize,
}
struct Shared<O> {
    source: Weak<eoe::Source>,
    execution: repo::HyperchainConfig<O>,
    producer: scoped::Producer<O>,
    pins: parent_pins::Client,
    metadata: Metadata,
    accounts: BTreeMap<AccountPublicKey, AccountPublicKey>,
    maximum_payload_bytes: usize,
}
pub struct Client<O>(Arc<Shared<O>>);
impl<O> Clone for Client<O> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Client<O> {
    pub(crate) fn new(input: Inputs<O>) -> Result<Self, Error> {
        if input.network.len() > 4096
            || input.accounts.len() > 65536
            || !(1..=65536).contains(&input.maximum_payload_bytes)
            || !input.source.uses_database(&input.database)
            || !input.producer.uses_database(&input.database)
            || !input
                .source
                .matches(&input.execution.identity, &input.execution.profile.network)
        {
            return Err(Error::Configuration);
        }
        input.producer.check().map_err(Error::Source)?;
        let chain = input.pins.chain().map_err(Error::Parent)?;
        let signing = input.pins.signing_network().map_err(Error::Parent)?;
        if chain == parent_pins::Chain::Aeternity
            && signing
                .as_ref()
                .is_some_and(|n| n.as_ref() != input.network)
        {
            return Err(Error::Configuration);
        }
        let signer = if chain == parent_pins::Chain::Aeternity && signing.is_some() {
            Signer::PresetKeys
        } else {
            Signer::None
        };
        Ok(Self(Arc::new(Shared {
            source: Arc::downgrade(&input.source),
            execution: input.execution,
            producer: input.producer,
            pins: input.pins,
            metadata: Metadata {
                chain,
                network: input.network.into(),
                signer,
            },
            accounts: input.accounts,
            maximum_payload_bytes: input.maximum_payload_bytes,
        })))
    }
    pub fn check(&self) -> Result<(), Error> {
        self.0.producer.check().map_err(Error::Source)?;
        self.0.source.upgrade().ok_or(Error::Closed)?;
        self.0.pins.check().map_err(Error::Parent)
    }
    pub fn metadata(&self) -> Result<Metadata, Error> {
        self.check()?;
        Ok(self.0.metadata.clone())
    }
    pub fn has_parent_account(&self, who: AccountPublicKey) -> Result<bool, Error> {
        self.check()?;
        Ok(self.0.accounts.contains_key(&who))
    }
    pub(crate) fn pinning_binding(&self) -> Result<Hash256, Error> {
        self.check()?;
        let mut bytes = b"ae-pinning-agent-v1".to_vec();
        fn append(bytes: &mut Vec<u8>, value: &[u8]) {
            bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
            bytes.extend_from_slice(value);
        }
        append(&mut bytes, &self.0.execution.identity.binding());
        append(&mut bytes, &self.0.metadata.network);
        bytes.push(match self.0.metadata.chain {
            parent_pins::Chain::Aeternity => 0,
            parent_pins::Chain::Dogecoin => 1,
            parent_pins::Chain::Bitcoin => 2,
        });
        bytes.push(u8::from(self.0.metadata.signer == Signer::PresetKeys));
        bytes.extend_from_slice(&self.0.execution.profile.contracts.election);
        bytes.extend_from_slice(&(self.0.accounts.len() as u64).to_be_bytes());
        for (child, parent) in &self.0.accounts {
            bytes.extend_from_slice(&child.0);
            bytes.extend_from_slice(&parent.0);
        }
        Ok(ae_crypto::blake2b_256(&bytes))
    }
    pub(crate) fn mapped(&self, who: AccountPublicKey) -> bool {
        self.0.accounts.contains_key(&who)
    }
    pub(crate) fn bound_to(
        &self,
        producer: &scoped::Producer<O>,
        source: &Arc<eoe::Source>,
    ) -> bool {
        self.0.producer.same_owner(producer) && self.0.source.ptr_eq(&Arc::downgrade(source))
    }
    pub(crate) fn service(&self) -> &parent_pins::Client {
        &self.0.pins
    }
    // This is a bounded blocking repository query, with no parent HTTP. Top
    // deliberately resolves a key at the selected height, as in aec_chain_hc.
    pub fn pinning_data(&self) -> Result<Data, Error> {
        self.check()?;
        let source = self.0.source.upgrade().ok_or(Error::Closed)?;
        let captured = source
            .snapshots()
            .view()
            .map_err(|_| Error::State("selected snapshot"))?;
        let view = captured.value;
        let tip = view.status().tip.ok_or(Error::State("selected tip"))?;
        let block = view
            .require_block(tip)
            .map_err(|_| Error::State("selected block"))?;
        let execution = &self.0.execution;
        let mut profile = execution.profile.as_ref().clone();
        profile.network = view
            .protocol_profile(
                &profile.network,
                block.header().common().height,
                execution.limits.history,
            )
            .map_err(|_| Error::State("selected protocol"))?;
        let reader = execution
            .reader(view.clone(), Arc::new(profile))
            .map_err(|_| Error::State("selected reader"))?;
        let info = reader
            .epoch_info(&RunEnv::Top)
            .map_err(|_| Error::State("current epoch"))?;
        let ae_vm_fate::value::Value::Integer(epoch) = info.epoch else {
            return Err(Error::State("epoch integer"));
        };
        let height: BigInt = info.first - 1;
        let h: u64 = height
            .clone()
            .try_into()
            .map_err(|_| Error::State("previous epoch height"))?;
        let hash = view
            .canonical_key(h)
            .map_err(|_| Error::State("previous epoch key"))?
            .ok_or(Error::State("missing previous epoch key"))?;
        let last_leader = self
            .0
            .producer
            .cached_leader(&info.last)
            .map_err(Error::Source)?
            .ok_or(Error::LeaderUnavailable)?;
        let pin = pin::Parent {
            epoch: epoch - 1,
            height,
            block_hash: hash.0.to_vec(),
        };
        let payload =
            pin::encode_parent(&pin, self.0.maximum_payload_bytes).map_err(Error::Payload)?;
        self.check()?;
        let current = source
            .snapshots()
            .view()
            .map_err(|_| Error::State("selected snapshot"))?;
        if current.value.status().revision != view.status().revision
            || current.value.status().tip != Some(tip)
        {
            return Err(Error::State("selected state changed"));
        }
        Ok(Data {
            pin,
            payload,
            last_leader,
            parent: self.0.metadata.clone(),
            selected_tip: tip,
            selected_revision: view.status().revision,
        })
    }
    pub(crate) fn prepare_post(
        &self,
        who: AccountPublicKey,
        amount: Unsigned,
        fee: Unsigned,
    ) -> Result<parent_pins::posting::Post, Error> {
        self.check()?;
        let account = *self
            .0
            .accounts
            .get(&who)
            .ok_or(Error::MissingAccount(who))?;
        Ok(parent_pins::posting::Post {
            pin: self.pinning_data()?.pin,
            account,
            amount,
            fee,
        })
    }
    // Busy means the request was not queued. An accepted request must be polled
    // through its receipt; dropping it cannot undo a possible parent broadcast.
    pub fn post(
        &self,
        who: AccountPublicKey,
        amount: Unsigned,
        fee: Unsigned,
    ) -> Result<parent_pins::posting::Request, Error> {
        let post = self.prepare_post(who, amount, fee)?;
        self.check()?;
        self.0.pins.post(post).map_err(Error::Parent)
    }
    pub fn pin_by_hash(&self, hash: &[u8]) -> Result<parent_pins::Request, Error> {
        self.check()?;
        self.0.pins.request(hash).map_err(Error::Parent)
    }
}
