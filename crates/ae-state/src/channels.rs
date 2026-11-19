// aesc_channels and aesc_state_tree, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::accounts::{Account, Authorization};
use ae_crypto::blake2b_256;
use ae_protocol::Protocol;
use ae_types::{AccountPublicKey, Hash256, Id, IdTag, Unsigned};

mod encoding;
mod tree;
pub use tree::Channels;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Object(ae_codec::object::ObjectError),
    Tree(ae_trie::Error),
    InvalidVersion,
    InvalidValue,
    InvalidIdType,
    InvalidAuthorization,
    InvalidDelegates,
    AmountUnderflow,
    NotParticipant,
    AlreadyPresent,
}
impl From<ae_codec::object::ObjectError> for Error {
    fn from(e: ae_codec::object::ObjectError) -> Self {
        Self::Object(e)
    }
}
impl From<ae_trie::Error> for Error {
    fn from(e: ae_trie::Error) -> Self {
        Self::Tree(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Initiator,
    Responder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delegates {
    Shared(Vec<Id>),
    Separate {
        initiator: Vec<Id>,
        responder: Vec<Id>,
    },
}
pub enum Nonce<'a> {
    Number(&'a Unsigned),
    Hash([u8; 32]),
}

pub fn channel_id(
    initiator: AccountPublicKey,
    nonce: Nonce<'_>,
    responder: AccountPublicKey,
) -> Hash256 {
    let mut bytes = [0; 96];
    bytes[..32].copy_from_slice(&initiator.0);
    match nonce {
        Nonce::Number(n) => {
            // Erlang's fixed-width bit syntax retains the low 256 bits.
            let raw = n.as_bytes();
            let len = raw.len().min(32);
            bytes[64 - len..64].copy_from_slice(&raw[raw.len() - len..]);
        }
        Nonce::Hash(hash) => bytes[32..64].copy_from_slice(&hash),
    }
    bytes[64..].copy_from_slice(&responder.0);
    blake2b_256(&bytes)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Channel {
    pub public_key: [u8; 32],
    pub initiator: AccountPublicKey,
    pub responder: AccountPublicKey,
    pub delegates: Delegates,
    pub channel_amount: Unsigned,
    pub initiator_amount: Unsigned,
    pub responder_amount: Unsigned,
    pub channel_reserve: Unsigned,
    pub initiator_auth: Option<Authorization>,
    pub responder_auth: Option<Authorization>,
    pub state_hash: Vec<u8>,
    pub round: Unsigned,
    pub solo_round: Unsigned,
    pub lock_period: Unsigned,
    pub locked_until: Unsigned,
    pub object_version: u8,
}
impl Channel {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        initiator: &Account,
        initiator_amount: Unsigned,
        responder: &Account,
        responder_amount: Unsigned,
        reserve: Unsigned,
        delegates: Delegates,
        state_hash: Vec<u8>,
        lock_period: Unsigned,
        nonce: Nonce<'_>,
        protocol: Protocol,
        round: Unsigned,
    ) -> Result<Self, Error> {
        let object_version = if protocol >= Protocol::Iris {
            3
        } else if protocol >= Protocol::Fortuna {
            2
        } else {
            1
        };
        let ids: Vec<_> = match (&delegates, object_version) {
            (Delegates::Shared(ids), 1 | 2) => ids.iter().collect(),
            (
                Delegates::Separate {
                    initiator,
                    responder,
                },
                3,
            ) => initiator.iter().chain(responder).collect(),
            _ => return Err(Error::InvalidDelegates),
        };
        if ids.iter().any(|id| id.tag != IdTag::Account) {
            return Err(Error::InvalidIdType);
        }
        Ok(Self {
            public_key: channel_id(initiator.public_key(), nonce, responder.public_key()).0,
            initiator: initiator.public_key(),
            responder: responder.public_key(),
            delegates,
            channel_amount: initiator_amount.add(&responder_amount),
            initiator_amount,
            responder_amount,
            channel_reserve: reserve,
            initiator_auth: initiator.authorization().cloned(),
            responder_auth: responder.authorization().cloned(),
            state_hash,
            round,
            solo_round: 0.into(),
            lock_period,
            locked_until: 0.into(),
            object_version,
        })
    }
    pub fn id(&self) -> Id {
        Id::new(IdTag::Channel, self.public_key)
    }
    pub fn peers(&self) -> [AccountPublicKey; 2] {
        [self.initiator, self.responder]
    }
    pub fn role(&self, id: Id) -> Option<Role> {
        if id == Id::new(IdTag::Account, self.initiator.0) {
            Some(Role::Initiator)
        } else if id == Id::new(IdTag::Account, self.responder.0) {
            Some(Role::Responder)
        } else {
            None
        }
    }
    pub fn authorization(&self, id: Id) -> Result<Option<&Authorization>, Error> {
        match self.role(id) {
            Some(Role::Initiator) => Ok(self.initiator_auth.as_ref()),
            Some(Role::Responder) => Ok(self.responder_auth.as_ref()),
            None => Err(Error::NotParticipant),
        }
    }
    pub fn auth_store_key(&self, id: Id) -> Hash256 {
        // The reference accepts every ID type here, not only participants.
        blake2b_256(&[self.public_key.as_slice(), &id.value].concat())
    }
    pub fn delegate_ids(&self, role: Option<Role>) -> Result<Vec<Id>, Error> {
        match (&self.delegates, self.object_version) {
            (Delegates::Shared(ids), 1 | 2) => Ok(ids.clone()),
            (
                Delegates::Separate {
                    initiator,
                    responder,
                },
                3,
            ) => Ok(match role {
                Some(Role::Initiator) => initiator.clone(),
                Some(Role::Responder) => responder.clone(),
                None => initiator.iter().chain(responder).copied().collect(),
            }),
            _ => Err(Error::InvalidDelegates),
        }
    }
    pub fn delegate_pubkeys(&self, role: Option<Role>) -> Result<Vec<AccountPublicKey>, Error> {
        self.delegate_ids(role)?
            .into_iter()
            .map(|id| {
                if id.tag == IdTag::Account {
                    Ok(AccountPublicKey(id.value))
                } else {
                    Err(Error::InvalidIdType)
                }
            })
            .collect()
    }
    pub fn set_delegates(
        &mut self,
        initiator: &[AccountPublicKey],
        responder: &[AccountPublicKey],
    ) {
        let ids =
            |keys: &[AccountPublicKey]| keys.iter().map(|k| Id::new(IdTag::Account, k.0)).collect();
        self.delegates = Delegates::Separate {
            initiator: ids(initiator),
            responder: ids(responder),
        };
        // Does not upgrade the object version; admission is a transaction rule.
    }
    pub fn is_active(&self) -> bool {
        self.locked_until == 0.into()
    }
    pub fn is_solo_closing(&self) -> bool {
        !self.is_active()
    }
    pub fn is_solo_closed(&self, height: &Unsigned) -> bool {
        self.is_solo_closing() && &self.locked_until <= height
    }
    pub fn is_last_state_forced(&self) -> bool {
        self.solo_round != 0.into()
    }
    pub fn snapshot(&mut self, round: Unsigned, state_hash: Vec<u8>) {
        self.round = round;
        self.state_hash = state_hash;
        self.solo_round = 0.into();
    }
    pub fn deposit(&mut self, amount: &Unsigned, round: Unsigned, state_hash: Vec<u8>) {
        self.channel_amount = self.channel_amount.add(amount);
        self.snapshot(round, state_hash);
    }
    pub fn withdraw(
        &mut self,
        amount: &Unsigned,
        round: Unsigned,
        state_hash: Vec<u8>,
    ) -> Result<(), Error> {
        // Negative balances have no serializable channel representation.
        let balance = self
            .channel_amount
            .checked_sub(amount)
            .map_err(|_| Error::AmountUnderflow)?;
        self.channel_amount = balance;
        self.snapshot(round, state_hash);
        Ok(())
    }
    // The transaction layer must supply balances obtained from verified account
    // proofs, and validate the payload signatures, round and committed root.
    pub fn close_solo(
        &mut self,
        balances: [Unsigned; 2],
        height: &Unsigned,
        payload: Option<(Unsigned, Vec<u8>)>,
    ) {
        [self.initiator_amount, self.responder_amount] = balances;
        self.locked_until = height.add(&self.lock_period);
        if let Some((round, state_hash)) = payload {
            self.round = round;
            self.state_hash = state_hash;
            if height >= &168_300.into() {
                self.solo_round = 0.into();
            }
        }
    }
    pub fn force_progress(
        &mut self,
        state_hash: Vec<u8>,
        round: Unsigned,
        balances: [Unsigned; 2],
        height: &Unsigned,
        has_payload: bool,
    ) {
        if self.solo_round == 0.into() || (has_payload && height >= &168_300.into()) {
            self.solo_round = round.clone();
        }
        self.round = round;
        self.state_hash = state_hash;
        [self.initiator_amount, self.responder_amount] = balances;
        if !self.is_active() {
            self.locked_until = height.add(&self.lock_period);
        }
    }
}
