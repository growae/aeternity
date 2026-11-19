// aec_hc_vote_tx, aeternity fa2a6d7.
// Copyright (c) 2024, Aeternity Foundation. ISC notice in repository LICENSE.
use crate::{CanonicalTransaction, Error, TransactionKind, WireTransaction, ga};
use ae_codec::{api, object::Value, rlp::DecodeLimits};
use ae_protocol::ConsensusType;
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HcVote {
    voter: AccountPublicKey,
    epoch: Unsigned,
    vote_type: Unsigned,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    wire_size: usize,
}

// The original client view retains binary map keys and values. JSON adapters
// must not replace arbitrary bytes with lossy UTF-8 strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientVote<'a> {
    pub voter_id: String,
    pub epoch: &'a Unsigned,
    pub vote_type: &'a Unsigned,
    pub data: &'a BTreeMap<Vec<u8>, Vec<u8>>,
}

impl HcVote {
    pub fn new(
        voter: AccountPublicKey,
        epoch: Unsigned,
        vote_type: Unsigned,
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Self {
        let mut tx = Self {
            voter,
            epoch,
            vote_type,
            data,
            wire_size: 0,
        };
        tx.wire_size = tx.encode().len();
        tx
    }
    pub fn fee(&self) -> &Unsigned {
        static ZERO: std::sync::LazyLock<Unsigned> = std::sync::LazyLock::new(|| 0.into());
        &ZERO
    }
    pub fn nonce(&self) -> &Unsigned {
        self.fee()
    }
    pub fn ttl(&self) -> &Unsigned {
        self.fee()
    }
    pub fn gas(&self) -> &Unsigned {
        self.fee()
    }
    pub fn voter(&self) -> AccountPublicKey {
        self.voter
    }
    pub fn epoch(&self) -> &Unsigned {
        &self.epoch
    }
    pub fn vote_type(&self) -> &Unsigned {
        &self.vote_type
    }
    pub fn data(&self) -> &BTreeMap<Vec<u8>, Vec<u8>> {
        &self.data
    }
    pub fn wire_size(&self) -> usize {
        self.wire_size
    }
    pub fn valid_at_consensus(&self, consensus: ConsensusType) -> bool {
        consensus == ConsensusType::Pos
    }
    pub fn for_client(&self) -> ClientVote<'_> {
        ClientVote {
            voter_id: api::encode(api::ApiType::AccountPubkey, &self.voter.0),
            epoch: &self.epoch,
            vote_type: &self.vote_type,
            data: &self.data,
        }
    }
    pub fn decode_with_limits(bytes: &[u8], limits: DecodeLimits) -> Result<Self, Error> {
        let wire = WireTransaction::decode_with_limits(bytes, limits)?;
        if wire.kind() != TransactionKind::HcVote {
            return Err(Error::WrongTransactionKind);
        }
        let voter = ga::account(&wire, "voter_id")?;
        let Some(Value::List(pairs)) = wire.field("data") else {
            unreachable!("checked schema")
        };
        let data = pairs
            .iter()
            .map(|pair| {
                let Value::Tuple(pair) = pair else {
                    unreachable!("checked schema")
                };
                let [Value::Binary(key), Value::Binary(value)] = pair.as_slice() else {
                    unreachable!("checked schema")
                };
                (key.clone(), value.clone())
            })
            .collect(); // maps:from_list: the last duplicate key wins.
        Ok(Self {
            voter,
            epoch: ga::int(&wire, "epoch"),
            vote_type: ga::int(&wire, "type"),
            data,
            wire_size: bytes.len(),
        })
    }
}
impl CanonicalTransaction for HcVote {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode_with_limits(bytes, DecodeLimits::default())
    }
    fn encode(&self) -> Vec<u8> {
        WireTransaction::new(
            TransactionKind::HcVote,
            1,
            vec![
                (
                    "voter_id".into(),
                    Value::Id(Id::new(IdTag::Account, self.voter.0)),
                ),
                ("epoch".into(), Value::Integer(self.epoch.clone())),
                ("type".into(), Value::Integer(self.vote_type.clone())),
                (
                    "data".into(),
                    Value::List(
                        self.data
                            .iter()
                            .map(|(k, v)| {
                                Value::Tuple(vec![
                                    Value::Binary(k.clone()),
                                    Value::Binary(v.clone()),
                                ])
                            })
                            .collect(),
                    ),
                ),
            ],
        )
        .expect("typed HC vote schema")
        .encode()
    }
}
