use super::*;
use crate::ga::{binary, int};
use ae_codec::rlp::DecodeLimits;

fn id(w: &WireTransaction, name: &str) -> Id {
    match w.field(name) {
        Some(Value::Id(id)) => *id,
        _ => unreachable!("validated channel schema"),
    }
}
fn typed_id(w: &WireTransaction, name: &str, tag: IdTag) -> Result<[u8; 32], Error> {
    let id = id(w, name);
    if id.tag == tag {
        Ok(id.value)
    } else {
        Err(Error::InvalidIdType)
    }
}
fn account(w: &WireTransaction, name: &str) -> Result<AccountPublicKey, Error> {
    Ok(AccountPublicKey(typed_id(w, name, IdTag::Account)?))
}
fn ids(w: &WireTransaction, name: &str) -> Vec<Id> {
    match w.field(name) {
        Some(Value::List(v)) => v
            .iter()
            .map(|x| match x {
                Value::Id(id) => *id,
                _ => unreachable!("ID list"),
            })
            .collect(),
        _ => unreachable!("ID list"),
    }
}
fn accounts(w: &WireTransaction, name: &str) -> Result<Vec<AccountPublicKey>, Error> {
    ids(w, name)
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
fn state_hash(w: &WireTransaction) -> Result<[u8; 32], Error> {
    binary(w, "state_hash")
        .try_into()
        .map_err(|_| Error::InvalidStateHash)
}
impl ChannelTransaction {
    pub fn decode_with_config(bytes: &[u8], config: StateConfig<'_>) -> Result<Self, Error> {
        let limits = DecodeLimits {
            max_input_bytes: config.limits.max_input_bytes,
            max_items: config.limits.max_items,
            max_depth: config.limits.max_depth,
        };
        let w = WireTransaction::decode_with_limits(bytes, limits)?;
        use TransactionKind as K;
        let c = || typed_id(&w, "channel_id", IdTag::Channel);
        let from = || account(&w, "from_id");
        let payload = || binary(&w, "payload");
        let operation = match w.kind() {
            K::ChannelCreate => Operation::Create {
                initiator: account(&w, "initiator_id")?,
                initiator_amount: int(&w, "initiator_amount"),
                responder: account(&w, "responder_id")?,
                responder_amount: int(&w, "responder_amount"),
                reserve: int(&w, "channel_reserve"),
                lock_period: int(&w, "lock_period"),
                delegates: if w.version() == 1 {
                    CreateDelegates::Shared(accounts(&w, "delegate_ids")?)
                } else {
                    CreateDelegates::Separate {
                        initiator: accounts(&w, "initiator_delegate_ids")?,
                        responder: accounts(&w, "responder_delegate_ids")?,
                    }
                },
                state_hash: state_hash(&w)?,
            },
            K::ChannelDeposit => Operation::Deposit {
                channel: c()?,
                from: from()?,
                amount: int(&w, "amount"),
                state_hash: state_hash(&w)?,
                round: int(&w, "round"),
            },
            K::ChannelWithdraw => Operation::Withdraw {
                channel: c()?,
                to: account(&w, "to_id")?,
                amount: int(&w, "amount"),
                state_hash: state_hash(&w)?,
                round: int(&w, "round"),
            },
            K::ChannelCloseMutual => Operation::CloseMutual {
                channel: c()?,
                from: id(&w, "from_id"),
                initiator_amount: int(&w, "initiator_amount_final"),
                responder_amount: int(&w, "responder_amount_final"),
            },
            K::ChannelSettle => Operation::Settle {
                channel: c()?,
                from: from()?,
                initiator_amount: int(&w, "initiator_amount_final"),
                responder_amount: int(&w, "responder_amount_final"),
            },
            K::ChannelCloseSolo | K::ChannelSlash => {
                let channel = c()?;
                let from = from()?;
                let payload = payload();
                let proof = Proofs::deserialize(&binary(&w, "poi"), config.limits)?;
                if w.kind() == K::ChannelCloseSolo {
                    Operation::CloseSolo {
                        channel,
                        from,
                        payload,
                        proof,
                    }
                } else {
                    Operation::Slash {
                        channel,
                        from,
                        payload,
                        proof,
                    }
                }
            }
            K::ChannelSnapshotSolo => Operation::SnapshotSolo {
                channel: c()?,
                from: from()?,
                payload: payload(),
            },
            K::ChannelSetDelegates => Operation::SetDelegates {
                channel: c()?,
                from: from()?,
                initiator_delegates: ids(&w, "initiator_delegate_ids"),
                responder_delegates: ids(&w, "responder_delegate_ids"),
                payload: payload(),
                state_hash: binary(&w, "state_hash"),
                round: int(&w, "round"),
            },
            K::ChannelForceProgress => Operation::ForceProgress {
                channel: c()?,
                from: from()?,
                payload: payload(),
                round: int(&w, "round"),
                update: Update::decode_with_limits(&binary(&w, "update"), limits)?,
                state_hash: binary(&w, "state_hash"),
                trees: OffchainTrees::decode(&binary(&w, "offchain_trees"), config)?,
            },
            K::ChannelOffchain => Operation::Offchain {
                channel: c()?,
                updates: if w.version() == 1 {
                    let Some(Value::List(v)) = w.field("updates") else {
                        unreachable!()
                    };
                    Some(
                        v.iter()
                            .map(|v| match v {
                                Value::Binary(b) => b.clone(),
                                _ => unreachable!(),
                            })
                            .collect(),
                    )
                } else {
                    None
                },
                state_hash: binary(&w, "state_hash"),
            },
            _ => return Err(Error::WrongTransactionKind),
        };
        let (nonce, fee, ttl) = if w.kind() == K::ChannelOffchain {
            (int(&w, "round"), 0.into(), 0.into())
        } else {
            (int(&w, "nonce"), int(&w, "fee"), int(&w, "ttl"))
        };
        let mut out = Self::new(operation, nonce, fee, ttl, config.limits)?;
        out.wire_size = bytes.len();
        Ok(out)
    }
    pub(super) fn wire(&self, limits: StateLimits) -> Result<WireTransaction, Error> {
        use Value as V;
        let a = |k: AccountPublicKey| V::Id(Id::new(IdTag::Account, k.0));
        let c = |k: [u8; 32]| V::Id(Id::new(IdTag::Channel, k));
        let n = |v: &Unsigned| V::Integer(v.clone());
        let b = |v: &[u8]| V::Binary(v.to_vec());
        let idlist = |v: &[Id]| V::List(v.iter().copied().map(V::Id).collect());
        let aks = |v: &[AccountPublicKey]| V::List(v.iter().copied().map(a).collect());
        let mut version = 1;
        let mut fields = match &self.operation {
            Operation::Create {
                initiator,
                initiator_amount,
                responder,
                responder_amount,
                reserve,
                lock_period,
                delegates,
                state_hash,
            } => {
                let mut f = vec![
                    ("initiator_id", a(*initiator)),
                    ("initiator_amount", n(initiator_amount)),
                    ("responder_id", a(*responder)),
                    ("responder_amount", n(responder_amount)),
                    ("channel_reserve", n(reserve)),
                    ("lock_period", n(lock_period)),
                    ("state_hash", b(state_hash)),
                ];
                match delegates {
                    CreateDelegates::Shared(v) => f.push(("delegate_ids", aks(v))),
                    CreateDelegates::Separate {
                        initiator,
                        responder,
                    } => {
                        version = 2;
                        f.extend([
                            ("initiator_delegate_ids", aks(initiator)),
                            ("responder_delegate_ids", aks(responder)),
                        ]);
                    }
                }
                f
            }
            Operation::Deposit {
                channel,
                from,
                amount,
                state_hash,
                round,
            } => vec![
                ("channel_id", c(*channel)),
                ("from_id", a(*from)),
                ("amount", n(amount)),
                ("state_hash", b(state_hash)),
                ("round", n(round)),
            ],
            Operation::Withdraw {
                channel,
                to,
                amount,
                state_hash,
                round,
            } => vec![
                ("channel_id", c(*channel)),
                ("to_id", a(*to)),
                ("amount", n(amount)),
                ("state_hash", b(state_hash)),
                ("round", n(round)),
            ],
            Operation::CloseMutual {
                channel,
                from,
                initiator_amount,
                responder_amount,
            } => vec![
                ("channel_id", c(*channel)),
                ("from_id", V::Id(*from)),
                ("initiator_amount_final", n(initiator_amount)),
                ("responder_amount_final", n(responder_amount)),
            ],
            Operation::Settle {
                channel,
                from,
                initiator_amount,
                responder_amount,
            } => vec![
                ("channel_id", c(*channel)),
                ("from_id", a(*from)),
                ("initiator_amount_final", n(initiator_amount)),
                ("responder_amount_final", n(responder_amount)),
            ],
            Operation::CloseSolo {
                channel,
                from,
                payload,
                proof,
            }
            | Operation::Slash {
                channel,
                from,
                payload,
                proof,
            } => vec![
                ("channel_id", c(*channel)),
                ("from_id", a(*from)),
                ("payload", b(payload)),
                ("poi", V::Binary(proof.serialize(limits)?)),
            ],
            Operation::SnapshotSolo {
                channel,
                from,
                payload,
            } => vec![
                ("channel_id", c(*channel)),
                ("from_id", a(*from)),
                ("payload", b(payload)),
            ],
            Operation::SetDelegates {
                channel,
                from,
                initiator_delegates,
                responder_delegates,
                payload,
                state_hash,
                round,
            } => vec![
                ("channel_id", c(*channel)),
                ("from_id", a(*from)),
                ("initiator_delegate_ids", idlist(initiator_delegates)),
                ("responder_delegate_ids", idlist(responder_delegates)),
                ("payload", b(payload)),
                ("state_hash", b(state_hash)),
                ("round", n(round)),
            ],
            Operation::ForceProgress {
                channel,
                from,
                payload,
                round,
                update,
                state_hash,
                trees,
            } => vec![
                ("channel_id", c(*channel)),
                ("from_id", a(*from)),
                ("payload", b(payload)),
                ("round", n(round)),
                ("update", V::Binary(update.encode())),
                ("state_hash", b(state_hash)),
                ("offchain_trees", b(trees.bytes())),
            ],
            Operation::Offchain {
                channel,
                updates,
                state_hash,
            } => {
                let mut f = vec![
                    ("channel_id", c(*channel)),
                    ("round", n(&self.nonce)),
                    ("state_hash", b(state_hash)),
                ];
                if let Some(u) = updates {
                    f.push((
                        "updates",
                        V::List(u.iter().cloned().map(V::Binary).collect()),
                    ));
                } else {
                    version = 2;
                }
                f
            }
        };
        if !matches!(self.operation, Operation::Offchain { .. }) {
            fields.extend([
                ("nonce", n(&self.nonce)),
                ("fee", n(&self.fee)),
                ("ttl", n(&self.ttl)),
            ]);
        }
        let schema = self.kind().schema(version).expect("typed channel schema");
        let fields = schema
            .iter()
            .map(|f| {
                let i = fields
                    .iter()
                    .position(|(n, _)| *n == f.name)
                    .expect("typed channel field");
                let (_, v) = fields.swap_remove(i);
                (f.name.clone(), v)
            })
            .collect();
        WireTransaction::new(self.kind(), version, fields)
    }
}
