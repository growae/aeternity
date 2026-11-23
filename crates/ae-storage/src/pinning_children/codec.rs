use super::*;
use ae_tx::{CanonicalTransaction, ContractOperation, TransactionLeaf};
use ae_types::{Id, IdTag};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Note,
    Proof([u8; 32]),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    pub network: Vec<u8>,
    pub parent_chain: Chain,
    pub parent_network: Vec<u8>,
    pub leader: AccountPublicKey,
    pub kind: Kind,
    pub parent_hash: Vec<u8>,
}
impl Intent {
    fn fields(&self) -> Result<Vec<Value>, Error> {
        if self.network.len() > 4096
            || self.parent_network.len() > 4096
            || self.parent_hash.len() > 65536
        {
            return Err(Error::ResourceLimit);
        }
        Ok(vec![
            bytes(&self.network),
            number(match self.parent_chain {
                Chain::Aeternity => 0,
                Chain::Dogecoin => 1,
                Chain::Bitcoin => 2,
            }),
            bytes(&self.parent_network),
            bytes(self.leader.0),
            bytes(match &self.kind {
                Kind::Note => &[][..],
                Kind::Proof(c) => c.as_slice(),
            }),
            bytes(&self.parent_hash),
        ])
    }
    // Fee, nonce, gas-price policy and the current selected top do not change
    // the identity of an already attempted note/proof.
    pub fn id(&self) -> Result<Hash256, Error> {
        let mut fields = vec![bytes(b"ae-child-pin-v1")];
        fields.extend(self.fields()?);
        Ok(blake2b_256(&Value::List(fields).encode()))
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Receipt {
    Submitted(pool::Submission),
    // Diagnostic text of the original non-operational policy error. Recovery
    // does not reconstruct a typed error from this text or retry the request.
    Rejected(String),
    Observed {
        location: Location,
        chain: Status,
        revision: u64,
    },
}
impl Receipt {
    fn encode(&self) -> Result<Vec<u8>, Error> {
        use ae_mempool::pool::{Event, PushOutcome};
        let mut fields = vec![number(1)];
        fields.extend(match self {
            Self::Submitted(s) => {
                Status::decode(&s.chain.encode())?;
                let (kind, hash, inserted) = match s.outcome {
                    PushOutcome::Cached => (0, vec![], false),
                    PushOutcome::Accepted {
                        hash,
                        inserted,
                        event,
                    } => (
                        if event == Event::Created { 1 } else { 2 },
                        hash.0.to_vec(),
                        inserted,
                    ),
                };
                vec![
                    number(0),
                    bytes(s.chain.encode()),
                    number(s.revision),
                    number(kind),
                    bytes(hash),
                    number(u64::from(inserted)),
                ]
            }
            Self::Rejected(reason) if !reason.is_empty() && reason.len() <= 4096 => {
                vec![number(1), bytes(reason)]
            }
            Self::Rejected(_) => return Err(Error::ResourceLimit),
            Self::Observed {
                location,
                chain,
                revision,
            } => {
                Status::decode(&chain.encode())?;
                let (kind, hash) = match location {
                    Location::Unknown => return Err(Error::Conflict),
                    Location::Deleted => (0, vec![]),
                    Location::Pool => (1, vec![]),
                    Location::Block(h) => (2, h.0.to_vec()),
                };
                vec![
                    number(2),
                    bytes(chain.encode()),
                    number(*revision),
                    number(kind),
                    bytes(hash),
                ]
            }
        });
        Ok(Value::List(fields).encode())
    }
    fn decode(raw: &[u8]) -> Result<Self, Error> {
        use ae_mempool::pool::{Event, PushOutcome};
        let Value::List(v) = flat(raw, 4352, 8)? else {
            return Err(Error::Corrupt("child pin receipt"));
        };
        if v.first().map(integer).transpose()? != Some(1) {
            return Err(Error::Corrupt("child pin receipt version"));
        }
        let r = match v.as_slice() {
            [_, tag, chain, revision, kind, hash, inserted] if integer(tag)? == 0 => {
                let b = integer(inserted)?;
                if b > 1 {
                    return Err(Error::Corrupt("child pin inserted"));
                }
                let outcome = match integer(kind)? {
                    0 if blob(hash)?.is_empty() && b == 0 => PushOutcome::Cached,
                    n @ (1 | 2) => PushOutcome::Accepted {
                        hash: Hash256(array(blob(hash)?)?),
                        inserted: b == 1,
                        event: if n == 1 {
                            Event::Created
                        } else {
                            Event::Received
                        },
                    },
                    _ => return Err(Error::Corrupt("child pin submission")),
                };
                Self::Submitted(pool::Submission {
                    outcome,
                    chain: Status::decode(blob(chain)?)?,
                    revision: integer(revision)?,
                })
            }
            [_, tag, reason] if integer(tag)? == 1 => Self::Rejected(
                String::from_utf8(blob(reason)?.to_vec())
                    .map_err(|_| Error::Corrupt("child pin rejection"))?,
            ),
            [_, tag, chain, revision, kind, hash] if integer(tag)? == 2 => {
                let location = match integer(kind)? {
                    0 if blob(hash)?.is_empty() => Location::Deleted,
                    1 if blob(hash)?.is_empty() => Location::Pool,
                    2 => Location::Block(Hash256(array(blob(hash)?)?)),
                    _ => return Err(Error::Corrupt("child pin location")),
                };
                Self::Observed {
                    location,
                    chain: Status::decode(blob(chain)?)?,
                    revision: integer(revision)?,
                }
            }
            _ => return Err(Error::Corrupt("child pin receipt fields")),
        };
        if r.encode()? != raw {
            return Err(Error::Corrupt("child pin receipt encoding"));
        }
        Ok(r)
    }
}
fn array(b: &[u8]) -> Result<[u8; 32], Error> {
    b.try_into().map_err(|_| Error::Corrupt("child pin key"))
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub intent: Intent,
    pub transaction: Vec<u8>,
    pub receipt: Option<Receipt>,
}
impl Record {
    pub fn signed(&self) -> Result<Signed, Error> {
        self.intent.fields()?;
        if self.transaction.is_empty() || self.transaction.len() > MAX_TRANSACTION {
            return Err(Error::ResourceLimit);
        }
        let tx = Signed::decode(&self.transaction)
            .map_err(|_| Error::Corrupt("child pin transaction"))?;
        if tx.encode() != self.transaction
            || !tx.transaction().view().layers().is_empty()
            || tx.signatures().len() != 1
        {
            return Err(Error::Corrupt("child pin canonical transaction"));
        }
        let leader = self.intent.leader;
        let valid = match (self.intent.kind, tx.transaction().view().leaf()) {
            (Kind::Note, TransactionLeaf::Spend(s)) => {
                s.sender() == leader
                    && s.recipient() == Id::new(IdTag::Account, leader.0)
                    && s.amount() == &1.into()
                    && s.ttl() == &0.into()
                    && ae_hyperchain::pin::decode_child(s.payload(), 65539)
                        .is_ok_and(|h| h == self.intent.parent_hash)
            }
            (Kind::Proof(target), TransactionLeaf::Contract(c)) => {
                let data = ae_vm_fate::abi::create_calldata(
                    b"pin",
                    vec![ae_vm_fate::value::Value::Bytes(
                        self.intent.parent_hash.clone(),
                    )],
                    Default::default(),
                )
                .map_err(|_| Error::Corrupt("child pin calldata"))?;
                c.actor() == Id::new(IdTag::Account, leader.0)
                    && c.amount() == &0.into()
                    && c.ttl() == &0.into()
                    && c.gas() == &1_000_000.into()
                    && c.fee() == &c.gas_price().mul_u64(1_000_000)
                    && matches!(c.operation(), ContractOperation::Call { contract, abi_version, call_data } if *contract == Id::new(IdTag::Contract, target) && abi_version == &3.into() && *call_data == data)
            }
            _ => false,
        };
        let mut message = self.intent.network.clone();
        message.extend(tx.transaction().encode());
        if !valid || !ae_crypto::verify_ed25519(&leader, &message, &tx.signatures()[0]) {
            return Err(Error::Corrupt("child pin transaction binding"));
        }
        Ok(tx)
    }
    pub(super) fn encode(&self, limit: usize) -> Result<Vec<u8>, Error> {
        let intent = self.intent.fields()?;
        if self.transaction.len() > limit.min(MAX_TRANSACTION) {
            return Err(Error::ResourceLimit);
        }
        let tx = self.signed()?;
        if let Some(Receipt::Submitted(pool::Submission {
            outcome: ae_mempool::pool::PushOutcome::Accepted { hash, .. },
            ..
        })) = &self.receipt
            && *hash != tx.hash()
        {
            return Err(Error::Conflict);
        }
        let mut fields = vec![number(1)];
        fields.extend(intent);
        fields.push(bytes(&self.transaction));
        fields.push(bytes(
            self.receipt
                .as_ref()
                .map(Receipt::encode)
                .transpose()?
                .unwrap_or_default(),
        ));
        Ok(Value::List(fields).encode())
    }
    pub(super) fn decode(key: &[u8], raw: &[u8], limit: usize) -> Result<Self, Error> {
        let Value::List(v) = flat(raw, MAX_TRANSACTION + 80_000, 10)? else {
            return Err(Error::Corrupt("child pin record"));
        };
        let [
            version,
            network,
            parent_chain,
            parent_network,
            leader,
            kind,
            parent_hash,
            transaction,
            receipt,
        ] = v.as_slice()
        else {
            return Err(Error::Corrupt("child pin fields"));
        };
        if integer(version)? != 1 {
            return Err(Error::Corrupt("child pin version"));
        }
        let kind = blob(kind)?;
        let r = Self {
            intent: Intent {
                network: blob(network)?.to_vec(),
                parent_chain: match integer(parent_chain)? {
                    0 => Chain::Aeternity,
                    1 => Chain::Dogecoin,
                    2 => Chain::Bitcoin,
                    _ => return Err(Error::Corrupt("child pin parent")),
                },
                parent_network: blob(parent_network)?.to_vec(),
                leader: AccountPublicKey(array(blob(leader)?)?),
                kind: if kind.is_empty() {
                    Kind::Note
                } else {
                    Kind::Proof(array(kind)?)
                },
                parent_hash: blob(parent_hash)?.to_vec(),
            },
            transaction: blob(transaction)?.to_vec(),
            receipt: if blob(receipt)?.is_empty() {
                None
            } else {
                Some(Receipt::decode(blob(receipt)?)?)
            },
        };
        if r.encode(MAX_TRANSACTION)? != raw || key != repo::key(RECORDS, r.intent.id()?) {
            return Err(Error::Corrupt("child pin encoding or key"));
        }
        if r.transaction.len() > limit {
            return Err(Error::ResourceLimit);
        }
        Ok(r)
    }
}
