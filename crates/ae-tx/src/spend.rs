use crate::{CanonicalTransaction, Error, TransactionKind, WireTransaction};
use ae_codec::object::Value;
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spend {
    sender: AccountPublicKey,
    recipient: Id,
    pub amount: Unsigned,
    pub fee: Unsigned,
    pub ttl: Unsigned,
    pub nonce: Unsigned,
    pub payload: Vec<u8>,
}
impl Spend {
    pub fn new(sender: AccountPublicKey, recipient: Id) -> Result<Self, Error> {
        if !matches!(
            recipient.tag,
            IdTag::Account | IdTag::Name | IdTag::Oracle | IdTag::Contract
        ) {
            return Err(Error::InvalidIdType);
        }
        Ok(Self {
            sender,
            recipient,
            amount: 0.into(),
            fee: 0.into(),
            ttl: 0.into(),
            nonce: 0.into(),
            payload: vec![],
        })
    }
    pub fn sender(&self) -> AccountPublicKey {
        self.sender
    }
    pub fn recipient(&self) -> Id {
        self.recipient
    }
}
impl CanonicalTransaction for Spend {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let wire = WireTransaction::decode(bytes)?;
        if wire.kind() != TransactionKind::Spend {
            return Err(Error::WrongTransactionKind);
        }
        let [
            (_, Value::Id(sender)),
            (_, Value::Id(recipient)),
            (_, Value::Integer(amount)),
            (_, Value::Integer(fee)),
            (_, Value::Integer(ttl)),
            (_, Value::Integer(nonce)),
            (_, Value::Binary(payload)),
        ] = wire.fields()
        else {
            unreachable!("validated spend schema")
        };
        if sender.tag != IdTag::Account {
            return Err(Error::InvalidIdType);
        }
        let mut tx = Self::new(AccountPublicKey(sender.value), *recipient)?;
        tx.amount = amount.clone();
        tx.fee = fee.clone();
        tx.ttl = ttl.clone();
        tx.nonce = nonce.clone();
        tx.payload = payload.clone();
        Ok(tx)
    }
    fn encode(&self) -> Vec<u8> {
        WireTransaction::new(
            TransactionKind::Spend,
            1,
            vec![
                (
                    "sender_id".into(),
                    Value::Id(Id::new(IdTag::Account, self.sender.0)),
                ),
                ("recipient_id".into(), Value::Id(self.recipient)),
                ("amount".into(), Value::Integer(self.amount.clone())),
                ("fee".into(), Value::Integer(self.fee.clone())),
                ("ttl".into(), Value::Integer(self.ttl.clone())),
                ("nonce".into(), Value::Integer(self.nonce.clone())),
                ("payload".into(), Value::Binary(self.payload.clone())),
            ],
        )
        .expect("typed spend fields")
        .encode()
    }
}
