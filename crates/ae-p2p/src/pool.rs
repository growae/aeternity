use crate::message::{Details, Error, Kind, Limits, Message, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Init,
    Unfold(Vec<Vec<u8>>),
    Get(Vec<Vec<u8>>),
    Finish(bool),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Init,
    Unfold(Vec<Vec<u8>>),
    Transactions(Vec<Vec<u8>>),
    Finished(bool),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseError {
    UnexpectedType,
    Peer(Vec<u8>),
    Protocol(Error),
}
impl Request {
    pub fn expected_response(&self) -> Kind {
        match self {
            Self::Init => Kind::PoolInit,
            Self::Unfold(_) => Kind::PoolUnfold,
            Self::Get(_) => Kind::Transactions,
            Self::Finish(_) => Kind::PoolFinish,
        }
    }
    pub fn into_message(self, limits: Limits) -> Result<Message, Error> {
        let (kind, fields) = match self {
            Self::Init => (Kind::PoolInit, BTreeMap::new()),
            Self::Unfold(v) => (Kind::PoolUnfold, binaries("unfolds", v, limits)?),
            Self::Get(v) => (Kind::PoolGet, binaries("tx_hashes", v, limits)?),
            Self::Finish(done) => (
                Kind::PoolFinish,
                BTreeMap::from([("done".into(), Value::Boolean(done))]),
            ),
        };
        Message::new(kind, 1, fields, limits)
    }
    pub fn from_message(message: &Message) -> Result<Self, Error> {
        match message.kind() {
            Kind::PoolInit => Ok(Self::Init),
            Kind::PoolUnfold => Ok(Self::Unfold(read_binaries(message, "unfolds")?)),
            Kind::PoolGet => Ok(Self::Get(read_binaries(message, "tx_hashes")?)),
            Kind::PoolFinish => match message.fields().get("done") {
                Some(Value::Boolean(done)) => Ok(Self::Finish(*done)),
                _ => Err(Error::InvalidMessage),
            },
            _ => Err(Error::InvalidMessage),
        }
    }
    pub fn response(&self, envelope: &Message) -> Result<Reply, ResponseError> {
        let Details::Response { kind, message } = envelope.details() else {
            return Err(ResponseError::UnexpectedType);
        };
        if *kind != self.expected_response() {
            return Err(ResponseError::UnexpectedType);
        }
        let body = match message {
            Some(Ok(message)) => message,
            Some(Err(error)) => return Err(ResponseError::Protocol(error.clone())),
            None => {
                let Some(Value::Binary(reason)) = envelope.fields().get("reason") else {
                    return Err(ResponseError::Protocol(Error::InvalidMessage));
                };
                return Err(ResponseError::Peer(reason.clone()));
            }
        };
        match self {
            Self::Init => Ok(Reply::Init),
            Self::Unfold(_) => Ok(Reply::Unfold(
                read_binaries(body, "unfolds").map_err(ResponseError::Protocol)?,
            )),
            Self::Get(_) => Ok(Reply::Transactions(
                read_binaries(body, "txs").map_err(ResponseError::Protocol)?,
            )),
            Self::Finish(_) => match body.fields().get("done") {
                Some(Value::Boolean(done)) => Ok(Reply::Finished(*done)),
                _ => Err(ResponseError::Protocol(Error::InvalidMessage)),
            },
        }
    }
}
impl Reply {
    pub fn into_response(self, limits: Limits) -> Result<Message, Error> {
        let body = match self {
            Self::Init => Request::Init.into_message(limits)?,
            Self::Unfold(v) => Request::Unfold(v).into_message(limits)?,
            Self::Transactions(v) => {
                Message::new(Kind::Transactions, 1, binaries("txs", v, limits)?, limits)?
            }
            Self::Finished(done) => Request::Finish(done).into_message(limits)?,
        };
        Message::response_ok(&body, limits)
    }
}
fn binaries(name: &str, v: Vec<Vec<u8>>, limits: Limits) -> Result<BTreeMap<String, Value>, Error> {
    if v.len() > limits.items {
        return Err(Error::ResourceLimit);
    }
    let mut total = 0usize;
    for b in &v {
        total = total.checked_add(b.len()).ok_or(Error::ResourceLimit)?;
        if total > limits.work_bytes {
            return Err(Error::ResourceLimit);
        }
    }
    Ok(BTreeMap::from([(
        name.into(),
        Value::List(v.into_iter().map(Value::Binary).collect()),
    )]))
}
fn read_binaries(message: &Message, name: &str) -> Result<Vec<Vec<u8>>, Error> {
    let Some(Value::List(v)) = message.fields().get(name) else {
        return Err(Error::InvalidMessage);
    };
    v.iter()
        .map(|v| {
            if let Value::Binary(b) = v {
                Ok(b.clone())
            } else {
                Err(Error::InvalidMessage)
            }
        })
        .collect()
}
