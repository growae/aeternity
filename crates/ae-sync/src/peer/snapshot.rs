use ae_chain::{
    block::Block,
    repository::{self, Read, View},
};
use ae_p2p::{
    message::{self, Kind, Message, Value},
    ping,
};
use ae_storage::pool::Stored;
use ae_types::Hash256;
use std::collections::BTreeMap;

#[derive(Debug)]
pub enum Error {
    Storage(repository::Error),
    Message(message::Error),
    Header,
    Uninitialized,
}
impl From<repository::Error> for Error {
    fn from(e: repository::Error) -> Self {
        Self::Storage(e)
    }
}
impl From<message::Error> for Error {
    fn from(e: message::Error) -> Self {
        Self::Message(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

pub fn local<S: Read>(view: &View<S>, temporary: bool) -> Result<ping::Local, Error> {
    let top = view.status().tip.ok_or(Error::Uninitialized)?;
    let block = view.require_block(top)?;
    let header = block.header();
    let best = match block {
        Block::Key(_) => top,
        Block::Micro(_) => Hash256(header.common().previous_key_hash),
    };
    Ok(ping::Local {
        genesis: view.identity().genesis,
        best,
        height: header.common().height,
        difficulty: view.require_record(best)?.difficulty,
        temporary,
    })
}
fn hash(value: &Value) -> Option<Hash256> {
    let Value::Binary(b) = value else { return None };
    b.as_slice().try_into().ok().map(Hash256)
}
fn integer(value: &Value) -> Option<u64> {
    let Value::Integer(n) = value else {
        return None;
    };
    n.to_u64().ok()
}
fn refusal(kind: Kind, reason: &[u8], limits: message::Limits) -> Result<Message, Error> {
    Ok(Message::response_error(kind, reason.to_vec(), limits)?)
}
fn answer(
    kind: Kind,
    fields: BTreeMap<String, Value>,
    limits: message::Limits,
) -> Result<Message, Error> {
    Ok(Message::response_ok(
        &Message::new(kind, 1, fields, limits)?,
        limits,
    )?)
}
// A single captured chain/pool snapshot serves the whole request. Operational
// failures propagate; they are never disguised as an ordinary missing header/tx.
pub fn serve<S: Read>(
    view: &Stored<S>,
    request: &Message,
    limits: message::Limits,
) -> Result<Option<Message>, Error> {
    let chain = view.chain();
    let fields = request.fields();
    match request.kind() {
        Kind::GetHeaderByHash | Kind::GetHeaderByHeight => {
            let key = if request.kind() == Kind::GetHeaderByHash {
                hash(&fields["hash"])
            } else {
                match integer(&fields["height"]) {
                    Some(n) => chain.canonical_key(n)?,
                    None => None,
                }
            };
            let block = key.map(|h| chain.block(h)).transpose()?.flatten();
            if request.kind() == Kind::GetHeaderByHeight && request.version() == 2 {
                let on_chain = match hash(&fields["top_hash"]) {
                    Some(h) => chain.is_canonical(h)?,
                    None => false,
                };
                if !on_chain {
                    return refusal(Kind::Header, b"not_on_chain", limits).map(Some);
                }
            }
            let Some(block) = block else {
                return refusal(Kind::Header, b"header_not_found", limits).map(Some);
            };
            let bytes = block.header().encode().map_err(|_| Error::Header)?;
            answer(
                Kind::Header,
                BTreeMap::from([("hdr".into(), Value::Binary(bytes))]),
                limits,
            )
            .map(Some)
        }
        Kind::GetBlockTransactions => {
            let Value::List(hashes) = &fields["tx_hashes"] else {
                unreachable!("decoded schema")
            };
            let mut transactions = Vec::new();
            let mut remaining = limits.body_bytes.min(limits.work_bytes);
            for value in hashes {
                let tx = hash(value)
                    .map(|h| view.transaction(h))
                    .transpose()?
                    .flatten();
                let Some(tx) = tx else {
                    return refusal(Kind::BlockTransactions, b"tx_missing", limits).map(Some);
                };
                let bytes = tx.encode();
                remaining = remaining
                    .checked_sub(bytes.len())
                    .ok_or(message::Error::ResourceLimit)?;
                transactions.push(Value::Binary(bytes));
            }
            answer(
                Kind::BlockTransactions,
                BTreeMap::from([
                    ("hash".into(), fields["hash"].clone()),
                    ("txs".into(), Value::List(transactions)),
                ]),
                limits,
            )
            .map(Some)
        }
        _ => Ok(None),
    }
}
