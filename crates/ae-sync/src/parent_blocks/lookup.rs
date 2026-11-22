use super::{Chain, Config, Node};
use crate::parent_pins::{Error, transport::Transport};
use ae_hyperchain::{
    parent_block::ParentBlock,
    parent_connector::fetch::{BackendError, Query, Remote},
};
use ae_types::Unsigned;
use base64::Engine;
use num_bigint::BigInt;
use serde_json::{Value, json};

enum Failure {
    NotFound,
    Other,
    Operational(BackendError),
}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Operational(match e {
            Error::ResourceLimit | Error::Busy => BackendError::ResourceLimit,
            _ => BackendError::Unavailable,
        })
    }
}
fn indirect(e: Failure) -> Failure {
    match e {
        Failure::NotFound => Failure::Other,
        e => e,
    }
}
fn integer(v: Option<&Value>, c: &Config) -> Result<BigInt, Failure> {
    let n = v
        .and_then(Value::as_number)
        .ok_or(Failure::Other)?
        .to_string();
    if n.len() > c.maximum_number_digits {
        return Err(Failure::Operational(BackendError::ResourceLimit));
    }
    if n.bytes().any(|b| matches!(b, b'.' | b'e' | b'E')) {
        return Err(Failure::Other);
    }
    n.parse().map_err(|_| Failure::Other)
}
fn unsigned(n: &BigInt) -> Result<Unsigned, Failure> {
    Unsigned::from_decimal(&n.to_string()).map_err(|_| Failure::Other)
}
fn hash(v: Option<&Value>, c: &Config) -> Result<Vec<u8>, Failure> {
    let s = v.and_then(Value::as_str).ok_or(Failure::Other)?;
    if s.len() > c.maximum_hash_bytes {
        return Err(Failure::Operational(BackendError::ResourceLimit));
    }
    Ok(s.as_bytes().to_vec())
}
fn json_value(bytes: &[u8]) -> Result<Value, Failure> {
    serde_json::from_slice(bytes).map_err(|e| {
        if e.to_string().starts_with("recursion limit exceeded") {
            Failure::Operational(BackendError::ResourceLimit)
        } else {
            Failure::Other
        }
    })
}
async fn rpc(
    t: &Transport,
    n: &Node,
    seed: &[u8; 16],
    method: &str,
    params: Vec<Value>,
) -> Result<Value, Failure> {
    // Both seed_to_utf8 and request_body apply Base64 in the native adapters.
    let id = base64::engine::general_purpose::STANDARD
        .encode(base64::engine::general_purpose::STANDARD.encode(seed));
    let body =
        serde_json::to_vec(&json!({"jsonrpc":"2.0","method":method,"params":params,"id":id}))
            .map_err(|_| Failure::Operational(BackendError::ResourceLimit))?;
    let reply = t
        .response(n, "/", Some(&body))
        .await?
        .ok_or(Failure::Other)?;
    if reply.status == 500 && (reply.body.starts_with(b"{\"result\":null,\"error\":{\"code\":-8,\"message\":\"Block height out of range\"}")
        || n.chain == Chain::Dogecoin && reply.body.starts_with(b"{\"result\":null,\"error\":{\"code\":-5,\"message\":\"No such mempool or blockchain")) {
        return Err(Failure::NotFound);
    }
    if reply.status != 200 {
        return Err(Failure::Other);
    }
    json_value(&reply.body)?
        .as_object_mut()
        .and_then(|m| m.remove("result"))
        .ok_or(Failure::Other)
}
async fn read(
    t: &Transport,
    n: &Node,
    c: &Config,
    q: &Query,
    seed: &[u8; 16],
) -> Result<ParentBlock, Failure> {
    match q {
        Query::Hash(h) if h.len() > c.maximum_hash_bytes => {
            return Err(Failure::Operational(BackendError::ResourceLimit));
        }
        Query::Height(h) if h.bits() > (c.maximum_number_digits as u64).saturating_mul(4) => {
            return Err(Failure::Operational(BackendError::ResourceLimit));
        }
        _ => (),
    }
    let height_text = if let Query::Height(h) = q {
        let text = h.to_string();
        if text.len() > c.maximum_number_digits {
            return Err(Failure::Operational(BackendError::ResourceLimit));
        }
        Some(text)
    } else {
        None
    };
    if n.chain == Chain::Aeternity {
        let path = match q {
            Query::Top => "/v3/key-blocks/current".to_owned(),
            Query::Height(_) => format!(
                "/v3/key-blocks/height/{}",
                height_text.as_ref().expect("height")
            ),
            Query::Hash(h) => format!(
                "/v3/key-blocks/hash/{}",
                std::str::from_utf8(h).map_err(|_| Failure::Other)?
            ),
        };
        let reply = t.response(n, &path, None).await?.ok_or(Failure::NotFound)?;
        if reply.status >= 400 {
            return Err(Failure::NotFound);
        }
        if reply.status != 200 {
            return Err(Failure::Other);
        }
        let v = json_value(&reply.body)?;
        let h = hash(v.get("hash"), c)?;
        let height = integer(v.get("height"), c)?;
        if matches!(q, Query::Hash(expected) if &h != expected)
            || matches!(q, Query::Height(expected) if &height != expected)
        {
            return Err(Failure::Other);
        }
        let previous_hash = match v.get("prev_key_hash") {
            Some(Value::Null) => None,
            v => Some(hash(v, c)?),
        };
        return Ok(ParentBlock {
            hash: h,
            height: unsigned(&height)?,
            previous_hash,
            time: unsigned(&integer(v.get("time"), c)?)?,
        });
    }
    let h = match q {
        Query::Top => hash(
            Some(
                &rpc(t, n, seed, "getbestblockhash", vec![])
                    .await
                    .map_err(indirect)?,
            ),
            c,
        )?,
        Query::Height(_) => hash(
            Some(
                &rpc(
                    t,
                    n,
                    seed,
                    "getblockhash",
                    vec![json_value(
                        height_text.as_ref().expect("height").as_bytes(),
                    )?],
                )
                .await?,
            ),
            c,
        )?,
        Query::Hash(h) => h.clone(),
    };
    let hstr = std::str::from_utf8(&h).map_err(|_| Failure::Other)?;
    let verbosity = if n.chain == Chain::Bitcoin {
        json!(1)
    } else {
        json!(true)
    };
    let v = rpc(t, n, seed, "getblock", vec![json!(hstr), verbosity])
        .await
        .map_err(indirect)?;
    if hash(v.get("hash"), c)? != h || v.get("tx").is_none() {
        return Err(Failure::Other);
    }
    let returned_height = integer(v.get("height"), c)?;
    // Native height lookups deliberately return the requested height even if
    // the getblock height differs. Hash and top lookups use the returned height.
    let height = unsigned(if let Query::Height(requested) = q {
        requested
    } else {
        &returned_height
    })?;
    let previous_hash = match v.get("previousblockhash") {
        value @ Some(Value::String(_)) => Some(hash(value, c)?),
        _ => None,
    };
    Ok(ParentBlock {
        hash: h,
        height,
        previous_hash,
        time: unsigned(&integer(v.get("time"), c)?)?.mul_u64(1000),
    })
}
pub(super) async fn fetch(
    t: &Transport,
    n: &Node,
    c: &Config,
    q: &Query,
    seed: &[u8; 16],
) -> Result<Remote, BackendError> {
    match read(t, n, c, q, seed).await {
        Ok(b) => Ok(Remote::Block(b)),
        Err(Failure::NotFound) => Ok(Remote::NotFound),
        Err(Failure::Other) => Ok(Remote::OtherError),
        Err(Failure::Operational(e)) => Err(e),
    }
}
