use super::{Chain, Error, Node, transport::Transport};
use ae_codec::api::{self, ApiType};
use ae_hyperchain::{block::Pin, pin};
use num_bigint::BigInt;
use serde_json::{Value, json};
fn integer(value: Option<&Value>, maximum: usize) -> Result<Option<BigInt>, Error> {
    let Some(number) = value.and_then(Value::as_number) else {
        return Ok(None);
    };
    let text = number.to_string();
    if text.len() > maximum {
        return Err(Error::ResourceLimit);
    }
    if text.bytes().any(|b| matches!(b, b'.' | b'e' | b'E')) {
        return Ok(None);
    }
    Ok(text.parse().ok())
}
fn decode(bytes: &[u8], maximum: usize, parent_height: BigInt) -> Result<Option<Pin>, Error> {
    match pin::decode_parent(bytes, maximum) {
        Ok(pin) => Ok(Some(Pin {
            epoch: pin.epoch,
            child_height: pin.height,
            child_hash: pin.block_hash,
            parent_height,
        })),
        Err(pin::Error::ResourceLimit) => Err(Error::ResourceLimit),
        Err(_) => Ok(None),
    }
}
async fn rpc(
    t: &Transport,
    n: &Node,
    method: &str,
    params: Vec<Value>,
) -> Result<Option<Value>, Error> {
    // seed_to_utf8 and request_body both encode the empty seed, yielding "".
    let body =
        serde_json::to_vec(&json!({"jsonrpc":"2.0","method":method,"params":params,"id":""}))
            .map_err(|_| Error::ResourceLimit)?;
    Ok(t.request(n, "/", Some(&body))
        .await?
        .and_then(|mut value| value.as_object_mut()?.remove("result")))
}
pub(super) async fn fetch(t: &Transport, n: &Node, hash: &[u8]) -> Result<Option<Pin>, Error> {
    if n.chain == Chain::Bitcoin {
        return Ok(None);
    } // native raises not_implemented
    let Ok(hash) = std::str::from_utf8(hash) else {
        return Ok(None);
    };
    if n.chain == Chain::Aeternity {
        let Some(value) = t
            .request(n, &format!("/v3/transactions/{hash}"), None)
            .await?
        else {
            return Ok(None);
        };
        let (Some(payload), Some(height)) = (
            value
                .get("tx")
                .and_then(|tx| tx.get("payload"))
                .and_then(Value::as_str),
            integer(value.get("block_height"), t.config.maximum_number_digits)?,
        ) else {
            return Ok(None);
        };
        let Ok((ApiType::Bytearray, bytes)) =
            api::decode_with_limit(payload, t.config.maximum_body_bytes)
        else {
            return Ok(None);
        };
        return decode(&bytes, t.config.maximum_payload_bytes, height);
    }
    let Some(tx) = rpc(t, n, "gettransaction", vec![json!(hash), json!(true)]).await? else {
        return Ok(None);
    };
    let (Some(raw), Some(conf)) = (tx.get("hex"), tx.get("confirmations")) else {
        return Ok(None);
    };
    let Some(decoded) = rpc(t, n, "decoderawtransaction", vec![raw.clone()]).await? else {
        return Ok(None);
    };
    // Native find_pin matches only the first output, not any OP_RETURN output.
    let Some(script) = decoded
        .get("vout")
        .and_then(Value::as_array)
        .and_then(|v| v.first())
        .and_then(|v| v.get("scriptPubKey"))
    else {
        return Ok(None);
    };
    if script.get("type").and_then(Value::as_str) != Some("nulldata") {
        return Ok(None);
    }
    let Some(hex) = script
        .get("asm")
        .and_then(Value::as_str)
        .and_then(|s| s.strip_prefix("OP_RETURN "))
    else {
        return Ok(None);
    };
    if hex.len() > t.config.maximum_payload_bytes.saturating_mul(2) {
        return Err(Error::ResourceLimit);
    }
    if !hex.len().is_multiple_of(2) || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(None);
    }
    let bytes: Vec<_> = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|p| {
            let n = |b: u8| {
                if b.is_ascii_digit() {
                    b - b'0'
                } else {
                    b.to_ascii_lowercase() - b'a' + 10
                }
            };
            n(p[0]) * 16 + n(p[1])
        })
        .collect();
    // Decode before fetching parent height, preserving the original call order.
    let Some(mut pin) = decode(&bytes, t.config.maximum_payload_bytes, (-1).into())? else {
        return Ok(None);
    };
    if integer(Some(conf), t.config.maximum_number_digits)? == Some(0.into()) {
        return Ok(Some(pin));
    }
    let Some(hash) = tx.get("blockhash") else {
        return Ok(None);
    };
    let Some(block) = rpc(t, n, "getblock", vec![hash.clone(), json!(true)]).await? else {
        return Ok(None);
    };
    let Some(txs) = block.get("tx").and_then(Value::as_array) else {
        return Ok(None);
    };
    if txs.len() > t.config.maximum_block_transactions {
        return Err(Error::ResourceLimit);
    }
    // Dogecoin getblock(...,2) fetches each transaction, discarding native errors.
    // Local exhaustion/cancellation must still abort the entire capture.
    for hash in txs {
        let _ = rpc(t, n, "getrawtransaction", vec![hash.clone(), json!(true)]).await?;
    }
    let (Some(_), Some(height), Some(_)) = (
        block.get("hash").and_then(Value::as_str),
        integer(block.get("height"), t.config.maximum_number_digits)?,
        integer(block.get("time"), t.config.maximum_number_digits)?,
    ) else {
        return Ok(None);
    };
    pin.parent_height = height;
    Ok(Some(pin))
}
