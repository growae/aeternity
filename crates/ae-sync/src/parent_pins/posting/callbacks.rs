use super::{Chain, Failure, Node, Post, Progress, Signing, Submission, Transport};
use ae_codec::api::{self, ApiType};
use ae_tx::{CanonicalTransaction, SignedTransaction, Spend};
use ae_types::{Id, IdTag, Unsigned};
use num_bigint::BigInt;
use serde_json::{Value, json};
use std::sync::Arc;

fn integer(v: &Value, maximum: usize) -> Result<BigInt, Failure> {
    let text = v.as_number().ok_or(Failure::ParentResponse)?.to_string();
    if text.len() > maximum {
        return Err(Failure::Local(super::Error::ResourceLimit));
    }
    text.parse().map_err(|_| Failure::ParentResponse)
}
fn float_satoshis(v: &Value, maximum: usize) -> Result<BigInt, Failure> {
    let text = v.as_number().ok_or(Failure::ParentResponse)?.to_string();
    if !text.bytes().any(|b| matches!(b, b'.' | b'e' | b'E')) {
        return integer(v, maximum);
    }
    if text.len() > maximum {
        return Err(Failure::Local(super::Error::ResourceLimit));
    }
    let value = v
        .as_f64()
        .filter(|n| n.is_finite())
        .ok_or(Failure::ParentResponse)?;
    // OTP 26 sys_double_to_chars_fast uses a separately rounded fractional
    // product up to 2^53, not a correctly rounded whole-number formatter.
    let absolute = value.abs();
    let rounded = if absolute <= 9_007_199_254_740_992.0 {
        let mut whole = absolute.floor() as u64;
        let mut fraction = ((absolute - absolute.floor()) * 100_000_000.0).round() as u64;
        if fraction >= 100_000_000 {
            whole += 1;
            fraction = 0;
        }
        format!(
            "{}{whole}.{fraction:08}",
            if value.is_sign_negative() { "-" } else { "" }
        )
    } else {
        format!("{value:.8}")
    };
    // float_to_list/2 uses a 256-byte buffer before removing trailing zeros.
    if rounded.len() >= 256 {
        return Err(Failure::ParentResponse);
    }
    if rounded.len() > maximum {
        return Err(Failure::Local(super::Error::ResourceLimit));
    }
    let (whole, fraction) = rounded.split_once('.').ok_or(Failure::ParentResponse)?;
    // Preserve native negative-float handling: the fraction is added even when
    // the whole part is negative (including -0).
    Ok(whole
        .parse::<BigInt>()
        .map_err(|_| Failure::ParentResponse)?
        * 100_000_000u64
        + fraction
            .parse::<BigInt>()
            .map_err(|_| Failure::ParentResponse)?)
}
fn decimal(satoshis: &BigInt) -> Result<String, Failure> {
    if satoshis < &0.into() {
        return Err(Failure::ParentResponse);
    }
    let mut digits = satoshis.to_string();
    if digits.len() <= 8 {
        digits = format!("{:0>9}", digits);
    }
    digits.insert(digits.len() - 8, '.');
    while digits.ends_with('0') && !digits.ends_with(".0") {
        digits.pop();
    }
    Ok(digits)
}
fn json_body(v: &Value, t: &Transport) -> Result<Vec<u8>, Failure> {
    let bytes = serde_json::to_vec(v).map_err(|_| Failure::ParentResponse)?;
    if bytes.len() > t.config.maximum_body_bytes {
        return Err(Failure::Local(super::Error::ResourceLimit));
    }
    Ok(bytes)
}
fn rpc_body(method: &str, params: Vec<Value>) -> Value {
    json!({"jsonrpc":"2.0","method":method,"params":params,"id":""})
}
async fn rpc(t: &Transport, n: &Node, method: &str, params: Vec<Value>) -> Result<Value, Failure> {
    let body = json_body(&rpc_body(method, params), t)?;
    t.request(n, "/", Some(&body))
        .await
        .map_err(Failure::Local)?
        .and_then(|mut v| v.as_object_mut()?.remove("result"))
        .ok_or(Failure::ParentResponse)
}
async fn publish(
    t: &Transport,
    n: &Node,
    response: (&str, &str),
    value: Value,
    transaction: Vec<u8>,
    progress: &Progress,
    mut journal: Option<&mut super::journal::Call<'_>>,
) -> Result<Vec<u8>, Failure> {
    let (path, field) = response;
    let body = json_body(&value, t)?;
    let submission = Submission {
        chain: n.chain,
        transaction: Arc::from(transaction),
    };
    if let Some(call) = &mut journal
        && let Some(record) = call.begin(&submission)?
    {
        progress.begin(super::journal::submission(&record));
        return record.response.ok_or(Failure::Recovered);
    }
    progress.begin(submission);
    // Mark conservatively before the transport is polled, including connect/TLS.
    // A cancellation can never be reported as definitely unsubmitted afterwards.
    let reply = t
        .request(n, path, Some(&body))
        .await
        .map_err(Failure::Local)?
        .ok_or(Failure::ParentResponse)?;
    let hash = reply
        .get(field)
        .and_then(Value::as_str)
        .ok_or(Failure::ParentResponse)?
        .as_bytes();
    if hash.len() > t.config.maximum_hash_bytes {
        return Err(Failure::Local(super::Error::ResourceLimit));
    }
    let hash = hash.to_vec();
    if let Some(call) = journal {
        call.response(hash.clone())?;
    }
    Ok(hash)
}
pub(super) async fn post(
    t: &Transport,
    n: &Node,
    signing: Option<&Signing>,
    post: &Post,
    payload: &[u8],
    progress: &Progress,
    journal: Option<&mut super::journal::Call<'_>>,
) -> Result<Vec<u8>, Failure> {
    match n.chain {
        Chain::Bitcoin => Err(Failure::Unsupported),
        Chain::Aeternity => {
            let who = api::encode(ApiType::AccountPubkey, &post.account.0);
            let reply = t
                .request(n, &format!("/v3/accounts/{who}/next-nonce"), None)
                .await
                .map_err(Failure::Local)?
                .ok_or(Failure::ParentResponse)?;
            let nonce = integer(
                reply.get("next_nonce").ok_or(Failure::ParentResponse)?,
                t.config.maximum_number_digits,
            )?;
            let nonce =
                Unsigned::from_decimal(&nonce.to_string()).map_err(|_| Failure::ParentResponse)?;
            let mut tx = Spend::new(post.account, Id::new(IdTag::Account, post.account.0))
                .expect("account self spend");
            tx.set_nonce(nonce);
            tx.set_amount(post.amount.clone());
            tx.set_fee(post.fee.clone());
            tx.set_payload(payload.to_vec());
            let signing = signing.ok_or(Failure::MissingKey)?;
            let mut message = signing.network.clone();
            message.extend_from_slice(&tx.encode());
            let signature = signing
                .keys
                .sign(post.account, &message)
                .map_err(|_| Failure::MissingKey)?;
            let bytes = SignedTransaction::new(tx, vec![signature]).encode();
            let value = json!({"tx":api::encode(ApiType::Transaction, &bytes)});
            publish(
                t,
                n,
                ("/v3/transactions", "tx_hash"),
                value,
                bytes,
                progress,
                journal,
            )
            .await
        }
        Chain::Dogecoin => {
            let unspent = rpc(t, n, "listunspent", vec![json!(0), json!(9999999)]).await?;
            let unspent = unspent.as_array().ok_or(Failure::ParentResponse)?;
            if unspent.len() > t.config.maximum_block_transactions {
                return Err(Failure::Local(super::Error::ResourceLimit));
            }
            // Native converts every float before selecting, including skipped rows.
            let amounts: Vec<_> = unspent
                .iter()
                .map(|u| {
                    u.get("amount")
                        .filter(|v| v.is_number())
                        .map(|v| float_satoshis(v, t.config.maximum_number_digits))
                        .transpose()
                })
                .collect::<Result<_, _>>()?;
            let amount: BigInt =
                BigInt::from_bytes_be(num_bigint::Sign::Plus, post.amount.as_bytes());
            let fee: BigInt = BigInt::from_bytes_be(num_bigint::Sign::Plus, post.fee.as_bytes());
            let needed = &amount + &fee;
            let mut selected = None;
            for (u, value) in unspent.iter().zip(amounts) {
                if u.get("spendable") != Some(&Value::Bool(true)) || u.get("address").is_none() {
                    continue;
                }
                // Malformed nonnumeric amounts that the native term comparison
                // selects fail before another wallet request, too.
                if u.get("amount").is_some() && value.is_none() {
                    return Err(Failure::ParentResponse);
                }
                if let Some(value) = value.filter(|v| v >= &needed) {
                    selected = Some((u, value));
                    break;
                }
            }
            let (u, total) = selected.ok_or(Failure::NoSuitableUtxo)?;
            let inputs = json!([{"txid":u.get("txid").ok_or(Failure::ParentResponse)?,"vout":u.get("vout").ok_or(Failure::ParentResponse)?}]);
            let address = u
                .get("address")
                .and_then(Value::as_str)
                .ok_or(Failure::ParentResponse)?;
            let mut outputs = serde_json::Map::new();
            // Native map literal writes Account twice: change replaces self-spend.
            outputs.insert(address.to_owned(), json!(decimal(&(total - fee - amount))?));
            let hex: String = payload.iter().map(|b| format!("{b:02X}")).collect();
            outputs.insert("data".into(), json!(hex));
            let raw = rpc(
                t,
                n,
                "createrawtransaction",
                vec![inputs, Value::Object(outputs)],
            )
            .await?;
            let signed = rpc(t, n, "signrawtransaction", vec![raw]).await?;
            if signed.get("complete") != Some(&Value::Bool(true)) {
                return Err(Failure::ParentResponse);
            }
            let hex = signed
                .get("hex")
                .and_then(Value::as_str)
                .ok_or(Failure::ParentResponse)?;
            publish(
                t,
                n,
                ("/", "result"),
                rpc_body("sendrawtransaction", vec![json!(hex)]),
                hex.as_bytes().to_vec(),
                progress,
                journal,
            )
            .await
        }
    }
}
#[cfg(test)]
mod tests {
    #[test]
    fn original_float_conversion() {
        let v: serde_json::Value =
            serde_json::from_str(include_str!("../../../tests/fixtures/hc_post.json")).unwrap();
        for row in v["floats"].as_array().unwrap() {
            let input = serde_json::from_str(row["input"].as_str().unwrap()).unwrap();
            let actual = super::float_satoshis(&input, 4096);
            if row.get("failure").is_some() {
                assert_eq!(actual, Err(super::Failure::ParentResponse), "{row}");
            } else {
                assert_eq!(
                    actual.unwrap().to_string(),
                    row["satoshis"].as_str().unwrap(),
                    "{row}"
                );
            }
        }
    }
}
