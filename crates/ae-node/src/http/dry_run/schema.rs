// aehttp_api_handler/validate and generated v2/v3 DryRunInput, fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
pub use super::super::json::{Body, Error};
use super::super::{json as body_json, model, response, transport};
use num_bigint::BigInt;
use serde_json::{Value, json};

#[derive(Debug)]
pub struct Parsed {
    pub input: Value,
    pub strings: bool,
}
#[derive(Clone, Copy)]
enum Kind {
    Input,
    Account,
    Item,
    Call,
    Context,
    Accounts,
    Items,
    String,
    Bytes(&'static str),
    Word(&'static str),
    Bool,
    UInt(Option<&'static str>),
}
fn invalid(error: &str, data: &Value, path: &[Value]) -> Error {
    Error::Validation {
        parameter: "body",
        info: json!({"error":error,"data":data,"path":path}),
    }
}
fn check(kind: Kind, value: &Value, v3: bool, path: &mut Vec<Value>) -> Result<(), Error> {
    use Kind::*;
    let (fields, required): (&[(&str, Kind)], &[&str]) = match kind {
        Input => (
            &[
                ("top", String),
                ("accounts", Accounts),
                ("txs", Items),
                ("tx_events", Bool),
            ],
            &["txs"],
        ),
        Account => (
            &[("pub_key", String), ("amount", UInt(None))],
            &["pub_key", "amount"],
        ),
        Item if v3 => (
            &[
                ("tx", Bytes("tx")),
                ("tx_hash", Word("th")),
                ("call_req", Call),
            ],
            &[],
        ),
        // The v2 tx_hash declaration is nested inside the tx property's $ref
        // object, so it is ignored by Jesse. Do not repair the legacy schema.
        Item => (&[("tx", String), ("call_req", Call)], &[]),
        Call => (
            &[
                ("calldata", Bytes("cb")),
                ("contract", String),
                ("amount", UInt(None)),
                ("gas", UInt(None)),
                ("caller", String),
                (
                    "nonce",
                    UInt(Some(if v3 {
                        "18446744073709552000"
                    } else {
                        "18446744073709551615"
                    })),
                ),
                ("abi_version", UInt(Some("65535"))),
                ("context", Context),
            ],
            &["calldata", "contract"],
        ),
        Context => (
            &[
                ("tx", Bytes("tx")),
                ("tx_hash", Word("th")),
                ("stateful", Bool),
            ],
            &[],
        ),
        Accounts | Items => {
            let array = value
                .as_array()
                .ok_or_else(|| invalid("wrong_type", value, path))?;
            for (index, value) in array.iter().enumerate() {
                path.push(json!(index));
                check(
                    if matches!(kind, Accounts) {
                        Account
                    } else {
                        Item
                    },
                    value,
                    v3,
                    path,
                )?;
                path.pop();
            }
            return Ok(());
        }
        String | Bytes(_) | Word(_) => {
            let string = value
                .as_str()
                .ok_or_else(|| invalid("wrong_type", value, path))?;
            let pattern = match kind {
                Bytes("tx") if v3 => Some("^tx_[0-9A-Za-z+/=]+$"),
                Bytes("cb") if v3 => Some("^cb_[0-9A-Za-z+/=]+$"),
                Word("th") if v3 => Some(r"^th_\w+$"),
                _ => None,
            };
            return if pattern.is_none_or(|p| model::matches_pattern(p, string)) {
                Ok(())
            } else {
                Err(invalid("no_match", value, path))
            };
        }
        Bool => {
            return if value.is_boolean() {
                Ok(())
            } else {
                Err(invalid("wrong_type", value, path))
            };
        }
        UInt(maximum) => {
            if v3 && value.is_string() {
                return Ok(());
            }
            let number = value
                .as_number()
                .and_then(|n| n.as_str().parse::<BigInt>().ok());
            let reason = match number {
                None => "wrong_type",
                Some(n)
                    if n < 0.into()
                        || maximum.is_some_and(|max| {
                            n > max.parse::<BigInt>().expect("schema integer")
                        }) =>
                {
                    "not_in_range"
                }
                Some(_) => return Ok(()),
            };
            return Err(invalid(
                if v3 { "not_one_schema_valid" } else { reason },
                value,
                path,
            ));
        }
    };
    let object = value
        .as_object()
        .ok_or_else(|| invalid("wrong_type", value, path))?;
    // Jesse walks the original YAML property order, then required fields.
    for (field, kind) in fields {
        if let Some(value) = object.get(*field) {
            path.push(json!(field));
            check(*kind, value, v3, path)?;
            path.pop();
        }
    }
    for field in required {
        if !object.contains_key(*field) {
            return Err(invalid("missing_required_property", &json!(field), path));
        }
    }
    Ok(())
}
// Both original operations read and validate the body before examining the v3
// integer-conversion query. Defaults in nested JSON schemas do not mutate input.
pub fn request(body: Body<'_>, v3: bool, query: &str, maximum: usize) -> Result<Parsed, Error> {
    let input = body_json::parse(body, maximum)?;
    check(Kind::Input, &input, v3, &mut vec![])?;
    let strings = if v3 {
        transport::strings(query).map_err(|response| Error::Stream(response.status))?
    } else {
        false
    };
    Ok(Parsed { input, strings })
}
// Rendering is independent of execution. The service must bound/retain the
// produced Value and socket lifetime; this function is not worker admission.
pub fn render(code: u16, mut body: Value, v3: bool, strings: bool) -> Result<Value, Error> {
    if strings {
        response::integers_as_strings(&mut body);
    }
    let valid = match code {
        500..=599 => true,
        200 => model::valid("DryRunResults", &body, v3),
        400 | 403 => model::valid("DryRunError", &body, v3),
        _ => false,
    };
    if valid {
        Ok(body)
    } else {
        Err(Error::Stream(500))
    }
}

#[cfg(test)]
mod tests;
