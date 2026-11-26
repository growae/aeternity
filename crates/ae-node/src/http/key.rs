// aehttp_dispatch_ext:GetPeerPubkey, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{Config, response::Response};
use crate::runtime;
use ae_codec::api::{self, ApiType};
use ae_sync::keys;
use ae_types::PeerPublicKey;
use serde_json::{Value, json};

fn failure(error: keys::Error) -> Response {
    match error {
        keys::Error::Busy => Response::unavailable(true),
        _ => Response::internal(),
    }
}
pub(super) fn body(key: PeerPublicKey) -> Value {
    json!({"pubkey":api::encode(ApiType::PeerPubkey, key.as_ref())})
}
pub(super) async fn query(parent: &runtime::Handle, config: &Config) -> Result<Value, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let binding = parent
        .worker_bindings()
        .and_then(|w| w.keys)
        .ok_or_else(Response::internal)?;
    let captured = binding.current().ok_or_else(Response::internal)?;
    let ticket = captured.peer_public_key().map_err(failure)?;
    let key = tokio::time::timeout(config.timeout, ticket)
        .await
        .map_err(|_| Response::internal())?
        .map_err(failure)?;
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    if !captured.info().accepting
        || !parent
            .worker_bindings()
            .and_then(|w| w.keys)
            .and_then(|b| b.current())
            .is_some_and(|c| c.same_worker(&captured))
    {
        return Err(Response::internal());
    }
    Ok(body(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn peer_public_key_body_and_dependency_errors_match_original_dispatcher() {
        let native: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/erlang_http_queries.json"
        ))
        .unwrap();
        let rows = native["peer_dispatch"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        for row in rows {
            let response = if row["failure"] == "none" {
                let key = PeerPublicKey(
                    hex::decode(row["input"]["peer_key"].as_str().unwrap())
                        .unwrap()
                        .try_into()
                        .unwrap(),
                );
                Response::json(200, body(key))
            } else {
                failure(keys::Error::Stopped)
            };
            assert_eq!(response.status, row["code"].as_u64().unwrap() as u16);
            assert_eq!(
                serde_json::from_slice::<Value>(&response.body).unwrap(),
                row["body"]
            );
        }
        let busy = failure(keys::Error::Busy);
        assert_eq!(busy.status, 503);
        assert_eq!(busy.headers["retry-after"], "3");
    }
}
