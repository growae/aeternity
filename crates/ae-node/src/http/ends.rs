// aehttp_dispatch_ext:GetChainEnds, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{
    Config,
    response::{self, Response},
};
use crate::runtime;
use ae_codec::api::{self, ApiType};
use ae_sync::chain::{self, query::Retained};
use ae_types::Hash256;
use serde_json::Value;
use std::sync::Arc;

const MAX_ENDS: usize = 16_384;
fn body(ends: Vec<Hash256>) -> Value {
    Value::Array(
        ends.into_iter()
            .map(|h| Value::String(api::encode(ApiType::KeyBlockHash, h.as_ref())))
            .collect(),
    )
}
pub(super) fn finish(mut value: Value, v3: bool, strings: bool) -> Response {
    if strings {
        response::integers_as_strings(&mut value);
    }
    if super::model::valid("GetChainEnds", &value, v3) {
        Response::json(200, value)
    } else {
        Response::invalid_response()
    }
}
pub(super) async fn query(
    parent: &runtime::Handle,
    config: &Config,
    captured: Option<&Arc<chain::Importer>>,
    v3: bool,
    strings: bool,
) -> Result<Retained<Response>, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let importer = captured.ok_or_else(|| Response::unavailable(false))?;
    // Hashes are fixed width; this covers encoded strings, JSON values and the
    // serialized response concurrently. It is a service bound, not consensus.
    let ticket = importer
        .chain_ends(MAX_ENDS, MAX_ENDS * 256, move |ends| {
            finish(body(ends), v3, strings)
        })
        .map_err(super::transport::failure)?;
    let reply = tokio::time::timeout(config.timeout, ticket.wait_retained())
        .await
        .map_err(|_| Response::internal())?
        .map_err(super::transport::failure)?;
    if !parent.status().ready
        || parent
            .importer()
            .is_none_or(|live| !Arc::ptr_eq(&live, importer))
    {
        return Err(Response::unavailable(false));
    }
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_chain_end_dispatcher_preserves_hash_bytes_and_encounter_order() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/erlang_http_chain_ends.json"
        ))
        .unwrap();
        let rows = fixture["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 20);
        for row in rows.iter().filter(|r| r["code"] == 200) {
            let hashes = row["hashes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| {
                    Hash256(
                        hex::decode(h.as_str().unwrap())
                            .unwrap()
                            .try_into()
                            .unwrap(),
                    )
                })
                .collect();
            assert_eq!(body(hashes), row["body"], "{}", row["name"]);
            assert_eq!(row["queue"], "http_read");
        }
    }
}
