// aehttp_dispatch_ext:GetStatus, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{Config, response::Response};
use crate::{configuration::resolution::ApplicationMode, runtime};
use ae_codec::api::{self, ApiType};
use ae_storage::pool::worker as pool;
use ae_sync::{chain, keys, network};
use serde_json::{Value, json};
use std::sync::Arc;

mod model;
#[cfg(test)]
mod tests;

pub(super) fn valid_response(body: &Value, v3: bool) -> bool {
    model::valid_response(body, v3)
}
fn failure(busy: bool) -> Response {
    if busy {
        Response::unavailable(true)
    } else {
        Response::internal()
    }
}
fn chain_failure(error: chain::Error) -> Response {
    failure(error == chain::Error::Busy)
}
fn key_failure(error: keys::Error) -> Response {
    failure(error == keys::Error::Busy)
}
fn pool_failure(error: pool::Error) -> Response {
    failure(error == pool::Error::Saturated)
}
async fn body(parent: &runtime::Handle) -> Result<Value, Response> {
    let importer = parent.importer().ok_or_else(Response::internal)?;
    let key = importer
        .top_key()
        .map_err(chain_failure)?
        .wait()
        .await
        .map_err(chain_failure)?;
    let information = &parent.information;
    let raw = model::raw_difficulty(key.header.target, key.algorithm)
        .map_err(|_| Response::internal())?;
    let difficulty = model::difficulty(&raw, key.algorithm);
    let hashrate = model::hashrate(
        key.header.target,
        key.algorithm,
        &information.expected_mine_rate,
    )
    .map_err(|_| Response::internal())?;
    let generation = parent.connections().and_then(|c| c.current());
    let progress = match &generation {
        Some(g) => g
            .sync
            .progress()
            .map_err(|e| failure(e == chain::sync::Error::Busy))?
            .wait()
            .await
            .map_err(|e| failure(e == chain::sync::Error::Busy))?,
        None if parent.mode() != ApplicationMode::Normal => chain::sync::lifecycle::Progress {
            syncing: false,
            percent: 100.0,
            target: 0,
            task: None,
        },
        None => return Err(Response::internal()),
    };
    let uptime = model::uptime(information.started.elapsed().as_millis());
    let protocols = model::protocols(
        key.network
            .activations()
            .iter()
            .map(|a| (a.protocol as u32, a.height))
            .collect(),
        key.header.common.version,
        key.network.community_fork(),
        &information.order,
    )
    .map_err(|_| Response::internal())?;
    let (peers, inbound, outbound) = match &generation {
        Some(g) => {
            let counts = g
                .network
                .metrics()
                .map_err(|e| failure(e == network::Error::Busy))?
                .wait()
                .await
                .map_err(|e| failure(e == network::Error::Busy))?;
            (
                counts
                    .verified
                    .checked_add(counts.unverified)
                    .ok_or_else(Response::internal)?,
                counts.registry.counts.inbound,
                counts.registry.counts.outbound,
            )
        }
        None => (0, 0, 0),
    };
    let bindings = parent.worker_bindings().ok_or_else(Response::internal)?;
    let admission = bindings
        .admission
        .current()
        .ok_or_else(Response::internal)?
        .client;
    let pool = admission
        .inspection()
        .map_err(pool_failure)?
        .await
        .map_err(pool_failure)?;
    let pending = pool
        .active
        .checked_add(pool.visited)
        .ok_or_else(Response::internal)?;
    let keys = parent
        .worker_bindings()
        .and_then(|w| w.keys)
        .and_then(|b| b.current())
        .ok_or_else(Response::internal)?;
    let peer_key = keys
        .peer_public_key()
        .map_err(key_failure)?
        .await
        .map_err(key_failure)?;
    // This is intentionally a later chain sample. The original status endpoint
    // reads dependencies sequentially; it does not offer a whole-node snapshot.
    let selected = importer
        .current()
        .map_err(chain_failure)?
        .wait()
        .await
        .map_err(chain_failure)?;
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let current = parent.connections().and_then(|c| c.current());
    let same_generation = match (&generation, &current) {
        (Some(a), Some(b)) => a.number == b.number && a.sync.same_owner(&b.sync),
        (None, None) => true,
        _ => false,
    };
    if !same_generation
        || parent
            .importer()
            .is_none_or(|c| !Arc::ptr_eq(&c, &importer))
        || !admission.info().accepting
        || !keys.info().accepting
        || !parent
            .worker_bindings()
            .and_then(|w| w.admission.current())
            .is_some_and(|p| p.client.same_worker(&admission))
        || !parent
            .worker_bindings()
            .and_then(|w| w.keys)
            .and_then(|b| b.current())
            .is_some_and(|k| k.same_worker(&keys))
    {
        return Err(Response::internal());
    }
    let text = |b: &[u8]| String::from_utf8(b.to_vec()).map_err(|_| Response::internal());
    Ok(json!({
        "genesis_key_block_hash":api::encode(ApiType::KeyBlockHash, key.genesis.as_ref()),
        "solutions":0, "difficulty":difficulty, "hashrate":hashrate,
        "syncing":progress.syncing, "sync_progress":progress.percent, "uptime":uptime,
        "listening":true, "protocols":protocols,
        "node_version":text(&information.build.version)?, "node_revision":text(&information.build.revision)?,
        "peer_count":peers, "peer_connections":{"inbound":inbound,"outbound":outbound},
        "pending_transactions_count":pending, "network_id":text(key.network.network_id())?,
        "peer_pubkey":api::encode(ApiType::PeerPubkey, peer_key.as_ref()),
        "top_key_block_hash":api::encode(ApiType::KeyBlockHash, selected.key.as_ref()),
        "top_block_height":selected.height,
    }))
}
pub(super) async fn query(parent: &runtime::Handle, config: &Config) -> Result<Value, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    tokio::time::timeout(config.timeout, body(parent))
        .await
        .map_err(|_| Response::internal())?
}
