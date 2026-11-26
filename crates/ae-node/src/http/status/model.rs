// aehttp_dispatch_ext:GetStatus and original Status schemas, fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use ae_consensus::target::Target;
use ae_protocol::{ConsensusAlgorithm, signaling::Fork};
use ae_vm_fate::{
    Limits,
    maps::{MapOrder, Traversal},
    value::Value as Fate,
};
use num_bigint::{BigInt, Sign};
use num_traits::ToPrimitive;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(super) fn integer(n: &BigInt) -> Value {
    serde_json::from_str(&n.to_string()).expect("integer JSON")
}
pub(super) fn raw_difficulty(target: u32, family: ConsensusAlgorithm) -> Result<BigInt, ()> {
    match family {
        ConsensusAlgorithm::Cuckoo => Target(target)
            .difficulty()
            .map(|n| BigInt::from_bytes_be(Sign::Plus, n.as_bytes()))
            .map_err(|_| ()),
        ConsensusAlgorithm::OnDemand => Ok(1338.into()),
        ConsensusAlgorithm::Hyperchain => Ok(target.into()),
    }
}
pub(super) fn difficulty(raw: &BigInt, family: ConsensusAlgorithm) -> Value {
    integer(&if family == ConsensusAlgorithm::Hyperchain {
        raw / 1_000_000_000_000u64
    } else {
        raw.clone()
    })
}
fn floating(value: &Value) -> Option<f64> {
    let number = value.as_number()?;
    if number.as_str().contains(['.', 'e', 'E']) {
        number.as_f64()
    } else {
        number.as_str().parse::<BigInt>().ok()?.to_f64()
    }
}
pub(super) fn hashrate(target: u32, family: ConsensusAlgorithm, rate: &Value) -> Result<Value, ()> {
    if family != ConsensusAlgorithm::Cuckoo || Target(target).value() == 0.into() {
        return Ok(json!(0));
    }
    let difficulty = raw_difficulty(target, family)?.to_f64().ok_or(())? / (1u32 << 24) as f64;
    let rate = floating(rate).ok_or(())? / 1000.0;
    let value = ((difficulty * 42.0) / rate).round();
    if !rate.is_finite() || rate == 0.0 || !value.is_finite() {
        return Err(());
    }
    let rounded = format!("{value:.0}").parse::<BigInt>().map_err(|_| ())?;
    Ok(integer(&rounded))
}
pub(super) fn uptime(milliseconds: u128) -> String {
    let seconds = milliseconds / 1000;
    if seconds < 60 {
        format!("{seconds}s.{}", milliseconds % 1000)
    } else if seconds < 3600 {
        format!(
            "{}m:{}s.{}",
            seconds / 60,
            seconds % 60,
            milliseconds % 1000
        )
    } else if seconds < 86400 {
        format!(
            "{}h:{}m:{}s",
            seconds / 3600,
            seconds % 3600 / 60,
            seconds % 60
        )
    } else {
        format!(
            "{}d:{}h:{}m:{}s",
            seconds / 86400,
            seconds % 86400 / 3600,
            seconds % 3600 / 60,
            seconds % 60
        )
    }
}
pub(super) fn protocols(
    mut entries: BTreeMap<u32, u64>,
    version: u32,
    fork: Option<Fork>,
    order: &impl MapOrder,
) -> Result<Value, ()> {
    if let Some(fork) = fork.filter(|f| f.version == version) {
        entries.insert(fork.version, fork.end);
    }
    let pairs: Vec<_> = entries.into_iter().collect();
    let keys: Vec<_> = pairs
        .iter()
        .map(|(v, _)| Fate::Integer((*v).into()))
        .collect();
    let indices = order
        .indices(
            &keys.iter().collect::<Vec<_>>(),
            Traversal::Fold,
            Limits::default(),
        )
        .map_err(|_| ())?;
    Ok(Value::Array(
        indices
            .into_iter()
            .rev()
            .map(|i| {
                let (version, height) = pairs[i];
                json!({"version":version, "effective_at_height":height})
            })
            .collect(),
    ))
}
fn uint(value: &Value, maximum: u128, v3: bool) -> bool {
    (v3 && value.is_string())
        || value
            .as_number()
            .is_some_and(|n| n.as_str().parse::<u128>().is_ok_and(|n| n <= maximum))
}
pub(super) fn valid_response(body: &Value, v3: bool) -> bool {
    let Some(body) = body.as_object() else {
        return false;
    };
    let max64 = if v3 {
        18_446_744_073_709_552_000u128
    } else {
        u128::from(u64::MAX)
    };
    if [
        "genesis_key_block_hash",
        "uptime",
        "node_version",
        "node_revision",
        "network_id",
        "peer_pubkey",
        "top_key_block_hash",
    ]
    .iter()
    .any(|k| !body.get(*k).is_some_and(Value::is_string))
        || ["syncing", "listening"]
            .iter()
            .any(|k| !body.get(*k).is_some_and(Value::is_boolean))
        || ["solutions", "difficulty", "hashrate", "top_block_height"]
            .iter()
            .any(|k| !body.get(*k).is_some_and(|n| uint(n, max64, v3)))
        || ["peer_count", "pending_transactions_count"]
            .iter()
            .any(|k| !body.get(*k).is_some_and(|n| uint(n, u32::MAX.into(), v3)))
    {
        return false;
    }
    if let Some(progress) = body.get("sync_progress")
        && !progress
            .as_f64()
            .is_some_and(|n| n.is_finite() && (0.0..=100.0).contains(&n))
    {
        return false;
    }
    let Some(connections) = body.get("peer_connections").and_then(Value::as_object) else {
        return false;
    };
    if ["inbound", "outbound"].iter().any(|k| {
        !connections
            .get(*k)
            .is_some_and(|n| uint(n, u32::MAX.into(), v3))
    }) {
        return false;
    }
    let Some(protocols) = body.get("protocols").and_then(Value::as_array) else {
        return false;
    };
    protocols.iter().all(|p| {
        p.as_object().is_some_and(|p| {
            p.get("version")
                .is_some_and(|n| uint(n, u32::MAX.into(), v3))
                && p.get("effective_at_height")
                    .is_some_and(|n| uint(n, max64, v3))
        })
    })
}
