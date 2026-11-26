// aehttp_dispatch_ext:GetSyncStatus, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::{Config, response::Response};
use crate::{configuration::resolution::ApplicationMode, runtime};
use ae_sync::chain::sync::{self, lifecycle::Progress};
use num_bigint::BigInt;
use serde_json::{Value, json};

trait Source {
    async fn progress(&self) -> Result<Progress, sync::Error>;
    fn estimate(&self, task: u64) -> Result<Option<Value>, sync::Error>;
    async fn height(&self) -> Result<u64, sync::Error>;
}
struct Live(Option<sync::Handle>);
impl Source for Live {
    async fn progress(&self) -> Result<Progress, sync::Error> {
        match &self.0 {
            Some(owner) => owner.progress()?.wait().await,
            None => Ok(Progress {
                syncing: false,
                percent: 100.0,
                target: 0,
                task: None,
            }),
        }
    }
    fn estimate(&self, task: u64) -> Result<Option<Value>, sync::Error> {
        self.0
            .as_ref()
            .ok_or(sync::Error::Closed)?
            .estimate(task)
            .map(|estimate| estimate.map(|n| json!(n)))
    }
    async fn height(&self) -> Result<u64, sync::Error> {
        self.0
            .as_ref()
            .ok_or(sync::Error::Closed)?
            .selected_height()?
            .wait()
            .await
    }
}
fn failure(error: sync::Error) -> Response {
    match error {
        sync::Error::Busy => Response::unavailable(true),
        _ => Response::internal(),
    }
}
async fn body(source: &impl Source) -> Result<Value, Response> {
    let progress = source.progress().await.map_err(failure)?;
    if !progress.syncing {
        return Err(Response::json(404, json!({"reason":"Node is not syncing"})));
    }
    let speed = source
        .estimate(progress.task.ok_or_else(Response::internal)?)
        .map_err(failure)?;
    let (speed, estimate) = match speed {
        None => (json!(0), json!(0)),
        Some(speed) => {
            let top = source.height().await.map_err(failure)?;
            let n = speed.as_f64().ok_or_else(Response::internal)?;
            // Erlang subtracts integers before converting to a floating point
            // quotient. Signed difference and half-away rounding are observable.
            let remaining = ((i128::from(progress.target) - i128::from(top)) as f64 / n).round();
            if !remaining.is_finite() {
                return Err(Response::internal());
            }
            let minutes = BigInt::parse_bytes(format!("{remaining:.0}").as_bytes(), 10)
                .ok_or_else(Response::internal)?;
            let seconds = minutes * BigInt::from(60);
            let estimate = serde_json::from_str::<Value>(&seconds.to_string())
                .map_err(|_| Response::internal())?;
            (speed, estimate)
        }
    };
    Ok(
        json!({"progress":progress.percent,"target":progress.target,"speed":speed,"estimate":estimate}),
    )
}
pub(super) fn valid_response(body: &Value, v3: bool) -> bool {
    // The generated GetSyncStatus operation has no int-as-string parameter.
    // Its UInt64 schema differs between the two original specifications.
    let maximum = if v3 {
        18_446_744_073_709_552_000u128
    } else {
        u128::from(u64::MAX)
    };
    ["target", "estimate"].into_iter().all(|field| {
        body[field].is_number()
            && body[field]
                .to_string()
                .parse::<u128>()
                .is_ok_and(|value| value <= maximum)
    }) && ["progress", "speed"]
        .into_iter()
        .all(|field| body[field].as_f64().is_some_and(f64::is_finite))
}
pub(super) async fn query(parent: &runtime::Handle, config: &Config) -> Result<Value, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let owner = parent
        .connections()
        .and_then(|c| c.current())
        .map(|g| g.sync);
    // Missing an expected normal-mode owner is a failed dependency. A disabled
    // application has the native inactive fallback. Neither uses watch samples.
    if owner.is_none() && parent.mode() == ApplicationMode::Normal {
        return Err(Response::internal());
    }
    let source = Live(owner);
    let result = tokio::time::timeout(config.timeout, body(&source))
        .await
        .map_err(|_| Response::internal())?;
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    if let Some(owner) = &source.0
        && !parent
            .connections()
            .and_then(|c| c.current())
            .is_some_and(|g| owner.same_owner(&g.sync))
    {
        return Err(Response::internal());
    }
    result
}

#[cfg(test)]
mod tests;
