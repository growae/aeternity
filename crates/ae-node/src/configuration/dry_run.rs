// aec_dry_run timeout/profile and worker configuration, fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::resolution::{Error, Resolved, Strategy};
use ae_execution::dry_run::Profile;
use num_bigint::BigInt;
use serde_json::{Value, json};
use std::time::Duration;

// Preserve native getter values before typed runtime validation. Application
// settings can bypass schema validation; invalid timer values are not infinity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Timeout {
    Unbounded,
    Milliseconds(Value),
}
impl Timeout {
    pub fn runtime(&self) -> Result<ae_sync::dry_run::Timeout, Error> {
        match self {
            Self::Unbounded => Ok(ae_sync::dry_run::Timeout::Unbounded),
            Self::Milliseconds(v) => {
                let ms = integer(v)
                    .and_then(|n| u64::try_from(n).ok())
                    .ok_or(Error::Field("timeout_ms"))?;
                Ok(ae_sync::dry_run::Timeout::After(Duration::from_millis(ms)))
            }
        }
    }
}
fn integer(v: &Value) -> Option<BigInt> {
    v.as_number()?.as_str().parse().ok()
}
fn setting(resolved: &Resolved<'_>, field: &str, default: &Value) -> Result<Value, Error> {
    resolved
        .find(
            &["http", "dry_run", field],
            &[
                Strategy::User,
                Strategy::ApplicationPath {
                    app: "aehttp",
                    path: &["dry_run", field],
                },
                Strategy::Value(default),
            ],
        )?
        .ok_or(Error::Lookup)
}
pub fn timeout(
    resolved: &Resolved<'_>,
    profile: Profile,
    explicit: Option<&Value>,
) -> Result<Timeout, Error> {
    if let Some(value) = explicit {
        integer(value).ok_or(Error::Field("timeout_ms"))?;
        return Ok(Timeout::Milliseconds(value.clone()));
    }
    let value = match profile {
        Profile::Public => {
            // The original evaluates micro_block_cycle before cfg_timeout,
            // even when an explicit public configuration value will win.
            let cycle = resolved
                .find(
                    &["mining", "micro_block_cycle"],
                    &[
                        Strategy::User,
                        Strategy::Application {
                            app: "aecore",
                            key: "micro_block_cycle",
                        },
                        Strategy::Value(&json!(3000)),
                    ],
                )?
                .ok_or(Error::Lookup)?;
            setting(resolved, "timeout_ms", &cycle)?
        }
        Profile::Replay => setting(resolved, "replay_timeout_ms", &json!(10000))?,
        _ => return Ok(Timeout::Unbounded),
    };
    Ok(if integer(&value) == Some(BigInt::ZERO) {
        Timeout::Unbounded
    } else {
        Timeout::Milliseconds(value)
    })
}
pub fn max_heap_words(resolved: &Resolved<'_>) -> Result<Value, Error> {
    setting(resolved, "max_heap_words", &json!(256000000))
}
pub fn heap_guard_words(value: &Value) -> Option<BigInt> {
    integer(value).filter(|n| n > &BigInt::ZERO)
}
// Native policy alone, for the child core phase. The service cap remains an
// independent bootstrap policy throughout decoding, execution and rendering.
pub fn native_heap_bytes(value: &Value, word_bytes: usize) -> Result<Option<usize>, Error> {
    if !matches!(word_bytes, 4 | 8) {
        return Err(Error::Field("dry_run_heap_budget"));
    }
    heap_guard_words(value)
        .map(|words| {
            usize::try_from(words * word_bytes)
                .ok()
                .filter(|n| *n < usize::MAX)
                .ok_or(Error::Field("max_heap_words"))
        })
        .transpose()
}
// Select an absolute Rust-worker allocation cap. Native words are interpreted
// with the captured Erlang word size; disabled native guarding never disables
// the independent service cap. Rust and Erlang heap layouts are not identical.
pub fn heap_limit(
    value: &Value,
    word_bytes: usize,
    service_bytes: usize,
) -> Result<ae_worker_heap::Limit, Error> {
    use ae_worker_heap::{Limit, Source};
    if !matches!(word_bytes, 4 | 8) || service_bytes == 0 || service_bytes == usize::MAX {
        return Err(Error::Field("dry_run_heap_budget"));
    }
    let native = native_heap_bytes(value, word_bytes)?;
    Ok(match native {
        Some(bytes) if bytes <= service_bytes => Limit {
            bytes,
            source: Source::Native,
        },
        _ => Limit {
            bytes: service_bytes,
            source: Source::Service,
        },
    })
}
pub fn store_read_gas_metering(resolved: &Resolved<'_>) -> Result<Value, Error> {
    setting(resolved, "store_read_gas_metering", &json!(true))
}

#[cfg(test)]
mod tests;
