// aehttp_dispatch_ext:dry_run_gas_limit, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::resolution::{Error, Resolved, Strategy};
use num_bigint::BigInt;
use serde_json::{Value, json};

// aehttp_app:enable_internal_debug_endpoints/0. This gate is distinct from
// registering the debug route group and defaults to disabled.
pub fn internal_debug_endpoints(resolved: &Resolved<'_>) -> Result<bool, Error> {
    resolved
        .find(
            &["http", "internal", "debug_endpoints"],
            &[
                Strategy::User,
                Strategy::ApplicationPath {
                    app: "aehttp",
                    path: &["internal", "debug_endpoints"],
                },
                Strategy::SchemaDefault,
                Strategy::Value(&json!(false)),
            ],
        )?
        .and_then(|value| value.as_bool())
        .ok_or(Error::Field("debug_endpoints"))
}

// The scalar dry-run getter must never read reporting_utilization_override.
pub fn reporting_floor(resolved: &Resolved<'_>) -> Result<Option<ae_types::Unsigned>, Error> {
    let value = resolved.find(
        &["http", "gas_price", "min_relay_gas_price"],
        &[
            Strategy::User,
            Strategy::ApplicationPath {
                app: "aehttp",
                path: &["gas_price", "min_relay_gas_price"],
            },
        ],
    )?;
    Ok(value
        .and_then(|v| {
            v.as_number()
                .and_then(|n| n.as_str().parse::<BigInt>().ok())
        })
        .filter(|n| *n > BigInt::ZERO)
        .map(|n| {
            ae_types::Unsigned::from_field_bytes(&n.to_bytes_be().1).expect("positive integer")
        }))
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GasReporting {
    pub price: Option<BigInt>,
    pub utilization: u8,
}
impl GasReporting {
    pub fn resolve(resolved: &Resolved<'_>) -> Result<Self, Error> {
        fn find(resolved: &Resolved<'_>, field: &str) -> Result<Option<Value>, Error> {
            resolved.find(
                &["http", "gas_price", field],
                &[
                    Strategy::User,
                    Strategy::ApplicationPath {
                        app: "aehttp",
                        path: &["gas_price", field],
                    },
                ],
            )
        }
        let price = find(resolved, "min_relay_gas_price")?
            .and_then(|v| {
                v.as_number()
                    .and_then(|n| n.as_str().parse::<BigInt>().ok())
            })
            .filter(|n| *n > BigInt::ZERO);
        // The native pair helper does not consult this setting at all when
        // the price override is disabled. An unused malformed application
        // property must not turn an otherwise valid observation into an error.
        let utilization = if price.is_some() {
            find(resolved, "reporting_utilization_override")?
                .and_then(|v| v.as_u64())
                .filter(|n| *n <= 100)
                .unwrap_or(0) as u8
        } else {
            0
        };
        Ok(Self { price, utilization })
    }
    pub fn apply(&self, summary: &mut ae_chain::gas_prices::Summary) {
        if let Some(price) = &self.price {
            summary.minimum = price.clone();
            summary.utilization = summary.utilization.clone().max(self.utilization.into());
        }
    }
    // The dry-run helper has different semantics from the observation pair.
    pub fn apply_price(&self, price: BigInt) -> BigInt {
        match &self.price {
            Some(floor) => price.max(floor.clone()),
            None => price,
        }
    }
}

#[cfg(test)]
mod tests;

pub fn external_dry_run_gas_limit(resolved: &Resolved<'_>) -> Result<Value, Error> {
    resolved
        .find(
            &["http", "external", "gas_limit"],
            &[
                Strategy::User,
                Strategy::ApplicationPath {
                    app: "aehttp",
                    path: &["external", "gas_limit"],
                },
                Strategy::SchemaDefault,
                Strategy::Value(&json!(6_000_000)),
            ],
        )?
        .ok_or(Error::Lookup)
}
