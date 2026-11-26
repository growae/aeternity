// aehttp_dispatch_ext:dry_run_gas_limit, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use super::resolution::{Error, Resolved, Strategy};
use serde_json::{Value, json};

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
