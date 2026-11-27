// aehttp_app:check_env/start_http_api and aehttp_cors_middleware, fa2a6d7.
// Copyright (c) Aeternity developers. ISC: repository LICENSE.
use crate::{
    configuration::resolution::{Error, Resolved, Strategy},
    http::{Cors, Target},
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, net::IpAddr};

mod address;
#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct Endpoint {
    pub target: Target,
    pub address: IpAddr,
    // Keep getter results intact. Socket ranges and service resource limits
    // are checked separately when converting this plan to running listeners.
    pub port: Value,
    pub acceptors: Value,
}
#[derive(Debug, Clone)]
pub struct Startup {
    pub endpoints: Vec<Endpoint>,
    pub groups: BTreeSet<String>,
    pub cors: Cors,
    pub idle_timeout_ms: u64,
    pub max_skip_body_length: Value,
}
impl Startup {
    pub fn resolve(resolved: &Resolved<'_>) -> Result<Self, Error> {
        let groups = groups(resolved)?;
        let endpoints = [
            (Target::External, "external", 8043, "0.0.0.0"),
            (Target::Internal, "internal", 8143, "127.0.0.1"),
            (Target::Rosetta, "rosetta", 8243, "127.0.0.1"),
            (Target::RosettaOffline, "rosetta_offline", 8343, "127.0.0.1"),
        ]
        .into_iter()
        .map(|(target, name, port, ip)| {
            let acceptors = property(
                resolved,
                &[name, "acceptors"],
                &[name, "acceptors"],
                json!(10),
            )?;
            let port = property(resolved, &[name, "port"], &[name, "port"], json!(port))?;
            // All four native getters share this legacy application path.
            let ip = property(
                resolved,
                &[name, "listen_address"],
                &["http", "websocket", "listen_address"],
                json!(ip),
            )?;
            let address = ip
                .as_str()
                .and_then(address::parse)
                .ok_or(Error::Field("listen_address"))?;
            Ok(Endpoint {
                target,
                address,
                port,
                acceptors,
            })
        })
        .collect::<Result<_, Error>>()?;
        Ok(Self {
            endpoints,
            groups,
            cors: cors(resolved)?,
            idle_timeout_ms: 480_000,
            max_skip_body_length: property(
                resolved,
                &["protocol_options", "max_skip_body_length"],
                &["protocol_options", "max_skip_body_length"],
                json!(1_000_000),
            )?,
        })
    }
}
fn property(
    resolved: &Resolved<'_>,
    path: &[&str],
    application: &[&str],
    fallback: Value,
) -> Result<Value, Error> {
    let mut full = vec!["http"];
    full.extend_from_slice(path);
    resolved
        .find(
            &full,
            &[
                Strategy::User,
                Strategy::ApplicationPath {
                    app: "aehttp",
                    path: application,
                },
                Strategy::SchemaDefault,
                Strategy::Value(&fallback),
            ],
        )?
        .ok_or(Error::Lookup)
}
pub fn groups(resolved: &Resolved<'_>) -> Result<BTreeSet<String>, Error> {
    let maintenance = resolved
        .find(
            &["system", "maintenance_mode"],
            &[Strategy::User, Strategy::SchemaDefault],
        )?
        .and_then(|v| v.as_bool())
        .ok_or(Error::Field("maintenance_mode"))?;
    let mut groups = BTreeSet::new();
    for (name, default) in [
        ("chain", !maintenance),
        ("transaction", !maintenance),
        ("account", !maintenance),
        ("contract", !maintenance),
        ("oracle", !maintenance),
        ("name_service", !maintenance),
        ("channel", !maintenance),
        ("node_info", true),
        ("node_settings", !maintenance),
        ("hyperchain", false),
        ("debug", true),
        ("dry-run", false),
        ("node-operator", false),
        ("rosetta", true),
        ("Account", true),
        ("Block", true),
        ("Call", !maintenance),
        ("Construction", !maintenance),
        ("Events", true),
        ("Mempool", true),
        ("Network", true),
        ("Search", true),
    ] {
        let enabled = resolved
            .find(
                &["http", "endpoints", name],
                &[
                    Strategy::User,
                    Strategy::SchemaDefault,
                    Strategy::Value(&json!(default)),
                ],
            )?
            .and_then(|v| v.as_bool())
            .ok_or(Error::Field("endpoints"))?;
        if enabled {
            groups.insert(name.to_owned());
        }
    }
    let obsolete = resolved
        .find(
            &["http", "endpoints", "obsolete"],
            &[
                Strategy::User,
                Strategy::SchemaDefault,
                Strategy::Value(&json!([])),
            ],
        )?
        .ok_or(Error::Lookup)?;
    // The native schema accepts a boolean here, but list subtraction fails.
    // Preserve the startup failure instead of interpreting false as no groups.
    for obsolete in obsolete.as_array().ok_or(Error::Field("obsolete"))? {
        if let Some(name) = obsolete.as_str() {
            groups.remove(name);
        }
    }
    Ok(groups)
}
pub fn cors(resolved: &Resolved<'_>) -> Result<Cors, Error> {
    let mut result = Cors::default();
    let Some(value) = resolved.get(&["http", "cors"])? else {
        return Ok(result);
    };
    let value = value.as_object().ok_or(Error::Field("cors"))?;
    let strings = |v: &Value| -> Result<Vec<String>, Error> {
        v.as_array()
            .ok_or(Error::Field("cors"))?
            .iter()
            .map(|v| v.as_str().map(str::to_owned).ok_or(Error::Field("cors")))
            .collect()
    };
    if let Some(v) = value.get("allow_domains") {
        result.domains = strings(v)?;
    }
    if let Some(v) = value.get("allow_methods") {
        result.methods = strings(v)?.join(", ");
    }
    if let Some(v) = value.get("allow_headers") {
        // Native Cowboy receives the list as an iolist, with no separator.
        result.headers = Some(strings(v)?.concat());
    }
    if let Some(v) = value.get("max_age") {
        result.max_age = v
            .as_number()
            .and_then(|n| n.as_str().parse().ok())
            .ok_or(Error::Field("max_age"))?;
    }
    Ok(result)
}
