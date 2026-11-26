// aehttp_cors_middleware and aehttp_dispatch_ext, aeternity fa2a6d7.
// Copyright (c) 2018-2026, Aeternity Anstalt. ISC: repository LICENSE.
use super::{Config, Target};
use ae_codec::api::{self, ApiType};
use ae_sync::chain::query::Current;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(super) type Headers = BTreeMap<String, String>;
#[derive(Debug, Clone)]
pub struct Cors {
    pub domains: Vec<String>,
    pub methods: String,
    pub headers: Option<String>,
    pub max_age: u64,
}
impl Default for Cors {
    fn default() -> Self {
        Self {
            domains: vec!["*".into()],
            methods: "DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT".into(),
            headers: None,
            max_age: 1800,
        }
    }
}
impl Cors {
    pub(super) fn valid(&self) -> bool {
        let values = self
            .domains
            .iter()
            .chain(std::iter::once(&self.methods))
            .chain(self.headers.iter());
        let mut size = 0usize;
        for value in values {
            if !value.bytes().all(|b| b == b'\t' || (32..127).contains(&b)) {
                return false;
            }
            size = size.saturating_add(value.len());
        }
        self.domains.len() <= 256 && size <= 16 << 10
    }
    pub(super) fn apply(&self, request: &Headers, response: &mut Headers) {
        let Some(origin) = request.get("origin") else {
            return;
        };
        if !self.domains.iter().any(|d| d == "*" || d == origin) {
            return;
        }
        for (key, value) in [
            ("vary", "origin".into()),
            ("access-control-allow-origin", origin.clone()),
            ("access-control-allow-methods", self.methods.clone()),
            ("access-control-allow-credentials", "true".into()),
            (
                "access-control-expose-headers",
                "retry-after, x-ae-height".into(),
            ),
            ("access-control-max-age", self.max_age.to_string()),
        ] {
            response.insert(key.into(), value);
        }
        if let Some(requested) = request.get("access-control-request-headers") {
            response.insert(
                "access-control-allow-headers".into(),
                self.headers.as_ref().unwrap_or(requested).clone(),
            );
        }
    }
}
#[derive(Debug)]
pub(super) struct Response {
    pub status: u16,
    pub headers: Headers,
    pub body: Vec<u8>,
    application_headers: bool,
}
impl Response {
    pub fn internal() -> Self {
        Self::json(500, json!({"reason":"Internal server error"}))
    }
    pub fn empty(status: u16) -> Self {
        Self {
            status,
            headers: Headers::new(),
            body: vec![],
            application_headers: true,
        }
    }
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: Headers::from([("content-type".into(), "application/json".into())]),
            body: serde_json::to_vec(&body).expect("JSON value"),
            application_headers: true,
        }
    }
    pub fn invalid_response() -> Self {
        Self::stream_error(500)
    }
    pub fn stream_error(status: u16) -> Self {
        // Cowboy's request-process error response has no application body,
        // CORS headers, or cached height marker.
        let mut response = Self::empty(status);
        response.application_headers = false;
        response
    }
    pub fn unavailable(overload: bool) -> Self {
        let mut response = Self::json(
            503,
            json!({"reason":if overload {"Temporary overload"} else {"Not yet started"}}),
        );
        response.headers.insert(
            "retry-after".into(),
            if overload { "3" } else { "30" }.into(),
        );
        response
    }
    pub fn stamp(&mut self, config: &Config, request: &Headers, height: Option<u64>) {
        if !self.application_headers {
            return;
        }
        if let Some(height) = height {
            self.headers
                .insert("x-ae-height".into(), height.to_string());
        }
        config.cors.apply(request, &mut self.headers);
    }
}
pub(super) fn integers_as_strings(value: &mut Value) {
    match value {
        Value::Number(n) if !n.to_string().contains(['.', 'e', 'E']) => {
            *value = Value::String(n.to_string())
        }
        Value::Array(values) => {
            for value in values {
                integers_as_strings(value);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                integers_as_strings(value);
            }
        }
        _ => (),
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Operation {
    PostTransaction {
        v3: bool,
    },
    Current {
        height: bool,
    },
    SyncStatus {
        v3: bool,
    },
    PeerPublicKey,
    Status {
        v3: bool,
    },
    Blocks {
        operation: super::blocks::Operation,
        v3: bool,
    },
    Transactions {
        operation: super::transactions::Operation,
        v3: bool,
    },
}
pub(super) fn route(path: &str, config: &Config) -> Option<(bool, Operation)> {
    if config.target != Target::External {
        return None;
    }
    let segments = super::transport::path_segments(path)?;
    let (version, suffix) = segments.split_first()?;
    let v3 = match version.as_slice() {
        b"v3" => true,
        b"v2" => false,
        _ => return None,
    };
    let suffix: Vec<_> = suffix.iter().map(Vec::as_slice).collect();
    if suffix.as_slice() == [b"transactions".as_slice()] {
        return config
            .groups
            .contains("transaction")
            .then_some((v3, Operation::PostTransaction { v3 }));
    }
    if let Some(operation) = super::transactions::route(&suffix) {
        return config
            .groups
            .contains(operation.group())
            .then_some((v3, Operation::Transactions { operation, v3 }));
    }
    let (operation, group) = match suffix.as_slice() {
        [b"key-blocks", b"current", b"height"] => (Operation::Current { height: true }, "chain"),
        [b"key-blocks", b"current", b"hash"] => (Operation::Current { height: false }, "chain"),
        [b"sync-status"] => (Operation::SyncStatus { v3 }, "node_info"),
        [b"peers", b"pubkey"] => (Operation::PeerPublicKey, "node_info"),
        [b"status"] => (Operation::Status { v3 }, "node_info"),
        _ => (
            Operation::Blocks {
                operation: super::blocks::route(v3, &suffix)?,
                v3,
            },
            "chain",
        ),
    };
    config
        .groups
        .iter()
        .any(|g| g == group || g == "external")
        .then_some((v3, operation))
}
pub(super) fn current(current: Current, height: bool) -> Value {
    if height {
        json!({"height":current.height})
    } else {
        json!({"hash":api::encode(ApiType::KeyBlockHash, current.key.as_ref())})
    }
}

#[cfg(test)]
mod tests;
