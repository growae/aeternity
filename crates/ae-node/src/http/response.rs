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
}
impl Response {
    pub fn empty(status: u16) -> Self {
        Self {
            status,
            headers: Headers::new(),
            body: vec![],
        }
    }
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: Headers::from([("content-type".into(), "application/json".into())]),
            body: serde_json::to_vec(&body).expect("JSON value"),
        }
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
pub(super) fn current(path: &str, config: &Config) -> Option<(bool, bool)> {
    if config.target != Target::External
        || !config
            .groups
            .iter()
            .any(|g| g == "chain" || g == "external")
    {
        return None;
    }
    let (v3, suffix) = if let Some(p) = path.strip_prefix("/v3") {
        (true, p)
    } else {
        (false, path.strip_prefix("/v2")?)
    };
    match suffix {
        "/key-blocks/current/height" => Some((v3, true)),
        "/key-blocks/current/hash" => Some((v3, false)),
        _ => None,
    }
}
pub(super) fn answer(current: Current, height: bool, strings: bool) -> Response {
    let mut body = if height {
        json!({"height":current.height})
    } else {
        json!({"hash":api::encode(ApiType::KeyBlockHash, current.key.as_ref())})
    };
    if strings {
        integers_as_strings(&mut body);
    }
    Response::json(200, body)
}

#[cfg(test)]
mod tests;
