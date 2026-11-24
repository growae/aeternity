// aestratum_jsonrpc 17b56c5. Internal message maps use string discriminators
// (type/method/reason) and a two-element [account, worker] array for user tuples.
// Incoming response contents are deliberately validated in a separate step.
use super::{json, lowercase};
use num_bigint::BigInt;
use num_traits::{FromPrimitive, ToPrimitive, Zero};
use serde_json::{Map, Number, Value, json as value};
use std::{cmp::Ordering, collections::BTreeMap, str::FromStr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaybeId {
    Undefined,
    Null,
    Integer(BigInt),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Parse,
    InvalidMessage(MaybeId),
    InvalidMethod(MaybeId),
    InvalidParam(&'static str, MaybeId),
    Internal(MaybeId),
    UnsupportedResponseMethod,
    Resource(json::Error),
    Option,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, Default)]
pub struct Options {
    values: BTreeMap<String, BigInt>,
    pub limits: json::Limits,
}
impl Options {
    pub fn set(&mut self, key: &str, value: BigInt) -> Result<(), Error> {
        if default_option(key).is_none() || value.bits() > crate::MAX_INTEGER_BITS {
            return Err(Error::Option);
        }
        self.values.insert(key.to_owned(), value);
        Ok(())
    }
    fn get(&self, key: &str) -> BigInt {
        self.values
            .get(key)
            .cloned()
            .unwrap_or_else(|| BigInt::from(default_option(key).unwrap_or(0)))
    }
    fn has(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }
}
fn default_option(key: &str) -> Option<i64> {
    Some(match key {
        "id_min" | "wait_time_min" | "pow_number_min" => 0,
        "id_max" | "block_version_max" | "pow_number_max" => 0xffff_ffff,
        "host_max_size" | "error_msg_max_size" => 255,
        "port_min" | "block_version_min" | "worker_min_size" => 1,
        "port_max" => 65535,
        "account_min_size" => 41,
        "account_max_size" => 53,
        "worker_max_size" | "user_agent_max_size" | "target_size" | "block_hash_size" => 64,
        "session_id_size" | "job_id_size" | "nonce_size" => 16,
        "wait_time_max" => 3600,
        "pow_size" => 42,
        "error_data_max_size" => 511,
        // Presence matters. The native password_min_size option is ignored.
        "password_size" | "password_min_size" | "password_max_size" => 0,
        _ => return None,
    })
}
fn integer(v: &Value) -> Option<BigInt> {
    let text = v.as_number()?.as_str();
    if text.len() > 4096 || text.contains(['.', 'e', 'E']) {
        return None;
    }
    BigInt::from_str(text).ok()
}
fn number(n: &BigInt) -> Result<Value, Error> {
    if n.bits() > crate::MAX_INTEGER_BITS {
        return Err(Error::Resource(json::Error::Limit("integer")));
    }
    Number::from_str(&n.to_string())
        .map(Value::Number)
        .map_err(|_| Error::Parse)
}
fn is_id(v: &Value, o: &Options) -> bool {
    integer(v).is_some_and(|n| n >= o.get("id_min") && n <= o.get("id_max"))
}
pub fn to_id(v: &Value, o: &Options) -> Option<BigInt> {
    integer(v).filter(|n| *n >= o.get("id_min") && *n <= o.get("id_max"))
}
pub fn next_id(id: &BigInt, o: &Options) -> Result<BigInt, Error> {
    if id.bits() > crate::MAX_INTEGER_BITS {
        return Err(Error::Resource(json::Error::Limit("integer")));
    }
    let next = (id + 1u8) & o.get("id_max");
    if next.bits() > crate::MAX_INTEGER_BITS {
        return Err(Error::Resource(json::Error::Limit("integer")));
    }
    Ok(if next.is_zero() {
        o.get("id_min")
    } else {
        next
    })
}
fn maybe_id(v: Option<&Value>, o: &Options) -> MaybeId {
    match v {
        Some(Value::Null) => MaybeId::Null,
        Some(v) => to_id(v, o).map_or(MaybeId::Undefined, MaybeId::Integer),
        None => MaybeId::Undefined,
    }
}
struct Check<'a> {
    opts: &'a Options,
    id: MaybeId,
}
impl Check<'_> {
    fn param(&self, p: &'static str) -> Error {
        Error::InvalidParam(p, self.id.clone())
    }
    fn invalid(&self) -> Error {
        Error::InvalidMessage(self.id.clone())
    }
    fn internal(&self) -> Error {
        Error::Internal(self.id.clone())
    }
    fn require(&self, yes: bool, p: &'static str) -> Result<(), Error> {
        if yes { Ok(()) } else { Err(self.param(p)) }
    }
    fn id(&self, v: &Value, null: bool, only_null: bool) -> Result<(), Error> {
        if (null && v.is_null()) || (!only_null && is_id(v, self.opts)) {
            Ok(())
        } else {
            Err(self.invalid())
        }
    }
    fn fields(&self, m: &Map<String, Value>, allowed: &[&str]) -> Result<(), Error> {
        if m.keys().all(|k| allowed.contains(&k.as_str())) {
            Ok(())
        } else {
            Err(self.invalid())
        }
    }
    fn size(&self, n: usize, key: &str) -> bool {
        BigInt::from(n) == self.opts.get(key)
    }
    fn max(&self, n: usize, key: &str) -> bool {
        BigInt::from(n) <= self.opts.get(key)
    }
    fn range(&self, n: usize, min: &str, max: &str) -> bool {
        let n = BigInt::from(n);
        n >= self.opts.get(min) && n <= self.opts.get(max)
    }
    fn hex(&self, v: &Value, key: &str, param: &'static str, null: bool) -> Result<(), Error> {
        self.require(
            (null && v.is_null())
                || v.as_str().is_some_and(|s| {
                    self.size(s.len(), key) && s.bytes().all(|b| b.is_ascii_hexdigit())
                }),
            param,
        )
    }
    fn num(
        &self,
        v: &Value,
        min: &str,
        max: &str,
        param: &'static str,
        strict: bool,
        null: bool,
    ) -> Result<(), Error> {
        self.require(
            (null && v.is_null())
                || integer(v).is_some_and(|n| {
                    (if strict {
                        n > self.opts.get(min)
                    } else {
                        n >= self.opts.get(min)
                    }) && n <= self.opts.get(max)
                }),
            param,
        )
    }
    fn text(
        &self,
        v: &Value,
        max: &str,
        param: &'static str,
        null: bool,
        whitespace: bool,
    ) -> Result<(), Error> {
        self.require(
            (null && v.is_null())
                || v.as_str().is_some_and(|s| {
                    !s.is_empty() && self.max(s.len(), max) && (!whitespace || valid_string(s))
                }),
            param,
        )
    }
    fn user(&self, v: &Value, internal: bool) -> Result<(), Error> {
        let (account, worker) = if internal && v.is_array() {
            let a = v.as_array().ok_or_else(|| self.param("user"))?;
            if a.len() != 2 {
                return Err(self.param("user"));
            }
            (
                a[0].as_str().ok_or_else(|| self.internal())?,
                a[1].as_str().ok_or_else(|| self.internal())?,
            )
        } else if let Some(s) = v.as_str() {
            let mut parts = s.split('.');
            let a = parts.next().ok_or_else(|| self.param("user"))?;
            let w = parts.next().ok_or_else(|| self.param("user"))?;
            if a.is_empty() || w.is_empty() || parts.next().is_some() {
                return Err(self.param("user"));
            }
            (a, w)
        } else {
            return Err(self.param("user"));
        };
        let account_valid = if self.range(account.len(), "account_min_size", "account_max_size") {
            let rest = account.strip_prefix("ak_").ok_or_else(|| self.internal())?;
            rest.bytes()
                .all(|b| b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(&b))
        } else {
            false
        };
        let w = worker.strip_suffix('\n').unwrap_or(worker);
        let worker_valid = self.range(worker.len(), "worker_min_size", "worker_max_size")
            && !w.is_empty()
            && w.bytes().all(|b| b.is_ascii_alphanumeric());
        self.require(account_valid && worker_valid, "user")
    }
    fn password(&self, v: &Value) -> Result<(), Error> {
        match (
            self.opts.has("password_size"),
            self.opts.has("password_max_size"),
        ) {
            (false, false) => self.require(v.is_null(), "password"),
            (true, false) => self.hex(v, "password_size", "password", false),
            (false, true) => {
                let s = v.as_str().ok_or_else(|| self.internal())?;
                self.require(
                    self.size(s.len(), "password_max_size") && valid_string(s),
                    "password",
                )
            }
            (true, true) => Err(self.internal()),
        }
    }
    fn nonce(&self, v: &Value, param: &'static str) -> Result<(), Error> {
        self.require(
            v.as_str().is_some_and(|s| {
                !s.is_empty()
                    && s.len().is_multiple_of(2)
                    && BigInt::from(s.len()) < self.opts.get("nonce_size")
                    && s.bytes().all(|b| b.is_ascii_hexdigit())
            }),
            param,
        )
    }
    fn pow(&self, v: &Value) -> Result<(), Error> {
        let low = self.opts.get("pow_number_min");
        let high = self.opts.get("pow_number_max");
        self.require(
            v.as_array().is_some_and(|a| {
                self.size(a.len(), "pow_size")
                    && a.iter().all(|n| {
                        compare(n, &low).is_some_and(|c| c != Ordering::Less)
                            && compare(n, &high).is_some_and(|c| c != Ordering::Greater)
                    })
            }),
            "pow",
        )
    }
    fn params(
        &self,
        method: &str,
        id: &Value,
        params: &Value,
        internal: bool,
    ) -> Result<(), Error> {
        let (count, param) = match method {
            "configure" => (0, "configure_params"),
            "subscribe" => (4, "subscribe_params"),
            "authorize" => (2, "authorize_params"),
            "submit" => (4, "submit_params"),
            "reconnect" => (3, "reconnect_params"),
            "set_target" => (1, "set_target_params"),
            "notify" => (4, "notify_params"),
            _ => return Err(Error::InvalidMethod(self.id.clone())),
        };
        let p = params.as_array().ok_or_else(|| self.param(param))?;
        if p.len() != count && !(method == "reconnect" && p.is_empty()) {
            return Err(self.param(param));
        }
        let ntf = matches!(method, "set_target" | "notify");
        self.id(id, ntf, ntf)?;
        match method {
            "configure" => Ok(()),
            "subscribe" => {
                self.text(&p[0], "user_agent_max_size", "user_agent", false, true)?;
                self.require(
                    p[0].as_str()
                        .unwrap_or("")
                        .split_once('/')
                        .is_some_and(|(client, version)| {
                            !client.is_empty() && !version.is_empty() && !version.contains('/')
                        }),
                    "user_agent",
                )?;
                self.hex(&p[1], "session_id_size", "session_id", true)?;
                self.text(&p[2], "host_max_size", "host", true, true)?;
                self.num(&p[3], "port_min", "port_max", "port", true, false)
            }
            "authorize" => {
                self.user(&p[0], internal)?;
                self.password(&p[1])
            }
            "submit" => {
                self.user(&p[0], internal)?;
                self.hex(&p[1], "job_id_size", "job_id", false)?;
                self.nonce(&p[2], "miner_nonce")?;
                self.pow(&p[3])
            }
            "reconnect" => {
                if p.is_empty() {
                    return Ok(());
                }
                self.text(&p[0], "host_max_size", "host", true, true)?;
                self.num(&p[1], "port_min", "port_max", "port", true, true)?;
                self.num(
                    &p[2],
                    "wait_time_min",
                    "wait_time_max",
                    "wait_time",
                    false,
                    false,
                )
            }
            "set_target" => self.hex(&p[0], "target_size", "target", false),
            "notify" => {
                self.hex(&p[0], "job_id_size", "job_id", false)?;
                self.num(
                    &p[1],
                    "block_version_min",
                    "block_version_max",
                    "block_version",
                    false,
                    false,
                )?;
                self.hex(&p[2], "block_hash_size", "block_hash", false)?;
                self.require(p[3].is_boolean(), "empty_queue")
            }
            _ => Err(self.internal()),
        }
    }
    fn result(&self, method: &str, v: &Value) -> Result<(), Error> {
        match method {
            "configure" => {
                self.require(v.as_array().is_some_and(Vec::is_empty), "configure_params")
            }
            "subscribe" => {
                let a = v
                    .as_array()
                    .filter(|a| a.len() == 2)
                    .ok_or_else(|| self.param("subscribe_params"))?;
                self.hex(&a[0], "session_id_size", "session_id", true)?;
                self.nonce(&a[1], "extra_nonce")
            }
            "authorize" => self.require(v.is_boolean(), "authorize_params"),
            "submit" => self.require(v.is_boolean(), "submit_params"),
            _ => Err(Error::UnsupportedResponseMethod),
        }
    }
}
fn valid_string(s: &str) -> bool {
    !s.bytes()
        .any(|b| matches!(b, b' ' | b'\n' | b'\t' | 0x0b | 0x0c | b'\r'))
}
fn compare(v: &Value, bound: &BigInt) -> Option<Ordering> {
    if let Some(i) = integer(v) {
        return Some(i.cmp(bound));
    }
    let n = v.as_number()?;
    if !n.as_str().contains(['.', 'e', 'E']) {
        return None;
    }
    let f = n.as_f64().filter(|n| n.is_finite())?;
    let whole = BigInt::from_f64(f)?;
    Some(match whole.cmp(bound) {
        Ordering::Equal => f.fract().partial_cmp(&0.0)?,
        other => other,
    })
}
fn wire_method(method: &str) -> Option<&'static str> {
    Some(match method {
        "configure" => "mining.configure",
        "subscribe" => "mining.subscribe",
        "authorize" => "mining.authorize",
        "submit" => "mining.submit",
        "reconnect" => "client.reconnect",
        "set_target" => "mining.set_target",
        "notify" => "mining.notify",
        _ => return None,
    })
}
fn method(wire: &str) -> Option<&'static str> {
    [
        "configure",
        "subscribe",
        "authorize",
        "submit",
        "reconnect",
        "set_target",
        "notify",
    ]
    .into_iter()
    .find(|m| wire_method(m) == Some(wire))
}
fn params_fields(method: &str) -> &'static [&'static str] {
    match method {
        "configure" => &["params"],
        "subscribe" => &["user_agent", "session_id", "host", "port"],
        "authorize" => &["user", "password"],
        "submit" => &["user", "job_id", "miner_nonce", "pow"],
        "reconnect" => &["host", "port", "wait_time"],
        "set_target" => &["target"],
        "notify" => &["job_id", "block_version", "block_hash", "empty_queue"],
        _ => &[],
    }
}
fn json_error(e: json::Error) -> Error {
    if e == json::Error::Parse {
        Error::Parse
    } else {
        Error::Resource(e)
    }
}
fn checked<'a>(input: &'a Value, o: &Options) -> Result<&'a Map<String, Value>, Error> {
    json::encode(input, o.limits).map_err(json_error)?;
    input
        .as_object()
        .ok_or(Error::InvalidMessage(MaybeId::Undefined))
}
fn check<'a>(m: &Map<String, Value>, o: &'a Options) -> Check<'a> {
    Check {
        opts: o,
        id: maybe_id(m.get("id"), o),
    }
}

pub fn decode(bytes: &[u8], o: &Options) -> Result<Value, Error> {
    let input = json::decode(bytes, o.limits).map_err(json_error)?;
    let id = maybe_id(input.get("id"), o);
    let m = input
        .as_object()
        .ok_or_else(|| Error::InvalidMessage(id.clone()))?;
    let c = Check { opts: o, id };
    if m.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(c.invalid());
    }
    if let (Some(id), Some(method_value), Some(params)) =
        (m.get("id"), m.get("method"), m.get("params"))
        && let Some(method) = method_value.as_str().and_then(method)
    {
        c.fields(m, &["jsonrpc", "id", "method", "params"])?;
        c.params(method, id, params, false)?;
        let mut out = Map::new();
        let ntf = matches!(method, "notify" | "set_target");
        out.insert("type".into(), value!(if ntf { "ntf" } else { "req" }));
        out.insert("method".into(), value!(method));
        out.insert("id".into(), id.clone());
        let mut p = params.as_array().cloned().ok_or_else(|| c.internal())?;
        if method == "configure" {
            out.insert("params".into(), params.clone());
        } else {
            if method == "reconnect" && p.is_empty() {
                p = vec![Value::Null, Value::Null, value!(0)];
            }
            for (key, v) in params_fields(method).iter().zip(p) {
                out.insert((*key).into(), v);
            }
        }
        if matches!(method, "authorize" | "submit") {
            let text = out["user"].as_str().ok_or_else(|| c.internal())?;
            let (a, w) = text.split_once('.').ok_or_else(|| c.internal())?;
            out.insert("user".into(), value!([a, w]));
        }
        for key in [
            "session_id",
            "password",
            "job_id",
            "miner_nonce",
            "target",
            "block_hash",
        ] {
            if let Some(Value::String(s)) = out.get_mut(key) {
                *s = lowercase::lowercase(s, o.limits.string_bytes).map_err(json_error)?;
            }
        }
        return Ok(Value::Object(out));
    }
    if let (Some(id), Some(result), Some(error)) = (m.get("id"), m.get("result"), m.get("error")) {
        let branch = if !result.is_null() && error.is_null() {
            Some(("result", result, false))
        } else if result.is_null() && !error.is_null() {
            Some(("error", error, true))
        } else {
            None
        };
        if let Some((key, v, null)) = branch {
            c.fields(m, &["jsonrpc", "id", "result", "error"])?;
            c.id(id, null, false)?;
            let mut out = Map::new();
            out.insert("type".into(), value!("rsp"));
            out.insert("id".into(), id.clone());
            out.insert(key.into(), v.clone());
            return Ok(Value::Object(out));
        }
    }
    if m.contains_key("id")
        && m.contains_key("method")
        && m.get("params").is_some_and(Value::is_array)
    {
        return Err(Error::InvalidMethod(c.id));
    }
    Err(c.invalid())
}

pub fn encode(input: &Value, o: &Options) -> Result<Vec<u8>, Error> {
    let m = checked(input, o)?;
    let c = check(m, o);
    let kind = m.get("type").and_then(Value::as_str).unwrap_or("");
    let method = m.get("method").and_then(Value::as_str).unwrap_or("");
    let id = m.get("id").unwrap_or(&Value::Null);
    let mut out = value!({"jsonrpc":"2.0","id":id});
    let ntf = matches!(method, "set_target" | "notify");
    if wire_method(method).is_some()
        && ((ntf && kind == "ntf") || (!ntf && kind == "req"))
        && (ntf || m.contains_key("id"))
        && params_fields(method).iter().all(|k| m.contains_key(*k))
    {
        let mut allowed = vec!["type", "method", "id"];
        allowed.extend_from_slice(params_fields(method));
        c.fields(m, &allowed)?;
        let mut p = if method == "configure" {
            m["params"].clone()
        } else {
            Value::Array(
                params_fields(method)
                    .iter()
                    .map(|k| m[*k].clone())
                    .collect(),
            )
        };
        if method == "reconnect" && p == value!([null, null, 0]) {
            p = value!([]);
        }
        c.params(method, if ntf { &Value::Null } else { id }, &p, true)?;
        if matches!(method, "authorize" | "submit") {
            let a = p[0]
                .as_array()
                .filter(|a| a.len() == 2)
                .ok_or_else(|| c.internal())?;
            let user = format!(
                "{}.{}",
                a[0].as_str().ok_or_else(|| c.internal())?,
                a[1].as_str().ok_or_else(|| c.internal())?
            );
            p[0] = value!(user);
        }
        out["method"] = value!(wire_method(method));
        out["params"] = p;
        if ntf {
            out["id"] = Value::Null;
        }
    } else if kind == "rsp"
        && m.contains_key("id")
        && m.contains_key("method")
        && m.contains_key("result")
    {
        c.fields(m, &["type", "method", "id", "result"])?;
        c.id(id, false, false)?;
        c.result(method, &m["result"]).map_err(|e| {
            if e == Error::UnsupportedResponseMethod {
                c.internal()
            } else {
                e
            }
        })?;
        out["result"] = m["result"].clone();
        out["error"] = Value::Null;
    } else if kind == "rsp"
        && m.contains_key("id")
        && m.contains_key("reason")
        && m.contains_key("data")
    {
        c.fields(m, &["type", "method", "id", "reason", "msg", "data"])?;
        c.id(id, true, false)?;
        c.text(&m["data"], "error_data_max_size", "error_data", true, false)?;
        let reason = m["reason"]
            .as_str()
            .and_then(Reason::parse)
            .ok_or_else(|| c.param("error_reason"))?;
        out["result"] = Value::Null;
        out["error"] = value!([reason.code(), reason.message(), m["data"]]);
    } else {
        return Err(c.invalid());
    }
    json::encode(&out, o.limits).map_err(json_error)
}

pub fn validate_response(method: &str, input: &Value, o: &Options) -> Result<Value, Error> {
    if method.len() > 1024 {
        return Err(Error::Resource(json::Error::Limit("method")));
    }
    let m = checked(input, o)?;
    let c = check(m, o);
    let mut out = if let Some(result) = m.get("result") {
        c.result(method, result)?;
        m.clone()
    } else if let Some(error) = m.get("error") {
        let e = error
            .as_array()
            .filter(|e| e.len() == 3)
            .ok_or_else(|| c.param("error_params"))?;
        let reason = integer(&e[0])
            .and_then(|n| n.to_i64())
            .and_then(Reason::from_code)
            .ok_or_else(|| c.param("error_code"))?;
        c.text(&e[1], "error_msg_max_size", "error_msg", false, false)?;
        c.text(&e[2], "error_data_max_size", "error_data", true, false)?;
        let mut out = m.clone();
        out.remove("error");
        out.insert("reason".into(), value!(reason.name()));
        out.insert("msg".into(), e[1].clone());
        out.insert("data".into(), e[2].clone());
        out
    } else {
        return Err(c.invalid());
    };
    out.insert("method".into(), value!(method));
    Ok(Value::Object(out))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Parse,
    InvalidMessage,
    InvalidMethod,
    InvalidParam,
    Internal,
    Unknown,
    JobNotFound,
    DuplicateShare,
    LowDifficultyShare,
    UnauthorizedWorker,
    NotSubscribed,
}
const REASONS: [(Reason, i64, &str, &str); 11] = [
    (Reason::Parse, -32700, "parse_error", "Parse error"),
    (
        Reason::InvalidMessage,
        -32000,
        "invalid_msg",
        "Invalid request",
    ),
    (
        Reason::InvalidMethod,
        -32601,
        "invalid_method",
        "Method not found",
    ),
    (
        Reason::InvalidParam,
        -32602,
        "invalid_param",
        "Invalid params",
    ),
    (Reason::Internal, -32603, "internal_error", "Internal error"),
    (Reason::Unknown, 20, "unknown_error", "Other/Unknown"),
    (Reason::JobNotFound, 21, "job_not_found", "Job not found"),
    (
        Reason::DuplicateShare,
        22,
        "duplicate_share",
        "Duplicate share",
    ),
    (
        Reason::LowDifficultyShare,
        23,
        "low_difficulty_share",
        "Low difficulty share",
    ),
    (
        Reason::UnauthorizedWorker,
        24,
        "unauthorized_worker",
        "Unauthorized worker",
    ),
    (
        Reason::NotSubscribed,
        25,
        "not_subscribed",
        "Not subscribed",
    ),
];
impl Reason {
    pub fn parse(s: &str) -> Option<Self> {
        REASONS.iter().find(|r| r.2 == s).map(|r| r.0)
    }
    pub fn from_code(n: i64) -> Option<Self> {
        REASONS.iter().find(|r| r.1 == n).map(|r| r.0)
    }
    pub fn code(self) -> i64 {
        REASONS.iter().find(|r| r.0 == self).map_or(20, |r| r.1)
    }
    pub fn name(self) -> &'static str {
        REASONS
            .iter()
            .find(|r| r.0 == self)
            .map_or("unknown_error", |r| r.2)
    }
    pub fn message(self) -> &'static str {
        REASONS
            .iter()
            .find(|r| r.0 == self)
            .map_or("Other/Unknown", |r| r.3)
    }
}

pub fn error_response(error: &Error, o: &Options) -> Result<Value, Error> {
    let (reason, id, data) = match error {
        Error::Parse => (Reason::Parse, MaybeId::Null, None),
        Error::InvalidMessage(id) => (Reason::InvalidMessage, id.clone(), None),
        Error::InvalidMethod(id) => (Reason::InvalidMethod, id.clone(), None),
        Error::InvalidParam(param, id) => (Reason::InvalidParam, id.clone(), Some(*param)),
        Error::Internal(id) => (Reason::Internal, id.clone(), None),
        other => return Err(other.clone()),
    };
    let id = match id {
        MaybeId::Integer(n) => number(&n)?,
        _ => Value::Null,
    };
    let id = to_id(&id, o)
        .map(|n| number(&n))
        .transpose()?
        .unwrap_or(Value::Null);
    Ok(value!({"type":"rsp","method":"undefined","id":id,"reason":reason.name(),"data":data}))
}
pub fn share_response(id: &BigInt, response: crate::share::Response) -> Result<Value, Error> {
    use crate::share::{Rejection, Response};
    let id = number(id)?;
    Ok(match response {
        Response::Result(result) => {
            value!({"type":"rsp","method":"submit","id":id,"result":result})
        }
        Response::Error(rejection, data) => {
            let reason = match rejection {
                Rejection::UnauthorizedWorker => Reason::UnauthorizedWorker,
                Rejection::DuplicateShare => Reason::DuplicateShare,
                Rejection::LowDifficultyShare => Reason::LowDifficultyShare,
                Rejection::JobNotFound => Reason::JobNotFound,
                Rejection::UnknownError => Reason::Unknown,
            };
            value!({"type":"rsp","method":"submit","id":id,"reason":reason.name(),"data":data})
        }
    })
}
// Canonical u32 domain conversion only, not submission validation. Sessions
// retain the wire numbers using Proof::from_json and submit that complete share
// to the work engine, preserving its earlier errors and arithmetic failures.
pub fn proof(v: &Value) -> Option<[u32; 42]> {
    let a = v.as_array()?;
    if a.len() != 42 {
        return None;
    }
    let mut proof = [0; 42];
    for (out, v) in proof.iter_mut().zip(a) {
        *out = integer(v)?.to_u32()?;
    }
    Some(proof)
}
