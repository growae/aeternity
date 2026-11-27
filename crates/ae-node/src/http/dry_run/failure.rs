// aec_dry_run preparation catch/crash_reason and aehttp_helpers:dry_run_err,
// aeternity fa2a6d7. ISC notice in repository LICENSE.
use ae_vm_fate::{
    Limits,
    diagnostic::{self, Style, Term},
    maps::{Traversal, order},
};
use serde_json::{Value, json};
use std::borrow::Cow;

#[derive(Debug)]
pub enum Error {
    Capacity,
    Number,
    Diagnostic(ae_vm_fate::Error),
}
impl Error {
    pub fn is_capacity(&self) -> bool {
        matches!(
            self,
            Self::Capacity | Self::Diagnostic(ae_vm_fate::Error::ResourceLimit)
        )
    }
}
pub fn bad_call_index<A, C>(error: &ae_execution::dry_run::Error<A, C>) -> Option<usize> {
    match error {
        ae_execution::dry_run::Error::Preparation(ae_execution::dry_run::prepare::Failure {
            index,
            error: ae_execution::dry_run::prepare::Error::BadCallRequest,
        }) => *index,
        _ => None,
    }
}
struct Budget {
    remaining: Limits,
}
impl Budget {
    fn bytes(&mut self, n: usize) -> Result<(), Error> {
        self.remaining.max_bytes = self
            .remaining
            .max_bytes
            .checked_sub(n)
            .ok_or(Error::Capacity)?;
        Ok(())
    }
    fn term<'a>(&mut self, value: &'a Value, depth: usize) -> Result<Term<'a>, Error> {
        if depth > self.remaining.max_depth.min(128) {
            return Err(Error::Capacity);
        }
        self.remaining.max_nodes = self
            .remaining
            .max_nodes
            .checked_sub(1)
            .ok_or(Error::Capacity)?;
        self.bytes(64)?;
        Ok(match value {
            Value::Null => Term::Atom(Cow::Borrowed("null")),
            Value::Bool(v) => Term::Atom(Cow::Borrowed(if *v { "true" } else { "false" })),
            Value::String(v) => {
                self.bytes(v.len())?;
                Term::Binary(Cow::Borrowed(v.as_bytes()))
            }
            Value::Number(v) => {
                let text = v.as_str();
                self.bytes(text.len())?;
                if text.bytes().any(|b| matches!(b, b'.' | b'e' | b'E')) {
                    let n: f64 = text.parse().map_err(|_| Error::Number)?;
                    if !n.is_finite() {
                        return Err(Error::Number);
                    }
                    Term::FloatBits(n.to_bits())
                } else {
                    Term::Integer(Cow::Owned(text.parse().map_err(|_| Error::Number)?))
                }
            }
            Value::Array(values) => {
                if values.len() > self.remaining.max_nodes {
                    return Err(Error::Capacity);
                }
                Term::List(
                    values
                        .iter()
                        .map(|v| self.term(v, depth + 1))
                        .collect::<Result<_, _>>()?,
                )
            }
            Value::Object(map) => {
                if map.len() > self.remaining.max_nodes / 2 {
                    return Err(Error::Capacity);
                }
                let pairs: Vec<_> = map.iter().collect();
                let keys: Vec<_> = pairs.iter().map(|(k, _)| k.as_bytes()).collect();
                for key in &keys {
                    self.bytes(key.len())?
                }
                // io_lib_pretty uses maps:iterator order. Binary-only map keys
                // share a native hash independently of OTP atom/CPU profiles.
                let indices = order::binary_indices(&keys, Traversal::Fold, self.remaining)
                    .map_err(Error::Diagnostic)?;
                let mut terms = Vec::new();
                for i in indices {
                    self.remaining.max_nodes = self
                        .remaining
                        .max_nodes
                        .checked_sub(1)
                        .ok_or(Error::Capacity)?;
                    self.bytes(64)?;
                    let (k, v) = pairs[i];
                    terms.push((
                        Term::Binary(Cow::Borrowed(k.as_bytes())),
                        self.term(v, depth + 1)?,
                    ));
                }
                Term::Map(terms)
            }
        })
    }
}

// A malformed call is rendered from its actual original JSON value. Never
// infer native errors from Rust Debug strings or synthesize Erlang stack frames.
pub fn bad_call(request: &Value, limits: Limits) -> Result<String, Error> {
    let mut budget = Budget { remaining: limits };
    let value = budget.term(request, 0)?;
    let term = Term::Tuple(vec![
        Term::Atom(Cow::Borrowed("bad_dry_run_call_request")),
        value,
    ]);
    let prefix = "Internal error ";
    budget.bytes(prefix.len())?;
    let (bytes, _) = diagnostic::format_with_width(
        &term,
        Style::Pretty,
        -1,
        prefix.len() + 1,
        120,
        budget.remaining,
    )
    .map_err(Error::Diagnostic)?;
    Ok(format!("{prefix}{}", crate::http::text::binary(&bytes)))
}
pub fn selection<E>(error: &ae_chain::dry_run::Error<E>) -> Option<&'static str> {
    match error {
        ae_chain::dry_run::Error::InvalidHash => Some("Invalid hash provided"),
        ae_chain::dry_run::Error::StateGarbageCollected => {
            Some("Block state of given hash was garbage collected")
        }
        _ => None,
    }
}
pub fn admission(error: ae_sync::dry_run::Error) -> Option<&'static str> {
    (error == ae_sync::dry_run::Error::Timeout).then_some("dry-run exceeded time limit")
}
// Phased pipelines distinguish their whole-job service deadline from the
// original core timer. Only this typed native expiry is a native HTTP reason.
pub fn native_admission(error: ae_sync::dry_run::Error) -> Option<&'static str> {
    (error == ae_sync::dry_run::Error::NativeTimeout).then_some("dry-run exceeded time limit")
}
pub fn process<E>(error: &ae_sync::dry_run::process::Error<E>) -> Option<&'static str> {
    use ae_sync::dry_run::process::Error;
    match error {
        Error::Control(e) => native_admission(*e).or_else(|| admission(*e)),
        Error::Exit(status)
            if status.code().and_then(ae_worker_heap::Exit::from_code)
                == Some(ae_worker_heap::Exit::NativeHeap) =>
        {
            Some("dry-run failed: killed")
        }
        _ => None,
    }
}
pub fn worker(failure: &crate::dry_run_worker::Failure) -> Option<&str> {
    (failure.kind == crate::dry_run_worker::Kind::BadCallRequest)
        .then_some(failure.native_reason.as_deref())
        .flatten()
}
pub fn body(reason: &str, maximum: usize) -> Result<(u16, Value), Error> {
    let prefix = "Bad request: ";
    let bytes = prefix
        .len()
        .checked_add(reason.len())
        .and_then(|n| n.checked_mul(6))
        .and_then(|n| n.checked_add(32))
        .ok_or(Error::Capacity)?;
    if bytes > maximum {
        return Err(Error::Capacity);
    }
    Ok((400, json!({"reason":format!("{prefix}{reason}")})))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn unpack(value: &Value, pool: &[Value]) -> Value {
        match value {
            Value::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                unpack(&pool[m["$ref"].as_u64().unwrap() as usize], pool)
            }
            Value::Object(m) => Value::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), unpack(v, pool)))
                    .collect(),
            ),
            Value::Array(a) => Value::Array(a.iter().map(|v| unpack(v, pool)).collect()),
            v => v.clone(),
        }
    }
    #[test]
    fn original_malformed_call_reasons_preserve_json_terms_float_bits_and_map_order() {
        let data: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/erlang_dry_run_failures.json"
        ))
        .unwrap();
        let data = unpack(&data["root"], data["values"].as_array().unwrap());
        let rows = data["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2084);
        for (index, row) in rows.iter().enumerate() {
            let raw = hex::decode(row["raw"].as_str().unwrap()).unwrap();
            let input: Value = serde_json::from_slice(&raw).unwrap();
            let reason = bad_call(&input, Limits::default()).unwrap();
            let expected = hex::decode(row["reason"].as_str().unwrap()).unwrap();
            assert_eq!(reason.as_bytes(), expected, "row {index}: {input}");
            assert_eq!(body(&reason, 1 << 20).unwrap(), (400, row["body"].clone()));
        }
    }
    #[test]
    fn local_limits_and_operational_faults_never_masquerade_as_native_rejections() {
        let call = json!({"calldata":"cb_AAAA","extra":[1,2,3]});
        for limits in [
            Limits {
                max_bytes: 8,
                ..Default::default()
            },
            Limits {
                max_nodes: 1,
                ..Default::default()
            },
            Limits {
                max_depth: 0,
                ..Default::default()
            },
        ] {
            assert!(bad_call(&call, limits).is_err());
        }
        assert!(body("failure", 0).is_err());
        assert!(selection(&ae_chain::dry_run::Error::Source("storage failed")).is_none());
        assert!(
            selection(&ae_chain::dry_run::Error::<()>::Returned(
                ae_chain::dry_run::Returned::StateGarbageCollected
            ))
            .is_none()
        );
        for e in [
            ae_sync::dry_run::Error::Busy,
            ae_sync::dry_run::Error::Stopped,
            ae_sync::dry_run::Error::Cancelled,
        ] {
            assert!(admission(e).is_none())
        }
        let too_large: Value = serde_json::from_str("1e1000").unwrap();
        assert!(bad_call(&too_large, Limits::default()).is_err());
        assert!(bad_call(&call, Limits::default()).is_ok());
    }
}
