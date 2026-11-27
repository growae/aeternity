use super::*;
use ae_vm_fate::{
    Limits,
    maps::{Traversal, order::binary_indices},
};

pub(super) fn iolist(value: &Value) -> Option<Vec<u8>> {
    fn append(value: &Value, out: &mut Vec<u8>) -> Option<()> {
        match value {
            Value::String(s) => out.extend_from_slice(s.as_bytes()),
            Value::Array(a) => {
                for v in a {
                    match v {
                        Value::Number(n) => out.push(u8::try_from(n.as_u64()?).ok()?),
                        _ => append(v, out)?,
                    }
                }
            }
            _ => return None,
        }
        Some(())
    }
    let mut out = Vec::new();
    append(value, &mut out)?;
    Some(out)
}
pub(super) fn stringify(value: &Value) -> Result<Vec<u8>, Error> {
    budget(value).map_err(|_| Error::Limit)?;
    if let Some(bytes) = iolist(value) {
        return Ok(bytes);
    }
    let mut out = String::new();
    write(value, &mut out, 0)?;
    Ok(out.into_bytes())
}
fn push(out: &mut String, text: &str) -> Result<(), Error> {
    if out
        .len()
        .checked_add(text.len())
        .is_none_or(|n| n > MAX_BYTES)
    {
        return Err(Error::Limit);
    }
    out.push_str(text);
    Ok(())
}
fn write(value: &Value, out: &mut String, depth: usize) -> Result<(), Error> {
    if depth > MAX_DEPTH {
        return Err(Error::Limit);
    }
    match value {
        Value::Null => push(out, "null"),
        Value::Bool(true) => push(out, "true"),
        Value::Bool(false) => push(out, "false"),
        Value::Number(n) => {
            if n.as_str().contains(['.', 'e', 'E']) {
                push(
                    out,
                    &ae_vm_fate::diagnostic::float(n.as_f64().ok_or(Error::Encoding)?.to_bits())
                        .map_err(|_| Error::Encoding)?,
                )
            } else {
                push(out, n.as_str())
            }
        }
        Value::String(s) => {
            push(out, "<<")?;
            for (i, b) in s.bytes().enumerate() {
                if i > 0 {
                    push(out, ",")?;
                }
                push(out, &b.to_string())?;
            }
            push(out, ">>")
        }
        Value::Array(a) => {
            push(out, "[")?;
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    push(out, ",")?;
                }
                write(v, out, depth + 1)?;
            }
            push(out, "]")
        }
        Value::Object(m) => {
            let entries: Vec<_> = m.iter().collect();
            let keys: Vec<_> = entries.iter().map(|(k, _)| k.as_bytes()).collect();
            let order = binary_indices(
                &keys,
                Traversal::Fold,
                Limits {
                    max_nodes: MAX_NODES,
                    max_bytes: MAX_BYTES,
                    max_depth: MAX_DEPTH,
                },
            )
            .map_err(|_| Error::Limit)?;
            push(out, "#{")?;
            for (i, j) in order.into_iter().enumerate() {
                if i > 0 {
                    push(out, ",")?;
                }
                let (k, v) = entries[j];
                write(&Value::String(k.clone()), out, depth + 1)?;
                push(out, " => ")?;
                write(v, out, depth + 1)?;
            }
            push(out, "}")
        }
    }
}
pub(super) fn atom(s: &str) -> String {
    if s.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_@".contains(&c))
    {
        s.into()
    } else {
        format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
    }
}
