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
                push(out, &float(n.as_f64().ok_or(Error::Encoding)?))
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
// OTP 26 ryu/d2s.c:to_chars chooses fixed/scientific spelling from the
// decimal exponent and significant digit count, with a special 2^53 bound.
fn float(value: f64) -> String {
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let raw = format!("{:?}", value.abs());
    let (mantissa, exponent) = raw.split_once('e').map_or((raw.as_str(), 0), |(m, e)| {
        (m, e.parse::<i32>().expect("float exponent"))
    });
    let decimal = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let mut point = decimal + exponent;
    while digits.starts_with('0') && digits.len() > 1 {
        digits.remove(0);
        point -= 1;
    }
    while digits.ends_with('0') && digits.len() > 1 {
        digits.pop();
    }
    if digits == "0" {
        return format!("{sign}0.0");
    }
    let fixed = if point <= 0 {
        format!("0.{}{}", "0".repeat((-point) as usize), digits)
    } else if point as usize >= digits.len() {
        format!("{}{}.0", digits, "0".repeat(point as usize - digits.len()))
    } else {
        let (a, b) = digits.split_at(point as usize);
        format!("{a}.{b}")
    };
    let tail = if digits.len() == 1 { "0" } else { &digits[1..] };
    let scientific = format!("{}.{tail}e{}", &digits[..1], point - 1);
    let exponent = point - digits.len() as i32;
    let lower = if digits.len() == 1 {
        -4
    } else {
        -(digits.len() as i32 + 2)
    };
    let upper = if digits.len() == 1 || point > 10 {
        2
    } else {
        1
    };
    let mantissa: u64 = digits.parse().expect("at most seventeen decimal digits");
    let large_integer = (exponent == 0 && mantissa >= 1 << 53)
        || (exponent == 1 && mantissa > (1 << 52) / 5)
        || (exponent == 2 && mantissa > (1 << 51) / 25);
    format!(
        "{sign}{}",
        if (lower..=upper).contains(&exponent) && !large_integer {
            fixed
        } else {
            scientific
        }
    )
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
