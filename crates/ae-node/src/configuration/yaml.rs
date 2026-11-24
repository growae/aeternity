use super::{Error, MAX_BYTES, MAX_DEPTH, MAX_NODES, MAX_NUMBER, budget};
use num_bigint::BigInt;
use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;
use yaml_rust2::{
    parser::{Event, Parser, Tag},
    scanner::TScalarStyle,
};

pub(super) fn parse(bytes: &[u8]) -> Result<Value, Error> {
    let text = decode(bytes)?;
    let mut legacy = false;
    for line in text.lines().filter(|l| l.starts_with("%YAML ")) {
        match line.split_whitespace().nth(1) {
            Some("1.1") => legacy = true,
            Some("1.2") => legacy = false,
            _ => return Err(Error::Unsupported("YAML version")),
        }
    }
    let mut reader = Reader {
        parser: Parser::new_from_str(&text),
        anchors: BTreeMap::new(),
        pending: Vec::new(),
        legacy,
        nodes: 0,
        bytes: 0,
    };
    if reader.event()? != Event::StreamStart || reader.event()? != Event::DocumentStart {
        return Err(Error::Parse);
    }
    let value = reader.node(0)?;
    if reader.event()? != Event::DocumentEnd || reader.event()? != Event::StreamEnd {
        return Err(Error::Parse);
    }
    Ok(value)
}
struct Reader<'a> {
    parser: Parser<std::str::Chars<'a>>,
    anchors: BTreeMap<usize, Value>,
    pending: Vec<usize>,
    legacy: bool,
    nodes: usize,
    bytes: usize,
}
impl Reader<'_> {
    fn event(&mut self) -> Result<Event, Error> {
        self.parser
            .next_token()
            .map(|e| e.0)
            .map_err(|_| Error::Parse)
    }
    fn charge(&mut self, nodes: usize, bytes: usize) -> Result<(), Error> {
        self.nodes = self
            .nodes
            .checked_add(nodes)
            .filter(|n| *n <= MAX_NODES)
            .ok_or(Error::Limit("YAML nodes"))?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|b| *b <= MAX_BYTES)
            .ok_or(Error::Limit("YAML expansion"))?;
        Ok(())
    }
    // Pinned Yamerl 0.7 emits collection anchors before the next scalar,
    // including after an empty collection. Preserve that observable behavior.
    fn bind(&mut self, value: &Value) -> Result<(), Error> {
        for anchor in std::mem::take(&mut self.pending) {
            let (nodes, bytes, _) = budget(value)?;
            self.charge(nodes, bytes)?;
            self.anchors.insert(anchor, value.clone());
        }
        Ok(())
    }
    fn node(&mut self, depth: usize) -> Result<Value, Error> {
        if depth > MAX_DEPTH {
            return Err(Error::Limit("depth"));
        }
        self.charge(1, 8)?;
        let event = self.event()?;
        match &event {
            Event::Scalar(_, _, id, _)
            | Event::MappingStart(id, _)
            | Event::SequenceStart(id, _)
                if *id != 0 =>
            {
                self.pending.push(*id)
            }
            _ => {}
        }
        let value = match event {
            Event::Scalar(text, style, _, tag) => {
                self.charge(0, text.len())?;
                let value = scalar(text, style, tag, self.legacy)?;
                self.bind(&value)?;
                value
            }
            Event::MappingStart(_, tag) => {
                collection_tag(tag, "map")?;
                let mut map = Map::new();
                while self.parser.peek().map_err(|_| Error::Parse)?.0 != Event::MappingEnd {
                    let Value::String(key) = self.node(depth + 1)? else {
                        return Err(Error::Unsupported("non-string YAML key"));
                    };
                    let value = self.node(depth + 1)?;
                    map.insert(key, value);
                }
                self.event()?;
                Value::Object(map)
            }
            Event::SequenceStart(_, tag) => {
                collection_tag(tag, "seq")?;
                let mut array = Vec::new();
                while self.parser.peek().map_err(|_| Error::Parse)?.0 != Event::SequenceEnd {
                    array.push(self.node(depth + 1)?);
                }
                self.event()?;
                Value::Array(array)
            }
            Event::Alias(anchor) => {
                let value = self.anchors.get(&anchor).ok_or(Error::Parse)?;
                let (nodes, bytes, height) = budget(value)?;
                if depth + height > MAX_DEPTH {
                    return Err(Error::Limit("depth"));
                }
                self.charge(nodes, bytes)?;
                let value = self.anchors[&anchor].clone();
                self.bind(&value)?;
                return Ok(value);
            }
            _ => return Err(Error::Parse),
        };
        Ok(value)
    }
}
fn collection_tag(tag: Option<Tag>, expected: &str) -> Result<(), Error> {
    if let Some(tag) = tag
        && (tag.handle != "tag:yaml.org,2002:" || tag.suffix != expected)
    {
        return Err(Error::Unsupported("YAML tag"));
    }
    Ok(())
}
fn scalar(
    text: String,
    style: TScalarStyle,
    tag: Option<Tag>,
    legacy: bool,
) -> Result<Value, Error> {
    let explicit = if let Some(tag) = &tag {
        if tag.handle != "tag:yaml.org,2002:" {
            return Err(Error::Unsupported("YAML tag"));
        }
        Some(tag.suffix.as_str())
    } else {
        None
    };
    if explicit == Some("str") || (explicit.is_none() && style != TScalarStyle::Plain) {
        return Ok(Value::String(text));
    }
    let null = matches!(text.as_str(), "" | "~" | "null" | "Null" | "NULL");
    let boolean = match text.as_str() {
        "y" | "Y" | "yes" | "Yes" | "YES" | "on" | "On" | "ON" if legacy => Some(true),
        "n" | "N" | "no" | "No" | "NO" | "off" | "Off" | "OFF" if legacy => Some(false),
        "true" | "True" | "TRUE" => Some(true),
        "false" | "False" | "FALSE" => Some(false),
        _ => None,
    };
    if explicit == Some("null") {
        return if null {
            Ok(Value::Null)
        } else {
            Err(Error::Parse)
        };
    }
    if explicit == Some("bool") {
        return boolean.map(Value::Bool).ok_or(Error::Parse);
    }
    if explicit == Some("int") {
        return native_integer(&text, legacy)?.ok_or(Error::Parse);
    }
    if explicit == Some("float") {
        return (if legacy {
            legacy_float(&text)?
        } else {
            float(&text)?
        })
        .ok_or(Error::Parse);
    }
    if explicit.is_some() {
        return Err(Error::Unsupported("YAML scalar tag"));
    }
    if null {
        return Ok(Value::Null);
    }
    if let Some(b) = boolean {
        return Ok(Value::Bool(b));
    }
    if let Some(i) = native_integer(&text, legacy)? {
        return Ok(i);
    }
    if let Some(f) = if legacy {
        legacy_float(&text)?
    } else {
        float(&text)?
    } {
        return Ok(f);
    }
    Ok(Value::String(text))
}
fn native_integer(text: &str, legacy: bool) -> Result<Option<Value>, Error> {
    let (negative, rest) = if let Some(s) = text.strip_prefix('-') {
        (true, s)
    } else {
        (false, text.strip_prefix('+').unwrap_or(text))
    };
    if rest.starts_with(['+', '-']) {
        return Ok(None);
    }
    let parsed = if legacy {
        legacy_integer(rest)?
    } else {
        let (radix, digits) = if let Some(s) = rest.strip_prefix("0x") {
            (16, s)
        } else if let Some(s) = rest.strip_prefix("0o") {
            (8, s)
        } else {
            (10, rest)
        };
        integer(digits, radix)?
    };
    let Some(value) = parsed else { return Ok(None) };
    if negative {
        let n: BigInt = value
            .as_number()
            .ok_or(Error::Parse)?
            .as_str()
            .parse()
            .map_err(|_| Error::Parse)?;
        Ok(Some(Value::Number(
            (-n).to_string().parse().map_err(|_| Error::Parse)?,
        )))
    } else {
        Ok(Some(value))
    }
}
pub(super) fn integer(text: &str, radix: u32) -> Result<Option<Value>, Error> {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if !digits.bytes().all(|c| (c as char).is_digit(radix)) {
        return Ok(None);
    }
    if digits.len() > MAX_NUMBER {
        return Err(Error::Limit("number digits"));
    }
    let n = if digits.is_empty() {
        BigInt::from(0)
    } else {
        BigInt::parse_bytes(text.as_bytes(), radix).ok_or(Error::Parse)?
    };
    Ok(Some(Value::Number(
        n.to_string().parse().map_err(|_| Error::Parse)?,
    )))
}
fn float(text: &str) -> Result<Option<Value>, Error> {
    // Native core floats use a finite decimal/exponent grammar; infinities
    // and NaN are Erlang atoms and have no JSON configuration representation.
    if matches!(
        text.trim_start_matches(['+', '-']),
        ".nan" | ".NaN" | ".NAN" | ".inf" | ".Inf" | ".INF"
    ) {
        return Err(Error::Unsupported("non-finite YAML scalar"));
    }
    let s = text.trim_start_matches(['+', '-']);
    let (mantissa, exponent) = s
        .split_once(['e', 'E'])
        .map_or((s, None), |(a, b)| (a, Some(b)));
    let (whole, fraction) = mantissa
        .split_once('.')
        .map_or((mantissa, None), |(a, b)| (a, Some(b)));
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if !digits(whole)
        || fraction.is_some_and(|f| !digits(f))
        || (whole.is_empty() && fraction.is_none_or(str::is_empty))
        || exponent.is_some_and(|e| {
            let e = e.strip_prefix(['+', '-']).unwrap_or(e);
            e.is_empty() || !digits(e)
        })
    {
        return Ok(None);
    }
    if text.len() > MAX_NUMBER {
        return Err(Error::Limit("number digits"));
    }
    let Ok(n) = text.parse::<f64>() else {
        return Ok(None);
    };
    Ok(Number::from_f64(n).map(Value::Number))
}
fn decode(bytes: &[u8]) -> Result<String, Error> {
    let (unit, little, skip) = if bytes.starts_with(&[0, 0, 254, 255]) {
        (4, false, 4)
    } else if bytes.starts_with(&[255, 254, 0, 0]) {
        (4, true, 4)
    } else if bytes.starts_with(&[254, 255]) {
        (2, false, 2)
    } else if bytes.starts_with(&[255, 254]) {
        (2, true, 2)
    } else if bytes.len() >= 4 && bytes[0..3] == [0, 0, 0] {
        (4, false, 0)
    } else if bytes.len() >= 4 && bytes[1..4] == [0, 0, 0] {
        (4, true, 0)
    } else if bytes.len() >= 2 && bytes[0] == 0 {
        (2, false, 0)
    } else if bytes.len() >= 2 && bytes[1] == 0 {
        (2, true, 0)
    } else {
        (
            1,
            false,
            usize::from(bytes.starts_with(&[239, 187, 191])) * 3,
        )
    };
    let data = &bytes[skip..];
    let result: String = match unit {
        1 => std::str::from_utf8(data)
            .map_err(|_| Error::Encoding)?
            .into(),
        2 if data.len().is_multiple_of(2) => {
            let words = data.chunks_exact(2).map(|a| {
                if little {
                    u16::from_le_bytes([a[0], a[1]])
                } else {
                    u16::from_be_bytes([a[0], a[1]])
                }
            });
            char::decode_utf16(words)
                .collect::<Result<String, _>>()
                .map_err(|_| Error::Encoding)?
        }
        4 if data.len().is_multiple_of(4) => data
            .chunks_exact(4)
            .map(|a| {
                let b = [a[0], a[1], a[2], a[3]];
                char::from_u32(if little {
                    u32::from_le_bytes(b)
                } else {
                    u32::from_be_bytes(b)
                })
                .ok_or(Error::Encoding)
            })
            .collect::<Result<String, _>>()?,
        _ => return Err(Error::Encoding),
    };
    if result.len() > MAX_BYTES {
        return Err(Error::Limit("decoded bytes"));
    }
    Ok(result)
}

fn legacy_integer(text: &str) -> Result<Option<Value>, Error> {
    if text.contains(':') {
        if !text.starts_with('0')
            && let Some(n) = sexagesimal(text)?
        {
            return Ok(Some(Value::Number(
                n.to_string().parse().map_err(|_| Error::Parse)?,
            )));
        }
        return Ok(None);
    }
    let (radix, digits) = if let Some(s) = text.strip_prefix("0b") {
        (2, s)
    } else if let Some(s) = text.strip_prefix("0x") {
        (16, s)
    } else if let Some(s) = text.strip_prefix('0') {
        (8, s)
    } else {
        (10, text)
    };
    integer(&digits.replace('_', ""), radix)
}
fn sexagesimal(text: &str) -> Result<Option<BigInt>, Error> {
    let mut parts = text.split(':');
    let first = parts.next().ok_or(Error::Parse)?;
    if first.is_empty()
        || !first.as_bytes()[0].is_ascii_digit()
        || !first.bytes().all(|b| b.is_ascii_digit() || b == b'_')
    {
        return Ok(None);
    }
    let clean = first.replace('_', "");
    if clean.len() > MAX_NUMBER {
        return Err(Error::Limit("number digits"));
    }
    let Some(mut n) = BigInt::parse_bytes(clean.as_bytes(), 10) else {
        return Ok(None);
    };
    for p in parts {
        if p.is_empty() || p.len() > 2 || !p.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(None);
        }
        let v: u8 = p.parse().map_err(|_| Error::Parse)?;
        if v >= 60 {
            return Ok(None);
        }
        n = n * 60 + v;
        if n.bits() > 13606 {
            return Err(Error::Limit("number digits"));
        }
    }
    Ok(Some(n))
}
fn legacy_float(text: &str) -> Result<Option<Value>, Error> {
    if text.trim_start_matches(['+', '-']).starts_with(".inf")
        || text.trim_start_matches(['+', '-']).starts_with(".nan")
    {
        return float(text);
    }
    let rest = text.strip_prefix(['+', '-']).unwrap_or(text);
    if rest == "0" {
        return float(text);
    }
    let Some((whole, tail)) = rest.split_once('.') else {
        return Ok(None);
    };
    if whole.contains(':') {
        if !tail.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(None);
        }
        let Some(n) = sexagesimal(whole)? else {
            return Ok(None);
        };
        let sign = if text.starts_with('-') { "-" } else { "" };
        return float(&format!("{sign}{n}.{tail}"));
    }
    if let Some((_, exp)) = tail.split_once(['e', 'E'])
        && !exp.starts_with(['+', '-'])
    {
        return Ok(None);
    }
    float(text)
}
