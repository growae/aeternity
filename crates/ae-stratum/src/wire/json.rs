// Stratum preserves its original one-MiB hard ceiling and LF framing.
pub use ae_codec::jsx::Limits;
use num_bigint::BigInt;
use serde_json::{Number, Value};
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Parse,
    Limit(&'static str),
    Limits,
    IncompleteFrame,
    Closed,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

impl From<ae_codec::jsx::Error> for Error {
    fn from(error: ae_codec::jsx::Error) -> Self {
        match error {
            ae_codec::jsx::Error::Parse => Self::Parse,
            ae_codec::jsx::Error::Limit(name) => Self::Limit(name),
            ae_codec::jsx::Error::Limits => Self::Limits,
        }
    }
}
fn check(limits: Limits) -> Result<Limits, Error> {
    if limits.bytes > 1_048_576
        || limits.depth > 64
        || limits.values > 65536
        || limits.string_bytes > 1_048_576
        || limits.number_bytes > 4096
    {
        Err(Error::Limits)
    } else {
        Ok(limits)
    }
}
pub fn decode(input: &[u8], limits: Limits) -> Result<Value, Error> {
    ae_codec::jsx::decode(input, check(limits)?).map_err(Into::into)
}

fn number(text: &str) -> Result<Number, Error> {
    if text.contains(['.', 'e', 'E']) {
        let float = text.parse::<f64>().map_err(|_| Error::Parse)?;
        Number::from_f64(float).ok_or(Error::Parse)
    } else {
        let integer = BigInt::from_str(text).map_err(|_| Error::Parse)?;
        Number::from_str(&integer.to_string()).map_err(|_| Error::Parse)
    }
}

// Output is standard JSON plus exactly one LF. Object key order and floating
// number spelling need not match JSX; decoded values and float bits do.
pub fn encode(value: &Value, limits: Limits) -> Result<Vec<u8>, Error> {
    let limits = check(limits)?;
    let mut writer = Writer {
        output: Vec::new(),
        limits,
        values: 0,
    };
    writer.value(value, 0)?;
    writer.push(b"\n")?;
    Ok(writer.output)
}
struct Writer {
    output: Vec<u8>,
    limits: Limits,
    values: usize,
}
impl Writer {
    fn push(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if bytes.len() > self.limits.bytes.saturating_sub(self.output.len()) {
            return Err(Error::Limit("bytes"));
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }
    fn string(&mut self, text: &str) -> Result<(), Error> {
        if text.len() > self.limits.string_bytes {
            return Err(Error::Limit("string_bytes"));
        }
        self.push(b"\"")?;
        for c in text.chars() {
            match c {
                '"' => self.push(b"\\\"")?,
                '\\' => self.push(b"\\\\")?,
                '\n' => self.push(b"\\n")?,
                '\r' => self.push(b"\\r")?,
                '\t' => self.push(b"\\t")?,
                '\u{8}' => self.push(b"\\b")?,
                '\u{c}' => self.push(b"\\f")?,
                c if c < '\u{20}' || matches!(c, '\u{2028}' | '\u{2029}') => {
                    self.push(format!("\\u{:04x}", c as u32).as_bytes())?;
                }
                c => {
                    self.push(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                }
            }
        }
        self.push(b"\"")
    }
    fn value(&mut self, value: &Value, depth: usize) -> Result<(), Error> {
        self.values += 1;
        if self.values > self.limits.values {
            return Err(Error::Limit("values"));
        }
        match value {
            Value::Null => self.push(b"null"),
            Value::Bool(true) => self.push(b"true"),
            Value::Bool(false) => self.push(b"false"),
            Value::String(text) => self.string(text),
            Value::Number(n) => {
                let text = n.as_str();
                if text.len() > self.limits.number_bytes {
                    return Err(Error::Limit("number_bytes"));
                }
                self.push(number(text)?.to_string().as_bytes())
            }
            Value::Array(_) | Value::Object(_) if depth >= self.limits.depth => {
                Err(Error::Limit("depth"))
            }
            Value::Array(values) => {
                self.push(b"[")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        self.push(b",")?;
                    }
                    self.value(v, depth + 1)?;
                }
                self.push(b"]")
            }
            Value::Object(values) => {
                self.push(b"{")?;
                for (i, (key, v)) in values.iter().enumerate() {
                    if i > 0 {
                        self.push(b",")?;
                    }
                    self.string(key)?;
                    self.push(b":")?;
                    self.value(v, depth + 1)?;
                }
                self.push(b"}")
            }
        }
    }
}

// A bounded, pull-based LF framer. At most one frame is returned per call, so
// a read containing many packets cannot create an unbounded queue of values.
pub struct Frames {
    pending: Vec<u8>,
    limits: Limits,
    closed: bool,
}
impl Frames {
    pub fn new(limits: Limits) -> Result<Self, Error> {
        Ok(Self {
            pending: Vec::new(),
            limits: check(limits)?,
            closed: false,
        })
    }
    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }
    pub fn next(&mut self, input: &mut &[u8]) -> Result<Option<Value>, Error> {
        self.next_raw(input)?
            .map(|bytes| decode(&bytes, self.limits))
            .transpose()
    }
    // Preserve raw line bytes for the session decoder, including malformed
    // JSON that must receive the original protocol error response.
    pub fn next_raw(&mut self, input: &mut &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        let available = self.limits.bytes.saturating_sub(self.pending.len());
        let scan = input.len().min(available.saturating_add(1));
        let newline = input[..scan].iter().position(|&b| b == b'\n');
        let count = newline.map_or(input.len(), |n| n + 1);
        if count > self.limits.bytes.saturating_sub(self.pending.len()) {
            self.pending.clear();
            self.closed = true;
            return Err(Error::Limit("bytes"));
        }
        self.pending.extend_from_slice(&input[..count]);
        *input = &input[count..];
        if newline.is_none() {
            return Ok(None);
        }
        Ok(Some(std::mem::take(&mut self.pending)))
    }
    pub fn finish(&mut self) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        self.closed = true;
        if self.pending.is_empty() {
            Ok(())
        } else {
            self.pending.clear();
            Err(Error::IncompleteFrame)
        }
    }
}
