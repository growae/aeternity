// The MIT License

// Copyright (c) 2010-2013 Alisdair Sullivan <alisdairsullivan@yahoo.ca>

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// JSX 2.8.0 (3074d486), return_maps and default parsing options.
// Shared by the original node HTTP and Stratum interfaces. These limits are
// explicit Rust service bounds; callers may enforce stricter protocol budgets.
use num_bigint::BigInt;
use serde_json::{Map, Number, Value};
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub bytes: usize,
    pub depth: usize,
    pub values: usize,
    pub string_bytes: usize,
    pub number_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            bytes: 65536,
            depth: 64,
            values: 8192,
            string_bytes: 65536,
            number_bytes: 4096,
        }
    }
}
impl Limits {
    fn check(self) -> Result<Self, Error> {
        if self.bytes > 64 << 20
            || self.depth > 64
            || self.values > 65536
            || self.string_bytes > 64 << 20
            || self.number_bytes > 4096
        {
            return Err(Error::Limits);
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Parse,
    Limit(&'static str),
    Limits,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

pub fn decode(input: &[u8], limits: Limits) -> Result<Value, Error> {
    let limits = limits.check()?;
    if input.len() > limits.bytes {
        return Err(Error::Limit("bytes"));
    }
    let mut parser = Parser {
        input,
        at: usize::from(input.starts_with(b"\xef\xbb\xbf")) * 3,
        limits,
        values: 0,
    };
    let value = parser.value(0)?;
    parser.skip(true)?;
    if parser.at != input.len() {
        return Err(Error::Parse);
    }
    Ok(value)
}

struct Parser<'a> {
    input: &'a [u8],
    at: usize,
    limits: Limits,
    values: usize,
}
impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.input.get(self.at).copied()
    }
    fn take(&mut self, byte: u8) -> bool {
        if self.peek() == Some(byte) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn skip(&mut self, done: bool) -> Result<(), Error> {
        loop {
            while self
                .peek()
                .is_some_and(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
            {
                self.at += 1;
            }
            if !self.input[self.at..].starts_with(b"//")
                && !self.input[self.at..].starts_with(b"/*")
            {
                return Ok(());
            }
            // JSX nests block comments, including ones opened inside a line comment.
            let mut stack = vec![self.input[self.at + 1] == b'/'];
            self.at += 2;
            loop {
                if stack.len() > self.limits.depth {
                    return Err(Error::Limit("depth"));
                }
                let rest = &self.input[self.at..];
                if rest.is_empty() {
                    return if done && stack.len() == 1 {
                        Ok(())
                    } else {
                        Err(Error::Parse)
                    };
                }
                if rest[0] == b'\n' && stack.last() == Some(&true) {
                    self.at += 1;
                    break;
                } else if rest.starts_with(b"/*") {
                    stack.push(false);
                    self.at += 2;
                } else if rest.starts_with(b"*/") && stack.last() == Some(&false) {
                    stack.pop();
                    self.at += 2;
                    if stack.is_empty() {
                        break;
                    }
                } else {
                    // A final slash/star in a block comment is an incomplete token,
                    // even in the otherwise permissive post-value comment state.
                    if rest.len() == 1
                        && stack.last() == Some(&false)
                        && matches!(rest[0], b'/' | b'*')
                    {
                        return Err(Error::Parse);
                    }
                    self.at += 1;
                }
            }
        }
    }
    fn value(&mut self, depth: usize) -> Result<Value, Error> {
        self.skip(false)?;
        self.values += 1;
        if self.values > self.limits.values {
            return Err(Error::Limit("values"));
        }
        match self.peek() {
            Some(b'{' | b'[') if depth >= self.limits.depth => Err(Error::Limit("depth")),
            Some(b'{') => {
                self.at += 1;
                let mut object = Map::new();
                loop {
                    self.skip(false)?;
                    if self.take(b'}') {
                        break;
                    }
                    let key = self.string()?;
                    self.skip(false)?;
                    if !self.take(b':') {
                        return Err(Error::Parse);
                    }
                    let value = self.value(depth + 1)?;
                    object.insert(key, value);
                    self.skip(false)?;
                    if self.take(b'}') {
                        break;
                    }
                    if !self.take(b',') {
                        return Err(Error::Parse);
                    }
                }
                Ok(Value::Object(object))
            }
            Some(b'[') => {
                self.at += 1;
                let mut array = Vec::new();
                loop {
                    self.skip(false)?;
                    if self.take(b']') {
                        break;
                    }
                    array.push(self.value(depth + 1)?);
                    self.skip(false)?;
                    if self.take(b']') {
                        break;
                    }
                    if !self.take(b',') {
                        return Err(Error::Parse);
                    }
                }
                Ok(Value::Array(array))
            }
            Some(b'"' | b'\'') => self.string().map(Value::String),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(b't') => self.literal(b"true", Value::Bool(true)),
            Some(b'f') => self.literal(b"false", Value::Bool(false)),
            Some(b'n') => self.literal(b"null", Value::Null),
            _ => Err(Error::Parse),
        }
    }
    fn literal(&mut self, text: &[u8], value: Value) -> Result<Value, Error> {
        if !self.input[self.at..].starts_with(text) {
            return Err(Error::Parse);
        }
        self.at += text.len();
        Ok(value)
    }
    fn number(&mut self) -> Result<Value, Error> {
        let start = self.at;
        self.take(b'-');
        if !self.take(b'0') {
            if !self.peek().is_some_and(|b| matches!(b, b'1'..=b'9')) {
                return Err(Error::Parse);
            }
            self.digits();
        }
        if self.take(b'.') && self.digits() == 0 {
            return Err(Error::Parse);
        }
        if self.take(b'e') || self.take(b'E') {
            if !self.take(b'+') {
                self.take(b'-');
            }
            if self.digits() == 0 {
                return Err(Error::Parse);
            }
        }
        if self.at - start > self.limits.number_bytes {
            return Err(Error::Limit("number_bytes"));
        }
        let text = std::str::from_utf8(&self.input[start..self.at]).map_err(|_| Error::Parse)?;
        number(text).map(Value::Number)
    }
    fn digits(&mut self) -> usize {
        let start = self.at;
        while self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.at += 1;
        }
        self.at - start
    }
    fn hex4(&self, at: usize) -> Option<u32> {
        let bytes = self.input.get(at..at + 4)?;
        let mut value = 0;
        for &b in bytes {
            value = value * 16 + char::from(b).to_digit(16)?;
        }
        Some(value)
    }
    fn string(&mut self) -> Result<String, Error> {
        let quote = self.peek().ok_or(Error::Parse)?;
        if !matches!(quote, b'"' | b'\'') {
            return Err(Error::Parse);
        }
        self.at += 1;
        let mut out = String::new();
        loop {
            if self.take(quote) {
                return Ok(out);
            }
            let b = self.peek().ok_or(Error::Parse)?;
            if b == b'\\' {
                self.at += 1;
                let b = self.peek().ok_or(Error::Parse)?;
                let escaped = match b {
                    b'b' => Some('\u{8}'),
                    b'f' => Some('\u{c}'),
                    b'n' => Some('\n'),
                    b'r' => Some('\r'),
                    b't' => Some('\t'),
                    b'"' | b'\'' | b'\\' | b'/' => Some(char::from(b)),
                    _ => None,
                };
                if let Some(c) = escaped {
                    out.push(c);
                    self.at += 1;
                } else if b == b'u' && self.hex4(self.at + 1).is_some() {
                    let code = self.hex4(self.at + 1).ok_or(Error::Parse)?;
                    self.at += 5;
                    if (0xd800..=0xdbff).contains(&code) && self.peek() == Some(b'\\') {
                        if self.input.get(self.at + 1) != Some(&b'u') {
                            return Err(Error::Parse);
                        }
                        let second = self.hex4(self.at + 2).ok_or(Error::Parse)?;
                        self.at += 6;
                        if (0xdc00..=0xdfff).contains(&second) {
                            let cp = 0x10000 + (code - 0xd800) * 0x400 + second - 0xdc00;
                            out.push(char::from_u32(cp).ok_or(Error::Parse)?);
                        } else {
                            out.push_str("\u{fffd}\u{fffd}");
                        }
                    } else {
                        out.push(char::from_u32(code).unwrap_or('\u{fffd}'));
                    }
                } else {
                    // Unknown escapes retain the slash; the following byte is
                    // processed normally (including invalid UTF-8 replacement).
                    out.push('\\');
                }
            } else if b < 128 {
                out.push(char::from(b));
                self.at += 1;
            } else {
                out.push(self.utf8()?);
            }
            if out.len() > self.limits.string_bytes {
                return Err(Error::Limit("string_bytes"));
            }
        }
    }
    fn utf8(&mut self) -> Result<char, Error> {
        let rest = &self.input[self.at..];
        let first = rest[0];
        let width = match first {
            0xc0..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf7 => 4,
            _ => 1,
        };
        if let Some(bytes) = rest.get(..width)
            && let Ok(text) = std::str::from_utf8(bytes)
            && let Some(c) = text.chars().next()
        {
            self.at += width;
            return Ok(c);
        }
        // This is the pinned decoder's replacement grouping, which differs
        // from String::from_utf8_lossy for overlong sequences and surrogates.
        if (rest.len() == 1 && first >= 0xc0)
            || (rest.len() == 2 && first >= 0xe0 && rest[1] >= 0x80)
            || (rest.len() == 3 && first >= 0xf0 && rest[1] >= 0x80 && rest[2] >= 0x80)
        {
            return Err(Error::Parse);
        }
        if first == 0xed && rest.len() >= 3 && rest[1] >= 0xa0 {
            self.at += 3;
        } else {
            self.at += 1;
            for _ in 1..width {
                if self.peek().is_some_and(|b| (0x80..=0xbf).contains(&b)) {
                    self.at += 1;
                } else if self.at == self.input.len() {
                    return Err(Error::Parse);
                } else {
                    break;
                }
            }
        }
        Ok('\u{fffd}')
    }
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
