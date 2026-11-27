use super::{Config, Headers, Response, Socket};
use crate::http::post::Body;
use std::time::Duration;
use tokio::io::AsyncReadExt;

pub(super) fn validate_headers(headers: &Headers) -> Result<(), Response> {
    if !headers
        .get("content-type")
        .is_some_and(|v| content_type(v.as_bytes()))
    {
        return Err(Response::empty(415));
    }
    Ok(())
}
fn token(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}
fn word<'a>(bytes: &mut &'a [u8]) -> &'a [u8] {
    let n = bytes.iter().take_while(|b| token(**b)).count();
    let (head, tail) = bytes.split_at(n);
    *bytes = tail;
    head
}
fn space(bytes: &mut &[u8]) {
    *bytes = bytes.trim_ascii_start();
}
fn content_type(mut bytes: &[u8]) -> bool {
    space(&mut bytes);
    if !word(&mut bytes).eq_ignore_ascii_case(b"application") || !bytes.starts_with(b"/") {
        return false;
    }
    bytes = &bytes[1..];
    if !word(&mut bytes).eq_ignore_ascii_case(b"json") {
        return false;
    }
    loop {
        space(&mut bytes);
        if bytes.is_empty() {
            return true;
        }
        if !bytes.starts_with(b";") {
            return false;
        }
        bytes = &bytes[1..];
        space(&mut bytes);
        if word(&mut bytes).is_empty() {
            return false;
        }
        space(&mut bytes);
        if !bytes.starts_with(b"=") {
            return false;
        }
        bytes = &bytes[1..];
        space(&mut bytes);
        if bytes.starts_with(b"\"") {
            bytes = &bytes[1..];
            loop {
                let Some((&b, tail)) = bytes.split_first() else {
                    return false;
                };
                bytes = tail;
                if b == b'"' {
                    break;
                }
                if b == b'\\' {
                    let Some((&escaped, tail)) = bytes.split_first() else {
                        return false;
                    };
                    if escaped != b'\t' && (escaped < 32 || escaped == 127) {
                        return false;
                    }
                    bytes = tail;
                } else if b != b'\t' && (b < 32 || b == 127) {
                    return false;
                }
            }
        } else if word(&mut bytes).is_empty() {
            return false;
        }
    }
}
pub(super) struct Input {
    bytes: Vec<u8>,
    at: usize,
    remaining: Option<u64>,
    complete: bool,
    valid: bool,
}
impl Input {
    pub fn new(headers: &Headers, bytes: Vec<u8>) -> Self {
        let remaining = if headers.contains_key("transfer-encoding") {
            None
        } else {
            Some(
                headers
                    .get("content-length")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0),
            )
        };
        Self {
            bytes,
            at: 0,
            complete: remaining == Some(0),
            remaining,
            valid: true,
        }
    }
    pub fn leftover(mut self) -> Vec<u8> {
        self.bytes.drain(..self.at);
        self.bytes
    }
    pub async fn monitor(&mut self, stream: &mut Socket, maximum: usize) {
        // Retain pipelined bytes while execution is in flight. A full bounded
        // buffer applies TCP backpressure; a legal write half-close is not cancellation.
        self.bytes.drain(..self.at);
        self.at = 0;
        let mut buffer = [0; 2048];
        loop {
            let count = maximum.saturating_sub(self.bytes.len()).min(buffer.len());
            if count == 0 {
                std::future::pending::<()>().await;
            }
            match stream.read(&mut buffer[..count]).await {
                Err(_) => return,
                Ok(0) => std::future::pending::<()>().await,
                Ok(n) => self.bytes.extend_from_slice(&buffer[..n]),
            }
        }
    }
    pub async fn finish(&mut self, stream: &mut Socket, config: &Config) -> bool {
        if !self.valid {
            return false;
        }
        if self.complete {
            return true;
        }
        if let Some(left) = self.remaining {
            let n = left.min((self.bytes.len() - self.at) as u64);
            self.at += n as usize;
            self.remaining = Some(left - n);
            if left == n {
                self.complete = true;
                return true;
            }
            if num_bigint::BigInt::from(left - n) > config.protocol.max_skip_body_length {
                return false;
            }
            let mut reader = Reader {
                stream,
                input: self,
                output: vec![],
                maximum: usize::MAX,
                metadata: config.header_bytes,
                discard: true,
                buffered_only: false,
            };
            while reader.input.remaining != Some(0) {
                let count = reader.input.remaining.unwrap().min(8192) as usize;
                if reader.data(count).await.is_err() {
                    return false;
                }
            }
            self.complete = true;
            true
        } else {
            // Cowboy cannot skip an unknown-length body after its handler
            // terminates. An already buffered, complete chunk stream is reusable.
            let mut reader = Reader {
                stream,
                input: self,
                output: vec![],
                maximum: usize::MAX,
                metadata: config.header_bytes,
                discard: true,
                buffered_only: true,
            };
            matches!(reader.chunked().await, Ok(true))
        }
    }
}
struct Reader<'a> {
    stream: &'a mut Socket,
    input: &'a mut Input,
    output: Vec<u8>,
    maximum: usize,
    metadata: usize,
    discard: bool,
    buffered_only: bool,
}
impl Reader<'_> {
    async fn fill(&mut self) -> Result<(), Response> {
        if self.input.at == self.input.bytes.len() {
            if self.buffered_only {
                return Err(Response::empty(400));
            }
            self.input.bytes.clear();
            self.input.at = 0;
            let mut buffer = [0; 8192];
            let n = self
                .stream
                .read(&mut buffer)
                .await
                .map_err(|_| Response::empty(400))?;
            if n == 0 {
                return Err(Response::empty(400));
            }
            self.input.bytes.extend_from_slice(&buffer[..n]);
        }
        Ok(())
    }
    async fn byte(&mut self) -> Result<u8, Response> {
        self.fill().await?;
        let b = self.input.bytes[self.input.at];
        self.input.at += 1;
        Ok(b)
    }
    async fn line(&mut self) -> Result<Vec<u8>, Response> {
        let mut line = vec![];
        loop {
            self.metadata = self
                .metadata
                .checked_sub(1)
                .ok_or_else(|| Response::empty(431))?;
            let b = self.byte().await?;
            if b == b'\n' {
                if line.pop() != Some(b'\r') {
                    return Err(Response::empty(400));
                }
                return Ok(line);
            }
            if b < 32 && !matches!(b, b'\t' | b'\r') {
                return Err(Response::empty(400));
            }
            line.push(b);
        }
    }
    async fn data(&mut self, mut count: usize) -> Result<(), Response> {
        if count > self.maximum.saturating_sub(self.output.len()) {
            return Err(Response::empty(413));
        }
        while count > 0 {
            self.fill().await?;
            let n = count.min(self.input.bytes.len() - self.input.at);
            if !self.discard {
                self.output
                    .extend_from_slice(&self.input.bytes[self.input.at..self.input.at + n]);
            }
            if let Some(left) = &mut self.input.remaining {
                *left -= n as u64;
            }
            self.input.at += n;
            count -= n;
        }
        Ok(())
    }
    async fn chunked(&mut self) -> Result<bool, Response> {
        loop {
            let line = self.line().await?;
            let size = line.split(|b| *b == b';').next().unwrap_or_default();
            if size.is_empty() || !size.iter().all(u8::is_ascii_hexdigit) {
                // The pinned transfer decoder leaves an invalid size prefix
                // unfinished; read_body reports more at its period boundary.
                // Reject that unrecoverable prefix without occupying the slot
                // for the remaining period, preserving the native error body.
                return Ok(false);
            }
            let size = size
                .iter()
                .try_fold(0usize, |n, b| {
                    n.checked_mul(16)?
                        .checked_add(char::from(*b).to_digit(16)? as usize)
                })
                .ok_or_else(|| Response::empty(413))?;
            if size == 0 {
                loop {
                    let trailer = self.line().await?;
                    if trailer.is_empty() {
                        self.input.complete = true;
                        return Ok(true);
                    }
                    let split = trailer
                        .iter()
                        .position(|b| *b == b':')
                        .ok_or_else(|| Response::empty(400))?;
                    let name = &trailer[..split];
                    if name.is_empty()
                        || !name
                            .iter()
                            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(b))
                        || [b"content-length".as_slice(), b"transfer-encoding", b"host"]
                            .iter()
                            .any(|n| name.eq_ignore_ascii_case(n))
                    {
                        return Err(Response::empty(400));
                    }
                }
            }
            self.data(size).await?;
            if self.byte().await? != b'\r' || self.byte().await? != b'\n' {
                return Err(Response::empty(400));
            }
        }
    }
}
pub(super) async fn read(
    stream: &mut Socket,
    input: &mut Input,
    config: &Config,
) -> Result<Body, Response> {
    let mut reader = Reader {
        stream,
        input,
        output: vec![],
        maximum: config.body_bytes,
        metadata: config.header_bytes,
        discard: false,
        buffered_only: false,
    };
    let result = tokio::time::timeout(config.timeout.min(Duration::from_secs(10)), async {
        if let Some(length) = reader.input.remaining {
            let length = usize::try_from(length).map_err(|_| Response::empty(413))?;
            reader.data(length).await?;
            reader.input.complete = true;
            Ok(true)
        } else {
            reader.chunked().await
        }
    })
    .await;
    match result {
        Ok(Ok(true)) => Ok(Body::Complete(reader.output)),
        Ok(Ok(false)) => {
            reader.input.valid = false;
            Ok(Body::More(reader.output))
        }
        Ok(Err(response)) => {
            reader.input.valid = false;
            Err(response)
        }
        Err(_) => {
            if reader.input.remaining.is_none() {
                reader.input.valid = false;
            }
            Ok(Body::More(reader.output))
        }
    }
}
