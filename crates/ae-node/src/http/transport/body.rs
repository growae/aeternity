use super::{Config, Headers, Response, TcpStream};
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
struct Reader<'a> {
    stream: &'a mut TcpStream,
    input: Vec<u8>,
    at: usize,
    output: Vec<u8>,
    maximum: usize,
    metadata: usize,
}
impl Reader<'_> {
    async fn fill(&mut self) -> Result<(), Response> {
        if self.at == self.input.len() {
            self.input.clear();
            self.at = 0;
            let mut buffer = [0; 8192];
            let n = self
                .stream
                .read(&mut buffer)
                .await
                .map_err(|_| Response::empty(400))?;
            if n == 0 {
                return Err(Response::empty(400));
            }
            self.input.extend_from_slice(&buffer[..n]);
        }
        Ok(())
    }
    async fn byte(&mut self) -> Result<u8, Response> {
        self.fill().await?;
        let b = self.input[self.at];
        self.at += 1;
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
            let n = count.min(self.input.len() - self.at);
            self.output
                .extend_from_slice(&self.input[self.at..self.at + n]);
            self.at += n;
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
    stream: &mut TcpStream,
    headers: &Headers,
    prefetched: Vec<u8>,
    config: &Config,
) -> Result<Body, Response> {
    let mut reader = Reader {
        stream,
        input: prefetched,
        at: 0,
        output: vec![],
        maximum: config.body_bytes,
        metadata: config.header_bytes,
    };
    let result = tokio::time::timeout(config.timeout.min(Duration::from_secs(10)), async {
        if headers.contains_key("transfer-encoding") {
            reader.chunked().await
        } else {
            let length = headers
                .get("content-length")
                .map_or(Ok(0), |v| v.parse::<usize>())
                .map_err(|_| Response::empty(413))?;
            reader.data(length).await.map(|()| true)
        }
    })
    .await;
    match result {
        Ok(Ok(true)) => Ok(Body::Complete(reader.output)),
        Ok(Ok(false)) => Ok(Body::More(reader.output)),
        Ok(Err(response)) => Err(response),
        // Match the original single read_body period: an incomplete read is
        // reported as body_too_big with the data obtained during that period.
        Err(_) => Ok(Body::More(reader.output)),
    }
}
