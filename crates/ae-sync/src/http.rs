use std::{future::poll_fn, pin::Pin};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub header_bytes: usize,
    pub body_bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    Http,
    Limit,
    Io(std::io::ErrorKind),
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::OutOfMemory
            || matches!(e.raw_os_error(), Some(12 | 23 | 24 | 55 | 105))
        {
            Self::Limit
        } else {
            Self::Io(e.kind())
        }
    }
}
#[derive(Debug)]
pub(crate) struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}
pub(crate) struct Received {
    pub response: Response,
    pub location: Option<String>,
}
pub(crate) async fn write<S: AsyncWrite + Unpin>(
    stream: &mut S,
    mut bytes: &[u8],
) -> Result<(), Error> {
    while !bytes.is_empty() {
        let n = poll_fn(|cx| Pin::new(&mut *stream).poll_write(cx, bytes)).await?;
        if n == 0 {
            return Err(Error::Io(std::io::ErrorKind::WriteZero));
        }
        bytes = &bytes[n..];
    }
    poll_fn(|cx| Pin::new(&mut *stream).poll_flush(cx)).await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Framing {
    Length(usize),
    Chunked,
    Close,
}
pub(crate) fn headers(raw: &[u8], settings: &Limits) -> Result<(u16, usize, Framing), Error> {
    let mut fields = [httparse::EMPTY_HEADER; 64];
    let mut response = httparse::Response::new(&mut fields);
    let end = match response.parse(raw).map_err(|e| {
        if e == httparse::Error::TooManyHeaders {
            Error::Limit
        } else {
            Error::Http
        }
    })? {
        httparse::Status::Complete(n) if n <= settings.header_bytes => n,
        _ => return Err(Error::Limit),
    };
    let mut length = None;
    let mut chunked = false;
    for h in response.headers.iter() {
        if h.name.eq_ignore_ascii_case("Content-Length") {
            if length.is_some() {
                return Err(Error::Http);
            }
            let value = std::str::from_utf8(h.value)
                .map_err(|_| Error::Http)?
                .trim();
            if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err(Error::Http);
            }
            let n = value.parse::<usize>().map_err(|_| Error::Limit)?;
            if n > settings.body_bytes {
                return Err(Error::Limit);
            }
            length = Some(n);
        } else if h.name.eq_ignore_ascii_case("Transfer-Encoding") {
            if chunked || !h.value.eq_ignore_ascii_case(b"chunked") {
                return Err(Error::Http);
            }
            chunked = true;
        } else if h.name.eq_ignore_ascii_case("Content-Encoding")
            && !h.value.eq_ignore_ascii_case(b"identity")
        {
            return Err(Error::Http);
        }
    }
    if chunked && length.is_some() {
        return Err(Error::Http);
    }
    let framing = if chunked {
        Framing::Chunked
    } else if let Some(n) = length {
        Framing::Length(n)
    } else {
        Framing::Close
    };
    Ok((response.code.ok_or(Error::Http)?, end, framing))
}

#[derive(Default)]
pub(crate) struct Chunks {
    offset: usize,
    next: Option<usize>,
    trailers: bool,
    body: Vec<u8>,
}
impl Chunks {
    pub fn advance(&mut self, raw: &[u8], s: &Limits) -> Result<Option<Vec<u8>>, Error> {
        loop {
            let tail = &raw[self.offset..];
            if self.trailers {
                if tail.starts_with(b"\r\n") {
                    if tail.len() != 2 {
                        return Err(Error::Http);
                    }
                    return Ok(Some(std::mem::take(&mut self.body)));
                }
                if tail.len() > s.header_bytes {
                    return Err(Error::Limit);
                }
                let mut fields = [httparse::EMPTY_HEADER; 64];
                match httparse::parse_headers(tail, &mut fields).map_err(|e| {
                    if e == httparse::Error::TooManyHeaders {
                        Error::Limit
                    } else {
                        Error::Http
                    }
                })? {
                    httparse::Status::Partial => return Ok(None),
                    httparse::Status::Complete((n, h)) => {
                        if n != tail.len()
                            || h.iter().any(|h| {
                                h.name.eq_ignore_ascii_case("Content-Length")
                                    || h.name.eq_ignore_ascii_case("Transfer-Encoding")
                            })
                        {
                            return Err(Error::Http);
                        }
                        return Ok(Some(std::mem::take(&mut self.body)));
                    }
                }
            }
            if let Some(n) = self.next {
                if tail.len() < n + 2 {
                    return Ok(None);
                }
                if &tail[n..n + 2] != b"\r\n" {
                    return Err(Error::Http);
                }
                self.body.extend_from_slice(&tail[..n]);
                self.offset += n + 2;
                self.next = None;
            } else {
                let end = match tail.windows(2).position(|w| w == b"\r\n") {
                    Some(n) if n <= 1024 => n,
                    None if tail.len() <= 1024 => return Ok(None),
                    _ => return Err(Error::Limit),
                };
                let size = tail[..end]
                    .split(|b| *b == b';')
                    .next()
                    .ok_or(Error::Http)?;
                if size.is_empty() || !size.iter().all(|b| b.is_ascii_hexdigit()) {
                    return Err(Error::Http);
                }
                let n =
                    usize::from_str_radix(std::str::from_utf8(size).map_err(|_| Error::Http)?, 16)
                        .map_err(|_| Error::Limit)?;
                if n > s.body_bytes.saturating_sub(self.body.len()) {
                    return Err(Error::Limit);
                }
                self.offset += end + 2;
                if n == 0 {
                    self.trailers = true;
                } else {
                    self.next = Some(n);
                }
            }
        }
    }
}

fn redirect_location(raw: &[u8]) -> Result<Option<String>, Error> {
    let mut fields = [httparse::EMPTY_HEADER; 64];
    let mut response = httparse::Response::new(&mut fields);
    response.parse(raw).map_err(|_| Error::Http)?;
    let mut location = None;
    for h in response
        .headers
        .iter()
        .filter(|h| h.name.eq_ignore_ascii_case("Location"))
    {
        if location.is_some() {
            return Err(Error::Http);
        }
        if h.value.len() > 4096 {
            return Err(Error::Limit);
        }
        location = Some(
            std::str::from_utf8(h.value)
                .map_err(|_| Error::Http)?
                .trim()
                .to_owned(),
        );
    }
    Ok(location)
}
pub(crate) async fn read<S: AsyncRead + Unpin>(
    stream: &mut S,
    settings: &Limits,
) -> Result<Received, Error> {
    let mut raw = Vec::new();
    let mut parsed = None;
    let mut chunks = Chunks::default();
    let wire_limit = settings
        .body_bytes
        .checked_add(settings.header_bytes)
        .and_then(|n| n.checked_mul(2))
        .ok_or(Error::Limit)?;
    loop {
        let mut buffer = [0u8; 4096];
        let mut target = ReadBuf::new(&mut buffer);
        poll_fn(|cx| Pin::new(&mut *stream).poll_read(cx, &mut target)).await?;
        let n = target.filled().len();
        if n > wire_limit.saturating_sub(raw.len()) {
            return Err(Error::Limit);
        }
        raw.extend_from_slice(&buffer[..n]);
        if parsed.is_none() {
            if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                let (status, end, framing) = headers(&raw, settings)?;
                parsed = Some((status, end, framing, redirect_location(&raw[..end])?));
            } else if raw.len() > settings.header_bytes {
                return Err(Error::Limit);
            } else if n == 0 {
                return Err(Error::Http);
            } else {
                continue;
            }
        }
        let (status, end, ref framing, ref location) = *parsed.as_ref().expect("parsed response");
        let body = &raw[end..];
        let complete = match framing {
            Framing::Length(length) if body.len() > *length => return Err(Error::Http),
            Framing::Length(length) if body.len() == *length => Some(body.to_vec()),
            Framing::Length(_) => None,
            Framing::Close => {
                if body.len() > settings.body_bytes {
                    return Err(Error::Limit);
                }
                (n == 0).then(|| body.to_vec())
            }
            Framing::Chunked => chunks.advance(body, settings)?,
        };
        if let Some(body) = complete {
            return Ok(Received {
                response: Response { status, body },
                location: location.clone(),
            });
        }
        if n == 0 {
            return Err(Error::Http);
        }
    }
}
