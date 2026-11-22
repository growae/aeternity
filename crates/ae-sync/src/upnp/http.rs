use super::{Error, Settings};
use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::net::{TcpSocket, TcpStream};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Url {
    pub address: SocketAddrV4,
    pub path: String,
}
impl Url {
    pub fn parse(input: &str) -> Result<Self, Error> {
        if input.len() > 4096 || input.bytes().any(|b| b <= 32 || b >= 127) {
            return Err(Error::Address);
        }
        let tail = input.strip_prefix("http://").ok_or(Error::Address)?;
        let split = tail.find(['/', '?', '#']).unwrap_or(tail.len());
        let authority = &tail[..split];
        let (host, port) = match authority.split_once(':') {
            Some((h, p)) => (h, p.parse::<u16>().map_err(|_| Error::Address)?),
            None => (authority, 80),
        };
        let ip = host.parse::<Ipv4Addr>().map_err(|_| Error::Address)?;
        if ip.is_unspecified() || ip.is_multicast() || ip.is_broadcast() || port == 0 {
            return Err(Error::Address);
        }
        let path = tail[split..].split('#').next().unwrap_or("");
        let path = if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("/{path}")
        };
        Ok(Self {
            address: SocketAddrV4::new(ip, port),
            path,
        })
    }
    pub fn control(&self, path: &str) -> Result<Self, Error> {
        // The reference library uses the description's origin, not URLBase.
        // Require an origin-relative path instead of silently constructing a
        // malformed URL from an absolute or directory-relative controlURL.
        if !path.starts_with('/') || path.starts_with("//") {
            return Err(Error::Address);
        }
        Self::parse(&format!("http://{}{}", self.address, path))
    }
}

#[derive(Debug)]
pub(super) struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

pub(super) async fn request(
    url: &Url,
    local: Ipv4Addr,
    soap: Option<(&str, &str)>,
    settings: &Settings,
) -> Result<Response, Error> {
    tokio::time::timeout(settings.request_timeout, async {
        let socket = TcpSocket::new_v4()?;
        socket.bind(SocketAddrV4::new(local,0).into())?;
        let stream = socket.connect(url.address.into()).await?;
        let mut request = match soap {
            None => format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", url.path, url.address),
            Some((action, body)) => format!(
                "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: text/xml; charset=\"utf-8\"\r\nSOAPAction: \"{}\"\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-cache\r\nPragma: no-cache\r\n\r\n{}",
                url.path,url.address,action,body.len(),body),
        }.into_bytes();
        // The request is generated solely from validated URLs and fixed XML.
        if request.len() > settings.header_bytes + settings.body_bytes { return Err(Error::Limit); }
        write(&stream, &request).await?;
        request.clear();
        read(&stream, settings).await
    }).await.map_err(|_| Error::Timeout)?
}

async fn write(stream: &TcpStream, mut bytes: &[u8]) -> Result<(), Error> {
    while !bytes.is_empty() {
        stream.writable().await?;
        match stream.try_write(bytes) {
            Ok(0) => return Err(Error::Io(std::io::ErrorKind::WriteZero)),
            Ok(n) => bytes = &bytes[n..],
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Framing {
    Length(usize),
    Chunked,
    Close,
}
pub(super) fn headers(raw: &[u8], settings: &Settings) -> Result<(u16, usize, Framing), Error> {
    let mut fields = [httparse::EMPTY_HEADER; 64];
    let mut response = httparse::Response::new(&mut fields);
    let end = match response.parse(raw).map_err(|_| Error::Http)? {
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
pub(super) struct Chunks {
    offset: usize,
    next: Option<usize>,
    trailers: bool,
    body: Vec<u8>,
}
impl Chunks {
    pub fn advance(&mut self, raw: &[u8], s: &Settings) -> Result<Option<Vec<u8>>, Error> {
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
                match httparse::parse_headers(tail, &mut fields).map_err(|_| Error::Http)? {
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

async fn read(stream: &TcpStream, settings: &Settings) -> Result<Response, Error> {
    let mut raw = Vec::new();
    let mut parsed = None;
    let mut chunks = Chunks::default();
    let wire_limit = settings.body_bytes * 2 + settings.header_bytes * 2;
    loop {
        stream.readable().await?;
        let mut buffer = [0u8; 4096];
        let n = match stream.try_read(&mut buffer) {
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        };
        if n > wire_limit.saturating_sub(raw.len()) {
            return Err(Error::Limit);
        }
        raw.extend_from_slice(&buffer[..n]);
        if parsed.is_none() {
            if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                parsed = Some(headers(&raw, settings)?);
            } else if raw.len() > settings.header_bytes {
                return Err(Error::Limit);
            } else if n == 0 {
                return Err(Error::Http);
            } else {
                continue;
            }
        }
        let (status, end, ref framing) = *parsed.as_ref().expect("parsed response");
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
            return Ok(Response { status, body });
        }
        if n == 0 {
            return Err(Error::Http);
        }
    }
}
