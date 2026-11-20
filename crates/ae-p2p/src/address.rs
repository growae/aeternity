use crate::registry::Info;
use ae_codec::api::{self, ApiType, SafeDecodeError};
use ae_types::PeerPublicKey;
use std::net::Ipv6Addr;

const MAX_DIGITS: usize = 4096;
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub bytes: usize,
    pub host_bytes: usize,
    pub userinfo_bytes: usize,
    pub port_digits: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            bytes: 8192,
            host_bytes: 1024,
            userinfo_bytes: 256,
            port_digits: 128,
        }
    }
}
impl Limits {
    pub fn validate(self) -> Result<(), Error> {
        if self.bytes == 0
            || self.bytes > 1 << 20
            || self.host_bytes > 65536
            || self.userinfo_bytes == 0
            || self.userinfo_bytes > 16384
            || self.port_digits == 0
            || self.port_digits > MAX_DIGITS
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    ResourceLimit,
    InvalidUri,
    InvalidInteger,
    Key(SafeDecodeError),
    MissingPort,
    UnusablePort,
    UnusableHost,
    UnsupportedFamily,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

// A bounded canonical decimal retains the native arbitrary integer without
// doing large-number arithmetic just to represent an unusable transport port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Port(String);
impl Port {
    pub fn from_decimal(text: &str) -> Result<Self, Error> {
        let (negative, digits) = if let Some(d) = text.strip_prefix('-') {
            (true, d)
        } else {
            (false, text)
        };
        if digits.len() > MAX_DIGITS {
            return Err(Error::ResourceLimit);
        }
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(Error::InvalidInteger);
        }
        let trimmed = digits.trim_start_matches('0');
        if trimmed.is_empty() {
            return Ok(Self("0".into()));
        }
        Ok(Self(if negative {
            format!("-{trimmed}")
        } else {
            trimmed.into()
        }))
    }
    pub fn as_decimal(&self) -> &str {
        &self.0
    }
    pub fn transport(&self) -> Result<u16, Error> {
        let port: u16 = self.0.parse().map_err(|_| Error::UnusablePort)?;
        if port == 0 {
            Err(Error::UnusablePort)
        } else {
            Ok(port)
        }
    }
}
impl From<u16> for Port {
    fn from(value: u16) -> Self {
        Self(value.to_string())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Address {
    pub id: PeerPublicKey,
    pub host: Vec<u8>,
    pub port: Option<Port>,
}
impl Address {
    // The original parser's accepted map is broader than a usable endpoint.
    // This explicit adapter targets the current IPv4 registry, without
    // changing the lossless codec result or quietly truncating a native port.
    pub fn registry_info(&self) -> Result<Info, Error> {
        if self.host.len() > 65536 {
            return Err(Error::ResourceLimit);
        }
        let port = self.port.as_ref().ok_or(Error::MissingPort)?.transport()?;
        if self.host.contains(&b':') || self.host.contains(&b'[') || self.host.contains(&b']') {
            return Err(Error::UnsupportedFamily);
        }
        if self.host.is_empty() || !self.host.iter().copied().all(reg_name) {
            return Err(Error::UnusableHost);
        }
        Ok(Info {
            id: self.id,
            host: self.host.clone(),
            port,
        })
    }
}
fn reg_name(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'-' | b'.'
                | b'_'
                | b'~'
                | b'%'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
        )
}
fn component(b: u8) -> bool {
    reg_name(b) || matches!(b, b':' | b'@' | b'/' | b'?')
}
fn tail_valid(tail: &[u8]) -> bool {
    let mut parts = tail.splitn(2, |b| *b == b'#');
    parts.next().unwrap().iter().copied().all(component)
        && parts
            .next()
            .is_none_or(|p| p.iter().copied().all(component))
}

pub fn parse(input: &[u8], limits: Limits) -> Result<Address, Error> {
    limits.validate()?;
    if input.len() > limits.bytes {
        return Err(Error::ResourceLimit);
    }
    let body = input.strip_prefix(b"aenode://").ok_or(Error::InvalidUri)?;
    let end = body
        .iter()
        .position(|b| matches!(b, b'/' | b'?' | b'#'))
        .unwrap_or(body.len());
    if !tail_valid(&body[end..]) {
        return Err(Error::InvalidUri);
    }
    let authority = &body[..end];
    let at = authority
        .iter()
        .position(|b| *b == b'@')
        .ok_or(Error::InvalidUri)?;
    let user = &authority[..at];
    if !user.iter().copied().all(|b| reg_name(b) || b == b':') {
        return Err(Error::InvalidUri);
    }
    let endpoint = &authority[at + 1..];
    let (host, digits) = if let Some(rest) = endpoint.strip_prefix(b"[") {
        let end = rest
            .iter()
            .position(|b| *b == b']')
            .ok_or(Error::InvalidUri)?;
        let inner = std::str::from_utf8(&rest[..end]).map_err(|_| Error::InvalidUri)?;
        inner.parse::<Ipv6Addr>().map_err(|_| Error::InvalidUri)?;
        let after = &rest[end + 1..];
        let colon = after
            .iter()
            .position(|b| *b == b':')
            .ok_or(Error::InvalidUri)?;
        if !after[..colon]
            .iter()
            .all(|b| b.is_ascii_hexdigit() || *b == b'.')
        {
            return Err(Error::InvalidUri);
        }
        // OTP26 accepts hex/dot bytes after the closing bracket and removes
        // only the leading bracket in that case. Keep this codec behavior;
        // registry_info rejects such non-IPv4 endpoint shapes explicitly.
        let host = if colon == 0 {
            &rest[..end]
        } else {
            &rest[..end + 1 + colon]
        };
        (host, &after[colon + 1..])
    } else {
        let colon = endpoint
            .iter()
            .position(|b| *b == b':')
            .ok_or(Error::InvalidUri)?;
        let host = &endpoint[..colon];
        if !host.iter().copied().all(reg_name) {
            return Err(Error::InvalidUri);
        }
        (host, &endpoint[colon + 1..])
    };
    if !digits.iter().all(|b| b.is_ascii_digit()) {
        return Err(Error::InvalidUri);
    }
    if host.len() > limits.host_bytes
        || user.len() > limits.userinfo_bytes
        || digits.len() > limits.port_digits
    {
        return Err(Error::ResourceLimit);
    }
    let user = std::str::from_utf8(user).map_err(|_| Error::InvalidUri)?;
    let key = api::decode_as(ApiType::PeerPubkey, user).map_err(Error::Key)?;
    let id = PeerPublicKey::try_from(key.as_slice())
        .map_err(|_| Error::Key(SafeDecodeError::InvalidEncoding))?;
    let port = if digits.is_empty() {
        None
    } else {
        Some(Port::from_decimal(
            std::str::from_utf8(digits).map_err(|_| Error::InvalidUri)?,
        )?)
    };
    Ok(Address {
        id,
        host: host.to_vec(),
        port,
    })
}

// Matches the native raw formatter: no escaping, case folding, DNS lookup or
// automatic IPv6 brackets. Formatting is not endpoint admission or validation.
pub fn encode(
    id: PeerPublicKey,
    host: &[u8],
    port: &Port,
    limits: Limits,
) -> Result<Vec<u8>, Error> {
    limits.validate()?;
    let digits = port.0.strip_prefix('-').unwrap_or(&port.0);
    if host.len() > limits.host_bytes || digits.len() > limits.port_digits {
        return Err(Error::ResourceLimit);
    }
    let key = api::encode(ApiType::PeerPubkey, id.as_bytes());
    let size = 9usize
        .checked_add(key.len())
        .and_then(|n| n.checked_add(host.len()))
        .and_then(|n| n.checked_add(port.0.len()))
        .and_then(|n| n.checked_add(2))
        .ok_or(Error::ResourceLimit)?;
    if size > limits.bytes || key.len() > limits.userinfo_bytes {
        return Err(Error::ResourceLimit);
    }
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(b"aenode://");
    out.extend_from_slice(key.as_bytes());
    out.push(b'@');
    out.extend_from_slice(host);
    out.push(b':');
    out.extend_from_slice(port.0.as_bytes());
    Ok(out)
}
