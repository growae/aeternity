use super::super::Error;
use std::net::Ipv4Addr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Url {
    inner: url::Url,
    pub path: String,
}
impl Url {
    fn input(input: &str) -> Result<(), Error> {
        if input.len() > 4096 || input.bytes().any(|b| b <= 32 || b >= 127 || b == b'\\') {
            Err(Error::Address)
        } else {
            Ok(())
        }
    }
    pub fn parse(input: &str) -> Result<Self, Error> {
        Self::input(input)?;
        Self::checked(url::Url::parse(input).map_err(|_| Error::Address)?)
    }
    fn checked(mut inner: url::Url) -> Result<Self, Error> {
        if !matches!(inner.scheme(), "http" | "https")
            || !inner.username().is_empty()
            || inner.password().is_some()
            || inner.port_or_known_default().is_none_or(|p| p == 0)
        {
            return Err(Error::Address);
        }
        match inner.host().ok_or(Error::Address)? {
            url::Host::Ipv4(ip) if valid_ip(ip) => (),
            url::Host::Domain(host) if !host.is_empty() && host.len() <= 253 => (),
            _ => return Err(Error::Address),
        }
        inner.set_fragment(None);
        if inner.as_str().len() > 4096 {
            return Err(Error::Limit);
        }
        let path = inner[url::Position::BeforePath..].to_string();
        Ok(Self { inner, path })
    }
    pub fn control(&self, reference: &str) -> Result<Self, Error> {
        Self::input(reference)?;
        let result = Self::checked(self.inner.join(reference).map_err(|_| Error::Address)?)?;
        if self.secure() && !result.secure() {
            return Err(Error::Redirect);
        }
        Ok(result)
    }
    pub fn secure(&self) -> bool {
        self.inner.scheme() == "https"
    }
    pub fn host(&self) -> &str {
        self.inner.host_str().expect("checked host")
    }
    pub fn port(&self) -> u16 {
        self.inner.port_or_known_default().expect("checked port")
    }
    pub fn authority(&self) -> &str {
        &self.inner[url::Position::BeforeHost..url::Position::AfterPort]
    }
    pub fn numeric(&self) -> Option<Ipv4Addr> {
        match self.inner.host() {
            Some(url::Host::Ipv4(ip)) => Some(ip),
            _ => None,
        }
    }
    pub fn same_origin(&self, other: &Self) -> bool {
        self.inner.origin() == other.inner.origin()
    }
}
pub(super) fn valid_ip(ip: Ipv4Addr) -> bool {
    !ip.is_unspecified() && !ip.is_multicast() && !ip.is_broadcast()
}
