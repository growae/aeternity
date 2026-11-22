// Port mapping behavior is based on aec_upnp and erlang-nat.
// aec_upnp: Copyright (C) 2019 Aeternity Anstalt.
// erlang-nat: Copyright (c) 2016-2018 Benoît Chesneau <benoitc@refuge.io>.
// See ../LICENSE-MIT-NAT for the original permission notice.

use std::{io, net::Ipv4Addr, net::SocketAddrV4, time::Duration};

mod discovery;
mod http;
mod listener;
mod pmp;
mod routes;
mod source;
mod worker;
mod xml;

pub use discovery::{Gateway, Network};
pub(crate) use listener::ListenerLease;
pub use listener::ListenerSource;
pub use source::PortsSource;
pub use worker::{Client, Config, Info, Ticket, Worker};

pub const MAPPING_LIFETIME: u32 = 7200;
pub const RENEWAL_INTERVAL: Duration = Duration::from_secs(7190);
pub const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAPPING_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ports {
    pub internal: u16,
    pub external: u16,
}
impl Ports {
    fn validate(self) -> Result<(), Error> {
        if self.internal == 0 {
            Err(Error::Configuration)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    NatPmp,
    Upnp1,
    Upnp2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mapping {
    pub protocol: Protocol,
    pub internal: u16,
    pub external: u16,
    // None denotes a permanent UPnP mapping. NAT-PMP always returns seconds.
    pub lifetime: Option<u32>,
    pub gateway_epoch: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Spawn,
    Closed,
    Busy,
    Disconnected,
    Panicked,
    Configuration,
    Limit,
    Timeout,
    NoGateway,
    NotReady,
    Address,
    Http,
    Xml,
    Response,
    HttpStatus(u16),
    Soap(u16),
    NatPmp(u16),
    Io(io::ErrorKind),
    Dns(io::ErrorKind),
    Tls(String),
    Redirect,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e.kind())
    }
}

#[derive(Debug, Clone)]
pub struct Https {
    pub public_roots: bool,
    // Additional DER trust anchors, e.g. a locally administered router CA.
    pub roots: Vec<Vec<u8>>,
}
impl Default for Https {
    fn default() -> Self {
        Self {
            public_roots: true,
            roots: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub bind: Ipv4Addr,
    pub ssdp: SocketAddrV4,
    // Supplied by the host's route discovery; no subnet scans are performed.
    pub gateways: Vec<SocketAddrV4>,
    pub discovery_timeout: Duration,
    pub request_timeout: Duration,
    pub initial_retry: Duration,
    pub attempts: u8,
    pub header_bytes: usize,
    pub body_bytes: usize,
    pub xml_nodes: u32,
    pub redirects: u8,
    pub https: Https,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            bind: Ipv4Addr::UNSPECIFIED,
            ssdp: SocketAddrV4::new(Ipv4Addr::new(239, 255, 255, 250), 1900),
            gateways: vec![],
            discovery_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10),
            initial_retry: Duration::from_millis(250),
            attempts: 5,
            header_bytes: 16 * 1024,
            body_bytes: 1024 * 1024,
            xml_nodes: 16384,
            redirects: 5,
            https: Https::default(),
        }
    }
}
impl Settings {
    fn validate(&self) -> Result<(), Error> {
        if self.gateways.len() > 32
            || self.redirects > 16
            || self.https.roots.len() > 32
            || self
                .https
                .roots
                .iter()
                .any(|r| r.is_empty() || r.len() > 65536)
            || self.https.roots.iter().map(Vec::len).sum::<usize>() > 1024 * 1024
            || self.ssdp.port() == 0
            || self
                .gateways
                .iter()
                .any(|g| g.port() == 0 || g.ip().is_unspecified() || g.ip().is_multicast())
            || !(1..=5).contains(&self.attempts)
            || !(512..=65536).contains(&self.header_bytes)
            || !(1024..=4 * 1024 * 1024).contains(&self.body_bytes)
            || !(64..=65536).contains(&self.xml_nodes)
            || [
                self.discovery_timeout,
                self.request_timeout,
                self.initial_retry,
            ]
            .iter()
            .any(|d| d.is_zero() || *d > Duration::from_secs(30))
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
