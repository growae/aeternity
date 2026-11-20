use crate::blocking;
use ae_p2p::registry::Token;
use std::{
    future::Future,
    io,
    net::{Ipv4Addr, ToSocketAddrs},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub threads: usize,
    pub jobs: usize,
    pub bytes: usize,
    pub maximum_host_bytes: usize,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            threads: 4,
            jobs: 256,
            bytes: 1 << 20,
            maximum_host_bytes: 1024,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Stopped,
    Cancelled,
    Panicked,
    Spawn,
    InvalidHost,
    ResourceLimit,
    NoIpv4Address,
    Lookup(io::ErrorKind),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<blocking::Error> for Error {
    fn from(error: blocking::Error) -> Self {
        match error {
            blocking::Error::Configuration => Self::Configuration,
            blocking::Error::Busy => Self::Busy,
            blocking::Error::Stopped => Self::Stopped,
            blocking::Error::Cancelled => Self::Cancelled,
            blocking::Error::Panicked => Self::Panicked,
            blocking::Error::Spawn => Self::Spawn,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Completion {
    pub token: Token,
    pub result: Result<Ipv4Addr, Error>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacity {
    pub available_jobs: usize,
    pub available_bytes: usize,
    pub stopped: bool,
}
pub struct Pending {
    token: Token,
    pending: blocking::Pending<Result<Ipv4Addr, Error>>,
}
impl Pending {
    pub fn token(&self) -> Token {
        self.token
    }
}
impl Future for Pending {
    type Output = Completion;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Completion> {
        Pin::new(&mut self.pending)
            .poll(cx)
            .map(|result| Completion {
                token: self.token,
                result: result.map_err(Error::from).and_then(|r| r),
            })
    }
}
type Lookup = dyn Fn(&str) -> Result<Ipv4Addr, Error> + Send + Sync;
pub struct Resolver {
    pool: blocking::Pool,
    lookup: Arc<Lookup>,
    maximum_host_bytes: usize,
}
impl Resolver {
    pub fn start(settings: Settings) -> Result<Self, Error> {
        Self::with_lookup(settings, Arc::new(system_lookup))
    }
    fn with_lookup(settings: Settings, lookup: Arc<Lookup>) -> Result<Self, Error> {
        if settings.jobs == 0
            || settings.jobs > 65536
            || settings.bytes == 0
            || settings.bytes > 1 << 30
            || settings.maximum_host_bytes == 0
            || settings.maximum_host_bytes > 65536
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            pool: blocking::Pool::named(
                "ae-peer-dns",
                settings.threads,
                settings.jobs,
                settings.bytes,
            )?,
            lookup,
            maximum_host_bytes: settings.maximum_host_bytes,
        })
    }
    // Admission owns the job, not a successful DNS result. The caller should
    // acknowledge its registry delivery only after submit succeeds, and apply
    // the completion with this exact registry generation token.
    pub fn submit(&self, token: Token, host: Vec<u8>) -> Result<Pending, Error> {
        if self.pool.stopped() {
            return Err(Error::Stopped);
        }
        if host.len() > self.maximum_host_bytes {
            return Err(Error::ResourceLimit);
        }
        // Registry endpoints are ASCII registered names. Preserve spelling and
        // percent bytes: no URI decoding, IDNA conversion or case rewriting.
        if host.is_empty()
            || host
                .iter()
                .any(|b| !b.is_ascii_graphic() || matches!(b, b':' | b'[' | b']'))
        {
            return Err(Error::InvalidHost);
        }
        let bytes = host.len().checked_add(128).ok_or(Error::ResourceLimit)?;
        let host = String::from_utf8(host).map_err(|_| Error::InvalidHost)?;
        let lookup = self.lookup.clone();
        let pending = self.pool.submit(bytes, move |cancelled| {
            if cancelled.cancelled() {
                return Err(Error::Cancelled);
            }
            lookup(&host)
        })?;
        Ok(Pending { token, pending })
    }
    // Running OS lookups and unread replies continue to consume quotas even
    // after an observer times out. Cancellation suppresses queued work; it
    // cannot interrupt a system getaddrinfo call that has already started.
    pub fn capacity(&self) -> Capacity {
        let (available_jobs, available_bytes) = self.pool.available();
        Capacity {
            available_jobs,
            available_bytes,
            stopped: self.pool.stopped(),
        }
    }
    pub fn stop(&self) {
        self.pool.stop();
    }
    // This joins real workers. Its duration depends on the system resolver;
    // it is not a hard DNS/shutdown deadline. Abrupt owner drop also joins.
    pub async fn join(&self) {
        self.pool.join().await;
    }
    pub async fn shutdown(self) {
        self.stop();
        self.join().await;
    }
}
fn system_lookup(host: &str) -> Result<Ipv4Addr, Error> {
    (host, 0)
        .to_socket_addrs()
        .map_err(|e| Error::Lookup(e.kind()))?
        .find_map(|address| match address.ip() {
            std::net::IpAddr::V4(address) => Some(address),
            std::net::IpAddr::V6(_) => None,
        })
        .ok_or(Error::NoIpv4Address)
}

#[cfg(test)]
mod tests;
