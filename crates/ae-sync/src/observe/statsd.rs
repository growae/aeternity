use super::Error;
pub mod supervision;
use std::{
    io,
    net::{IpAddr, SocketAddr, ToSocketAddrs, UdpSocket},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Host {
    #[default]
    System,
    Name(String),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub host: Host,
    pub port: u16,
    pub reconnect_interval: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            host: Host::System,
            port: 8125,
            reconnect_interval: Duration::from_secs(10),
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        if matches!(&self.host, Host::Name(h) if h.len() > 1024)
            || Instant::now()
                .checked_add(self.reconnect_interval)
                .is_none()
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}

// Bounded by one sink thread and its retained queue. OS name resolution is
// joined on shutdown; an already-running getaddrinfo has no hard deadline.
pub(super) trait Backend: Send {
    fn hostname(&mut self) -> io::Result<String>;
    fn resolve(&mut self, host: &str) -> io::Result<IpAddr>;
    fn open(&mut self, address: SocketAddr) -> io::Result<UdpSocket>;
}
struct System;
impl Backend for System {
    fn hostname(&mut self) -> io::Result<String> {
        // OTP26 inet:gethostname/0 opens a temporary UDP socket and strips
        // the domain. Its open-failure fallback is literal nohost.nodomain.
        let Ok(_socket) = UdpSocket::bind("0.0.0.0:0") else {
            return Ok("nohost.nodomain".into());
        };
        let name = gethostname::gethostname()
            .into_string()
            .map_err(|_| io::ErrorKind::InvalidData)?;
        Ok(name.split('.').next().unwrap_or("").to_owned())
    }
    fn resolve(&mut self, host: &str) -> io::Result<IpAddr> {
        // The original gethostbyname/1 selects inet addresses.
        (host, 0)
            .to_socket_addrs()?
            .find_map(|a| match a.ip() {
                IpAddr::V4(ip) => Some(IpAddr::V4(ip)),
                IpAddr::V6(_) => None,
            })
            .ok_or_else(|| io::ErrorKind::AddrNotAvailable.into())
    }
    fn open(&mut self, address: SocketAddr) -> io::Result<UdpSocket> {
        let bind = if address.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = UdpSocket::bind(bind)?;
        socket.set_nonblocking(true)?;
        Ok(socket)
    }
}
pub(super) enum Target {
    Configured(Config),
    Address(SocketAddr),
}
pub(super) struct Udp {
    target: Target,
    pub address: Option<SocketAddr>,
    socket: Option<UdpSocket>,
    pub next: Option<Instant>,
    backend: Box<dyn Backend>,
}
impl Udp {
    #[cfg(test)]
    pub(super) fn with_backend(target: Target, backend: Box<dyn Backend>) -> Self {
        Self {
            backend,
            ..Self::new(target)
        }
    }
    pub fn new(target: Target) -> Self {
        Self {
            target,
            address: None,
            socket: None,
            next: None,
            backend: Box::new(System),
        }
    }
    pub fn resolve(&mut self) -> io::Result<()> {
        self.address = Some(match &self.target {
            Target::Address(a) => *a,
            Target::Configured(config) => {
                // default_host eagerly evaluates gethostname, including when
                // a user/application hostname replaces the resulting default.
                let system = self.backend.hostname()?;
                let host = match &config.host {
                    Host::System => &system,
                    Host::Name(h) => h,
                };
                SocketAddr::new(self.backend.resolve(host)?, config.port)
            }
        });
        Ok(())
    }
    pub fn connected(&self) -> bool {
        self.socket.is_some()
    }
    pub fn connect(&mut self) -> io::Result<()> {
        self.next = None;
        let address = self.address.expect("resolved target");
        if address.port() == 0 {
            return Ok(());
        }
        match self.backend.open(address) {
            Ok(socket) => {
                self.socket = Some(socket);
                Ok(())
            }
            Err(error) => {
                let delay = match &self.target {
                    Target::Configured(c) => c.reconnect_interval,
                    Target::Address(_) => Duration::from_secs(10),
                };
                self.next = Instant::now().checked_add(delay);
                Err(error)
            }
        }
    }
    pub fn write(&mut self, lines: &[Vec<u8>]) -> io::Result<bool> {
        let Some(socket) = &self.socket else {
            return Ok(false);
        };
        // A native send error logs the error and retains the same socket. It
        // neither triggers a new connection nor changes the retry timer.
        let mut failure = None;
        for line in lines {
            match socket.send_to(line, self.address.expect("resolved target")) {
                Ok(n) if n == line.len() => (),
                Ok(_) => {
                    failure.get_or_insert(io::ErrorKind::WriteZero.into());
                }
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        failure.map_or(Ok(true), Err)
    }
}
#[derive(Debug, Clone)]
pub struct Settings {
    pub target: Config,
    pub records: usize,
    pub bytes: usize,
    pub pipeline: Option<super::pipeline::Config>,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            target: Default::default(),
            records: 128,
            bytes: 1 << 20,
            pipeline: None,
        }
    }
}
impl Settings {
    fn sink(&self) -> super::Config {
        super::Config {
            statsd: Some(self.target.clone()),
            sink_records: self.records,
            sink_bytes: self.bytes,
            record_bytes: 1024,
            ..Default::default()
        }
    }
    pub fn validate(&self) -> Result<(), Error> {
        self.sink().validate()?;
        if let Some(p) = &self.pipeline {
            p.limits.validate().map_err(|_| Error::Configuration)?;
        }
        Ok(())
    }
}
#[derive(Clone)]
enum Binding {
    Direct(super::sink::Client),
    Managed(supervision::Handle),
}
#[derive(Clone)]
pub struct Client(
    Binding,
    Option<std::sync::Arc<super::reporter::Store>>,
    Option<super::reporter::Client>,
    Option<super::registry::Client>,
    Option<std::sync::Arc<super::registry::predefined::Feed>>,
);
impl Client {
    pub fn with_predefined(
        mut self,
        feed: std::sync::Arc<super::registry::predefined::Feed>,
    ) -> Self {
        self.4 = Some(feed);
        self
    }
    pub fn predefined(&self) -> Option<std::sync::Arc<super::registry::predefined::Feed>> {
        self.4.clone()
    }
    pub fn with_registry(mut self, registry: super::registry::Client) -> Self {
        self.3 = Some(registry);
        self
    }
    pub fn registry(&self) -> Option<super::registry::Client> {
        self.3.clone()
    }
    pub fn with_reporter(
        mut self,
        store: std::sync::Arc<super::reporter::Store>,
        reporter: super::reporter::Client,
    ) -> Self {
        self.1 = Some(store);
        self.2 = Some(reporter);
        self
    }
    pub fn store(&self) -> Option<std::sync::Arc<super::reporter::Store>> {
        self.1.clone()
    }
    pub fn reporter(&self) -> Option<super::reporter::Client> {
        self.2.clone()
    }
    pub fn is_pipeline(&self) -> bool {
        match &self.0 {
            Binding::Direct(c) => c.input().is_some(),
            Binding::Managed(h) => h.is_pipeline(),
        }
    }
    pub fn input(&self) -> Option<super::pipeline::Input> {
        match &self.0 {
            Binding::Direct(c) => c.input(),
            Binding::Managed(h) => h.current().and_then(|g| g.client.input()),
        }
    }
    pub fn report(&self, event: super::report::Event) -> Result<(), super::pipeline::Error> {
        match &self.0 {
            Binding::Direct(c) => c
                .input()
                .ok_or(super::pipeline::Error::Closed)?
                .report(event),
            Binding::Managed(h) => h.report(event),
        }
    }
    pub fn supervisor(&self) -> Option<supervision::Handle> {
        match &self.0 {
            Binding::Managed(h) => Some(h.clone()),
            _ => None,
        }
    }
    pub fn request_shutdown(&self) {
        match &self.0 {
            Binding::Direct(c) => c.stop(),
            Binding::Managed(h) => h.shutdown(),
        }
    }
    pub fn info(&self) -> super::SinkInfo {
        match &self.0 {
            Binding::Direct(c) => c.info(),
            Binding::Managed(h) => h.info(),
        }
    }
    pub async fn changed(&mut self) {
        match &mut self.0 {
            Binding::Direct(c) => c.changed().await,
            Binding::Managed(h) => h.changed().await,
        }
    }
    pub fn submit(&self, lines: Vec<Vec<u8>>) -> bool {
        match &self.0 {
            Binding::Direct(c) => c.submit(super::sink::Batch::Metrics(lines)),
            Binding::Managed(h) => h.submit(lines),
        }
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Binding::Direct(a), Binding::Direct(b)) => a.same_owner(b),
            (Binding::Managed(a), Binding::Managed(b)) => a.same_owner(b),
            _ => false,
        }
    }
}
pub struct Worker(super::sink::Worker);
impl Worker {
    pub fn spawn(settings: Settings) -> Result<Self, Error> {
        let worker = Self::prepare(settings)?;
        worker.activate();
        Ok(worker)
    }
    pub fn prepare(settings: Settings) -> Result<Self, Error> {
        settings.validate()?;
        match &settings.pipeline {
            Some(pipeline) => {
                super::sink::Worker::pipeline(&settings.sink(), pipeline.clone()).map(Self)
            }
            None => super::sink::Worker::metrics(&settings.sink(), false).map(Self),
        }
    }
    pub fn activate(&self) {
        self.0.activate();
    }
    pub fn client(&self) -> Client {
        Client(Binding::Direct(self.0.client()), None, None, None, None)
    }
    pub async fn finish(&mut self) {
        self.0.finish().await;
    }
}
#[cfg(test)]
mod tests;
