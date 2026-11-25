use super::Error;
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
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            target: Default::default(),
            records: 128,
            bytes: 1 << 20,
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
        self.sink().validate()
    }
}
#[derive(Clone)]
pub struct Client(super::sink::Client);
impl Client {
    pub fn request_shutdown(&self) {
        self.0.stop();
    }
    pub fn info(&self) -> super::SinkInfo {
        self.0.info()
    }
    pub async fn changed(&mut self) {
        self.0.changed().await;
    }
    pub fn submit(&self, lines: Vec<Vec<u8>>) -> bool {
        self.0.submit(super::sink::Batch::Metrics(lines))
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        self.0.same_owner(&other.0)
    }
}
pub struct Worker(super::sink::Worker);
impl Worker {
    pub fn spawn(settings: Settings) -> Result<Self, Error> {
        super::sink::Worker::metrics(&settings.sink(), true).map(Self)
    }
    pub fn prepare(settings: Settings) -> Result<Self, Error> {
        super::sink::Worker::metrics(&settings.sink(), false).map(Self)
    }
    pub fn activate(&self) {
        self.0.activate();
    }
    pub fn client(&self) -> Client {
        Client(self.0.client())
    }
    pub async fn finish(&mut self) {
        self.0.finish().await;
    }
}
#[cfg(test)]
mod tests;
