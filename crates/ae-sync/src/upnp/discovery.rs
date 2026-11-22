use super::{Error, Mapping, Ports, Protocol, Settings, http, pmp, xml};
use crate::tasks::Tasks;
use rand::Rng;
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};
use tokio::{
    net::UdpSocket,
    time::{Instant, timeout, timeout_at},
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Backend {
    Pmp {
        address: SocketAddrV4,
        local: Ipv4Addr,
    },
    Upnp {
        endpoint: http::Endpoint,
        local: Ipv4Addr,
        version: u8,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gateway(Backend);
impl Gateway {
    pub fn protocol(&self) -> Protocol {
        match self.0 {
            Backend::Pmp { .. } => Protocol::NatPmp,
            Backend::Upnp { version: 2, .. } => Protocol::Upnp2,
            _ => Protocol::Upnp1,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Network {
    settings: Arc<Settings>,
    transport: Arc<http::Transport>,
}
impl Network {
    // Queries local interface/route metadata only. Actual gateway discovery
    // begins when discover() is polled by its owner.
    pub fn from_system(settings: Settings) -> Result<Self, Error> {
        Self::new(super::routes::system(settings)?)
    }
    pub fn new(settings: Settings) -> Result<Self, Error> {
        settings.validate()?;
        let settings = Arc::new(settings);
        let transport = Arc::new(http::Transport::new(settings.clone())?);
        Ok(Self {
            settings,
            transport,
        })
    }
    // All competing attempts are owned futures: dropping this call closes
    // every socket. The first successful backend wins; losers are cancelled.
    pub async fn discover(&self) -> Result<Gateway, Error> {
        timeout(self.settings.discovery_timeout, async {
            let mut tasks = Tasks::new();
            for version in [1, 2] {
                let settings = self.settings.clone();
                let transport = self.transport.clone();
                tasks.insert(u16::from(version), async move {
                    upnp(version, &settings, &transport).await.map(Gateway)
                });
            }
            for (index, gateway) in self.settings.gateways.iter().copied().enumerate() {
                let settings = self.settings.clone();
                tasks.insert(3 + index as u16, async move {
                    let local = pmp::discover(gateway, &settings).await?;
                    Ok(Gateway(Backend::Pmp {
                        address: gateway,
                        local,
                    }))
                });
            }
            while tasks.len() != 0 {
                if let (_, Ok(gateway)) = tasks.next().await {
                    return Ok(gateway);
                }
            }
            Err(Error::NoGateway)
        })
        .await
        .map_err(|_| Error::NoGateway)?
    }
    pub async fn add(&self, gateway: &Gateway, ports: Ports) -> Result<Mapping, Error> {
        self.add_named(gateway, ports, None).await
    }
    pub(super) async fn add_named(
        &self,
        gateway: &Gateway,
        ports: Ports,
        description: Option<&str>,
    ) -> Result<Mapping, Error> {
        ports.validate()?;
        if description.is_some_and(|d| {
            d.is_empty()
                || d.len() > 64
                || !d
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b":_-".contains(&c))
        }) {
            return Err(Error::Configuration);
        }
        match &gateway.0 {
            Backend::Pmp { address, local } => {
                let settings = Settings {
                    bind: *local,
                    ..(*self.settings).clone()
                };
                pmp::map(*address, ports, super::MAPPING_LIFETIME, &settings).await
            }
            Backend::Upnp {
                endpoint,
                local,
                version,
            } => {
                let attempts = if ports.external == 0 { 3 } else { 1 };
                let mut last = Error::Response;
                for _ in 0..attempts {
                    let ports = if ports.external == 0 {
                        Ports {
                            external: rand::thread_rng().gen_range(10001..=65535),
                            ..ports
                        }
                    } else {
                        ports
                    };
                    match self
                        .upnp_add(endpoint, *local, *version, ports, description)
                        .await
                    {
                        Ok(mapping) => return Ok(mapping),
                        Err(e) => last = e,
                    }
                }
                Err(last)
            }
        }
    }
    async fn upnp_add(
        &self,
        endpoint: &http::Endpoint,
        local: Ipv4Addr,
        version: u8,
        ports: Ports,
        description: Option<&str>,
    ) -> Result<Mapping, Error> {
        let mut lifetime = super::MAPPING_LIFETIME;
        loop {
            let started = Instant::now();
            let action = xml::Action::Add { ports, lifetime };
            let (header, body) = xml::request_named(action, version, local, description);
            let response = self.transport.post(endpoint, local, &header, &body).await?;
            match xml::response(&response, action, version, &self.settings) {
                Err(Error::Soap(725)) if lifetime != 0 => lifetime = 0,
                Err(e) => return Err(e),
                Ok(port) => {
                    return Ok(Mapping {
                        protocol: if version == 1 {
                            Protocol::Upnp1
                        } else {
                            Protocol::Upnp2
                        },
                        internal: ports.internal,
                        external: port.unwrap_or(ports.external),
                        lifetime: (lifetime != 0).then(|| {
                            lifetime.saturating_sub(
                                started.elapsed().as_secs().min(u64::from(u32::MAX)) as u32,
                            )
                        }),
                        gateway_epoch: None,
                    });
                }
            }
        }
    }
    pub async fn delete(&self, gateway: &Gateway, ports: Ports) -> Result<(), Error> {
        ports.validate()?;
        match &gateway.0 {
            Backend::Pmp { address, local } => {
                let settings = Settings {
                    bind: *local,
                    ..(*self.settings).clone()
                };
                pmp::map(*address, ports, 0, &settings).await?;
            }
            Backend::Upnp {
                endpoint,
                local,
                version,
            } => {
                let action = xml::Action::Delete {
                    external: ports.external,
                };
                let (header, body) = xml::request(action, *version, *local);
                let response = self
                    .transport
                    .post(endpoint, *local, &header, &body)
                    .await?;
                xml::response(&response, action, *version, &self.settings)?;
            }
        }
        Ok(())
    }
    pub(super) async fn owned_ports(
        &self,
        gateway: &Gateway,
        internal: u16,
        description: &str,
        known: Option<u16>,
    ) -> Result<Vec<u16>, Error> {
        let Backend::Upnp {
            endpoint,
            local,
            version,
        } = &gateway.0
        else {
            return Ok(vec![0]);
        };
        let own_client = local.to_string();
        let matches = |e: &xml::Entry| {
            e.tcp
                && e.wildcard
                && e.client == own_client
                && e.internal == internal
                && e.description == description
        };
        let query = async |action| {
            let (header, body) = xml::request(action, *version, *local);
            let response = self
                .transport
                .post(endpoint, *local, &header, &body)
                .await?;
            xml::entry(&response, action, *version, &self.settings)
        };
        if let Some(external) = known {
            return match query(xml::Action::Specific { external }).await {
                Ok(e) => Ok(if matches(&e) { vec![external] } else { vec![] }),
                Err(Error::Soap(714)) => Ok(vec![]),
                Err(e) => Err(e),
            };
        }
        let mut ports = std::collections::BTreeSet::new();
        // End-of-table (713) is required before an uncertain write is resolved.
        // A resource limit or transport failure leaves its ownership record.
        for index in 0..512 {
            match query(xml::Action::Generic { index }).await {
                Ok(e) if matches(&e) => {
                    if e.external == 0 {
                        return Err(Error::Response);
                    }
                    ports.insert(e.external);
                }
                Ok(_) => {}
                Err(Error::Soap(713)) => return Ok(ports.into_iter().collect()),
                Err(e) => return Err(e),
            }
        }
        Err(Error::Limit)
    }
}

pub(super) fn msearch(version: u8) -> String {
    format!(
        "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nST: {}\r\nMX: 3\r\n\r\n",
        xml::target(version)
    )
}
pub(super) fn location(raw: &[u8], version: u8) -> Result<Option<http::Url>, Error> {
    if raw.len() > 8192 {
        return Err(Error::Limit);
    }
    let mut fields = [httparse::EMPTY_HEADER; 64];
    let mut response = httparse::Response::new(&mut fields);
    if !response.parse(raw).map_err(|_| Error::Http)?.is_complete() || response.code != Some(200) {
        return Err(Error::Http);
    }
    let mut target = None;
    let mut location = None;
    for h in response.headers.iter() {
        if h.name.eq_ignore_ascii_case("ST") {
            if target.is_some() {
                return Err(Error::Http);
            }
            target = Some(h.value);
        }
        if h.name.eq_ignore_ascii_case("Location") {
            if location.is_some() {
                return Err(Error::Http);
            }
            location = Some(h.value);
        }
    }
    if target != Some(xml::target(version).as_bytes()) {
        return Ok(None);
    }
    http::Url::parse(std::str::from_utf8(location.ok_or(Error::Http)?).map_err(|_| Error::Address)?)
        .map(Some)
}
async fn upnp(
    version: u8,
    settings: &Settings,
    transport: &http::Transport,
) -> Result<Backend, Error> {
    let socket = UdpSocket::bind(SocketAddrV4::new(settings.bind, 0)).await?;
    socket.set_multicast_ttl_v4(2)?;
    let search = msearch(version);
    let mut packets = 0usize;
    // Native v1 starts at backoff index 3, giving 2s then 4s waits.
    // v2's decrementing counter is repaired to the same finite schedule.
    for attempt in 3..5 {
        socket.send_to(search.as_bytes(), settings.ssdp).await?;
        let deadline = Instant::now() + settings.initial_retry * (1u32 << attempt);
        loop {
            let mut packet = [0u8; 8193];
            let (n, sender) = match timeout_at(deadline, socket.recv_from(&mut packet)).await {
                Ok(r) => r?,
                Err(_) => break,
            };
            packets += 1;
            if packets > 128 {
                return Err(Error::Limit);
            }
            let root = match location(&packet[..n], version) {
                Ok(Some(root)) => root,
                _ => continue,
            };
            let route = UdpSocket::bind(SocketAddrV4::new(settings.bind, 0)).await?;
            route.connect(sender).await?;
            let local = match route.local_addr()? {
                SocketAddr::V4(a) => *a.ip(),
                _ => return Err(Error::Address),
            };
            let fetched = transport.fetch(&root, local).await?;
            let response = fetched.response;
            if response.status != 200 {
                return Err(Error::HttpStatus(response.status));
            }
            let (url, service_version) =
                xml::control(&response.body, &fetched.endpoint.url, version, settings)?;
            let endpoint = transport.endpoint(url, &fetched.endpoint).await?;
            if version == 2 {
                let action = xml::Action::NatStatus;
                let (header, body) = xml::request(action, service_version, local);
                let response = transport.post(&endpoint, local, &header, &body).await?;
                if xml::response(&response, action, service_version, settings)? != Some(1) {
                    return Err(Error::NoGateway);
                }
            }
            return Ok(Backend::Upnp {
                endpoint,
                local,
                version: service_version,
            });
        }
    }
    Err(Error::NoGateway)
}
