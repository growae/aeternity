use super::{Error, Response, Settings, Url, address::valid_ip};
use crate::blocking;
use std::{
    net::{Ipv4Addr, SocketAddrV4, ToSocketAddrs},
    sync::{Arc, OnceLock},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpSocket,
    time::timeout,
};
use tokio_rustls::TlsConnector;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Endpoint {
    pub url: Url,
    pub address: Ipv4Addr,
}
pub(crate) struct Fetched {
    pub endpoint: Endpoint,
    pub response: Response,
}
pub(crate) struct Transport {
    settings: Arc<Settings>,
    tls: Arc<rustls::ClientConfig>,
    resolver: OnceLock<Result<blocking::Pool, Error>>,
}
impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport")
            .field("resolver_started", &self.resolver.get().is_some())
            .finish_non_exhaustive()
    }
}
trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}
fn worker_error(error: blocking::Error) -> Error {
    match error {
        blocking::Error::Configuration => Error::Configuration,
        blocking::Error::Busy => Error::Busy,
        blocking::Error::Stopped | blocking::Error::Cancelled => Error::Closed,
        blocking::Error::Panicked => Error::Panicked,
        blocking::Error::Spawn => Error::Spawn,
    }
}
impl Transport {
    pub fn new(settings: Arc<Settings>) -> Result<Self, Error> {
        let mut roots = rustls::RootCertStore::empty();
        if settings.https.public_roots {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        for der in &settings.https.roots {
            roots
                .add(rustls::pki_types::CertificateDer::from(der.clone()))
                .map_err(|_| Error::Configuration)?;
        }
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| Error::Configuration)?
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            settings,
            tls: Arc::new(tls),
            resolver: OnceLock::new(),
        })
    }
    async fn addresses(&self, url: &Url) -> Result<Vec<Ipv4Addr>, Error> {
        if let Some(ip) = url.numeric() {
            return Ok(vec![ip]);
        }
        let pool = self
            .resolver
            .get_or_init(|| blocking::Pool::named("ae-upnp-dns", 1, 4, 16384).map_err(worker_error))
            .as_ref()
            .map_err(Clone::clone)?;
        let host = url.host().to_owned();
        let pending = pool
            .submit(host.len() + 128, move |cancelled| {
                if cancelled.cancelled() {
                    return Err(Error::Closed);
                }
                let addresses = (host.as_str(), 0)
                    .to_socket_addrs()
                    .map_err(|e| Error::Dns(e.kind()))?;
                let mut result = Vec::new();
                for address in addresses {
                    if let std::net::IpAddr::V4(ip) = address.ip()
                        && valid_ip(ip)
                        && !result.contains(&ip)
                    {
                        result.push(ip);
                        if result.len() == 8 {
                            break;
                        }
                    }
                }
                if cancelled.cancelled() {
                    return Err(Error::Closed);
                }
                if result.is_empty() {
                    Err(Error::Address)
                } else {
                    Ok(result)
                }
            })
            .map_err(worker_error)?;
        pending.await.map_err(worker_error)?
    }
    pub async fn endpoint(&self, url: Url, origin: &Endpoint) -> Result<Endpoint, Error> {
        if url.same_origin(&origin.url) {
            return Ok(Endpoint {
                url,
                address: origin.address,
            });
        }
        self.resolve(&url).await
    }
    pub async fn resolve(&self, url: &Url) -> Result<Endpoint, Error> {
        let address = timeout(self.settings.request_timeout, self.addresses(url))
            .await
            .map_err(|_| Error::Timeout)??[0];
        Ok(Endpoint {
            url: url.clone(),
            address,
        })
    }
    async fn connect(
        &self,
        endpoint: &Endpoint,
        local: Ipv4Addr,
    ) -> Result<Box<dyn Stream>, Error> {
        let socket = TcpSocket::new_v4()?;
        socket.bind(SocketAddrV4::new(local, 0).into())?;
        let stream = socket
            .connect(SocketAddrV4::new(endpoint.address, endpoint.url.port()).into())
            .await?;
        if !endpoint.url.secure() {
            return Ok(Box::new(stream));
        }
        let name = rustls::pki_types::ServerName::try_from(endpoint.url.host().to_owned())
            .map_err(|_| Error::Address)?;
        let stream = TlsConnector::from(self.tls.clone())
            .connect(name, stream)
            .await
            .map_err(|e| Error::Tls(e.to_string().chars().take(512).collect()))?;
        Ok(Box::new(stream))
    }
    async fn exchange(
        &self,
        endpoint: &Endpoint,
        local: Ipv4Addr,
        soap: Option<(&str, &str)>,
    ) -> Result<super::Received, Error> {
        let mut stream = self.connect(endpoint, local).await?;
        let url = &endpoint.url;
        let bytes = match soap {
            None => format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", url.path, url.authority()),
            Some((action, body)) => format!("POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: text/xml; charset=\"utf-8\"\r\nSOAPAction: \"{}\"\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-cache\r\nPragma: no-cache\r\n\r\n{}", url.path,url.authority(),action,body.len(),body),
        }.into_bytes();
        if bytes.len() > self.settings.header_bytes + self.settings.body_bytes {
            return Err(Error::Limit);
        }
        super::write(&mut stream, &bytes).await?;
        super::read(&mut stream, &self.settings).await
    }
    pub async fn fetch(&self, url: &Url, local: Ipv4Addr) -> Result<Fetched, Error> {
        timeout(self.settings.request_timeout, async {
            let mut current = url.clone();
            for hop in 0..=self.settings.redirects {
                let addresses = self.addresses(&current).await?;
                let mut last = Error::Address;
                let mut received = None;
                for address in addresses {
                    let endpoint = Endpoint {
                        url: current.clone(),
                        address,
                    };
                    match self.exchange(&endpoint, local, None).await {
                        Ok(response) => {
                            received = Some((endpoint, response));
                            break;
                        }
                        Err(error) => last = error,
                    }
                }
                let (endpoint, received) = received.ok_or(last)?;
                if matches!(received.response.status, 300 | 301 | 302 | 303 | 307 | 308) {
                    let Some(location) = received.location else {
                        return Ok(Fetched {
                            endpoint,
                            response: received.response,
                        });
                    };
                    if hop == self.settings.redirects {
                        return Err(Error::Redirect);
                    }
                    current = current.control(&location)?;
                } else {
                    return Ok(Fetched {
                        endpoint,
                        response: received.response,
                    });
                }
            }
            unreachable!("bounded redirect loop")
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    pub async fn post_once(
        &self,
        endpoint: &Endpoint,
        local: Ipv4Addr,
        action: &str,
        body: &str,
    ) -> Result<super::Received, Error> {
        timeout(
            self.settings.request_timeout,
            self.exchange(endpoint, local, Some((action, body))),
        )
        .await
        .map_err(|_| Error::Timeout)?
    }
    pub async fn redirect(
        &self,
        endpoint: &Endpoint,
        received: &super::Received,
    ) -> Result<Option<Endpoint>, Error> {
        // Keep SOAP as POST for 301/302 (RFC9110 permits preserving the method).
        // A 303 is an indirect representation, not evidence of a SOAP mutation.
        if !matches!(received.response.status, 300 | 301 | 302 | 307 | 308) {
            return Ok(None);
        }
        match &received.location {
            Some(location) => self
                .endpoint(endpoint.url.control(location)?, endpoint)
                .await
                .map(Some),
            None => Ok(None),
        }
    }
    pub async fn post_tracked(
        &self,
        endpoint: &Endpoint,
        local: Ipv4Addr,
        action: &str,
        body: &str,
        before_write: &mut impl FnMut(&Endpoint) -> Result<(), Error>,
    ) -> Result<Fetched, Error> {
        timeout(self.settings.request_timeout, async {
            let mut current = endpoint.clone();
            let mut visited = Vec::new();
            for hop in 0..=self.settings.redirects {
                if visited.contains(&current) {
                    return Err(Error::Redirect);
                }
                visited.push(current.clone());
                // The caller can reserve ownership/capacity before any bytes
                // reach this destination. Cancellation never discards its intent.
                before_write(&current)?;
                let received = self.post_once(&current, local, action, body).await?;
                if let Some(next) = self.redirect(&current, &received).await? {
                    if hop == self.settings.redirects {
                        return Err(Error::Redirect);
                    }
                    current = next;
                } else {
                    return Ok(Fetched {
                        endpoint: current,
                        response: received.response,
                    });
                }
            }
            unreachable!("bounded redirect loop")
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    pub async fn post(
        &self,
        endpoint: &Endpoint,
        local: Ipv4Addr,
        action: &str,
        body: &str,
    ) -> Result<Response, Error> {
        self.post_tracked(endpoint, local, action, body, &mut |_| Ok(()))
            .await
            .map(|r| r.response)
    }
}
// DNS uses a bounded owned worker, as the peer resolver does. Cancellation
// suppresses queued work, but cannot interrupt getaddrinfo already in progress.
// Dropping the last Transport joins it; that join has an OS-dependent duration.
