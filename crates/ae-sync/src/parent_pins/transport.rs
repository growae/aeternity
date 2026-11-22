use super::{Config, Error, Node};
use crate::{blocking, http};
use base64::Engine;
use std::{
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::timeout,
};
trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}
pub(crate) struct Transport {
    pub config: Arc<Config>,
    tls: Arc<rustls::ClientConfig>,
    resolver: OnceLock<Result<blocking::Pool, Error>>,
}
fn exhausted(e: &std::io::Error) -> bool {
    // ENOMEM, system/process descriptor exhaustion, and ENOBUFS on the
    // supported Unix hosts. These are local failures, not negative proofs.
    e.kind() == std::io::ErrorKind::OutOfMemory
        || matches!(e.raw_os_error(), Some(12 | 23 | 24 | 55 | 105))
}
fn worker(e: blocking::Error) -> Error {
    match e {
        blocking::Error::Busy => Error::Busy,
        blocking::Error::Configuration => Error::Configuration,
        blocking::Error::Stopped | blocking::Error::Cancelled => Error::Closed,
        blocking::Error::Spawn => Error::Spawn,
        blocking::Error::Panicked => Error::Worker,
    }
}
pub(super) async fn cancelled(stop: &AtomicBool, cancel: &AtomicBool, deadline: Instant) -> Error {
    loop {
        if stop.load(Ordering::Acquire) || cancel.load(Ordering::Acquire) {
            return Error::Closed;
        }
        if Instant::now() >= deadline {
            return Error::Deadline;
        }
        tokio::time::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(10)),
        )
        .await;
    }
}
impl Transport {
    pub fn new(config: Arc<Config>) -> Result<Self, Error> {
        let mut roots = rustls::RootCertStore::empty();
        if config.public_roots {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        for der in &config.roots {
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
            config,
            tls: Arc::new(tls),
            resolver: OnceLock::new(),
        })
    }
    async fn addresses(&self, node: &Node) -> Result<Vec<SocketAddr>, Error> {
        let port = node.origin.port_or_known_default().expect("checked port");
        let host = match node.origin.host().expect("checked host") {
            url::Host::Ipv4(ip) => return Ok(vec![SocketAddr::new(IpAddr::V4(ip), port)]),
            url::Host::Ipv6(ip) => return Ok(vec![SocketAddr::new(IpAddr::V6(ip), port)]),
            url::Host::Domain(host) => host.to_owned(),
        };
        let pool = self
            .resolver
            .get_or_init(|| blocking::Pool::named("ae-parent-pin-dns", 1, 2, 16384).map_err(worker))
            .as_ref()
            .map_err(|e| *e)?;
        pool.submit(host.len() + 128, move |cancel| {
            if cancel.cancelled() {
                return Err(Error::Closed);
            }
            let addresses = match (host.as_str(), port).to_socket_addrs() {
                Ok(a) => a.take(16).collect(),
                Err(e) if exhausted(&e) => return Err(Error::ResourceLimit),
                Err(_) => vec![],
            };
            if cancel.cancelled() {
                return Err(Error::Closed);
            }
            Ok(addresses)
        })
        .map_err(worker)?
        .await
        .map_err(worker)?
    }
    async fn exchange(
        &self,
        node: &Node,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<Option<http::Response>, Error> {
        let addresses = self.addresses(node).await?;
        let mut stream = None;
        for address in addresses {
            match TcpStream::connect(address).await {
                Ok(tcp) => {
                    stream = Some(tcp);
                    break;
                }
                Err(e) if exhausted(&e) => return Err(Error::ResourceLimit),
                Err(_) => (),
            }
        }
        let Some(tcp) = stream else {
            return Ok(None);
        };
        let mut stream: Box<dyn Stream> = if node.origin.scheme() == "https" {
            let host = node
                .origin
                .host_str()
                .expect("checked host")
                .trim_matches(['[', ']'])
                .to_owned();
            let name =
                rustls::pki_types::ServerName::try_from(host).map_err(|_| Error::Configuration)?;
            match tokio_rustls::TlsConnector::from(self.tls.clone())
                .connect(name, tcp)
                .await
            {
                Ok(tls) => Box::new(tls),
                Err(e) if exhausted(&e) => return Err(Error::ResourceLimit),
                Err(_) => return Ok(None),
            }
        } else {
            Box::new(tcp)
        };
        let authority = &node.origin[url::Position::BeforeHost..url::Position::AfterPort];
        let mut headers = format!(
            "{} {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n",
            if body.is_some() { "POST" } else { "GET" }
        );
        if let Some(body) = body {
            if body.len() > self.config.maximum_body_bytes {
                return Err(Error::ResourceLimit);
            }
            let (user, password) = node.credentials.as_ref().ok_or(Error::Configuration)?;
            let mut plain = user.clone();
            plain.push(b':');
            plain.extend_from_slice(password);
            use zeroize::Zeroize;
            let auth = base64::engine::general_purpose::STANDARD.encode(&plain);
            plain.zeroize();
            headers.push_str(&format!("Authorization: Basic {auth}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n", body.len()));
        }
        headers.push_str("\r\n");
        if headers.len() > self.config.maximum_header_bytes {
            return Err(Error::ResourceLimit);
        }
        for bytes in [Some(headers.as_bytes()), body].into_iter().flatten() {
            match http::write(&mut stream, bytes).await {
                Ok(()) => (),
                Err(http::Error::Limit) => return Err(Error::ResourceLimit),
                Err(_) => return Ok(None),
            }
        }
        match http::read(
            &mut stream,
            &http::Limits {
                header_bytes: self.config.maximum_header_bytes,
                body_bytes: self.config.maximum_body_bytes,
            },
        )
        .await
        {
            Ok(reply) => Ok(Some(reply.response)),
            Err(http::Error::Limit | http::Error::Io(std::io::ErrorKind::OutOfMemory)) => {
                Err(Error::ResourceLimit)
            }
            Err(_) => Ok(None),
        }
    }
    pub(crate) async fn response(
        &self,
        node: &Node,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<Option<http::Response>, Error> {
        if path
            .bytes()
            .any(|b| b <= 32 || b >= 127 || matches!(b, b'#' | b'\\'))
        {
            return Ok(None);
        }
        let reply =
            match timeout(self.config.request_timeout, self.exchange(node, path, body)).await {
                Ok(reply) => reply?,
                Err(_) => return Ok(None), // native HTTP timeout is a failed lookup
            };
        // Redirect policy is an acquisition capability boundary, not evidence
        // that a pin is invalid. A supervisor can fix the configured endpoint.
        if reply
            .as_ref()
            .is_some_and(|r| matches!(r.status, 300..=308))
        {
            return Err(Error::Redirect);
        }
        Ok(reply)
    }
    pub async fn request(
        &self,
        node: &Node,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<Option<serde_json::Value>, Error> {
        let Some(reply) = self
            .response(node, path, body)
            .await?
            .filter(|r| r.status == 200)
        else {
            return Ok(None);
        };
        match serde_json::from_slice(&reply.body) {
            Ok(json) => Ok(Some(json)),
            Err(e) if e.to_string().starts_with("recursion limit exceeded") => {
                Err(Error::ResourceLimit)
            }
            Err(_) => Ok(None),
        }
    }
}
