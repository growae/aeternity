use crate::{pool::Connection, tasks::Tasks};
use ae_p2p::{
    discovery::Peer,
    message::{Kind, Value},
    noise,
    registry::{Info, Token},
    runtime as net,
};
use ae_types::PeerPublicKey;
use std::{collections::BTreeMap, io, net::Ipv4Addr, sync::Arc, time::Duration};
use tokio::{net::TcpStream, time::Instant};

#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub connections: usize,
    pub connect_timeout: Duration,
    pub first_ping_timeout: Duration,
    pub pre_admission_events: usize,
    pub runtime: net::Config,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            connections: 512,
            connect_timeout: Duration::from_secs(1),
            first_ping_timeout: Duration::from_secs(30),
            pre_admission_events: 64,
            runtime: Default::default(),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Connect,
    FirstPing,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Duplicate,
    Stopped,
    Address,
    InvalidPort,
    AdmissionLimit,
    Timeout(Stage),
    Io(io::ErrorKind),
    Runtime(net::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error.kind())
    }
}
impl From<net::Error> for Error {
    fn from(error: net::Error) -> Self {
        Self::Runtime(error)
    }
}

pub struct Ready {
    pub token: Token,
    pub peer: Info,
    pub address: Ipv4Addr,
    pub inbound: bool,
    pub connection: Connection,
}
pub struct Authenticated {
    pub token: Token,
    pub address: Ipv4Addr,
    pub deadline: Instant,
    pub connection: Connection,
}
pub enum Event {
    Ready(Box<Ready>),
    Authenticated(Box<Authenticated>),
    AdmissionExpired {
        token: Token,
        peer: PeerPublicKey,
    },
    Probe {
        token: Token,
        peer: PeerPublicKey,
        result: Result<(), Error>,
    },
    Failed {
        token: Token,
        // Inbound failures before authentication have no trustworthy identity.
        peer: Option<PeerPublicKey>,
        error: Error,
    },
    Closed {
        token: Token,
        peer: PeerPublicKey,
        exit: net::Exit,
    },
}
#[derive(Clone, Copy)]
enum Attempt {
    Dial(PeerPublicKey),
    Probe(PeerPublicKey),
    Inbound,
}
enum Completed {
    Ready(Box<Ready>),
    Authenticated(Box<Authenticated>),
    Probe,
}

// A single consumer drives next(). No sockets or tasks run in the background.
// Retained policy deliveries must be acknowledged after submission and before
// consuming their completion; retired tokens must never be submitted again.
pub struct Sockets {
    identity: Arc<noise::Identity>,
    network: Arc<noise::Network>,
    settings: Settings,
    attempts: BTreeMap<Token, Attempt>,
    work: Tasks<Token, Result<Completed, Error>>,
    clients: BTreeMap<Token, net::Client>,
    terminals: Tasks<Token, net::Exit>,
    admissions: BTreeMap<Token, Instant>,
    stopped: bool,
}
impl Sockets {
    pub fn new(
        identity: Arc<noise::Identity>,
        network: Arc<noise::Network>,
        settings: Settings,
    ) -> Result<Self, Error> {
        if settings.connections == 0
            || settings.connections > 65_536
            || settings.pre_admission_events == 0
            || settings.pre_admission_events > 65_536
            || [settings.connect_timeout, settings.first_ping_timeout]
                .iter()
                .any(|d| d.is_zero() || Instant::now().checked_add(*d).is_none())
        {
            return Err(Error::Configuration);
        }
        settings.runtime.validate()?;
        Ok(Self {
            identity,
            network,
            settings,
            attempts: BTreeMap::new(),
            work: Tasks::new(),
            clients: BTreeMap::new(),
            terminals: Tasks::new(),
            admissions: BTreeMap::new(),
            stopped: false,
        })
    }
    pub fn occupied(&self) -> usize {
        self.attempts.len() + self.clients.len()
    }
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }
    fn reserve(&self, token: Token) -> Result<(), Error> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        if self.attempts.contains_key(&token) || self.clients.contains_key(&token) {
            return Err(Error::Duplicate);
        }
        if self.occupied() >= self.settings.connections {
            return Err(Error::Busy);
        }
        Ok(())
    }
    // Both ordinary dials and TCP-only probes use the already resolved address.
    // No second hostname lookup may change the policy-selected destination.
    pub fn dial(&mut self, token: Token, peer: Peer, probe: bool) -> Result<(), Error> {
        self.reserve(token)?;
        let attempt = if probe {
            Attempt::Probe(peer.id)
        } else {
            Attempt::Dial(peer.id)
        };
        let settings = self.settings;
        let identity = self.identity.clone();
        let network = self.network.clone();
        self.work.insert(token, async move {
            let socket =
                tokio::time::timeout(settings.connect_timeout, TcpStream::connect(peer.socket()))
                    .await
                    .map_err(|_| Error::Timeout(Stage::Connect))??;
            if probe {
                // Native tcp probes stop on connect, without writing Noise or
                // ping bytes. Close the socket before reporting reachability.
                drop(socket);
                return Ok(Completed::Probe);
            }
            let (client, events, driver) = net::establish(
                socket,
                &identity,
                &network,
                noise::Role::Initiator(peer.id),
                settings.runtime,
            )
            .await?;
            let connection =
                Connection::new(client, events, driver).map_err(|_| Error::Configuration)?;
            Ok(Completed::Ready(Box::new(Ready {
                token,
                peer: Info::from(&peer),
                address: peer.address,
                inbound: false,
                connection,
            })))
        });
        self.attempts.insert(token, attempt);
        Ok(())
    }
    // The listener must obtain a registry ReserveInbound token before calling
    // this method. Every failure still requires that reservation to be retired
    // by the policy owner. Rejection drops the supplied socket immediately.
    pub fn inbound(&mut self, token: Token, socket: TcpStream) -> Result<(), Error> {
        self.inbound_mode(token, socket, false)
    }
    pub(crate) fn inbound_authenticated(
        &mut self,
        token: Token,
        socket: TcpStream,
    ) -> Result<(), Error> {
        if !self.settings.runtime.defer_pings {
            return Err(Error::Configuration);
        }
        self.inbound_mode(token, socket, true)
    }
    fn inbound_mode(&mut self, token: Token, socket: TcpStream, early: bool) -> Result<(), Error> {
        self.reserve(token)?;
        let std::net::SocketAddr::V4(remote) = socket.peer_addr()? else {
            return Err(Error::Address);
        };
        let address = *remote.ip();
        let settings = self.settings;
        let identity = self.identity.clone();
        let network = self.network.clone();
        self.work.insert(token, async move {
            let (client, events, driver) = net::establish(
                socket,
                &identity,
                &network,
                noise::Role::Responder,
                settings.runtime,
            )
            .await?;
            let id = client.peer();
            let mut connection =
                Connection::new(client, events, driver).map_err(|_| Error::Configuration)?;
            let deadline = Instant::now()
                .checked_add(settings.first_ping_timeout)
                .ok_or(Error::Configuration)?;
            if early {
                return Ok(Completed::Authenticated(Box::new(Authenticated {
                    token,
                    address,
                    deadline,
                    connection,
                })));
            }
            // Native first-ping timeout starts only after the Noise handshake.
            let port = tokio::time::timeout_at(deadline, async {
                for _ in 0..settings.pre_admission_events {
                    let event = connection.admission_event().await?;
                    let port = match &event {
                        net::Event::Message(received)
                            if received.message().kind() == Kind::Ping =>
                        {
                            Some(advertised_port(received.message())?)
                        }
                        _ => None,
                    };
                    connection.retain_admission_event(event);
                    if let Some(port) = port {
                        return Ok(port);
                    }
                }
                Err(Error::AdmissionLimit)
            })
            .await
            .map_err(|_| Error::Timeout(Stage::FirstPing))??;
            Ok(Completed::Ready(Box::new(Ready {
                token,
                peer: Info {
                    id,
                    host: address.to_string().into_bytes(),
                    port,
                },
                address,
                inbound: true,
                connection,
            })))
        });
        self.attempts.insert(token, Attempt::Inbound);
        Ok(())
    }
    // Cancelling a pending attempt synchronously drops its socket/driver.
    // After handoff cancellation is a request; retain the slot until Closed.
    pub fn cancel(&mut self, token: Token) -> bool {
        self.admissions.remove(&token);
        if self.attempts.remove(&token).is_some() {
            self.work.remove(&token);
            return true;
        }
        if let Some(client) = self.clients.get(&token) {
            client.shutdown();
            return true;
        }
        false
    }
    pub fn stop(&mut self) {
        self.stopped = true;
        self.work.clear();
        self.attempts.clear();
        self.admissions.clear();
        for client in self.clients.values() {
            client.shutdown();
        }
    }
    pub(crate) fn begin_admission(&mut self, token: Token) -> bool {
        let Some(deadline) = self.admissions.remove(&token) else {
            return false;
        };
        if deadline <= Instant::now() {
            self.cancel(token);
            return false;
        }
        self.clients.get(&token).is_some_and(|c| !c.is_closed())
    }
    fn track(&mut self, token: Token, client: net::Client) {
        let monitor = client.monitor();
        self.clients.insert(token, client);
        self.terminals
            .insert(token, async move { monitor.wait().await });
    }
    // A Ready connection must be polled by its new owner (or dropped). Continue
    // next() after stop to observe actual handed-off driver teardown. Neither
    // stop nor Drop can join drivers that another service currently owns.
    pub async fn next(&mut self) -> Option<Event> {
        if self.stopped && self.occupied() == 0 {
            return None;
        }
        let deadline = self
            .admissions
            .iter()
            .min_by_key(|(_, d)| *d)
            .map(|(token, d)| (*token, *d));
        tokio::select! {
            _ = async {
                match deadline {
                    Some((_, d)) => tokio::time::sleep_until(d).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                let (token, _) = deadline.unwrap();
                let peer = self.clients[&token].peer();
                self.cancel(token);
                Some(Event::AdmissionExpired { token, peer })
            },
            (token, result) = self.work.next() => {
                let attempt = self.attempts.remove(&token).expect("owned attempt");
                Some(match (attempt, result) {
                    (_, Ok(Completed::Ready(ready))) => {
                        let client = ready.connection.client().clone();
                        self.track(token, client);
                        Event::Ready(ready)
                    }
                    (_, Ok(Completed::Authenticated(ready))) => {
                        self.track(token, ready.connection.client().clone());
                        self.admissions.insert(token, ready.deadline);
                        Event::Authenticated(ready)
                    }
                    (Attempt::Probe(peer), result) => Event::Probe {
                        token, peer, result: result.map(|_| ()),
                    },
                    (attempt, Err(error)) => Event::Failed {
                        token,
                        peer: match attempt {
                            Attempt::Dial(peer) => Some(peer),
                            _ => None,
                        },
                        error,
                    },
                    (_, Ok(Completed::Probe)) => unreachable!("probe attempt"),
                })
            }
            (token, exit) = self.terminals.next() => {
                self.admissions.remove(&token);
                let client = self.clients.remove(&token).expect("owned generation");
                debug_assert_eq!(client.generation(), &exit.generation);
                Some(Event::Closed { token, peer: client.peer(), exit })
            }
        }
    }
}
impl Drop for Sockets {
    fn drop(&mut self) {
        self.stop();
    }
}
pub(crate) fn advertised_port(message: &ae_p2p::message::Message) -> Result<u16, Error> {
    let Some(Value::Integer(port)) = message.fields().get("port") else {
        return Err(Error::InvalidPort);
    };
    port.to_u64()
        .ok()
        .and_then(|n| u16::try_from(n).ok())
        .ok_or(Error::InvalidPort)
}

#[cfg(test)]
mod tests;
