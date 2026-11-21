use crate::{framing, message, noise, requests};
use ae_types::PeerPublicKey;
use std::{fmt, io, sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    sync::{Semaphore, mpsc, watch},
    time::Instant,
};

mod driver;
mod lifecycle;
mod queue;
mod wire;
pub use driver::Driver;
pub use lifecycle::Monitor;
pub use queue::{
    Client, Event, Generation, PingObservation, PingRetirement, Prepared, Received, Reply, Ticket,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Closed,
    Cancelled,
    Shutdown,
    Timeout,
    Backpressure,
    RemoteClose,
    LocalClose,
    InvalidPing,
    Request(requests::Error),
    RequestFailed(requests::Failure),
    Noise(noise::Error),
    Frame(framing::Error),
    Message(message::Error),
    Io(io::ErrorKind),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value.kind())
    }
}
impl From<noise::Error> for Error {
    fn from(value: noise::Error) -> Self {
        Self::Noise(value)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub commands: usize,
    pub events: usize,
    pub outgoing_bytes: usize,
    pub incoming_bytes: usize,
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
    pub partial_timeout: Duration,
    pub messages: message::Limits,
    pub fragments: framing::Limits,
    pub requests: requests::Limits,
    pub minimum_ping: u16,
    pub defer_pings: bool,
    pub ping_timeout: Duration,
    pub close_timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            commands: 64,
            events: 64,
            outgoing_bytes: 64 * 1024 * 1024,
            incoming_bytes: 64 * 1024 * 1024,
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(30),
            partial_timeout: Duration::from_secs(30),
            messages: message::Limits::default(),
            fragments: framing::Limits::default(),
            requests: requests::Limits::default(),
            minimum_ping: 1,
            defer_pings: false,
            ping_timeout: Duration::from_secs(60),
            close_timeout: Duration::from_secs(3),
        }
    }
}
impl Config {
    pub fn validate(self) -> Result<(), Error> {
        let max = Semaphore::MAX_PERMITS.min(u32::MAX as usize);
        if self.commands == 0
            || self.commands > max
            || self.events == 0
            || self.events > max
            || self.outgoing_bytes < 2
            || self.outgoing_bytes > max
            || self.incoming_bytes < 2
            || self.incoming_bytes > max
            || self.fragments.message_bytes < 2
            || self.fragments.fragments == 0
        {
            return Err(Error::Configuration);
        }
        requests::Requests::new(self.requests, self.minimum_ping).map_err(Error::Request)?;
        for duration in [
            self.handshake_timeout,
            self.write_timeout,
            self.partial_timeout,
            self.ping_timeout,
            self.close_timeout,
        ] {
            if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
                return Err(Error::Configuration);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exit {
    pub generation: Generation,
    pub reason: Error,
}

pub struct Events(mpsc::Receiver<Event>);
impl Events {
    pub async fn recv(&mut self) -> Option<Event> {
        self.0.recv().await
    }
    pub fn try_recv(&mut self) -> Result<Event, mpsc::error::TryRecvError> {
        self.0.try_recv()
    }
}

pub async fn establish(
    socket: TcpStream,
    identity: &noise::Identity,
    network: &noise::Network,
    role: noise::Role,
    config: Config,
) -> Result<(Client, Events, Driver), Error> {
    config.validate()?;
    let requests =
        requests::Requests::new(config.requests, config.minimum_ping).map_err(Error::Request)?;
    socket.set_nodelay(true)?;
    let crypto =
        wire::handshake(&socket, identity, network, role, config.handshake_timeout).await?;
    let peer: PeerPublicKey = crypto.peer();
    let generation = Generation::new();
    let (commands, receiver) = mpsc::channel(config.commands);
    // Earlier non-ping callbacks may still need to send replies while a ping
    // waits in their application queue. They share the outgoing byte budget.
    let (replies, reply_commands) = mpsc::channel(config.commands);
    let (finishes, finishing) = mpsc::channel(1);
    let (events, incoming) = mpsc::channel(config.events);
    let (stop, stopped) = watch::channel(None);
    let (disconnect, disconnecting) = watch::channel(false);
    let (guard, monitor) = lifecycle::Guard::new(peer, generation.clone());
    let selected_ping = Arc::new(std::sync::atomic::AtomicU16::new(0));
    let client = Client::new(
        commands,
        replies,
        finishes,
        stop.clone(),
        disconnect,
        generation.clone(),
        peer,
        config,
        monitor,
        selected_ping.clone(),
    );
    let driver = Driver::new(
        socket,
        crypto,
        receiver,
        reply_commands,
        finishing,
        events,
        stopped,
        stop,
        disconnecting,
        generation,
        requests,
        config,
        selected_ping,
        guard,
    );
    Ok((client, Events(incoming), driver))
}

#[cfg(test)]
mod tests;
