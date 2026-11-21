use crate::discovery::{self, Class, Peer, Pool, Selection, Target, Update};
use ae_types::PeerPublicKey;
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng, seq::SliceRandom};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{Ipv4Addr, SocketAddrV4},
    sync::atomic::{AtomicU64, Ordering},
};
use zeroize::Zeroizing;

mod connections;
mod resolver;
mod state;
#[cfg(test)]
mod tests;
mod timers;

static NEXT_REGISTRY: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    ResourceLimit,
    TimeReversed,
    TimeOverflow,
    StalePlan,
    Closed,
    Entropy,
    Pool(discovery::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<discovery::Error> for Error {
    fn from(e: discovery::Error) -> Self {
        Self::Pool(e)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub pool: discovery::Config,
    pub maximum_inbound: usize,
    pub maximum_outbound: usize,
    pub maximum_probes: usize,
    pub maximum_temporary: usize,
    pub maximum_pending_inbound: usize,
    pub maximum_connections: usize,
    pub maximum_blocked: usize,
    pub maximum_hosts: usize,
    pub maximum_peers_per_host: usize,
    pub maximum_resolving_peers: usize,
    pub maximum_exclusions: usize,
    pub resolver_retries: u64,
    pub resolver_backoff: Vec<u64>,
    pub resolution_timeout: u64,
    pub admission_timeout: u64,
    pub attempt_timeout: u64,
    pub ping_interval: u64,
    pub unblock_interval: u64,
    pub single_outbound_per_group: bool,
    pub analytics: bool,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            pool: Default::default(),
            maximum_inbound: 100,
            maximum_outbound: 10,
            maximum_probes: 100,
            maximum_temporary: 128,
            maximum_pending_inbound: 128,
            maximum_connections: 512,
            maximum_blocked: 4096,
            maximum_hosts: 100,
            maximum_peers_per_host: 128,
            maximum_resolving_peers: 4096,
            maximum_exclusions: 4096,
            resolver_retries: 7,
            resolver_backoff: vec![5000, 15000, 30000, 60000, 120000, 300000, 600000],
            resolution_timeout: 30_000,
            admission_timeout: 30_000,
            attempt_timeout: 30_000,
            ping_interval: 120_000,
            unblock_interval: 900_000,
            single_outbound_per_group: true,
            analytics: false,
        }
    }
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        let direction_limit = if self.analytics { 1_000_000 } else { 65_536 };
        if self.maximum_inbound > direction_limit || self.maximum_outbound > direction_limit {
            return Err(Error::Configuration);
        }
        for n in [
            self.maximum_probes,
            self.maximum_temporary,
            self.maximum_pending_inbound,
            self.maximum_connections,
            self.maximum_blocked,
            self.maximum_hosts,
            self.maximum_peers_per_host,
            self.maximum_resolving_peers,
            self.maximum_exclusions,
        ] {
            if n > 65_536 {
                return Err(Error::Configuration);
            }
        }
        if self.maximum_connections == 0
            || self.resolver_backoff.is_empty()
            || self.resolver_backoff.len() > 1024
            || self.resolver_backoff.contains(&0)
            || [
                self.resolution_timeout,
                self.admission_timeout,
                self.attempt_timeout,
                self.ping_interval,
                self.unblock_interval,
            ]
            .contains(&0)
            || self.ping_interval.checked_mul(3).is_none()
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
    fn maximum_delay(&self) -> u64 {
        *self
            .resolver_backoff
            .iter()
            .chain(self.pool.standby_times.iter())
            .chain([
                &self.resolution_timeout,
                &self.admission_timeout,
                &self.attempt_timeout,
                &self.ping_interval,
                &self.unblock_interval,
            ])
            .max()
            .unwrap()
            .max(&(self.ping_interval * 3 / 2))
            .max(&30_000)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub id: PeerPublicKey,
    pub host: Vec<u8>,
    pub port: u16,
}
impl From<&Peer> for Info {
    fn from(p: &Peer) -> Self {
        Self {
            id: p.id,
            host: p.host.clone(),
            port: p.port,
        }
    }
}
impl Info {
    fn resolved(self, address: Ipv4Addr, source: Option<Ipv4Addr>, trusted: bool) -> Peer {
        Peer {
            id: self.id,
            host: self.host,
            port: self.port,
            address,
            source: source.unwrap_or(address),
            trusted,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Token {
    registry: u64,
    sequence: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Inbound,
    Outbound,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Connecting,
    Connected,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Permanent,
    Temporary,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    pub peer: Peer,
    pub token: Token,
    pub direction: Direction,
    pub status: Status,
    pub kind: Kind,
    pub probe: Option<Class>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    Start,
    Failed,
    Closed,
    Down,
    Dead,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    Blocked,
    AlreadyConnected,
    TrustedAddressChanged,
    TrustedPending,
    Capacity,
    Ignored,
    Invalid,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Done,
    Ignored,
    Stale,
    Bool(bool),
    Updated(Update),
    Reserved(Token),
    Accepted(Kind),
    Rejected(Rejection),
    Selected(Token),
    Wait(u64),
    Unavailable,
    Peers(Vec<Info>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    Store(discovery::Effect),
    Dial {
        token: Token,
        peer: Peer,
        probe: Option<Class>,
    },
    Close(Token),
    Ping {
        token: Token,
        peer: PeerPublicKey,
    },
    Resolve {
        token: Token,
        host: Vec<u8>,
    },
    CancelResolve(Token),
    Blocked(PeerPublicKey),
    Unblocked(PeerPublicKey),
    ProbeStatus {
        peer: PeerPublicKey,
        alive: bool,
    },
}
impl Effect {
    pub fn retained_bytes(&self) -> Result<usize, Error> {
        let heap = match self {
            Self::Dial { peer, .. } => peer.host.len(),
            Self::Resolve { host, .. } => host.len(),
            Self::Store(discovery::Effect::Persist(peer) | discovery::Effect::Delete(peer)) => {
                peer.host.len()
            }
            _ => 0,
        };
        std::mem::size_of::<Self>()
            .checked_add(heap)
            .ok_or(Error::ResourceLimit)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub result: Outcome,
    pub effects: Vec<Effect>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Add(Peer),
    Delete(PeerPublicKey),
    Block(Info),
    Unblock(PeerPublicKey),
    IsBlocked(PeerPublicKey),
    Resolve {
        peer: Info,
        source: Option<Ipv4Addr>,
        trusted: bool,
    },
    Resolved {
        token: Token,
        address: Option<Ipv4Addr>,
    },
    Connect(Option<PeerPublicKey>),
    Probe(Class),
    ReserveInbound {
        address: Ipv4Addr,
    },
    Accept {
        token: Token,
        peer: Info,
    },
    AbandonInbound(Token),
    RejectInbound {
        token: Token,
        reason: Rejection,
    },
    Connected {
        peer: PeerPublicKey,
        token: Token,
    },
    Alive {
        peer: PeerPublicKey,
        token: Token,
    },
    Failed {
        peer: PeerPublicKey,
        token: Token,
        reason: Failure,
    },
    Suspect(PeerPublicKey),
    PingDone {
        peer: PeerPublicKey,
        token: Token,
    },
    Timeout(Token),
    Sample {
        maximum: usize,
        percent_connected: Option<u8>,
        exclude: Vec<PeerPublicKey>,
    },
    SampleConnected {
        maximum: usize,
        direction: Option<Direction>,
    },
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Counts {
    pub inbound: usize,
    pub outbound: usize,
    pub probes: usize,
    pub temporary: usize,
    pub pending_inbound: usize,
    pub blocked: usize,
    pub hosts: usize,
    pub resolving_peers: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timer {
    pub token: Token,
    pub deadline: u64,
}
#[derive(Clone)]
struct OwnedConnection {
    connection: Connection,
    timer: Option<Token>,
}
#[derive(Clone)]
struct PendingInbound {
    address: Ipv4Addr,
    timer: Token,
}
#[derive(Clone)]
struct ResolutionPeer {
    info: Info,
    source: Option<Ipv4Addr>,
    trusted: bool,
}
#[derive(Clone)]
struct Resolution {
    peers: BTreeMap<PeerPublicKey, ResolutionPeer>,
    retries: u64,
    attempt: Option<Token>,
    timer: Token,
}
#[derive(Clone)]
enum TimerKind {
    Connect,
    Probe(Class),
    Owner(PeerPublicKey, Token),
    Inbound(Token),
    Resolve(Vec<u8>),
}
#[derive(Clone)]
struct Deadline {
    at: u64,
    kind: TimerKind,
}
#[derive(Clone)]
struct State {
    local: PeerPublicKey,
    config: Config,
    pool: Pool,
    random: StdRng,
    connections: BTreeMap<PeerPublicKey, OwnedConnection>,
    sockets: BTreeMap<SocketAddrV4, BTreeSet<PeerPublicKey>>,
    blocked: BTreeMap<PeerPublicKey, crate::address::Address>,
    pending: BTreeMap<Token, PendingInbound>,
    hosts: BTreeMap<Vec<u8>, Resolution>,
    probe_status: BTreeMap<PeerPublicKey, bool>,
    timers: BTreeMap<Token, Deadline>,
    connect_timer: Option<Token>,
    probe_timers: [Option<Token>; 2],
    last_connect: Option<u64>,
    last_probe: [Option<u64>; 2],
    next_unblock: Option<u64>,
    now: u64,
    registry: u64,
    sequence: u64,
    closed: bool,
}

pub struct Registry {
    state: State,
    revision: u64,
}
pub struct Prepared {
    registry: u64,
    revision: u64,
    next: State,
    change: Change,
}
impl Prepared {
    pub fn persistence(&self) -> impl Iterator<Item = &discovery::Effect> {
        self.change.effects.iter().filter_map(|e| match e {
            Effect::Store(e) => Some(e),
            _ => None,
        })
    }
    pub fn result(&self) -> &Outcome {
        &self.change.result
    }
    // Expose budgets and liveness, not executable payloads, before publication.
    pub fn effect_budget(&self) -> Result<(usize, usize), Error> {
        self.change
            .effects
            .iter()
            .filter(|e| self.next.effect_is_current(e))
            .try_fold((0usize, 0usize), |(count, bytes), e| {
                Ok((
                    count.checked_add(1).ok_or(Error::ResourceLimit)?,
                    bytes
                        .checked_add(e.retained_bytes()?)
                        .ok_or(Error::ResourceLimit)?,
                ))
            })
    }
    pub fn effect_is_current(&self, effect: &Effect) -> bool {
        self.next.effect_is_current(effect)
    }
}
impl State {
    fn effect_is_current(&self, effect: &Effect) -> bool {
        match effect {
            Effect::Dial { token, peer, probe } => {
                self.connections.get(&peer.id).is_some_and(|c| {
                    c.connection.token == *token
                        && c.connection.status == Status::Connecting
                        && c.connection.probe == *probe
                })
            }
            Effect::Ping { token, peer } => self.connections.get(peer).is_some_and(|c| {
                c.connection.token == *token
                    && c.connection.status == Status::Connected
                    && c.connection.probe.is_none()
            }),
            Effect::Resolve { token, host } => self
                .hosts
                .get(host)
                .is_some_and(|r| r.attempt == Some(*token)),
            Effect::Store(_) => false,
            _ => true,
        }
    }
}
impl Registry {
    pub fn new(local: PeerPublicKey, config: Config) -> Result<Self, Error> {
        config.validate()?;
        let pool = Pool::new(config.pool.clone())?;
        let mut seed = Zeroizing::new([0u8; 32]);
        rand::rngs::OsRng
            .try_fill_bytes(seed.as_mut())
            .map_err(|_| Error::Entropy)?;
        Self::from_pool(local, config, pool, *seed)
    }
    pub fn with_seed(
        local: PeerPublicKey,
        config: Config,
        secret: &[u8],
        seed: [u8; 32],
    ) -> Result<Self, Error> {
        config.validate()?;
        let pool = Pool::with_seed(config.pool.clone(), secret, seed)?;
        Self::from_pool(local, config, pool, seed)
    }
    fn from_pool(
        local: PeerPublicKey,
        config: Config,
        pool: Pool,
        seed: [u8; 32],
    ) -> Result<Self, Error> {
        let registry = NEXT_REGISTRY
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_add(1))
            .map_err(|_| Error::ResourceLimit)?;
        let seed = Zeroizing::new(seed);
        Ok(Self {
            revision: 0,
            state: State {
                local,
                config,
                pool,
                random: StdRng::from_seed(*seed),
                connections: BTreeMap::new(),
                sockets: BTreeMap::new(),
                blocked: BTreeMap::new(),
                pending: BTreeMap::new(),
                hosts: BTreeMap::new(),
                probe_status: BTreeMap::new(),
                timers: BTreeMap::new(),
                connect_timer: None,
                probe_timers: [None; 2],
                last_connect: None,
                last_probe: [None; 2],
                next_unblock: None,
                now: 0,
                registry,
                sequence: 0,
                closed: false,
            },
        })
    }
    pub fn prepare(&self, now: u64, command: Command) -> Result<Prepared, Error> {
        if self.state.closed {
            return Err(Error::Closed);
        }
        self.revision.checked_add(1).ok_or(Error::ResourceLimit)?;
        if now < self.state.now {
            return Err(Error::TimeReversed);
        }
        now.checked_add(self.state.config.maximum_delay())
            .ok_or(Error::TimeOverflow)?;
        self.state.validate_command(&command)?;
        let mut next = self.state.clone();
        next.now = now;
        let mut effects = vec![];
        let result = next.apply(command, &mut effects)?;
        next.reconcile();
        Ok(Prepared {
            registry: self.state.registry,
            revision: self.revision,
            next,
            change: Change { result, effects },
        })
    }
    // Startup block installation is one prepared transition and is available
    // only before any other policy command has been published.
    pub fn prepare_initial_blocks(
        &self,
        now: u64,
        peers: &[crate::address::Address],
    ) -> Result<Prepared, Error> {
        if self.revision != 0 || self.state.closed {
            return Err(Error::Configuration);
        }
        if peers.len() > self.state.config.maximum_blocked
            || peers
                .iter()
                .any(|p| p.host.len() > self.state.config.pool.maximum_host_bytes)
        {
            return Err(Error::ResourceLimit);
        }
        now.checked_add(self.state.config.maximum_delay())
            .ok_or(Error::TimeOverflow)?;
        let mut next = self.state.clone();
        next.now = now;
        let mut effects = vec![];
        for peer in peers {
            next.block_address(peer.clone(), &mut effects)?;
        }
        next.schedule_connect(None)?;
        next.reconcile();
        Ok(Prepared {
            registry: self.state.registry,
            revision: self.revision,
            next,
            change: Change {
                result: Outcome::Done,
                effects,
            },
        })
    }
    pub fn configuration(&self) -> &Config {
        &self.state.config
    }
    pub fn local_peer(&self) -> PeerPublicKey {
        self.state.local
    }
    pub fn resolution_capacity(&self, host: &[u8], peer: PeerPublicKey) -> bool {
        self.state.resolution_capacity(host, peer)
    }
    pub fn resolving_peer(&self, host: &[u8], peer: PeerPublicKey) -> bool {
        self.state
            .hosts
            .get(host)
            .is_some_and(|r| r.peers.contains_key(&peer))
    }
    // The owner must serialize preparation, durable persistence and publication.
    pub fn commit(&mut self, prepared: Prepared) -> Result<Change, Error> {
        if prepared.registry != self.state.registry || prepared.revision != self.revision {
            return Err(Error::StalePlan);
        }
        self.revision = self.revision.checked_add(1).ok_or(Error::ResourceLimit)?;
        self.state = prepared.next;
        Ok(prepared.change)
    }
    pub fn pool(&self) -> &Pool {
        &self.state.pool
    }
    pub fn counts(&self) -> Counts {
        self.state.counts()
    }
    pub fn connection(&self, id: PeerPublicKey) -> Option<&Connection> {
        self.state.connections.get(&id).map(|c| &c.connection)
    }
    pub fn connections(&self) -> impl Iterator<Item = &Connection> {
        self.state.connections.values().map(|c| &c.connection)
    }
    pub fn blocked(&self) -> impl Iterator<Item = &crate::address::Address> {
        self.state.blocked.values()
    }
    pub fn socket_peers(&self, socket: SocketAddrV4) -> impl Iterator<Item = PeerPublicKey> {
        self.state
            .sockets
            .get(&socket)
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
    }
    pub fn probe_status(&self, peer: PeerPublicKey) -> Option<bool> {
        self.state.probe_status.get(&peer).copied()
    }
    pub fn timers(&self) -> impl Iterator<Item = Timer> {
        self.state.timers.iter().map(|(&token, t)| Timer {
            token,
            deadline: t.at,
        })
    }
    pub fn is_closed(&self) -> bool {
        self.state.closed
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn effect_is_current(&self, effect: &Effect) -> bool {
        self.state.effect_is_current(effect)
    }
    pub fn connection_timer(&self, peer: PeerPublicKey) -> Option<Timer> {
        let token = self.state.connections.get(&peer)?.timer?;
        Some(Timer {
            token,
            deadline: self.state.timers.get(&token)?.at,
        })
    }
    pub fn resolution_timer(&self, host: &[u8]) -> Option<Timer> {
        let token = self.state.hosts.get(host)?.timer;
        Some(Timer {
            token,
            deadline: self.state.timers.get(&token)?.at,
        })
    }
}
