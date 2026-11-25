// Native configuration consumers at aeternity fa2a6d7. Resolution is pure;
// socket/worker activation belongs to the existing connection owner.
use super::resolution::{self, Resolved, Strategy};
use ae_p2p::{listener, ping};
use ae_sync::{registry::startup, runtime::connection};
use serde_json::{Value, json};
use std::{
    net::{IpAddr, SocketAddrV4},
    time::Duration,
};
mod optional;
pub use optional::Optional;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(resolution::Error),
    Field(&'static str),
    Range(&'static str),
    Address,
    UnsupportedAddress,
    Family,
}
impl From<resolution::Error> for Error {
    fn from(value: resolution::Error) -> Self {
        Self::Resolution(value)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "network configuration error: {self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transport {
    pub connect_timeout: Duration,
    pub first_ping_timeout: Duration,
    pub noise_handshake_timeout: Duration,
    pub close_timeout: Duration,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peers {
    pub ping_interval: u64,
    pub unblock_interval: u64,
    pub maximum_inbound: usize,
    pub maximum_outbound: usize,
    pub maximum_probes: usize,
    pub single_outbound_per_group: bool,
    pub resolver_retries: u64,
    pub resolver_backoff: Vec<u64>,
}
#[derive(Debug, Clone)]
pub struct Network {
    listen: SocketAddrV4,
    advertised_port: u16,
    listener: listener::Config,
    transport: Transport,
    peers: Peers,
    user_limits: ae_sync::analytics::NetworkLimits,
    ping: ping::Config,
    minimum_ping: u16,
    share_node_info: bool,
    pool_sync_ms: u64,
    peer_count_interval: Duration,
    startup: startup::Configuration,
}
fn number(value: &Value, name: &'static str) -> Result<u64, Error> {
    value.as_u64().ok_or(Error::Range(name))
}
fn size(value: &Value, name: &'static str) -> Result<usize, Error> {
    usize::try_from(number(value, name)?).map_err(|_| Error::Range(name))
}
fn port(value: &Value, name: &'static str) -> Result<u16, Error> {
    u16::try_from(number(value, name)?).map_err(|_| Error::Range(name))
}
fn flag(value: &Value, name: &'static str) -> Result<bool, Error> {
    value.as_bool().ok_or(Error::Field(name))
}
fn schema(r: &Resolved<'_>, path: &[&str]) -> Result<Value, Error> {
    r.find(path, &[Strategy::User, Strategy::SchemaDefault])?
        .ok_or(Error::Field("schema default"))
}
fn user_limits(r: &Resolved<'_>) -> Result<ae_sync::analytics::NetworkLimits, Error> {
    let read = |name| r.get(&["sync", name])?.map(|v| size(v, name)).transpose();
    Ok(ae_sync::analytics::NetworkLimits {
        inbound: read("max_inbound")?,
        outbound: read("max_outbound")?,
    })
}
impl Network {
    pub fn resolve(r: &Resolved<'_>) -> Result<Self, Error> {
        let read = |name, application, fallback| {
            r.user_or_application(&["sync", name], "aecore", application, &fallback)
        };
        let listening_port = port(&read("port", "sync_port", json!(3015))?, "port")?;
        // ext_sync_port/0 calls sync_port/0 for its fallback; the schema's
        // unrelated default must not overwrite a configured listening port.
        let advertised_port = port(
            &read("external_port", "ext_sync_port", json!(listening_port))?,
            "external_port",
        )?;
        let address = read("listen_address", "sync_listen_address", json!("0.0.0.0"))?;
        let address: IpAddr = address
            .as_str()
            .ok_or(Error::Field("listen_address"))?
            .parse()
            .map_err(|_| Error::Address)?;
        let IpAddr::V4(address) = address else {
            return Err(Error::UnsupportedAddress);
        };
        let listener = listener::Config {
            acceptors: size(
                &read("acceptors", "sync_acceptors", json!(10))?,
                "acceptors",
            )?,
            maximum_connections: size(
                &read("max_inbound_hard", "sync_max_inbound_hard", json!(1000))?,
                "max_inbound_hard",
            )?,
        };
        listener.validate().map_err(|_| Error::Range("acceptors"))?;
        let duration = |name, application, fallback| -> Result<Duration, Error> {
            let millis = number(&read(name, application, json!(fallback))?, name)?;
            // Current transport owners require positive operation deadlines.
            if millis == 0 {
                return Err(Error::Range(name));
            }
            Ok(Duration::from_millis(millis))
        };
        let transport = Transport {
            connect_timeout: duration("connect_timeout", "sync_connect_timeout", 1000)?,
            first_ping_timeout: duration("first_ping_timeout", "sync_first_ping_timeout", 30000)?,
            noise_handshake_timeout: duration("noise_hs_timeout", "sync_noise_hs_timeout", 5000)?,
            close_timeout: duration("close_timeout", "sync_close_timeout", 3000)?,
        };
        let backoff = read(
            "resolver_backoff_times",
            "sync_resolver_backoff_times",
            json!([5000, 15000, 30000, 60000, 120000, 300000, 600000]),
        )?;
        let resolver_backoff = backoff
            .as_array()
            .ok_or(Error::Field("resolver_backoff_times"))?
            .iter()
            .map(|v| number(v, "resolver_backoff_times"))
            .collect::<Result<Vec<_>, _>>()?;
        if resolver_backoff.is_empty()
            || resolver_backoff.len() > 1024
            || resolver_backoff.contains(&0)
        {
            return Err(Error::Range("resolver_backoff_times"));
        }
        let unblock = r
            .raw_application("aecore", "peer_unblock_interval")
            .cloned()
            .unwrap_or_else(|| json!(900000));
        let peers = Peers {
            ping_interval: number(
                &read("ping_interval", "ping_interval", json!(120000))?,
                "ping_interval",
            )?,
            unblock_interval: number(&unblock, "peer_unblock_interval")?,
            maximum_inbound: size(
                &read("max_inbound", "sync_max_inbound", json!(100))?,
                "max_inbound",
            )?,
            maximum_outbound: size(
                &read("max_outbound", "sync_max_outbound", json!(10))?,
                "max_outbound",
            )?,
            maximum_probes: size(
                &read("max_tcp_probes", "sync_max_tcp_probes", json!(100))?,
                "max_tcp_probes",
            )?,
            single_outbound_per_group: flag(
                &read(
                    "single_outbound_per_group",
                    "sync_single_outbound_per_group",
                    json!(true),
                )?,
                "single_outbound_per_group",
            )?,
            resolver_retries: number(
                &read(
                    "resolver_max_retries",
                    "sync_resolver_max_retries",
                    json!(7),
                )?,
                "resolver_max_retries",
            )?,
            resolver_backoff,
        };
        if peers.ping_interval == 0 || peers.ping_interval.checked_mul(3).is_none() {
            return Err(Error::Range("ping_interval"));
        }
        if peers.unblock_interval == 0 {
            return Err(Error::Range("peer_unblock_interval"));
        }
        if peers.maximum_probes > 65_536 {
            return Err(Error::Range("max_tcp_probes"));
        }
        let share = number(
            &read("gossiped_peers_count", "gossiped_peers_count", json!(32))?,
            "gossiped_peers_count",
        )?;
        let percent = number(
            &r.user_or_application(
                &["sync", "ping", "peer_percentage"],
                "aecore",
                "sync_ping_connected_peer_percentage",
                &json!(10),
            )?,
            "peer_percentage",
        )?;
        let ping = ping::Config {
            share,
            connected_percent: u8::try_from(percent)
                .map_err(|_| Error::Range("peer_percentage"))?,
            ..Default::default()
        };
        ping.validate().map_err(|_| Error::Range("ping"))?;
        let minimum_ping = port(
            &schema(r, &["sync", "minimum_ping_version"])?,
            "minimum_ping_version",
        )?;
        ae_p2p::requests::PingVersions::new(minimum_ping)
            .map_err(|_| Error::Range("minimum_ping_version"))?;
        Ok(Self {
            listen: SocketAddrV4::new(address, listening_port),
            advertised_port,
            listener,
            transport,
            peers,
            ping,
            minimum_ping,
            share_node_info: flag(
                &schema(r, &["sync", "provide_node_info"])?,
                "provide_node_info",
            )?,
            pool_sync_ms: number(
                &r.user_or_application(
                    &["mempool", "sync_interval"],
                    "aecore",
                    "mempool_sync_interval",
                    &json!(1800000),
                )?,
                "mempool.sync_interval",
            )?,
            peer_count_interval: Duration::from_millis(number(
                &read(
                    "log_peer_connection_count_interval",
                    "log_peer_connection_count_interval",
                    json!(300000),
                )?,
                "log_peer_connection_count_interval",
            )?),
            startup: r.peers()?,
            user_limits: user_limits(r)?,
        })
    }
    pub fn listen(&self) -> SocketAddrV4 {
        self.listen
    }
    pub fn advertised_port(&self) -> u16 {
        self.advertised_port
    }
    pub fn listener(&self) -> listener::Config {
        self.listener
    }
    pub fn transport(&self) -> Transport {
        self.transport
    }
    pub fn peers(&self) -> &Peers {
        &self.peers
    }
    pub fn ping(&self) -> ping::Config {
        self.ping
    }
    pub fn minimum_ping(&self) -> u16 {
        self.minimum_ping
    }
    pub fn share_node_info(&self) -> bool {
        self.share_node_info
    }
    pub fn pool_sync_ms(&self) -> u64 {
        self.pool_sync_ms
    }
    pub fn peer_count_interval(&self) -> Duration {
        self.peer_count_interval
    }

    pub fn apply(&self, config: &mut connection::Config) {
        config.analytics_limits = self.user_limits;
        let network = &mut config.network;
        network.listen = Some(self.listen);
        network.listener = self.listener;
        let transport = &mut network.transport;
        transport.connect_timeout = self.transport.connect_timeout;
        transport.first_ping_timeout = self.transport.first_ping_timeout;
        transport.runtime.handshake_timeout = self.transport.noise_handshake_timeout;
        transport.runtime.close_timeout = self.transport.close_timeout;
        transport.runtime.minimum_ping = self.minimum_ping;
        let policy = &mut network.registry.policy;
        policy.ping_interval = self.peers.ping_interval;
        policy.unblock_interval = self.peers.unblock_interval;
        policy.maximum_inbound = self.peers.maximum_inbound;
        policy.maximum_outbound = self.peers.maximum_outbound;
        policy.maximum_probes = self.peers.maximum_probes;
        policy.single_outbound_per_group = self.peers.single_outbound_per_group;
        policy.resolver_retries = self.peers.resolver_retries;
        policy
            .resolver_backoff
            .clone_from(&self.peers.resolver_backoff);
        network.pool.session.resync_ms = self.pool_sync_ms;
        network.startup.peers.clone_from(&self.startup.peers);
        network
            .startup
            .blocked_peers
            .clone_from(&self.startup.blocked_peers);
        network.startup.include_default_peers = self.startup.include_default_peers;
        config.peer.port = self.advertised_port;
        config.peer.ping.share = self.ping.share;
        config.peer.ping.connected_percent = self.ping.connected_percent;
        if self.share_node_info {
            config
                .peer
                .node_info
                .get_or_insert_with(ae_sync::peer::info::Metadata::current);
        } else {
            config.peer.node_info = None;
        }
        config.observer.peer_count_interval = Some(self.peer_count_interval);
    }
}
