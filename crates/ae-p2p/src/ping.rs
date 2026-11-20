// aec_peer_connection, aeternity fa2a6d7. Copyright (c) 2018,
// Aeternity Anstalt. ISC notice: repository LICENSE.
use crate::message::{self, ChainPoi, Details, Kind, Message, Peer, Value};
use ae_types::{Hash256, Unsigned};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Local {
    pub genesis: Hash256,
    pub best: Hash256,
    pub height: u64,
    pub difficulty: Unsigned,
    pub temporary: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub share: u64,
    pub connected_percent: u8,
    pub sync_start: i64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            share: 32,
            connected_percent: 10,
            sync_start: -100,
        }
    }
}
impl Config {
    pub fn validate(self) -> Result<(), Error> {
        if self.share > 32 || self.connected_percent > 100 {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    NotPing,
    Message(message::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<message::Error> for Error {
    fn from(e: message::Error) -> Self {
        Self::Message(e)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    BadPing,
    TooManyPeers,
    WrongGenesis,
}
impl Rejection {
    pub fn reason(self) -> &'static [u8] {
        match self {
            Self::BadPing => b"bad_ping_message",
            Self::TooManyPeers => b"too_many_peers_in_ping_message",
            Self::WrongGenesis => b"wrong_genesis_hash",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    Status {
        temporary: bool,
        genesis: Vec<u8>,
        best: Vec<u8>,
        difficulty: Unsigned,
    },
    Generation(Vec<u8>),
    SyncDone,
    Sync {
        best: Vec<u8>,
        difficulty: Unsigned,
    },
    Peers(Vec<Peer>),
    Capabilities(Vec<ChainPoi>),
    Pool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    // None means the original callback fails at malformed capabilities after
    // its earlier effects. It must not turn into a successful/error response.
    pub response: Option<Result<(), Rejection>>,
    pub effects: Vec<Effect>,
}
pub struct Remote<'a> {
    pub version: u16,
    pub share: &'a Unsigned,
    pub genesis: &'a [u8],
    pub best: &'a [u8],
    pub difficulty: &'a Unsigned,
    pub sync_allowed: bool,
    pub peers: &'a [Peer],
    pub capabilities: Option<&'a Result<Vec<ChainPoi>, message::Error>>,
}
impl<'a> Remote<'a> {
    pub fn from_message(message: &'a Message) -> Result<Self, Error> {
        let Details::Ping {
            peers,
            capabilities,
        } = message.details()
        else {
            return Err(Error::NotPing);
        };
        let fields = message.fields();
        let Value::Integer(share) = &fields["share"] else {
            return Err(Error::NotPing);
        };
        let Value::Binary(genesis) = &fields["genesis_hash"] else {
            return Err(Error::NotPing);
        };
        let Value::Binary(best) = &fields["best_hash"] else {
            return Err(Error::NotPing);
        };
        let Value::Integer(difficulty) = &fields["difficulty"] else {
            return Err(Error::NotPing);
        };
        let Value::Boolean(sync_allowed) = &fields["sync_allowed"] else {
            return Err(Error::NotPing);
        };
        Ok(Self {
            version: message.version(),
            share,
            genesis,
            best,
            difficulty,
            sync_allowed: *sync_allowed,
            peers,
            capabilities: capabilities.as_ref(),
        })
    }
}
// This decision layer never treats an announced top/work as validated chain
// data. The ordered effects must be executed by the owned application service.
pub fn evaluate(
    local: &Local,
    message: &Message,
    negotiated: Option<u16>,
    top_target: u64,
    config: Config,
) -> Result<Decision, Error> {
    config.validate()?;
    let remote = Remote::from_message(message)?;
    let mut decision = Decision {
        response: Some(Ok(())),
        effects: vec![],
    };
    if negotiated.is_none() && remote.version > 1 {
        return Ok(decision);
    }
    let rejection = if Unsigned::from(remote.peers.len() as u64) > *remote.share {
        Some(Rejection::BadPing)
    } else if *remote.share > Unsigned::from(32) {
        Some(Rejection::TooManyPeers)
    } else {
        None
    };
    let mut pool = false;
    if let Some(rejection) = rejection {
        decision.response = Some(Err(rejection));
    } else {
        decision.effects.push(Effect::Status {
            temporary: !remote.sync_allowed,
            genesis: remote.genesis.to_vec(),
            best: remote.best.to_vec(),
            difficulty: remote.difficulty.clone(),
        });
        if remote.genesis != local.genesis.0 {
            decision.response = Some(Err(Rejection::WrongGenesis));
        } else {
            if remote.sync_allowed && !local.temporary {
                if remote.best == local.best.0 {
                    decision
                        .effects
                        .push(Effect::Generation(remote.best.to_vec()));
                    decision.effects.push(Effect::SyncDone);
                    pool = true;
                } else if local.difficulty > *remote.difficulty {
                    decision.effects.push(Effect::SyncDone);
                    pool = true;
                } else {
                    decision.effects.push(Effect::Sync {
                        best: remote.best.to_vec(),
                        difficulty: remote.difficulty.clone(),
                    });
                    pool = pool_ready(local.height, top_target, config.sync_start);
                }
            }
            decision.effects.push(Effect::Peers(remote.peers.to_vec()));
        }
    }
    match remote.capabilities {
        Some(Err(_)) => {
            decision.response = None;
            return Ok(decision);
        }
        Some(Ok(caps)) if !caps.is_empty() => {
            decision.effects.push(Effect::Capabilities(caps.clone()))
        }
        _ => (),
    }
    if pool {
        decision.effects.push(Effect::Pool);
    }
    Ok(decision)
}
pub fn pool_ready(local_height: u64, top_target: u64, sync_start: i64) -> bool {
    if top_target == 0 {
        return false;
    }
    let threshold = if sync_start < 0 {
        top_target.saturating_sub(sync_start.unsigned_abs())
    } else {
        sync_start as u64
    };
    local_height >= threshold
}

// Erlang sorts peer maps by their keys (host, port, pubkey) and values. Numeric
// port order is arbitrary-precision numeric order, not lexical byte order.
pub fn sort_peers(peers: &mut [Peer]) {
    peers.sort_by(|a, b| {
        a.host
            .cmp(&b.host)
            .then(a.port.cmp(&b.port))
            .then(a.pubkey.cmp(&b.pubkey))
    });
}
pub fn exclusions(peer: &[u8], response_to: Option<&Message>) -> Result<Vec<Vec<u8>>, Error> {
    let mut out = vec![peer.to_vec()];
    if let Some(message) = response_to {
        out.extend(
            Remote::from_message(message)?
                .peers
                .iter()
                .map(|p| p.pubkey.clone()),
        );
    }
    Ok(out)
}
#[derive(Debug, Clone)]
pub struct Encoding {
    pub version: u16,
    pub minimum_version: u16,
    pub port: u16,
    pub share: Unsigned,
    pub limits: message::Limits,
}
pub fn encode(
    local: &Local,
    mut peers: Vec<Peer>,
    capabilities: &[ChainPoi],
    config: Encoding,
) -> Result<Message, Error> {
    if !(1..=2).contains(&config.version) || !(1..=2).contains(&config.minimum_version) {
        return Err(Error::Configuration);
    }
    if config.share == Unsigned::from(0) {
        peers.clear();
    }
    if peers.len() > config.limits.items {
        return Err(message::Error::ResourceLimit.into());
    }
    sort_peers(&mut peers);
    let mut available = config.limits.body_bytes.min(config.limits.work_bytes);
    let mut encoded_peers = Vec::new();
    for peer in peers {
        let bytes = peer.encode(config.limits)?;
        available = available
            .checked_sub(bytes.len())
            .ok_or(message::Error::ResourceLimit)?;
        encoded_peers.push(Value::Binary(bytes));
    }
    let peers = encoded_peers;
    let mut fields = BTreeMap::from([
        ("port".into(), Value::Integer(u64::from(config.port).into())),
        ("share".into(), Value::Integer(config.share)),
        (
            "genesis_hash".into(),
            Value::Binary(local.genesis.0.to_vec()),
        ),
        ("best_hash".into(), Value::Binary(local.best.0.to_vec())),
        (
            "difficulty".into(),
            Value::Integer(local.difficulty.clone()),
        ),
        ("sync_allowed".into(), Value::Boolean(!local.temporary)),
        ("peers".into(), Value::List(peers)),
    ]);
    if config.version == 2 {
        fields.insert("height".into(), Value::Integer(local.height.into()));
        fields.insert(
            "versions".into(),
            Value::List(vec![Value::Record(BTreeMap::from([
                ("protocol".into(), Value::Binary(b"ping".to_vec())),
                (
                    "vsns".into(),
                    Value::List(
                        (config.minimum_version..=2)
                            .map(|v| Value::Integer(u64::from(v).into()))
                            .collect(),
                    ),
                ),
            ]))]),
        );
        fields.insert(
            "capabilities".into(),
            Value::Binary(message::encode_capabilities(capabilities, config.limits)?),
        );
    }
    Ok(Message::new(
        Kind::Ping,
        config.version,
        fields,
        config.limits,
    )?)
}
