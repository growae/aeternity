use crate::message::{Details, Kind, Message, Value};
use ae_types::Hash256;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    AlreadyPending,
    ResourceLimit,
    Disconnected,
    InvalidRequest,
    InvalidConfiguration,
    DeadlineOverflow,
    MalformedResponse,
    NoCommonPingVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    Timeout,
    Disconnected,
    NoCommonPingVersion,
}

#[derive(Debug, Clone)]
pub struct Token(Arc<()>);
impl PartialEq for Token {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for Token {}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub pending: usize,
    pub expansions: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            pending: 64,
            expansions: 32,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PingAction {
    #[default]
    None,
    ProbeV2,
    Selected(u16),
}

#[derive(Debug)]
pub struct PingVersions {
    minimum: u16,
    selected: Option<u16>,
}
impl PingVersions {
    pub fn new(minimum: u16) -> Result<Self, Error> {
        if !(1..=2).contains(&minimum) {
            return Err(Error::InvalidConfiguration);
        }
        Ok(Self {
            minimum,
            selected: None,
        })
    }
    pub fn selected(&self) -> Option<u16> {
        self.selected
    }
    pub fn outgoing(&self) -> u16 {
        self.selected.unwrap_or(self.minimum)
    }
    pub fn observe(&mut self, ping: &Message, solicited: bool) -> Result<PingAction, Error> {
        if ping.kind() != Kind::Ping {
            return Err(Error::InvalidRequest);
        }
        let version = ping.version();
        if version < self.minimum {
            return Err(Error::NoCommonPingVersion);
        }
        if self.selected == Some(version) {
            return Ok(PingAction::None);
        }
        if self.selected.is_none() && version == 1 {
            return Ok(if solicited {
                PingAction::ProbeV2
            } else {
                PingAction::None
            });
        }
        let selected = self.choose(ping)?;
        self.selected = Some(selected);
        Ok(PingAction::Selected(selected))
    }
    fn choose(&self, ping: &Message) -> Result<u16, Error> {
        let mut matches = Vec::new();
        if let Some(Value::List(versions)) = ping.fields().get("versions") {
            for version in versions {
                let Value::Record(fields) = version else {
                    return Err(Error::MalformedResponse);
                };
                if matches!(fields.get("protocol"), Some(Value::Binary(p)) if p == b"ping") {
                    matches.push(fields);
                    if matches.len() == 2 {
                        break;
                    }
                }
            }
        }
        if matches.len() != 1 {
            return Ok(2);
        }
        let Some(Value::List(versions)) = matches[0].get("vsns") else {
            return Err(Error::MalformedResponse);
        };
        (self.minimum..=2)
            .rev()
            .find(|v| {
                versions
                    .iter()
                    .any(|value| *value == Value::Integer(u64::from(*v).into()))
            })
            .ok_or(Error::NoCommonPingVersion)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Slot {
    Ping(u16),
    Header,
    Successors,
    Generation,
    Pool,
    NodeInfo,
    Expansion(Hash256),
}
struct Pending {
    token: Token,
    deadline: Instant,
    expected: Kind,
    probe: bool,
}
#[derive(Debug)]
pub struct Started {
    pub token: Token,
    pub deadline: Instant,
}
#[derive(Debug)]
pub struct Completion {
    pub token: Token,
    pub probe: bool,
    pub result: Result<Message, Failure>,
}
#[derive(Debug, Default)]
pub struct Update {
    pub completions: Vec<Completion>,
    pub ping: PingAction,
    pub ping_before: Option<u16>,
    pub ping_busy: bool,
    pub error: Option<Error>,
}
pub struct Requests {
    limits: Limits,
    pending: BTreeMap<Slot, Pending>,
    closed: bool,
    ping: PingVersions,
}
impl Requests {
    pub fn new(limits: Limits, minimum_ping: u16) -> Result<Self, Error> {
        Ok(Self {
            limits,
            pending: BTreeMap::new(),
            closed: false,
            ping: PingVersions::new(minimum_ping)?,
        })
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    pub fn len(&self) -> usize {
        self.pending.len()
    }
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
    pub fn contains(&self, token: &Token) -> bool {
        self.pending.values().any(|p| p.token == *token)
    }
    pub fn next_deadline(&self) -> Option<Instant> {
        self.pending.values().map(|p| p.deadline).min()
    }
    pub fn ping_version(&self) -> u16 {
        self.ping.outgoing()
    }
    pub fn selected_ping_version(&self) -> Option<u16> {
        self.ping.selected()
    }
    pub fn receive_ping(&mut self, ping: &Message, now: Instant) -> Update {
        let mut update = self.expire(now);
        if self.closed {
            return update;
        }
        update.ping_before = self.ping.selected();
        if ping.kind() == Kind::Ping && self.pending.contains_key(&Slot::Ping(ping.version())) {
            update.ping_busy = true;
            return update;
        }
        match self.ping.observe(ping, false) {
            Ok(action) => update.ping = action,
            Err(error) => {
                update.error = Some(error);
                update.completions.extend(self.disconnect());
            }
        }
        update
    }
    pub fn start(
        &mut self,
        request: &Message,
        now: Instant,
        node_info_timeout: Option<Duration>,
    ) -> Result<Started, Error> {
        self.insert(request, now, node_info_timeout, false)
    }
    pub fn start_probe(&mut self, ping: &Message, now: Instant) -> Result<Started, Error> {
        if ping.kind() != Kind::Ping || ping.version() != 2 || self.ping.selected.is_some() {
            return Err(Error::InvalidRequest);
        }
        self.insert(ping, now, None, true)
    }
    fn insert(
        &mut self,
        request: &Message,
        now: Instant,
        timeout: Option<Duration>,
        probe: bool,
    ) -> Result<Started, Error> {
        if self.closed {
            return Err(Error::Disconnected);
        }
        if timeout.is_some() && request.kind() != Kind::GetNodeInfo {
            return Err(Error::InvalidRequest);
        }
        let (slot, expected) = request_slot(request)?;
        if matches!(slot, Slot::Ping(v) if v < self.ping.minimum) {
            return Err(Error::InvalidRequest);
        }
        if self.pending.contains_key(&slot) {
            return Err(Error::AlreadyPending);
        }
        if self.pending.len() >= self.limits.pending
            || (matches!(slot, Slot::Expansion(_))
                && self
                    .pending
                    .keys()
                    .filter(|s| matches!(s, Slot::Expansion(_)))
                    .count()
                    >= self.limits.expansions)
        {
            return Err(Error::ResourceLimit);
        }
        let deadline = now
            .checked_add(timeout.unwrap_or(DEFAULT_TIMEOUT))
            .ok_or(Error::DeadlineOverflow)?;
        let token = Token(Arc::new(()));
        self.pending.insert(
            slot,
            Pending {
                token: token.clone(),
                deadline,
                expected,
                probe,
            },
        );
        Ok(Started { token, deadline })
    }
    pub fn timeout(&mut self, token: &Token, now: Instant) -> Update {
        let Some(slot) = self
            .pending
            .iter()
            .find(|(_, p)| p.token == *token && p.deadline <= now)
            .map(|(slot, _)| *slot)
        else {
            return Update::default();
        };
        let pending = self.pending.remove(&slot).unwrap();
        let mut completions = vec![complete(pending, Err(Failure::Timeout))];
        let ping = if slot == Slot::Ping(2) && self.ping.minimum == 1 {
            self.ping.selected = Some(1);
            PingAction::Selected(1)
        } else {
            completions.extend(self.disconnect());
            PingAction::None
        };
        Update {
            completions,
            ping,
            ping_before: None,
            ping_busy: false,
            error: None,
        }
    }
    pub fn expire(&mut self, now: Instant) -> Update {
        let mut update = Update::default();
        while let Some(pending) = self.pending.values().min_by_key(|p| p.deadline) {
            if pending.deadline > now {
                break;
            }
            let token = pending.token.clone();
            let expired = self.timeout(&token, now);
            update.completions.extend(expired.completions);
            if expired.ping != PingAction::None {
                update.ping = expired.ping;
            }
        }
        update
    }
    pub fn disconnect(&mut self) -> Vec<Completion> {
        self.closed = true;
        std::mem::take(&mut self.pending)
            .into_values()
            .map(|p| complete(p, Err(Failure::Disconnected)))
            .collect()
    }
    pub fn receive_response(&mut self, envelope: Message, now: Instant) -> Update {
        let mut update = self.expire(now);
        if self.closed {
            return update;
        }
        let Details::Response { kind, message } = envelope.details() else {
            return self.malformed(update);
        };
        let body = match message {
            Some(Ok(body)) => Some(body.as_ref()),
            Some(Err(_)) => return self.malformed(update),
            None => None,
        };
        let slot = match response_slot(*kind, body, envelope.version()) {
            Ok(Some(slot)) => slot,
            Ok(None) => return update,
            Err(_) => return self.malformed(update),
        };
        let Some(pending) = self.pending.get(&slot) else {
            return update;
        };
        if pending.expected != *kind {
            return update;
        }
        if *kind == Kind::Ping {
            update.ping_before = self.ping.selected();
        }
        if let Some(body) = body.filter(|b| b.kind() == Kind::Ping) {
            match self.ping.observe(body, true) {
                Ok(action) => update.ping = action,
                Err(error) => {
                    let pending = self.pending.remove(&slot).unwrap();
                    update
                        .completions
                        .push(complete(pending, Err(Failure::NoCommonPingVersion)));
                    update.completions.extend(self.disconnect());
                    update.error = Some(error);
                    return update;
                }
            }
        }
        let pending = self.pending.remove(&slot).unwrap();
        update.completions.push(complete(pending, Ok(envelope)));
        update
    }
    fn malformed(&mut self, mut update: Update) -> Update {
        update.error = Some(Error::MalformedResponse);
        update.completions.extend(self.disconnect());
        update
    }
}
fn complete(pending: Pending, result: Result<Message, Failure>) -> Completion {
    Completion {
        token: pending.token,
        probe: pending.probe,
        result,
    }
}
fn hash(message: &Message) -> Result<Hash256, Error> {
    match message.fields().get("hash") {
        Some(Value::Binary(hash)) => {
            Hash256::try_from(hash.as_slice()).map_err(|_| Error::InvalidRequest)
        }
        _ => Err(Error::InvalidRequest),
    }
}
fn request_slot(message: &Message) -> Result<(Slot, Kind), Error> {
    use Kind::*;
    Ok(match message.kind() {
        Ping => (Slot::Ping(message.version()), Ping),
        GetHeaderByHash | GetHeaderByHeight => (Slot::Header, Header),
        GetSuccessors => (Slot::Successors, HeaderHashes),
        GetGeneration => (Slot::Generation, Generation),
        GetBlockTransactions => (Slot::Expansion(hash(message)?), BlockTransactions),
        PoolInit => (Slot::Pool, PoolInit),
        PoolUnfold => (Slot::Pool, PoolUnfold),
        PoolGet => (Slot::Pool, Transactions),
        PoolFinish => (Slot::Pool, PoolFinish),
        GetNodeInfo => (Slot::NodeInfo, NodeInfo),
        _ => return Err(Error::InvalidRequest),
    })
}
fn response_slot(
    kind: Kind,
    body: Option<&Message>,
    outer_version: u16,
) -> Result<Option<Slot>, Error> {
    use Kind::*;
    Ok(Some(match kind {
        Ping => Slot::Ping(body.map_or(outer_version, Message::version)),
        Header => Slot::Header,
        HeaderHashes => Slot::Successors,
        Generation => Slot::Generation,
        PoolInit | PoolUnfold | Transactions | PoolFinish => Slot::Pool,
        NodeInfo => Slot::NodeInfo,
        BlockTransactions => {
            let Some(body) = body else {
                return Ok(None);
            };
            Slot::Expansion(hash(body)?)
        }
        _ => return Ok(None),
    }))
}
