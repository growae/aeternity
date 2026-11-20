// aec_tx_pool_sync, aeternity fa2a6d7. Copyright (c) 2018,
// Aeternity Anstalt. ISC notice: LICENSE.
use super::{Error, Snapshot, Unfold, codec};
use ae_trie::Nibbles;
use ae_types::Hash256;
use std::sync::Arc;

pub const ACTION_TIMEOUT_MS: u64 = 33_000;
pub const GET_TIMEOUT_MS: u64 = 330_000;
pub const RESYNC_MS: u64 = 30 * 60 * 1000;
pub const MAX_INCOMING: usize = 5;

// Process-local capabilities: never reused, serializable, or forgeable by a peer.
#[derive(Debug, Clone)]
pub struct Token(Arc<()>);
impl Token {
    fn new() -> Self {
        Self(Arc::new(()))
    }
}
impl PartialEq for Token {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for Token {}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub incoming: usize,
    pub exchange: super::Limits,
    pub get_items: usize,
    pub get_bytes: usize,
    pub rounds: usize,
    pub resync_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            incoming: MAX_INCOMING,
            exchange: Default::default(),
            get_items: 100_000,
            get_bytes: 16 * 1024 * 1024,
            rounds: 1024,
            resync_ms: RESYNC_MS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Work {
    Tree,
    Init,
    Unfold { request: Vec<Unfold>, delay_ms: u64 },
    Get(Vec<Nibbles>),
    Finish(bool),
}
impl Work {
    pub fn timeout_ms(&self) -> u64 {
        if matches!(self, Self::Get(_)) {
            GET_TIMEOUT_MS
        } else {
            ACTION_TIMEOUT_MS
        }
    }
}

#[derive(Debug, Clone)]
pub struct Action {
    pub token: Token,
    pub peer: Hash256,
    pub work: Arc<Work>,
}
#[derive(Debug, Clone)]
pub enum Effect {
    Watch {
        token: Token,
        peer: Hash256,
        connection: u64,
    },
    Unwatch(Token),
    Start(Action),
    CancelAction(Token),
    Resync {
        token: Token,
        delay_ms: u64,
    },
    CancelResync(Token),
}
#[derive(Debug, Clone)]
pub enum Reply {
    Tree(Arc<Snapshot>),
    Init,
    Unfold(Vec<Unfold>),
    TreeNotReady,
    // Number returned by the peer, including transactions rejected by admission.
    Fetched(usize),
    Finished,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Action,
    Timeout,
    InvalidReply,
    Protocol(Error),
    ResourceLimit,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestError {
    TooManySyncs,
    AlreadySyncing,
    NoActiveSync,
    TreeNotReady,
    Protocol(Error),
}
#[derive(Debug, Default)]
pub struct Transition {
    pub effects: Vec<Effect>,
    pub failure: Option<Failure>,
}

#[derive(Debug)]
pub struct Session {
    peer: Hash256,
    connection: u64,
    monitor: Token,
    tree: Option<Arc<Snapshot>>,
    gets: Option<Vec<Nibbles>>,
    rounds: usize,
    pending: Option<Action>,
}
impl Session {
    pub fn peer(&self) -> Hash256 {
        self.peer
    }
    pub fn connection(&self) -> u64 {
        self.connection
    }
    pub fn monitor(&self) -> &Token {
        &self.monitor
    }
    pub fn tree(&self) -> Option<&Arc<Snapshot>> {
        self.tree.as_ref()
    }
    pub fn gets(&self) -> Option<&[Nibbles]> {
        self.gets.as_deref()
    }
    pub fn pending(&self) -> Option<&Action> {
        self.pending.as_ref()
    }
    fn start(&mut self, work: Work, effects: &mut Vec<Effect>) {
        debug_assert!(self.pending.is_none());
        let action = Action {
            token: Token::new(),
            peer: self.peer,
            work: Arc::new(work),
        };
        self.pending = Some(action.clone());
        effects.push(Effect::Start(action));
    }
    fn new(peer: Hash256, connection: u64, effects: &mut Vec<Effect>) -> Self {
        let monitor = Token::new();
        effects.push(Effect::Watch {
            token: monitor.clone(),
            peer,
            connection,
        });
        let mut session = Self {
            peer,
            connection,
            monitor,
            tree: None,
            gets: None,
            rounds: 0,
            pending: None,
        };
        session.start(Work::Tree, effects);
        session
    }
    fn close(mut self, effects: &mut Vec<Effect>) {
        if let Some(action) = self.pending.take() {
            effects.push(Effect::CancelAction(action.token));
        }
        effects.push(Effect::Unwatch(self.monitor));
    }
    fn repair(&mut self, connection: u64, effects: &mut Vec<Effect>) {
        effects.push(Effect::Unwatch(self.monitor.clone()));
        self.monitor = Token::new();
        self.connection = connection;
        effects.push(Effect::Watch {
            token: self.monitor.clone(),
            peer: self.peer,
            connection,
        });
    }
}

#[derive(Debug)]
pub enum Local {
    Idle,
    Active(Session),
    Cooldown(Token),
}
#[derive(Debug)]
pub struct Manager {
    limits: Limits,
    local: Local,
    remote: Vec<Session>,
}
impl Default for Manager {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}
impl Manager {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            local: Local::Idle,
            remote: vec![],
        }
    }
    pub fn limits(&self) -> Limits {
        self.limits
    }
    pub fn local(&self) -> &Local {
        &self.local
    }
    pub fn incoming(&self) -> &[Session] {
        &self.remote
    }
    pub fn connect(&mut self, peer: Hash256, connection: u64) -> Vec<Effect> {
        let mut effects = vec![];
        if matches!(self.local, Local::Idle) {
            self.local = Local::Active(Session::new(peer, connection, &mut effects));
        }
        effects
    }
    pub fn accept(&mut self, peer: Hash256, connection: u64) -> Result<Vec<Effect>, RequestError> {
        if self.remote.len() >= self.limits.incoming.min(MAX_INCOMING) {
            return Err(RequestError::TooManySyncs);
        }
        if self.remote.iter().any(|s| s.peer == peer) {
            return Err(RequestError::AlreadySyncing);
        }
        let mut effects = vec![];
        let session = Session::new(peer, connection, &mut effects);
        self.remote.insert(0, session);
        Ok(effects)
    }
    pub fn authorize_get(&self, peer: Hash256) -> Result<(), RequestError> {
        self.remote
            .iter()
            .any(|s| s.peer == peer)
            .then_some(())
            .ok_or(RequestError::NoActiveSync)
    }
    pub fn respond_wire(
        &self,
        peer: Hash256,
        requests: &[Vec<u8>],
    ) -> Result<Vec<Vec<u8>>, RequestError> {
        let session = self
            .remote
            .iter()
            .find(|s| s.peer == peer)
            .ok_or(RequestError::NoActiveSync)?;
        let tree = session.tree.as_ref().ok_or(RequestError::TreeNotReady)?;
        tree.respond_wire(requests, self.limits.exchange)
            .map_err(RequestError::Protocol)
    }
    pub fn finish_remote(&mut self, peer: Hash256) -> Vec<Effect> {
        let mut effects = vec![];
        if let Some(index) = self.remote.iter().position(|s| s.peer == peer) {
            self.remote.remove(index).close(&mut effects);
        }
        effects
    }
    pub fn resync(&mut self, token: &Token) -> bool {
        if matches!(&self.local, Local::Cooldown(current) if current == token) {
            self.local = Local::Idle;
            true
        } else {
            false
        }
    }
    pub fn connection_down(&mut self, monitor: &Token, replacement: Option<u64>) -> Vec<Effect> {
        let mut effects = vec![];
        if matches!(&self.local, Local::Active(s) if &s.monitor == monitor) {
            if let Local::Active(mut session) = std::mem::replace(&mut self.local, Local::Idle) {
                if let Some(connection) = replacement {
                    session.repair(connection, &mut effects);
                    self.local = Local::Active(session);
                } else {
                    session.close(&mut effects);
                }
            }
        } else if let Some(index) = self.remote.iter().position(|s| &s.monitor == monitor) {
            if let Some(connection) = replacement {
                self.remote[index].repair(connection, &mut effects);
            } else {
                self.remote.remove(index).close(&mut effects);
            }
        }
        effects
    }
    pub fn shutdown(&mut self) -> Vec<Effect> {
        let mut effects = vec![];
        match std::mem::replace(&mut self.local, Local::Idle) {
            Local::Active(s) => s.close(&mut effects),
            Local::Cooldown(token) => effects.push(Effect::CancelResync(token)),
            Local::Idle => {}
        }
        for session in self.remote.drain(..) {
            session.close(&mut effects);
        }
        effects
    }
    pub fn timeout(&mut self, token: &Token) -> Transition {
        self.complete(token, Err(Failure::Timeout))
    }
    pub fn complete(&mut self, token: &Token, result: Result<Reply, Failure>) -> Transition {
        let matches = |s: &Session| s.pending.as_ref().is_some_and(|a| &a.token == token);
        let mut out = Transition::default();
        if matches!(&self.local, Local::Active(s) if matches(s)) {
            let Local::Active(mut session) = std::mem::replace(&mut self.local, Local::Idle) else {
                unreachable!()
            };
            let action = session.pending.take().expect("matching pending action");
            out.effects.push(Effect::CancelAction(action.token));
            if let Work::Finish(done) = *action.work {
                // Finish is best effort. A failed/timed-out finish cannot keep retrying forever.
                if let Err(failure) = result.as_ref() {
                    out.failure = Some(failure.clone());
                } else if !matches!(result, Ok(Reply::Finished)) {
                    out.failure = Some(Failure::InvalidReply);
                }
                session.close(&mut out.effects);
                if done && out.failure.is_none() {
                    let token = Token::new();
                    out.effects.push(Effect::Resync {
                        token: token.clone(),
                        delay_ms: self.limits.resync_ms,
                    });
                    self.local = Local::Cooldown(token);
                }
                return out;
            }
            let next =
                result.and_then(|reply| advance(&mut session, &action.work, reply, self.limits));
            let work = match next {
                Ok(work) => work,
                Err(failure) => {
                    out.failure = Some(failure);
                    Work::Finish(false)
                }
            };
            session.start(work, &mut out.effects);
            self.local = Local::Active(session);
        } else if let Some(index) = self.remote.iter().position(matches) {
            let action = self.remote[index]
                .pending
                .take()
                .expect("matching pending action");
            out.effects.push(Effect::CancelAction(action.token));
            let tree = result.and_then(|reply| match reply {
                Reply::Tree(tree) => check_tree(tree, self.limits),
                _ => Err(Failure::InvalidReply),
            });
            match tree {
                Ok(tree) => self.remote[index].tree = Some(tree),
                Err(failure) => {
                    out.failure = Some(failure);
                    self.remote.remove(index).close(&mut out.effects);
                }
            }
        }
        out
    }
}

fn check_tree(tree: Arc<Snapshot>, limits: Limits) -> Result<Arc<Snapshot>, Failure> {
    let store = tree.tree.store();
    if tree.len() > limits.exchange.entries
        || store.nodes.nodes().len() > limits.exchange.tree_nodes
        || store.bytes > limits.exchange.tree_bytes
    {
        Err(Failure::ResourceLimit)
    } else {
        Ok(tree)
    }
}
fn check_keys(keys: &[Nibbles], limits: Limits) -> Result<(), Failure> {
    if keys.len() > limits.get_items {
        return Err(Failure::ResourceLimit);
    }
    let mut bytes = 0usize;
    for key in keys {
        bytes = bytes
            .checked_add(key.as_slice().len().div_ceil(2))
            .ok_or(Failure::ResourceLimit)?;
        if bytes > limits.get_bytes {
            return Err(Failure::ResourceLimit);
        }
    }
    Ok(())
}
// The native analyzer can return odd nibble paths. They cannot be encoded as
// peer get binaries; preserve them until this boundary, then fail explicitly.
pub fn encode_get(keys: &[Nibbles], limits: Limits) -> Result<Vec<Vec<u8>>, Failure> {
    check_keys(keys, limits)?;
    keys.iter()
        .map(|key| codec::bytes(key).map_err(Failure::Protocol))
        .collect()
}
fn advance(
    session: &mut Session,
    work: &Work,
    reply: Reply,
    limits: Limits,
) -> Result<Work, Failure> {
    match (work, reply) {
        (Work::Tree, Reply::Tree(tree)) => {
            session.tree = Some(check_tree(tree, limits)?);
            Ok(Work::Init)
        }
        (Work::Init, Reply::Init) => {
            if limits.rounds == 0 {
                return Err(Failure::ResourceLimit);
            }
            let tree = session.tree.as_ref().ok_or(Failure::InvalidReply)?;
            session.gets = Some(vec![]);
            session.rounds = 1;
            Ok(Work::Unfold {
                request: vec![tree.initial()],
                delay_ms: 50,
            })
        }
        (Work::Unfold { request, delay_ms }, Reply::TreeNotReady) => {
            if *delay_ms >= 10_000 {
                return Ok(Work::Finish(false));
            }
            next_round(session, limits)?;
            Ok(Work::Unfold {
                request: request.clone(),
                delay_ms: delay_ms + 1000,
            })
        }
        (Work::Unfold { .. }, Reply::Unfold(response)) => {
            let tree = session.tree.as_ref().ok_or(Failure::InvalidReply)?;
            let analysis = tree
                .analyze(&response, limits.exchange)
                .map_err(Failure::Protocol)?;
            let gets = session.gets.as_ref().ok_or(Failure::InvalidReply)?;
            if gets
                .len()
                .checked_add(analysis.get.len())
                .is_none_or(|n| n > limits.get_items)
            {
                return Err(Failure::ResourceLimit);
            }
            let mut keys = gets.clone();
            keys.extend(analysis.get);
            check_keys(&keys, limits)?;
            if analysis.unfold.is_empty() {
                session.gets = Some(vec![]);
                Ok(Work::Get(keys))
            } else {
                next_round(session, limits)?;
                session.gets = Some(keys);
                Ok(Work::Unfold {
                    request: analysis.unfold,
                    delay_ms: 0,
                })
            }
        }
        (Work::Get(_), Reply::Fetched(_)) => Ok(Work::Finish(true)),
        _ => Err(Failure::InvalidReply),
    }
}
fn next_round(session: &mut Session, limits: Limits) -> Result<(), Failure> {
    if session.rounds >= limits.rounds {
        return Err(Failure::ResourceLimit);
    }
    session.rounds += 1;
    Ok(())
}
