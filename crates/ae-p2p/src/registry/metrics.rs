use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerMetrics {
    pub inbound: usize,
    pub outbound: usize,
    pub verified: usize,
    pub unverified: usize,
    pub standby: usize,
    pub blocked: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerCallback {
    Connect,
    Probe(Class),
    Connection,
    Ping,
    Inbound,
    Resolve,
}
impl TimerCallback {
    fn observes_peers(self) -> bool {
        matches!(self, Self::Connect | Self::Probe(_) | Self::Connection)
    }
}
impl State {
    pub(super) fn timer_callback(&self, kind: &TimerKind) -> TimerCallback {
        match kind {
            TimerKind::Connect => TimerCallback::Connect,
            TimerKind::Probe(class) => TimerCallback::Probe(*class),
            TimerKind::Inbound(_) => TimerCallback::Inbound,
            TimerKind::Resolve(_) => TimerCallback::Resolve,
            TimerKind::Owner(peer, _) => {
                if self
                    .connections
                    .get(peer)
                    .is_some_and(|c| c.connection.status == Status::Connected)
                {
                    TimerCallback::Ping
                } else {
                    TimerCallback::Connection
                }
            }
        }
    }
    pub(super) fn observes_peers(&self, command: &Command) -> bool {
        match command {
            Command::Add(_)
            | Command::Delete(_)
            | Command::Block(_)
            | Command::Unblock(_)
            | Command::UnblockAll
            | Command::IsBlocked(_)
            | Command::Suspect(_)
            | Command::Failed { .. }
            | Command::Connected { .. }
            | Command::Alive { .. }
            | Command::Accept { .. }
            | Command::RejectInbound { .. }
            | Command::AllPeers { .. } => true,
            // Initial success dispatches add_peer; initial nxdomain only
            // queues resolve_peer. Later retries dispatch resolved_hostname.
            Command::Resolved { token, address } => self
                .hosts
                .values()
                .any(|h| h.attempt == Some(*token) && (h.retries != 0 || address.is_some())),
            // The Rust temporary-connection path folds the native subsequent
            // connection_closed callback into this transition.
            Command::PingDone { peer, token } => self.connections.get(peer).is_some_and(|c| {
                c.connection.token == *token
                    && c.connection.status == Status::Connected
                    && c.connection.kind == Kind::Temporary
            }),
            Command::TimerFired(timer) => timer.callback.observes_peers(),
            // Legacy token-only callers can identify only a live deadline.
            // Runtime delivery uses TimerFired to retain stale callback identity.
            Command::Timeout(token) => self
                .timers
                .get(token)
                .is_some_and(|t| self.timer_callback(&t.kind).observes_peers()),
            _ => false,
        }
    }
    pub(super) fn peer_metrics(&self) -> PeerMetrics {
        let counts = self.counts();
        PeerMetrics {
            inbound: counts.inbound,
            outbound: counts.outbound,
            verified: self.pool.count(discovery::Count::All, Target::Verified),
            unverified: self.pool.count(discovery::Count::All, Target::Unverified),
            standby: self.pool.count(discovery::Count::Standby, Target::Both),
            blocked: counts.blocked,
        }
    }
}
