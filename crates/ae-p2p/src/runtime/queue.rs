use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU16, Ordering},
    task::{Context, Poll},
};
use tokio::sync::{OwnedSemaphorePermit, oneshot};

#[derive(Debug, Clone)]
pub struct Generation(Arc<()>);
impl Generation {
    pub(super) fn new() -> Self {
        Self(Arc::new(()))
    }
}
impl PartialEq for Generation {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for Generation {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PingObservation {
    pub previous: Option<u16>,
    pub action: requests::PingAction,
    pub busy: bool,
}

#[derive(Debug)]
pub struct Received {
    pub generation: Generation,
    pub ping: Option<PingObservation>,
    message: message::Message,
    _budget: OwnedSemaphorePermit,
    receipt: Option<PingReceipt>,
}
impl Received {
    pub fn message(&self) -> &message::Message {
        &self.message
    }
    pub(super) fn new(
        message: message::Message,
        generation: Generation,
        budget: OwnedSemaphorePermit,
        ping: Option<PingObservation>,
        receipt: Option<PingReceipt>,
    ) -> Self {
        Self {
            message,
            generation,
            ping,
            _budget: budget,
            receipt,
        }
    }
}
#[derive(Debug, Default)]
pub(super) struct PingState {
    pub submitted: AtomicBool,
    pub finished: AtomicBool,
}
#[derive(Debug)]
pub(super) struct PingReceipt {
    pub token: requests::Token,
    pub generation: Generation,
    pub state: Arc<PingState>,
    pub incoming: bool,
    pub probe_required: bool,
    pub stop: watch::Sender<Option<Error>>,
}
impl Drop for PingReceipt {
    fn drop(&mut self) {
        if !self.state.submitted.load(Ordering::Acquire)
            && !self.state.finished.load(Ordering::Acquire)
        {
            stop(&self.stop, Error::Cancelled);
        }
    }
}
#[derive(Debug)]
pub enum Reply {
    Sent,
    Response(Received),
}
#[derive(Debug)]
pub enum Event {
    Message(Received),
    Ping {
        generation: Generation,
        action: requests::PingAction,
    },
}

pub struct Ticket {
    reply: oneshot::Receiver<Result<Reply, Error>>,
    finished: Arc<AtomicBool>,
    stop: watch::Sender<Option<Error>>,
    generation: Generation,
}
impl Ticket {
    pub fn generation(&self) -> &Generation {
        &self.generation
    }
}
impl Future for Ticket {
    type Output = Result<Reply, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.reply).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(reply) => {
                self.finished.store(true, Ordering::Release);
                Poll::Ready(reply.unwrap_or(Err(Error::Closed)))
            }
        }
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        // A wire request has no identifier. Abandoning it must never free its
        // slot for a later request on the same connection.
        if !self.finished.load(Ordering::Acquire) {
            stop(&self.stop, Error::Cancelled);
        }
    }
}
pub(super) fn stop(sender: &watch::Sender<Option<Error>>, reason: Error) {
    sender.send_if_modified(|value| {
        if value.is_none() {
            *value = Some(reason);
            true
        } else {
            false
        }
    });
}
pub(super) struct Answer {
    sender: Option<oneshot::Sender<Result<Reply, Error>>>,
    finished: Arc<AtomicBool>,
}
impl Answer {
    pub fn finish(mut self, reply: Result<Reply, Error>) {
        self.finished.store(true, Ordering::Release);
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(reply);
        }
    }
}
impl Drop for Answer {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(Error::Closed));
        }
    }
}
pub(super) enum Mode {
    Send,
    Request {
        timeout: Option<Duration>,
        probe: bool,
    },
}
pub(super) struct Command {
    pub packet: Arc<[u8]>,
    pub mode: Mode,
    pub deadline: Instant,
    pub answer: Answer,
    pub budget: OwnedSemaphorePermit,
}
pub(super) struct Finish {
    pub token: requests::Token,
    pub packet: Option<Arc<[u8]>>,
    pub deadline: Instant,
    pub answer: Answer,
    pub probe: Option<Answer>,
    pub budget: OwnedSemaphorePermit,
}

#[derive(Debug, Clone)]
pub struct Prepared {
    packet: Arc<[u8]>,
    limits: message::Limits,
    response: Option<message::Kind>,
}
impl Prepared {
    pub fn new(message: &message::Message, limits: message::Limits) -> Result<Self, Error> {
        Ok(Self {
            packet: message
                .encode_packet(limits)
                .map_err(Error::Message)?
                .into(),
            limits,
            response: match message.details() {
                message::Details::Response { kind, .. } => Some(*kind),
                _ => None,
            },
        })
    }
    pub fn bytes(&self) -> usize {
        self.packet.len()
    }
    fn validate(&self, limits: message::Limits) -> Result<(), Error> {
        if limits.body_bytes < self.limits.body_bytes
            || limits.work_bytes < self.limits.work_bytes
            || limits.items < self.limits.items
            || limits.depth < self.limits.depth
        {
            message::Message::decode_packet(&self.packet, limits).map_err(Error::Message)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Client {
    commands: mpsc::Sender<Command>,
    replies: mpsc::Sender<Command>,
    finishes: mpsc::Sender<Finish>,
    stop: watch::Sender<Option<Error>>,
    generation: Generation,
    peer: PeerPublicKey,
    budget: Arc<Semaphore>,
    config: Config,
    monitor: Monitor,
    selected_ping: Arc<AtomicU16>,
}
impl Client {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        commands: mpsc::Sender<Command>,
        replies: mpsc::Sender<Command>,
        finishes: mpsc::Sender<Finish>,
        stop: watch::Sender<Option<Error>>,
        generation: Generation,
        peer: PeerPublicKey,
        config: Config,
        monitor: Monitor,
        selected_ping: Arc<AtomicU16>,
    ) -> Self {
        Self {
            commands,
            replies,
            finishes,
            stop,
            generation,
            peer,
            budget: Arc::new(Semaphore::new(config.outgoing_bytes)),
            config,
            monitor,
            selected_ping,
        }
    }
    pub fn generation(&self) -> &Generation {
        &self.generation
    }
    pub fn peer(&self) -> PeerPublicKey {
        self.peer
    }
    pub fn monitor(&self) -> Monitor {
        self.monitor.clone()
    }
    pub fn selected_ping_version(&self) -> Option<u16> {
        match self.selected_ping.load(Ordering::Acquire) {
            0 => None,
            version => Some(version),
        }
    }
    pub fn ping_version(&self) -> u16 {
        self.selected_ping_version()
            .unwrap_or(self.config.minimum_ping)
    }
    pub fn minimum_ping_version(&self) -> u16 {
        self.config.minimum_ping
    }
    pub fn defers_pings(&self) -> bool {
        self.config.defer_pings
    }
    pub fn message_limits(&self) -> message::Limits {
        self.config.messages
    }
    pub fn shutdown(&self) {
        stop(&self.stop, Error::Shutdown);
    }
    pub fn is_closed(&self) -> bool {
        self.commands.is_closed() || self.stop.borrow().is_some()
    }
    pub fn send(&self, message: &message::Message) -> Result<Ticket, Error> {
        self.submit(message, Mode::Send)
    }
    pub fn send_prepared(&self, prepared: &Prepared) -> Result<Ticket, Error> {
        if self.config.defer_pings && prepared.response == Some(message::Kind::Ping) {
            return Err(Error::InvalidPing);
        }
        let slot = self.reserve(prepared.response.is_some())?;
        prepared.validate(self.config.messages)?;
        self.submit_packet(prepared.packet.clone(), Mode::Send, slot)
    }
    pub fn request(
        &self,
        message: &message::Message,
        node_info_timeout: Option<Duration>,
    ) -> Result<Ticket, Error> {
        self.submit(
            message,
            Mode::Request {
                timeout: node_info_timeout,
                probe: false,
            },
        )
    }
    pub fn probe(&self, message: &message::Message) -> Result<Ticket, Error> {
        self.submit(
            message,
            Mode::Request {
                timeout: None,
                probe: true,
            },
        )
    }
    // The application has completed this ping's effects. An incoming ping
    // requires its response here; a solicited response requires no packet,
    // except an initial v1 response, which requires finish_ping_with_probe.
    // Version selection follows the completed write, before the ticket resolves.
    pub fn finish_ping(
        &self,
        received: &Received,
        response: Option<&message::Message>,
    ) -> Result<Ticket, Error> {
        self.finish(received, response, false)
            .map(|(finished, _)| finished)
    }
    // First ticket completes the callback after the probe write; the second
    // owns the probe response. The callback keeps later reads/requests fenced
    // while the application constructs the probe and while it is written.
    pub fn finish_ping_with_probe(
        &self,
        received: &Received,
        probe: &message::Message,
    ) -> Result<(Ticket, Ticket), Error> {
        self.finish(received, Some(probe), true)
            .map(|(finished, response)| (finished, response.expect("probe response ticket")))
    }
    fn finish(
        &self,
        received: &Received,
        packet: Option<&message::Message>,
        probe: bool,
    ) -> Result<(Ticket, Option<Ticket>), Error> {
        let receipt = received.receipt.as_ref().ok_or(Error::InvalidPing)?;
        if receipt.generation != self.generation
            || receipt.probe_required != probe
            || (receipt.incoming || probe) != packet.is_some()
            || receipt.state.finished.load(Ordering::Acquire)
        {
            return Err(Error::InvalidPing);
        }
        if self.is_closed() {
            return Err(Error::Closed);
        }
        if let Some(packet) = packet {
            if probe {
                if receipt.incoming || packet.kind() != message::Kind::Ping || packet.version() != 2
                {
                    return Err(Error::InvalidPing);
                }
            } else {
                match packet.details() {
                    message::Details::Response {
                        kind: message::Kind::Ping,
                        message,
                    } if message.as_ref().is_none_or(|body| {
                        body.as_ref()
                            .is_ok_and(|body| body.version() == received.message().version())
                    }) => {}
                    _ => return Err(Error::InvalidPing),
                }
            }
        }
        let slot = self.finishes.try_reserve().map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::Busy,
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })?;
        let packet = packet
            .map(|m| Prepared::new(m, self.config.messages).map(|p| p.packet))
            .transpose()?;
        let budget = self.packet_budget(packet.as_deref().unwrap_or_default())?;
        let deadline = Instant::now()
            .checked_add(self.config.write_timeout)
            .ok_or(Error::Configuration)?;
        receipt
            .state
            .submitted
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::InvalidPing)?;
        let (answer, ticket) = self.answer();
        let (probe, response) = if probe {
            let (answer, ticket) = self.answer();
            (Some(answer), Some(ticket))
        } else {
            (None, None)
        };
        slot.send(Finish {
            token: receipt.token.clone(),
            packet,
            deadline,
            answer,
            probe,
            budget,
        });
        Ok((ticket, response))
    }
    fn reserve(&self, reply: bool) -> Result<mpsc::Permit<'_, Command>, Error> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let queue = if reply && self.config.defer_pings {
            &self.replies
        } else {
            &self.commands
        };
        queue.try_reserve().map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::Busy,
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })
    }
    fn submit(&self, message: &message::Message, mode: Mode) -> Result<Ticket, Error> {
        let response = match (&mode, message.details()) {
            (Mode::Send, message::Details::Response { kind, .. }) => Some(*kind),
            _ => None,
        };
        if self.config.defer_pings && response == Some(message::Kind::Ping) {
            return Err(Error::InvalidPing);
        }
        let slot = self.reserve(response.is_some())?;
        let prepared = Prepared::new(message, self.config.messages)?;
        self.submit_packet(prepared.packet, mode, slot)
    }
    fn submit_packet(
        &self,
        packet: Arc<[u8]>,
        mode: Mode,
        slot: mpsc::Permit<'_, Command>,
    ) -> Result<Ticket, Error> {
        let budget = self.packet_budget(&packet)?;
        let deadline = Instant::now()
            .checked_add(self.config.write_timeout)
            .ok_or(Error::Configuration)?;
        let (answer, ticket) = self.answer();
        slot.send(Command {
            packet,
            mode,
            deadline,
            answer,
            budget,
        });
        Ok(ticket)
    }
    fn packet_budget(&self, packet: &[u8]) -> Result<OwnedSemaphorePermit, Error> {
        if packet.len() > self.config.fragments.message_bytes {
            return Err(Error::Frame(framing::Error::ResourceLimit));
        }
        let count = if packet.len() > framing::MAX_PAYLOAD {
            packet.len().div_ceil(framing::MAX_FRAGMENT)
        } else {
            1
        };
        if count > self.config.fragments.fragments || count > usize::from(u16::MAX) {
            return Err(Error::Frame(framing::Error::ResourceLimit));
        }
        let bytes = u32::try_from(packet.len()).map_err(|_| Error::Busy)?;
        self.budget
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| Error::Busy)
    }
    fn answer(&self) -> (Answer, Ticket) {
        let (sender, reply) = oneshot::channel();
        let finished = Arc::new(AtomicBool::new(false));
        (
            Answer {
                sender: Some(sender),
                finished: finished.clone(),
            },
            Ticket {
                reply,
                finished,
                stop: self.stop.clone(),
                generation: self.generation.clone(),
            },
        )
    }
}
