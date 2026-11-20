use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
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

#[derive(Debug)]
pub struct Received {
    pub generation: Generation,
    message: message::Message,
    _budget: OwnedSemaphorePermit,
}
impl Received {
    pub fn message(&self) -> &message::Message {
        &self.message
    }
    pub(super) fn new(
        message: message::Message,
        generation: Generation,
        budget: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            message,
            generation,
            _budget: budget,
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
    pub packet: Vec<u8>,
    pub mode: Mode,
    pub deadline: Instant,
    pub answer: Answer,
    pub budget: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub struct Client {
    commands: mpsc::Sender<Command>,
    stop: watch::Sender<Option<Error>>,
    generation: Generation,
    peer: PeerPublicKey,
    budget: Arc<Semaphore>,
    config: Config,
}
impl Client {
    pub(super) fn new(
        commands: mpsc::Sender<Command>,
        stop: watch::Sender<Option<Error>>,
        generation: Generation,
        peer: PeerPublicKey,
        config: Config,
    ) -> Self {
        Self {
            commands,
            stop,
            generation,
            peer,
            budget: Arc::new(Semaphore::new(config.outgoing_bytes)),
            config,
        }
    }
    pub fn generation(&self) -> &Generation {
        &self.generation
    }
    pub fn peer(&self) -> PeerPublicKey {
        self.peer
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
    fn submit(&self, message: &message::Message, mode: Mode) -> Result<Ticket, Error> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let slot = self.commands.try_reserve().map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::Busy,
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })?;
        let packet = message
            .encode_packet(self.config.messages)
            .map_err(Error::Message)?;
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
        let budget = self
            .budget
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| Error::Busy)?;
        let deadline = Instant::now()
            .checked_add(self.config.write_timeout)
            .ok_or(Error::Configuration)?;
        let (sender, reply) = oneshot::channel();
        let finished = Arc::new(AtomicBool::new(false));
        slot.send(Command {
            packet,
            mode,
            deadline,
            answer: Answer {
                sender: Some(sender),
                finished: finished.clone(),
            },
            budget,
        });
        Ok(Ticket {
            reply,
            finished,
            stop: self.stop.clone(),
            generation: self.generation.clone(),
        })
    }
}
