use super::*;
use queue::{Answer, Command, Finish, Mode, PingReceipt, PingState};
use tokio::sync::OwnedSemaphorePermit;

struct PingWait {
    token: requests::Token,
    state: Arc<PingState>,
    deadline: Instant,
}
struct PostPing {
    close: queue::ClosingPing,
    deadline: Instant,
}

pub struct Driver {
    socket: TcpStream,
    crypto: noise::Transport,
    commands: mpsc::Receiver<Command>,
    replies: mpsc::Receiver<Command>,
    finishes: mpsc::Receiver<Finish>,
    events: mpsc::Sender<Event>,
    stopped: watch::Receiver<Option<Error>>,
    stop: watch::Sender<Option<Error>>,
    disconnect: watch::Receiver<bool>,
    closing: bool,
    sending_close: bool,
    close_deadline: Option<Instant>,
    generation: Generation,
    requests: requests::Requests,
    pending: Vec<(requests::Token, Answer)>,
    outgoing: Option<wire::Outgoing>,
    finishing: bool,
    prepared_finish: Option<requests::Update>,
    stop_after_ping: bool,
    close_after_ping: Option<queue::ClosingPing>,
    post_ping: Option<PostPing>,
    ping_wait: Option<PingWait>,
    buffered: Vec<u8>,
    decoder: noise::record::Decoder,
    assembler: framing::Assembler,
    record_deadline: Option<Instant>,
    fragment_deadline: Option<Instant>,
    budget: Arc<Semaphore>,
    selected_ping: Arc<std::sync::atomic::AtomicU16>,
    config: Config,
    // Struct fields drop in declaration order. Keep this guard last: observers
    // must not see completion until the socket, queues and pending replies drop,
    // including cancellation before run() is polled and task abort/unwinding.
    terminal: lifecycle::Guard,
}
impl Driver {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        socket: TcpStream,
        crypto: noise::Transport,
        commands: mpsc::Receiver<Command>,
        replies: mpsc::Receiver<Command>,
        finishes: mpsc::Receiver<Finish>,
        events: mpsc::Sender<Event>,
        stopped: watch::Receiver<Option<Error>>,
        stop: watch::Sender<Option<Error>>,
        disconnect: watch::Receiver<bool>,
        generation: Generation,
        requests: requests::Requests,
        config: Config,
        selected_ping: Arc<std::sync::atomic::AtomicU16>,
        terminal: lifecycle::Guard,
    ) -> Self {
        Self {
            socket,
            crypto,
            commands,
            replies,
            finishes,
            events,
            stopped,
            stop,
            disconnect,
            closing: false,
            sending_close: false,
            close_deadline: None,
            generation,
            requests,
            pending: vec![],
            outgoing: None,
            finishing: false,
            prepared_finish: None,
            stop_after_ping: false,
            close_after_ping: None,
            post_ping: None,
            ping_wait: None,
            buffered: vec![],
            decoder: noise::record::Decoder::new(noise::MAX_CIPHERTEXT),
            assembler: framing::Assembler::new(config.fragments),
            record_deadline: None,
            fragment_deadline: None,
            budget: Arc::new(Semaphore::new(config.incoming_bytes)),
            selected_ping,
            config,
            terminal,
        }
    }
    pub fn generation(&self) -> &Generation {
        &self.generation
    }
    // This future owns the connection. It creates no detached tasks; its caller
    // must await it (or join the task running it) during service shutdown.
    pub async fn run(mut self) -> Exit {
        let reason = self.run_inner().await.unwrap_err();
        let update = requests::Update {
            completions: self.requests.disconnect(),
            ..Default::default()
        };
        let _ = self.settle(update, None, None);
        self.commands.close();
        self.replies.close();
        self.finishes.close();
        self.crypto.close();
        self.decoder.close();
        self.assembler.reset();
        self.terminal.reason(reason.clone());
        Exit {
            generation: self.generation.clone(),
            reason,
        }
    }
    async fn run_inner(&mut self) -> Result<(), Error> {
        let mut bytes = [0; 8192];
        loop {
            if let Some(reason) = self.stopped.borrow().clone() {
                return Err(reason);
            }
            self.close_if_requested()?;
            if !self.fenced() && !self.sending_close && !self.buffered.is_empty() {
                let buffered = std::mem::take(&mut self.buffered);
                self.input(&buffered)?;
            }
            let now = Instant::now();
            if self.close_deadline.is_some_and(|deadline| deadline <= now) {
                return Err(Error::LocalClose);
            }
            if !self.fenced() && !self.sending_close {
                let update = self.requests.expire(now.into_std());
                self.settle(update, None, None)?;
            }
            if [
                self.record_deadline,
                self.fragment_deadline,
                self.outgoing.as_ref().map(|o| o.deadline),
                self.ping_wait.as_ref().map(|p| p.deadline),
                self.post_ping.as_ref().map(|p| p.deadline),
            ]
            .into_iter()
            .flatten()
            .any(|deadline| deadline <= now)
            {
                return Err(Error::Timeout);
            }
            if let Some(outgoing) = &mut self.outgoing {
                outgoing.prepare(&mut self.crypto)?;
            }
            let deadline = [
                self.close_deadline,
                self.record_deadline,
                self.fragment_deadline,
                self.outgoing.as_ref().map(|o| o.deadline),
                (!self.fenced() && !self.sending_close)
                    .then(|| self.requests.next_deadline().map(Instant::from_std))
                    .flatten(),
                self.ping_wait.as_ref().map(|p| p.deadline),
                self.post_ping.as_ref().map(|p| p.deadline),
            ]
            .into_iter()
            .flatten()
            .min();
            tokio::select! {
                released = async { (&mut self.post_ping.as_mut().expect("post-ping fence").close.release).await }, if self.post_ping.is_some() => {
                    if released.is_err() {
                        return Err(self.stopped.borrow().clone().unwrap_or(Error::Cancelled));
                    }
                    self.post_ping = None;
                }
                changed = self.disconnect.changed(), if !*self.disconnect.borrow() => {
                    if changed.is_err() { return Err(Error::Closed); }
                }
                _ = self.stopped.changed() => {
                    if self.stopped.borrow().is_none() { return Err(Error::Closed); }
                }
                _ = self.events.closed() => return Err(Error::Closed),
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => (),
                ready = self.socket.readable(), if !self.fenced() && !self.sending_close => {
                    ready?;
                    match self.socket.try_read(&mut bytes) {
                        Ok(0) => return Err(if self.closing { Error::LocalClose } else { Error::Io(io::ErrorKind::UnexpectedEof) }),
                        Ok(n) => self.input(&bytes[..n])?,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                        Err(e) => return Err(e.into()),
                    }
                }
                ready = self.socket.writable(), if self.outgoing.is_some() => {
                    ready?;
                    let outgoing = self.outgoing.as_mut().expect("active output");
                    if outgoing.write(&self.socket)? {
                        let outgoing = self.outgoing.take().unwrap();
                        if self.sending_close {
                            self.sending_close = false;
                            self.close_deadline = Some(Instant::now().checked_add(self.config.close_timeout).ok_or(Error::Configuration)?);
                        }
                        if let Some(answer) = outgoing.answer {
                            if self.finishing {
                                self.finishing = false;
                                self.commit_ping(answer)?;
                            } else {
                                answer.finish(Ok(Reply::Sent));
                            }
                        }
                    }
                }
                command = self.commands.recv(), if self.outgoing.is_none() && !self.fenced()
                    && (!self.closing || !self.commands.is_closed() || !self.commands.is_empty()) => {
                    match command {
                        Some(command) => self.start(command)?,
                        None => return Err(Error::Closed),
                    }
                }
                finish = self.finishes.recv(), if self.outgoing.is_none() && self.ping_wait.is_some() => {
                    match finish {
                        Some(finish) => self.finish(finish)?,
                        None => return Err(Error::Closed),
                    }
                }
                reply = self.replies.recv(), if self.outgoing.is_none() && self.post_ping.is_none() && (!self.replies.is_closed() || !self.replies.is_empty()) => {
                    if let Some(reply) = reply { self.start(reply)?; }
                }
            }
        }
    }
    fn fenced(&self) -> bool {
        self.ping_wait.is_some() || self.post_ping.is_some()
    }
    fn close_if_requested(&mut self) -> Result<(), Error> {
        if self.closing || !*self.disconnect.borrow() || self.fenced() || self.outgoing.is_some() {
            return Ok(());
        }
        let deadline = Instant::now()
            .checked_add(self.config.write_timeout)
            .ok_or(Error::Configuration)?;
        self.outgoing = Some(wire::Outgoing::close(deadline)?);
        self.closing = true;
        self.sending_close = true;
        Ok(())
    }
    fn finish(&mut self, finish: Finish) -> Result<(), Error> {
        if self
            .ping_wait
            .as_ref()
            .is_none_or(|p| p.token != finish.token)
        {
            finish.answer.finish(Err(Error::InvalidPing));
            return Ok(());
        }
        if finish.deadline <= Instant::now() {
            finish.answer.finish(Err(Error::Timeout));
            return Err(Error::Timeout);
        }
        self.stop_after_ping = finish.stop_after;
        self.close_after_ping = finish.close_after;
        if let Some(packet) = finish.packet {
            if let Some(probe) = finish.probe {
                match self.prepare_probe(&finish.token, &packet, probe) {
                    Ok(update) => self.prepared_finish = Some(update),
                    Err(error) => {
                        finish.answer.finish(Err(error.clone()));
                        return Err(error);
                    }
                }
            }
            if self.closing {
                // Native send_msg suppresses bytes while disconnecting, but
                // the callback still commits negotiation and probe ownership.
                return self.commit_ping(finish.answer);
            }
            self.finishing = true;
            self.outgoing = Some(wire::Outgoing::new(
                packet,
                finish.deadline,
                Some(finish.answer),
                finish.budget,
            ));
            Ok(())
        } else {
            self.commit_ping(finish.answer)
        }
    }
    fn prepare_probe(
        &mut self,
        token: &requests::Token,
        packet: &[u8],
        answer: Answer,
    ) -> Result<requests::Update, Error> {
        let request = message::Message::decode_packet(packet, self.config.messages)
            .map_err(Error::Message)?;
        let update = self.requests.finish_ping(token).map_err(Error::Request)?;
        if update.ping != requests::PingAction::ProbeV2 {
            self.settle(update, None, None)?;
            return Err(Error::InvalidPing);
        }
        let pending = self
            .requests
            .start_probe(&request, Instant::now().into_std())
            .map_err(Error::Request)?;
        self.pending.push((pending.token, answer));
        Ok(update)
    }
    fn commit_ping(&mut self, answer: Answer) -> Result<(), Error> {
        let ping = self.ping_wait.take().expect("validated ping completion");
        ping.state
            .finished
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let update = match self.prepared_finish.take() {
            Some(update) => update,
            None => self
                .requests
                .finish_ping(&ping.token)
                .map_err(Error::Request)?,
        };
        if let Err(error) = self.settle(update, None, None) {
            answer.finish(Err(error.clone()));
            return Err(error);
        }
        if self.stop_after_ping || ping.state.retire.load(std::sync::atomic::Ordering::SeqCst) {
            answer.finish(Ok(Reply::Sent));
            Err(Error::Shutdown)
        } else {
            if let Some(close) = self.close_after_ping.take() {
                close.ready.store(
                    if self.closing { 2 } else { 1 },
                    std::sync::atomic::Ordering::Release,
                );
                self.post_ping = Some(PostPing {
                    close,
                    deadline: ping.deadline,
                });
            }
            answer.finish(Ok(Reply::Sent));
            Ok(())
        }
    }
    fn stage_ping(
        &mut self,
        update: &requests::Update,
        incoming: bool,
    ) -> Result<Option<PingReceipt>, Error> {
        let Some(token) = &update.ping_deferred else {
            return Ok(None);
        };
        if self.ping_wait.is_some() {
            return Err(Error::InvalidPing);
        }
        let deadline = Instant::now()
            .checked_add(self.config.ping_timeout)
            .ok_or(Error::Configuration)?;
        let state = Arc::new(PingState::default());
        self.ping_wait = Some(PingWait {
            token: token.clone(),
            state: state.clone(),
            deadline,
        });
        Ok(Some(PingReceipt {
            token: token.clone(),
            generation: self.generation.clone(),
            state,
            incoming,
            probe_required: update.ping == requests::PingAction::ProbeV2,
            stop: self.stop.clone(),
        }))
    }
    fn start(&mut self, command: Command) -> Result<(), Error> {
        let Command {
            packet,
            mode,
            deadline,
            answer,
            budget,
        } = command;
        if deadline <= Instant::now() {
            answer.finish(Err(Error::Timeout));
            return Ok(());
        }
        // A command and close intent can wake the same select. Recheck at
        // command start, before registering/writing a new application packet.
        // Earlier replies needed by a deferred ping remain allowed to finish.
        self.close_if_requested()?;
        let answer = match mode {
            Mode::Send => Some(answer),
            Mode::Request { timeout, probe } => {
                let request = message::Message::decode_packet(&packet, self.config.messages)
                    .map_err(Error::Message)?;
                let now = Instant::now().into_std();
                let started = if probe {
                    self.requests.start_probe(&request, now)
                } else {
                    self.requests.start(&request, now, timeout)
                };
                match started {
                    Ok(started) => {
                        self.pending.push((started.token, answer));
                        None
                    }
                    Err(error) => {
                        answer.finish(Err(Error::Request(error)));
                        return Ok(());
                    }
                }
            }
        };
        if self.closing {
            if let Some(answer) = answer {
                answer.finish(Ok(Reply::Sent));
            }
        } else {
            self.outgoing = Some(wire::Outgoing::new(packet, deadline, answer, budget));
        }
        Ok(())
    }
    fn input(&mut self, mut bytes: &[u8]) -> Result<(), Error> {
        while !bytes.is_empty() {
            let now = Instant::now();
            if !self.decoder.is_pending() {
                self.record_deadline = Some(
                    now.checked_add(self.config.partial_timeout)
                        .ok_or(Error::Configuration)?,
                );
            }
            let progress = self.decoder.push(bytes)?;
            bytes = &bytes[progress.consumed..];
            if let Some(record) = progress.record {
                self.record_deadline = None;
                let plaintext = self.crypto.open(&record)?;
                let packet = self.assembler.push(&plaintext).map_err(Error::Frame)?;
                if self.assembler.is_pending() {
                    if self.fragment_deadline.is_none() {
                        self.fragment_deadline = Some(
                            now.checked_add(self.config.partial_timeout)
                                .ok_or(Error::Configuration)?,
                        );
                    }
                } else {
                    self.fragment_deadline = None;
                }
                if let Some(packet) = packet {
                    self.message(packet)?;
                    if self.ping_wait.is_some() {
                        // At most the unconsumed tail of one 8192-byte read.
                        // Keep ciphertext order and let socket backpressure
                        // bound later traffic while the application finishes.
                        self.buffered = bytes.to_vec();
                        break;
                    }
                }
            }
        }
        Ok(())
    }
    fn message(&mut self, packet: Vec<u8>) -> Result<(), Error> {
        let bytes = u32::try_from(packet.len()).map_err(|_| Error::Backpressure)?;
        let budget = self
            .budget
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| Error::Backpressure)?;
        let message = message::Message::decode_packet(&packet, self.config.messages)
            .map_err(Error::Message)?;
        let now = Instant::now().into_std();
        match message.kind() {
            message::Kind::Response => {
                let update = if self.config.defer_pings {
                    self.requests.receive_response_deferred(message, now)
                } else {
                    self.requests.receive_response(message, now)
                };
                let receipt = self.stage_ping(&update, false)?;
                self.settle(update, Some(budget), receipt)
            }
            message::Kind::Close => Err(if self.closing {
                Error::LocalClose
            } else {
                Error::RemoteClose
            }),
            kind => {
                let mut ping = None;
                let mut receipt = None;
                if kind == message::Kind::Ping {
                    let update = if self.config.defer_pings {
                        self.requests.receive_ping_deferred(&message, now)
                    } else {
                        self.requests.receive_ping(&message, now)
                    };
                    ping = Some(PingObservation {
                        previous: update.ping_before,
                        action: update.ping,
                        busy: update.ping_busy,
                    });
                    receipt = self.stage_ping(&update, true)?;
                    self.settle(update, None, None)?;
                }
                self.event(Event::Message(Received::new(
                    message,
                    self.generation.clone(),
                    budget,
                    ping,
                    receipt,
                )))
            }
        }
    }
    fn settle(
        &mut self,
        update: requests::Update,
        mut budget: Option<OwnedSemaphorePermit>,
        mut receipt: Option<PingReceipt>,
    ) -> Result<(), Error> {
        self.selected_ping.store(
            self.requests.selected_ping_version().unwrap_or(0),
            std::sync::atomic::Ordering::Release,
        );
        let observation = PingObservation {
            previous: update.ping_before,
            action: update.ping,
            busy: update.ping_busy,
        };
        for completion in update.completions {
            if let Some(index) = self
                .pending
                .iter()
                .position(|(token, _)| *token == completion.token)
            {
                let (_, answer) = self.pending.swap_remove(index);
                let reply = match completion.result {
                    Ok(message) => {
                        let ping = matches!(
                            message.details(),
                            message::Details::Response {
                                kind: message::Kind::Ping,
                                ..
                            }
                        )
                        .then_some(observation);
                        Ok(Reply::Response(Received::new(
                            message,
                            self.generation.clone(),
                            budget.take().expect("one wire response"),
                            ping,
                            receipt.take(),
                        )))
                    }
                    Err(error) => Err(Error::RequestFailed(error)),
                };
                answer.finish(reply);
            }
        }
        if update.ping_deferred.is_none() && update.ping != requests::PingAction::None {
            self.event(Event::Ping {
                generation: self.generation.clone(),
                action: update.ping,
            })?;
        }
        if let Some(error) = update.error {
            return Err(Error::Request(error));
        }
        if self.requests.is_closed() {
            return Err(Error::Closed);
        }
        Ok(())
    }
    fn event(&self, event: Event) -> Result<(), Error> {
        self.events.try_send(event).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::Backpressure,
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })
    }
}
