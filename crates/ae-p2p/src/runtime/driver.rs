use super::*;
use queue::{Answer, Command, Mode};
use tokio::sync::OwnedSemaphorePermit;

pub struct Driver {
    socket: TcpStream,
    crypto: noise::Transport,
    commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<Event>,
    stopped: watch::Receiver<Option<Error>>,
    generation: Generation,
    requests: requests::Requests,
    pending: Vec<(requests::Token, Answer)>,
    outgoing: Option<wire::Outgoing>,
    decoder: noise::record::Decoder,
    assembler: framing::Assembler,
    record_deadline: Option<Instant>,
    fragment_deadline: Option<Instant>,
    budget: Arc<Semaphore>,
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
        events: mpsc::Sender<Event>,
        stopped: watch::Receiver<Option<Error>>,
        generation: Generation,
        requests: requests::Requests,
        config: Config,
        terminal: lifecycle::Guard,
    ) -> Self {
        Self {
            socket,
            crypto,
            commands,
            events,
            stopped,
            generation,
            requests,
            pending: vec![],
            outgoing: None,
            decoder: noise::record::Decoder::new(noise::MAX_CIPHERTEXT),
            assembler: framing::Assembler::new(config.fragments),
            record_deadline: None,
            fragment_deadline: None,
            budget: Arc::new(Semaphore::new(config.incoming_bytes)),
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
        let _ = self.settle(update, None);
        self.commands.close();
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
            let now = Instant::now();
            let update = self.requests.expire(now.into_std());
            self.settle(update, None)?;
            if [
                self.record_deadline,
                self.fragment_deadline,
                self.outgoing.as_ref().map(|o| o.deadline),
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
                self.record_deadline,
                self.fragment_deadline,
                self.outgoing.as_ref().map(|o| o.deadline),
                self.requests.next_deadline().map(Instant::from_std),
            ]
            .into_iter()
            .flatten()
            .min();
            tokio::select! {
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
                ready = self.socket.readable() => {
                    ready?;
                    match self.socket.try_read(&mut bytes) {
                        Ok(0) => return Err(Error::Io(io::ErrorKind::UnexpectedEof)),
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
                        if let Some(answer) = outgoing.answer { answer.finish(Ok(Reply::Sent)); }
                    }
                }
                command = self.commands.recv(), if self.outgoing.is_none() => {
                    match command {
                        Some(command) => self.start(command)?,
                        None => return Err(Error::Closed),
                    }
                }
            }
        }
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
        self.outgoing = Some(wire::Outgoing::new(packet, deadline, answer, budget));
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
                let update = self.requests.receive_response(message, now);
                self.settle(update, Some(budget))
            }
            message::Kind::Close => Err(Error::RemoteClose),
            kind => {
                if kind == message::Kind::Ping {
                    let update = self.requests.receive_ping(&message, now);
                    self.settle(update, None)?;
                }
                self.event(Event::Message(Received::new(
                    message,
                    self.generation.clone(),
                    budget,
                )))
            }
        }
    }
    fn settle(
        &mut self,
        update: requests::Update,
        mut budget: Option<OwnedSemaphorePermit>,
    ) -> Result<(), Error> {
        for completion in update.completions {
            if let Some(index) = self
                .pending
                .iter()
                .position(|(token, _)| *token == completion.token)
            {
                let (_, answer) = self.pending.swap_remove(index);
                let reply = match completion.result {
                    Ok(message) => Ok(Reply::Response(Received::new(
                        message,
                        self.generation.clone(),
                        budget.take().expect("one wire response"),
                    ))),
                    Err(error) => Err(Error::RequestFailed(error)),
                };
                answer.finish(reply);
            }
        }
        if update.ping != requests::PingAction::None {
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
