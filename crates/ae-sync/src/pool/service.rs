use super::*;
use connections::{Connections, Event as PeerEvent};

pub struct Service {
    db: Arc<Database>,
    admission: worker::Client,
    config: Config,
    commands: mpsc::Receiver<Command>,
    forwarded: mpsc::Sender<Forwarded>,
    stopped: watch::Receiver<bool>,
    status: watch::Sender<Status>,
    current: Status,
    peers: Connections,
    next_id: u64,
    manager: session::Manager,
    watches: Vec<(session::Token, u64)>,
    actions: Vec<Action>,
    incoming: Vec<Incoming>,
    cooldown: Option<(session::Token, Instant)>,
    tasks: Tasks<Key, Done>,
    blocking: blocking::Pool,
}
impl Service {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        db: Arc<Database>,
        admission: worker::Client,
        config: Config,
        blocking: blocking::Pool,
        commands: mpsc::Receiver<Command>,
        forwarded: mpsc::Sender<Forwarded>,
        stopped: watch::Receiver<bool>,
        status: watch::Sender<Status>,
    ) -> Self {
        Self {
            db,
            admission,
            config,
            commands,
            forwarded,
            stopped,
            status,
            current: Status::default(),
            peers: Connections::new(),
            next_id: 1,
            manager: session::Manager::new(config.session),
            watches: vec![],
            actions: vec![],
            incoming: vec![],
            cooldown: None,
            tasks: Tasks::new(),
            blocking,
        }
    }
    pub async fn run(mut self) -> Status {
        self.run_inner().await;
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            drop(command);
        }
        self.manager.shutdown();
        self.tasks.clear();
        self.actions.clear();
        self.incoming.clear();
        self.watches.clear();
        self.cooldown = None;
        self.peers.close_all();
        while self.peers.len() != 0 {
            let _ = self.peers.next().await;
        }
        self.blocking.stop();
        self.blocking.join().await;
        self.current.running = false;
        self.refresh();
        self.current.clone()
    }
    async fn run_inner(&mut self) {
        loop {
            if *self.stopped.borrow() {
                break;
            }
            self.expire();
            self.refresh();
            let deadline = self.deadline();
            tokio::select! {
                _ = self.stopped.changed() => {
                    if *self.stopped.borrow() || self.stopped.has_changed().is_err() {
                        break;
                    }
                },
                _ = self.forwarded.closed() => break,
                _ = async {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending::<()>().await,
                    }
                } => (),
                command = self.commands.recv() => match command {
                    Some(command) => self.command(command),
                    None => break,
                },
                event = self.peers.next() => self.peer(event),
                (key, result) = self.tasks.next() => self.finished(key, result),
            }
        }
    }
    fn command(&mut self, command: Command) {
        match command {
            Command::Peers(reply) => {
                let _ = reply.send(Ok(self.peers.clients()));
            }
            Command::Attach {
                connection,
                initiate,
                admitted,
                permit,
                reply,
            } => {
                let Some(next) = self.next_id.checked_add(1) else {
                    let _ = reply.send(Err(Error::IdExhausted));
                    return;
                };
                let id = self.next_id;
                self.next_id = next;
                let peer = connection.peer();
                self.peers.add(id, *connection, permit, admitted);
                if initiate {
                    let effects = self.manager.connect(Hash256(peer.0), id);
                    self.effects(effects);
                }
                let _ = reply.send(Ok(id));
            }
            Command::Promote { client, reply } => {
                let _ = reply.send(self.peers.promote(&client));
            }
            Command::Sync { peer, reply } => {
                if let Some(entry) = self.peers.current(peer) {
                    let effects = self.manager.connect(Hash256(peer.0), entry.id);
                    let started = !effects.is_empty();
                    self.effects(effects);
                    let _ = reply.send(Ok(started));
                } else {
                    let _ = reply.send(Err(Error::NoPeer));
                }
            }
        }
    }
    fn deadline(&self) -> Option<Instant> {
        self.actions
            .iter()
            .map(|a| a.deadline)
            .chain(self.incoming.iter().map(|r| r.deadline))
            .chain(self.cooldown.as_ref().map(|(_, d)| *d))
            .min()
    }
    fn expire(&mut self) {
        let now = Instant::now();
        let expired: Vec<_> = self
            .actions
            .iter()
            .filter(|a| a.deadline <= now)
            .map(|a| a.action.token.clone())
            .collect();
        for token in expired {
            let transition = self.manager.timeout(&token);
            self.transition(transition);
        }
        let incoming: Vec<_> = self
            .incoming
            .iter()
            .filter(|r| r.deadline <= now)
            .map(|r| r.id)
            .collect();
        for id in incoming {
            self.tasks.remove(&Key::Incoming(id));
            self.incoming.retain(|r| r.id != id);
            self.peers.close(id);
        }
        if self
            .cooldown
            .as_ref()
            .is_some_and(|(_, deadline)| *deadline <= now)
        {
            let (token, _) = self.cooldown.take().unwrap();
            self.manager.resync(&token);
        }
    }
    fn transition(&mut self, transition: session::Transition) {
        if let Some(failure) = transition.failure {
            self.current.counts.failures = self.current.counts.failures.saturating_add(1);
            self.current.last_failure = Some(failure);
        }
        self.effects(transition.effects);
    }
    fn effects(&mut self, effects: Vec<session::Effect>) {
        let mut queue: VecDeque<_> = effects.into();
        while let Some(effect) = queue.pop_front() {
            match effect {
                session::Effect::Watch {
                    token, connection, ..
                } => self.watches.push((token, connection)),
                session::Effect::Unwatch(token) => self.watches.retain(|(t, _)| *t != token),
                session::Effect::CancelAction(token) => {
                    self.tasks.remove(&Key::Action(token.clone()));
                    self.actions.retain(|a| a.action.token != token);
                }
                session::Effect::Resync { token, delay_ms } => {
                    if let Some(deadline) =
                        Instant::now().checked_add(Duration::from_millis(delay_ms))
                    {
                        self.cooldown = Some((token, deadline));
                    } else {
                        self.manager.resync(&token);
                    }
                }
                session::Effect::CancelResync(token) => {
                    if self.cooldown.as_ref().is_some_and(|(t, _)| *t == token) {
                        self.cooldown = None;
                    }
                }
                session::Effect::Start(action) => {
                    let timeout = if matches!(*action.work, session::Work::Get(_)) {
                        self.config.get_timeout
                    } else {
                        self.config.action_timeout
                    };
                    let deadline = Instant::now().checked_add(timeout);
                    if self.tasks.len() >= self.config.operations || deadline.is_none() {
                        let transition = self
                            .manager
                            .complete(&action.token, Err(session::Failure::ResourceLimit));
                        self.current.counts.failures =
                            self.current.counts.failures.saturating_add(1);
                        self.current.last_failure = Some(session::Failure::ResourceLimit);
                        queue.extend(transition.effects);
                        continue;
                    }
                    self.actions.push(Action {
                        action: action.clone(),
                        deadline: deadline.unwrap(),
                    });
                    self.start(action);
                }
            }
        }
    }
    fn start(&mut self, action: session::Action) {
        let key = Key::Action(action.token.clone());
        let config = self.config;
        let jobs = self.blocking.clone();
        let db = self.db.clone();
        match &*action.work {
            session::Work::Tree => self.insert_action(key, async move {
                Done::Action(operations::tree(db, jobs, config).await)
            }),
            session::Work::Unfold { delay_ms, .. } if *delay_ms != 0 => {
                let delay = Duration::from_millis(*delay_ms);
                self.insert_action(key, async move {
                    tokio::time::sleep(delay).await;
                    Done::Wake(action)
                });
            }
            _ => self.network(action),
        }
    }
    fn insert_action(
        &mut self,
        key: Key,
        future: impl std::future::Future<Output = Done> + Send + 'static,
    ) {
        let Key::Action(token) = &key else {
            unreachable!()
        };
        let deadline = self
            .actions
            .iter()
            .find(|a| a.action.token == *token)
            .expect("active action")
            .deadline;
        self.tasks.insert(key, async move {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    Done::Action(Outcome::reply(Err(session::Failure::Timeout)))
                },
                result = future => result,
            }
        });
    }
    fn network(&mut self, action: session::Action) {
        let key = Key::Action(action.token.clone());
        if let Some(entry) = self.peers.current(PeerPublicKey(action.peer.0)) {
            let client = entry.connection.client.clone();
            let db = self.db.clone();
            let admission = self.admission.clone();
            let jobs = self.blocking.clone();
            let config = self.config;
            self.insert_action(key, async move {
                Done::Action(operations::action(action, client, db, admission, jobs, config).await)
            });
        } else {
            let result = if matches!(*action.work, session::Work::Finish(_)) {
                Ok(session::Reply::Finished)
            } else {
                Err(session::Failure::Action)
            };
            self.insert_action(key, async move { Done::Action(Outcome::reply(result)) });
        }
    }
    fn finished(&mut self, key: Key, result: Done) {
        if let Key::Action(token) = &key
            && self
                .actions
                .iter()
                .any(|a| a.action.token == *token && a.deadline <= Instant::now())
        {
            let transition = self.manager.timeout(token);
            self.transition(transition);
            return;
        }
        match (key, result) {
            (Key::Action(token), Done::Wake(action)) => {
                if self.actions.iter().any(|a| a.action.token == token) {
                    self.network(action);
                }
            }
            (Key::Action(token), Done::Action(outcome)) => {
                let Some(index) = self.actions.iter().position(|a| a.action.token == token) else {
                    return;
                };
                self.actions.swap_remove(index);
                self.current.counts.fetched = self
                    .current
                    .counts
                    .fetched
                    .saturating_add(outcome.fetched as u64);
                self.current.counts.admitted = self
                    .current
                    .counts
                    .admitted
                    .saturating_add(outcome.admitted as u64);
                self.current.counts.rejected = self
                    .current
                    .counts
                    .rejected
                    .saturating_add(outcome.rejected as u64);
                let finishing = matches!(self.manager.local(),session::Local::Active(s)if s.pending().is_some_and(|a|matches!(*a.work,session::Work::Finish(true))));
                let transition = self.manager.complete(&token, outcome.reply);
                self.transition(transition);
                if finishing && matches!(self.manager.local(), session::Local::Cooldown(_)) {
                    self.current.counts.completed = self.current.counts.completed.saturating_add(1);
                }
            }
            (Key::Incoming(id), Done::Sent(result)) => {
                self.incoming.retain(|r| r.id != id);
                self.peers.busy(id, false);
                if result.is_err() {
                    self.peers.close(id);
                }
            }
            _ => unreachable!("typed task/result pair"),
        }
    }
    fn peer(&mut self, event: PeerEvent) {
        match event {
            PeerEvent::Down { id, peer } => {
                self.current.counts.connections_closed =
                    self.current.counts.connections_closed.saturating_add(1);
                self.tasks.remove(&Key::Incoming(id));
                self.incoming.retain(|r| r.id != id);
                let replacement = self.peers.current(peer).map(|e| e.id);
                let watches: Vec<_> = self
                    .watches
                    .iter()
                    .filter(|(_, connection)| *connection == id)
                    .map(|(token, _)| token.clone())
                    .collect();
                for monitor in watches {
                    let effects = self.manager.connection_down(&monitor, replacement);
                    self.effects(effects);
                }
            }
            PeerEvent::Message { id, peer, event } => {
                let Some(entry) = self
                    .peers
                    .get(id)
                    .filter(|e| !e.connection.client.is_closed() && e.connection.peer() == peer)
                else {
                    return;
                };
                let generation = match &event {
                    net::Event::Message(m) => &m.generation,
                    net::Event::Ping { generation, .. } => generation,
                };
                if generation != entry.connection.client.generation() {
                    self.peers.close(id);
                    return;
                }
                let client = entry.connection.client.clone();
                if let net::Event::Message(received) = event {
                    if matches!(
                        received.message().kind(),
                        Kind::PoolInit | Kind::PoolUnfold | Kind::PoolGet | Kind::PoolFinish
                    ) {
                        self.request(id, peer, received);
                        return;
                    }
                    if self
                        .forwarded
                        .try_send(Forwarded {
                            client,
                            peer,
                            event: net::Event::Message(received),
                        })
                        .is_err()
                    {
                        self.peers.close(id);
                    }
                } else if self
                    .forwarded
                    .try_send(Forwarded {
                        client,
                        peer,
                        event,
                    })
                    .is_err()
                {
                    self.peers.close(id);
                }
            }
        }
    }
    fn request(&mut self, id: u64, peer: PeerPublicKey, received: net::Received) {
        if self.tasks.len() >= self.config.operations {
            self.peers.close(id);
            return;
        }
        let Some(entry) = self.peers.get(id) else {
            return;
        };
        let client = entry.connection.client.clone();
        let key = Hash256(peer.0);
        let kind = received.message().kind();
        let response = match kind {
            Kind::PoolInit => match self.manager.accept(key, id) {
                Ok(effects) => {
                    self.effects(effects);
                    wire::Reply::Init.into_response(self.config.messages)
                }
                Err(error) => Message::response_error(
                    kind,
                    operations::reason(error).to_vec(),
                    self.config.messages,
                ),
            },
            Kind::PoolFinish => {
                let Ok(wire::Request::Finish(done)) =
                    wire::Request::from_message(received.message())
                else {
                    self.peers.close(id);
                    return;
                };
                let effects = self.manager.finish_remote(key);
                self.effects(effects);
                wire::Reply::Finished(done).into_response(self.config.messages)
            }
            Kind::PoolGet | Kind::PoolUnfold => {
                let authorized = self.manager.authorize_get(key);
                let tree = self
                    .manager
                    .incoming()
                    .iter()
                    .find(|s| s.peer() == key)
                    .and_then(|s| s.tree().cloned());
                let authorized = authorized.and_then(|()| {
                    if kind == Kind::PoolUnfold && tree.is_none() {
                        Err(session::RequestError::TreeNotReady)
                    } else {
                        Ok(())
                    }
                });
                match authorized {
                    Ok(()) => {
                        let db = self.db.clone();
                        let jobs = self.blocking.clone();
                        let config = self.config;
                        self.respond(id, async move {
                            operations::serve(received, tree, db, client, jobs, config).await
                        });
                        return;
                    }
                    Err(error) => Message::response_error(
                        if kind == Kind::PoolGet {
                            Kind::Transactions
                        } else {
                            kind
                        },
                        operations::reason(error).to_vec(),
                        self.config.messages,
                    ),
                }
            }
            _ => unreachable!(),
        };
        match response {
            Ok(response) => {
                let ticket = client.send(&response);
                self.respond(id, operations::send(ticket));
            }
            Err(_) => self.peers.close(id),
        }
    }
    fn respond(
        &mut self,
        id: u64,
        future: impl std::future::Future<Output = Result<(), Error>> + Send + 'static,
    ) {
        if self.tasks.len() >= self.config.operations {
            self.peers.close(id);
            return;
        }
        let Some(deadline) = Instant::now().checked_add(self.config.response_timeout) else {
            self.peers.close(id);
            return;
        };
        self.peers.busy(id, true);
        self.incoming.push(Incoming { id, deadline });
        self.tasks
            .insert(Key::Incoming(id), async move { Done::Sent(future.await) });
    }
    fn refresh(&mut self) {
        self.current.peers = self.peers.len();
        self.current.incoming = self.manager.incoming().len();
        self.current.operations = self.tasks.len();
        self.current.local = match self.manager.local() {
            session::Local::Idle => Local::Idle,
            session::Local::Cooldown(_) => Local::Cooldown,
            session::Local::Active(s) => Local::Active {
                peer: PeerPublicKey(s.peer().0),
                action: match s.pending().map(|a| &*a.work) {
                    Some(session::Work::Tree) => "tree",
                    Some(session::Work::Init) => "init",
                    Some(session::Work::Unfold { .. }) => "unfold",
                    Some(session::Work::Get(_)) => "get",
                    Some(session::Work::Finish(_)) => "finish",
                    None => "idle",
                },
            },
        };
        if *self.status.borrow() != self.current {
            self.status.send_replace(self.current.clone());
        }
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            drop(command);
        }
        self.peers.clear();
        self.tasks.clear();
        self.blocking.stop();
    }
}
