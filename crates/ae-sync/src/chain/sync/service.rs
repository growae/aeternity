use super::*;
#[cfg(test)]
mod tests;
type Key = (PeerPublicKey, net::Generation);
struct Lane {
    client: net::Client,
    pending: Option<Action>,
    active: Option<Action>,
    replacement: Option<(net::Client, Action)>,
    progress: watch::Receiver<Progress>,
    publish: watch::Sender<Progress>,
}
pub struct Service<O: MapOrder + Send + Sync + 'static = Otp26> {
    context: Arc<Context<O>>,
    application: peer::Handle,
    incoming: peer::Events,
    forwarded: mpsc::Sender<peer::Event>,
    stopped: watch::Receiver<bool>,
    changed: watch::Receiver<()>,
    published: watch::Sender<Status>,
    current: Status,
    lanes: BTreeMap<PeerPublicKey, Lane>,
    ready: VecDeque<PeerPublicKey>,
    tasks: Tasks<Key, Completion>,
    target: u64,
    blocks: blocks::Owner<O>,
    lifecycle: lifecycle::Owner<O>,
    policy_config: watch::Receiver<ae_chain::resilience::Config>,
}
impl<O: MapOrder + Send + Sync + 'static> Service<O> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        context: Arc<Context<O>>,
        application: peer::Handle,
        incoming: peer::Events,
        forwarded: mpsc::Sender<peer::Event>,
        stopped: watch::Receiver<bool>,
        changed: watch::Receiver<()>,
        published: watch::Sender<Status>,
        current: Status,
        policy_config: watch::Receiver<ae_chain::resilience::Config>,
    ) -> Self {
        Self {
            blocks: blocks::Owner::new(context.clone()),
            lifecycle: lifecycle::Owner::new(context.clone()),
            policy_config,
            context,
            application,
            incoming,
            forwarded,
            stopped,
            changed,
            published,
            current,
            lanes: BTreeMap::new(),
            ready: VecDeque::new(),
            tasks: Tasks::new(),
            target: 0,
        }
    }
    fn route(&mut self, client: net::Client, intent: &peer::ChainIntent) -> Result<(), Error> {
        if client.is_closed() {
            return Err(Error::Stale);
        }
        let hash = |raw: &[u8]| raw.try_into().map(Hash256).map_err(|_| Error::Hash);
        let action = match intent {
            peer::ChainIntent::Done => {
                self.current.counts.done_announcements =
                    self.current.counts.done_announcements.saturating_add(1);
                self.lifecycle.done(
                    client.peer(),
                    client.generation().clone(),
                    lifecycle::DoneReason::Ping,
                );
                return Ok(());
            }
            peer::ChainIntent::Generation(bytes) => {
                if self.lifecycle.suppress_generation() {
                    return Ok(());
                }
                Action::Forward(hash(bytes)?)
            }
            peer::ChainIntent::Start { best, .. } => Action::Start(hash(best)?),
        };
        if client.is_closed() {
            return Err(Error::Stale);
        }
        if client.is_disconnecting() {
            // Existing work can consume crossing replies during grace, but
            // closing callbacks must not schedule another outbound session.
            return Ok(());
        }
        let peer = client.peer();
        if let Some(lane) = self.lanes.get_mut(&peer)
            && lane.client.generation() != client.generation()
        {
            if !lane.client.is_closed() {
                if lane.client.is_disconnecting() {
                    if lane.active.is_some() {
                        // Dropping this task would cancel its request ticket
                        // and abruptly stop the old transport. Retain one
                        // replacement intent until the old task settles.
                        if lane.replacement.replace((client, action)).is_some() {
                            self.current.counts.coalesced =
                                self.current.counts.coalesced.saturating_add(1);
                        }
                        lane.pending = None;
                        return Ok(());
                    }
                } else {
                    return Err(Error::Stale);
                }
            }
            if lane.active.is_some() {
                self.current.counts.cancelled = self.current.counts.cancelled.saturating_add(1);
            }
            self.tasks.remove(&(peer, lane.client.generation().clone()));
            self.lanes.remove(&peer);
            self.ready.retain(|key| *key != peer);
            self.current.counts.replaced = self.current.counts.replaced.saturating_add(1);
        }
        if !self.lanes.contains_key(&peer) {
            if self.lanes.len() >= self.context.config.peers {
                return Err(Error::Busy);
            }
            let (publish, progress) = watch::channel(Progress::default());
            self.lanes.insert(
                peer,
                Lane {
                    client,
                    pending: None,
                    active: None,
                    replacement: None,
                    progress,
                    publish,
                },
            );
        }
        let lane = self.lanes.get_mut(&peer).expect("inserted lane");
        if lane.active == Some(action) || lane.pending == Some(action) {
            self.current.counts.coalesced = self.current.counts.coalesced.saturating_add(1);
            return Ok(());
        }
        if lane.pending.replace(action).is_some() {
            self.current.counts.coalesced = self.current.counts.coalesced.saturating_add(1);
        }
        if lane.active.is_none() {
            self.ready.retain(|key| *key != peer);
            self.ready.push_back(peer);
        }
        Ok(())
    }
    fn start_ready(&mut self) {
        while self.tasks.len() < self.context.config.concurrent {
            let Some(peer) = self.ready.pop_front() else {
                break;
            };
            let Some(lane) = self.lanes.get_mut(&peer) else {
                continue;
            };
            if lane.active.is_some() {
                continue;
            }
            if lane.client.is_closed() || lane.client.is_disconnecting() {
                self.lanes.remove(&peer);
                continue;
            }
            let Some(action) = lane.pending.take() else {
                continue;
            };
            lane.active = Some(action);
            lane.publish.send_replace(Progress::default());
            self.tasks.insert(
                (peer, lane.client.generation().clone()),
                session::run(
                    self.context.clone(),
                    lane.client.clone(),
                    action,
                    lane.publish.clone(),
                ),
            );
            self.current.counts.started = self.current.counts.started.saturating_add(1);
        }
    }
    fn observe_shared(&mut self) -> u64 {
        let book = self
            .context
            .shared
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (tasks, target, transitions) = book.lifecycle();
        let notices = book.notices();
        if let Some(error) = notices.error {
            self.current.last_error = Some(Error::Events(error));
        }
        self.current.statistics = book.statistics();
        drop(book);
        self.lifecycle.observe(tasks, target, transitions, notices);
        self.current.lifecycle = self.lifecycle.status();
        target
    }
    fn publish(&mut self) -> Result<(), Error> {
        self.current.blocks = self.blocks.status();
        self.current.active = self
            .lanes
            .iter()
            .filter(|(_, lane)| lane.active.is_some())
            .map(|(peer, lane)| Active {
                peer: *peer,
                generation: lane.client.generation().clone(),
                progress: lane.progress.borrow().clone(),
            })
            .collect();
        self.current.queued = self
            .lanes
            .values()
            .filter(|lane| lane.pending.is_some() || lane.replacement.is_some())
            .count();
        let target = self.observe_shared();
        if let Some(Error::Events(error)) = self.current.last_error {
            return Err(Error::Events(error));
        }
        if target != self.target {
            self.application
                .set_sync_target(target)
                .map_err(|_| Error::ApplicationClosed)?;
            self.target = target;
        }
        self.published.send_replace(self.current.clone());
        Ok(())
    }
    fn finish(&mut self, key: Key, completion: Completion) {
        let Some(lane) = self.lanes.get_mut(&key.0) else {
            return;
        };
        if *lane.client.generation() != key.1 {
            return;
        }
        if matches!(completion.result, Ok(Finish::Forward { .. })) {
            self.lifecycle.generation_fetched();
        }
        if completion.progress.peer_exhausted {
            self.lifecycle.done(
                completion.peer,
                completion.generation.clone(),
                lifecycle::DoneReason::Exhausted,
            );
        }
        self.lifecycle.refresh();
        lane.active = None;
        if let Some((client, action)) = lane.replacement.take() {
            lane.client = client;
            lane.pending = Some(action);
            self.current.counts.replaced = self.current.counts.replaced.saturating_add(1);
        }
        if lane.client.is_closed() || lane.client.is_disconnecting() {
            lane.pending = None;
        }
        if lane.pending.is_some() {
            self.ready.push_back(key.0);
        } else {
            self.lanes.remove(&key.0);
        }
        match &completion.result {
            Ok(_) => self.current.counts.finished = self.current.counts.finished.saturating_add(1),
            Err(e) => {
                self.current.counts.failed = self.current.counts.failed.saturating_add(1);
                self.current.last_error = Some(e.clone());
            }
        }
        self.current.last = Some(completion);
    }
    pub async fn run(mut self) -> Status {
        let mut refresh = tokio::time::interval(self.context.config.refresh_interval);
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if let Some(error) = self.context.importer.recovery_reason() {
                self.current.last_error = Some(Error::Import(super::super::Failure {
                    error,
                    progress: None,
                }));
                break;
            }
            self.lifecycle
                .start(*self.policy_config.borrow_and_update());
            if self.lifecycle.ready() {
                self.start_ready();
            }
            if let Err(error) = self.publish() {
                self.current.last_error = Some(error);
                break;
            }
            if *self.stopped.borrow() {
                break;
            }
            tokio::select! {
                _ = self.stopped.changed() => break,
                _ = self.forwarded.closed() => break,
                _ = self.changed.changed() => (),
                _ = refresh.tick() => self.lifecycle.refresh(),
                _ = self.policy_config.changed() => (),
                result = self.lifecycle.next() => if let Err(error) = result { self.current.last_error = Some(error); break; },
                event = self.incoming.recv(), if self.lifecycle.ready() => match event {
                    None => break,
                    Some(event) => {
                        if blocks::Owner::<O>::handles(&event) { self.blocks.route(event); continue; }
                        if let peer::EventKind::Chain { client, intent } = event.kind()
                            && let Err(error) = self.route(client.clone(), intent) {
                                self.current.counts.refused = self.current.counts.refused.saturating_add(1);
                                self.current.last_error = Some(error);
                            }
                        if self.forwarded.try_send(event).is_err() { self.current.last_error = Some(Error::Busy); break; }
                    },
                },
                _ = self.blocks.next() => self.lifecycle.refresh(),
                (key, completion) = self.tasks.next() => self.finish(key, completion),
            }
        }
        self.context.stopped.store(true, Ordering::Release);
        let mut monitors: Vec<_> = self
            .lanes
            .values()
            .filter(|l| l.active.is_some())
            .map(|lane| {
                lane.client.shutdown();
                lane.client.monitor()
            })
            .collect();
        monitors.extend(self.blocks.cancel());
        self.lifecycle.cancel();
        self.current.counts.cancelled = self
            .current
            .counts
            .cancelled
            .saturating_add(self.tasks.len() as u64);
        self.tasks.clear();
        self.lanes.clear();
        self.ready.clear();
        self.context.importer.stop();
        self.context.reads.stop();
        self.context.importer.join().await;
        self.context.reads.join().await;
        for monitor in monitors {
            monitor.wait().await;
        }
        self.current.blocks = self.blocks.status();
        self.observe_shared();
        self.context
            .shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .close_events();
        self.current.lifecycle.pending = false;
        self.current.running = false;
        self.current.joined = true;
        self.current.active.clear();
        self.current.queued = 0;
        let _ = self.application.set_sync_target(0);
        self.published.send_replace(self.current.clone());
        self.current.clone()
    }
}
impl<O: MapOrder + Send + Sync + 'static> Drop for Service<O> {
    fn drop(&mut self) {
        self.lifecycle.cancel();
        self.blocks.cancel();
        self.current.blocks = self.blocks.status();
        self.context.stopped.store(true, Ordering::Release);
        for lane in self.lanes.values().filter(|lane| lane.active.is_some()) {
            lane.client.shutdown();
        }
        self.current.counts.cancelled = self
            .current
            .counts
            .cancelled
            .saturating_add(self.tasks.len() as u64);
        self.tasks.clear();
        self.observe_shared();
        self.context
            .shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .close_events();
        self.current.lifecycle.pending = false;
        self.context.importer.stop();
        self.context.reads.stop();
        let _ = self.application.set_sync_target(0);
        if !self.current.joined {
            self.current.running = false;
            self.current.active.clear();
            self.current.queued = 0;
            self.current.last_error.get_or_insert(Error::Closed);
            self.published.send_replace(self.current.clone());
        }
    }
}
