use super::*;
#[cfg(test)]
mod tests;
type Key = (PeerPublicKey, net::Generation);
struct Lane {
    client: net::Client,
    pending: Option<Action>,
    active: Option<Action>,
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
    ) -> Self {
        Self {
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
        let hash = |raw: &[u8]| raw.try_into().map(Hash256).map_err(|_| Error::Hash);
        let action = match intent {
            peer::ChainIntent::Done => {
                self.current.counts.done_announcements =
                    self.current.counts.done_announcements.saturating_add(1);
                return Ok(());
            }
            peer::ChainIntent::Generation(bytes) => Action::Forward(hash(bytes)?),
            peer::ChainIntent::Start { best, .. } => Action::Start(hash(best)?),
        };
        if client.is_closed() {
            return Err(Error::Stale);
        }
        let peer = client.peer();
        if let Some(lane) = self.lanes.get(&peer)
            && lane.client.generation() != client.generation()
        {
            if !lane.client.is_closed() {
                return Err(Error::Stale);
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
    fn publish(&mut self) -> Result<(), Error> {
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
            .filter(|lane| lane.pending.is_some())
            .count();
        let target = self
            .current
            .active
            .iter()
            .filter_map(|a| a.progress.target.map(|p| p.height))
            .max()
            .unwrap_or(0);
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
        lane.active = None;
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
        loop {
            if let Some(error) = self.context.importer.recovery_reason() {
                self.current.last_error = Some(Error::Import(super::super::Failure {
                    error,
                    progress: None,
                }));
                break;
            }
            self.start_ready();
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
                event = self.incoming.recv() => match event {
                    None => break,
                    Some(event) => {
                        if let peer::EventKind::Chain { client, intent } = event.kind()
                            && let Err(error) = self.route(client.clone(), intent) {
                                self.current.counts.refused = self.current.counts.refused.saturating_add(1);
                                self.current.last_error = Some(error);
                            }
                        if self.forwarded.try_send(event).is_err() { self.current.last_error = Some(Error::Busy); break; }
                    },
                },
                (key, completion) = self.tasks.next() => self.finish(key, completion),
            }
        }
        self.context.stopped.store(true, Ordering::Release);
        let monitors: Vec<_> = self
            .lanes
            .values()
            .filter(|l| l.active.is_some())
            .map(|lane| {
                lane.client.shutdown();
                lane.client.monitor()
            })
            .collect();
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
