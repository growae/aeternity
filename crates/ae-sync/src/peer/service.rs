use super::*;
type Key = (PeerPublicKey, net::Generation);
pub struct Service {
    context: Context,
    scheduled: network::Events,
    incoming: gossip::Events,
    stopped: watch::Receiver<bool>,
    published: watch::Sender<Status>,
    progress: watch::Receiver<Counts>,
    current: Status,
    lanes: Vec<Lane>,
    tasks: Tasks<Key, Result<(), Error>>,
    jobs: blocking::Pool,
}
impl Drop for Service {
    fn drop(&mut self) {
        if let Ok(mut registry) = self.context.capabilities.lock() {
            registry.close();
        }
    }
}
impl Service {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        context: Context,
        scheduled: network::Events,
        incoming: gossip::Events,
        stopped: watch::Receiver<bool>,
        published: watch::Sender<Status>,
        current: Status,
        jobs: blocking::Pool,
    ) -> Self {
        let progress = context.progress.subscribe();
        Self {
            context,
            scheduled,
            incoming,
            stopped,
            published,
            progress,
            current,
            lanes: Vec::new(),
            tasks: Tasks::new(),
            jobs,
        }
    }
    fn publish(&mut self) {
        self.current.connections = self.lanes.len();
        let failures = self.current.counts.failed_connections;
        self.current.counts = self.progress.borrow_and_update().clone();
        self.current.counts.failed_connections = failures;
        self.published.send_replace(self.current.clone());
    }
    fn error(&mut self, error: Error) {
        self.current.last_error = Some(error);
    }
    fn route(&mut self, client: net::Client, input: Input) {
        if client.is_closed() {
            return;
        }
        let peer = client.peer();
        self.lanes.retain(|lane| {
            if lane.client.is_closed() {
                self.tasks
                    .remove(&(lane.client.peer(), lane.client.generation().clone()));
                false
            } else {
                true
            }
        });
        let mut index = self.lanes.iter().position(|lane| {
            lane.client.peer() == peer && lane.client.generation() == client.generation()
        });
        if index.is_none() {
            if self.lanes.len() >= self.context.config.connections {
                client.shutdown();
                self.error(Error::Busy);
                return;
            }
            let (send, receive) = mpsc::channel(self.context.config.queued_per_connection);
            self.tasks.insert(
                (peer, client.generation().clone()),
                lane::run(
                    self.context.clone(),
                    self.jobs.clone(),
                    client.clone(),
                    receive,
                ),
            );
            index = Some(self.lanes.len());
            self.lanes.push(Lane {
                client: client.clone(),
                input: send,
                capabilities: self.context.capabilities.clone(),
            });
        }
        let index = index.unwrap();
        if self.lanes[index].input.try_send(input).is_err() {
            self.tasks.remove(&(peer, client.generation().clone()));
            self.lanes.swap_remove(index);
            self.error(Error::Busy);
        }
    }
    pub async fn run(mut self) -> Status {
        loop {
            self.publish();
            if *self.stopped.borrow() {
                break;
            }
            tokio::select! {
                _ = self.stopped.changed() => break,
                _ = self.context.published.events.closed() => break,
                _ = self.progress.changed() => (),
                event = self.scheduled.recv() => match event {
                    Some(network::Event::Ping(ping)) => self.route(ping.client.clone(), Input::Ping(ping)),
                    Some(network::Event::Notice(effect)) => {
                        let weight = effect.retained_bytes().unwrap_or(usize::MAX);
                        if let Err(error) = self.context.published.send(EventKind::Network(effect), weight) { self.error(error); break; }
                    },
                    None => break,
                },
                event = self.incoming.recv() => match event {
                    Some(event) => if let net::Event::Message(message) = event.event { self.route(event.client, Input::Message(message)); },
                    None => break,
                },
                ((peer, generation), result) = self.tasks.next() => {
                    self.lanes.retain(|l| l.client.peer() != peer || *l.client.generation() != generation);
                    if let Err(error) = result {
                        self.current.counts.failed_connections = self.current.counts.failed_connections.saturating_add(1);
                        self.error(error);
                    }
                },
            }
        }
        self.lanes.clear();
        self.tasks.clear();
        self.jobs.stop();
        self.jobs.join().await;
        self.current.running = false;
        self.current.joined = true;
        self.publish();
        self.current.clone()
    }
}
