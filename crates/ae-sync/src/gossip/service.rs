use super::*;
pub struct Service {
    context: Context,
    binding: crate::bindings::Admission,
    lease: Arc<()>,
    feed_generation: u64,
    feed_client: worker::Client,
    peers: pool::Reader,
    incoming: pool::Events,
    commands: mpsc::Receiver<Command>,
    forwarded: mpsc::Sender<pool::Forwarded>,
    vote_events: mpsc::Receiver<Arc<Signed>>,
    vote_consumer: Option<crate::votes::Consumer>,
    policies: watch::Receiver<Policy>,
    stopped: watch::Receiver<bool>,
    status: watch::Sender<Status>,
    current: Status,
    cursor: worker::events::Cursor,
    pending: VecDeque<Arc<Signed>>,
    pending_vote: Option<crate::votes::Publication>,
    tasks: Tasks<Key, Done>,
    active: usize,
    next_id: u64,
    feeding: bool,
    sending: bool,
    random: RandomState,
    round: u64,
    jobs: blocking::Pool,
    feed_jobs: blocking::Pool,
}
impl Service {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        context: Context,
        peers: pool::Reader,
        incoming: pool::Events,
        commands: mpsc::Receiver<Command>,
        forwarded: mpsc::Sender<pool::Forwarded>,
        vote_events: mpsc::Receiver<Arc<Signed>>,
        vote_consumer: Option<crate::votes::Consumer>,
        policies: watch::Receiver<Policy>,
        stopped: watch::Receiver<bool>,
        status: watch::Sender<Status>,
        current: Status,
        cursor: worker::events::Cursor,
        jobs: blocking::Pool,
        feed_jobs: blocking::Pool,
        binding: crate::bindings::Admission,
        lease: Arc<()>,
        feed_generation: u64,
    ) -> Self {
        Self {
            feed_client: context.admission.clone(),
            context,
            binding,
            lease,
            feed_generation,
            peers,
            incoming,
            commands,
            forwarded,
            vote_events,
            vote_consumer,
            policies,
            stopped,
            status,
            current,
            cursor,
            pending: VecDeque::new(),
            pending_vote: None,
            tasks: Tasks::new(),
            active: 0,
            next_id: 0,
            feeding: false,
            sending: false,
            random: RandomState::new(),
            round: 0,
            jobs,
            feed_jobs,
        }
    }
    pub async fn run(mut self) -> Status {
        self.run_inner().await;
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            drop(command);
        }
        self.tasks.clear();
        self.pending.clear();
        self.pending_vote.take();
        self.vote_events.close();
        self.jobs.stop();
        self.feed_jobs.stop();
        self.jobs.join().await;
        self.feed_jobs.join().await;
        self.active = 0;
        self.current.running = false;
        self.refresh();
        self.current.clone()
    }
    async fn run_inner(&mut self) {
        loop {
            if *self.stopped.borrow() {
                break;
            }
            if self.binding.is_closed() {
                self.current.last_error = Some(Error::Closed);
                break;
            }
            self.rebind();
            if self.policies.has_changed().unwrap_or(false) {
                self.policy();
            }
            self.schedule();
            self.refresh();
            let available = self.active < self.context.config.operations;
            tokio::select! {
                _ = self.stopped.changed() => {
                    if *self.stopped.borrow() || self.stopped.has_changed().is_err() {break;}
                },
                _ = self.forwarded.closed() => break,
                _ = self.binding.changed() => (),
                _ = self.policies.changed() => {
                    self.policy();
                },
                command = self.commands.recv(), if available => match command {
                    Some(command)=>self.command(command),None=>break,
                },
                event = self.incoming.recv(), if available => match event {
                    Some(event)=>self.incoming(event),None=>break,
                },
                vote = self.vote_events.recv(), if !self.sending && self.pending.is_empty() => {
                    if let Some(tx)=vote {self.pending.push_back(tx);}
                },
                vote = async { match &mut self.vote_consumer {Some(c)=>c.recv().await,None=>std::future::pending().await} }, if !self.sending && self.pending.is_empty() => {
                    match vote {
                        Some(publication)=>{self.pending.push_back(publication.transaction.clone());self.pending_vote=Some(publication);}
                        None=>{self.current.last_error=Some(Error::Closed);break;}
                    }
                },
                (key,result) = self.tasks.next() => self.finished(key,result),
            }
        }
    }
    fn rebind(&mut self) {
        let snapshot = self.binding.snapshot();
        if Arc::ptr_eq(&self.lease, &snapshot.lease) {
            return;
        }
        let Some(bound) = snapshot.value else {
            return;
        };
        // Refresh feed/local-vote context. Incoming ordinary transactions
        // resolve an available binding before their single receive attempt;
        // accepted tickets are never redirected or resubmitted.
        self.context.admission = bound.client.clone();
        if self.feeding || self.sending || !self.pending.is_empty() {
            return;
        }
        let recover = match self.current.feed {
            Feed::Closed => snapshot.generation != self.feed_generation.saturating_add(1),
            Feed::Paused(_) | Feed::Recovering => true,
            Feed::Live => return, // Drain the retired worker's acknowledged tail.
        };
        if recover {
            self.current.counts.gaps = self.current.counts.gaps.saturating_add(1);
        }
        self.feed_client = bound.client;
        self.cursor = bound.after;
        self.current.feed = if recover {
            Feed::Recovering
        } else {
            Feed::Live
        };
        self.lease = snapshot.lease;
        self.feed_generation = snapshot.generation;
    }
    fn policy(&mut self) {
        self.current.policy = *self.policies.borrow_and_update();
        if matches!(self.current.feed, Feed::Paused(_)) {
            self.current.feed = Feed::Recovering;
        }
    }
    fn schedule(&mut self) {
        if !self.sending && !self.pending.is_empty() {
            if self.current.policy.enabled {
                let tx = self.pending.pop_front().unwrap();
                let publication = self.pending_vote.take();
                let ctx = self.context.clone();
                let jobs = self.jobs.clone();
                let peers = self.peers.clone();
                let policy = self.current.policy;
                let random = self.random.clone();
                let round = self.round;
                self.round = self.round.wrapping_add(1);
                self.sending = true;
                let timeout = ctx.config.fanout_timeout;
                self.tasks.insert(Key::Fanout, async move {
                    let result = operations::bounded(
                        timeout,
                        operations::fanout(ctx, jobs, peers, tx, policy, random, round),
                    )
                    .await;
                    drop(publication);
                    Done::Fanout(result)
                });
            } else {
                self.current.counts.suppressed = self
                    .current
                    .counts
                    .suppressed
                    .saturating_add(self.pending.len() as u64);
                self.pending.clear();
                self.pending_vote.take();
            }
        }
        if !self.feeding && !self.sending && self.pending.is_empty() {
            let admission = self.feed_client.clone();
            let jobs = self.feed_jobs.clone();
            match self.current.feed {
                Feed::Live => {
                    self.feeding = true;
                    let cursor = self.cursor.clone();
                    let limit = self.context.config.feed_batch;
                    self.tasks.insert(Key::Feed, async move {
                        Done::Feed(operations::feed(admission, jobs, cursor, limit).await)
                    });
                }
                Feed::Recovering => {
                    self.feeding = true;
                    let maximum = self.current.policy.snapshot_limit;
                    let timeout = self.context.config.work_timeout;
                    self.tasks.insert(Key::Feed, async move {
                        Done::Feed(
                            operations::bounded(
                                timeout,
                                operations::recover(admission, jobs, maximum),
                            )
                            .await,
                        )
                    });
                }
                _ => (),
            }
        }
    }
    fn operation(
        &mut self,
        future: impl std::future::Future<Output = Result<Input, Error>> + Send + 'static,
    ) {
        let Some(next) = self.next_id.checked_add(1) else {
            self.current.last_error = Some(Error::IdExhausted);
            return;
        };
        let id = self.next_id;
        self.next_id = next;
        self.active += 1;
        self.tasks.insert(
            Key::Operation(id),
            async move { Done::Operation(future.await) },
        );
    }
    fn command(&mut self, command: Command) {
        let ctx = self.context.clone();
        let jobs = self.jobs.clone();
        self.operation(operations::command(ctx, jobs, command));
    }
    fn incoming(&mut self, event: pool::Forwarded) {
        if matches!(&event.event,net::Event::Message(m)if m.message().kind()==Kind::Transactions) {
            let ctx = self.context.clone();
            let jobs = self.jobs.clone();
            let binding = self.binding.clone();
            let timeout = ctx.config.work_timeout;
            self.operation(operations::bounded(
                timeout,
                operations::incoming(ctx, jobs, binding, event),
            ));
        } else {
            let client = event.client.clone();
            if self.forwarded.try_send(event).is_err() {
                client.shutdown();
            }
        }
    }
    fn finished(&mut self, key: Key, result: Done) {
        match (key, result) {
            (Key::Feed, Done::Feed(result)) => {
                self.feeding = false;
                match result {
                    Ok(FeedResult::Batch(batch)) => {
                        self.cursor = batch.cursor;
                        if batch.closed && batch.events.is_empty() {
                            self.current.feed = Feed::Closed;
                        }
                        self.pending
                            .extend(batch.events.into_iter().map(|e| e.transaction));
                    }
                    Ok(FeedResult::Snapshot(snapshot)) => {
                        if !snapshot.complete {
                            let error = Error::SnapshotIncomplete {
                                visible: snapshot.transactions.len(),
                                total: snapshot.pool.active.saturating_add(snapshot.pool.visited),
                            };
                            self.current.last_error = Some(error.clone());
                            self.current.feed = Feed::Paused(error);
                        } else {
                            self.cursor = snapshot.cursor;
                            self.pending.extend(snapshot.transactions);
                            self.current.feed = Feed::Live;
                            self.current.counts.recovered =
                                self.current.counts.recovered.saturating_add(1);
                        }
                    }
                    Err(Error::Feed(error))
                        if matches!(
                            error,
                            worker::events::ReadError::Lagged { .. }
                                | worker::events::ReadError::WrongStream
                                | worker::events::ReadError::Ahead
                        ) =>
                    {
                        self.current.counts.gaps = self.current.counts.gaps.saturating_add(1);
                        self.current.last_error = Some(Error::Feed(error));
                        self.current.feed = Feed::Recovering;
                    }
                    Err(error) => {
                        self.current.last_error = Some(error.clone());
                        self.current.feed = Feed::Paused(error);
                    }
                }
            }
            (Key::Fanout, Done::Fanout(result)) => {
                self.sending = false;
                self.current.counts.fanout_events =
                    self.current.counts.fanout_events.saturating_add(1);
                match result {
                    Ok(r) => {
                        self.current.counts.sent = self.current.counts.sent.saturating_add(r.sent);
                        self.current.counts.send_failed =
                            self.current.counts.send_failed.saturating_add(r.failed);
                    }
                    Err(error) => {
                        self.current.counts.send_failed =
                            self.current.counts.send_failed.saturating_add(1);
                        self.current.last_error = Some(error);
                    }
                }
            }
            (Key::Operation(_), Done::Operation(result)) => {
                self.active -= 1;
                match result {
                    Ok(c) => {
                        self.current.counts.decoded =
                            self.current.counts.decoded.saturating_add(c.decoded);
                        self.current.counts.malformed =
                            self.current.counts.malformed.saturating_add(c.malformed);
                        self.current.counts.ordinary_admitted = self
                            .current
                            .counts
                            .ordinary_admitted
                            .saturating_add(c.admitted);
                        self.current.counts.rejected =
                            self.current.counts.rejected.saturating_add(c.rejected);
                        self.current.counts.votes_admitted =
                            self.current.counts.votes_admitted.saturating_add(c.votes);
                    }
                    Err(error) => self.current.last_error = Some(error),
                }
            }
            _ => unreachable!("typed gossip task result"),
        }
    }
    fn refresh(&mut self) {
        self.current.operations = self.active;
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
        self.tasks.clear();
        self.jobs.stop();
        self.feed_jobs.stop();
    }
}
