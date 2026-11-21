use super::super::Direction;
use super::*;
use agreement::{Probe, Search, Step};

struct Session<O> {
    context: Arc<Context<O>>,
    client: net::Client,
    progress: Progress,
    published: watch::Sender<Progress>,
}
impl<O: MapOrder + Send + Sync + 'static> Session<O> {
    fn publish(&self) {
        self.published.send_replace(self.progress.clone());
        self.context.wake.send_replace(());
    }
    fn phase(&mut self, phase: Phase) {
        self.progress.phase = phase;
        self.publish();
    }
    async fn agree(&mut self, target: Point) -> Result<Point, Error> {
        self.phase(Phase::Agreement);
        let local = self.context.selected().await?;
        self.progress.selected = Some(local);
        let genesis = self.context.importer.context.rules.genesis_hash;
        let mut search = Search::new(local.height, target, genesis);
        for _ in 0..self.context.config.probes {
            let hash = match search.next() {
                Step::Agreed(point) => return Ok(point),
                Step::Probe(Probe::Known(point)) => point.hash,
                Step::Probe(Probe::Header(height)) => {
                    self.progress.queries = self.progress.queries.saturating_add(1);
                    self.publish();
                    self.context
                        .header_by_height(&self.client, height, target.hash)
                        .await?
                }
            };
            // Presence of validated fork metadata is the native predicate.
            // A persisted noncanonical branch is also connected to genesis.
            let connected = self
                .context
                .read(move |db| Ok(db.view()?.record(hash)?.is_some()))
                .await?;
            search
                .observe(hash, connected)
                .map_err(|_| Error::NoProgress)?;
        }
        match search.next() {
            Step::Agreed(point) => Ok(point),
            _ => Err(Error::ResourceLimit),
        }
    }
    fn accepted(&mut self, report: &super::super::Report) {
        for accepted in &report.accepted {
            match accepted.outcome {
                Outcome::Present => self.progress.present = self.progress.present.saturating_add(1),
                Outcome::Committed { .. } => {
                    self.progress.committed = self.progress.committed.saturating_add(1)
                }
            }
        }
    }
    async fn generation(&mut self, hash: Hash256, direction: Direction) -> Result<(), Error> {
        self.phase(if direction == Direction::Forward {
            Phase::Forward
        } else {
            Phase::Generation
        });
        let deadline = tokio::time::Instant::now() + self.context.config.capacity_timeout;
        loop {
            self.context.live(&self.client).await?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| Error::Clock)?
                .as_millis()
                .try_into()
                .map_err(|_| Error::Clock)?;
            match self
                .context
                .importer
                .fetch(
                    &self.context.network,
                    self.client.clone(),
                    hash,
                    direction,
                    now,
                )
                .await
            {
                Ok(report) => {
                    self.accepted(&report);
                    self.progress.generations = self.progress.generations.saturating_add(1);
                    self.progress.selected = Some(self.context.selected().await?);
                    self.publish();
                    return Ok(());
                }
                Err(failure)
                    if failure.error == super::super::Error::Busy
                        && failure
                            .progress
                            .as_ref()
                            .is_some_and(|r| r.accepted.is_empty()) =>
                {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(Error::Busy);
                    }
                    self.progress.retries = self.progress.retries.saturating_add(1);
                    self.publish();
                    tokio::time::sleep_until(
                        (tokio::time::Instant::now() + self.context.config.retry_delay)
                            .min(deadline),
                    )
                    .await;
                }
                Err(failure) => {
                    if let Some(report) = &failure.progress {
                        self.accepted(report);
                    }
                    self.publish();
                    return Err(Error::Import(failure));
                }
            }
        }
    }
    async fn start(&mut self, announced: Hash256) -> Result<Finish, Error> {
        self.phase(Phase::Header);
        self.progress.queries = self.progress.queries.saturating_add(1);
        let header = self.context.header_by_hash(&self.client, announced).await?;
        let target = wire::target(&header)?;
        self.progress.target = Some(target);
        self.publish();
        let agreement = self.agree(target).await?;
        self.progress.agreed = Some(agreement);
        // Search heights are remote announcements. Subsequent progress is
        // measured from the stored header, including microblocks and retained
        // side branches. Agreement itself does not require a key header.
        let mut cursor = self.context.anchor(agreement.hash).await?;
        self.progress.cursor = Some(cursor);
        loop {
            self.phase(Phase::Successors);
            self.progress.queries = self.progress.queries.saturating_add(1);
            let successors = self
                .context
                .successors(&self.client, cursor.hash, target.hash)
                .await?;
            if successors.is_empty() {
                self.generation(cursor.hash, Direction::Forward).await?;
                let hash = target.hash;
                let (target_selected, selected) = self
                    .context
                    .read(move |db| {
                        let view = db.view()?;
                        let top = view.status().tip.ok_or(repository::Error::Uninitialized)?;
                        let selected = Point {
                            height: view.require_block(top)?.header().common().height,
                            hash: top,
                        };
                        Ok((view.is_canonical(hash)?, selected))
                    })
                    .await?;
                self.progress.selected = Some(selected);
                return Ok(Finish::Exhausted { target_selected });
            }
            for announced in successors {
                let point = if let Some(point) = self.context.stored_key(announced.hash).await? {
                    self.progress.present = self.progress.present.saturating_add(1);
                    point
                } else {
                    self.generation(announced.hash, Direction::Backward).await?;
                    self.context
                        .stored_key(announced.hash)
                        .await?
                        .ok_or(repository::Error::Missing(announced.hash))?
                };
                // An operational liveness bound, not a new consensus rule:
                // known/repeated hashes must not keep a peer task cycling.
                if point.height <= cursor.height {
                    return Err(Error::NoProgress);
                }
                cursor = point;
                self.progress.cursor = Some(cursor);
                self.publish();
            }
        }
    }
    async fn operation(&mut self, action: Action) -> Result<Finish, Error> {
        match action {
            Action::Start(hash) => self.start(hash).await,
            Action::Forward(hash) => {
                self.generation(hash, Direction::Forward).await?;
                Ok(Finish::Forward { requested: hash })
            }
        }
    }
}
pub(super) async fn run<O: MapOrder + Send + Sync + 'static>(
    context: Arc<Context<O>>,
    client: net::Client,
    action: Action,
    published: watch::Sender<Progress>,
) -> Completion {
    let mut session = Session {
        context,
        client,
        progress: Progress::default(),
        published,
    };
    let result = session.operation(action).await;
    // A cancelled/expired wire request closes its generation. Observe the
    // driver teardown before reporting that this task has finished.
    if session.client.is_closed() {
        session.client.monitor().wait().await;
    }
    session.phase(Phase::Finished);
    Completion {
        peer: session.client.peer(),
        generation: session.client.generation().clone(),
        progress: session.progress,
        result,
    }
}
