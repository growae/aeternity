use super::*;
use coordination::Chain;
use shared::{Assignment, Join, Membership, Operation, Payload};

impl<O: MapOrder + Send + Sync + 'static> Session<O> {
    fn statistics_clock() -> i128 {
        match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => duration.as_micros() as i128,
            Err(error) => -(error.duration().as_micros() as i128),
        }
    }
    fn clock() -> Result<u64, Error> {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Clock)?
            .as_millis()
            .try_into()
            .map_err(|_| Error::Clock)
    }
    fn notify(&self) {
        self.context.wake.send_replace(());
    }
    async fn attach(&mut self, header: ae_codec::headers::Header) -> Result<Membership<O>, Error> {
        self.phase(Phase::Matching);
        let limits = self.context.config.coordination_limits();
        let id = self
            .context
            .shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .number()?;
        let mut candidate = Chain::from_header(id, self.client.peer(), &header, limits)?;
        self.progress.target = Some(candidate.target());
        self.publish();
        for _ in 0..self.context.config.probes {
            self.context.live(&self.client).await?;
            let join = self
                .context
                .shared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .join(
                    &candidate,
                    &self.client,
                    limits,
                    Self::statistics_clock().div_euclid(1000),
                )?;
            match join {
                Join::Joined(task) => {
                    let member = Membership::new(self.context.clone(), self.client.clone(), task);
                    self.progress.task = Some(task);
                    self.notify();
                    return Ok(member);
                }
                Join::Lookup {
                    chain,
                    height,
                    top,
                    clients,
                } => {
                    let mut found = None;
                    let mut last = Error::Stale;
                    for client in clients {
                        self.progress.queries = self.progress.queries.saturating_add(1);
                        self.publish();
                        match self.context.header_at(&client, height, top).await {
                            Ok(header) => {
                                found = Some((client, header));
                                break;
                            }
                            Err(error) => last = error,
                        }
                    }
                    let (client, header) = found.ok_or(last)?;
                    let hint = Chain::from_header(chain, client.peer(), &header, limits)?;
                    if chain == candidate.id() {
                        let merged = hint.merge(&candidate, limits)?;
                        if merged == candidate {
                            return Err(Error::NoProgress);
                        }
                        candidate = merged;
                    } else {
                        match self
                            .context
                            .shared
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .add_hint(&hint, &client, limits)
                        {
                            Ok(()) => (),
                            Err(Error::Stale | Error::NoProgress) => continue,
                            Err(e) => return Err(e),
                        }
                    }
                    self.notify();
                }
            }
        }
        Err(Error::ResourceLimit)
    }
    pub(super) async fn coordinate(
        &mut self,
        header: ae_codec::headers::Header,
    ) -> Result<Finish, Error> {
        let target = wire::target(&header)?;
        let member = self.attach(header).await?;
        let mut changed = self.context.wake.subscribe();
        let mut capacity_since = None;
        loop {
            self.context.live(&self.client).await?;
            let (snapshot, assignment) = {
                let mut book = self
                    .context
                    .shared
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let snapshot = book.snapshot(member.task, &self.client)?;
                let assignment = book.next(member.task, &self.client, &self.context)?;
                (snapshot, assignment)
            };
            self.progress.target = Some(snapshot.target);
            self.progress.agreed = snapshot.agreed;
            self.progress.cursor = snapshot.cursor;
            self.progress.selected = snapshot.selected.or(self.progress.selected);
            self.progress.members = snapshot.members;
            self.progress.buffered = snapshot.buffered;
            match assignment {
                Assignment::Finished => {
                    let hash = target.hash;
                    let (selected, canonical) = self
                        .context
                        .read(move |db| {
                            let view = db.view()?;
                            let top = view.status().tip.ok_or(repository::Error::Uninitialized)?;
                            Ok((
                                Point {
                                    height: view.require_block(top)?.header().common().height,
                                    hash: top,
                                },
                                view.is_canonical(hash)?,
                            ))
                        })
                        .await?;
                    self.progress.selected = Some(selected);
                    self.publish();
                    self.progress.peer_exhausted = true;
                    self.publish();
                    return Ok(Finish::Exhausted {
                        target_selected: canonical,
                    });
                }
                Assignment::Wait { capacity } => {
                    self.phase(Phase::Waiting);
                    if capacity {
                        let start = *capacity_since.get_or_insert_with(tokio::time::Instant::now);
                        if start.elapsed() >= self.context.config.capacity_timeout {
                            return Err(Error::Busy);
                        }
                        self.progress.retries = self.progress.retries.saturating_add(1);
                        self.publish();
                    } else {
                        capacity_since = None;
                    }
                    changed.borrow_and_update();
                    if capacity {
                        tokio::time::sleep(self.context.config.retry_delay).await;
                    } else {
                        tokio::select! {_=changed.changed()=>(),_=tokio::time::sleep(self.context.config.retry_delay)=>()}
                    }
                }
                Assignment::Run(lease, operation) => {
                    capacity_since = None;
                    self.notify();
                    match operation {
                        Operation::Agree(target) => {
                            let agreed = self.agree(target).await?;
                            let anchor = self.context.anchor(agreed.hash).await?;
                            let selected = self.context.selected().await?;
                            self.context
                                .shared
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .agreed(&lease, agreed, anchor, selected)?;
                        }
                        Operation::Fill { from, target } => {
                            self.phase(Phase::Successors);
                            self.progress.queries = self.progress.queries.saturating_add(1);
                            self.publish();
                            let points =
                                self.context.successors(&self.client, from, target).await?;
                            self.context
                                .shared
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .successors(
                                    &lease,
                                    points,
                                    self.context.config.coordination_limits(),
                                )?;
                        }
                        Operation::Get { point, reserved } => {
                            self.phase(Phase::Generation);
                            let payload =
                                if let Some(stored) = self.context.stored_key(point.hash).await? {
                                    drop(reserved);
                                    Payload::Local(stored)
                                } else {
                                    let downloaded = self
                                        .context
                                        .importer
                                        .download_reserved(
                                            &self.context.network,
                                            self.client.clone(),
                                            point.hash,
                                            Direction::Backward,
                                            reserved,
                                        )
                                        .await
                                        .map_err(Error::Import)?;
                                    Payload::Download(
                                        self.context.inspect(downloaded, Self::clock()?).await?,
                                    )
                                };
                            self.context
                                .shared
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .received(&lease, point, payload)?;
                        }
                        Operation::Post(items) => {
                            self.phase(Phase::Generation);
                            let mut samples = Vec::with_capacity(items.len());
                            let mut items = items.into_iter();
                            let mut failed = false;
                            while let Some((item, payload)) = items.next() {
                                let result = self.post_payload(item.point, payload).await;
                                match result {
                                    Ok((point, selected, sample)) => {
                                        self.context
                                            .shared
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner())
                                            .posted(&lease, point, selected)?;
                                        if let Some(sample) = sample {
                                            samples.push(sample);
                                        }
                                        self.notify();
                                    }
                                    Err(error) => {
                                        let suspects = self
                                            .context
                                            .shared
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner())
                                            .post_failed(
                                                &lease,
                                                item,
                                                error.clone(),
                                                items.collect(),
                                            )?;
                                        self.context.reject(suspects).await?;
                                        // Closing a rejected/fatal task's sockets must
                                        // not replace the original typed import failure
                                        // with a later generic stale-connection result.
                                        self.context
                                            .shared
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner())
                                            .snapshot(member.task, &self.client)?;
                                        failed = true;
                                        break;
                                    }
                                }
                            }
                            if !failed {
                                self.context
                                    .shared
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .post_complete(
                                        &lease,
                                        &samples,
                                        Self::statistics_clock().div_euclid(1000),
                                    )?;
                            }
                        }
                        Operation::Forward(hash) => {
                            // Native completion follows an empty successor reply even
                            // if its final forward fetch returns an ordinary error.
                            // Preserve that signal separately from admission success.
                            let result = self.generation(hash, Direction::Forward).await;
                            self.progress.peer_exhausted = true;
                            self.publish();
                            result?;
                            let selected = self.context.selected().await?;
                            self.context
                                .shared
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .forwarded(&lease, selected)?;
                        }
                    }
                    self.notify();
                }
            }
        }
    }
    async fn post_payload(
        &mut self,
        expected: Point,
        payload: Payload<O>,
    ) -> Result<(Point, Point, Option<statistics::Sample>), Error> {
        let mut sample = None;
        let point = match payload {
            Payload::Local(point) => {
                let actual = self
                    .context
                    .stored_key(point.hash)
                    .await?
                    .ok_or(repository::Error::Missing(point.hash))?;
                self.progress.present = self.progress.present.saturating_add(1);
                actual
            }
            Payload::Download(download) => {
                let start_us = Self::statistics_clock();
                match self
                    .context
                    .importer
                    .import_download(&self.context.network, download, Self::clock()?)
                    .await
                {
                    Ok(report) => {
                        let end_us = Self::statistics_clock();
                        sample = Some(statistics::Sample {
                            height: expected.height,
                            start_us,
                            end_us,
                            micro_blocks: report
                                .accepted
                                .iter()
                                .filter(|b| b.micro_transactions.is_some())
                                .count() as u64,
                            transactions: report
                                .accepted
                                .iter()
                                .filter_map(|b| b.micro_transactions)
                                .sum(),
                            work_us: None,
                        });
                        self.accepted(&report);
                        self.progress.generations = self.progress.generations.saturating_add(1);
                    }
                    Err(failure) => {
                        if let Some(report) = &failure.progress {
                            self.accepted(report);
                        }
                        self.publish();
                        return Err(Error::Import(failure));
                    }
                }
                self.context
                    .stored_key(expected.hash)
                    .await?
                    .ok_or(repository::Error::Missing(expected.hash))?
            }
        };
        if point.hash != expected.hash {
            return Err(Error::Hash);
        }
        let selected = self.context.selected().await?;
        self.progress.selected = Some(selected);
        self.publish();
        Ok((point, selected, sample))
    }
}
