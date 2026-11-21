use super::*;
impl<O: MapOrder + Send + Sync + 'static> Book<O> {
    pub fn next(
        &mut self,
        id: u64,
        client: &net::Client,
        context: &Context<O>,
    ) -> Result<Assignment<O>, Error> {
        self.snapshot(id, client)?;
        let sequence = self.number()?;
        let g = self.group_mut(id)?;
        if g.finished {
            return Ok(Assignment::Finished);
        }
        let member = g.members.get(&client.peer()).ok_or(Error::Stale)?;
        if member.lease.is_some() {
            return Err(Error::Stale);
        }
        let exhausted = member.exhausted.is_some();
        let busy: Vec<_> = g
            .members
            .values()
            .filter_map(|m| m.lease.map(|(_, k)| k))
            .collect();
        let excluded: Vec<_> = busy
            .iter()
            .filter_map(|k| match k {
                Kind::Get(p) => Some(p.hash),
                _ => None,
            })
            .collect();
        let allow_fill = !exhausted && !busy.iter().any(|k| matches!(k, Kind::Fill { .. }));
        let allow_agree = !exhausted && !busy.iter().any(|k| matches!(k, Kind::Agree));
        let mut model = g.model.clone();
        let work = model.next_available(&excluded, allow_fill, allow_agree)?;
        let (kind, operation) = match work {
            Work::Agree => (Kind::Agree, Operation::Agree(model.state().chain.target())),
            Work::Fill { from, target } => (Kind::Fill { from }, Operation::Fill { from, target }),
            Work::Generation(mut point) => {
                let weight = context.importer.download_weight();
                let (slots, bytes) = context.importer.available();
                let first = g.model.state().pool.first().ok_or(Error::NoProgress)?.point;
                // Speculation always leaves one real count/byte reservation
                // available. Head requests can use that lane across chains.
                if point.hash != first.hash && (slots <= 1 || bytes / weight <= 1) {
                    if excluded.contains(&first.hash) || g.model.state().pool[0].got.is_some() {
                        return Ok(Assignment::Wait { capacity: true });
                    }
                    model = g.model.clone();
                    let skip: Vec<_> = g
                        .model
                        .state()
                        .pool
                        .iter()
                        .filter(|i| i.point.hash != first.hash)
                        .map(|i| i.point.hash)
                        .chain(excluded.iter().copied())
                        .collect();
                    let Work::Generation(p) = model.next_available(&skip, false, false)? else {
                        return Ok(Assignment::Wait { capacity: true });
                    };
                    point = p;
                }
                let reserved = match context.importer.reserve_download(&context.network, client) {
                    Ok(reserved) => reserved,
                    Err(chain::Error::Busy) => return Ok(Assignment::Wait { capacity: true }),
                    Err(error) => {
                        return Err(Error::Import(chain::Failure {
                            error,
                            progress: Some(Default::default()),
                        }));
                    }
                };
                (Kind::Get(point), Operation::Get { point, reserved })
            }
            Work::Post(items) => {
                if items
                    .iter()
                    .any(|i| i.got.is_none_or(|r| !g.payloads.contains_key(&r.ticket)))
                {
                    return Err(Error::Stale);
                }
                let payloads = items
                    .into_iter()
                    .map(|i| {
                        let data = g
                            .payloads
                            .remove(&i.got.expect("ready item").ticket)
                            .expect("checked payload");
                        (i, data)
                    })
                    .collect();
                (Kind::Post { posted: 0 }, Operation::Post(payloads))
            }
            Work::Break | Work::Abort => {
                let state = model.state();
                let drained = state.agreed.is_some()
                    && state.pool.is_empty()
                    && state.pending.is_empty()
                    && state.adding.is_empty()
                    && !matches!(state.next_pool, NextPool::Ready(_));
                if drained
                    && busy.is_empty()
                    && g.members
                        .values()
                        .all(|m| m.exhausted.is_some() || m.failed.is_some())
                {
                    let hash = g.cursor.ok_or(Error::NoProgress)?.hash;
                    if member.exhausted == Some(hash) {
                        (Kind::Forward, Operation::Forward(hash))
                    } else if !g.members.values().any(|m| {
                        m.exhausted == Some(hash) && m.failed.is_none() && !m.client.is_closed()
                    }) {
                        return Err(Error::Stale);
                    } else {
                        return Ok(Assignment::Wait { capacity: false });
                    }
                } else {
                    g.model = model;
                    return Ok(Assignment::Wait { capacity: false });
                }
            }
        };
        g.model = model;
        g.members.get_mut(&client.peer()).expect("member").lease = Some((sequence, kind));
        Ok(Assignment::Run(
            Lease {
                task: id,
                sequence,
                peer: client.peer(),
                generation: client.generation().clone(),
            },
            operation,
        ))
    }
}
