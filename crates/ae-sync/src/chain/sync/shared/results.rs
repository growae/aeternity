use super::*;
impl<O> Book<O> {
    fn release(g: &mut Group<O>, lease: &Lease) {
        g.members
            .get_mut(&lease.peer)
            .expect("checked member")
            .lease = None;
    }
    pub fn agreed(
        &mut self,
        lease: &Lease,
        agreed: Point,
        anchor: Point,
        selected: Point,
    ) -> Result<(), Error> {
        let g = self.check(lease)?;
        if !matches!(g.members[&lease.peer].lease, Some((_, Kind::Agree))) {
            return Err(Error::Stale);
        }
        g.model.apply(ResultInput::Agreed(agreed))?;
        g.agreed.get_or_insert(agreed);
        g.cursor.get_or_insert(anchor);
        g.selected = Some(selected);
        Self::release(g, lease);
        Ok(())
    }
    pub fn successors(
        &mut self,
        lease: &Lease,
        points: Vec<Point>,
        limits: coordination::Limits,
    ) -> Result<(), Error> {
        let g = self.check(lease)?;
        let Some((_, Kind::Fill { from })) = g.members[&lease.peer].lease else {
            return Err(Error::Stale);
        };
        if points.is_empty() {
            g.members.get_mut(&lease.peer).expect("member").exhausted = Some(from);
            let mut state = g.model.state().clone();
            state.next_pool = NextPool::None;
            g.model = Scheduler::restore(state, limits)?;
        } else {
            g.model.apply(ResultInput::Hashes {
                first_height: points[0].height,
                items: points.into_iter().map(Item::missing).collect(),
            })?;
        }
        Self::release(g, lease);
        Ok(())
    }
    pub fn received(
        &mut self,
        lease: &Lease,
        point: Point,
        payload: Payload<O>,
    ) -> Result<(), Error> {
        let g = self.check(lease)?;
        if !matches!(g.members[&lease.peer].lease,Some((_,Kind::Get(expected)))if expected==point) {
            return Err(Error::Stale);
        }
        if !g
            .model
            .state()
            .pool
            .iter()
            .any(|i| i.point == point && i.got.is_none())
        {
            return Err(Error::Stale);
        }
        g.model.apply(ResultInput::Generation {
            point,
            ready: Ready {
                peer: lease.peer,
                ticket: lease.sequence,
            },
        })?;
        g.payloads.insert(lease.sequence, payload);
        Self::release(g, lease);
        Ok(())
    }
    pub fn posted(&mut self, lease: &Lease, point: Point, selected: Point) -> Result<(), Error> {
        let g = self.check(lease)?;
        let Some((_, Kind::Post { posted })) = g.members[&lease.peer].lease else {
            return Err(Error::Stale);
        };
        let item = g.model.state().adding.get(posted).ok_or(Error::Stale)?;
        if item.point.hash != point.hash {
            return Err(Error::Stale);
        }
        if g.cursor.is_some_and(|p| point.height <= p.height) {
            return Err(Error::NoProgress);
        }
        g.cursor = Some(point);
        g.selected = Some(selected);
        g.members.get_mut(&lease.peer).expect("member").lease =
            Some((lease.sequence, Kind::Post { posted: posted + 1 }));
        Ok(())
    }
    pub fn post_complete(&mut self, lease: &Lease) -> Result<(), Error> {
        let g = self.check(lease)?;
        let Some((_, Kind::Post { posted })) = g.members[&lease.peer].lease else {
            return Err(Error::Stale);
        };
        if posted != g.model.state().adding.len() {
            return Err(Error::Stale);
        }
        g.model.apply(ResultInput::Posted)?;
        Self::release(g, lease);
        Ok(())
    }
    pub fn post_failed(
        &mut self,
        lease: &Lease,
        item: Item,
        error: Error,
        remaining: Vec<(Item, Payload<O>)>,
    ) -> Result<Vec<net::Client>, Error> {
        let g = self.check(lease)?;
        let Some((_, Kind::Post { posted })) = g.members[&lease.peer].lease else {
            return Err(Error::Stale);
        };
        if g.model.state().adding.get(posted) != Some(&item) {
            return Err(Error::Stale);
        }
        let supplier = item.got.ok_or(Error::Stale)?.peer;
        let rejected = matches!(&error,Error::Import(f)if matches!(f.error,chain::Error::Policy{..}|chain::Error::BlockedByWhitelist));
        let retry = matches!(&error,Error::Import(f)if f.progress.is_some() && matches!(f.error,chain::Error::Validation{operational:false,..}|chain::Error::Generation(_)|chain::Error::Stale));
        if rejected || retry {
            let effects = g.model.apply(ResultInput::PostError {
                peer: supplier,
                height: item.point.height,
                rejected,
            })?;
            for (item, data) in remaining {
                g.payloads
                    .insert(item.got.ok_or(Error::Stale)?.ticket, data);
            }
            if let Some(m) = g.members.get_mut(&supplier) {
                m.failed = Some(error.clone());
            }
            let suspects = effects
                .into_iter()
                .filter_map(|effect| match effect {
                    coordination::Effect::Suspect(peer) => {
                        g.members.get(&peer).map(|m| m.client.clone())
                    }
                    _ => None,
                })
                .collect();
            if rejected {
                g.failed = Some(error);
            }
            Self::release(g, lease);
            Ok(suspects)
        } else {
            g.failed = Some(error);
            for m in g.members.values() {
                if m.lease.is_some() {
                    m.client.shutdown();
                }
            }
            Self::release(g, lease);
            Ok(vec![])
        }
    }
    pub fn forwarded(&mut self, lease: &Lease, selected: Point) -> Result<(), Error> {
        let g = self.check(lease)?;
        if !matches!(g.members[&lease.peer].lease, Some((_, Kind::Forward))) {
            return Err(Error::Stale);
        }
        g.selected = Some(selected);
        g.finished = true;
        Self::release(g, lease);
        Ok(())
    }
}
