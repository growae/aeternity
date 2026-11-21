use super::super as chain;
use super::super::Download;
use super::*;
use coordination::{Chain, Item, NextPool, Ready, ResultInput, Scheduler, Work};

mod dispatch;
mod results;

#[derive(Clone)]
pub(super) struct Lease {
    task: u64,
    sequence: u64,
    peer: PeerPublicKey,
    generation: net::Generation,
}
#[derive(Clone, Copy)]
enum Kind {
    Agree,
    Fill { from: Hash256 },
    Get(Point),
    Post { posted: usize },
    Forward,
}
struct Member {
    client: net::Client,
    lease: Option<(u64, Kind)>,
    exhausted: Option<Hash256>,
    sync_height: Option<u64>,
    failed: Option<Error>,
}
pub(super) enum Payload<O> {
    Local(Point),
    Download(Download<O>),
}
pub(super) enum Operation<O> {
    Agree(Point),
    Fill {
        from: Hash256,
        target: Hash256,
    },
    Get {
        point: Point,
        reserved: blocking::Reservation,
    },
    Post(Vec<(Item, Payload<O>)>),
    Forward(Hash256),
}
pub(super) enum Assignment<O> {
    Wait { capacity: bool },
    Run(Lease, Operation<O>),
    Finished,
}
pub(super) struct Snapshot {
    pub target: Point,
    pub agreed: Option<Point>,
    pub cursor: Option<Point>,
    pub selected: Option<Point>,
    pub members: usize,
    pub buffered: usize,
}
struct Group<O> {
    model: Scheduler,
    statistics: statistics::Task,
    statistics_failure: Option<statistics::Error>,
    members: BTreeMap<PeerPublicKey, Member>,
    payloads: BTreeMap<u64, Payload<O>>,
    agreed: Option<Point>,
    cursor: Option<Point>,
    selected: Option<Point>,
    finished: bool,
    failed: Option<Error>,
}
impl<O> Group<O> {
    fn statistics(&self) -> statistics::Snapshot {
        let mut snapshot = self.statistics.snapshot();
        snapshot.failure = self.statistics_failure;
        if snapshot.failure.is_some() {
            snapshot.generations_per_minute = Err(statistics::Error::Stopped);
        }
        snapshot
    }
}
pub(super) struct Book<O> {
    groups: Vec<Group<O>>,
    sequence: u64,
    top_target: u64,
    lifecycle: lifecycle::Signal,
    statistics_terminated: u64,
    last_statistics: Option<statistics::Snapshot>,
}
pub(super) enum Join {
    Joined(u64),
    Lookup {
        chain: u64,
        height: u64,
        top: Hash256,
        clients: Vec<net::Client>,
    },
}
impl<O> Book<O> {
    pub fn statistics(&self) -> statistics::Status {
        statistics::Status {
            tasks: self.groups.iter().map(Group::statistics).collect(),
            terminated: self.statistics_terminated,
            last_terminated: self.last_statistics.clone(),
        }
    }
    pub fn lifecycle(&self) -> (Vec<lifecycle::Task>, u64, u64) {
        (
            self.groups
                .iter()
                .map(|g| lifecycle::Task {
                    id: g.model.state().chain.id(),
                    target: g.model.state().chain.target().height,
                    suspect: g.model.state().suspect,
                })
                .collect(),
            self.top_target,
            self.lifecycle.changes,
        )
    }

    fn lifecycle_changed(&mut self) {
        // Count every task transition before watch notifications can coalesce.
        self.lifecycle
            .update(self.groups.iter().any(|g| !g.model.state().suspect));
    }
    #[cfg(test)]
    pub fn posting_client(&self) -> Option<net::Client> {
        self.groups
            .iter()
            .flat_map(|g| g.members.values())
            .find(|m| matches!(m.lease, Some((_, Kind::Post { .. }))))
            .map(|m| m.client.clone())
    }
    pub fn sync_height(&self, client: &net::Client) -> Option<u64> {
        self.groups
            .iter()
            .filter_map(|g| g.members.get(&client.peer()))
            .find(|m| m.client.generation() == client.generation())
            .and_then(|m| m.sync_height)
    }
    pub fn new() -> Self {
        Self {
            groups: vec![],
            sequence: 0,
            top_target: 0,
            lifecycle: Default::default(),
            statistics_terminated: 0,
            last_statistics: None,
        }
    }
    pub fn number(&mut self) -> Result<u64, Error> {
        self.sequence = self.sequence.checked_add(1).ok_or(Error::ResourceLimit)?;
        Ok(self.sequence)
    }
    fn group(&self, id: u64) -> Result<&Group<O>, Error> {
        self.groups
            .iter()
            .find(|g| g.model.state().chain.id() == id)
            .ok_or(Error::Stale)
    }
    fn group_mut(&mut self, id: u64) -> Result<&mut Group<O>, Error> {
        self.groups
            .iter_mut()
            .find(|g| g.model.state().chain.id() == id)
            .ok_or(Error::Stale)
    }
    pub fn join(
        &mut self,
        candidate: &Chain,
        client: &net::Client,
        limits: coordination::Limits,
        now_ms: i128,
    ) -> Result<Join, Error> {
        let candidates: Vec<_> = self
            .groups
            .iter()
            .filter(|g| !g.finished && (g.failed.is_none() || g.model.state().suspect))
            .map(|g| g.model.state().chain.clone())
            .collect();
        let id = match coordination::match_tasks(candidate, &candidates) {
            coordination::TaskMatch::New => {
                if self.groups.len() >= limits.peers {
                    return Err(Error::Busy);
                }
                self.groups.push(Group {
                    model: Scheduler::new(candidate.clone(), limits)?,
                    statistics: statistics::Task::new(
                        candidate.id(),
                        candidate.target(),
                        limits.items,
                    )
                    .map_err(|_| Error::Configuration)?,
                    statistics_failure: None,
                    members: BTreeMap::new(),
                    payloads: BTreeMap::new(),
                    agreed: None,
                    cursor: None,
                    selected: None,
                    finished: false,
                    failed: None,
                });
                candidate.id()
            }
            coordination::TaskMatch::Existing(index) => {
                let id = candidates[index].id();
                self.group_mut(id)?
                    .model
                    .merge_chain(&candidate.clone().with_id(id))?;
                id
            }
            coordination::TaskMatch::Need { chain, height } => {
                let clients = if chain == candidate.id() {
                    vec![client.clone()]
                } else {
                    let group = self.group(chain)?;
                    group
                        .model
                        .state()
                        .chain
                        .peers()
                        .iter()
                        .filter_map(|p| group.members.get(p))
                        .filter(|m| m.failed.is_none())
                        .map(|m| m.client.clone())
                        .collect()
                };
                return Ok(Join::Lookup {
                    chain,
                    height,
                    top: candidate.next_known_hash(height.into()),
                    clients,
                });
            }
        };
        let group = self.group_mut(id)?;
        if group.model.state().suspect {
            return Err(group.failed.clone().unwrap_or(Error::BlockedByWhitelist));
        }
        if group.members.contains_key(&client.peer()) {
            return Err(Error::Stale);
        }
        group.members.insert(
            client.peer(),
            Member {
                client: client.clone(),
                lease: None,
                exhausted: None,
                sync_height: None,
                failed: None,
            },
        );
        group
            .model
            .workers(group.members.keys().copied().collect())?;
        group
            .statistics
            .update_target(group.model.state().chain.target(), now_ms);
        self.top_target = self.top_target.max(candidate.target().height);
        self.lifecycle_changed();
        Ok(Join::Joined(id))
    }
    pub fn add_hint(
        &mut self,
        hint: &Chain,
        client: &net::Client,
        limits: coordination::Limits,
    ) -> Result<(), Error> {
        let group = self.group_mut(hint.id())?;
        let member = group.members.get(&client.peer()).ok_or(Error::Stale)?;
        if member.client.generation() != client.generation() || member.failed.is_some() {
            return Err(Error::Stale);
        }
        let current = group.model.state().chain.clone();
        let hint = Chain::new(
            hint.id(),
            current.peers().to_vec(),
            hint.blocks().to_vec(),
            limits,
        )?;
        group.model.merge_chain(&hint)?;
        if group.model.state().chain == current {
            return Err(Error::NoProgress);
        }
        Ok(())
    }
    pub fn snapshot(&self, id: u64, client: &net::Client) -> Result<Snapshot, Error> {
        let g = self.group(id)?;
        let member = g.members.get(&client.peer()).ok_or(Error::Stale)?;
        if member.client.generation() != client.generation() {
            return Err(Error::Stale);
        }
        if let Some(error) = g.failed.as_ref().or(member.failed.as_ref()) {
            return Err(error.clone());
        }
        Ok(Snapshot {
            target: g.model.state().chain.target(),
            agreed: g.agreed,
            cursor: g.cursor,
            selected: g.selected,
            members: g.members.len(),
            buffered: g.payloads.len(),
        })
    }
    fn check(&mut self, lease: &Lease) -> Result<&mut Group<O>, Error> {
        let group = self.group_mut(lease.task)?;
        let member = group.members.get(&lease.peer).ok_or(Error::Stale)?;
        if member.client.generation() != &lease.generation
            || member.lease.is_none_or(|(n, _)| n != lease.sequence)
            || member.failed.is_some()
            || group.failed.is_some()
        {
            return Err(Error::Stale);
        }
        Ok(group)
    }
    pub fn detach(&mut self, id: u64, client: &net::Client, limits: coordination::Limits) {
        let Ok(group) = self.group_mut(id) else {
            return;
        };
        let Some(member) = group.members.get(&client.peer()) else {
            return;
        };
        if member.client.generation() != client.generation() {
            return;
        }
        if let Some((_, kind)) = member.lease {
            let mut state = group.model.state().clone();
            match kind {
                Kind::Fill { .. } => {
                    if matches!(state.next_pool, NextPool::Fetching) {
                        state.next_pool = NextPool::None;
                    }
                }
                Kind::Post { posted } => {
                    let mut retry: Vec<_> = state
                        .adding
                        .iter()
                        .skip(posted)
                        .map(|i| Item::missing(i.point))
                        .collect();
                    retry.extend(state.pending.drain(..).flatten());
                    retry.append(&mut state.pool);
                    state.pool = retry;
                    state.adding.clear();
                }
                _ => (),
            }
            match Scheduler::restore(state, limits) {
                Ok(model) => group.model = model,
                Err(e) => group.failed = Some(e.into()),
            }
        }
        group.members.remove(&client.peer());
        let _ = group.model.remove_peer(client.peer());
        let ended = group.members.is_empty();
        if ended {
            // Retain suspect chain hints, never downloaded bodies or credits
            // after the final member has left this terminal task.
            group.payloads.clear();
        }
        let terminated = if ended && !group.model.state().suspect {
            let mut snapshot = group.statistics();
            if let Err(error) = snapshot.total.rates() {
                snapshot.failure.get_or_insert(error);
            }
            Some(snapshot)
        } else {
            None
        };
        if let Some(snapshot) = terminated {
            self.last_statistics = Some(snapshot);
            self.statistics_terminated = self.statistics_terminated.saturating_add(1);
        }
        self.groups
            .retain(|g| !g.members.is_empty() || g.model.state().suspect);
        if ended {
            self.top_target = self
                .groups
                .iter()
                .filter(|g| !g.model.state().suspect)
                .map(|g| g.model.state().chain.target().height)
                .max()
                .unwrap_or(0);
        }
        self.lifecycle_changed();
    }
}
pub(super) struct Membership<O: MapOrder + Send + Sync + 'static> {
    context: Arc<Context<O>>,
    client: net::Client,
    pub task: u64,
}
impl<O: MapOrder + Send + Sync + 'static> Membership<O> {
    pub fn new(context: Arc<Context<O>>, client: net::Client, task: u64) -> Self {
        Self {
            context,
            client,
            task,
        }
    }
}
impl<O: MapOrder + Send + Sync + 'static> Drop for Membership<O> {
    fn drop(&mut self) {
        self.context
            .shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .detach(
                self.task,
                &self.client,
                self.context.config.coordination_limits(),
            );
        self.context.wake.send_replace(());
    }
}

#[cfg(test)]
mod tests;
