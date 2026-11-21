use super::*;

// Tickets identify payloads owned by the asynchronous coordinator. This model
// never treats a received payload or a successful lookup as block admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ready {
    pub peer: PeerPublicKey,
    pub ticket: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Item {
    pub point: Point,
    pub got: Option<Ready>,
    pub attempts: u64,
}
impl Item {
    pub fn missing(point: Point) -> Self {
        Self {
            point,
            got: None,
            attempts: 0,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NextPool {
    None,
    Fetching,
    Ready(Vec<Item>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct State {
    pub chain: Chain,
    pub suspect: bool,
    pub agreed: Option<Point>,
    pub pool: Vec<Item>,
    pub next_pool: NextPool,
    pub adding: Vec<Item>,
    // Oldest chunk first. The last chunk is the native pending accumulator.
    pub pending: VecDeque<Vec<Item>>,
    pub workers: Vec<PeerPublicKey>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Work {
    Break,
    Abort,
    Agree,
    Fill { from: Hash256, target: Hash256 },
    Generation(Point),
    Post(Vec<Item>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultInput {
    Agreed(Point),
    Hashes {
        first_height: u64,
        items: Vec<Item>,
    },
    Generation {
        point: Point,
        ready: Ready,
    },
    Posted,
    PostedAt {
        height: u64,
        stats: u64,
    },
    PostError {
        peer: PeerPublicKey,
        height: u64,
        rejected: bool,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    SyncHeight { peer: PeerPublicKey, height: u64 },
    Disconnect(PeerPublicKey),
    Suspect(PeerPublicKey),
    Done(PeerPublicKey),
    Statistics { task: u64, stats: u64 },
}
#[derive(Clone)]
pub struct Scheduler {
    state: State,
    limits: Limits,
}
impl Scheduler {
    pub fn new(chain: Chain, limits: Limits) -> Result<Self, Error> {
        Self::restore(
            State {
                chain,
                suspect: false,
                agreed: None,
                pool: vec![],
                next_pool: NextPool::None,
                adding: vec![],
                pending: VecDeque::new(),
                workers: vec![],
            },
            limits,
        )
    }
    pub fn restore(state: State, limits: Limits) -> Result<Self, Error> {
        Self::validate(&state, limits)?;
        Ok(Self { state, limits })
    }
    pub fn state(&self) -> &State {
        &self.state
    }
    fn validate(s: &State, limits: Limits) -> Result<(), Error> {
        s.chain.validate(limits)?;
        let next = match &s.next_pool {
            NextPool::Ready(items) => {
                if items.is_empty() || items.iter().any(|i| i.got.is_some()) {
                    return Err(Error::State);
                }
                items.len()
            }
            _ => 0,
        };
        let total = s
            .pool
            .len()
            .checked_add(s.adding.len())
            .and_then(|n| n.checked_add(next))
            .and_then(|n| s.pending.iter().try_fold(n, |n, p| n.checked_add(p.len())))
            .ok_or(Error::Capacity)?;
        if total > limits.items || s.workers.len() > limits.peers || s.pending.len() > limits.items
        {
            return Err(Error::Capacity);
        }
        if s.adding.len() > limits.chunk
            || s.pending.iter().any(|p| p.len() > limits.chunk)
            || s.adding
                .iter()
                .chain(s.pending.iter().flatten())
                .any(|i| i.got.is_none())
        {
            return Err(Error::State);
        }
        Ok(())
    }
    fn update<T>(
        &mut self,
        operation: impl FnOnce(&mut State, Limits) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut next = self.state.clone();
        let result = operation(&mut next, self.limits)?;
        Self::validate(&next, self.limits)?;
        self.state = next;
        Ok(result)
    }
    pub fn merge_chain(&mut self, chain: &Chain) -> Result<(), Error> {
        self.update(|s, l| {
            s.chain = chain.merge(&s.chain, l)?;
            Ok(())
        })
    }
    pub fn workers(&mut self, workers: Vec<PeerPublicKey>) -> Result<(), Error> {
        self.update(|s, _| {
            s.workers = workers;
            Ok(())
        })
    }
    pub fn remove_peer(&mut self, peer: PeerPublicKey) -> Result<(), Error> {
        self.update(|s, _| {
            s.chain.remove_peer(peer);
            s.workers.retain(|p| *p != peer);
            Ok(())
        })
    }
    // The runtime must bind a result to its active request/connection before
    // entering this native transition. In particular, height alone is not a
    // secure request identity and a Done effect is not canonical admission.
    pub fn apply(&mut self, input: ResultInput) -> Result<Vec<Effect>, Error> {
        self.update(|s, _| {
            let mut effects = vec![];
            match input {
                ResultInput::Agreed(point) => {
                    s.agreed.get_or_insert(point);
                }
                ResultInput::Hashes {
                    first_height,
                    items,
                } => {
                    if items.is_empty() || items.iter().any(|i| i.got.is_some()) {
                        return Err(Error::State);
                    }
                    if s.pool.is_empty() {
                        s.agreed = Some(items.last().expect("nonempty").point);
                        s.pool = items;
                        s.next_pool = NextPool::None;
                    } else if first_height > s.agreed.ok_or(Error::State)?.height {
                        s.next_pool = NextPool::Ready(items);
                    }
                }
                ResultInput::Generation { point, ready } => {
                    if let Some(item) = s.pool.iter_mut().find(|i| i.point.height == point.height) {
                        *item = Item {
                            point,
                            got: Some(ready),
                            attempts: 0,
                        };
                    }
                }
                ResultInput::Posted => s.adding.clear(),
                ResultInput::PostedAt { height, stats } => {
                    effects.extend(s.workers.iter().map(|peer| Effect::SyncHeight {
                        peer: *peer,
                        height,
                    }));
                    effects.push(Effect::Statistics {
                        task: s.chain.id(),
                        stats,
                    });
                    s.adding.clear();
                }
                ResultInput::PostError {
                    peer,
                    height,
                    rejected,
                } => {
                    for worker in &s.workers {
                        if rejected {
                            effects.extend([
                                Effect::Disconnect(*worker),
                                Effect::Suspect(*worker),
                                Effect::Done(*worker),
                            ]);
                        } else {
                            effects.push(Effect::SyncHeight {
                                peer: *worker,
                                height,
                            });
                        }
                    }
                    let start = s
                        .adding
                        .iter()
                        .position(|i| i.point.height >= height)
                        .unwrap_or(s.adding.len());
                    let mut remaining: Vec<_> = s.adding.drain(start..).collect();
                    remaining.extend(s.pending.drain(..).flatten());
                    let first = remaining.first_mut().ok_or(Error::State)?;
                    if first.point.height != height {
                        return Err(Error::State);
                    }
                    *first = Item::missing(first.point);
                    remaining.append(&mut s.pool);
                    s.pool = remaining;
                    s.adding.clear();
                    s.chain.remove_peer(peer);
                    if rejected {
                        s.suspect = true;
                        s.workers.clear();
                    }
                }
            }
            Ok(effects)
        })
    }
    pub fn next_for(&mut self, peer: PeerPublicKey) -> Result<Work, Error> {
        if self.state.suspect || !self.state.chain.peers().contains(&peer) {
            return Ok(Work::Abort);
        }
        self.next_work()
    }
    pub fn next_work(&mut self) -> Result<Work, Error> {
        self.next_available(&[], true, true)
    }
    pub(crate) fn next_available(
        &mut self,
        leased: &[Hash256],
        fill_allowed: bool,
        agree_allowed: bool,
    ) -> Result<Work, Error> {
        self.update(|s, l| {
            loop {
                // Original pp_sync_task evaluates an invalid pending-list printer
                // on this branch. Keep the intended break instead of that crash.
                if s.suspect {
                    return Ok(Work::Break);
                }
                let Some(agreed) = s.agreed else {
                    return Ok(if agree_allowed {
                        Work::Agree
                    } else {
                        Work::Break
                    });
                };
                let prefix = s.pool.iter().take_while(|i| i.got.is_some()).count();
                if prefix > 0 {
                    for item in s.pool.drain(..prefix) {
                        if s.pending.back().is_none_or(|p| p.len() == l.chunk) {
                            s.pending.push_back(vec![]);
                        }
                        s.pending.back_mut().expect("chunk exists").push(item);
                    }
                    continue;
                }
                if s.adding.is_empty()
                    && let Some(items) = s.pending.pop_front()
                {
                    s.adding = items;
                    return Ok(Work::Post(s.adding.clone()));
                }
                if !s.pool.is_empty() {
                    if fill_allowed && matches!(s.next_pool, NextPool::None) && s.workers.len() > 2
                    {
                        return Ok(fill(s, agreed, l));
                    }
                    let Some(index) = s
                        .pool
                        .iter()
                        .enumerate()
                        .filter(|(_, i)| i.got.is_none() && !leased.contains(&i.point.hash))
                        .min_by_key(|(_, i)| (i.attempts, i.point.height))
                        .map(|(i, _)| i)
                    else {
                        return Ok(Work::Break);
                    };
                    let item = &mut s.pool[index];
                    item.attempts = item.attempts.checked_add(1).ok_or(Error::Overflow)?;
                    return Ok(Work::Generation(item.point));
                }
                if s.pending.len().saturating_sub(1) > l.forward_chunks {
                    return Ok(Work::Break);
                }
                if matches!(s.next_pool, NextPool::Ready(_)) {
                    let NextPool::Ready(items) =
                        std::mem::replace(&mut s.next_pool, NextPool::None)
                    else {
                        unreachable!()
                    };
                    s.agreed = Some(items.last().ok_or(Error::State)?.point);
                    s.pool = items;
                    continue;
                }
                return Ok(if fill_allowed {
                    fill(s, agreed, l)
                } else {
                    Work::Break
                });
            }
        })
    }
}
fn fill(s: &mut State, agreed: Point, limits: Limits) -> Work {
    s.next_pool = NextPool::Fetching;
    Work::Fill {
        from: agreed.hash,
        target: s
            .chain
            .next_known_hash(u128::from(agreed.height) + limits.chunk as u128),
    }
}
